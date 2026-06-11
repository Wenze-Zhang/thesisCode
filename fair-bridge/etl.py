#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import config
import registry


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
log = logging.getLogger("fair-bridge-etl")


#  metadata
_META_KEYS = {
    "deviceName",
    "deviceType",
    "deviceId",
    "ts",
    "timestamp",
    "ds",
    "tenantId",
    "customerId",
    "name",
    "type",
}



def _wait_kafka(bootstrap: str, timeout: int) -> None:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    deadline = time.time() + timeout
    last_exc: Exception | None = None
    
    
    while time.time() < deadline:
        # test kafka connection by creating and closing a producer
        try:
            KafkaProducer(bootstrap_servers=bootstrap).close()
            return
        except NoBrokersAvailable as exc:
            last_exc = exc
            time.sleep(4)
    raise RuntimeError(f"Kafka not ready after {timeout}s ({last_exc})")


# Normalise timestamp to ISO 8601  
def _normalise_ts(raw_ts: Any) -> str:
    if isinstance(raw_ts, str):
        try:
            datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            return raw_ts
        except ValueError:
            pass
    try:
        value = float(raw_ts)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).isoformat()
    # millisecond to second
    if value > 1e12:   
        value /= 1000.0
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def validate_payload(
    values: dict,
    sensor_type: str,
    reg: registry.Registry,
) -> str | None:
    return reg.validate(values, sensor_type)


def process_message(
    msg_value: dict,
    headers: dict,
    *,
    reg: registry.Registry | None = None,
) -> tuple[str, dict]:
    
    # load registry
    reg = reg if reg is not None else registry.load_registry(config.REGISTRY_PATH)
    
    # get device name & ID from message value or Kafka headers
    device_name = (
        msg_value.get("deviceName")
        or headers.get("deviceName")
        or msg_value.get("name")
        or ""
    )
    device_id = msg_value.get("deviceId") or headers.get("deviceId") or ""

    # transform values from message into canonical.
    values = msg_value.get("values")
    if values is None:
        values = msg_value.get("data")
    if values is None and isinstance(msg_value, dict):
        values = {key: value for key, value in msg_value.items() if key not in _META_KEYS}

    # normalised timestamp
    raw_ts = msg_value.get("ts") or msg_value.get("timestamp") or headers.get("ts")
    ts_iso = _normalise_ts(raw_ts)
    
    # sensor type
    sensor_type = reg.classify(device_name)

    # core process
    payload = {
        "device_name": device_name,
        "device_id": device_id,
        "sensor_type": sensor_type,
        "ts": ts_iso,
        "values": values,
    }

    if not isinstance(values, dict) or not values:
        payload["error"] = "missing or non-dict 'values'"
        return config.KAFKA_TOPIC_DLQ, payload

    # canonicalize
    values, renamed, unknown = reg.canonicalize(values)
    payload["values"] = values

    if unknown:
        payload["error"] = f"unknown fields: {', '.join(str(key) for key in unknown)}"
        return config.KAFKA_TOPIC_DLQ, payload


    # validate with schema
    error = validate_payload(values, sensor_type, reg)
    if error is not None:
        payload["error"] = f"schema: {error}"
        return config.KAFKA_TOPIC_DLQ, payload

    payload["quality"] = "validated"
    if renamed:
        payload["canonicalized"] = renamed
    return config.KAFKA_TOPIC_TELEMETRY_VALIDATED, payload


def main() -> int:
    from kafka import KafkaConsumer, KafkaProducer

    reg = registry.load_registry(config.REGISTRY_PATH)
    log.info("Registry ready: %d sensor types.", len(reg.sensor_types))
    log.info("Waiting for Kafka at %s ...", config.KAFKA_BOOTSTRAP_SERVERS)
    _wait_kafka(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_READY_TIMEOUT_S)

    # define Kafka consumer and producer with JSON 
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC_TELEMETRY_RAW,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.KAFKA_CONSUMER_GROUP_ETL,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        # tansmit from JSON bytes to Python dict
        value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else {},
    )
    
    
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        # transmit from Python dict to JSON bytes
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )

    log.info("ETL listening on '%s' ...", config.KAFKA_TOPIC_TELEMETRY_RAW)
    validated_count = 0
    dlq_count = 0

    for msg in consumer:
        try:
            # tb_msg_md_deviceName -> deviceName
            headers = {
                key.removeprefix("tb_msg_md_"): value.decode("utf-8", "replace")
                for key, value in (msg.headers or [])
            }
            target, payload = process_message(msg.value or {}, headers, reg=reg)
            producer.send(target, payload)

            if target == config.KAFKA_TOPIC_DLQ:
                dlq_count += 1
                log.warning("DLQ <- %s : %s", payload.get("device_name"), payload.get("error"))
            else:
                validated_count += 1

            if (validated_count + dlq_count) % 100 == 0:
                log.info(
                    "ETL stats: validated=%d dlq=%d",
                    validated_count,
                    dlq_count,
                )
        except Exception:
            log.exception("Unhandled error at offset %s; routing to DLQ", msg.offset)
            try:
                producer.send(
                    config.KAFKA_TOPIC_DLQ,
                    {
                        "raw": msg.value,
                        "error": "etl-exception",
                        "offset": msg.offset,
                    },
                )
                dlq_count += 1
            except Exception:
                log.exception("Failed to publish to DLQ.")

    producer.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
