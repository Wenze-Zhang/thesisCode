#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"
DEFAULT_TARGET_ACHIEVED_THRESHOLD = 0.95
DEFAULT_LIVE_POLL_INTERVAL_S = 0.05

if str(FAIR_BRIDGE_DIR) not in sys.path:
    sys.path.insert(0, str(FAIR_BRIDGE_DIR))

import config  # noqa: E402

KAFKA_EVENT_COLUMNS = [
    "test_id",
    "repetition",
    "workload",
    "offered_load_msg_s",
    "device_count",
    "device_index",
    "sensor_type",
    "seq",
    "device_id",
    "expected_topic",
    "actual_topic",
    "payload_valid",
    "invalid_variant",
    "sent_at",
    "produced_at",
    "validated_seen_at",
    "dlq_seen_at",
    "output_seen_at",
    "latency_ms",
]

INVALID_VARIANTS = [
    "out_of_range",
    "wrong_datatype",
    "unknown_field",
    "missing_values",
    "non_dict_values",
    "invalid_enum",
    "unknown_sensor_type",
]

SENSOR_TYPES = [
    "climate",
    "energy",
    "water",
    "air_quality",
    "ev_charger",
]

ENUM_SENSOR_TYPES = [
    "climate",
    "energy",
    "ev_charger",
]

# Device count is fixed across presets so offered load is the only controlled variable.
WORKLOAD_PRESETS = {
    "small": {"offered_load_msg_s": 100.0, "device_count": 100},
    "medium": {"offered_load_msg_s": 500.0, "device_count": 100},
    "big": {"offered_load_msg_s": 1000.0, "device_count": 100},
}

SENSOR_TYPE_CHOICES = [*SENSOR_TYPES, "mixed"]

# parameters into python dict
def _parse_rates(value: str) -> list[float]:
    rates = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        rate = float(token)
        if rate <= 0:
            raise argparse.ArgumentTypeError("Rates must be positive")
        rates.append(rate)
    if not rates:
        raise argparse.ArgumentTypeError("At least one rate is required")
    return rates


def _parse_workloads(value: str) -> list[str]:
    workloads = []
    for token in value.split(","):
        workload = token.strip().lower()
        if not workload:
            continue
        if workload not in WORKLOAD_PRESETS:
            choices = ", ".join(WORKLOAD_PRESETS)
            raise argparse.ArgumentTypeError(
                f"Unsupported workload {workload!r}; choose from {choices}"
            )
        workloads.append(workload)
    if not workloads:
        raise argparse.ArgumentTypeError("At least one workload is required")
    return workloads


def _workload_specs(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.rate:
        device_count = args.device_count or 1
        if device_count <= 0:
            raise SystemExit("--device-count must be positive")
        return [
            {
                "workload": f"custom-{_rate_label(rate)}",
                "offered_load_msg_s": rate,
                "device_count": device_count,
            }
            for rate in _parse_rates(args.rate)
        ]

    return [
        {
            "workload": workload,
            "offered_load_msg_s": WORKLOAD_PRESETS[workload]["offered_load_msg_s"],
            "device_count": WORKLOAD_PRESETS[workload]["device_count"],
        }
        for workload in _parse_workloads(args.workloads)
    ]

# 1.5 into 1p5 to put into deviceId or kafka consumer group id
def _rate_label(rate: float) -> str:
    return str(rate).replace(".", "p")

# return timestamp in iso format with timezone
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ISO string to datatime
def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# calcualte p50 and p95 percentiles
def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    # linear interpolation 
    rank = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(ordered[int(rank)], 6)
    weight = rank - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 6)


def _join_fail_reasons(reasons: list[str]) -> str:
    return ";".join(dict.fromkeys(reason for reason in reasons if reason))


def _is_unexpected_dlq_rate_ok(
    unexpected_dlq_count: int,
    expected_validated_count: int,
    max_unexpected_dlq_rate: float,
) -> bool:
    if expected_validated_count <= 0:
        return unexpected_dlq_count == 0
    return (unexpected_dlq_count / expected_validated_count) <= max_unexpected_dlq_rate


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)

# payload size padding to increase processing time and message size
def _padding(payload_size: str) -> str:
    if payload_size == "small":
        return ""
    if payload_size == "medium":
        return "x" * 1024
    if payload_size == "large":
        return "x" * 10_240
    raise ValueError(f"Unsupported payload size: {payload_size}")


def _invalid_variant_for_seq(seq: int, sensor_type: str) -> str:
    variants = INVALID_VARIANTS
    if sensor_type not in ENUM_SENSOR_TYPES:
        variants = [variant for variant in INVALID_VARIANTS if variant != "invalid_enum"]
    return variants[(seq - 1) % len(variants)]


def _sensor_label(sensor_type: str) -> str:
    return sensor_type.replace("_", "-")


def _sensor_type_for_device(device_index: int, sensor_type_mode: str) -> str:
    if sensor_type_mode == "mixed":
        return SENSOR_TYPES[(device_index - 1) % len(SENSOR_TYPES)]
    return sensor_type_mode


def _benchmark_device_name(
    sensor_type: str,
    test_id: str,
    workload: str,
    device_index: int,
) -> str:
    return f"{_sensor_label(sensor_type)}-benchmark-{test_id}-{workload}-{device_index:04d}"


def _benchmark_device_id(test_id: str, workload: str, device_index: int) -> str:
    return f"{test_id}-{workload}-device-{device_index:04d}"


def _event_key(device_id: str, ts: str) -> str:
    return f"{device_id}|{ts}"


def _valid_values(sensor_type: str, seq: int) -> dict[str, Any]:
    if sensor_type == "climate":
        return {
            "temperature_c": round(20.0 + (seq % 100) / 10.0, 2),
            "humidity_pct": round(40.0 + (seq % 50) / 2.0, 2),
        }
    if sensor_type == "energy":
        return {
            "power_w": round(500.0 + (seq % 100) * 25.0, 3),
            "voltage_v": round(220.0 + (seq % 20), 3),
        }
    if sensor_type == "water":
        return {
            "flow_lpm": round(5.0 + (seq % 100) * 2.0, 3),
            "pressure_bar": round(1.0 + (seq % 20) / 2.0, 3),
        }
    if sensor_type == "air_quality":
        return {
            "pm2_5_ugm3": round(5.0 + (seq % 100) * 1.5, 3),
            "aqi": seq % 300,
        }
    if sensor_type == "ev_charger":
        states = ["idle", "charging", "complete", "fault"]
        return {
            "state": states[seq % len(states)],
            "power_kw": round((seq % 50) * 2.5, 3),
        }
    raise ValueError(f"Unsupported sensor type: {sensor_type}")


def _out_of_range_values(sensor_type: str) -> dict[str, Any]:
    values = {
        "climate": {"temperature_c": 999.0},
        "energy": {"power_w": 100_001.0},
        "water": {"flow_lpm": 5_001.0},
        "air_quality": {"pm2_5_ugm3": 1_001.0},
        "ev_charger": {"power_kw": 351.0},
    }
    return values[sensor_type]


def _wrong_datatype_values(sensor_type: str) -> dict[str, Any]:
    values = {
        "climate": {"temperature_c": "not-a-number"},
        "energy": {"power_w": "not-a-number"},
        "water": {"leak_detected": "false"},
        "air_quality": {"aqi": "not-an-integer"},
        "ev_charger": {"power_kw": "not-a-number"},
    }
    return values[sensor_type]


def _invalid_enum_values(sensor_type: str) -> dict[str, Any]:
    values = {
        "climate": {"hvac_state": "running"},
        "energy": {"phase": "4P"},
        "ev_charger": {"state": "paused"},
    }
    return values[sensor_type]


# generate messages for testing with valid or invalid values
def _synthetic_message(
    *,
    test_id: str,
    workload: str,
    rate: float,
    seq: int,
    device_index: int,
    sensor_type: str,
    valid: bool,
    payload_size: str,
    invalid_variant: str = "",
) -> dict[str, Any]:
    sent_at = _now_iso()
    variant = "" if valid else (invalid_variant or _invalid_variant_for_seq(seq, sensor_type))
    message = {
        "deviceName": _benchmark_device_name(sensor_type, test_id, workload, device_index),
        "deviceId": _benchmark_device_id(test_id, workload, device_index),
        "ts": sent_at,
    }

    if valid:
        values: Any = _valid_values(sensor_type, seq)
        message["values"] = values
    else:
        if variant == "out_of_range":
            values = _out_of_range_values(sensor_type)
        elif variant == "wrong_datatype":
            values = _wrong_datatype_values(sensor_type)
        elif variant == "unknown_field":
            values = _valid_values(sensor_type, seq)
            values["unknown_benchmark_field"] = 1
        elif variant == "missing_values":
            values = None
        elif variant == "non_dict_values":
            values = ["not", "an", "object"]
        elif variant == "invalid_enum":
            values = _invalid_enum_values(sensor_type)
        elif variant == "unknown_sensor_type":
            message["deviceName"] = f"unknown-benchmark-{test_id}"
            values = _valid_values("climate", seq)
        else:
            raise ValueError(f"Unsupported invalid variant: {variant}")

        if variant != "missing_values":
            message["values"] = values

    # Extra benchmark metadata is safe when values exists. For missing_values,
    # keep the raw payload minimal so etl.py reports the intended error.
    if "values" in message:
        message["test_id"] = test_id
        message["workload"] = workload
        message["seq"] = seq
        message["device_index"] = device_index
        message["sent_at"] = sent_at
        padding = _padding(payload_size)
        if padding:
            message["padding"] = padding
    return message


# write csv file with header and rows
def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # with is file mangaer. After use, file is closed automatically 
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _import_kafka_clients():
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "kafka-python is required for --mode kafka-e2e. "
            "Install fair-bridge/requirements.txt in the local environment or run inside the project environment."
        ) from exc
    return KafkaConsumer, KafkaProducer


def _make_consumer(KafkaConsumer, topic: str, group_id: str, bootstrap_server: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # bytes in kafka -> python dict
        value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else {},
    )
    deadline = time.time() + 10
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(timeout_ms=200)
    return consumer

# poll messages from validated and dlq topics
def _collect_outputs(
    consumer,
    topic: str,
    events: dict[str, dict[str, Any]],
    lock: threading.Lock,
    *,
    timeout_ms: int = 0,
    max_records: int = 500,
) -> int:
    seen = 0
    polled = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
    seen_at = _now_iso()
    seen_perf = time.perf_counter()
    with lock:
        for batch in polled.values():
            for record in batch:
                payload = record.value or {}
                device_id = payload.get("device_id") or payload.get("deviceId")
                ts = payload.get("ts") or payload.get("timestamp") or ""
                event_key = _event_key(str(device_id), str(ts))
                if event_key not in events:
                    continue
                event = events[event_key]
                if topic == config.KAFKA_TOPIC_TELEMETRY_VALIDATED:
                    if event.get("validated_seen_at"):
                        continue
                    event["validated_seen_at"] = seen_at
                else:
                    if event.get("dlq_seen_at"):
                        continue
                    event["dlq_seen_at"] = seen_at
                if not event.get("output_seen_at"):
                    event["output_seen_at"] = seen_at
                    event["seen_perf"] = seen_perf
                    event["actual_topic"] = topic
                    try:
                        # latency calculation
                        sent_perf = event.get("sent_perf")
                        if sent_perf is not None:
                            latency_s = seen_perf - float(sent_perf)
                        else:
                            latency_s = (
                                _parse_iso(seen_at) - _parse_iso(event["sent_at"])
                            ).total_seconds()
                        event["latency_ms"] = round(latency_s * 1000.0, 6)
                    except Exception:
                        event["latency_ms"] = None
                elif event.get("actual_topic") not in ("", topic, "multiple"):
                    event["actual_topic"] = "multiple"
                seen += 1
    return seen


def _collector_loop(
    validated_consumer,
    dlq_consumer,
    events: dict[str, dict[str, Any]],
    lock: threading.Lock,
    stop_event: threading.Event,
    poll_timeout_ms: int,
) -> None:
    # Each consumer is only ever used from this thread; the events dict is the
    # shared state and is guarded by the lock.
    while not stop_event.is_set():
        _collect_outputs(
            validated_consumer,
            config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            events,
            lock,
            timeout_ms=poll_timeout_ms,
        )
        _collect_outputs(
            dlq_consumer,
            config.KAFKA_TOPIC_DLQ,
            events,
            lock,
            timeout_ms=poll_timeout_ms,
        )


def _pace_until(target_perf: float) -> None:
    # Open-loop pacing: sleep for the bulk of the wait, then yield-spin the
    # last ~2 ms because time.sleep granularity is too coarse at 1000 msg/s.
    while True:
        remaining_s = target_perf - time.perf_counter()
        if remaining_s <= 0:
            return
        if remaining_s > 0.002:
            time.sleep(remaining_s - 0.001)
        else:
            time.sleep(0)


def _summarize_kafka_run(
    *,
    args: argparse.Namespace,
    repetition: int,
    workload: str,
    rate: float,
    device_count: int,
    events: dict[str, dict[str, Any]],
    produce_elapsed_s: float,
    backlog_end_of_production: int,
) -> dict[str, Any]:
    produced_count = len(events)
    validated_count = sum(1 for event in events.values() if event.get("validated_seen_at"))
    dlq_count = sum(1 for event in events.values() if event.get("dlq_seen_at"))
    etl_output_count = validated_count + dlq_count
    message_loss_count = produced_count - etl_output_count
    expected_validated_count = sum(
        1
        for event in events.values()
        if event.get("expected_topic") == config.KAFKA_TOPIC_TELEMETRY_VALIDATED
    )
    expected_dlq_count = sum(
        1
        for event in events.values()
        if event.get("expected_topic") == config.KAFKA_TOPIC_DLQ
    )
    unexpected_dlq_count = sum(
        1
        for event in events.values()
        if event.get("expected_topic") == config.KAFKA_TOPIC_TELEMETRY_VALIDATED
        and event.get("dlq_seen_at")
    )
    unexpected_validated_count = sum(
        1
        for event in events.values()
        if event.get("expected_topic") == config.KAFKA_TOPIC_DLQ
        and event.get("validated_seen_at")
    )
    misrouted_count = unexpected_dlq_count + unexpected_validated_count
    correctly_routed_count = sum(
        1
        for event in events.values()
        if event.get("actual_topic") == event.get("expected_topic")
    )
    backlog_after_cooldown = message_loss_count
    input_rate_msg_s = round(
        produced_count / max(produce_elapsed_s, 0.000001),
        6,
    )
    target_achieved_ratio = (
        round(input_rate_msg_s / rate, 6)
        if rate > 0
        else None
    )

    # ETL throughput over the real processing window: first send to last
    # observed output (covers messages drained during cooldown).
    sent_perfs = [
        float(event["sent_perf"])
        for event in events.values()
        if event.get("sent_perf") is not None
    ]
    seen_perfs = [
        float(event["seen_perf"])
        for event in events.values()
        if event.get("seen_perf") is not None
    ]
    if sent_perfs and seen_perfs:
        process_window_s = max(seen_perfs) - min(sent_perfs)
        etl_throughput_msg_s = round(
            etl_output_count / max(process_window_s, 0.000001),
            6,
        )
        process_window_s = round(process_window_s, 6)
    else:
        process_window_s = None
        etl_throughput_msg_s = None

    # Validation latency strictly follows t_validated - t_raw, so only
    # messages that landed on the validated topic count; the DLQ path is
    # reported separately as a sanity check.
    validation_latencies = [
        float(event["latency_ms"])
        for event in events.values()
        if event.get("actual_topic") == config.KAFKA_TOPIC_TELEMETRY_VALIDATED
        and event.get("latency_ms") is not None
    ]
    dlq_latencies = [
        float(event["latency_ms"])
        for event in events.values()
        if event.get("actual_topic") == config.KAFKA_TOPIC_DLQ
        and event.get("latency_ms") is not None
    ]
    p95 = _percentile(validation_latencies, 95)
    unexpected_dlq_rate = (
        round(unexpected_dlq_count / expected_validated_count, 6)
        if expected_validated_count
        else 0.0
    )
    fail_reasons: list[str] = []
    if backlog_after_cooldown != 0:
        fail_reasons.append("observed_backlog_after_cooldown")
    elif message_loss_count != 0:
        fail_reasons.append("message_loss")
    if misrouted_count != 0:
        fail_reasons.append("misrouted_message")
    if p95 is None or p95 > args.latency_threshold_ms:
        fail_reasons.append("p95_latency_above_threshold")
    if not _is_unexpected_dlq_rate_ok(
        unexpected_dlq_count,
        expected_validated_count,
        args.max_unexpected_dlq_rate,
    ):
        fail_reasons.append("unexpected_dlq_rate_above_threshold")
    if (
        target_achieved_ratio is None
        or target_achieved_ratio < args.target_achieved_threshold
    ):
        fail_reasons.append("target_load_not_achieved")
    passed = not fail_reasons

    return {
        # Run identity
        "test_id": args.test_id,
        "repetition": repetition,
        "workload": workload,
        "mode": "kafka-e2e",
        "payload_size": args.payload_size,
        "sensor_type": args.sensor_type,
        "invalid_ratio": args.invalid_ratio,
        "offered_load_msg_s": rate,
        "device_count": device_count,
        "per_device_rate_msg_s": round(rate / device_count, 6) if device_count else None,
        "duration_s": args.duration_s,
        "produce_elapsed_s": round(produce_elapsed_s, 6),
        # Core metrics
        "input_rate_msg_s": input_rate_msg_s,
        "etl_throughput_msg_s": etl_throughput_msg_s,
        "validation_latency_p50_ms": _percentile(validation_latencies, 50),
        "validation_latency_p95_ms": p95,
        "validation_latency_p99_ms": _percentile(validation_latencies, 99),
        "validation_latency_max_ms": (
            round(max(validation_latencies), 6) if validation_latencies else None
        ),
        "backlog_end_of_production": backlog_end_of_production,
        "backlog_after_cooldown": backlog_after_cooldown,
        # Sanity-check metrics
        "process_window_s": process_window_s,
        "target_produced_count": round(rate * args.duration_s),
        "produced_count": produced_count,
        "target_achieved_ratio": target_achieved_ratio,
        "target_achieved_threshold": args.target_achieved_threshold,
        "validated_count": validated_count,
        "dlq_count": dlq_count,
        "etl_output_count": etl_output_count,
        "etl_output_throughput_msg_s": round(
            etl_output_count / max(produce_elapsed_s, 0.000001),
            6,
        ),
        "dlq_latency_p50_ms": _percentile(dlq_latencies, 50),
        "dlq_latency_p95_ms": _percentile(dlq_latencies, 95),
        "message_loss_count": message_loss_count,
        "expected_validated_count": expected_validated_count,
        "expected_dlq_count": expected_dlq_count,
        "unexpected_dlq_count": unexpected_dlq_count,
        "unexpected_dlq_rate": unexpected_dlq_rate,
        "max_unexpected_dlq_rate": args.max_unexpected_dlq_rate,
        "unexpected_validated_count": unexpected_validated_count,
        "misrouted_count": misrouted_count,
        "valid_acceptance_rate": _safe_rate(
            expected_validated_count - unexpected_dlq_count,
            expected_validated_count,
        ),
        "invalid_rejection_rate": _safe_rate(
            expected_dlq_count - unexpected_validated_count,
            expected_dlq_count,
        ),
        "routing_success_rate": _safe_rate(correctly_routed_count, produced_count),
        "cooldown_s": args.cooldown_s,
        "latency_threshold_ms": args.latency_threshold_ms,
        "fail_reason": _join_fail_reasons(fail_reasons),
        "benchmark_result_pass_fail": "pass" if passed else "fail",
    }

# run through etl with kafka
def run_kafka_e2e(args: argparse.Namespace) -> tuple[Path, Path]:
    KafkaConsumer, KafkaProducer = _import_kafka_clients()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=0,
        acks="all",
    )
    rng = random.Random(args.seed)
    all_events: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    try:
        for repetition in range(1, args.repeat + 1):
            for workload_spec in args.workload_specs:
                workload = workload_spec["workload"]
                rate = float(workload_spec["offered_load_msg_s"])
                device_count = int(workload_spec["device_count"])
                rate_label = _rate_label(rate)
                group_suffix = f"{workload}-{repetition}-{time.time_ns()}"
                validated_consumer = _make_consumer(
                    KafkaConsumer,
                    config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
                    f"eval-{args.test_id}-{rate_label}-validated-{group_suffix}",
                    args.bootstrap_server,
                )
                dlq_consumer = _make_consumer(
                    KafkaConsumer,
                    config.KAFKA_TOPIC_DLQ,
                    f"eval-{args.test_id}-{rate_label}-dlq-{group_suffix}",
                    args.bootstrap_server,
                )
                events: dict[str, dict[str, Any]] = {}
                events_lock = threading.Lock()
                stop_event = threading.Event()
                # Output collection runs in a background thread so the send
                # loop can sustain high offered loads (1000 msg/s and above).
                collector = threading.Thread(
                    target=_collector_loop,
                    args=(
                        validated_consumer,
                        dlq_consumer,
                        events,
                        events_lock,
                        stop_event,
                        max(int(args.live_poll_interval_s * 1000), 10),
                    ),
                    daemon=True,
                )
                collector.start()
                send_futures: list[tuple[dict[str, Any], Any]] = []
                seq = 0
                invalid_seq = 0
                start = time.perf_counter()
                next_send = start
                deadline = start + args.duration_s
                benchmark_test_id = f"{args.test_id}-r{repetition}"

                while time.perf_counter() < deadline:
                    seq += 1
                    device_index = ((seq - 1) % device_count) + 1
                    sensor_type = _sensor_type_for_device(device_index, args.sensor_type)
                    valid = rng.random() >= args.invalid_ratio
                    invalid_variant = ""
                    if not valid:
                        invalid_seq += 1
                        invalid_variant = _invalid_variant_for_seq(invalid_seq, sensor_type)
                    sent_perf = time.perf_counter()
                    msg = _synthetic_message(
                        test_id=benchmark_test_id,
                        workload=workload,
                        rate=rate,
                        seq=seq,
                        device_index=device_index,
                        sensor_type=sensor_type,
                        valid=valid,
                        payload_size=args.payload_size,
                        invalid_variant=invalid_variant,
                    )
                    device_id = msg["deviceId"]
                    sent_at = msg.get("sent_at") or msg["ts"]
                    event_key = _event_key(str(device_id), str(sent_at))
                    expected_topic = (
                        config.KAFKA_TOPIC_TELEMETRY_VALIDATED
                        if valid
                        else config.KAFKA_TOPIC_DLQ
                    )
                    event = {
                        "test_id": args.test_id,
                        "repetition": repetition,
                        "workload": workload,
                        "offered_load_msg_s": rate,
                        "device_count": device_count,
                        "device_index": device_index,
                        "sensor_type": sensor_type,
                        "seq": seq,
                        "device_id": device_id,
                        "expected_topic": expected_topic,
                        "actual_topic": "",
                        "payload_valid": valid,
                        "invalid_variant": invalid_variant,
                        "sent_at": sent_at,
                        "sent_perf": sent_perf,
                        "produced_at": "",
                        "validated_seen_at": "",
                        "dlq_seen_at": "",
                        "output_seen_at": "",
                        "latency_ms": "",
                    }
                    with events_lock:
                        events[event_key] = event
                    future = producer.send(config.KAFKA_TOPIC_TELEMETRY_RAW, msg)
                    future.add_callback(
                        lambda _metadata, event=event: event.__setitem__(
                            "produced_at",
                            _now_iso(),
                        )
                    )
                    send_futures.append((event, future))

                    next_send += 1.0 / rate
                    if next_send > time.perf_counter():
                        _pace_until(next_send)

                producer.flush(timeout=10)
                for event, future in send_futures:
                    future.get(timeout=10)
                    if not event.get("produced_at"):
                        event["produced_at"] = _now_iso()
                produce_elapsed_s = max(time.perf_counter() - start, 0.000001)
                with events_lock:
                    backlog_end_of_production = sum(
                        1 for event in events.values() if not event.get("actual_topic")
                    )
                output_deadline = time.time() + args.cooldown_s
                while time.time() < output_deadline:
                    with events_lock:
                        pending = sum(
                            1 for event in events.values() if not event.get("actual_topic")
                        )
                    if pending == 0:
                        break
                    time.sleep(0.25)
                stop_event.set()
                collector.join(timeout=10)

                summaries.append(
                    _summarize_kafka_run(
                        args=args,
                        repetition=repetition,
                        workload=workload,
                        rate=rate,
                        device_count=device_count,
                        events=events,
                        produce_elapsed_s=produce_elapsed_s,
                        backlog_end_of_production=backlog_end_of_production,
                    )
                )
                all_events.extend(events.values())
                validated_consumer.close()
                dlq_consumer.close()
    finally:
        producer.close()

    # Benchmark event and summary files are written here for reproducible analysis.
    events_path = args.results_dir / "kafka_e2e_benchmark_events.csv"
    summary_path = args.results_dir / "kafka_e2e_benchmark_summary.json"
    summary_csv_path = args.results_dir / "benchmark_kafka_summary.csv"
    _write_csv(events_path, KAFKA_EVENT_COLUMNS, all_events)
    if summaries:
        _write_csv(summary_csv_path, list(summaries[0].keys()), summaries)
        print(f"Wrote {summary_csv_path}")
    summary_doc = {
        "generated_at": _now_iso(),
        "bootstrap_server": args.bootstrap_server,
        "raw_topic": config.KAFKA_TOPIC_TELEMETRY_RAW,
        "validated_topic": config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        "dlq_topic": config.KAFKA_TOPIC_DLQ,
        "repeat": args.repeat,
        "latency_threshold_ms": args.latency_threshold_ms,
        "max_unexpected_dlq_rate": args.max_unexpected_dlq_rate,
        "target_achieved_threshold": args.target_achieved_threshold,
        "cooldown_s": args.cooldown_s,
        "workloads": args.workload_specs,
        "sensor_type": args.sensor_type,
        "runs": summaries,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary_doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"Wrote {events_path}")
    print(f"Wrote {summary_path}")
    return events_path, summary_path

# CLI parameters
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the FAIR Bridge Kafka E2E performance benchmark."
    )
    parser.add_argument(
        "--workloads",
        default="small,medium,big",
        help="Comma-separated workload presets: small,medium,big.",
    )
    parser.add_argument(
        "--rate",
        default=None,
        help="Optional comma-separated offered load override. When set, --workloads is ignored.",
    )
    parser.add_argument(
        "--device-count",
        type=int,
        default=None,
        help="Device count for custom --rate workloads.",
    )
    parser.add_argument("--duration-s", type=int, default=60)
    parser.add_argument("--payload-size", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--sensor-type", choices=SENSOR_TYPE_CHOICES, default="climate")
    parser.add_argument("--invalid-ratio", type=float, default=0.2)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--test-id", default="perf-run-001")
    parser.add_argument("--bootstrap-server", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--cooldown-s", type=int, default=30)
    parser.add_argument("--latency-threshold-ms", type=float, default=1000.0)
    parser.add_argument("--max-unexpected-dlq-rate", type=float, default=0.01)
    parser.add_argument(
        "--target-achieved-threshold",
        type=float,
        default=DEFAULT_TARGET_ACHIEVED_THRESHOLD,
        help="Minimum produced throughput / offered load ratio required to count the run as pass.",
    )
    parser.add_argument(
        "--live-poll-interval-s",
        type=float,
        default=DEFAULT_LIVE_POLL_INTERVAL_S,
        help="Maximum interval between non-blocking output polls while generating load.",
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if not 0.0 <= args.invalid_ratio <= 1.0:
        raise SystemExit("--invalid-ratio must be between 0.0 and 1.0")
    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")
    if not 0.0 <= args.max_unexpected_dlq_rate <= 1.0:
        raise SystemExit("--max-unexpected-dlq-rate must be between 0.0 and 1.0")
    if not 0.0 <= args.target_achieved_threshold <= 1.0:
        raise SystemExit("--target-achieved-threshold must be between 0.0 and 1.0")
    if args.live_poll_interval_s < 0.0:
        raise SystemExit("--live-poll-interval-s must be non-negative")
    args.workload_specs = _workload_specs(args)
    args.results_dir.mkdir(parents=True, exist_ok=True)

    run_kafka_e2e(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
