#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from ckanapi import RemoteCKAN
from ckanapi.errors import NotFound
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from kafka.structs import OffsetAndMetadata, TopicPartition

import config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
log = logging.getLogger("fair-bridge-telemetry-exporter")
logging.getLogger("kafka").setLevel(logging.WARNING)


BASE_COLUMNS = ["ts", "device_name", "device_id", "sensor_type", "quality"]
CANONICALIZED_COLUMN = "canonicalized"


# Keep this logic in sync with enricher.slugify(); CKAN dataset names must match.
def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if len(slug) < 2:
        slug = f"sensor-{slug or 'unnamed'}"
    return slug[:100]


def dataset_slug_for_device(device_name: str) -> str:
    return slugify(f"sensor-{device_name}")


def wait_for_ckan(url: str, timeout: int) -> None:
    deadline = time.time() + timeout
    log.info("Waiting for CKAN at %s ...", url)
    while time.time() < deadline:
        try:
            response = requests.get(f"{url}/api/action/status_show", timeout=3)
            if response.status_code == 200 and response.json().get("success"):
                log.info("CKAN ready.")
                return
        except Exception:
            pass
        time.sleep(4)
    raise RuntimeError(f"CKAN not ready after {timeout}s")


def wait_for_kafka(bootstrap: str, timeout: int) -> KafkaConsumer:
    deadline = time.time() + timeout
    log.info("Waiting for Kafka at %s ...", bootstrap)
    last_exc = None
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
                bootstrap_servers=bootstrap,
                group_id=config.KAFKA_CONSUMER_GROUP_EXPORTER,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else {},
            )
            log.info("Kafka ready; subscribed to %s", config.KAFKA_TOPIC_TELEMETRY_VALIDATED)
            return consumer
        except NoBrokersAvailable as exc:
            last_exc = exc
            time.sleep(4)
    raise RuntimeError(f"Kafka not ready after {timeout}s ({last_exc})")


def telemetry_date(ts_value: Any) -> str:
    if isinstance(ts_value, str):
        try:
            return datetime.fromisoformat(ts_value.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            if len(ts_value) >= 10:
                return ts_value[:10]
    return datetime.now(timezone.utc).date().isoformat()


# cell value for CSV
def _cell_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _canonicalized_value(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)

# row normalization 
def _normalise_row(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
    return {key: row.get(key, "") for key in fieldnames}


# csv header
def _fieldnames_for(
    existing_fieldnames: list[str],
    incoming_value_keys: list[str],
) -> list[str]:
    existing_dynamic = [
        key
        for key in existing_fieldnames
        if key not in BASE_COLUMNS and key != CANONICALIZED_COLUMN
    ]
    incoming_dynamic = [
        key
        for key in incoming_value_keys
        if key not in BASE_COLUMNS
        and key != CANONICALIZED_COLUMN
        and key not in existing_dynamic
    ]
    return BASE_COLUMNS + existing_dynamic + incoming_dynamic + [CANONICALIZED_COLUMN]


def _append_or_rewrite_csv(
    csv_path: Path,
    row: dict[str, Any],
    incoming_value_keys: list[str],
) -> None:
    existing_rows: list[dict[str, Any]] = []
    existing_fieldnames: list[str] = []

    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            existing_fieldnames = reader.fieldnames or []
            existing_rows = list(reader)

    fieldnames = _fieldnames_for(existing_fieldnames, incoming_value_keys)

    if csv_path.exists() and csv_path.stat().st_size > 0 and fieldnames == existing_fieldnames:
        with csv_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writerow(_normalise_row(row, fieldnames))
        return

    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for existing_row in existing_rows:
            writer.writerow(_normalise_row(existing_row, fieldnames))
        writer.writerow(_normalise_row(row, fieldnames))
    os.replace(tmp_path, csv_path)

# dynamic CSV header management
def append_telemetry_to_csv(payload: dict[str, Any], export_dir: Path) -> tuple[str, str, Path]:
    device_name = str(
        payload.get("device_name")
        or payload.get("deviceName")
        or payload.get("name")
        or "unknown-device"
    )
    device_id = str(payload.get("device_id") or payload.get("deviceId") or "")
    sensor_type = str(payload.get("sensor_type") or "other")
    ts_value = payload.get("ts") or payload.get("timestamp")
    day = telemetry_date(ts_value)
    dataset_slug = dataset_slug_for_device(device_name)
    csv_path = export_dir / dataset_slug / f"{day}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    values = payload.get("values")
    if not isinstance(values, dict):
        values = {}

    row: dict[str, Any] = {
        "ts": ts_value or "",
        "device_name": device_name,
        "device_id": device_id,
        "sensor_type": sensor_type,
        "quality": payload.get("quality") or "",
    }
    incoming_value_keys = [str(key) for key in values.keys()]
    for key, value in values.items():
        row[str(key)] = _cell_value(value)
    row[CANONICALIZED_COLUMN] = _canonicalized_value(payload.get(CANONICALIZED_COLUMN))

    _append_or_rewrite_csv(csv_path, row, incoming_value_keys)
    log.info("CSV append <- device=%s date=%s path=%s", device_name, day, csv_path)
    return dataset_slug, day, csv_path


def discover_pending_csvs(export_dir: Path) -> set[tuple[str, str, Path]]:
    pending: set[tuple[str, str, Path]] = set()
    if not export_dir.exists():
        return pending
    for csv_path in export_dir.glob("sensor-*/*.csv"):
        pending.add((csv_path.parent.name, csv_path.stem, csv_path))
    return pending


def _device_name_from_csv(csv_path: Path, dataset_slug: str) -> str:
    try:
        with csv_path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row.get("device_name"):
                    return row["device_name"]
    except Exception:
        log.exception("Failed to read device name from %s", csv_path)
    return dataset_slug.removeprefix("sensor-")


def _resource_name(device_name: str, day: str) -> str:
    return f"telemetry-{slugify(device_name)}-{day}.csv"


def _existing_resource(dataset: dict[str, Any], resource_name: str) -> dict[str, Any] | None:
    for resource in dataset.get("resources") or []:
        if resource.get("name") == resource_name:
            return resource
    return None


def upload_csv_to_ckan(
    ckan: RemoteCKAN,
    dataset_slug: str,
    day: str,
    csv_path: Path,
) -> bool:
    try:
        dataset = ckan.action.package_show(id=dataset_slug)
    except NotFound:
        log.info("CKAN dataset not ready: %s; keeping CSV pending", dataset_slug)
        return False
    except Exception:
        log.exception("CKAN package_show failed for %s; keeping CSV pending", dataset_slug)
        return False

    device_name = _device_name_from_csv(csv_path, dataset_slug)
    resource_name = _resource_name(device_name, day)
    existing = _existing_resource(dataset, resource_name)
    package_id = dataset.get("id") or dataset_slug

    try:
        with csv_path.open("rb") as upload:
            if existing:
                ckan.action.resource_patch(
                    id=existing["id"],
                    name=resource_name,
                    format="CSV",
                    mimetype="text/csv",
                    description=config.EXPORT_RESOURCE_DESCRIPTION,
                    upload=upload,
                )
                log.info("CKAN resource patched: %s under %s", resource_name, dataset_slug)
            else:
                ckan.action.resource_create(
                    package_id=package_id,
                    name=resource_name,
                    format="CSV",
                    mimetype="text/csv",
                    description=config.EXPORT_RESOURCE_DESCRIPTION,
                    upload=upload,
                )
                log.info("CKAN resource created: %s under %s", resource_name, dataset_slug)
        return True
    except Exception:
        log.exception("CKAN resource upload failed for %s; keeping CSV pending", csv_path)
        return False


def flush_dirty_csvs(
    ckan: RemoteCKAN,
    dirty: set[tuple[str, str, Path]],
) -> set[tuple[str, str, Path]]:
    remaining: set[tuple[str, str, Path]] = set()
    for dataset_slug, day, csv_path in sorted(
        dirty,
        key=lambda item: (item[0], item[1], str(item[2])),
    ):
        if not csv_path.exists():
            continue
        if not upload_csv_to_ckan(ckan, dataset_slug, day, csv_path):
            remaining.add((dataset_slug, day, csv_path))
    return remaining


def commit_message(consumer: KafkaConsumer, msg: Any) -> None:
    partition = TopicPartition(msg.topic, msg.partition)
    consumer.commit({partition: OffsetAndMetadata(msg.offset + 1, None)})


def main() -> int:
    log.info("Telemetry exporter starting...")
    log.info("Listening on topic %s", config.KAFKA_TOPIC_TELEMETRY_VALIDATED)
    log.info("Export directory: %s", config.EXPORT_DIR)

    if not config.CKAN_API_KEY:
        log.error(
            "CKAN_API_KEY is empty. Create a CKAN API token, set CKAN_API_KEY, "
            "and restart fair-bridge-telemetry-exporter."
        )
        while True:
            time.sleep(30)

    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)

    wait_for_ckan(config.CKAN_URL, config.CKAN_READY_TIMEOUT_S)
    ckan = RemoteCKAN(
        config.CKAN_URL,
        apikey=config.CKAN_API_KEY,
        user_agent="fair-bridge-telemetry-exporter/1.0",
    )
    consumer = wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_READY_TIMEOUT_S)

    dirty = discover_pending_csvs(export_dir)
    if dirty:
        log.info("Found %d pending CSV export(s) on disk.", len(dirty))

    last_flush = 0.0
    flush_interval = max(1, config.EXPORT_FLUSH_INTERVAL_S)

    while True:
        try:
            records = consumer.poll(timeout_ms=1000, max_records=100)
        except Exception:
            log.exception("Kafka poll failed.")
            time.sleep(2)
            continue

        for batch in records.values():
            for msg in batch:
                try:
                    dataset_slug, day, csv_path = append_telemetry_to_csv(msg.value or {}, export_dir)
                    dirty.add((dataset_slug, day, csv_path))
                except Exception:
                    log.exception("Failed to write CSV for Kafka offset %s; offset not committed", msg.offset)
                    continue

                try:
                    commit_message(consumer, msg)
                except Exception:
                    log.exception("Kafka commit failed after CSV append at offset %s", msg.offset)

        if time.time() - last_flush >= flush_interval:
            if dirty:
                dirty = flush_dirty_csvs(ckan, dirty)
            last_flush = time.time()


if __name__ == "__main__":
    sys.exit(main())
