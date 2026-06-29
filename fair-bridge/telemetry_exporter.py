#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Parallel CKAN uploads per flush. Uploads run on a background thread (off the
# Kafka consume path) and fan out across this many workers.
UPLOAD_WORKERS = max(1, int(os.getenv("EXPORT_UPLOAD_WORKERS", "6")))

# When the consume loop catches up (an empty poll) and CSVs are still pending, the
# uploader flushes immediately instead of waiting out the full flush interval -- this
# is what collapses the benchmark's drain tail. Rate-limit those idle-triggered flushes
# to at most one per MIN_FLUSH_GAP_S so a bursty steady-state load can't trigger an
# excessive number of whole-file re-uploads.
MIN_FLUSH_GAP_S = max(0.0, float(os.getenv("EXPORT_MIN_FLUSH_GAP_S", "2")))

# One RemoteCKAN client per worker thread. RemoteCKAN wraps a `requests` session
# (not thread-safe to share), and construction is network-free, so a thread-local
# instance is both correct and cheap.
_thread_local = threading.local()


def _ckan_client() -> RemoteCKAN:
    client = getattr(_thread_local, "ckan", None)
    if client is None:
        client = RemoteCKAN(
            config.CKAN_URL,
            apikey=config.CKAN_API_KEY,
            user_agent="fair-bridge-telemetry-exporter/1.0",
        )
        _thread_local.ckan = client
    return client


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


def _read_header(csv_path: Path) -> list[str]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        try:
            return next(csv.reader(fh))
        except StopIteration:
            return []


def _append_or_rewrite_csv(
    csv_path: Path,
    row: dict[str, Any],
    incoming_value_keys: list[str],
    header_cache: dict[Path, list[str]],
) -> None:
    # Cache each file's header so the steady-state path never re-reads the file;
    # the exporter is the sole writer, so the cache stays consistent with disk.
    existing_fieldnames = header_cache.get(csv_path)
    if existing_fieldnames is None:
        existing_fieldnames = _read_header(csv_path)

    fieldnames = _fieldnames_for(existing_fieldnames, incoming_value_keys)
    file_has_data = csv_path.exists() and csv_path.stat().st_size > 0

    # Fast path: header unchanged -> append one row, no full-file read.
    if file_has_data and fieldnames == existing_fieldnames:
        with csv_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writerow(_normalise_row(row, fieldnames))
        header_cache[csv_path] = fieldnames
        return

    # New file or grown header: rewrite. Reading every existing row happens
    # only here, not on every message.
    existing_rows: list[dict[str, Any]] = []
    if file_has_data:
        with csv_path.open("r", newline="", encoding="utf-8") as fh:
            existing_rows = list(csv.DictReader(fh))

    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for existing_row in existing_rows:
            writer.writerow(_normalise_row(existing_row, fieldnames))
        writer.writerow(_normalise_row(row, fieldnames))
    os.replace(tmp_path, csv_path)
    header_cache[csv_path] = fieldnames

# dynamic CSV header management
def append_telemetry_to_csv(
    payload: dict[str, Any],
    export_dir: Path,
    header_cache: dict[Path, list[str]],
) -> tuple[str, str, Path]:
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

    _append_or_rewrite_csv(csv_path, row, incoming_value_keys, header_cache)
    log.debug("CSV append <- device=%s date=%s path=%s", device_name, day, csv_path)
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
    dataset_slug: str,
    day: str,
    csv_path: Path,
) -> bool:
    ckan = _ckan_client()
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
    dirty: set[tuple[str, str, Path]],
    max_workers: int = UPLOAD_WORKERS,
) -> set[tuple[str, str, Path]]:
    """Upload the given dirty CSVs to CKAN in parallel; return the ones that failed."""
    items = [item for item in dirty if item[2].exists()]
    if not items:
        return set()
    started = time.perf_counter()
    total_bytes = 0
    for _slug, _day, path in items:
        try:
            total_bytes += path.stat().st_size
        except OSError:
            pass
    remaining: set[tuple[str, str, Path]] = set()
    workers = max(1, min(max_workers, len(items)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_item = {
            pool.submit(upload_csv_to_ckan, slug, day, path): (slug, day, path)
            for slug, day, path in items
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                ok = future.result()
            except Exception:
                log.exception("Upload task crashed for %s", item)
                ok = False
            if not ok:
                remaining.add(item)
    # Per-flush timing: how long the whole flush took, how much was uploaded, and how
    # many files still failed. Used to verify the drain-tail fix (see the throughput
    # benchmark) -- under load this stays bounded; on drain it should be small and fast.
    log.info(
        "Flush done: %d file(s), %.1f KiB, %d worker(s) in %.2fs (%d still pending)",
        len(items), total_bytes / 1024.0, workers,
        time.perf_counter() - started, len(remaining),
    )
    return remaining


def uploader_loop(
    dirty: set[tuple[str, str, Path]],
    lock: threading.Lock,
    stop: threading.Event,
    flush_interval: float,
    flush_now: threading.Event,
) -> None:
    """Flush pending CSVs to CKAN, event-driven.

    Two triggers:
      * periodic   -- at most every `flush_interval` (staleness cap, keeps steady-state
                      whole-file re-uploads bounded under sustained load);
      * idle/drain -- `flush_now` set by the consume loop when it catches up, rate-limited
                      to one per MIN_FLUSH_GAP_S. This is what collapses the benchmark's
                      drain tail: once the backlog is gone the last rows go out in ~seconds
                      instead of waiting the full flush_interval.
    """
    last_flush = time.time()
    while not stop.is_set():
        since = time.time() - last_flush
        due_periodic = since >= flush_interval
        due_idle = flush_now.is_set() and since >= MIN_FLUSH_GAP_S
        if due_periodic or due_idle:
            flush_now.clear()
            with lock:
                claimed = set(dirty)
                dirty.clear()
            if claimed:
                remaining = flush_dirty_csvs(claimed)
                if remaining:
                    with lock:
                        dirty |= remaining
            last_flush = time.time()
            continue
        # Sleep until the next possible flush decision, waking early on a fresh signal.
        # Event.wait() returns immediately if already set, so ride out the min-gap window
        # with the (interruptible) stop event instead to avoid a busy loop.
        if flush_now.is_set():
            stop.wait(max(0.05, MIN_FLUSH_GAP_S - since))
        else:
            flush_now.wait(min(0.5, max(0.05, flush_interval - since)))


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
    consumer = wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_READY_TIMEOUT_S)

    # dirty is shared between the consume loop (producer) and the uploader
    dirty: set[tuple[str, str, Path]] = discover_pending_csvs(export_dir)
    if dirty:
        log.info("Found %d pending CSV export(s) on disk.", len(dirty))

    flush_interval = max(1, config.EXPORT_FLUSH_INTERVAL_S)
    header_cache: dict[Path, list[str]] = {}
    lock = threading.Lock()
    stop = threading.Event()
    # Set by the consume loop when it has caught up (empty poll) so the uploader flushes
    # pending CSVs immediately instead of waiting the full flush interval.
    flush_now = threading.Event()
    uploader = threading.Thread(
        target=uploader_loop,
        args=(dirty, lock, stop, flush_interval, flush_now),
        daemon=True,
    )
    uploader.start()
    log.info(
        "Uploader thread started (flush cap %ss, idle-flush gap %ss, %d workers).",
        flush_interval, MIN_FLUSH_GAP_S, UPLOAD_WORKERS,
    )

    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=1000, max_records=100)
            except Exception:
                log.exception("Kafka poll failed.")
                time.sleep(2)
                continue


            offsets_to_commit: dict[TopicPartition, OffsetAndMetadata] = {}
            for tp, batch in records.items():
                for msg in batch:
                    try:
                        dataset_slug, day, csv_path = append_telemetry_to_csv(
                            msg.value or {}, export_dir, header_cache
                        )
                        with lock:
                            dirty.add((dataset_slug, day, csv_path))
                    except Exception:
                        log.exception(
                            "Failed to write CSV for Kafka offset %s on %s; "
                            "not committing past it",
                            msg.offset,
                            tp,
                        )
                        break
                    offsets_to_commit[tp] = OffsetAndMetadata(msg.offset + 1, None)

            if offsets_to_commit:
                try:
                    consumer.commit(offsets_to_commit)
                except Exception:
                    log.exception(
                        "Kafka batch commit failed for %d partition(s)", len(offsets_to_commit)
                    )

            # Empty poll => consumer has caught up. If CSVs are still pending, ask the
            # uploader to flush now rather than wait out the flush interval; under load
            # polls are rarely empty, so steady-state flush cadence stays at flush_interval.
            if not records:
                with lock:
                    has_pending = bool(dirty)
                if has_pending:
                    flush_now.set()
    finally:
        stop.set()
        uploader.join(timeout=flush_interval + 30)


if __name__ == "__main__":
    sys.exit(main())
