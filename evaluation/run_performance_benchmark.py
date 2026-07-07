#!/usr/bin/env python3
"""FAIR Bridge end-to-end pipeline latency benchmark.

Measures four stage latencies along the *real* pipeline
(sensor → ThingsBoard → Kafka → ETL → exporter → CKAN), each as P50 / P95:

    L1  Ingestion   t(raw receives)      - t(sensor sends)   [MQTT -> TB -> raw]
    L2  Validation  t(validated topic)   - t(raw topic)      [etl.py]
    L3  Export      t(row in local CSV)  - t(validated topic)[telemetry_exporter]
    L4  Publish     t(CKAN resource)     - t(local CSV)       [exporter flush]

L1-L4 are computed ONLY over validated-bound (non-DLQ) messages; DLQ messages are
still routed and counted (dlq_seen) but carry no reported latency.

Design notes (see docs/performance_evaluation.md and the plan file):
  * The four stages span four containers, so latency is measured with the shared
    host WALL clock (time.time(), UTC), not perf_counter. Reported values are an
    upper bound (they include observer poll/scan intervals).
  * The load generator drives the REAL MQTT -> ThingsBoard path so L1 is the true
    ingestion latency. A run is NEVER aborted/failed: if the offered load cannot be
    fully sustained, the shortfall is recorded as informational
    (generator_sustained / rate_limited_detail) and the real latency is still
    reported (a slow/overloaded run is a valid real-world measurement).
  * Between runs the benchmark drains the exporter (so each run is fully captured)
    and then clears its own local CSVs + CKAN resources, so no run starts on top of
    a previous run's backlog or accumulated CSV size. fair-bridge is never modified
    and the exporter's consumer group is never reset.
  * Measurement is OBSERVE-ONLY: etl.py / telemetry_exporter.py are not modified.
    The benchmark consumes raw/validated/DLQ with fresh consumer groups, tails the
    exporter's CSV volume, and polls CKAN. Pipeline behaviour is unaffected.
  * Correlation key = (device_name, ts_ms). The benchmark publishes in
    ThingsBoard's client-timestamp format {"ts": <ts_ms>, "values": {...}} so it
    controls a unique per-device ts that survives raw -> validated -> CSV. ts_ms
    doubles as the "sensor sent" instant. No field is added to `values` (that
    would route to DLQ via etl.py unknown-field handling).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import random
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
SIMULATOR_DIR = REPO_ROOT / "simulator"
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"
DEFAULT_TARGET_ACHIEVED_THRESHOLD = 0.95
# Device count is fixed so offered load is the only controlled variable.
DEVICE_COUNT = 100
# ThingsBoard MQTT telemetry topic (same as simulator.py).
TB_TELEMETRY_TOPIC = "v1/devices/me/telemetry"

for _path in (FAIR_BRIDGE_DIR, SIMULATOR_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import config  # noqa: E402  (fair-bridge/config.py)
import telemetry_exporter as exporter  # noqa: E402  (slug / resource naming helpers)
import tb_client  # noqa: E402  (simulator/tb_client.py: ensure_device, mqtt_client)


# Invalid variants that are well-formed MQTT telemetry objects yet fail the ETL
# downstream. missing_values / non_dict_values / unknown_sensor_type are dropped
# because ThingsBoard would reject (or cannot express) them at ingest.
INVALID_VARIANTS = [
    "out_of_range",
    "wrong_datatype",
    "unknown_field",
    "invalid_enum",
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

WORKLOAD_PRESETS = {
    "small": 100.0,
    "medium": 400.0,
    # 800 (not 1000) keeps the big tier comfortably under the single-host
    # generator ceiling (~950 msg/s). 1000+ is left for the multi-process
    # stress/capacity work.
    "big": 800.0,
}


# --------------------------------------------------------------------------- #
# Small helpers (several carried over from the previous benchmark).
# --------------------------------------------------------------------------- #
def _parse_workloads(value: str) -> list[dict[str, Any]]:
    # Each token is a preset name or a numeric offered load in msg/s.
    specs: list[dict[str, Any]] = []
    for token in value.split(","):
        token = token.strip().lower()
        if not token:
            continue
        if token in WORKLOAD_PRESETS:
            rate = WORKLOAD_PRESETS[token]
            name = token
        else:
            try:
                rate = float(token)
            except ValueError:
                choices = ", ".join(WORKLOAD_PRESETS)
                raise argparse.ArgumentTypeError(
                    f"Unsupported workload {token!r}; use a preset ({choices}) "
                    "or a numeric rate in msg/s"
                ) from None
            if rate <= 0:
                raise argparse.ArgumentTypeError("Numeric workloads must be positive")
            name = _rate_label(rate)
        specs.append({"workload": name, "offered_load_msg_s": rate})
    if not specs:
        raise argparse.ArgumentTypeError("At least one workload is required")
    return specs


def _rate_label(rate: float) -> str:
    return f"{rate:g}".replace(".", "p")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(ordered[int(rank)], 6)
    weight = rank - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 6)


def _stage_stats(deltas_s: list[float]) -> dict[str, Any]:
    # deltas are in seconds; report P50/P95 in ms plus a count.
    ms = [round(value * 1000.0, 6) for value in deltas_s]
    return {
        "p50_ms": _percentile(ms, 50),
        "p95_ms": _percentile(ms, 95),
        "count": len(ms),
    }


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _canonical_ts_ms(value: Any) -> int | None:
    """Normalise any stage's ts representation to integer epoch milliseconds.

    Accepts epoch-ms int/float, epoch-seconds, or ISO-8601 strings (the form the
    ETL writes onto validated/CSV via etl._normalise_ts). Round-trips ms exactly.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return int(round(number if number > 1e12 else number * 1000.0))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Pure number (epoch ms/s) encoded as string.
        try:
            number = float(text)
            return int(round(number if number > 1e12 else number * 1000.0))
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(round(dt.timestamp() * 1000.0))
    return None


def _day_for_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()


def _parse_ckan_dt(value: Any) -> float | None:
    # CKAN stores naive UTC ISO timestamps (e.g. resource last_modified/created).
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _pace_until(target_perf: float) -> None:
    # Open-loop pacing: sleep most of the wait, spin the last ~2 ms because
    # time.sleep granularity is too coarse at 1000 msg/s.
    while True:
        remaining_s = target_perf - time.perf_counter()
        if remaining_s <= 0:
            return
        if remaining_s > 0.002:
            time.sleep(remaining_s - 0.001)
        else:
            time.sleep(0)


# --------------------------------------------------------------------------- #
# Payload generation (valid + invalid value builders).
# --------------------------------------------------------------------------- #
def _sensor_type_for_device(device_index: int) -> str:
    return SENSOR_TYPES[(device_index - 1) % len(SENSOR_TYPES)]


def _benchmark_device_name(sensor_type: str, test_id: str, device_index: int) -> str:
    # Label must contain a registry name_alias so etl.classify() routes it.
    label = sensor_type.replace("_", "-")
    return f"{label}-bench-{test_id}-{device_index:04d}"


def _invalid_variant_for_seq(seq: int, sensor_type: str) -> str:
    variants = INVALID_VARIANTS
    if sensor_type not in ENUM_SENSOR_TYPES:
        variants = [variant for variant in INVALID_VARIANTS if variant != "invalid_enum"]
    return variants[(seq - 1) % len(variants)]


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
    return {
        "climate": {"temperature_c": 999.0},
        "energy": {"power_w": 100_001.0},
        "water": {"flow_lpm": 5_001.0},
        "air_quality": {"pm2_5_ugm3": 1_001.0},
        "ev_charger": {"power_kw": 351.0},
    }[sensor_type]


def _wrong_datatype_values(sensor_type: str) -> dict[str, Any]:
    return {
        "climate": {"temperature_c": "not-a-number"},
        "energy": {"power_w": "not-a-number"},
        "water": {"flow_lpm": "not-a-number"},
        "air_quality": {"aqi": "not-an-integer"},
        "ev_charger": {"power_kw": "not-a-number"},
    }[sensor_type]


def _invalid_enum_values(sensor_type: str) -> dict[str, Any]:
    return {
        "climate": {"hvac_state": "running"},
        "energy": {"phase": "4P"},
        "ev_charger": {"state": "paused"},
    }[sensor_type]


def _values_for(sensor_type: str, seq: int, valid: bool, variant: str) -> dict[str, Any]:
    if valid:
        return _valid_values(sensor_type, seq)
    if variant == "out_of_range":
        return _out_of_range_values(sensor_type)
    if variant == "wrong_datatype":
        return _wrong_datatype_values(sensor_type)
    if variant == "unknown_field":
        values = _valid_values(sensor_type, seq)
        values["unknown_benchmark_field"] = 1
        return values
    if variant == "invalid_enum":
        return _invalid_enum_values(sensor_type)
    raise ValueError(f"Unsupported invalid variant: {variant}")


# --------------------------------------------------------------------------- #
# Event record + shared state.
# --------------------------------------------------------------------------- #
class Event:
    __slots__ = (
        "device_name",
        "ts_ms",
        "dataset_slug",
        "day",
        "expected_validated",
        "t_send",
        "t_raw",
        "t_validated",
        "t_dlq",
        "t_csv",
        "t_ckan",
    )

    def __init__(self, device_name: str, ts_ms: int, dataset_slug: str, day: str,
                 expected_validated: bool, t_send: float):
        self.device_name = device_name
        self.ts_ms = ts_ms
        self.dataset_slug = dataset_slug
        self.day = day
        self.expected_validated = expected_validated
        self.t_send = t_send
        self.t_raw: float | None = None
        self.t_validated: float | None = None
        self.t_dlq: float | None = None
        self.t_csv: float | None = None
        self.t_ckan: float | None = None


# --------------------------------------------------------------------------- #
# Kafka observers (raw / validated / DLQ).
# --------------------------------------------------------------------------- #
def _import_kafka():
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise SystemExit(
            "kafka-python is required. Run inside the fair-bridge image / project "
            "environment."
        ) from exc
    return KafkaConsumer


def _make_consumer(KafkaConsumer, topic: str, group_id: str, bootstrap: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
    )
    deadline = time.time() + 10
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(timeout_ms=200)
    return consumer


def _exporter_lag(bootstrap: str, group: str, topic: str) -> int | None:
    """Total consumer-group lag for `group` on `topic` (None on error).

    Used to wait until the REAL exporter has fully processed a run before the
    next one starts — the exporter is never reset/modified, we just observe it.
    """
    from kafka import KafkaAdminClient, KafkaConsumer
    admin = None
    consumer = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        committed = admin.list_consumer_group_offsets(group)
        tps = [tp for tp in committed if tp.topic == topic]
        if not tps:
            return None
        consumer = KafkaConsumer(bootstrap_servers=bootstrap)
        end = consumer.end_offsets(tps)
        lag = 0
        for tp in tps:
            committed_offset = committed[tp].offset
            if committed_offset is None or committed_offset < 0:
                return None  # no commit yet -> treat as not drained
            lag += max(0, end.get(tp, committed_offset) - committed_offset)
        return lag
    except Exception:
        return None
    finally:
        if consumer is not None:
            consumer.close()
        if admin is not None:
            admin.close()


def _decode_headers(record) -> dict[str, str]:
    return {
        key.removeprefix("tb_msg_md_"): value.decode("utf-8", "replace")
        for key, value in (record.headers or [])
    }


def _name_and_ts(value: dict, headers: dict) -> tuple[str, int | None]:
    name = (
        value.get("device_name")
        or value.get("deviceName")
        or headers.get("deviceName")
        or value.get("name")
        or ""
    )
    raw_ts = (
        value.get("ts")
        or value.get("timestamp")
        or headers.get("ts")
    )
    return str(name), _canonical_ts_ms(raw_ts)


def _record_time(record, fallback: float) -> float:
    """Intrinsic Kafka log timestamp (epoch seconds) of a record: WHEN the message
    actually landed in its topic (set by the producer/broker), NOT when this
    (possibly backlogged) observer got around to polling it. Stamping stages with
    this instead of the observation wall-clock decouples a stage's makespan from
    the OBSERVER's own throughput -- the same reason t_ckan uses CKAN's
    last_modified rather than the poll-detection time. Without it, a slow in-process
    observer (4 threads sharing one GIL) drags max(t_stage) past the true completion
    at high load, deflating the makespan rate even though every message arrived and
    drained (a pure measurement artifact). Falls back to the observation time when
    the broker supplied no usable timestamp (kafka-python reports -1)."""
    ts = getattr(record, "timestamp", None)
    return ts / 1000.0 if ts is not None and ts > 0 else fallback


def _kafka_observer(
    consumer,
    topic: str,
    stage: str,
    events: dict[tuple[str, int], Event],
    lock: threading.Lock,
    stop_event: threading.Event,
    sample_box: dict[str, Any],
) -> None:
    """Stamp each raw / validated / dlq stage with the message's intrinsic Kafka
    log timestamp (see _record_time), so the stage throughput is observer-speed
    independent. `seen` is only the fallback when the broker omitted a timestamp."""
    while not stop_event.is_set():
        polled = consumer.poll(timeout_ms=200, max_records=2000)
        if not polled:
            continue
        seen = time.time()
        with lock:
            for batch in polled.values():
                for record in batch:
                    value = record.value or {}
                    headers = _decode_headers(record)
                    if stage == "raw" and "sample" not in sample_box:
                        # Captured once for the smoke/verify diagnostics.
                        sample_box["sample"] = {"value": value, "headers": headers}
                    name, ts_ms = _name_and_ts(value, headers)
                    if ts_ms is None:
                        continue
                    event = events.get((name, ts_ms))
                    if event is None:
                        continue
                    if stage == "raw":
                        if event.t_raw is None:
                            event.t_raw = _record_time(record, seen)
                    elif stage == "validated":
                        if event.t_validated is None:
                            event.t_validated = _record_time(record, seen)
                    elif stage == "dlq":
                        if event.t_dlq is None:
                            event.t_dlq = _record_time(record, seen)


# --------------------------------------------------------------------------- #
# CSV tailer (validated -> local CSV).
# --------------------------------------------------------------------------- #
class _CsvCursor:
    __slots__ = ("offset", "header", "size")

    def __init__(self):
        self.offset = 0      # byte position of next unread row
        self.header: list[str] | None = None
        self.size = 0


def _csv_tailer(
    export_dir: Path,
    events: dict[tuple[str, int], Event],
    lock: threading.Lock,
    stop_event: threading.Event,
    scan_interval_s: float,
) -> None:
    """Tail per-device daily CSVs and stamp t_csv with the file's flush mtime on
    first sighting of each row (see _drain_csv: intrinsic exporter-write time, not
    the tailer's scan time).

    Reads only newly appended bytes; on a full-file rewrite (size shrank, which
    the exporter does when the header changes) it re-reads from the header.
    """
    cursors: dict[Path, _CsvCursor] = {}
    while not stop_event.is_set():
        # Which (slug, day) files do we care about right now?
        with lock:
            wanted = {(e.dataset_slug, e.day) for e in events.values()}
        for slug, day in wanted:
            csv_path = export_dir / slug / f"{day}.csv"
            try:
                stat = csv_path.stat()
            except FileNotFoundError:
                continue
            cursor = cursors.setdefault(csv_path, _CsvCursor())
            if stat.st_size < cursor.size:        # rewritten -> restart
                cursor.offset = 0
                cursor.header = None
            cursor.size = stat.st_size
            try:
                _drain_csv(csv_path, cursor, events, lock, stat.st_mtime)
            except Exception:
                # Best-effort tailer: a transient partial read must not kill it.
                continue
        if stop_event.wait(scan_interval_s):
            break


def _drain_csv(
    csv_path: Path,
    cursor: _CsvCursor,
    events: dict[tuple[str, int], Event],
    lock: threading.Lock,
    mtime: float,
) -> None:
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        if cursor.header is None:
            header_line = fh.readline()
            if not header_line.endswith("\n"):
                return  # header still being written
            cursor.header = next(csv.reader([header_line]))
            cursor.offset = fh.tell()
        fh.seek(cursor.offset)
        chunk = fh.read()
    if not chunk:
        return
    # Only process complete lines; keep a trailing partial line for next time.
    last_newline = chunk.rfind("\n")
    if last_newline == -1:
        return
    complete = chunk[: last_newline + 1]
    cursor.offset += len(complete.encode("utf-8"))
    # Stamp t_csv with the CSV file's mtime (when the exporter FLUSHED these rows),
    # not this tailer's scan time -- decouples T3's makespan from the tailer's own
    # scan lateness. These are the freshly-appended rows, so the file's mtime is the
    # flush that wrote them; the makespan uses max(t_csv), i.e. the last flush, which
    # this captures exactly. (Mirrors t_ckan=last_modified and t_raw/t_validated=record ts.)
    reader = csv.DictReader(io.StringIO(complete), fieldnames=cursor.header)
    with lock:
        for row in reader:
            name = row.get("device_name") or ""
            ts_ms = _canonical_ts_ms(row.get("ts"))
            if ts_ms is None:
                continue
            event = events.get((name, ts_ms))
            if event is not None and event.t_csv is None:
                event.t_csv = mtime


# --------------------------------------------------------------------------- #
# CKAN poller (local CSV -> CKAN resource).
# --------------------------------------------------------------------------- #
def _ckan_package_show(ckan_url: str, api_key: str, slug: str, timeout: float = 10.0):
    import requests

    headers = {"Authorization": api_key} if api_key else {}
    try:
        response = requests.get(
            f"{ckan_url.rstrip('/')}/api/3/action/package_show",
            params={"id": slug},
            headers=headers,
            timeout=timeout,
        )
    except Exception:
        return None
    if response.status_code != 200:
        return None
    body = response.json()
    if not body.get("success"):
        return None
    return body.get("result")


def _ckan_poller(
    ckan_url: str,
    api_key: str,
    export_dir: Path,
    events: dict[tuple[str, int], Event],
    lock: threading.Lock,
    stop_event: threading.Event,
    poll_interval_s: float,
    batch_latencies_s: list[float],
) -> None:
    """Watch each dataset's daily resource last_modified.

    * Per-message: stamp t_ckan = that resource's last_modified once it first reaches a
      row's t_csv (so t_ckan is CKAN's real publish time, free of poll-detection latency).
    * Batch-level: when a resource's last_modified advances, record
      (last_modified - csv_mtime_from_previous_poll) as the CSV->CKAN batch lag.
    """
    # prev_lm[(slug, day)] = (last_modified_epoch, csv_mtime_epoch_at_that_poll)
    prev: dict[tuple[str, str], tuple[float | None, float | None]] = {}
    while not stop_event.is_set():
        with lock:
            # device_name is needed to reconstruct the exporter's resource name.
            resources = {
                (e.dataset_slug, e.day, e.device_name) for e in events.values()
            }
        # Fetch every dataset's package metadata CONCURRENTLY. Serial package_show made
        # one poll cycle take ~20 round-trips, which coarsened t_ckan; in parallel a
        # cycle is ~one round-trip, so detection keeps up with the ~2s drain flushes and
        # the sampled last_modified is the covering flush's (not a later one).
        slug_list = list({slug for slug, _, _ in resources})
        results: dict[str, Any] = {}
        if slug_list:
            with ThreadPoolExecutor(max_workers=min(len(slug_list), 16)) as pool:
                fetched = pool.map(
                    lambda s: _ckan_package_show(ckan_url, api_key, s), slug_list
                )
                results = dict(zip(slug_list, fetched))

        lm_by_resource: dict[tuple[str, str], float | None] = {}
        for slug in slug_list:
            result = results.get(slug)
            lm_by_name = {}
            if result:
                for resource in result.get("resources") or []:
                    if resource.get("name"):
                        lm_by_name[resource["name"]] = _parse_ckan_dt(
                            resource.get("last_modified") or resource.get("created")
                        )
            for s, day, device_name in resources:
                if s != slug:
                    continue
                lm = lm_by_name.get(exporter._resource_name(device_name, day))
                lm_by_resource[(slug, day)] = lm
                # Batch-level: resource last_modified advanced since last poll.
                key = (slug, day)
                prev_lm, prev_mtime = prev.get(key, (None, None))
                try:
                    csv_mtime = (export_dir / slug / f"{day}.csv").stat().st_mtime
                except FileNotFoundError:
                    csv_mtime = None
                if lm is not None and lm != prev_lm and prev_mtime is not None:
                    batch_latencies_s.append(max(0.0, lm - prev_mtime))
                prev[key] = (lm, csv_mtime)

        # Stamp t_ckan with the resource's REAL publish time (CKAN last_modified), NOT the
        # poll-detection time. last_modified is CKAN's own server timestamp of when the
        # flush carrying this row completed; using it removes the poller's sleep + round
        # trip from the measurement, so T4's makespan reflects the true CSV->CKAN publish
        # time. (CKAN and the benchmark share the Docker host clock, so skew is negligible.)
        with lock:
            for event in events.values():
                if event.t_csv is None or event.t_ckan is not None:
                    continue
                lm = lm_by_resource.get((event.dataset_slug, event.day))
                if lm is not None and lm >= event.t_csv:
                    event.t_ckan = lm
        if stop_event.wait(poll_interval_s):
            break


# --------------------------------------------------------------------------- #
# Cross-tier clearing (drop the previous run's local CSVs + CKAN resources).
# --------------------------------------------------------------------------- #
def _ckan_resource_delete(
    ckan_url: str, api_key: str, resource_id: str, timeout: float = 20.0
) -> bool:
    import requests

    headers = {"Authorization": api_key} if api_key else {}
    try:
        response = requests.post(
            f"{ckan_url.rstrip('/')}/api/3/action/resource_delete",
            json={"id": resource_id},
            headers=headers,
            timeout=timeout,
        )
    except Exception:
        return False
    return response.status_code in (200, 409)


def clear_run_data(
    devices: list[dict[str, Any]],
    export_dir: Path,
    ckan_url: str,
    api_key: str,
) -> None:


    slugs = sorted({device["dataset_slug"] for device in devices})
    removed_dirs = 0
    for slug in slugs:
        slug_dir = export_dir / slug
        if slug_dir.exists():
            try:
                shutil.rmtree(slug_dir)
                removed_dirs += 1
            except Exception:
                pass
    deleted_resources = 0
    for slug in slugs:
        result = _ckan_package_show(ckan_url, api_key, slug)
        if not result:
            continue
        for resource in result.get("resources") or []:
            rid = resource.get("id")
            if rid and _ckan_resource_delete(ckan_url, api_key, rid):
                deleted_resources += 1
    print(
        f"[clear] reset {removed_dirs} export dir(s) and "
        f"{deleted_resources} CKAN resource(s) for {len(slugs)} dataset(s)."
    )



# Device provisioning (ThingsBoard) + MQTT connections.

def provision_devices(test_id: str, device_count: int) -> list[dict[str, Any]]:

    devices: list[dict[str, Any]] = []
    print(f"[provision] ensuring {device_count} ThingsBoard devices ...")
    for index in range(1, device_count + 1):
        sensor_type = _sensor_type_for_device(index)
        name = _benchmark_device_name(sensor_type, test_id, index)
        try:
            device_id, token = tb_client.ensure_device(name, label=name)
            client = tb_client.mqtt_client(token)
        except Exception as exc:
            raise SystemExit(
                f"[provision] failed to provision/connect device {name!r}: {exc}\n"
                "Is ThingsBoard reachable (TB_HOST / TB_MQTT_HOST)?"
            ) from exc
        devices.append(
            {
                "index": index,
                "name": name,
                "sensor_type": sensor_type,
                "device_id": device_id,
                "client": client,
                "dataset_slug": exporter.dataset_slug_for_device(name),
                "last_ts_ms": 0,
            }
        )
    print(f"[provision] {len(devices)} devices ready.")
    return devices


def wait_for_datasets(
    devices: list[dict[str, Any]],
    ckan_url: str,
    api_key: str,
    timeout_s: float,
) -> bool:
    """Block until every device's CKAN dataset exists (enricher caught up)."""
    slugs = {device["dataset_slug"] for device in devices}
    deadline = time.time() + timeout_s
    print(f"[ckan] waiting for {len(slugs)} datasets (timeout {timeout_s:g}s) ...")
    pending = set(slugs)
    while pending and time.time() < deadline:
        for slug in list(pending):
            if _ckan_package_show(ckan_url, api_key, slug) is not None:
                pending.discard(slug)
        if pending:
            time.sleep(2)
    if pending:
        print(
            f"[ckan] WARNING: {len(pending)} datasets still missing; "
            "L4 (publish) will be N/A for those devices."
        )
        return False
    print("[ckan] all datasets present.")
    return True


# --------------------------------------------------------------------------- #
# One workload run.
# --------------------------------------------------------------------------- #
def run_workload(
    args: argparse.Namespace,
    repetition: int,
    workload: str,
    rate: float,
    devices: list[dict[str, Any]],
    KafkaConsumer,
) -> dict[str, Any]:
    # Cross-tier clearing: start every run on empty local CSVs + CKAN resources so
    # no run sits on top of a previous run's accumulated file size / backlog.
    if args.clear_between_runs:
        clear_run_data(
            devices, Path(args.export_dir), args.ckan_url, args.ckan_api_key
        )

    events: dict[tuple[str, int], Event] = {}
    lock = threading.Lock()
    stop_event = threading.Event()
    sample_box: dict[str, Any] = {}
    batch_latencies_s: list[float] = []

    group_suffix = f"{workload}-{repetition}-{time.time_ns()}"
    raw_consumer = _make_consumer(
        KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_RAW,
        f"eval-{args.test_id}-raw-{group_suffix}", args.bootstrap_server,
    )
    validated_consumer = _make_consumer(
        KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        f"eval-{args.test_id}-validated-{group_suffix}", args.bootstrap_server,
    )
    dlq_consumer = _make_consumer(
        KafkaConsumer, config.KAFKA_TOPIC_DLQ,
        f"eval-{args.test_id}-dlq-{group_suffix}", args.bootstrap_server,
    )

    export_dir = Path(args.export_dir)
    threads = [
        threading.Thread(target=_kafka_observer, args=(
            raw_consumer, config.KAFKA_TOPIC_TELEMETRY_RAW, "raw",
            events, lock, stop_event, sample_box), daemon=True),
        threading.Thread(target=_kafka_observer, args=(
            validated_consumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED, "validated",
            events, lock, stop_event, sample_box), daemon=True),
        threading.Thread(target=_kafka_observer, args=(
            dlq_consumer, config.KAFKA_TOPIC_DLQ, "dlq",
            events, lock, stop_event, sample_box), daemon=True),
        threading.Thread(target=_csv_tailer, args=(
            export_dir, events, lock, stop_event, args.csv_scan_interval_s),
            daemon=True),
        threading.Thread(target=_ckan_poller, args=(
            args.ckan_url, args.ckan_api_key, export_dir, events, lock, stop_event,
            args.ckan_poll_interval_s, batch_latencies_s), daemon=True),
    ]
    for thread in threads:
        thread.start()

    rng = random.Random(args.seed + repetition)
    seq = 0
    invalid_seq = 0
    produced = 0
    failed_sends = 0
    start = time.perf_counter()
    next_send = start
    deadline = start + args.duration_s
    last_guard = start
    rate_limited_reason = ""

    try:
        while time.perf_counter() < deadline:
            seq += 1
            device = devices[(seq - 1) % len(devices)]
            sensor_type = device["sensor_type"]
            valid = rng.random() >= args.invalid_ratio
            variant = ""
            if not valid:
                invalid_seq += 1
                variant = _invalid_variant_for_seq(invalid_seq, sensor_type)
            values = _values_for(sensor_type, seq, valid, variant)

            send_wall = time.time()
            ts_ms = max(int(send_wall * 1000.0), device["last_ts_ms"] + 1)
            device["last_ts_ms"] = ts_ms
            day = _day_for_ts_ms(ts_ms)
            key = (device["name"], ts_ms)
            with lock:
                events[key] = Event(
                    device_name=device["name"],
                    ts_ms=ts_ms,
                    dataset_slug=device["dataset_slug"],
                    day=day,
                    expected_validated=valid,
                    t_send=send_wall,
                )

            payload = json.dumps({"ts": ts_ms, "values": values}, ensure_ascii=False)
            info = device["client"].publish(TB_TELEMETRY_TOPIC, payload)
            if info.rc != 0:
                failed_sends += 1
            else:
                produced += 1

            # Open-loop pacing.
            next_send += 1.0 / rate
            if next_send > time.perf_counter():
                _pace_until(next_send)

            # Live rate-limit guard (client side): publishing can't keep up.
            now_perf = time.perf_counter()
            if now_perf - last_guard >= 1.0:
                last_guard = now_perf
                elapsed = now_perf - start
                if elapsed > args.warmup_s:
                    achieved = produced / max(elapsed, 1e-6)
                    if achieved < args.target_achieved_threshold * rate:
                        rate_limited_reason = (
                            f"client_send_rate {achieved:.1f} < "
                            f"{args.target_achieved_threshold:g}x{rate:g} msg/s"
                        )
                        break
    finally:
        produce_elapsed_s = max(time.perf_counter() - start, 1e-6)

    # Post-send phase. Observers keep running so this run's L3/L4 are fully
    # captured AND the next run never starts on top of this run's backlog:
    #   1) minimum settle (--cooldown-s),
    #   2) wait until the exporter has consumed everything (validated lag -> 0,
    #      capped by --max-drain-s) — the exporter is observed, never reset,
    #   3) a publish grace (--publish-grace-s) so the final CKAN flush of the
    #      tail rows is captured for L4.
    settle_until = time.time() + args.cooldown_s
    drain_until = time.time() + args.max_drain_s
    lag: int | None = None
    while time.time() < drain_until:
        if args.drain_between_runs:
            lag = _exporter_lag(
                args.bootstrap_server,
                config.KAFKA_CONSUMER_GROUP_EXPORTER,
                config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            )
            consumed = lag is not None and lag <= args.drain_lag_threshold
        else:
            consumed = True
        if time.time() >= settle_until and consumed:
            break
        time.sleep(args.drain_poll_s)
    else:
        print(f"[drain] max-drain-s reached with exporter lag={lag}; continuing.")

    grace_until = time.time() + args.publish_grace_s
    while time.time() < grace_until:
        with lock:
            pending_ckan = sum(
                1 for e in events.values()
                if e.t_csv is not None and e.t_ckan is None
            )
        if pending_ckan == 0:
            break
        time.sleep(1.0)
    print(f"[drain] run settled (exporter lag={lag}).")
    stop_event.set()
    for thread in threads:
        thread.join(timeout=10)
    raw_consumer.close()
    validated_consumer.close()
    dlq_consumer.close()

    summary = _summarize(
        args=args, repetition=repetition, workload=workload, rate=rate,
        events=events, produced=produced, failed_sends=failed_sends,
        produce_elapsed_s=produce_elapsed_s, batch_latencies_s=batch_latencies_s,
        sample_box=sample_box,
    )

    # Post-cooldown ingest check (server side): did the generator/pipeline deliver
    # the offered load? Informational only — a slow/overloaded run is still a valid
    # real-world measurement and is never marked failed or aborted.
    if not rate_limited_reason:
        raw_ratio = summary["raw_seen"] / produced if produced else 0.0
        if raw_ratio < args.target_achieved_threshold:
            rate_limited_reason = (
                f"raw_arrival_ratio {raw_ratio:.3f} < "
                f"{args.target_achieved_threshold:g} "
                f"(produced={produced}, raw_seen={summary['raw_seen']})"
            )

    if rate_limited_reason:
        summary["generator_sustained"] = False
        summary["rate_limited_detail"] = rate_limited_reason

    return summary


def _summarize(
    *,
    args: argparse.Namespace,
    repetition: int,
    workload: str,
    rate: float,
    events: dict[tuple[str, int], Event],
    produced: int,
    failed_sends: int,
    produce_elapsed_s: float,
    batch_latencies_s: list[float],
    sample_box: dict[str, Any],
) -> dict[str, Any]:
    values = list(events.values())
    raw_seen = sum(1 for e in values if e.t_raw is not None)
    validated_seen = sum(1 for e in values if e.t_validated is not None)
    dlq_seen = sum(1 for e in values if e.t_dlq is not None)
    csv_seen = sum(1 for e in values if e.t_csv is not None)
    ckan_seen = sum(1 for e in values if e.t_ckan is not None)

    # L1-L4 are computed ONLY over validated-bound (non-DLQ) messages. DLQ-bound
    # messages are routed and counted (dlq_seen) but carry no reported latency.
    # L2/L3/L4 are already DLQ-free (DLQ messages never reach validated/CSV/CKAN);
    # L1 must additionally exclude the invalid messages that reach raw.
    l1 = [
        e.t_raw - e.t_send
        for e in values
        if e.t_raw is not None and e.expected_validated
    ]
    l2 = [
        e.t_validated - e.t_raw
        for e in values
        if e.t_validated is not None and e.t_raw is not None
    ]
    l3 = [
        e.t_csv - e.t_validated
        for e in values
        if e.t_csv is not None and e.t_validated is not None
    ]
    l4 = [
        e.t_ckan - e.t_csv
        for e in values
        if e.t_ckan is not None and e.t_csv is not None
    ]

    l1_stats = _stage_stats(l1)
    l2_stats = _stage_stats(l2)
    l3_stats = _stage_stats(l3)
    l4_stats = _stage_stats(l4)
    l4_batch_stats = _stage_stats(batch_latencies_s)

    input_rate = round(produced / produce_elapsed_s, 6)
    target_ratio = round(input_rate / rate, 6) if rate > 0 else None
    sample = sample_box.get("sample")

    return {
        # Identity
        "test_id": args.test_id,
        "repetition": repetition,
        "workload": workload,
        "offered_load_msg_s": rate,
        "device_count": len(set(e.device_name for e in values)) or args.device_count,
        "duration_s": args.duration_s,
        "invalid_ratio": args.invalid_ratio,
        "seed": args.seed,
        # Headline latencies (P50 / P95, ms)
        "l1_ingestion_p50_ms": l1_stats["p50_ms"],
        "l1_ingestion_p95_ms": l1_stats["p95_ms"],
        "l2_validation_p50_ms": l2_stats["p50_ms"],
        "l2_validation_p95_ms": l2_stats["p95_ms"],
        "l3_export_p50_ms": l3_stats["p50_ms"],
        "l3_export_p95_ms": l3_stats["p95_ms"],
        "l4_publish_p50_ms": l4_stats["p50_ms"],
        "l4_publish_p95_ms": l4_stats["p95_ms"],
        "l4_publish_batch_p50_ms": l4_batch_stats["p50_ms"],
        "l4_publish_batch_p95_ms": l4_batch_stats["p95_ms"],
        # Sanity / guard
        "input_rate_msg_s": input_rate,
        "target_achieved_ratio": target_ratio,
        "target_achieved_threshold": args.target_achieved_threshold,
        "produced_count": produced,
        "failed_sends": failed_sends,
        "raw_seen": raw_seen,
        "validated_seen": validated_seen,
        "dlq_seen": dlq_seen,
        "csv_seen": csv_seen,
        "ckan_seen": ckan_seen,
        "l1_count": l1_stats["count"],
        "l2_count": l2_stats["count"],
        "l3_count": l3_stats["count"],
        "l4_count": l4_stats["count"],
        "l4_batch_count": l4_batch_stats["count"],
        "backlog_ingest": produced - raw_seen,
        "backlog_etl": raw_seen - (validated_seen + dlq_seen),
        "backlog_export": validated_seen - csv_seen,
        "backlog_publish": csv_seen - ckan_seen,
        "produce_elapsed_s": round(produce_elapsed_s, 6),
        "cooldown_s": args.cooldown_s,
        "raw_sample": json.dumps(sample, ensure_ascii=False) if sample else "",
        "generator_sustained": True,
        "rate_limited_detail": "",
        "run_status": "completed",
    }


# --------------------------------------------------------------------------- #
# Verify / smoke: confirm ThingsBoard forwards to tb.telemetry.raw.
# --------------------------------------------------------------------------- #
def verify_forwarding(args: argparse.Namespace, devices: list[dict[str, Any]],
                      KafkaConsumer) -> None:
    print("[verify] checking ThingsBoard -> tb.telemetry.raw forwarding ...")
    consumer = _make_consumer(
        KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_RAW,
        f"eval-{args.test_id}-verify-{time.time_ns()}", args.bootstrap_server,
    )
    try:
        device = devices[0]
        ts_ms = int(time.time() * 1000.0)
        payload = json.dumps(
            {"ts": ts_ms, "values": _valid_values(device["sensor_type"], 1)}
        )
        device["client"].publish(TB_TELEMETRY_TOPIC, payload)
        deadline = time.time() + 15
        sample = None
        matched_ts = False
        while time.time() < deadline:
            polled = consumer.poll(timeout_ms=500, max_records=200)
            for batch in polled.values():
                for record in batch:
                    value = record.value or {}
                    headers = _decode_headers(record)
                    if sample is None:
                        sample = {"value": value, "headers": headers}
                    name, seen_ts = _name_and_ts(value, headers)
                    if name == device["name"] and seen_ts == ts_ms:
                        matched_ts = True
            if sample is not None:
                break
    finally:
        consumer.close()

    if sample is None:
        raise SystemExit(
            "[verify] FAILED: nothing arrived on tb.telemetry.raw within 15s.\n"
            "ThingsBoard is not forwarding MQTT telemetry to the raw topic — "
            "check the TB rule chain (Kafka node -> tb.telemetry.raw)."
        )
    print("[verify] sample raw record:")
    print(json.dumps(sample, indent=2, ensure_ascii=False))
    if matched_ts:
        print("[verify] OK: client ts survived to raw; (device_name, ts_ms) "
              "correlation is valid.")
    else:
        print("[verify] WARNING: could not match the client ts on raw. Correlation "
              "by (device_name, ts_ms) may not hold — inspect the sample above "
              "before trusting the latencies.")


# --------------------------------------------------------------------------- #
# Output.
# --------------------------------------------------------------------------- #
def write_outputs(args: argparse.Namespace, summaries: list[dict[str, Any]]) -> None:
    if not summaries:
        return
    csv_path = args.results_dir / "pipeline_latency_summary.csv"
    json_path = args.results_dir / "pipeline_latency_summary.json"
    _write_csv(csv_path, list(summaries[0].keys()), summaries)
    print(f"Wrote {csv_path}")
    doc = {
        "generated_at": _now_iso(),
        "bootstrap_server": args.bootstrap_server,
        "raw_topic": config.KAFKA_TOPIC_TELEMETRY_RAW,
        "validated_topic": config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        "dlq_topic": config.KAFKA_TOPIC_DLQ,
        "ckan_url": args.ckan_url,
        "export_dir": args.export_dir,
        "device_count": args.device_count,
        "duration_s": args.duration_s,
        "invalid_ratio": args.invalid_ratio,
        "repeat": args.repeat,
        "cooldown_s": args.cooldown_s,
        "clear_between_runs": args.clear_between_runs,
        "drain_between_runs": args.drain_between_runs,
        "target_achieved_threshold": args.target_achieved_threshold,
        "csv_scan_interval_s": args.csv_scan_interval_s,
        "ckan_poll_interval_s": args.ckan_poll_interval_s,
        "latency_definitions": {
            "scope": "L1-L4 are computed only over validated-bound (non-DLQ) "
                     "messages; DLQ messages are routed/counted (dlq_seen) but "
                     "carry no reported latency.",
            "l1_ingestion": "t(raw) - t(sensor send)  [MQTT -> ThingsBoard -> raw]",
            "l2_validation": "t(validated) - t(raw)  [etl.py]",
            "l3_export": "t(local CSV row) - t(validated)  [telemetry_exporter]",
            "l4_publish": "t(CKAN resource) - t(local CSV); per-message + batch",
        },
        "runs": summaries,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"Wrote {json_path}")


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        description="FAIR Bridge end-to-end pipeline latency benchmark (L1-L4)."
    )
    parser.add_argument(
        "--workloads", default="small,medium,big",
        help="Comma-separated presets (small,medium,big) and/or numeric msg/s.",
    )
    parser.add_argument("--duration-s", type=int, default=60)
    parser.add_argument("--invalid-ratio", type=float, default=0.2)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--device-count", type=int, default=DEVICE_COUNT)
    parser.add_argument("--test-id", default="perf-run-001")
    parser.add_argument("--bootstrap-server", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--ckan-url", default=config.CKAN_URL)
    parser.add_argument("--ckan-api-key", default=config.CKAN_API_KEY)
    parser.add_argument("--export-dir", default=config.EXPORT_DIR)
    parser.add_argument("--cooldown-s", type=int, default=30,
                        help="Minimum settle after send before a run may finish.")
    parser.add_argument(
        "--drain-between-runs", action=argparse.BooleanOptionalAction, default=True,
        help="Wait until the exporter's validated lag returns to ~0 before the "
             "next run, so no run starts on top of the previous run's backlog. "
             "The exporter is observed only (never reset/modified).",
    )
    parser.add_argument(
        "--clear-between-runs", action=argparse.BooleanOptionalAction, default=True,
        help="Before each run, delete the bench devices' local CSVs and CKAN "
             "resources (datasets/devices are kept) so every run starts on empty "
             "files and no run's L4 is inflated by a previous run's accumulated CSV "
             "size. fair-bridge is never modified.",
    )
    parser.add_argument("--max-drain-s", type=float, default=900.0,
                        help="Cap on the per-run exporter-drain wait.")
    parser.add_argument("--publish-grace-s", type=float, default=45.0,
                        help="Extra wait after drain so the final CKAN flush "
                             "(tail of L4) is captured.")
    parser.add_argument("--drain-poll-s", type=float, default=4.0)
    parser.add_argument("--drain-lag-threshold", type=int, default=0)
    parser.add_argument("--warmup-s", type=float, default=5.0)
    parser.add_argument("--csv-scan-interval-s", type=float, default=0.5)
    parser.add_argument("--ckan-poll-interval-s", type=float, default=3.0)
    parser.add_argument("--dataset-wait-s", type=float, default=120.0)
    parser.add_argument(
        "--target-achieved-threshold", type=float,
        default=DEFAULT_TARGET_ACHIEVED_THRESHOLD,
        help="Min achieved/offered ratio (client send + raw arrival) to not be "
             "flagged rate_limited.",
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Provision devices, confirm TB->raw forwarding, print a sample, exit.",
    )
    args = parser.parse_args()

    if not 0.0 <= args.invalid_ratio <= 1.0:
        raise SystemExit("--invalid-ratio must be between 0.0 and 1.0")
    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")
    if not 0.0 <= args.target_achieved_threshold <= 1.0:
        raise SystemExit("--target-achieved-threshold must be between 0.0 and 1.0")
    args.workload_specs = _parse_workloads(args.workloads)
    args.results_dir.mkdir(parents=True, exist_ok=True)

    KafkaConsumer = _import_kafka()
    devices = provision_devices(args.test_id, args.device_count)

    try:
        # Always verify forwarding before timing anything.
        verify_forwarding(args, devices, KafkaConsumer)
        if args.verify_only:
            return 0

        wait_for_datasets(devices, args.ckan_url, args.ckan_api_key, args.dataset_wait_s)

        summaries: list[dict[str, Any]] = []
        any_unsustained = False
        for repetition in range(1, args.repeat + 1):
            for spec in args.workload_specs:
                workload = spec["workload"]
                rate = float(spec["offered_load_msg_s"])
                print(f"\n=== run: workload={workload} rate={rate:g} msg/s "
                      f"rep={repetition}/{args.repeat} ===")
                summary = run_workload(
                    args, repetition, workload, rate, devices, KafkaConsumer
                )
                summaries.append(summary)
                if not summary.get("generator_sustained", True):
                    any_unsustained = True
                    print(
                        f"[note] generator did not fully sustain {rate:g} msg/s: "
                        f"{summary['rate_limited_detail']} — reported as a real "
                        "measurement, not a failure."
                    )
                print(
                    f"[done] L1 p50/p95={summary['l1_ingestion_p50_ms']}/"
                    f"{summary['l1_ingestion_p95_ms']} ms  "
                    f"L2={summary['l2_validation_p50_ms']}/"
                    f"{summary['l2_validation_p95_ms']}  "
                    f"L3={summary['l3_export_p50_ms']}/"
                    f"{summary['l3_export_p95_ms']}  "
                    f"L4={summary['l4_publish_p50_ms']}/"
                    f"{summary['l4_publish_p95_ms']} "
                    f"(batch {summary['l4_publish_batch_p50_ms']}/"
                    f"{summary['l4_publish_batch_p95_ms']})  "
                    f"target_ratio={summary['target_achieved_ratio']}"
                )

        write_outputs(args, summaries)
        if any_unsustained:
            print("\n[note] one or more runs did not fully sustain the offered load "
                  "(generator_sustained=False); their latencies are still reported "
                  "as real measurements.")
        return 0
    finally:
        for device in devices:
            try:
                device["client"].loop_stop()
                device["client"].disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
