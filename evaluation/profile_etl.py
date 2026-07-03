#!/usr/bin/env python3
"""Diagnostic microbenchmark for the ETL/validation hot path (T2 stage).

The capacity benchmark (throughput_capacity.py) shows T2 knee at ~2000 msg/s while
T1 (raw Kafka) stays linear -- i.e. the single-process, single-thread ETL loop is
CPU-bound on one core (GIL wall). This script LOCATES *where* that per-message CPU
goes, WITHOUT touching the ETL hot path or needing Kafka/Docker running.

It imports the real `etl.process_message` / `etl._normalise_ts` / `registry` and:
  1. builds N raw-message-shaped valid payloads identical to the load generator
     (`{"ts": <int ms>, "values": {...}}` in the value, deviceName in the headers),
     covering every sensor_type via evaluation.run_performance_benchmark helpers;
  2. times each sub-stage (deserialize / _normalise_ts / classify / canonicalize /
     validate / serialize) with time.perf_counter, plus the whole process_message,
     and reports us/msg, %-share, and the implied single-core msg/s ceiling;
  3. runs cProfile over the same batch and prints the Top-20 by cumulative time.

Run:  python evaluation/profile_etl.py [-n 200000] [--registry PATH] [--csv OUT.csv]
The single-core ceiling ~= 1e6 / (process_message us/msg); compare it to the
observed T2 plateau to confirm the bottleneck, and re-run after each optimization.
"""

from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import sys
import time
from pathlib import Path
from typing import Any

# Make the fair-bridge modules (config, registry, etl) and the bench helpers
# importable no matter where the script is launched from.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "fair-bridge"))
sys.path.insert(0, str(REPO_ROOT / "evaluation"))

import registry  # noqa: E402  (fair-bridge/registry.py)
import etl  # noqa: E402       (fair-bridge/etl.py)
import config  # noqa: E402    (fair-bridge/config.py)

DEFAULT_REGISTRY = REPO_ROOT / "fair-bridge" / "registry" / "field_registry.yaml"

# Payload builders copied verbatim from run_performance_benchmark.py (kept in sync).
# Inlined rather than imported to avoid that module's heavy import chain
# (telemetry_exporter -> ckanapi) which is irrelevant to this pure-CPU microbench.
SENSOR_TYPES = ["climate", "energy", "water", "air_quality", "ev_charger"]


def _sensor_type_for_device(device_index: int) -> str:
    return SENSOR_TYPES[(device_index - 1) % len(SENSOR_TYPES)]


def _benchmark_device_name(sensor_type: str, test_id: str, device_index: int) -> str:
    label = sensor_type.replace("_", "-")
    return f"{label}-bench-{test_id}-{device_index:04d}"


def _valid_values(sensor_type: str, seq: int) -> dict[str, Any]:
    if sensor_type == "climate":
        return {"temperature_c": round(20.0 + (seq % 100) / 10.0, 2),
                "humidity_pct": round(40.0 + (seq % 50) / 2.0, 2)}
    if sensor_type == "energy":
        return {"power_w": round(500.0 + (seq % 100) * 25.0, 3),
                "voltage_v": round(220.0 + (seq % 20), 3)}
    if sensor_type == "water":
        return {"flow_lpm": round(5.0 + (seq % 100) * 2.0, 3),
                "pressure_bar": round(1.0 + (seq % 20) / 2.0, 3)}
    if sensor_type == "air_quality":
        return {"pm2_5_ugm3": round(5.0 + (seq % 100) * 1.5, 3), "aqi": seq % 300}
    if sensor_type == "ev_charger":
        states = ["idle", "charging", "complete", "fault"]
        return {"state": states[seq % len(states)], "power_kw": round((seq % 50) * 2.5, 3)}
    raise ValueError(f"Unsupported sensor type: {sensor_type}")


def _build_messages(n: int, device_count: int) -> list[tuple[bytes, dict[str, str]]]:
    """Build N raw-message-shaped (value_bytes, headers) pairs identical in shape
    to what ThingsBoard forwards to tb.telemetry.raw for the load generator: the
    value is JSON `{"ts": <int ms>, "values": {...}}`, deviceName rides in headers.
    Cycles through device_count devices (round-robin over the 5 sensor types) so
    the classify cache sees the same repetition the real stream does."""
    base_ts = int(time.time() * 1000)
    messages: list[tuple[bytes, dict[str, str]]] = []
    for i in range(n):
        device_index = (i % device_count) + 1
        sensor_type = _sensor_type_for_device(device_index)
        name = _benchmark_device_name(sensor_type, "profile", device_index)
        values = _valid_values(sensor_type, i)
        value = json.dumps({"ts": base_ts + i, "values": values},
                           ensure_ascii=False).encode("utf-8")
        messages.append((value, {"deviceName": name}))
    return messages


def _stage_timings(messages: list[tuple[bytes, dict[str, str]]],
                   reg: registry.Registry) -> tuple[dict[str, float], int, int]:
    """Replay the exact per-message steps process_message performs, timing each
    sub-stage separately. Returns (totals_seconds_by_stage, validated, dlq).
    The sum of the composed stages reproduces process_message's routing so we can
    also assert correctness (validated count == len(messages) for pure-valid)."""
    totals = {k: 0.0 for k in
              ("deserialize", "normalise_ts", "classify", "canonicalize",
               "validate", "serialize", "process_message_total")}
    validated = 0
    dlq = 0
    pc = time.perf_counter

    for value_bytes, headers in messages:
        # --- deserialize (KafkaConsumer.value_deserializer) ---
        t0 = pc()
        msg_value = json.loads(value_bytes.decode("utf-8")) if value_bytes else {}
        t1 = pc(); totals["deserialize"] += t1 - t0

        # --- whole process_message (the real routed call) ---
        t0 = pc()
        target, payload = etl.process_message(msg_value, headers, reg=reg)
        t1 = pc(); totals["process_message_total"] += t1 - t0
        if target == config.KAFKA_TOPIC_TELEMETRY_VALIDATED:
            validated += 1
        else:
            dlq += 1

        # --- individual sub-stages (re-run on the same inputs to attribute cost) ---
        device_name = headers.get("deviceName", "")
        values = msg_value.get("values")
        raw_ts = msg_value.get("ts")

        t0 = pc(); etl._normalise_ts(raw_ts); t1 = pc()
        totals["normalise_ts"] += t1 - t0

        t0 = pc(); reg.classify(device_name); t1 = pc()
        totals["classify"] += t1 - t0

        t0 = pc(); canon, _renamed, _unknown = reg.canonicalize(values); t1 = pc()
        totals["canonicalize"] += t1 - t0

        sensor_type = reg.classify(device_name)
        t0 = pc(); reg.validate(canon, sensor_type); t1 = pc()
        totals["validate"] += t1 - t0

        t0 = pc()
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
        t1 = pc(); totals["serialize"] += t1 - t0

    return totals, validated, dlq


def _print_table(totals: dict[str, float], n: int) -> list[dict[str, Any]]:
    proc_us = totals["process_message_total"] / n * 1e6
    # The attributed sub-stages (exclude the composite total) for %-share.
    stage_order = ["deserialize", "normalise_ts", "classify", "canonicalize",
                   "validate", "serialize"]
    attributed = sum(totals[s] for s in stage_order)
    rows: list[dict[str, Any]] = []
    print(f"\n=== per-message sub-stage cost (N={n:,}) ===")
    print(f"{'stage':<22}{'us/msg':>12}{'%share':>10}")
    print("-" * 44)
    for stage in stage_order:
        us = totals[stage] / n * 1e6
        share = (totals[stage] / attributed * 100.0) if attributed else 0.0
        print(f"{stage:<22}{us:>12.3f}{share:>9.1f}%")
        rows.append({"stage": stage, "us_per_msg": round(us, 4),
                     "pct_share": round(share, 2)})
    print("-" * 44)
    print(f"{'process_message TOTAL':<22}{proc_us:>12.3f}")
    ceiling = 1e6 / proc_us if proc_us else float("inf")
    print(f"\nimplied single-core ceiling: {ceiling:,.0f} msg/s "
          f"(1e6 / {proc_us:.3f} us)")
    rows.append({"stage": "process_message_total", "us_per_msg": round(proc_us, 4),
                 "pct_share": 100.0})
    rows.append({"stage": "single_core_ceiling_msg_s", "us_per_msg": round(ceiling, 2),
                 "pct_share": None})
    return rows


def _run_cprofile(messages: list[tuple[bytes, dict[str, str]]],
                  reg: registry.Registry, top: int = 20) -> None:
    def workload() -> None:
        for value_bytes, headers in messages:
            msg_value = json.loads(value_bytes.decode("utf-8")) if value_bytes else {}
            target, payload = etl.process_message(msg_value, headers, reg=reg)
            json.dumps(payload, ensure_ascii=False).encode("utf-8")

    profiler = cProfile.Profile()
    profiler.enable()
    workload()
    profiler.disable()
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
    stats.print_stats(top)
    print(f"\n=== cProfile top {top} by cumulative time ===")
    print(stream.getvalue())


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL hot-path microbenchmark (T2).")
    parser.add_argument("-n", "--num", type=int, default=200_000,
                        help="Number of messages to process (default 200k).")
    parser.add_argument("--device-count", type=int, default=20,
                        help="Distinct devices to cycle (matches capacity default).")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY,
                        help="Path to field_registry.yaml (default: repo copy).")
    parser.add_argument("--csv", type=Path, default=None,
                        help="Optional path to write the sub-stage table as CSV.")
    parser.add_argument("--no-cprofile", action="store_true",
                        help="Skip the cProfile pass (timing table only).")
    parser.add_argument("--warmup", type=int, default=2000,
                        help="Warmup messages before timing (JIT-less, but warms "
                             "caches / imports; default 2000).")
    args = parser.parse_args()

    reg = registry.load_registry(str(args.registry))
    print(f"Registry ready: {len(reg.sensor_types)} sensor types "
          f"(validator: {type(next(iter(reg.validators.values()))).__name__ if reg.validators else 'n/a'}).")

    messages = _build_messages(args.num + args.warmup, args.device_count)
    warm, hot = messages[:args.warmup], messages[args.warmup:]

    if warm:
        _stage_timings(warm, reg)  # warm caches; discard timings

    wall0 = time.perf_counter()
    totals, validated, dlq = _stage_timings(hot, reg)
    wall = time.perf_counter() - wall0
    n = len(hot)

    print(f"\nprocessed {n:,} msgs in {wall:.3f}s wall "
          f"({n / wall:,.0f} msg/s incl. per-stage re-timing overhead)")
    print(f"routing: validated={validated:,}  dlq={dlq:,}  "
          f"(expect dlq=0 for pure-valid input)")
    if dlq:
        print("WARNING: some messages went to DLQ -- payloads are not all valid; "
              "the validate timing includes error paths.")

    rows = _print_table(totals, n)

    if args.csv:
        import csv as _csv
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", newline="") as fh:
            writer = _csv.DictWriter(fh, fieldnames=["stage", "us_per_msg", "pct_share"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nwrote sub-stage table to {args.csv}")

    if not args.no_cprofile:
        _run_cprofile(hot[: min(len(hot), 50_000)], reg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
