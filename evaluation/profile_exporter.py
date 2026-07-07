#!/usr/bin/env python3
"""Diagnostic microbenchmark for the telemetry-exporter hot path (T3/T4 stages).

The 2026-07-03 capacity sweep (results/thesis_capacity_6000) shows T3 (validated
-> local CSV) leaving the diagonal first past ~4000 msg/s input (T3=4610 vs
T2=5082 at input 5985), i.e. the EXPORTER is now the binding stage, not the ETL.
This script locates where the exporter's cost goes, mirroring profile_etl.py's
method (per-stage timing + cProfile, no change to the production code).

Part A (default; needs no Kafka/CKAN) -- consume side, per-message:
  imports the real telemetry_exporter.append_telemetry_to_csv and, against a
  scratch export dir, times each sub-stage of the per-row fast path
  (deserialize / parse+slug+route / mkdir / stat / csv append) plus the whole
  real function; reports us/msg, %-share and the implied single-core ceiling
  (compare to the observed T3 plateau ~4600 msg/s). It then times a GROUPED
  batch-write variant -- one file open per (file, poll batch) instead of one
  per row, byte-identical output -- to quantify the batch-write lever.
  NOT covered here (needs live Kafka): consumer.poll fetch cost and the
  synchronous consumer.commit round-trip the loop pays once per <=100-record
  poll; treat Part A as a LOWER bound on per-message consume-side cost.

Part B (--ckan; run inside the compose network) -- publish side, per-flush:
  times package_show + resource_create / resource_patch (whole-file upload)
  against a scratch '-bench-' dataset for increasing CSV row counts, giving the
  per-flush publish cost curve. The exporter re-uploads each dirty CSV WHOLE
  every flush, so this cost is O(accumulated file size) -- the T4 lever.
  The scratch dataset carries the '-bench-' marker so
  cleanup_benchmark_data.py --apply removes it even if this script crashes.

Run (host or container, part A):  python evaluation/profile_exporter.py
Run (container, parts A+B):       ... --ckan   (needs ckannet + CKAN_API_KEY)
"""

from __future__ import annotations

import argparse
import cProfile
import csv
import io
import json
import pstats
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# Make the fair-bridge modules importable no matter the launch cwd.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "fair-bridge"))
sys.path.insert(0, str(REPO_ROOT / "evaluation"))

import config  # noqa: E402              (fair-bridge/config.py)
import etl  # noqa: E402                 (fair-bridge/etl.py, for _normalise_ts)
import telemetry_exporter as texp  # noqa: E402  (fair-bridge/telemetry_exporter.py)

# Payload builders kept in sync with run_performance_benchmark.py (inlined, same
# as profile_etl.py, to avoid importing that module's heavy chain).
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


def _build_messages(n: int, device_count: int) -> list[bytes]:
    """Build N validated-message-shaped value bytes exactly as the exporter's
    KafkaConsumer receives them from tb.telemetry.validated: the JSON the ETL
    emits for a valid message (see etl.process_message), serialized. ts goes
    through the real etl._normalise_ts so the ISO format is byte-identical."""
    base_ts = int(time.time() * 1000)
    messages: list[bytes] = []
    for i in range(n):
        device_index = (i % device_count) + 1
        sensor_type = _sensor_type_for_device(device_index)
        name = _benchmark_device_name(sensor_type, "profile", device_index)
        payload = {
            "device_name": name,
            "device_id": "",
            "sensor_type": sensor_type,
            "ts": etl._normalise_ts(base_ts + i),
            "values": _valid_values(sensor_type, i),
            "quality": "validated",
        }
        messages.append(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    return messages


# --------------------------------------------------------------------------- #
# Part A: per-message consume-side cost.
# --------------------------------------------------------------------------- #
def _stage_timings(messages: list[bytes], workdir: Path) -> dict[str, float]:
    """Replay the exporter's per-message fast path, timing each sub-stage.

    Sub-stages replicate append_telemetry_to_csv's steady-state body (keep in
    sync with telemetry_exporter.py) against `workdir/substage`; the composite
    `append_total` is the REAL append_telemetry_to_csv into `workdir/per_row`,
    so the attribution can be checked against the true total.
    """
    totals = {key: 0.0 for key in
              ("deserialize", "parse_route", "mkdir", "stat_exists",
               "row_build", "csv_append", "append_total")}
    pc = time.perf_counter
    sub_dir = workdir / "substage"
    total_dir = workdir / "per_row"
    sub_dir.mkdir(parents=True, exist_ok=True)
    total_dir.mkdir(parents=True, exist_ok=True)
    real_header_cache: dict[Path, list[str]] = {}
    sub_headers: dict[Path, list[str]] = {}

    for value_bytes in messages:
        # --- deserialize (KafkaConsumer.value_deserializer) ---
        t0 = pc()
        payload = json.loads(value_bytes.decode("utf-8")) if value_bytes else {}
        t1 = pc(); totals["deserialize"] += t1 - t0

        # --- parse + slug + route (head of append_telemetry_to_csv) ---
        t0 = pc()
        device_name = str(payload.get("device_name")
                          or payload.get("deviceName")
                          or payload.get("name") or "unknown-device")
        device_id = str(payload.get("device_id") or payload.get("deviceId") or "")
        sensor_type = str(payload.get("sensor_type") or "other")
        ts_value = payload.get("ts") or payload.get("timestamp")
        day = texp.telemetry_date(ts_value)
        dataset_slug = texp.dataset_slug_for_device(device_name)
        csv_path = sub_dir / dataset_slug / f"{day}.csv"
        t1 = pc(); totals["parse_route"] += t1 - t0

        # --- mkdir (issued per message in the real code) ---
        t0 = pc()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        t1 = pc(); totals["mkdir"] += t1 - t0

        # --- row build (values -> CSV row dict) ---
        t0 = pc()
        values = payload.get("values")
        if not isinstance(values, dict):
            values = {}
        row: dict[str, Any] = {
            "ts": ts_value or "", "device_name": device_name,
            "device_id": device_id, "sensor_type": sensor_type,
            "quality": payload.get("quality") or "",
        }
        incoming_value_keys = [str(key) for key in values.keys()]
        for key, value in values.items():
            row[str(key)] = texp._cell_value(value)
        row[texp.CANONICALIZED_COLUMN] = texp._canonicalized_value(
            payload.get(texp.CANONICALIZED_COLUMN))
        t1 = pc(); totals["row_build"] += t1 - t0

        # --- stat/exists check (fast-path guard in _append_or_rewrite_csv) ---
        t0 = pc()
        _has_data = csv_path.exists() and csv_path.stat().st_size > 0
        t1 = pc(); totals["stat_exists"] += t1 - t0

        # --- csv append fast path: open + DictWriter + 1 row + close ---
        fieldnames = sub_headers.get(csv_path)
        new_file = fieldnames is None
        if new_file:
            fieldnames = texp._fieldnames_for([], incoming_value_keys)
            sub_headers[csv_path] = fieldnames
        t0 = pc()
        with csv_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            if new_file:
                writer.writeheader()
            writer.writerow(texp._normalise_row(row, fieldnames))
        t1 = pc(); totals["csv_append"] += t1 - t0

        # --- the whole REAL function (its own dir + header cache) ---
        t0 = pc()
        texp.append_telemetry_to_csv(payload, total_dir, real_header_cache)
        t1 = pc(); totals["append_total"] += t1 - t0

    return totals


def _batched_timings(messages: list[bytes], workdir: Path,
                     batch_size: int) -> float:
    """Grouped batch-write variant: within each poll-sized batch, group rows by
    csv file and write each file ONCE (one open, writerows). Output rows are
    identical to the per-row path; only the number of opens/mkdirs changes.
    Returns wall seconds for the whole run (includes deserialize + row build,
    so it is directly comparable to deserialize+...+append_total)."""
    out_dir = workdir / f"batched_{batch_size}"
    out_dir.mkdir(parents=True, exist_ok=True)
    headers: dict[Path, list[str]] = {}
    pc = time.perf_counter
    t_start = pc()
    for start in range(0, len(messages), batch_size):
        groups: dict[Path, list[dict[str, Any]]] = {}
        key_lists: dict[Path, list[str]] = {}
        for value_bytes in messages[start:start + batch_size]:
            payload = json.loads(value_bytes.decode("utf-8"))
            device_name = str(payload.get("device_name") or "unknown-device")
            ts_value = payload.get("ts") or ""
            day = texp.telemetry_date(ts_value)
            slug = texp.dataset_slug_for_device(device_name)
            csv_path = out_dir / slug / f"{day}.csv"
            values = payload.get("values")
            if not isinstance(values, dict):
                values = {}
            row: dict[str, Any] = {
                "ts": ts_value, "device_name": device_name,
                "device_id": str(payload.get("device_id") or ""),
                "sensor_type": str(payload.get("sensor_type") or "other"),
                "quality": payload.get("quality") or "",
            }
            for key, value in values.items():
                row[str(key)] = texp._cell_value(value)
            row[texp.CANONICALIZED_COLUMN] = texp._canonicalized_value(
                payload.get(texp.CANONICALIZED_COLUMN))
            groups.setdefault(csv_path, []).append(row)
            if csv_path not in key_lists:
                key_lists[csv_path] = [str(key) for key in values.keys()]
        for csv_path, rows in groups.items():
            fieldnames = headers.get(csv_path)
            new_file = fieldnames is None
            if new_file:
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                fieldnames = texp._fieldnames_for([], key_lists[csv_path])
                headers[csv_path] = fieldnames
            with csv_path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                if new_file:
                    writer.writeheader()
                writer.writerows(texp._normalise_row(row, fieldnames)
                                 for row in rows)
    return pc() - t_start


def _count_data_rows(root: Path) -> int:
    total = 0
    for path in root.glob("sensor-*/*.csv"):
        with path.open("r", encoding="utf-8") as fh:
            total += sum(1 for _ in fh) - 1  # minus header
    return total


def _print_table_a(totals: dict[str, float], n: int) -> list[dict[str, Any]]:
    total_us = (totals["deserialize"] + totals["append_total"]) / n * 1e6
    stage_order = ["deserialize", "parse_route", "mkdir", "row_build",
                   "stat_exists", "csv_append"]
    attributed = sum(totals[stage] for stage in stage_order)
    rows: list[dict[str, Any]] = []
    print(f"\n=== Part A: per-message consume-side cost (N={n:,}) ===")
    print(f"{'stage':<22}{'us/msg':>12}{'%share':>10}")
    print("-" * 44)
    for stage in stage_order:
        us = totals[stage] / n * 1e6
        share = (totals[stage] / attributed * 100.0) if attributed else 0.0
        print(f"{stage:<22}{us:>12.3f}{share:>9.1f}%")
        rows.append({"stage": stage, "us_per_msg": round(us, 4),
                     "pct_share": round(share, 2)})
    print("-" * 44)
    append_us = totals["append_total"] / n * 1e6
    print(f"{'append_telemetry REAL':<22}{append_us:>12.3f}")
    print(f"{'deserialize + REAL':<22}{total_us:>12.3f}")
    ceiling = 1e6 / total_us if total_us else float("inf")
    print(f"\nimplied single-core ceiling (excl. Kafka poll/commit): "
          f"{ceiling:,.0f} msg/s (1e6 / {total_us:.3f} us)")
    rows.append({"stage": "append_total_real", "us_per_msg": round(append_us, 4),
                 "pct_share": None})
    rows.append({"stage": "deserialize_plus_real", "us_per_msg": round(total_us, 4),
                 "pct_share": None})
    rows.append({"stage": "single_core_ceiling_msg_s",
                 "us_per_msg": round(ceiling, 2), "pct_share": None})
    return rows


def _run_cprofile(messages: list[bytes], workdir: Path, top: int = 20) -> None:
    prof_dir = workdir / "cprofile"
    prof_dir.mkdir(parents=True, exist_ok=True)
    header_cache: dict[Path, list[str]] = {}

    def workload() -> None:
        for value_bytes in messages:
            payload = json.loads(value_bytes.decode("utf-8"))
            texp.append_telemetry_to_csv(payload, prof_dir, header_cache)

    profiler = cProfile.Profile()
    profiler.enable()
    workload()
    profiler.disable()
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
    stats.print_stats(top)
    print(f"\n=== cProfile top {top} by cumulative time ===")
    print(stream.getvalue())


# --------------------------------------------------------------------------- #
# Part B: per-flush CKAN publish cost (--ckan).
# --------------------------------------------------------------------------- #
def _build_flush_csv(path: Path, rows: int, device_name: str) -> int:
    """Write an exporter-shaped CSV with `rows` data rows; returns bytes."""
    fieldnames = texp.BASE_COLUMNS + ["temperature_c", "humidity_pct",
                                      texp.CANONICALIZED_COLUMN]
    base_ts = int(time.time() * 1000)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(rows):
            values = _valid_values("climate", i)
            writer.writerow({
                "ts": etl._normalise_ts(base_ts + i), "device_name": device_name,
                "device_id": "", "sensor_type": "climate", "quality": "validated",
                "temperature_c": values["temperature_c"],
                "humidity_pct": values["humidity_pct"],
                texp.CANONICALIZED_COLUMN: "",
            })
    return path.stat().st_size


def _ckan_flush_bench(sizes: list[int], workdir: Path,
                      patch_reps: int) -> list[dict[str, Any]]:
    from ckanapi import RemoteCKAN

    ckan = RemoteCKAN(config.CKAN_URL, apikey=config.CKAN_API_KEY,
                      user_agent="fair-bridge-profile-exporter/1.0")
    device_name = f"climate-bench-profile-exporter-{int(time.time())}"
    slug = texp.dataset_slug_for_device(device_name)
    print(f"\n=== Part B: per-flush CKAN publish cost (dataset {slug}) ===")
    ckan.action.package_create(
        name=slug, title=f"profiler scratch {slug}",
        notes="Scratch dataset for profile_exporter.py; safe to purge.",
        owner_org=config.CKAN_ORG,
        tags=[{"name": "benchmark"}],
    )
    pc = time.perf_counter
    results: list[dict[str, Any]] = []
    try:
        print(f"{'rows':>8}{'KiB':>10}{'show_ms':>10}{'create_ms':>12}"
              f"{'patch_ms':>10}")
        print("-" * 50)
        for size in sizes:
            csv_path = workdir / "flush" / f"{size}.csv"
            nbytes = _build_flush_csv(csv_path, size, device_name)

            t0 = pc()
            dataset = ckan.action.package_show(id=slug)
            show_ms = (pc() - t0) * 1e3

            resource_name = f"telemetry-{slug}-{size}.csv"
            t0 = pc()
            with csv_path.open("rb") as upload:
                resource = ckan.action.resource_create(
                    package_id=dataset["id"], name=resource_name, format="CSV",
                    mimetype="text/csv", upload=upload)
            create_ms = (pc() - t0) * 1e3

            patch_total = 0.0
            for _ in range(patch_reps):
                t0 = pc()
                with csv_path.open("rb") as upload:
                    ckan.action.resource_patch(
                        id=resource["id"], name=resource_name, format="CSV",
                        mimetype="text/csv", upload=upload)
                patch_total += pc() - t0
            patch_ms = patch_total / patch_reps * 1e3

            print(f"{size:>8,}{nbytes / 1024.0:>10.1f}{show_ms:>10.1f}"
                  f"{create_ms:>12.1f}{patch_ms:>10.1f}")
            results.append({"rows": size, "bytes": nbytes,
                            "package_show_ms": round(show_ms, 2),
                            "resource_create_ms": round(create_ms, 2),
                            "resource_patch_ms": round(patch_ms, 2)})
    finally:
        try:
            ckan.action.package_delete(id=slug)
            ckan.action.dataset_purge(id=slug)
            print(f"[cleanup] purged scratch dataset {slug}")
        except Exception as exc:  # '-bench-' marker: operator cleanup catches it
            print(f"[cleanup] could not purge {slug} ({exc}); "
                  f"cleanup_benchmark_data.py --apply will remove it")

    if results:
        top = results[-1]
        per_file_s = (top["package_show_ms"] + top["resource_patch_ms"]) / 1e3
        workers = texp.UPLOAD_WORKERS
        for n_files in (24,):
            flush_s = -(-n_files // workers) * per_file_s  # ceil
            print(f"\nsteady-state estimate at {top['rows']:,}-row files: "
                  f"{n_files} dirty files / flush, {workers} workers -> "
                  f"flush wall ~= {flush_s:.1f}s "
                  f"(vs flush interval {config.EXPORT_FLUSH_INTERVAL_S}s)")
    return results


# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exporter hot-path microbenchmark (T3/T4).")
    parser.add_argument("-n", "--num", type=int, default=100_000,
                        help="Messages for Part A (default 100k).")
    parser.add_argument("--device-count", type=int, default=24,
                        help="Distinct devices to cycle (capacity default 24).")
    parser.add_argument("--warmup", type=int, default=2_000,
                        help="Warmup messages (creates files/headers) before timing.")
    parser.add_argument("--batch-sizes", default="100,1000",
                        help="Poll-batch sizes for the grouped-write variant.")
    parser.add_argument("--workdir", type=Path, default=None,
                        help="Scratch dir (default: a fresh temp dir, deleted "
                             "afterwards unless --keep).")
    parser.add_argument("--keep", action="store_true",
                        help="Keep the scratch dir for inspection.")
    parser.add_argument("--csv", type=Path, default=None,
                        help="Optional path to write the Part A table as CSV "
                             "(Part B table goes next to it as *_ckan.csv).")
    parser.add_argument("--no-cprofile", action="store_true")
    parser.add_argument("--ckan", action="store_true",
                        help="Also run Part B against live CKAN (compose network).")
    parser.add_argument("--ckan-sizes", default="1000,10000,50000",
                        help="CSV row counts for Part B uploads.")
    parser.add_argument("--ckan-patch-reps", type=int, default=3,
                        help="resource_patch repetitions per size (averaged).")
    args = parser.parse_args()

    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="exporter_profile_"))
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"scratch dir: {workdir}")

    messages = _build_messages(args.num + args.warmup, args.device_count)
    warm, hot = messages[:args.warmup], messages[args.warmup:]
    n = len(hot)

    try:
        if warm:
            _stage_timings(warm, workdir)  # create files/headers; discard timings

        wall0 = time.perf_counter()
        totals = _stage_timings(hot, workdir)
        wall = time.perf_counter() - wall0
        print(f"\nprocessed {n:,} msgs in {wall:.3f}s wall "
              f"({n / wall:,.0f} msg/s incl. per-stage re-timing overhead)")
        rows_a = _print_table_a(totals, n)

        # Grouped batch-write variant (fresh dirs; includes warmup msgs so row
        # counts match the per-row dirs for the correctness check).
        per_row_us = (totals["deserialize"] + totals["append_total"]) / n * 1e6
        print(f"\n=== grouped batch-write variant (one open per file per poll) ===")
        print(f"{'batch':>8}{'us/msg':>12}{'msg/s':>12}{'speedup':>10}")
        print("-" * 44)
        batch_rows: list[dict[str, Any]] = []
        for batch_size in [int(v) for v in args.batch_sizes.split(",") if v]:
            batched_wall = _batched_timings(messages, workdir, batch_size)
            us = batched_wall / len(messages) * 1e6
            speedup = per_row_us / us if us else float("inf")
            print(f"{batch_size:>8}{us:>12.3f}{1e6 / us:>12,.0f}{speedup:>9.2f}x")
            batch_rows.append({"stage": f"batched_write_{batch_size}",
                               "us_per_msg": round(us, 4),
                               "pct_share": None})
            expected = len(messages)
            got = _count_data_rows(workdir / f"batched_{batch_size}")
            if got != expected:
                print(f"WARNING: batched_{batch_size} wrote {got} rows, "
                      f"expected {expected}")

        per_row_written = _count_data_rows(workdir / "per_row")
        if per_row_written != len(messages):
            print(f"WARNING: per_row dir has {per_row_written} rows, "
                  f"expected {len(messages)}")

        if args.csv:
            args.csv.parent.mkdir(parents=True, exist_ok=True)
            with args.csv.open("w", newline="") as fh:
                writer = csv.DictWriter(
                    fh, fieldnames=["stage", "us_per_msg", "pct_share"])
                writer.writeheader()
                writer.writerows(rows_a + batch_rows)
            print(f"\nwrote Part A table to {args.csv}")

        if not args.no_cprofile:
            _run_cprofile(hot[: min(n, 50_000)], workdir)

        if args.ckan:
            sizes = [int(v) for v in args.ckan_sizes.split(",") if v]
            rows_b = _ckan_flush_bench(sizes, workdir, args.ckan_patch_reps)
            if args.csv and rows_b:
                ckan_csv = args.csv.with_name(args.csv.stem + "_ckan.csv")
                with ckan_csv.open("w", newline="") as fh:
                    writer = csv.DictWriter(fh, fieldnames=list(rows_b[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows_b)
                print(f"wrote Part B table to {ckan_csv}")
    finally:
        if not args.keep and args.workdir is None:
            shutil.rmtree(workdir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
