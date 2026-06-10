#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import tempfile
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"

if str(FAIR_BRIDGE_DIR) not in sys.path:
    sys.path.insert(0, str(FAIR_BRIDGE_DIR))

RESULT_COLUMNS = [
    "test_id",
    "repetition",
    "payload_size",
    "requested_rows",
    "csv_write_count",
    "elapsed_s",
    "csv_write_throughput_rows_s",
    "p50_csv_write_latency_ms",
    "p95_csv_write_latency_ms",
    "max_csv_write_latency_ms",
    "output_csv_count",
    "export_dir",
]


def _install_optional_dependency_stubs() -> None:
    try:
        import ckanapi  # noqa: F401
    except ImportError:
        ckanapi_module = types.ModuleType("ckanapi")
        ckanapi_errors = types.ModuleType("ckanapi.errors")

        class RemoteCKAN:  # pragma: no cover - only used for importability
            pass

        class NotFound(Exception):
            pass

        ckanapi_module.RemoteCKAN = RemoteCKAN
        ckanapi_errors.NotFound = NotFound
        sys.modules.setdefault("ckanapi", ckanapi_module)
        sys.modules.setdefault("ckanapi.errors", ckanapi_errors)

    try:
        import kafka  # noqa: F401
    except ImportError:
        kafka_module = types.ModuleType("kafka")
        kafka_errors = types.ModuleType("kafka.errors")
        kafka_structs = types.ModuleType("kafka.structs")

        class KafkaConsumer:  # pragma: no cover - only used for importability
            pass

        class NoBrokersAvailable(Exception):
            pass

        class OffsetAndMetadata:
            def __init__(self, offset, metadata):
                self.offset = offset
                self.metadata = metadata

        class TopicPartition:
            def __init__(self, topic, partition):
                self.topic = topic
                self.partition = partition

        kafka_module.KafkaConsumer = KafkaConsumer
        kafka_errors.NoBrokersAvailable = NoBrokersAvailable
        kafka_structs.OffsetAndMetadata = OffsetAndMetadata
        kafka_structs.TopicPartition = TopicPartition
        sys.modules.setdefault("kafka", kafka_module)
        sys.modules.setdefault("kafka.errors", kafka_errors)
        sys.modules.setdefault("kafka.structs", kafka_structs)


_install_optional_dependency_stubs()

import telemetry_exporter  # noqa: E402


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


def _padding(payload_size: str) -> str:
    if payload_size == "small":
        return ""
    if payload_size == "medium":
        return "x" * 1024
    if payload_size == "large":
        return "x" * 10_240
    raise ValueError(f"Unsupported payload size: {payload_size}")


def _payload(*, test_id: str, seq: int, payload_size: str) -> dict[str, Any]:
    values: dict[str, Any] = {
        "temperature_c": round(20.0 + (seq % 100) / 10.0, 2),
        "humidity_pct": round(40.0 + (seq % 50) / 2.0, 2),
    }
    padding = _padding(payload_size)
    if padding:
        values["benchmark_padding"] = padding

    device_slot = seq % 10
    return {
        "ts": _now_iso(),
        "device_name": f"climate-exporter-{test_id}-{device_slot:02d}",
        "device_id": f"{test_id}-{device_slot:02d}",
        "sensor_type": "climate",
        "quality": "validated",
        "values": values,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def _run_once(
    *,
    args: argparse.Namespace,
    repetition: int,
    export_dir: Path,
) -> dict[str, Any]:
    latencies_ms: list[float] = []
    touched_csvs: set[Path] = set()
    start = time.perf_counter()

    for seq in range(1, args.rows + 1):
        payload = _payload(test_id=args.test_id, seq=seq, payload_size=args.payload_size)
        before = time.perf_counter_ns()
        _dataset_slug, _day, csv_path = telemetry_exporter.append_telemetry_to_csv(
            payload,
            export_dir,
        )
        after = time.perf_counter_ns()
        latencies_ms.append((after - before) / 1_000_000)
        touched_csvs.add(csv_path)

    elapsed_s = max(time.perf_counter() - start, 0.000001)
    return {
        "test_id": args.test_id,
        "repetition": repetition,
        "payload_size": args.payload_size,
        "requested_rows": args.rows,
        "csv_write_count": len(latencies_ms),
        "elapsed_s": round(elapsed_s, 6),
        "csv_write_throughput_rows_s": round(len(latencies_ms) / elapsed_s, 6),
        "p50_csv_write_latency_ms": _percentile(latencies_ms, 50),
        "p95_csv_write_latency_ms": _percentile(latencies_ms, 95),
        "max_csv_write_latency_ms": round(max(latencies_ms), 6) if latencies_ms else None,
        "output_csv_count": len(touched_csvs),
        "export_dir": str(export_dir),
    }


def _run_with_export_dir(args: argparse.Namespace, export_dir: Path) -> list[dict[str, Any]]:
    export_dir.mkdir(parents=True, exist_ok=True)
    return [
        _run_once(args=args, repetition=repetition, export_dir=export_dir)
        for repetition in range(1, args.repeat + 1)
    ]


def run(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.export_dir is None:
        with tempfile.TemporaryDirectory(prefix="fair-bridge-exporter-") as tmpdir:
            rows = _run_with_export_dir(args, Path(tmpdir))
    else:
        rows = _run_with_export_dir(args, args.export_dir)

    csv_path = args.results_dir / "exporter_csv_benchmark.csv"
    summary_path = args.results_dir / "exporter_csv_benchmark_summary.json"
    _write_csv(csv_path, rows)
    _write_json(
        summary_path,
        {
            "generated_at": _now_iso(),
            "description": (
                "Optional FAIR Bridge local CSV exporter benchmark; no CKAN API "
                "calls are performed."
            ),
            "runs": rows,
        },
    )
    print(f"Wrote {csv_path}")
    print(f"Wrote {summary_path}")
    return csv_path, summary_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run optional FAIR Bridge local CSV exporter benchmark."
    )
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--payload-size", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--test-id", default="exporter-csv-run-001")
    parser.add_argument("--export-dir", type=Path, default=None)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    args = parser.parse_args()

    if args.rows <= 0:
        raise SystemExit("--rows must be positive")
    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
