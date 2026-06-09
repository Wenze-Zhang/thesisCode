#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_kafka_summary(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload.get("runs") or []


def _read_ckan_summary(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    rows = []
    for operation, summary in sorted((payload.get("operations") or {}).items()):
        row = {"operation": operation}
        row.update(summary)
        rows.append(row)
    return rows


def _float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _plot_outputs(results_dir: Path, local_rows: list[dict[str, Any]], kafka_rows: list[dict[str, Any]]) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed; skipping PNG plots.")
        return

    if local_rows or kafka_rows:
        plt.figure()
        if local_rows:
            x = [_float(row, "offered_load_msg_s") for row in local_rows]
            y = [_float(row, "throughput_msg_s") for row in local_rows]
            plt.plot(x, y, marker="o", label="local-etl")
        if kafka_rows:
            x = [_float(row, "offered_load_msg_s") for row in kafka_rows]
            y = [_float(row, "output_rate_msg_s") for row in kafka_rows]
            plt.plot(x, y, marker="o", label="kafka-e2e")
        plt.xlabel("Offered load (msg/s)")
        plt.ylabel("Observed throughput (msg/s)")
        plt.title("Throughput vs offered load")
        plt.legend()
        plt.tight_layout()
        plt.savefig(results_dir / "throughput_vs_offered_load.png")
        plt.close()

        plt.figure()
        if local_rows:
            x = [_float(row, "offered_load_msg_s") for row in local_rows]
            y = [_float(row, "p95_processing_latency_ms") for row in local_rows]
            plt.plot(x, y, marker="o", label="local-etl processing")
        if kafka_rows:
            x = [_float(row, "offered_load_msg_s") for row in kafka_rows]
            y = [_float(row, "p95_raw_to_validated_latency_ms") for row in kafka_rows]
            plt.plot(x, y, marker="o", label="kafka raw-to-validated")
        plt.xlabel("Offered load (msg/s)")
        plt.ylabel("p95 latency (ms)")
        plt.title("p95 latency vs offered load")
        plt.legend()
        plt.tight_layout()
        plt.savefig(results_dir / "p95_latency_vs_offered_load.png")
        plt.close()

        workloads = kafka_rows or local_rows
        labels = [str(row.get("offered_load_msg_s")) for row in workloads]
        validated = [int(float(row.get("validated_count") or 0)) for row in workloads]
        dlq = [int(float(row.get("dlq_count") or 0)) for row in workloads]
        plt.figure()
        positions = range(len(labels))
        plt.bar(positions, validated, label="validated")
        plt.bar(positions, dlq, bottom=validated, label="DLQ")
        plt.xticks(list(positions), labels)
        plt.xlabel("Offered load (msg/s)")
        plt.ylabel("Message count")
        plt.title("Validated vs DLQ counts by workload")
        plt.legend()
        plt.tight_layout()
        plt.savefig(results_dir / "validated_vs_dlq_counts.png")
        plt.close()

    ckan_rows = _read_csv(results_dir / "ckan_query_latency.csv")
    if ckan_rows:
        grouped: dict[str, list[float]] = {}
        for row in ckan_rows:
            if str(row.get("success")).lower() not in {"true", "1", "yes"}:
                continue
            latency = _float(row, "elapsed_ms")
            if latency is None:
                continue
            grouped.setdefault(row["operation"], []).append(latency)
        if grouped:
            plt.figure()
            labels = list(grouped.keys())
            plt.boxplot([grouped[label] for label in labels], labels=labels)
            plt.xticks(rotation=20, ha="right")
            plt.ylabel("Latency (ms)")
            plt.title("CKAN query latency distribution")
            plt.tight_layout()
            plt.savefig(results_dir / "ckan_query_latency_distribution.png")
            plt.close()


def run(args: argparse.Namespace) -> None:
    results_dir = args.results_dir
    local_rows = _read_csv(results_dir / "local_etl_benchmark.csv")
    kafka_rows = _read_kafka_summary(results_dir / "kafka_e2e_benchmark_summary.json")
    ckan_summary_rows = _read_ckan_summary(results_dir / "ckan_query_latency_summary.json")

    _write_csv(results_dir / "benchmark_local_summary.csv", local_rows)
    _write_csv(results_dir / "benchmark_kafka_summary.csv", kafka_rows)
    _write_csv(results_dir / "ckan_query_latency_summary_table.csv", ckan_summary_rows)

    if args.plots:
        _plot_outputs(results_dir, local_rows, kafka_rows)

    print(f"Read results from {results_dir}")
    if local_rows:
        print(f"Wrote {results_dir / 'benchmark_local_summary.csv'}")
    if kafka_rows:
        print(f"Wrote {results_dir / 'benchmark_kafka_summary.csv'}")
    if ckan_summary_rows:
        print(f"Wrote {results_dir / 'ckan_query_latency_summary_table.csv'}")
    if args.plots:
        print(f"PNG plots, when enough data exists, are saved in {results_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyse FAIR Bridge benchmark result files.")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--plots", action="store_true", help="Generate simple matplotlib PNG plots.")
    args = parser.parse_args()
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
