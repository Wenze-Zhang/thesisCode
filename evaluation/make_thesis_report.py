#!/usr/bin/env python3
"""Aggregate benchmark runs into the thesis summary table and figures.

Reads kafka_e2e_benchmark_summary.json produced by run_performance_benchmark.py,
aggregates the repetitions of each workload (mean +/- std) and writes:

- thesis/thesis_summary_table.csv / .md  (Table 1)
- thesis/latency_by_workload.png         (Figure 1: validated-path P50/P95 bars)
- thesis/throughput_by_workload.png      (Figure 2: ETL throughput vs offered load)
- thesis/dlq_latency_by_workload.png     (Figure 3: DLQ-path P50/P95 bars)
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"

# Key fallbacks keep the report compatible with result files written before
# the metric renaming (fast_path -> validation latency, etc.).
METRIC_KEYS = {
    "input_rate_msg_s": ["input_rate_msg_s", "produced_throughput_msg_s"],
    "etl_throughput_msg_s": ["etl_throughput_msg_s", "etl_output_throughput_msg_s"],
    "p50_ms": ["validation_latency_p50_ms", "p50_fast_path_latency_ms"],
    "p95_ms": ["validation_latency_p95_ms", "p95_fast_path_latency_ms"],
    "dlq_p50_ms": ["dlq_latency_p50_ms"],
    "dlq_p95_ms": ["dlq_latency_p95_ms"],
    "produced_count": ["produced_count"],
    "backlog_after_cooldown": [
        "backlog_after_cooldown",
        "observed_backlog_after_cooldown_count",
    ],
}

TABLE_COLUMNS = [
    "workload",
    "offered_load_msg_s",
    "repetitions",
    "produced_count_mean",
    "input_rate_msg_s_mean",
    "etl_throughput_msg_s_mean",
    "etl_throughput_msg_s_std",
    "p50_ms_mean",
    "p50_ms_std",
    "p95_ms_mean",
    "p95_ms_std",
    "dlq_p50_ms_mean",
    "dlq_p50_ms_std",
    "dlq_p95_ms_mean",
    "dlq_p95_ms_std",
    "backlog_after_cooldown_max",
]


def _float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric(row: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = _float(row, key)
        if value is not None:
            return value
    return None


def _read_runs(results_dir: Path) -> list[dict[str, Any]]:
    path = results_dir / "kafka_e2e_benchmark_summary.json"
    if not path.exists():
        raise SystemExit(f"Missing {path}; run the benchmark first.")
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    runs = payload.get("runs") or []
    if not runs:
        raise SystemExit(f"No runs found in {path}.")
    return runs


def _mean_std(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return round(mean, 3), round(std, 3)


def aggregate_workloads(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for run in runs:
        workload = str(run.get("workload") or "unknown")
        if workload not in groups:
            order.append(workload)
        groups.setdefault(workload, []).append(run)

    # Sort by offered load so small/medium/big come out in pressure order.
    order.sort(key=lambda w: _float(groups[w][0], "offered_load_msg_s") or 0.0)

    rows: list[dict[str, Any]] = []
    for workload in order:
        group = groups[workload]
        values = {
            name: [v for v in (_metric(run, keys) for run in group) if v is not None]
            for name, keys in METRIC_KEYS.items()
        }
        input_mean, _ = _mean_std(values["input_rate_msg_s"])
        tput_mean, tput_std = _mean_std(values["etl_throughput_msg_s"])
        p50_mean, p50_std = _mean_std(values["p50_ms"])
        p95_mean, p95_std = _mean_std(values["p95_ms"])
        dlq_p50_mean, dlq_p50_std = _mean_std(values["dlq_p50_ms"])
        dlq_p95_mean, dlq_p95_std = _mean_std(values["dlq_p95_ms"])
        produced_mean, _ = _mean_std(values["produced_count"])
        rows.append(
            {
                "workload": workload,
                "offered_load_msg_s": _float(group[0], "offered_load_msg_s"),
                "repetitions": len(group),
                "produced_count_mean": produced_mean,
                "input_rate_msg_s_mean": input_mean,
                "etl_throughput_msg_s_mean": tput_mean,
                "etl_throughput_msg_s_std": tput_std,
                "p50_ms_mean": p50_mean,
                "p50_ms_std": p50_std,
                "p95_ms_mean": p95_mean,
                "p95_ms_std": p95_std,
                "dlq_p50_ms_mean": dlq_p50_mean,
                "dlq_p50_ms_std": dlq_p50_std,
                "dlq_p95_ms_mean": dlq_p95_mean,
                "dlq_p95_ms_std": dlq_p95_std,
                "backlog_after_cooldown_max": (
                    max(values["backlog_after_cooldown"])
                    if values["backlog_after_cooldown"]
                    else None
                ),
            }
        )
    return rows


def _write_table_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=TABLE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any, suffix: str = "") -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}{suffix}" if isinstance(value, float) else f"{value}{suffix}"


def _write_table_md(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [
        "| Workload | Offered load | Produced count | Input rate | ETL throughput | P50 | P95 |",
        "| -------- | -----------: | -------------: | ---------: | -------------: | --: | --: |",
    ]
    for row in rows:
        lines.append(
            "| {workload} | {offered} | {produced} | {input_rate} | {tput} | {p50} | {p95} |".format(
                workload=row["workload"],
                offered=_fmt(row["offered_load_msg_s"], " msg/s"),
                produced=_fmt(row["produced_count_mean"]),
                input_rate=_fmt(row["input_rate_msg_s_mean"], " msg/s"),
                tput=_fmt(row["etl_throughput_msg_s_mean"], " msg/s"),
                p50=_fmt(row["p50_ms_mean"], " ms"),
                p95=_fmt(row["p95_ms_mean"], " ms"),
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _plot_latency(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    metric_prefix: str,
    ylabel: str,
    title: str,
) -> None:
    import matplotlib.pyplot as plt

    labels = [row["workload"] for row in rows]
    positions = range(len(labels))
    width = 0.35
    p50 = [row[f"{metric_prefix}p50_ms_mean"] or 0.0 for row in rows]
    p95 = [row[f"{metric_prefix}p95_ms_mean"] or 0.0 for row in rows]
    p50_err = [row[f"{metric_prefix}p50_ms_std"] or 0.0 for row in rows]
    p95_err = [row[f"{metric_prefix}p95_ms_std"] or 0.0 for row in rows]

    plt.figure(figsize=(7, 5))
    plt.bar(
        [p - width / 2 for p in positions], p50, width,
        yerr=p50_err, capsize=4, label="P50",
    )
    plt.bar(
        [p + width / 2 for p in positions], p95, width,
        yerr=p95_err, capsize=4, label="P95",
    )
    plt.xticks(list(positions), labels)
    plt.xlabel("Workload")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def _plot_throughput(path: Path, rows: list[dict[str, Any]]) -> None:
    import matplotlib.pyplot as plt

    labels = [row["workload"] for row in rows]
    positions = list(range(len(labels)))
    tput = [row["etl_throughput_msg_s_mean"] or 0.0 for row in rows]
    tput_err = [row["etl_throughput_msg_s_std"] or 0.0 for row in rows]
    offered = [row["offered_load_msg_s"] or 0.0 for row in rows]

    plt.figure(figsize=(7, 5))
    plt.bar(
        positions, tput, 0.5,
        yerr=tput_err, capsize=4, label="ETL throughput",
    )
    plt.scatter(
        positions, offered,
        marker="_", s=600, linewidths=2, color="black", label="Offered load",
    )
    plt.xticks(positions, labels)
    plt.xlabel("Workload")
    plt.ylabel("Throughput (msg/s)")
    plt.title("ETL throughput vs offered load by workload")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def run(results_dir: Path, output_dir: Path) -> None:
    runs = _read_runs(results_dir)
    rows = aggregate_workloads(runs)

    csv_path = output_dir / "thesis_summary_table.csv"
    md_path = output_dir / "thesis_summary_table.md"
    _write_table_csv(csv_path, rows)
    _write_table_md(md_path, rows)
    print(f"Wrote {csv_path}")
    print(f"Wrote {md_path}")

    try:
        import matplotlib

        matplotlib.use("Agg")
    except ImportError:
        print("matplotlib is not installed; skipping PNG figures.")
        return
    latency_path = output_dir / "latency_by_workload.png"
    throughput_path = output_dir / "throughput_by_workload.png"
    dlq_latency_path = output_dir / "dlq_latency_by_workload.png"
    _plot_latency(
        latency_path,
        rows,
        metric_prefix="",
        ylabel="Validation latency (ms)",
        title="Validation latency (raw → validated) by workload",
    )
    _plot_throughput(throughput_path, rows)
    _plot_latency(
        dlq_latency_path,
        rows,
        metric_prefix="dlq_",
        ylabel="DLQ latency (ms)",
        title="DLQ-path latency (raw → DLQ) by workload",
    )
    print(f"Wrote {latency_path}")
    print(f"Wrote {throughput_path}")
    print(f"Wrote {dlq_latency_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the thesis summary table and figures from benchmark results."
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Defaults to <results-dir>/thesis.",
    )
    args = parser.parse_args()
    output_dir = args.output_dir or (args.results_dir / "thesis")
    run(args.results_dir, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
