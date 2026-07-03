#!/usr/bin/env python3
"""Plot the FAIR Bridge pipeline latency benchmark (L1-L4, P50/P95).

Reads the summary written by run_performance_benchmark.py
(evaluation/results/pipeline_latency_summary.json) and renders the four stage
latencies as a 2x2 grid of grouped P50/P95 bar charts, one panel per stage:

    L1 Ingestion   (ms)   MQTT -> ThingsBoard -> raw
    L2 Validation  (ms)   raw -> etl -> validated
    L3 Export      (s)    validated -> local CSV
    L4 Publish     (s)    local CSV -> CKAN resource

Workloads are ordered small -> medium -> big and labelled with their offered
load. When the summary contains several repetitions of a workload the reported
P50/P95 are averaged across those repetitions (mean of the per-run percentiles).
"""

from __future__ import annotations

import argparse
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY = REPO_ROOT / "evaluation" / "results" / "pipeline_latency_summary.json"

# Fixed workload order + offered-load annotation used on the x axis.
WORKLOAD_ORDER = ["small", "medium", "big"]

# (panel title, p50 field, p95 field, y-axis unit, seconds?) -- L1/L2 in ms,
# L3/L4 converted to seconds because their magnitudes are 10-90 s.
PANELS = [
    ("L1 Ingestion", "l1_ingestion_p50_ms", "l1_ingestion_p95_ms", "Latency (ms)", False),
    ("L2 Validation", "l2_validation_p50_ms", "l2_validation_p95_ms", "Latency (ms)", False),
    ("L3 Export", "l3_export_p50_ms", "l3_export_p95_ms", "Latency (s)", True),
    ("L4 Publish", "l4_publish_p50_ms", "l4_publish_p95_ms", "Latency (s)", True),
]

P50_COLOR = "#1f77b4"
P95_COLOR = "#ff7f0e"


def _mean(values: list[float]) -> float | None:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _aggregate(runs: list[dict[str, Any]]) -> "OrderedDict[str, dict[str, Any]]":
    """Group runs by workload (keeping small/medium/big order) and average each
    P50/P95 field across the workload's repetitions."""
    by_workload: dict[str, list[dict[str, Any]]] = {}
    for run in runs:
        by_workload.setdefault(str(run.get("workload")), []).append(run)

    ordered_names = [w for w in WORKLOAD_ORDER if w in by_workload]
    ordered_names += [w for w in by_workload if w not in WORKLOAD_ORDER]

    aggregated: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    for name in ordered_names:
        group = by_workload[name]
        row: dict[str, Any] = {
            "offered_load_msg_s": group[0].get("offered_load_msg_s"),
            "reps": len(group),
        }
        for _title, p50_field, p95_field, _unit, _sec in PANELS:
            row[p50_field] = _mean([g.get(p50_field) for g in group])
            row[p95_field] = _mean([g.get(p95_field) for g in group])
        aggregated[name] = row
    return aggregated


def _xlabel(name: str, offered: Any) -> str:
    if offered is None:
        return name
    return f"{name}\n({offered:g} msg/s)"


def _annotate(ax, bars, values, seconds: bool) -> None:
    for bar, value in zip(bars, values):
        if value is None:
            continue
        shown = value / 1000.0 if seconds else value
        # Sub-second panels (L3) round two distinct values (e.g. 0.26 and 0.33) to
        # the same "0.3" at one decimal, so use two decimals when |value| < 10.
        label = f"{shown:.2f}" if seconds and abs(shown) < 10 else f"{shown:.1f}"
        ax.annotate(
            label,
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 3), textcoords="offset points",
            ha="center", va="bottom", fontsize=8,
        )


def _draw_panel(ax, title, p50_field, p95_field, unit, seconds, aggregated) -> None:
    names = list(aggregated.keys())
    x = range(len(names))
    width = 0.38

    def scale(raw):
        return [None if v is None else (v / 1000.0 if seconds else v) for v in raw]

    p50_raw = [aggregated[n][p50_field] for n in names]
    p95_raw = [aggregated[n][p95_field] for n in names]
    p50 = scale(p50_raw)
    p95 = scale(p95_raw)

    bars50 = ax.bar([i - width / 2 for i in x], [v or 0 for v in p50],
                    width, label="P50", color=P50_COLOR)
    bars95 = ax.bar([i + width / 2 for i in x], [v or 0 for v in p95],
                    width, label="P95", color=P95_COLOR)

    # Dashed guide lines at each bar height (matches the reference figure).
    for v in p50:
        if v:
            ax.axhline(v, color=P50_COLOR, linestyle="--", linewidth=0.6, alpha=0.5)
    for v in p95:
        if v:
            ax.axhline(v, color=P95_COLOR, linestyle="--", linewidth=0.6, alpha=0.5)

    _annotate(ax, bars50, p50_raw, seconds)
    _annotate(ax, bars95, p95_raw, seconds)

    ax.set_title(title)
    ax.set_xticks(list(x))
    ax.set_xticklabels([_xlabel(n, aggregated[n]["offered_load_msg_s"]) for n in names])
    ax.set_xlabel("Workload")
    ax.set_ylabel(unit)
    ax.legend(loc="upper left", fontsize=8)
    ax.margins(y=0.18)


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot pipeline latency (L1-L4, P50/P95).")
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY,
                        help="pipeline_latency_summary.json from the latency benchmark.")
    parser.add_argument("--out", type=Path, default=None,
                        help="Output PNG (default: alongside the summary).")
    args = parser.parse_args()

    doc = json.loads(args.summary.read_text(encoding="utf-8"))
    runs = doc.get("runs", doc if isinstance(doc, list) else [])
    if not runs:
        raise SystemExit(f"No runs found in {args.summary}")
    aggregated = _aggregate(runs)

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    fig.suptitle("Latency (P50/P95)", fontsize=14, y=0.98)
    for ax, (title, p50_field, p95_field, unit, seconds) in zip(axes.flat, PANELS):
        _draw_panel(ax, title, p50_field, p95_field, unit, seconds, aggregated)

    reps = {aggregated[n]["reps"] for n in aggregated}
    note = f"averaged over {max(reps)} repetition(s)" if max(reps) > 1 else "single repetition"
    fig.text(0.99, 0.01, note, ha="right", va="bottom", fontsize=8, color="gray")

    fig.tight_layout(rect=(0, 0.02, 1, 0.96))
    out = args.out or args.summary.with_name("pipeline_latency.png")
    fig.savefig(out, dpi=130)
    print(f"Wrote {out}")

    # Console recap.
    print("\nworkload   offered   L1p50/p95(ms)   L2p50/p95(ms)   L3p50/p95(s)   L4p50/p95(s)")
    for name, row in aggregated.items():
        def g(field, sec=False):
            v = row.get(field)
            if v is None:
                return "  n/a"
            return f"{v/1000.0:.1f}" if sec else f"{v:.1f}"
        print(
            f"{name:8s}  {row['offered_load_msg_s'] or 0:6g}   "
            f"{g('l1_ingestion_p50_ms')}/{g('l1_ingestion_p95_ms')}       "
            f"{g('l2_validation_p50_ms')}/{g('l2_validation_p95_ms')}       "
            f"{g('l3_export_p50_ms', True)}/{g('l3_export_p95_ms', True)}     "
            f"{g('l4_publish_p50_ms', True)}/{g('l4_publish_p95_ms', True)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
