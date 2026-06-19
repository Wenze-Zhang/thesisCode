#!/usr/bin/env python3
"""Plot ONE curve: the overall end-to-end throughput vs offered load.

Overall throughput (per offered load) is the conservation-respecting makespan
rate of the whole pipeline:

    throughput = (rows that reached CKAN) / (time CKAN saw the last row
                                             - time the first telemetry was sent)
               = count(t_ckan) / (max(t_ckan) - first t_send)

which is exactly throughput_four.py's T4 (t_ckan) curve. With multiple
repetitions per offered load the point is the mean over reps and the error bar
is +/- 1 stdev. Read the knee (bottleneck) off the curve by eye."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import plot4throughput as p4

T4_COL = "throughput_t4_ckan_msg_s"


def plot_end_to_end(rows, out: Path) -> None:
    agg = p4.aggregate(rows, [T4_COL])
    pts = [(a["x"], a[T4_COL], a[f"{T4_COL}__std"])
           for a in agg if a["x"] is not None and a[T4_COL] is not None]
    if not pts:
        raise SystemExit("no end-to-end throughput points to plot")
    xs = [x for x, _, _ in pts]
    ys = [y for _, y, _ in pts]
    es = [e for _, _, e in pts]
    lim = max(xs)
    ymax = max(max(ys), lim)
    reps = max((a["n"] for a in agg), default=1)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
            linewidth=1.2, label="ideal (y = x)", zorder=1)
    ax.errorbar(xs, ys, yerr=es if reps > 1 else None, marker="D",
                color="#d62728", linewidth=2.0, markersize=7, capsize=4,
                label="end-to-end throughput (CKAN)", zorder=3)

    ax.set_xlim(0, lim * 1.02)
    ax.set_ylim(0, ymax * 1.08)
    ax.set_xlabel("Offered load (msg/s)")
    ax.set_ylabel("Throughput (msg/s)")
    title = "end-to-end throughput vs offered load"
    if reps > 1:
        title += f"  (mean +/- 1 sd, {reps} reps)"
    ax.set_title(title, fontsize=12)
    ax.legend(loc="upper left", frameon=True, fontsize=9)
    ax.grid(True, linestyle=":", alpha=0.4)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path,
                    default=Path(__file__).resolve().parent / "results" / "thesis_throughput4")
    args = ap.parse_args()

    csv_path = args.results_dir / "throughput_ramp4_summary.csv"
    rows = p4.load_rows(csv_path)
    fig_dir = args.results_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    plot_end_to_end(rows, fig_dir / "end_to_end_throughput_vs_offered_load.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
