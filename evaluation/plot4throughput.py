#!/usr/bin/env python3
"""Plot the 4-stage throughput-vs-offered-load figure from throughput_four.py's
output. Read the knee (bottleneck) off the curve by eye -- it is not annotated."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


X_COL = "input_rate_msg_s"
X_LABEL = "Offered load (msg/s)"

# (csv column, legend label, colour, marker) -- colours/markers match the
# reference thesis figure (raw highest, ckan lowest = end-to-end bottleneck).
CURVES = [
    ("throughput_t1_raw_msg_s", "T1 ingest (raw)", "#2ca02c", "o"),
    ("throughput_t2_validated_msg_s", "T2 validate (validated)", "#1f77b4", "s"),
    ("throughput_t3_csv_msg_s", "T3 export (CSV)", "#9467bd", "^"),
    ("throughput_t4_ckan_msg_s", "T4 publish (CKAN, end-to-end)", "#d62728", "D"),
]


def _f(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    rows = list(csv.DictReader(csv_path.open()))
    rows.sort(key=lambda r: float(r["offered_load_msg_s"]))
    return rows


def plot_throughput4(rows, out: Path) -> None:
    offered = [float(r[X_COL]) for r in rows]
    lim = max(offered) if offered else 1.0

    fig, ax = plt.subplots(figsize=(9, 6))
    # Ideal y = x: the system keeps perfect pace with offered load.
    ax.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
            linewidth=1.2, label="ideal (y = x)", zorder=1)

    ymax = lim
    for col, label, color, marker in CURVES:
        pts = [(float(r[X_COL]), _f(r.get(col))) for r in rows]
        pts = [(x, y) for x, y in pts if y is not None]
        if not pts:
            continue
        xs = [x for x, _ in pts]
        ys = [y for _, y in pts]
        ymax = max(ymax, max(ys))
        ax.plot(xs, ys, marker=marker, color=color, linewidth=1.8, markersize=6,
                label=label, zorder=3)

    ax.set_xlim(0, lim * 1.02)
    ax.set_ylim(0, ymax * 1.08)
    ax.set_xlabel(X_LABEL)
    ax.set_ylabel("Throughput (msg/s)")
    ax.set_title("throughput vs offered load", fontsize=12)
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
    rows = load_rows(csv_path)
    fig_dir = args.results_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    plot_throughput4(rows, fig_dir / "throughput4_vs_offered_load.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
