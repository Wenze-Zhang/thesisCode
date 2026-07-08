#!/usr/bin/env python3
"""Plot the STEADY-STATE capacity figure from throughput_capacity.py's output.

Companion to plot_capacity.py, reading the ss_* columns instead of the makespan
columns. The makespan metric divides by "first to last appearance", so past the
knee the drain tail inflates the denominator and the plateau climbs with backlog
depth; the ss_* columns count stage-crossings inside a fixed interior of the
load window (the queueing-theory service rate mu), so overload shows the true
sustainable plateau. Two panels, same layout/colors as plot_capacity.py so the
two figures read side by side:

  left  : per-stage steady-state throughput (ss T1..T4) vs steady-state input,
          with the ideal y = x. The knee is where a stage leaves the diagonal.
  right : steady-state delivery ratio (ss_T4 / ss_input). ss_T4 is stamped per
          exporter flush (~30s batches), so single points may sit a hair above
          1.0 at low load -- batch-quantisation, not time travel.

Rows whose ss fields are blank (load window too short to trim, e.g. small
--dwell-s) are skipped. Hollow markers = generator did not sustain the load.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import plot_capacity as base  # noqa: E402  (reuse loader + house style)

X_COL = "ss_input_msg_s"
CURVES = [
    ("ss_t1_msg_s", "T1 raw (Kafka)", "#2ca02c", "o"),
    ("ss_t2_msg_s", "T2 validated (ETL)", "#1f77b4", "s"),
    ("ss_t3_msg_s", "T3 csv (exporter)", "#9467bd", "^"),
    ("ss_t4_msg_s", "T4 ckan (goodput)", "#d62728", "D"),
]


def _aggregate_median(rows: list[dict]) -> list[dict]:
    """Collapse repetitions of the same offered load into one point per load
    (median of every ss_* column). The knee region's service rate has large
    run-to-run variance, so single reps zigzag; the median across --repetitions
    is the thesis-figure estimator. generator_sustained is AND-ed (a load is
    only 'sustained' if every rep sustained it)."""
    from statistics import median
    groups: dict[float, list[dict]] = {}
    for r in rows:
        key = base._f(r.get("offered_load_msg_s"))
        if key is not None:
            groups.setdefault(key, []).append(r)
    out: list[dict] = []
    for key in sorted(groups):
        grp = groups[key]
        agg = dict(grp[0])
        for col in [X_COL, "ss_deliv"] + [c for c, _, _, _ in CURVES]:
            vals = [v for v in (base._f(g.get(col)) for g in grp) if v is not None]
            agg[col] = median(vals) if vals else None
        agg["generator_sustained"] = all(
            str(g.get("generator_sustained")).lower() == "true" for g in grp)
        out.append(agg)
    return out


def plot(rows: list[dict], out: Path) -> None:
    rows = [r for r in rows if base._f(r.get(X_COL))]
    rows.sort(key=lambda r: base._f(r[X_COL]) or 0.0)
    xs = [base._f(r[X_COL]) for r in rows]
    sustained = [str(r.get("generator_sustained")).lower() == "true" for r in rows]
    lim = max(xs) * 1.05

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(14, 6))

    axL.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
             linewidth=1.2, label="ideal (y = x)", zorder=1)
    for col, label, color, marker in CURVES:
        ys = [base._f(r.get(col)) for r in rows]
        axL.plot(xs, ys, color=color, linewidth=1.8, zorder=2, label=label)
        for x, y, ok in zip(xs, ys, sustained):
            axL.plot(x, y, marker=marker, color=color, markersize=7, zorder=3,
                     markerfacecolor=(color if ok else "white"))
    axL.set_xlim(0, lim)
    axL.set_ylim(0, lim)
    axL.set_xlabel("Steady-state input rate (msg/s)")
    axL.set_ylabel("Per-stage steady-state throughput (msg/s)")
    axL.set_title("Per-stage throughput inside the load window (drain excluded)")
    axL.legend(loc="upper left", fontsize=9, frameon=True)
    axL.grid(True, linestyle=":", alpha=0.4)

    dr = [base._f(r.get("ss_deliv")) for r in rows]
    axR.axhline(1.0, linestyle=(0, (5, 4)), color="#999999", linewidth=1.2,
                label="ideal (ratio = 1)")
    axR.plot(xs, dr, color="#d62728", linewidth=1.8, zorder=2)
    for x, y, ok in zip(xs, dr, sustained):
        axR.plot(x, y, marker="D", color="#d62728", markersize=7, zorder=3,
                 markerfacecolor=("#d62728" if ok else "white"))
    axR.set_xlim(0, lim)
    axR.set_ylim(0, 1.12)
    axR.set_xlabel("Steady-state input rate (msg/s)")
    axR.set_ylabel("Delivery ratio (ss T4 / ss input)")
    axR.set_title("Steady-state delivery ratio (saturation knee)")
    axR.legend(loc="lower left", fontsize=9, frameon=True)
    axR.grid(True, linestyle=":", alpha=0.4)

    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path,
                    default=Path(__file__).resolve().parent / "results" / "thesis_capacity")
    ap.add_argument("--median", action="store_true",
                    help="Collapse repetitions of each offered load into one "
                         "median point (recommended for the thesis figure).")
    args = ap.parse_args()
    rows = base.load_rows(args.results_dir / "throughput_ramp4_summary.csv")
    name = "capacity_curve_ss_median.png" if args.median else "capacity_curve_ss.png"
    if args.median:
        rows = _aggregate_median(rows)
    plot(rows, args.results_dir / "figures" / name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
