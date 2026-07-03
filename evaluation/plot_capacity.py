#!/usr/bin/env python3
"""Plot the FAIR Bridge capacity figure from throughput_capacity.py's output.

X-axis is the ACHIEVED input rate (the real load the system saw), which is the
honest independent variable for a saturation test: where the multi-process
generator sustains the offered load, achieved input ~= offered. Two panels:

  left  : per-stage makespan throughput (T1..T4) vs achieved input, with the
          ideal y = x. The knee is where a stage leaves the diagonal -- T1 (raw)
          stays on it; T2 (ETL validation) is the binding constraint.
  right : delivery ratio (T4 goodput / achieved input) vs achieved input. The
          saturation knee is the steep drop; the pipeline keeps real-time pace
          while this is ~1, and falls behind once it bends down.

Only points where the generator sustained the offered load (generator_sustained)
are trustworthy for reading the limit; others are drawn hollow.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

X_COL = "input_rate_msg_s"
CURVES = [
    ("throughput_t1_raw_msg_s", "T1 raw (Kafka)", "#2ca02c", "o"),
    ("throughput_t2_validated_msg_s", "T2 validated (ETL)", "#1f77b4", "s"),
    ("throughput_t3_csv_msg_s", "T3 csv (exporter)", "#9467bd", "^"),
    ("throughput_t4_ckan_msg_s", "T4 ckan (goodput)", "#d62728", "D"),
]


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_rows(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    rows.sort(key=lambda r: _f(r[X_COL]) or 0.0)
    return rows


def plot(rows: list[dict], out: Path) -> None:
    xs = [_f(r[X_COL]) for r in rows]
    sustained = [str(r.get("generator_sustained")).lower() == "true" for r in rows]
    lim = max(xs) * 1.05

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(14, 6))

    # ---- left: per-stage throughput vs achieved input ----
    axL.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
             linewidth=1.2, label="ideal (y = x)", zorder=1)
    for col, label, color, marker in CURVES:
        ys = [_f(r.get(col)) for r in rows]
        axL.plot(xs, ys, color=color, linewidth=1.8, zorder=2, label=label)
        for x, y, ok in zip(xs, ys, sustained):
            axL.plot(x, y, marker=marker, color=color, markersize=7, zorder=3,
                     markerfacecolor=(color if ok else "white"))
    axL.set_xlim(0, lim)
    axL.set_ylim(0, lim)
    axL.set_xlabel("Achieved input rate (msg/s)")
    axL.set_ylabel("Per-stage throughput (msg/s)")
    axL.set_title("Per-stage makespan throughput vs achieved input")
    axL.legend(loc="upper left", fontsize=9, frameon=True)
    axL.grid(True, linestyle=":", alpha=0.4)

    # ---- right: delivery ratio (knee) ----
    dr = [_f(r.get("delivery_ratio")) for r in rows]
    axR.axhline(1.0, linestyle=(0, (5, 4)), color="#999999", linewidth=1.2,
                label="ideal (ratio = 1)")
    axR.plot(xs, dr, color="#d62728", linewidth=1.8, zorder=2)
    for x, y, ok in zip(xs, dr, sustained):
        axR.plot(x, y, marker="D", color="#d62728", markersize=7, zorder=3,
                 markerfacecolor=("#d62728" if ok else "white"))
    axR.set_xlim(0, lim)
    axR.set_ylim(0, 1.08)
    axR.set_xlabel("Achieved input rate (msg/s)")
    axR.set_ylabel("Delivery ratio (T4 goodput / input)")
    axR.set_title("End-to-end delivery ratio (saturation knee)")
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
    args = ap.parse_args()
    rows = load_rows(args.results_dir / "throughput_ramp4_summary.csv")
    plot(rows, args.results_dir / "figures" / "capacity_curve.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
