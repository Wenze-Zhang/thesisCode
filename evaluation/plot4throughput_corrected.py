#!/usr/bin/env python3
"""APPROXIMATE corrected throughput-vs-offered-load figure from an OLD run.

The previous throughput_four runs reported each stage as a *windowed completion
rate*: completions (t_ckan, ...) landing in a fixed wall-clock window, divided by
the window length. Because every step starts from a cold, empty pipeline, the
warm-up backlog drains INTO that window, so the window counts more CKAN commits
than messages were actually sent during it -- pushing the end-to-end (T4) curve
ABOVE the y=x ideal (delivery_ratio > 1, which is physically impossible).

This script recomputes a conservation-respecting *makespan* throughput from the
saved aggregates so the figure is physically valid (nothing exceeds y=x):

    T4 (end-to-end) = publishable produced / (produce_len + drain_s)
                      = produced_in_step / (produced_in_step/input_rate + drain_s)

    T1/T2/T3        = min(reported, input_rate)   (their warm-up leak is tiny;
                      clamp the small overshoot to the offered load)

It is APPROXIMATE: the raw per-event timestamps were not persisted, so only T4
(the broken curve) can be recomputed exactly. For exact per-stage makespan rates,
re-run throughput_four.py (now fixed) and use plot4throughput.py.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def _f(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def corrected_rows(csv_path: Path) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    rows = list(csv.DictReader(csv_path.open()))
    rows.sort(key=lambda r: _f(r["offered_load_msg_s"]))
    for r in rows:
        offered = _f(r["offered_load_msg_s"])
        input_rate = _f(r["input_rate_msg_s"])
        produced = _f(r["produced_in_step"])
        drain_s = _f(r["drain_s"])
        produce_len = produced / input_rate if input_rate > 0 else 0.0
        # Conservation-respecting end-to-end goodput: N messages pushed all the
        # way to CKAN in (production span + drain tail).
        t4 = produced / (produce_len + drain_s) if (produce_len + drain_s) > 0 else 0.0
        out.append({
            "offered": offered,
            "input": input_rate,
            "t1": min(_f(r["throughput_t1_raw_msg_s"]), input_rate),
            "t2": min(_f(r["throughput_t2_validated_msg_s"]), input_rate),
            "t3": min(_f(r["throughput_t3_csv_msg_s"]), input_rate),
            "t4": t4,
            "t4_old": _f(r["throughput_t4_ckan_msg_s"]),
        })
    return out


CURVES = [
    ("t1", "T1 ingest (raw)", "#2ca02c", "o"),
    ("t2", "T2 validate (validated)", "#1f77b4", "s"),
    ("t3", "T3 export (CSV)", "#9467bd", "^"),
    ("t4", "T4 publish (CKAN, end-to-end)", "#d62728", "D"),
]


def plot(rows: list[dict[str, float]], out: Path) -> None:
    xs = [r["input"] for r in rows]
    lim = max(xs) if xs else 1.0

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
            linewidth=1.2, label="ideal (y = x)", zorder=1)

    ymax = lim
    for key, label, color, marker in CURVES:
        ys = [r[key] for r in rows]
        ymax = max(ymax, max(ys))
        ax.plot(xs, ys, marker=marker, color=color, linewidth=1.8, markersize=6,
                label=label, zorder=3)

    ax.set_xlim(0, lim * 1.02)
    ax.set_ylim(0, ymax * 1.08)
    ax.set_xlabel("Offered load (msg/s)")
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


def plot_single(rows: list[dict[str, float]], out: Path) -> None:
    """Single-curve preview: end-to-end (CKAN) throughput vs offered load."""
    xs = [r["input"] for r in rows]
    ys = [r["t4"] for r in rows]
    lim = max(xs) if xs else 1.0
    ymax = max(max(ys), lim)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999",
            linewidth=1.2, label="ideal (y = x)", zorder=1)
    ax.plot(xs, ys, marker="D", color="#d62728", linewidth=2.0, markersize=7,
            label="end-to-end throughput (CKAN)", zorder=3)
    ax.set_xlim(0, lim * 1.02)
    ax.set_ylim(0, ymax * 1.08)
    ax.set_xlabel("Offered load (msg/s)")
    ax.set_ylabel("Throughput (msg/s)")
    ax.set_title("end-to-end throughput vs offered load  (PREVIEW, approx)",
                 fontsize=12)
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
    rows = corrected_rows(csv_path)
    fig_dir = args.results_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    plot(rows, fig_dir / "throughput4_vs_offered_load_corrected.png")
    plot_single(rows, fig_dir / "end_to_end_throughput_corrected_preview.png")

    print("\noffered  input    T1     T2     T3     T4(new)  T4(old)")
    for r in rows:
        print(f"{r['offered']:7.0f}  {r['input']:6.1f}  {r['t1']:5.1f}  "
              f"{r['t2']:5.1f}  {r['t3']:5.1f}  {r['t4']:7.1f}  {r['t4_old']:7.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
