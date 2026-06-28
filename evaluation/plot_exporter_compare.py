#!/usr/bin/env python3
"""Overlay the baseline run vs the decoupled-exporter run.

Left panel:  end-to-end (T4) throughput vs offered load (+ y=x ideal).
Right panel: end-to-end p95 latency vs offered load.
Each result dir is aggregated by offered load (mean over repetitions)."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def agg(csv_path: Path):
    by_load = defaultdict(lambda: {"t4": [], "p95": []})
    for r in csv.DictReader(csv_path.open()):
        off = float(r["offered_load_msg_s"])
        by_load[off]["t4"].append(float(r["throughput_t4_ckan_msg_s"]))
        if r.get("e2e_p95_ms"):
            by_load[off]["p95"].append(float(r["e2e_p95_ms"]) / 1000.0)
    loads = sorted(by_load)
    t4 = [statistics.fmean(by_load[l]["t4"]) for l in loads]
    p95 = [statistics.fmean(by_load[l]["p95"]) if by_load[l]["p95"] else None for l in loads]
    return loads, t4, p95


def main() -> int:
    ap = argparse.ArgumentParser()
    base = Path(__file__).resolve().parent / "results"
    ap.add_argument("--baseline", type=Path, default=base / "thesis_throughput4" / "throughput_ramp4_summary.csv")
    ap.add_argument("--decoupled", type=Path, default=base / "thesis_throughput4_decoupled" / "throughput_ramp4_summary.csv")
    ap.add_argument("--out", type=Path, default=base / "thesis_throughput4_decoupled" / "figures" / "compare_baseline_vs_decoupled.png")
    args = ap.parse_args()

    bl, bt4, bp95 = agg(args.baseline)
    dl, dt4, dp95 = agg(args.decoupled)
    lim = max(max(bl), max(dl))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    ax1.plot([0, lim], [0, lim], linestyle=(0, (5, 4)), color="#999999", linewidth=1.2, label="ideal (y = x)")
    ax1.plot(bl, bt4, marker="o", color="#888888", linewidth=1.8, label="T4 baseline (coupled)")
    ax1.plot(dl, dt4, marker="D", color="#d62728", linewidth=2.0, label="T4 decoupled + parallel")
    ax1.set_xlim(0, lim * 1.02); ax1.set_ylim(0, lim * 1.08)
    ax1.set_xlabel("Offered load (msg/s)"); ax1.set_ylabel("Throughput (msg/s)")
    ax1.set_title("End-to-end throughput (T4)")
    ax1.legend(loc="upper left", fontsize=9); ax1.grid(True, linestyle=":", alpha=0.4)

    ax2.plot(bl, bp95, marker="o", color="#888888", linewidth=1.8, label="baseline (coupled)")
    ax2.plot(dl, dp95, marker="D", color="#d62728", linewidth=2.0, label="decoupled + parallel")
    ax2.set_xlim(0, lim * 1.02); ax2.set_ylim(bottom=0)
    ax2.set_xlabel("Offered load (msg/s)"); ax2.set_ylabel("End-to-end p95 latency (s)")
    ax2.set_title("End-to-end p95 latency")
    ax2.legend(loc="upper left", fontsize=9); ax2.grid(True, linestyle=":", alpha=0.4)

    for ax in (ax1, ax2):
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
    fig.suptitle("Exporter: coupled (baseline, 3 reps) vs decoupled + parallel (1 rep)", fontsize=13)
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=150)
    plt.close(fig)
    print(f"Wrote {args.out}")

    print(f"\n{'offered':>7} {'T4 base':>8} {'T4 deco':>8} {'p95 base(s)':>11} {'p95 deco(s)':>11}")
    dmap = dict(zip(dl, zip(dt4, dp95)))
    for i, l in enumerate(bl):
        if l in dmap:
            print(f"{l:7.0f} {bt4[i]:8.1f} {dmap[l][0]:8.1f} {bp95[i]:11.1f} {dmap[l][1]:11.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
