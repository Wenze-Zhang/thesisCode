#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"
DEFAULT_TARGET_ACHIEVED_THRESHOLD = 0.95

SUSTAINABLE_COLUMNS = [
    "payload_size",
    "invalid_ratio",
    "mode",
    "sustainable_throughput_msg_s",
    "first_fail_rate_msg_s",
    "fail_reason",
    "latency_threshold_ms",
    "number_of_repetitions",
    "pass_count",
    "fail_count",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    fieldnames: list[str] | None = None,
) -> None:
    if not rows and fieldnames is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = fieldnames or list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def _read_kafka_summary(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload.get("runs") or []


def _float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(row: dict[str, Any], key: str) -> int:
    value = _float(row, key)
    return int(value) if value is not None else 0


def _metric(row: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = _float(row, key)
        if value is not None:
            return value
    return None


def _target_achieved_ratio(row: dict[str, Any]) -> float | None:
    explicit = _float(row, "target_achieved_ratio")
    if explicit is not None:
        return explicit

    offered_load = _float(row, "offered_load_msg_s")
    if offered_load is None or offered_load <= 0:
        return None

    produced_throughput = _float(row, "produced_throughput_msg_s")
    if produced_throughput is not None:
        return produced_throughput / offered_load

    produced_count = _float(row, "produced_count")
    duration_s = _float(row, "duration_s")
    if produced_count is None or duration_s is None or duration_s <= 0:
        return None
    return (produced_count / duration_s) / offered_load


def _target_load_ok(row: dict[str, Any]) -> bool:
    ratio = _target_achieved_ratio(row)
    if ratio is None:
        return True
    threshold = _float(row, "target_achieved_threshold")
    if threshold is None:
        threshold = DEFAULT_TARGET_ACHIEVED_THRESHOLD
    return ratio >= threshold


def _is_pass(row: dict[str, Any]) -> bool:
    explicit_pass = str(row.get("benchmark_result_pass_fail") or "").lower() == "pass"
    return explicit_pass and _target_load_ok(row)


def _split_reasons(value: Any) -> list[str]:
    if not value:
        return []
    return [
        token.strip()
        for token in str(value).replace(",", ";").split(";")
        if token.strip()
    ]


def _derived_fail_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = _split_reasons(row.get("fail_reason"))
    if _int(row, "message_loss_count") != 0:
        reasons.append("message_loss")
    if _int(row, "observed_backlog_after_cooldown_count") != 0:
        reasons.append("observed_backlog_after_cooldown")
    if _int(row, "misrouted_count") != 0:
        reasons.append("misrouted_message")

    threshold = _float(row, "latency_threshold_ms")
    p95 = _metric(row, ["p95_fast_path_latency_ms"])
    if threshold is not None and (p95 is None or p95 > threshold):
        reasons.append("p95_latency_above_threshold")
    if not _target_load_ok(row):
        reasons.append("target_load_not_achieved")

    return list(dict.fromkeys(reasons))


def _fail_reason_text(rows: list[dict[str, Any]]) -> str:
    reasons: list[str] = []
    for row in rows:
        if _is_pass(row):
            continue
        reasons.extend(_derived_fail_reasons(row))
    return ";".join(dict.fromkeys(reasons))


def _group_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("payload_size") or ""),
        str(row.get("invalid_ratio") or ""),
        str(row.get("mode") or ""),
    )


def _calculate_sustainable_throughput(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(_group_key(row), []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for (payload_size, invalid_ratio, mode), group_rows in sorted(groups.items()):
        by_rate: dict[float, list[dict[str, Any]]] = {}
        for row in group_rows:
            rate = _float(row, "offered_load_msg_s")
            if rate is None:
                continue
            by_rate.setdefault(rate, []).append(row)

        sustainable_rate: float | None = None
        first_fail_rate: float | None = None
        first_fail_rows: list[dict[str, Any]] = []
        for rate in sorted(by_rate):
            rate_rows = by_rate[rate]
            if rate_rows and all(_is_pass(row) for row in rate_rows):
                sustainable_rate = rate if sustainable_rate is None else max(sustainable_rate, rate)
            elif first_fail_rate is None:
                first_fail_rate = rate
                first_fail_rows = rate_rows

        latency_thresholds = [
            value
            for value in (_float(row, "latency_threshold_ms") for row in group_rows)
            if value is not None
        ]
        repetitions_per_rate = [len(rate_rows) for rate_rows in by_rate.values()]
        pass_count = sum(1 for row in group_rows if _is_pass(row))
        fail_count = len(group_rows) - pass_count

        summary_rows.append(
            {
                "payload_size": payload_size,
                "invalid_ratio": invalid_ratio,
                "mode": mode,
                "sustainable_throughput_msg_s": sustainable_rate if sustainable_rate is not None else "",
                "first_fail_rate_msg_s": first_fail_rate if first_fail_rate is not None else "",
                "fail_reason": _fail_reason_text(first_fail_rows),
                "latency_threshold_ms": (
                    latency_thresholds[0] if latency_thresholds else ""
                ),
                "number_of_repetitions": max(repetitions_per_rate) if repetitions_per_rate else 0,
                "pass_count": pass_count,
                "fail_count": fail_count,
            }
        )
    return summary_rows


def _series_label(row: dict[str, Any]) -> str:
    payload_size = row.get("payload_size") or "payload"
    invalid_ratio = row.get("invalid_ratio") or "0"
    mode = row.get("mode") or "mode"
    return f"{mode} {payload_size} invalid={invalid_ratio}"


def _mean_series(
    rows: list[dict[str, Any]],
    metric_keys: list[str],
) -> dict[str, list[tuple[float, float]]]:
    grouped: dict[tuple[str, float], list[float]] = {}
    for row in rows:
        rate = _float(row, "offered_load_msg_s")
        value = _metric(row, metric_keys)
        if rate is None or value is None:
            continue
        grouped.setdefault((_series_label(row), rate), []).append(value)

    series: dict[str, list[tuple[float, float]]] = {}
    for (label, rate), values in grouped.items():
        if values:
            series.setdefault(label, []).append((rate, statistics.mean(values)))

    for values in series.values():
        values.sort(key=lambda item: item[0])
    return series


def _plot_line_series(
    *,
    results_dir: Path,
    rows: list[dict[str, Any]],
    metric_keys: list[str],
    filename: str,
    ylabel: str,
    title: str,
) -> None:
    import matplotlib.pyplot as plt

    series = _mean_series(rows, metric_keys)
    if not series:
        return

    plt.figure()
    for label, points in sorted(series.items()):
        plt.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            marker="o",
            label=label,
        )
    plt.xlabel("Offered load (msg/s)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(results_dir / filename)
    plt.close()


def _plot_validated_vs_dlq(results_dir: Path, rows: list[dict[str, Any]]) -> None:
    import matplotlib.pyplot as plt

    grouped: dict[tuple[str, float], dict[str, list[float]]] = {}
    for row in rows:
        rate = _float(row, "offered_load_msg_s")
        if rate is None:
            continue
        key = (_series_label(row), rate)
        bucket = grouped.setdefault(key, {"validated": [], "dlq": []})
        bucket["validated"].append(float(_int(row, "validated_count")))
        bucket["dlq"].append(float(_int(row, "dlq_count")))

    if not grouped:
        return

    labels: list[str] = []
    validated: list[float] = []
    dlq: list[float] = []
    for (label, rate), values in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        labels.append(f"{label}\n{rate:g}")
        validated.append(statistics.mean(values["validated"]))
        dlq.append(statistics.mean(values["dlq"]))

    positions = range(len(labels))
    plt.figure(figsize=(max(8, len(labels) * 0.8), 5))
    plt.bar(positions, validated, label="validated")
    plt.bar(positions, dlq, bottom=validated, label="DLQ")
    plt.xticks(list(positions), labels, rotation=35, ha="right")
    plt.xlabel("Workload and offered load (msg/s)")
    plt.ylabel("Message count")
    plt.title("Validated vs DLQ counts")
    plt.legend()
    plt.tight_layout()
    plt.savefig(results_dir / "validated_vs_dlq_counts.png")
    plt.close()


def _plot_sustainable(
    results_dir: Path,
    sustainable_rows: list[dict[str, Any]],
) -> None:
    import matplotlib.pyplot as plt

    values = [
        row
        for row in sustainable_rows
        if row.get("sustainable_throughput_msg_s") not in ("", None)
    ]
    payload_sizes = {row.get("payload_size") for row in values}
    if len(values) < 2 and len(payload_sizes) < 2:
        return

    labels = [
        f"{row['mode']}\n{row['payload_size']}\ninvalid={row['invalid_ratio']}"
        for row in values
    ]
    heights = [float(row["sustainable_throughput_msg_s"]) for row in values]
    positions = range(len(labels))

    plt.figure(figsize=(max(8, len(labels) * 0.8), 5))
    plt.bar(positions, heights)
    plt.xticks(list(positions), labels, rotation=20, ha="right")
    plt.xlabel("Payload size")
    plt.ylabel("Sustainable throughput (msg/s)")
    plt.title("Sustainable throughput by payload size")
    plt.tight_layout()
    plt.savefig(results_dir / "sustainable_throughput_by_payload_size.png")
    plt.close()


def _plot_outputs(
    results_dir: Path,
    rows: list[dict[str, Any]],
    sustainable_rows: list[dict[str, Any]],
) -> bool:
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("matplotlib is not installed; skipping PNG plots.")
        return False

    _plot_line_series(
        results_dir=results_dir,
        rows=rows,
        metric_keys=["etl_output_throughput_msg_s"],
        filename="throughput_vs_offered_load.png",
        ylabel="ETL output throughput (msg/s)",
        title="Throughput vs offered load",
    )
    _plot_line_series(
        results_dir=results_dir,
        rows=rows,
        metric_keys=["p95_fast_path_latency_ms"],
        filename="p95_fast_path_latency_vs_offered_load.png",
        ylabel="p95 latency (ms)",
        title="p95 fast-path latency vs offered load",
    )
    _plot_validated_vs_dlq(results_dir, rows)
    _plot_sustainable(results_dir, sustainable_rows)
    return True


def run(args: argparse.Namespace) -> None:
    results_dir = args.results_dir
    kafka_rows = _read_kafka_summary(results_dir / "kafka_e2e_benchmark_summary.json")
    sustainable_rows = _calculate_sustainable_throughput(kafka_rows)

    _write_csv(results_dir / "benchmark_kafka_summary.csv", kafka_rows)
    _write_csv(
        results_dir / "sustainable_throughput_summary.csv",
        sustainable_rows,
        SUSTAINABLE_COLUMNS,
    )
    _write_json(
        results_dir / "sustainable_throughput_summary.json",
        {
            "generated_at": _now_iso(),
            "definition": (
                "Highest offered_load_msg_s for which all repetitions pass the "
                "FAIR Bridge stability criteria and achieve the target input load."
            ),
            "rows": sustainable_rows,
        },
    )

    plots_written = False
    if args.plots:
        plots_written = _plot_outputs(results_dir, kafka_rows, sustainable_rows)

    print(f"Read results from {results_dir}")
    if kafka_rows:
        print(f"Wrote {results_dir / 'benchmark_kafka_summary.csv'}")
    print(f"Wrote {results_dir / 'sustainable_throughput_summary.csv'}")
    print(f"Wrote {results_dir / 'sustainable_throughput_summary.json'}")
    if args.plots and plots_written:
        print(f"PNG plots, when enough data exists, are saved in {results_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyse FAIR Bridge benchmark result files.")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--plots", action="store_true", help="Generate matplotlib PNG plots.")
    args = parser.parse_args()
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
