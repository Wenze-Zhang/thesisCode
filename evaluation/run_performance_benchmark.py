#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
REGISTRY_PATH = FAIR_BRIDGE_DIR / "registry" / "field_registry.yaml"
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"

if str(FAIR_BRIDGE_DIR) not in sys.path:
    sys.path.insert(0, str(FAIR_BRIDGE_DIR))
os.environ.setdefault("REGISTRY_PATH", str(REGISTRY_PATH))

import config  # noqa: E402
import etl  # noqa: E402
import registry  # noqa: E402


LOCAL_COLUMNS = [
    "test_id",
    "mode",
    "payload_size",
    "invalid_ratio",
    "offered_load_msg_s",
    "duration_s",
    "produced_count",
    "validated_count",
    "dlq_count",
    "process_error_count",
    "message_loss_count",
    "elapsed_s",
    "throughput_msg_s",
    "p50_processing_latency_ms",
    "p95_processing_latency_ms",
    "p99_processing_latency_ms",
    "max_processing_latency_ms",
    "latency_threshold_ms",
    "benchmark_result_pass_fail",
]

KAFKA_EVENT_COLUMNS = [
    "test_id",
    "offered_load_msg_s",
    "seq",
    "device_id",
    "expected_topic",
    "actual_topic",
    "payload_valid",
    "sent_at",
    "produced_at",
    "validated_seen_at",
    "dlq_seen_at",
    "latency_ms",
]

# parameters into python dict
def _parse_rates(value: str) -> list[float]:
    rates = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        rate = float(token)
        if rate <= 0:
            raise argparse.ArgumentTypeError("Rates must be positive")
        rates.append(rate)
    if not rates:
        raise argparse.ArgumentTypeError("At least one rate is required")
    return rates

# 1.5 into 1p5 to put into deviceId or kafka consumer group id
def _rate_label(rate: float) -> str:
    return str(rate).replace(".", "p")

# return timestamp in iso format with timezone
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ISO string to datatime
def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# calcualte p50, p95, p99 percentiles
def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    # linear interpolation 
    rank = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(ordered[int(rank)], 6)
    weight = rank - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 6)

# payload size padding to increase processing time and message size
def _padding(payload_size: str) -> str:
    if payload_size == "small":
        return ""
    if payload_size == "medium":
        return "x" * 1024
    if payload_size == "large":
        return "x" * 10_240
    raise ValueError(f"Unsupported payload size: {payload_size}")


# generate messages for testing with valid or invalid values
def _synthetic_message(
    *,
    test_id: str,
    rate: float,
    seq: int,
    valid: bool,
    payload_size: str,
) -> dict[str, Any]:
    sent_at = _now_iso()
    rate_label = _rate_label(rate)
    values = (
        {
            "temperature_c": round(20.0 + (seq % 100) / 10.0, 2),
            "humidity_pct": round(40.0 + (seq % 50) / 2.0, 2),
        }
        if valid
        else {"temperature_c": 999.0}
    )
    message = {
        "deviceName": f"climate-benchmark-{test_id}",
        "deviceId": f"{test_id}-{rate_label}-{seq:06d}",
        "ts": sent_at,
        "values": values,
        "test_id": test_id,
        "seq": seq,
        "sent_at": sent_at,
    }
    padding = _padding(payload_size)
    if padding:
        message["padding"] = padding
    return message


# write csv file with header and rows
def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # with is file mangaer. After use, file is closed automatically 
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# test etl without kafka
def run_local_etl(args: argparse.Namespace) -> Path:
    reg = registry.load_registry(str(REGISTRY_PATH))
    rng = random.Random(args.seed)
    rows = []

    # different rates
    for rate in args.rates:
        produced_count = max(1, int(rate * args.duration_s))
        validated_count = 0
        dlq_count = 0
        process_error_count = 0
        latencies_ms: list[float] = []
        start = time.perf_counter()

        # generate testing messages
        for seq in range(1, produced_count + 1):
            valid = rng.random() >= args.invalid_ratio
            msg = _synthetic_message(
                test_id=args.test_id,
                rate=rate,
                seq=seq,
                valid=valid,
                payload_size=args.payload_size,
            )
            
            before = time.perf_counter_ns()
            
            try:
                topic, _payload = etl.process_message(msg, {}, reg=reg)
            except Exception:
                process_error_count += 1
                continue
            after = time.perf_counter_ns()
            latencies_ms.append((after - before) / 1_000_000)
            if topic == config.KAFKA_TOPIC_TELEMETRY_VALIDATED:
                validated_count += 1
            elif topic == config.KAFKA_TOPIC_DLQ:
                dlq_count += 1

        elapsed_s = max(time.perf_counter() - start, 0.000001)
        message_loss_count = produced_count - validated_count - dlq_count
        p95 = _percentile(latencies_ms, 95) or 0.0
        passed = (
            message_loss_count == 0
            and process_error_count == 0
            and p95 <= args.latency_threshold_ms
        )

        rows.append(
            {
                "test_id": args.test_id,
                "mode": "local-etl",
                "payload_size": args.payload_size,
                "invalid_ratio": args.invalid_ratio,
                "offered_load_msg_s": rate,
                "duration_s": args.duration_s,
                "produced_count": produced_count,
                "validated_count": validated_count,
                "dlq_count": dlq_count,
                "process_error_count": process_error_count,
                "message_loss_count": message_loss_count,
                "elapsed_s": round(elapsed_s, 6),
                "throughput_msg_s": round(produced_count / elapsed_s, 6),
                "p50_processing_latency_ms": _percentile(latencies_ms, 50),
                "p95_processing_latency_ms": p95,
                "p99_processing_latency_ms": _percentile(latencies_ms, 99),
                "max_processing_latency_ms": round(max(latencies_ms), 6) if latencies_ms else None,
                "latency_threshold_ms": args.latency_threshold_ms,
                "benchmark_result_pass_fail": "pass" if passed else "fail",
            }
        )

    # Benchmark results are written here so thesis figures can be reproduced.
    output_path = args.results_dir / "local_etl_benchmark.csv"
    _write_csv(output_path, LOCAL_COLUMNS, rows)
    print(f"Wrote {output_path}")
    return output_path


def _import_kafka_clients():
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "kafka-python is required for --mode kafka-e2e. "
            "Install fair-bridge/requirements.txt in the local environment or run inside the project environment."
        ) from exc
    return KafkaConsumer, KafkaProducer


def _make_consumer(KafkaConsumer, topic: str, group_id: str, bootstrap_server: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # bytes in kafka -> python dict
        value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else {},
    )
    deadline = time.time() + 10
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(timeout_ms=200)
    return consumer

# poll messages from validated and dlq topics
def _collect_outputs(consumer, topic: str, events: dict[str, dict[str, Any]]) -> int:
    seen = 0
    polled = consumer.poll(timeout_ms=250, max_records=500)
    seen_at = _now_iso()
    for batch in polled.values():
        for record in batch:
            payload = record.value or {}
            device_id = payload.get("device_id") or payload.get("deviceId")
            if device_id not in events:
                continue
            event = events[device_id]
            if topic == config.KAFKA_TOPIC_TELEMETRY_VALIDATED:
                if event.get("validated_seen_at"):
                    continue
                event["validated_seen_at"] = seen_at
                event["actual_topic"] = config.KAFKA_TOPIC_TELEMETRY_VALIDATED
            else:
                if event.get("dlq_seen_at"):
                    continue
                event["dlq_seen_at"] = seen_at
                event["actual_topic"] = config.KAFKA_TOPIC_DLQ
            try:
                # latency calculation 
                event["latency_ms"] = round(
                    (_parse_iso(seen_at) - _parse_iso(event["sent_at"])).total_seconds() * 1000.0,
                    6,
                )
            except Exception:
                event["latency_ms"] = None
            seen += 1
    return seen


def _check_export_csv(results_export_dir: Path, test_id: str) -> bool | None:
    if not results_export_dir.exists():
        return None
    pattern = f"sensor-climate-benchmark-{test_id}/*.csv"
    return any(results_export_dir.glob(pattern))


def _summarize_kafka_run(
    *,
    args: argparse.Namespace,
    rate: float,
    events: dict[str, dict[str, Any]],
    produce_elapsed_s: float,
) -> dict[str, Any]:
    produced_count = len(events)
    validated_count = sum(1 for event in events.values() if event.get("validated_seen_at"))
    dlq_count = sum(1 for event in events.values() if event.get("dlq_seen_at"))
    output_count = validated_count + dlq_count
    message_loss_count = produced_count - output_count
    validated_latencies = [
        float(event["latency_ms"])
        for event in events.values()
        if event.get("validated_seen_at") and event.get("latency_ms") is not None
    ]
    p95 = _percentile(validated_latencies, 95)
    passed = (
        message_loss_count == 0
        and output_count == produced_count
        and (p95 is None or p95 <= args.latency_threshold_ms)
    )

    return {
        "test_id": args.test_id,
        "mode": "kafka-e2e",
        "payload_size": args.payload_size,
        "invalid_ratio": args.invalid_ratio,
        "offered_load_msg_s": rate,
        "duration_s": args.duration_s,
        "produced_count": produced_count,
        "validated_count": validated_count,
        "dlq_count": dlq_count,
        "output_rate_msg_s": round(output_count / max(produce_elapsed_s, 0.000001), 6),
        "message_loss_count": message_loss_count,
        "p50_raw_to_validated_latency_ms": _percentile(validated_latencies, 50),
        "p95_raw_to_validated_latency_ms": p95,
        "p99_raw_to_validated_latency_ms": _percentile(validated_latencies, 99),
        "max_latency_ms": round(max(validated_latencies), 6) if validated_latencies else None,
        "latency_threshold_ms": args.latency_threshold_ms,
        "benchmark_result_pass_fail": "pass" if passed else "fail",
        "csv_file_seen": (
            _check_export_csv(args.export_dir, args.test_id)
            if args.check_export_dir
            else None
        ),
    }

# run through etl with kafka
def run_kafka_e2e(args: argparse.Namespace) -> tuple[Path, Path]:
    KafkaConsumer, KafkaProducer = _import_kafka_clients()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=0,
        acks="all",
    )
    rng = random.Random(args.seed)
    all_events: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    try:
        for rate in args.rates:
            rate_label = _rate_label(rate)
            validated_consumer = _make_consumer(
                KafkaConsumer,
                config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
                f"eval-{args.test_id}-{rate_label}-validated-{int(time.time())}",
                args.bootstrap_server,
            )
            dlq_consumer = _make_consumer(
                KafkaConsumer,
                config.KAFKA_TOPIC_DLQ,
                f"eval-{args.test_id}-{rate_label}-dlq-{int(time.time())}",
                args.bootstrap_server,
            )
            events: dict[str, dict[str, Any]] = {}
            seq = 0
            start = time.perf_counter()
            next_send = start
            deadline = start + args.duration_s

            while time.perf_counter() < deadline:
                seq += 1
                valid = rng.random() >= args.invalid_ratio
                msg = _synthetic_message(
                    test_id=args.test_id,
                    rate=rate,
                    seq=seq,
                    valid=valid,
                    payload_size=args.payload_size,
                )
                device_id = msg["deviceId"]
                expected_topic = (
                    config.KAFKA_TOPIC_TELEMETRY_VALIDATED
                    if valid
                    else config.KAFKA_TOPIC_DLQ
                )
                producer.send(config.KAFKA_TOPIC_TELEMETRY_RAW, msg).get(timeout=10)
                produced_at = _now_iso()
                events[device_id] = {
                    "test_id": args.test_id,
                    "offered_load_msg_s": rate,
                    "seq": seq,
                    "device_id": device_id,
                    "expected_topic": expected_topic,
                    "actual_topic": "",
                    "payload_valid": valid,
                    "sent_at": msg["sent_at"],
                    "produced_at": produced_at,
                    "validated_seen_at": "",
                    "dlq_seen_at": "",
                    "latency_ms": "",
                }

                _collect_outputs(validated_consumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED, events)
                _collect_outputs(dlq_consumer, config.KAFKA_TOPIC_DLQ, events)

                next_send += 1.0 / rate
                sleep_s = next_send - time.perf_counter()
                if sleep_s > 0:
                    time.sleep(sleep_s)

            producer.flush()
            produce_elapsed_s = max(time.perf_counter() - start, 0.000001)
            output_deadline = time.time() + args.cooldown_s
            while time.time() < output_deadline:
                seen = 0
                seen += _collect_outputs(validated_consumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED, events)
                seen += _collect_outputs(dlq_consumer, config.KAFKA_TOPIC_DLQ, events)
                if sum(1 for event in events.values() if event.get("actual_topic")) == len(events):
                    break
                if seen == 0:
                    time.sleep(0.25)

            summaries.append(
                _summarize_kafka_run(
                    args=args,
                    rate=rate,
                    events=events,
                    produce_elapsed_s=produce_elapsed_s,
                )
            )
            all_events.extend(events.values())
            validated_consumer.close()
            dlq_consumer.close()
    finally:
        producer.close()

    # Benchmark event and summary files are written here for reproducible analysis.
    events_path = args.results_dir / "kafka_e2e_benchmark_events.csv"
    summary_path = args.results_dir / "kafka_e2e_benchmark_summary.json"
    _write_csv(events_path, KAFKA_EVENT_COLUMNS, all_events)
    summary_doc = {
        "generated_at": _now_iso(),
        "bootstrap_server": args.bootstrap_server,
        "raw_topic": config.KAFKA_TOPIC_TELEMETRY_RAW,
        "validated_topic": config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        "dlq_topic": config.KAFKA_TOPIC_DLQ,
        "runs": summaries,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary_doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"Wrote {events_path}")
    print(f"Wrote {summary_path}")
    return events_path, summary_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run FAIR Bridge performance benchmarks.")
    parser.add_argument("--mode", choices=["local-etl", "kafka-e2e"], required=True)
    parser.add_argument("--rate", default="1,5,10,25,50", help="Comma-separated offered load values.")
    parser.add_argument("--duration-s", type=int, default=300)
    parser.add_argument("--payload-size", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--invalid-ratio", type=float, default=0.0)
    parser.add_argument("--test-id", default="perf-run-001")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--cooldown-s", type=int, default=30)
    parser.add_argument("--latency-threshold-ms", type=float, default=1000.0)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--check-export-dir", action="store_true")
    parser.add_argument("--export-dir", type=Path, default=Path(config.EXPORT_DIR))
    args = parser.parse_args()

    if not 0.0 <= args.invalid_ratio <= 1.0:
        raise SystemExit("--invalid-ratio must be between 0.0 and 1.0")
    args.rates = _parse_rates(args.rate)
    args.results_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "local-etl":
        run_local_etl(args)
    else:
        run_kafka_e2e(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
