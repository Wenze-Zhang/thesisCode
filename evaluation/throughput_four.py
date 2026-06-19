#!/usr/bin/env python3
"""FAIR Bridge 4-stage throughput benchmark (discrete, fully isolated steps).

Each offered-load step is an INDEPENDENT experiment: a brand-new set of devices
(=> brand-new CKAN datasets) is provisioned, load is applied for `dwell_s`, then
the pipeline is drained to ZERO (exporter Kafka lag == 0 AND every produced
message committed to CKAN) before the next, higher offered load starts. No state,
backlog, CSV, or dataset is shared between steps, so each point is measured on a
clean system and the low-load stages track the ideal y = x.

Four throughput curves are reported, one per pipeline stage. Each is the
conservation-respecting MAKESPAN rate at that stage -- (messages that reached the
stage) / (last completion at the stage - first send) -- i.e. how fast the step's
whole workload was pushed all the way to that stage:
    T1 raw       sensor MQTT publish        -> tb.telemetry.raw        (t_raw)
    T2 validated raw                          -> tb.telemetry.validated  (t_validated)
    T3 csv       validated -> exporter        -> row in local CSV        (t_csv)
    T4 ckan      CSV                           -> CKAN resource update    (t_ckan)
Because you cannot complete more messages than you sent and pushing N messages
through always takes at least as long as sending them, no curve can exceed the
offered load (no >y=x artifact). The metric needs no warm-up guard or measurement
sub-window: it spans the whole isolated step (load + drain), and cross-step
isolation is guaranteed by draining the pipeline to zero between steps.
The end-to-end T4 curve is the system's goodput; read the knee (bottleneck) off
the plotted curve by eye -- this benchmark does not compute it.
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
from pathlib import Path
from typing import Any

import run_performance_benchmark as bench

config = bench.config
exporter = bench.exporter

DEFAULT_RESULTS_DIR = bench.REPO_ROOT / "evaluation" / "results" / "thesis_throughput4"
# Slowly increasing offered load; each step is an isolated experiment.
DEFAULT_STEPS = "50,100,150,200,300,400,500,600"

# The four reported throughput curves: each is the makespan completion rate at a
# pipeline stage (stage arrivals / (last stage arrival - first send)). They map
# 1:1 onto the Event timestamps stamped by the four observers.
#   (Event attr, output column, legend label)
STAGE_CURVES: list[tuple[str, str, str]] = [
    ("t_raw", "throughput_t1_raw_msg_s", "T1 raw"),
    ("t_validated", "throughput_t2_validated_msg_s", "T2 validated"),
    ("t_csv", "throughput_t3_csv_msg_s", "T3 csv"),
    ("t_ckan", "throughput_t4_ckan_msg_s", "T4 ckan"),
]
# The end-to-end curve (t_ckan) is the whole system's goodput. Kept under the
# `throughput_msg_s` alias so output is consistent with the single-curve benchmark.
THROUGHPUT_ATTR = "t_ckan"


# --------------------------------------------------------------------------- #
# Step parsing.
# --------------------------------------------------------------------------- #
def _parse_steps(value: str) -> list[float]:
    steps: list[float] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            rate = float(token)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"step {token!r} is not a numeric msg/s rate"
            ) from None
        if rate <= 0:
            raise argparse.ArgumentTypeError("steps must be positive msg/s")
        steps.append(rate)
    if not steps:
        raise argparse.ArgumentTypeError("at least one step is required")
    return steps


# --------------------------------------------------------------------------- #
# Windowed counters (the slope of the cumulative stage-arrival curve).
# --------------------------------------------------------------------------- #
def _count_in_window(
    values: list[bench.Event], attr: str, lo: float, hi: float
) -> int:
    n = 0
    for event in values:
        stamp = getattr(event, attr)
        if stamp is not None and lo <= stamp <= hi:
            n += 1
    return n


# --------------------------------------------------------------------------- #
# Load generation for one step.
# --------------------------------------------------------------------------- #
def _produce_load(
    args: argparse.Namespace,
    rate: float,
    devices: list[dict[str, Any]],
    events: dict[tuple[str, int], bench.Event],
    lock: threading.Lock,
    rng: random.Random,
) -> tuple[float, float, int, int]:
    """Open-loop publish at `rate` msg/s for dwell_s; returns (s, e, produced, failed)."""
    seq = 0
    invalid_seq = 0
    produced = 0
    failed_sends = 0
    next_send = time.perf_counter()
    s_wall = time.time()
    deadline = time.perf_counter() + args.dwell_s
    while time.perf_counter() < deadline:
        seq += 1
        device = devices[(seq - 1) % len(devices)]
        sensor_type = device["sensor_type"]
        valid = rng.random() >= args.invalid_ratio
        variant = ""
        if not valid:
            invalid_seq += 1
            variant = bench._invalid_variant_for_seq(invalid_seq, sensor_type)
        values = bench._values_for(sensor_type, seq, valid, variant)

        send_wall = time.time()
        ts_ms = max(int(send_wall * 1000.0), device["last_ts_ms"] + 1)
        device["last_ts_ms"] = ts_ms
        day = bench._day_for_ts_ms(ts_ms)
        key = (device["name"], ts_ms)
        with lock:
            events[key] = bench.Event(
                device_name=device["name"], ts_ms=ts_ms,
                dataset_slug=device["dataset_slug"], day=day,
                expected_validated=valid, t_send=send_wall,
            )
        payload = json.dumps({"ts": ts_ms, "values": values}, ensure_ascii=False)
        info = device["client"].publish(bench.TB_TELEMETRY_TOPIC, payload)
        if info.rc != 0:
            failed_sends += 1
        else:
            produced += 1

        next_send += 1.0 / rate
        if next_send > time.perf_counter():
            bench._pace_until(next_send)
    e_wall = time.time()
    return s_wall, e_wall, produced, failed_sends


# --------------------------------------------------------------------------- #
# Drain to zero: wait until the step's load is FULLY processed end-to-end.
# --------------------------------------------------------------------------- #
def _drain_to_zero(
    args: argparse.Namespace,
    events: dict[tuple[str, int], bench.Event],
    lock: threading.Lock,
) -> tuple[bool, float]:
    """Block until the pipeline is empty for this step (or drain_timeout_s).

    "Empty" = exporter validated-topic consumer lag <= threshold AND every
    publishable (expected_validated) message produced this step has a t_ckan.
    Because the previous step was already drained, the shared exporter group's
    lag reflects only this step's residue -> no cross-step contamination.
    """
    start = time.time()
    deadline = start + args.drain_timeout_s
    last_print = 0.0
    while time.time() < deadline:
        lag = bench._exporter_lag(
            args.bootstrap_server,
            config.KAFKA_CONSUMER_GROUP_EXPORTER,
            config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        )
        with lock:
            expected = sum(1 for ev in events.values() if ev.expected_validated)
            done = sum(1 for ev in events.values() if ev.t_ckan is not None)
        lag_ok = lag is not None and lag <= args.drain_lag_threshold
        ckan_ok = done >= expected
        now = time.time()
        if now - last_print >= 15:
            print(f"    [drain] lag={lag} ckan={done}/{expected} "
                  f"({now - start:.0f}s elapsed)")
            last_print = now
        if lag_ok and ckan_ok:
            return True, round(now - start, 1)
        time.sleep(args.drain_poll_s)
    return False, round(time.time() - start, 1)


# --------------------------------------------------------------------------- #
# One isolated step: provision fresh datasets -> load -> drain -> measure.
# --------------------------------------------------------------------------- #
def run_single_step(
    args: argparse.Namespace,
    rate: float,
    index: int,
    KafkaConsumer,
) -> dict[str, Any]:
    ns = time.time_ns()
    step_test_id = f"{args.test_id}-r{int(round(rate))}-{ns}"
    print(f"--- provisioning {args.device_count} fresh devices for step "
          f"(test-id {step_test_id}) ---")
    devices = bench.provision_devices(step_test_id, args.device_count)

    events: dict[tuple[str, int], bench.Event] = {}
    lock = threading.Lock()
    stop_event = threading.Event()
    batch_latencies_s: list[float] = []
    export_dir = Path(args.export_dir)
    raw_consumer = None
    val_consumer = None
    try:
        bench.wait_for_datasets(
            devices, args.ckan_url, args.ckan_api_key, args.dataset_wait_s
        )

        # Fresh observers + fresh consumer groups for this step only.
        raw_consumer = bench._make_consumer(
            KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_RAW,
            f"eval-{step_test_id}-raw-{ns}", args.bootstrap_server,
        )
        val_consumer = bench._make_consumer(
            KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            f"eval-{step_test_id}-validated-{ns}", args.bootstrap_server,
        )
        sample_box: dict[str, Any] = {}
        threads = [
            threading.Thread(target=bench._kafka_observer, args=(
                raw_consumer, config.KAFKA_TOPIC_TELEMETRY_RAW, "raw",
                events, lock, stop_event, sample_box), daemon=True),
            threading.Thread(target=bench._kafka_observer, args=(
                val_consumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED, "validated",
                events, lock, stop_event, sample_box), daemon=True),
            threading.Thread(target=bench._csv_tailer, args=(
                export_dir, events, lock, stop_event, args.csv_scan_interval_s),
                daemon=True),
            threading.Thread(target=bench._ckan_poller, args=(
                args.ckan_url, args.ckan_api_key, export_dir, events, lock,
                stop_event, args.ckan_poll_interval_s, batch_latencies_s),
                daemon=True),
        ]
        for thread in threads:
            thread.start()

        rng = random.Random(args.seed + index)
        s_wall, e_wall, produced, failed_sends = _produce_load(
            args, rate, devices, events, lock, rng
        )
        print(f"    produced {produced} msgs in {e_wall - s_wall:.0f}s; "
              f"draining to zero (timeout {args.drain_timeout_s:g}s) ...")
        drained, drain_s = _drain_to_zero(args, events, lock)
        if drained:
            print(f"    [drain] complete in {drain_s:g}s.")
        else:
            print(f"    [drain] TIMEOUT after {drain_s:g}s (system could not "
                  f"fully drain this offered load).")

        stop_event.set()
        for thread in threads:
            thread.join(timeout=10)

        return _measure_step(
            args=args, events=events, rate=rate,
            s=s_wall, e=e_wall, produced=produced, failed_sends=failed_sends,
            drained=drained, drain_s=drain_s,
        )
    finally:
        for consumer in (raw_consumer, val_consumer):
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        for device in devices:
            try:
                device["client"].loop_stop()
                device["client"].disconnect()
            except Exception:
                pass


def _measure_step(
    *,
    args: argparse.Namespace,
    events: dict[tuple[str, int], bench.Event],
    rate: float,
    s: float,
    e: float,
    produced: int,
    failed_sends: int,
    drained: bool,
    drain_s: float,
) -> dict[str, Any]:
    """Compute one row of per-stage makespan throughput for the whole step.

    Each stage's throughput is the conservation-respecting makespan rate:
        tput_X = (messages that reached stage X) / (last completion at X - first send)
    i.e. how fast the step's entire workload was pushed all the way to stage X.
    Because you cannot complete more messages than you sent, and pushing N
    messages through always takes at least as long as sending them, this can never
    exceed the offered load (no >y=x artifact). It spans the whole isolated step
    -- ramp-up, steady state AND drain -- so no warm-up guard or measurement
    sub-window is needed; cross-step isolation is guaranteed by draining the
    pipeline to zero between steps.
    """
    values = list(events.values())
    produce_len = max(e - s, 1e-6)

    produced_in_step = _count_in_window(values, "t_send", s, e)
    input_rate = round(produced_in_step / produce_len, 3)

    # Per-stage makespan throughput = (messages that reached the stage) /
    # (last completion at that stage - first send across the step).
    first_send = min((ev.t_send for ev in values if ev.t_send is not None), default=s)
    stage_tput: dict[str, float] = {}
    stage_count: dict[str, int] = {}
    stage_span: dict[str, float] = {}
    for attr, col, _label in STAGE_CURVES:
        stamps = [getattr(ev, attr) for ev in values if getattr(ev, attr) is not None]
        span = max((max(stamps) - first_send) if stamps else 0.0, 1e-6)
        stage_count[col] = len(stamps)
        stage_span[col] = span
        stage_tput[col] = round(len(stamps) / span, 3) if stamps else 0.0
    throughput = stage_tput["throughput_t4_ckan_msg_s"]

    publishable = input_rate * (1.0 - args.invalid_ratio)
    delivery_ratio = round(throughput / publishable, 4) if publishable > 0 else None

    e2e = [
        ev.t_ckan - ev.t_send
        for ev in values
        if ev.t_ckan is not None and s <= ev.t_send <= e
    ]
    e2e_stats = bench._stage_stats(e2e)

    return {
        "step": 0,  # filled in by the caller (1-based)
        "offered_load_msg_s": rate,
        "input_rate_msg_s": input_rate,
        "generator_sustained": input_rate >= args.target_achieved_threshold * rate,
        "throughput_t1_raw_msg_s": stage_tput["throughput_t1_raw_msg_s"],
        "throughput_t2_validated_msg_s": stage_tput["throughput_t2_validated_msg_s"],
        "throughput_t3_csv_msg_s": stage_tput["throughput_t3_csv_msg_s"],
        "throughput_t4_ckan_msg_s": stage_tput["throughput_t4_ckan_msg_s"],
        "throughput_msg_s": throughput,
        "delivery_ratio": delivery_ratio,
        "e2e_p50_ms": e2e_stats["p50_ms"],
        "e2e_p95_ms": e2e_stats["p95_ms"],
        "e2e_count": e2e_stats["count"],
        "produced_in_step": produced_in_step,
        "ckan_count": stage_count["throughput_t4_ckan_msg_s"],
        "drained": drained,
        "drain_s": drain_s,
        "t4_makespan_s": round(stage_span["throughput_t4_ckan_msg_s"], 3),
    }


# --------------------------------------------------------------------------- #
# Drive every isolated step and assemble the per-stage throughput curve.
# --------------------------------------------------------------------------- #
def run_steps(args: argparse.Namespace, KafkaConsumer) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    total_produced = 0
    total_failed = 0
    print(f"[steps] {len(args.steps)} isolated steps x {args.dwell_s:g}s dwell, "
          f"{args.device_count} fresh devices each (makespan metric, no guard).")
    for index, rate in enumerate(args.steps):
        print(f"\n=== step {index + 1}/{len(args.steps)}: offered={rate:g} msg/s "
              f"(brand-new datasets) ===")
        row = run_single_step(args, rate, index, KafkaConsumer)
        row["step"] = index + 1
        total_produced += row["produced_in_step"]
        rows.append(row)
        # A step that could not drain leaves backlog in the SHARED exporter group;
        # the next fresh-dataset step would queue behind it (contamination). That
        # first non-draining step is where the system's capacity is exceeded, so stop.
        if not row["drained"] and args.stop_after_crash:
            print(f"[steps] step {index + 1} did not drain within "
                  f"{args.drain_timeout_s:g}s -> capacity exceeded; stopping the "
                  f"ramp here (later steps would be contaminated by its backlog).")
            break

    return {
        "rows": rows,
        "totals": {
            "produced": total_produced,
            "failed_sends": total_failed,
            "max_throughput_msg_s": max((r["throughput_msg_s"] for r in rows), default=0.0),
            "all_steps_drained": all(r["drained"] for r in rows),
        },
    }


# --------------------------------------------------------------------------- #
# Output.
# --------------------------------------------------------------------------- #
def write_outputs(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    rows = summary["rows"]
    if not rows:
        return
    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "throughput_ramp4_summary.csv"
    json_path = args.results_dir / "throughput_ramp4_summary.json"

    bench._write_csv(csv_path, list(rows[0].keys()), rows)
    print(f"Wrote {csv_path}")

    doc = {
        "generated_at": bench._now_iso(),
        "bootstrap_server": args.bootstrap_server,
        "raw_topic": config.KAFKA_TOPIC_TELEMETRY_RAW,
        "validated_topic": config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
        "ckan_url": args.ckan_url,
        "export_dir": args.export_dir,
        "device_count": args.device_count,
        "steps_msg_s": args.steps,
        "dwell_s": args.dwell_s,
        "drain_timeout_s": args.drain_timeout_s,
        "invalid_ratio": args.invalid_ratio,
        "seed": args.seed,
        "csv_scan_interval_s": args.csv_scan_interval_s,
        "ckan_poll_interval_s": args.ckan_poll_interval_s,
        "methodology": {
            "isolation": "each offered-load step provisions brand-new devices "
                         "(=> brand-new CKAN datasets); after load it drains the "
                         "pipeline to ZERO before the next step. No backlog, CSV, "
                         "or dataset is shared between steps.",
            "drain_rule": "exporter validated-topic lag <= drain_lag_threshold AND "
                          "every produced message committed to CKAN (t_ckan), or "
                          "drain_timeout_s.",
        },
        "throughput_definition": {
            "metric": "per-stage throughput = makespan completion rate = (messages "
                      "that reached the stage) / (last completion at the stage - "
                      "first send). Conservation-respecting: cannot exceed the "
                      "offered load. Spans the whole isolated step (load + drain); "
                      "cross-step isolation comes from draining to zero between steps.",
            "stages": {
                "T1 raw": "device MQTT publish -> tb.telemetry.raw (t_raw)",
                "T2 validated": "raw -> ETL -> tb.telemetry.validated (t_validated)",
                "T3 csv": "validated -> exporter -> row in local CSV (t_csv)",
                "T4 ckan": "CSV -> CKAN resource update (t_ckan, end-to-end goodput)",
            },
        },
        "totals": summary["totals"],
        "runs": rows,
    }
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"Wrote {json_path}")

    _print_table(summary)


def _print_table(summary: dict[str, Any]) -> None:
    totals = summary["totals"]
    print("\noffered  input    T1raw   T2val   T3csv  T4ckan  deliv  e2e_p95  drain  flags")
    for row in summary["rows"]:
        flags = []
        if not row["generator_sustained"]:
            flags.append("gen_limited")
        print(
            f"{row['offered_load_msg_s']:7g}  "
            f"{row['input_rate_msg_s']:6.1f}  "
            f"{row['throughput_t1_raw_msg_s']:6.1f}  "
            f"{row['throughput_t2_validated_msg_s']:6.1f}  "
            f"{row['throughput_t3_csv_msg_s']:6.1f}  "
            f"{row['throughput_t4_ckan_msg_s']:6.1f}  "
            f"{(row['delivery_ratio'] if row['delivery_ratio'] is not None else 0):5.2f}  "
            f"{(row['e2e_p95_ms'] if row['e2e_p95_ms'] is not None else float('nan')):8.0f}  "
            f"{('Y' if row['drained'] else 'N'):>5}  "
            f"{' '.join(flags)}"
        )
    print(
        f"\n[summary] max end-to-end throughput="
        f"{totals['max_throughput_msg_s']} msg/s; "
        f"all_steps_drained={totals['all_steps_drained']}. "
        f"(Read the knee/bottleneck off the plot by eye.)"
    )


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        description="FAIR Bridge 4-stage throughput benchmark (isolated steps)."
    )
    parser.add_argument("--steps", type=_parse_steps, default=_parse_steps(DEFAULT_STEPS),
                        help="Comma-separated offered loads in msg/s, ascending.")
    parser.add_argument("--dwell-s", type=float, default=360.0,
                        help="Seconds of load applied per step. Should span many "
                             "exporter flush cycles (30s each) so T4's makespan rate "
                             "is stable (calibrated at 20 devices). No warm-up guard "
                             "is needed: the makespan metric covers the whole step "
                             "(load + drain) and isolation comes from the drain to "
                             "zero between steps.")
    parser.add_argument("--device-count", type=int, default=20,
                        help="Fresh devices/datasets provisioned per step.")
    parser.add_argument("--invalid-ratio", type=float, default=0.0,
                        help="Fraction of invalid (DLQ-bound) messages. Default 0 "
                             "(pure valid) so offered load == expected goodput.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--test-id", default="thesis-throughput4")
    parser.add_argument("--bootstrap-server", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--ckan-url", default=config.CKAN_URL)
    parser.add_argument("--ckan-api-key", default=config.CKAN_API_KEY)
    parser.add_argument("--export-dir", default=config.EXPORT_DIR)
    parser.add_argument("--csv-scan-interval-s", type=float, default=0.5)
    parser.add_argument("--ckan-poll-interval-s", type=float, default=3.0)
    parser.add_argument("--dataset-wait-s", type=float, default=120.0)
    # Drain-to-zero (isolation between steps).
    parser.add_argument("--drain-timeout-s", type=float, default=900.0,
                        help="Max seconds to wait for a step to fully drain before "
                             "giving up (recorded as drained=false).")
    parser.add_argument(
        "--stop-after-crash", action=argparse.BooleanOptionalAction, default=True,
        help="Stop the ramp at the first step that fails to drain within "
             "drain_timeout_s. Its undrained backlog sits in the shared exporter "
             "group and would contaminate later fresh-dataset steps.")
    parser.add_argument("--drain-poll-s", type=float, default=5.0,
                        help="How often to check drain progress.")
    parser.add_argument("--drain-lag-threshold", type=int, default=0,
                        help="Exporter validated-topic lag at/below which the step "
                             "is considered drained (0 = truly empty).")
    parser.add_argument("--target-achieved-threshold", type=float, default=0.9,
                        help="Min input/offered ratio per step before flagging "
                             "generator_sustained=false (informational).")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Provision devices, confirm TB->raw forwarding, print a sample, exit.",
    )
    args = parser.parse_args()

    if not 0.0 <= args.invalid_ratio <= 1.0:
        raise SystemExit("--invalid-ratio must be between 0.0 and 1.0")
    if args.steps != sorted(args.steps):
        print("[warn] steps are not ascending; the ramp assumes increasing "
              "offered load.")
    args.results_dir.mkdir(parents=True, exist_ok=True)

    KafkaConsumer = bench._import_kafka()

    if args.verify_only:
        devices = bench.provision_devices(f"{args.test_id}-verify", args.device_count)
        try:
            bench.verify_forwarding(args, devices, KafkaConsumer)
        finally:
            for device in devices:
                try:
                    device["client"].loop_stop()
                    device["client"].disconnect()
                except Exception:
                    pass
        return 0

    summary = run_steps(args, KafkaConsumer)
    write_outputs(args, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
