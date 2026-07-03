#!/usr/bin/env python3
"""FAIR Bridge capacity benchmark: find the true throughput ceiling C*.

This is the multi-threaded sibling of `throughput_four.py`. The 4-stage benchmark
there is single-threaded in its load generator (`_produce_load`), which caps the
*actual injected rate* at ~735 msg/s on this machine -- below the pipeline's own
capacity C. As a result its curve never reaches saturation and shows no knee: it
measures pipeline EFFICIENCY (delivery ratio of whatever was injected), not the
system LIMIT.

To locate C* we must drive the *actual injected rate* PAST C and watch the T4
goodput plateau. The only change needed is the load generator: this file provides
a MULTI-THREADED `_produce_load_mt` that partitions the devices across N worker
threads (each device already owns its own paho client / network thread, so the
partition is contention-free) and paces each worker at rate/N. Everything else --
fresh-dataset isolation, the four observer threads, drain-to-zero, and the
makespan metric -- is REUSED UNCHANGED from `throughput_four.py` / the bench
module by importing them; nothing in those files is edited. We install the
multi-threaded generator by swapping the module-local name at import time:

    import throughput_four as t4
    t4._produce_load = _produce_load_mt   # run-time swap, no file edit

Per step we therefore record the three quantities needed to read AND validate the
ceiling (all already produced by `t4._measure_step`):
  * offered_load_msg_s   -- commanded rate (control variable)
  * input_rate_msg_s     -- ACTUAL injected rate (the real load = plot x-axis)
  * throughput_t4_ckan_msg_s -- T4 goodput (plot y-axis; its plateau = C*)
plus the validity discriminator:
  * generator_sustained  -- did the generator actually emit the offered load?
  * drained              -- did the pipeline fully drain this step?

Read C* where T4 stops following the diagonal and plateaus, USING ONLY points
with generator_sustained=true (input ~= offered). If the plateau coincides with
generator_sustained=false, you hit a NEW generator ceiling, not the pipeline --
raise --producer-threads (or cut per-message CPU) and re-probe.

Because points past C* do NOT drain, run them INDIVIDUALLY with --append and
--no-stop-after-crash, cleaning the backlog between runs (this script removes its
own export dirs via --cleanup-exports; the operator resets the exporter consumer
group + restarts the exporter between isolated high-load runs).
"""

from __future__ import annotations

import argparse
import csv as _csv
import json
import multiprocessing as mp
import random
import shutil
import threading
import time
from pathlib import Path
from typing import Any

import run_performance_benchmark as bench
import throughput_four as t4

config = bench.config

DEFAULT_RESULTS_DIR = bench.REPO_ROOT / "evaluation" / "results" / "thesis_capacity"
# Push the offered load well past the single-thread ceiling (~735) so the knee
# becomes reachable. Phase A (these) should still drain; map the plateau (Phase B)
# by running higher loads individually with --append.
DEFAULT_STEPS = "200,400,600,800,1000,1200"

# Pre-serialized valid-payload value JSON, indexed by seq % _VALUES_PERIOD, to
# keep json.dumps out of the hot send loop (the single-process generator's main
# per-message CPU cost under the GIL). bench._valid_values is periodic; the LCM of
# its field periods (100, 50, 20, 300, 4) is 300, so 300 entries reproduce its
# output EXACTLY -- the fast path is byte-identical to json.dumps of the full dict.
_VALUES_PERIOD = 300
_VALUES_JSON_CACHE: dict[str, list[str]] = {}


def _values_json_cache() -> dict[str, list[str]]:
    if not _VALUES_JSON_CACHE:
        for sensor_type in bench.SENSOR_TYPES:
            _VALUES_JSON_CACHE[sensor_type] = [
                json.dumps(bench._valid_values(sensor_type, i), ensure_ascii=False)
                for i in range(_VALUES_PERIOD)
            ]
    return _VALUES_JSON_CACHE


# --------------------------------------------------------------------------- #
# Multi-threaded load generation (the ONLY new logic).
# --------------------------------------------------------------------------- #
def _produce_load_mt(
    args: argparse.Namespace,
    rate: float,
    devices: list[dict[str, Any]],
    events: dict[tuple[str, int], bench.Event],
    lock: threading.Lock,
    rng: random.Random,
) -> tuple[float, float, int, int]:
    """Open-loop publish at `rate` msg/s for dwell_s, spread over N worker threads.

    Signature is identical to throughput_four._produce_load so it can replace it.
    Devices are partitioned round-robin into `args.producer_threads` DISJOINT
    groups; each worker publishes only to its own devices' paho clients and paces
    at rate/N. Every message is inserted into the shared `events` dict (under the
    shared `lock`) BEFORE publish -- the observers match arrivals back by
    (device_name, ts_ms), so the Event must pre-exist. Returns the aggregate
    (first send, last send, produced, failed) across all workers.
    """
    n_threads = max(1, min(int(getattr(args, "producer_threads", 1)), len(devices)))
    per_rate = rate / n_threads
    groups = [devices[i::n_threads] for i in range(n_threads)]
    vcache = _values_json_cache()  # build once (no thread race) before workers start
    # Derive independent per-worker RNG seeds from the step's rng so different
    # steps/reps still get different (but reproducible) value streams.
    worker_seeds = [rng.randrange(1 << 30) for _ in range(n_threads)]

    results: list[tuple[float, float, int, int] | None] = [None] * n_threads
    start_barrier = threading.Barrier(n_threads)

    def worker(widx: int, my_devices: list[dict[str, Any]]) -> None:
        wrng = random.Random(worker_seeds[widx])
        seq = 0
        invalid_seq = 0
        produced = 0
        failed_sends = 0
        start_barrier.wait()  # release all workers together for a clean combined rate
        next_send = time.perf_counter()
        s_wall = time.time()
        deadline = time.perf_counter() + args.dwell_s
        while time.perf_counter() < deadline:
            seq += 1
            device = my_devices[(seq - 1) % len(my_devices)]
            sensor_type = device["sensor_type"]
            valid = wrng.random() >= args.invalid_ratio

            send_wall = time.time()
            ts_ms = max(int(send_wall * 1000.0), device["last_ts_ms"] + 1)
            device["last_ts_ms"] = ts_ms  # device owned by exactly this worker -> no race
            day = bench._day_for_ts_ms(ts_ms)
            key = (device["name"], ts_ms)
            with lock:
                events[key] = bench.Event(
                    device_name=device["name"], ts_ms=ts_ms,
                    dataset_slug=device["dataset_slug"], day=day,
                    expected_validated=valid, t_send=send_wall,
                )
            if valid:
                # Fast path: splice ts into a pre-serialized value JSON; no
                # per-message json.dumps. Byte-identical to dumping the full dict.
                payload = '{"ts": %d, "values": %s}' % (
                    ts_ms, vcache[sensor_type][seq % _VALUES_PERIOD])
            else:
                invalid_seq += 1
                variant = bench._invalid_variant_for_seq(invalid_seq, sensor_type)
                payload = json.dumps(
                    {"ts": ts_ms,
                     "values": bench._values_for(sensor_type, seq, False, variant)},
                    ensure_ascii=False)
            info = device["client"].publish(bench.TB_TELEMETRY_TOPIC, payload)
            if info.rc != 0:
                failed_sends += 1
            else:
                produced += 1

            next_send += 1.0 / per_rate
            if next_send > time.perf_counter():
                bench._pace_until(next_send)
        results[widx] = (s_wall, time.time(), produced, failed_sends)

    threads = [
        threading.Thread(target=worker, args=(i, groups[i]), daemon=True)
        for i in range(n_threads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    done = [r for r in results if r is not None]
    starts = [r[0] for r in done]
    ends = [r[1] for r in done]
    produced = sum(r[2] for r in done)
    failed = sum(r[3] for r in done)
    return min(starts), max(ends), produced, failed


# Install the multi-threaded generator into throughput_four's per-step machinery.
# t4.run_single_step / _measure_step / _drain_to_zero are reused unchanged; only
# the generator name is rebound (no edit to the throughput_four.py file on disk).
t4._produce_load = _produce_load_mt


# --------------------------------------------------------------------------- #
# Multi-PROCESS generator (lever 3): each producer process has its own GIL, so
# the aggregate injected rate scales past the single-process ceiling (~1958 msg/s
# here). Producers do NOT bookkeep events -- the main process reconstructs every
# Event from the raw Kafka stream (each message carries its own send ms in "ts"),
# so there is no cross-process event IPC. This path is opt-in via --producer-procs
# > 1 and assumes invalid_ratio == 0 (pure valid, the capacity-mode default).
# --------------------------------------------------------------------------- #
def _blast(devices: list[dict[str, Any]], per_rate: float, dwell_s: float,
           vcache: dict[str, list[str]], n_threads: int) -> int:
    """Publish fast-payload at `per_rate` msg/s for dwell_s over `devices`, split
    across n_threads. No Event/lock bookkeeping (the main process's observers
    rebuild events from the stream), so this loop is leaner than the in-process
    generator. Returns total produced."""
    n_threads = max(1, min(n_threads, len(devices)))
    groups = [devices[i::n_threads] for i in range(n_threads)]
    sub_rate = per_rate / n_threads
    counts = [0] * n_threads

    def worker(widx: int, my_devices: list[dict[str, Any]]) -> None:
        seq = 0
        produced = 0
        next_send = time.perf_counter()
        deadline = next_send + dwell_s
        while time.perf_counter() < deadline:
            seq += 1
            device = my_devices[(seq - 1) % len(my_devices)]
            send_wall = time.time()
            ts_ms = max(int(send_wall * 1000.0), device["last_ts_ms"] + 1)
            device["last_ts_ms"] = ts_ms
            payload = '{"ts": %d, "values": %s}' % (
                ts_ms, vcache[device["sensor_type"]][seq % _VALUES_PERIOD])
            if device["client"].publish(bench.TB_TELEMETRY_TOPIC, payload).rc == 0:
                produced += 1
            next_send += 1.0 / sub_rate
            if next_send > time.perf_counter():
                bench._pace_until(next_send)
        counts[widx] = produced

    threads = [threading.Thread(target=worker, args=(i, groups[i]), daemon=True)
               for i in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return sum(counts)


def _producer_process(pidx: int, nprocs: int, n_threads: int, rate: float,
                      step_test_id: str, device_count: int, dwell_s: float,
                      ckan_url: str, ckan_api_key: str, dataset_wait_s: float,
                      produced_val, barrier) -> None:
    """Child process: provision its disjoint device subset (own paho clients),
    wait for its CKAN datasets, sync on the barrier, blast fast-payload at
    rate/nprocs, report produced. Spawned (fresh interpreter), so it owns its GIL.
    """
    my_indices = list(range(pidx + 1, device_count + 1, nprocs))
    devices: list[dict[str, Any]] = []
    for index in my_indices:
        sensor_type = bench._sensor_type_for_device(index)
        name = bench._benchmark_device_name(sensor_type, step_test_id, index)
        _device_id, token = bench.tb_client.ensure_device(name, label=name)
        devices.append({
            "name": name, "sensor_type": sensor_type,
            "client": bench.tb_client.mqtt_client(token),
            "dataset_slug": bench.exporter.dataset_slug_for_device(name),
            "last_ts_ms": 0,
        })
    bench.wait_for_datasets(devices, ckan_url, ckan_api_key, dataset_wait_s)
    vcache = _values_json_cache()
    try:
        barrier.wait()  # release together with siblings + main
    except threading.BrokenBarrierError:
        return
    produced = _blast(devices, rate / nprocs, dwell_s, vcache, n_threads)
    with produced_val.get_lock():
        produced_val.value += produced
    time.sleep(2)  # let paho network threads flush QoS-0 buffers before disconnect
    for device in devices:
        try:
            device["client"].loop_stop()
            device["client"].disconnect()
        except Exception:
            pass


def _raw_observer_upsert(consumer, events: dict[tuple[str, int], "bench.Event"],
                         lock: threading.Lock, stop_event: threading.Event) -> None:
    """Raw-stage observer that UPSERTS: multi-process producers do not pre-insert
    events, so this creates the Event on first sighting of a raw message. t_send is
    the message's own ts (= producer send wall-clock ms); t_raw = observation time;
    expected_validated=True (multi-process capacity mode is pure-valid). Downstream
    observers (validated / csv / ckan) then find the event, because raw is the first
    pipeline stage and always arrives first."""
    while not stop_event.is_set():
        polled = consumer.poll(timeout_ms=200, max_records=2000)
        if not polled:
            continue
        seen = time.time()
        with lock:
            for batch in polled.values():
                for record in batch:
                    value = record.value or {}
                    name, ts_ms = bench._name_and_ts(value, bench._decode_headers(record))
                    if ts_ms is None or not name:
                        continue
                    key = (name, ts_ms)
                    event = events.get(key)
                    if event is None:
                        event = bench.Event(
                            device_name=name, ts_ms=ts_ms,
                            dataset_slug=bench.exporter.dataset_slug_for_device(name),
                            day=bench._day_for_ts_ms(ts_ms),
                            expected_validated=True, t_send=ts_ms / 1000.0,
                        )
                        events[key] = event
                    if event.t_raw is None:
                        event.t_raw = seen


def run_single_step_mp(args: argparse.Namespace, rate: float, index: int,
                       KafkaConsumer) -> dict[str, Any]:
    """One isolated step driven by --producer-procs child processes. Observers run
    in the main process; the raw observer upserts, so no per-message cross-process
    IPC is needed. Returns the same row schema as t4._measure_step."""
    ns = time.time_ns()
    step_test_id = f"{args.test_id}-r{int(round(rate))}-{ns}"
    nprocs = args.producer_procs
    print(f"--- [mp] provisioning {args.device_count} devices across {nprocs} "
          f"producer processes x {args.producer_threads} threads (test-id "
          f"{step_test_id}) ---")
    events: dict[tuple[str, int], bench.Event] = {}
    lock = threading.Lock()
    stop_event = threading.Event()
    export_dir = Path(args.export_dir)
    ctx = mp.get_context("spawn")
    produced_val = ctx.Value("l", 0)
    barrier = ctx.Barrier(nprocs + 1)
    procs: list[Any] = []
    raw_consumer = None
    val_consumer = None
    threads: list[threading.Thread] = []
    try:
        for pidx in range(nprocs):
            proc = ctx.Process(target=_producer_process, args=(
                pidx, nprocs, args.producer_threads, rate, step_test_id,
                args.device_count, args.dwell_s, args.ckan_url, args.ckan_api_key,
                args.dataset_wait_s, produced_val, barrier))
            proc.start()
            procs.append(proc)

        # Observers must be assigned (auto_offset_reset=latest) BEFORE load starts.
        raw_consumer = bench._make_consumer(
            KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_RAW,
            f"eval-{step_test_id}-raw-{ns}", args.bootstrap_server)
        val_consumer = bench._make_consumer(
            KafkaConsumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            f"eval-{step_test_id}-validated-{ns}", args.bootstrap_server)
        threads = [
            threading.Thread(target=_raw_observer_upsert, args=(
                raw_consumer, events, lock, stop_event), daemon=True),
            threading.Thread(target=bench._kafka_observer, args=(
                val_consumer, config.KAFKA_TOPIC_TELEMETRY_VALIDATED, "validated",
                events, lock, stop_event, {}), daemon=True),
            threading.Thread(target=bench._csv_tailer, args=(
                export_dir, events, lock, stop_event, args.csv_scan_interval_s),
                daemon=True),
            threading.Thread(target=bench._ckan_poller, args=(
                args.ckan_url, args.ckan_api_key, export_dir, events, lock,
                stop_event, args.ckan_poll_interval_s, []), daemon=True),
        ]
        for thread in threads:
            thread.start()

        print(f"    [mp] waiting for {nprocs} producers to provision + sync ...")
        try:
            barrier.wait(timeout=args.dataset_wait_s + 180)
        except threading.BrokenBarrierError:
            print("    [mp] ERROR: producers did not all reach the start barrier "
                  "(a child likely failed to provision); aborting this step.")
            raise
        load_start = time.time()
        print(f"    [mp] load started: offered={rate:g} msg/s, dwell {args.dwell_s:g}s ...")
        for proc in procs:
            proc.join()
        produced = produced_val.value
        print(f"    [mp] produced {produced} msgs; draining to zero "
              f"(timeout {args.drain_timeout_s:g}s) ...")
        drained, drain_s = t4._drain_to_zero(args, events, lock)
        if drained:
            print(f"    [drain] complete in {drain_s:g}s.")
        else:
            print(f"    [drain] TIMEOUT after {drain_s:g}s (could not fully drain).")

        stop_event.set()
        for thread in threads:
            thread.join(timeout=10)

        # Achieved-input window = makespan of the producers' ACTUAL sends (from the
        # upserted events' t_send), so input_rate is the true injected rate, free of
        # spawn/barrier startup skew. Falls back to the nominal window if empty.
        sends = [ev.t_send for ev in events.values() if ev.t_send is not None]
        s_wall = min(sends) if sends else load_start
        e_wall = max(sends) if sends else load_start + args.dwell_s

        row = t4._measure_step(
            args=args, events=events, rate=rate, s=s_wall, e=e_wall,
            produced=produced, failed_sends=0, drained=drained, drain_s=drain_s)
        row["producer_procs"] = nprocs
        return row
    finally:
        stop_event.set()
        for consumer in (raw_consumer, val_consumer):
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        for proc in procs:
            if proc.is_alive():
                proc.terminate()


# --------------------------------------------------------------------------- #
# Backlog cleanup (CSV/export-dir side; the Kafka-group reset is operator-side).
# --------------------------------------------------------------------------- #
def _cleanup_step_exports(args: argparse.Namespace) -> None:
    """Remove this benchmark's export sub-dirs so a non-draining step's CSV
    residue does not pile up across a long capacity sweep. Scoped to this run's
    test-id (we created them). Drained steps' dirs are safe to drop too -- their
    rows are already in CKAN.
    """
    export_dir = Path(args.export_dir)
    removed = 0
    for path in export_dir.glob(f"*{args.test_id}*"):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed += 1
        except OSError:
            pass
    if removed:
        print(f"    [cleanup] removed {removed} export entries matching *{args.test_id}*")


# --------------------------------------------------------------------------- #
# Outer loop: probe each offered load; map the plateau past the knee.
# --------------------------------------------------------------------------- #
def run_steps_capacity(args: argparse.Namespace, KafkaConsumer) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    total_produced = 0
    reps = args.repetitions
    mode = (f"{args.producer_procs} procs x {args.producer_threads} threads"
            if args.producer_procs > 1 else f"{args.producer_threads} threads (1 proc)")
    print(f"[capacity] {len(args.steps)} offered loads x {reps} reps x {args.dwell_s:g}s "
          f"dwell, {args.device_count} fresh devices each, generator={mode}. "
          f"stop_after_crash={args.stop_after_crash}, append={args.append}, "
          f"cleanup_exports={args.cleanup_exports}.")
    stop = False
    for index, rate in enumerate(args.steps):
        for rep in range(1, reps + 1):
            print(f"\n=== step {index + 1}/{len(args.steps)} rep {rep}/{reps}: "
                  f"offered={rate:g} msg/s ({mode}) ===")
            if args.producer_procs > 1:
                row = run_single_step_mp(args, rate, index * reps + (rep - 1), KafkaConsumer)
            else:
                row = t4.run_single_step(args, rate, index * reps + (rep - 1), KafkaConsumer)
            row["step"] = index + 1
            row["rep"] = rep
            row["producer_threads"] = args.producer_threads
            total_produced += row["produced_in_step"]
            rows.append(row)
            if args.cleanup_exports:
                _cleanup_step_exports(args)
            if not row["drained"] and args.stop_after_crash:
                print(f"[capacity] offered={rate:g} rep {rep} did not drain within "
                      f"{args.drain_timeout_s:g}s -> capacity exceeded; stopping ramp.")
                stop = True
                break
        if stop:
            break

    return {
        "rows": rows,
        "totals": {
            "produced": total_produced,
            "failed_sends": 0,
            "max_throughput_msg_s": max((r["throughput_msg_s"] for r in rows), default=0.0),
            "all_steps_drained": all(r["drained"] for r in rows),
        },
    }


def write_outputs_capacity(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    """Write/append the summary. In --append mode, append the new rows to the
    existing combined CSV (no header) so low-load ramps and individually-run
    high-load points accumulate into one curve; otherwise delegate to
    t4.write_outputs (fresh CSV + JSON + table)."""
    rows = summary["rows"]
    if not rows:
        return
    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "throughput_ramp4_summary.csv"
    if args.append and csv_path.exists():
        fieldnames = list(rows[0].keys())
        with csv_path.open("a", newline="") as fh:
            writer = _csv.DictWriter(fh, fieldnames=fieldnames)
            for row in rows:
                writer.writerow(row)
        print(f"Appended {len(rows)} rows to {csv_path}")
        t4._print_table(summary)
    else:
        t4.write_outputs(args, summary)


# --------------------------------------------------------------------------- #
# CLI (mirrors throughput_four.main with capacity-mode defaults + extra flags).
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        description="FAIR Bridge capacity benchmark (multi-threaded generator, find C*)."
    )
    parser.add_argument("--steps", type=t4._parse_steps,
                        default=t4._parse_steps(DEFAULT_STEPS),
                        help="Comma-separated offered loads in msg/s, ascending.")
    parser.add_argument("--producer-threads", type=int, default=4,
                        help="Load-generator worker threads per process (devices "
                             "partitioned across them). With --producer-procs 1 this "
                             "is the single-process generator; peak ~1958 msg/s (GIL).")
    parser.add_argument("--producer-procs", type=int, default=1,
                        help="Number of producer PROCESSES (each its own GIL). >1 "
                             "enables the multi-process generator that scales past "
                             "the single-process ceiling; each process self-provisions "
                             "device_count/procs devices and the main process measures "
                             "from the Kafka stream. Requires --invalid-ratio 0.")
    parser.add_argument("--dwell-s", type=float, default=360.0,
                        help="Seconds of load applied per step (many 30s flush cycles).")
    parser.add_argument("--device-count", type=int, default=20,
                        help="Fresh devices/datasets per rep (also the units the "
                             "producer threads partition).")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--invalid-ratio", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--test-id", default="thesis-capacity")
    parser.add_argument("--bootstrap-server", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--ckan-url", default=config.CKAN_URL)
    parser.add_argument("--ckan-api-key", default=config.CKAN_API_KEY)
    parser.add_argument("--export-dir", default=config.EXPORT_DIR)
    parser.add_argument("--csv-scan-interval-s", type=float, default=0.5)
    parser.add_argument("--ckan-poll-interval-s", type=float, default=3.0)
    parser.add_argument("--dataset-wait-s", type=float, default=120.0)
    parser.add_argument("--drain-timeout-s", type=float, default=900.0)
    # Capacity mode: probe PAST the knee by default (do not stop at first non-drain).
    parser.add_argument(
        "--stop-after-crash", action=argparse.BooleanOptionalAction, default=False,
        help="Stop the ramp at the first step that fails to drain. Default False "
             "for capacity mode (we want to map the plateau past the knee).")
    parser.add_argument("--drain-poll-s", type=float, default=5.0)
    parser.add_argument("--drain-lag-threshold", type=int, default=0)
    parser.add_argument("--target-achieved-threshold", type=float, default=0.9,
                        help="Min input/offered ratio before flagging "
                             "generator_sustained=false (the validity discriminator).")
    parser.add_argument("--append", action="store_true",
                        help="Append rows to an existing combined summary CSV "
                             "(for running high-load points individually).")
    parser.add_argument("--cleanup-exports", action="store_true",
                        help="After each step, delete this run's export sub-dirs "
                             "(test-id scoped) so non-draining backlog does not pile up.")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Provision devices, confirm TB->raw forwarding, print a sample, exit.",
    )
    args = parser.parse_args()

    if not 0.0 <= args.invalid_ratio <= 1.0:
        raise SystemExit("--invalid-ratio must be between 0.0 and 1.0")
    if args.repetitions < 1:
        raise SystemExit("--repetitions must be >= 1")
    if args.producer_threads < 1:
        raise SystemExit("--producer-threads must be >= 1")
    if args.producer_procs < 1:
        raise SystemExit("--producer-procs must be >= 1")
    if args.producer_procs > 1 and args.invalid_ratio != 0.0:
        raise SystemExit("--producer-procs > 1 (multi-process) requires "
                         "--invalid-ratio 0 (the upserting observer assumes pure-valid)")
    if args.producer_procs > args.device_count:
        raise SystemExit("--producer-procs must be <= --device-count "
                         "(each process needs at least one device)")
    if args.steps != sorted(args.steps):
        print("[warn] steps are not ascending; the ramp assumes increasing offered load.")
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

    summary = run_steps_capacity(args, KafkaConsumer)
    write_outputs_capacity(args, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
