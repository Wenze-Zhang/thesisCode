# Performance Evaluation — Operation Steps and Commands

This document summarizes every step and command used to run the full
performance evaluation of FAIR Bridge. For the design rationale, metric
definitions, and result interpretation, see
[docs/performance_evaluation.md](../docs/performance_evaluation.md).

## Prerequisites

- The full compose stack is running (`docker compose up -d`), in particular
  `kafka`, `fair-bridge-etl`, and their dependencies are healthy
  (`docker ps` shows `thesiscode-kafka-1` as healthy).
- The benchmark must run **inside the compose network**: the Kafka broker only
  advertises the container address `kafka:9092`, so a host-side producer
  cannot connect. All benchmark commands below therefore run inside a
  throwaway container based on the `thesiscode-fair-bridge-etl:latest` image
  with the repo mounted at `/work`.
- Report generation and plotting run on the host using `.venv`
  (requires `matplotlib`; `kafka-python>=2.1` is needed on Python 3.12).

All commands are executed from the repository root.

## Step 0 — Stop the simulator

The simulator continuously feeds `tb.telemetry.raw` and would act as
uncontrolled background load, breaking the "offered load is the only
variable" design.

```bash
docker compose stop simulator
```

## Step 1 — Smoke test (5 s @ 100 msg/s)

Verifies the end-to-end chain (producer → raw topic → ETL → validated/DLQ →
observer) before spending time on the real experiments.

```bash
docker run --rm --network thesiscode_tbnet --user 1000:1000 \
  -v $PWD:/work -w /work thesiscode-fair-bridge-etl:latest \
  python evaluation/run_performance_benchmark.py \
    --workloads 100 --duration-s 5 --repeat 1 --cooldown-s 15 \
    --bootstrap-server kafka:9092 --test-id smoke \
    --results-dir evaluation/results/smoke
```

Expected: `pass`, `target_achieved_ratio ≈ 1`, `routing_success_rate = 1.0`,
`validated:dlq ≈ 4:1`, `backlog_after_cooldown = 0`.

## Step 2 — Experiment 1: three-tier comparison (~10 min)

Defaults: workloads `small,medium,big` (100/500/1000 msg/s), 60 s per run,
3 repetitions, invalid ratio 0.2, seed 42.

```bash
docker run --rm --network thesiscode_tbnet --user 1000:1000 \
  -v $PWD:/work -w /work thesiscode-fair-bridge-etl:latest \
  python evaluation/run_performance_benchmark.py \
    --bootstrap-server kafka:9092 \
    --test-id thesis-perf --results-dir evaluation/results/thesis_perf
```

## Step 3 — Experiment 2: capacity limit (~25 min)

Rate sweep 1000 → 5000 msg/s, 3 repetitions per rate.

Single-invocation form (as originally documented):

```bash
docker run --rm --network thesiscode_tbnet --user 1000:1000 \
  -v $PWD:/work -w /work thesiscode-fair-bridge-etl:latest \
  python evaluation/run_performance_benchmark.py \
    --workloads 1000,1500,2000,3000,4000,5000 \
    --bootstrap-server kafka:9092 \
    --test-id thesis-capacity --results-dir evaluation/results/thesis_capacity
```

**Recommended robust form (per-rate invocations).** The benchmark only writes
its result files after *all* workloads finish, so a single crash late in the
sweep (e.g. a `KafkaTimeoutError: Batch ... expired` caused by a WSL2 clock
jump) loses the entire run. Running one rate per invocation isolates failures;
each rate writes its own summary:

```bash
for rate in 1000 1500 2000 3000 4000 5000; do
  docker run --rm --network thesiscode_tbnet --user 1000:1000 \
    -v $PWD:/work -w /work thesiscode-fair-bridge-etl:latest \
    python evaluation/run_performance_benchmark.py \
      --workloads $rate \
      --bootstrap-server kafka:9092 \
      --test-id thesis-capacity \
      --results-dir evaluation/results/thesis_capacity_parts/$rate \
    || echo "rate $rate FAILED"
done
```

Then merge the per-rate JSON files into the directory the analysis script
expects:

```bash
.venv/bin/python - <<'EOF'
import json
from pathlib import Path

base = Path("evaluation/results")
parts_dir = base / "thesis_capacity_parts"
out_dir = base / "thesis_capacity"
out_dir.mkdir(parents=True, exist_ok=True)

merged, runs, workloads = None, [], []
for rate_dir in sorted(parts_dir.iterdir(), key=lambda p: float(p.name)):
    doc = json.loads((rate_dir / "kafka_e2e_benchmark_summary.json").read_text())
    if merged is None:
        merged = {k: v for k, v in doc.items() if k not in ("runs", "workloads")}
    workloads.extend(doc.get("workloads", []))
    runs.extend(doc.get("runs", []))

merged["workloads"] = workloads
merged["runs"] = runs
out = out_dir / "kafka_e2e_benchmark_summary.json"
out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n")
print(f"Merged {len(runs)} runs -> {out}")
EOF
```

(`analyse_benchmark_results.py` regenerates `benchmark_kafka_summary.csv`
from the merged JSON, so only the JSON needs to be merged.)

## Step 4 — Optional: local CSV exporter benchmark

Host-side, no Kafka or CKAN involved; measures
`telemetry_exporter.append_telemetry_to_csv` throughput per payload size.

```bash
for size in small medium large; do
  .venv/bin/python evaluation/run_exporter_csv_benchmark.py \
    --rows 1000 --repeat 3 --payload-size $size \
    --test-id exporter-csv-$size \
    --results-dir evaluation/results/exporter_csv/$size
done
```

## Step 5 — Generate thesis artifacts

```bash
# Table 1 + Figures 1-3 from Experiment 1
.venv/bin/python evaluation/make_thesis_report.py \
  --results-dir evaluation/results/thesis_perf

# Sustainable-throughput table + capacity curves from Experiment 2
.venv/bin/python evaluation/analyse_benchmark_results.py \
  --results-dir evaluation/results/thesis_capacity --plots
```

## Step 6 — Restart the simulator

```bash
docker compose start simulator
```

## Output files

| Path | Content |
|---|---|
| `evaluation/results/smoke/` | Smoke-test run record |
| `evaluation/results/thesis_perf/kafka_e2e_benchmark_summary.json` / `benchmark_kafka_summary.csv` | Raw per-run records, Experiment 1 (9 runs) |
| `evaluation/results/thesis_perf/thesis/thesis_summary_table.{csv,md}` | Table 1: per-tier means ± std |
| `evaluation/results/thesis_perf/thesis/{latency,throughput,dlq_latency}_by_workload.png` | Figures 1–3 |
| `evaluation/results/thesis_capacity_parts/<rate>/` | Raw per-rate records, Experiment 2 |
| `evaluation/results/thesis_capacity/kafka_e2e_benchmark_summary.json` / `benchmark_kafka_summary.csv` | Merged Experiment 2 records (18 runs) |
| `evaluation/results/thesis_capacity/sustainable_throughput_summary.{csv,json}` | Sustainable-throughput verdict |
| `evaluation/results/thesis_capacity/*.png` | Throughput / p95-latency vs offered load, validated-vs-DLQ counts |
| `evaluation/results/exporter_csv/<size>/` | CSV exporter benchmark results |

## Latest results (run of 2026-06-11)

- **Experiment :** 9/9 pass. ETL throughput tracks input rate at all tiers
  (std ≤ 0.22), validated P95 stays in a flat 44–56 ms band across a 10×
  load increase, `backlog_after_cooldown = 0` everywhere, routing success
  100 %. Note the latency-vs-load curve is U-shaped, not monotonic: the
  small tier pays the ETL output producer's `linger_ms=20` batching wait
  (batches never fill at 100 msg/s), which shrinks as rate grows, while
  queueing delay only becomes visible well above 1000 msg/s.
ow.
