# FAIR Bridge Throughput Benchmark — Working Guide

> Context file for the **end-to-end throughput experiment** (the `throughput_four.py`
> ramp). Scope: only this benchmark. Read this first if you pick the work up in a
> fresh session — it captures the design, the metric, how to run/plot/clean up, the
> gotchas, and the open questions.

---

## 1. Purpose

Measure how fast telemetry can travel **device → Kafka raw → ETL validated →
exporter CSV → CKAN resource**, as a function of offered load, to find the whole
prototype's process capacity (not an isolated component). The end-to-end (T4)
curve is the headline; T1–T3 localise where a bottleneck is.

## 2. Files (all under `evaluation/`)

| File | Role |
|---|---|
| `throughput_four.py` | The benchmark. Runs the isolated load ramp, drains to zero between steps, writes `results/thesis_throughput4/throughput_ramp4_summary.{csv,json}`. |
| `run_performance_benchmark.py` | Shared helpers imported by `throughput_four.py` (device provisioning, Kafka observers, CSV tailer, CKAN poller, `Event` class, `_stage_stats`, `_write_csv`). |
| `plot4throughput.py` | Plots the 4 curves (T1–T4) vs offered load; averages over repetitions. |
| `plot_end_to_end_throughput.py` | Plots the single end-to-end (T4) curve with mean ± 1 sd error bars. |
| `cleanup_benchmark_data.py` | Deletes test data whose name carries the `-bench-` marker (TB devices + CKAN datasets + export dirs). Dry-run by default; `--apply` to delete. |
| `results/thesis_throughput4/METHODOLOGY.md` | Paper-ready methods (中文). |
| `results/thesis_throughput4/RESULTS_DISCUSSION.md` | Paper-ready results + discussion (中文). |
| `plot4throughput_corrected.py` | One-off; recomputed the OLD broken run's data into a valid preview. Obsolete now that the metric is fixed — keep only as history. |

## 3. Methodology (the design principles — do not silently change these)

1. **Isolated steps.** Each offered load is an independent experiment. Every
   repetition provisions a **brand-new set of devices ⇒ brand-new CKAN datasets**
   (unique `test-id` with a timestamp), so nothing is shared between runs.
2. **Open-loop load** at the target rate for a fixed **dwell** (default 360 s).
3. **Drain to zero between steps.** After the dwell, block until the pipeline is
   empty — exporter validated-topic **Kafka lag == 0 AND every produced message is
   in CKAN** — or until `--drain-timeout-s` (900 s). Only then start the next,
   higher load. This (plus fresh datasets) is the *entire* isolation mechanism;
   there is **no cooldown** and **no mid-run dataset wiping**.
4. **Repetitions** (`--repetitions`, used 3): plots show the mean; the end-to-end
   plot adds ± 1 sd.
5. **Ascending ramp;** the first step that fails to drain marks capacity and stops
   the ramp (`--stop-after-crash`, default on).
6. **No knee/crash detection.** Removed on purpose — read the bottleneck off the
   plot by eye.

## 4. Metric definition (makespan — this is the important part)

For a step `k`: `s` = first device send time (shared by all stages). For stage
`X ∈ {raw, val, csv, ckan}`, `e_X` = time the **last** message reaches stage X,
`N_X` = number of messages that reached stage X. Then

    throughput_X = N_X / (e_X − s)          # conservation-respecting makespan rate

- **T1 raw, T2 validated, T3 csv, T4 ckan**; **end-to-end ≡ T4**.
- Achieved offered/input rate (x-axis): `input = P / (e_send − s)`, `P` = produced.
- `e_X = s + dwell + δ_X`, where `δ_X` = the stage's **drain tail** (time after load
  stops for the last in-flight messages to arrive). So `throughput_X = N_X/(dwell+δ_X)`
  ⇒ T1 has δ≈0 (tracks y=x), T4 has the largest δ (sits below y=x).

**Why makespan, not a windowed rate:** it can never exceed offered load (you can't
complete more than you sent). The earlier *windowed completion-rate* metric (count
completions inside `[s+guard, e]`) was WRONG: under a cold pipeline it counted
warm-up backlog draining into the window and produced throughput **above y = x**
(delivery ratio > 1). Do not reintroduce a guard/window.

## 5. Parameters (defaults in `throughput_four.py`)

| Flag | Default used | Meaning |
|---|---|---|
| `--steps` | `50,100,150,200,300,400,500,600,700,800` | offered loads (msg/s) |
| `--repetitions` | 3 (CLI default 1) | reps per load |
| `--device-count` | **20** | fresh devices/datasets per rep |
| `--dwell-s` | 360 | seconds of load per rep |
| `--invalid-ratio` | 0 | invalid msgs go to DLQ, not CKAN |
| `--drain-timeout-s` | 900 | upper bound on drain (actual: tens of s) |
| `--ckan-poll-interval-s` | 3 | CKAN resource poll |
| `--seed` | 42 | reproducibility |
| (exporter) `EXPORT_FLUSH_INTERVAL_S` | 30 | CKAN flush cadence — drives T4's tail |

## 6. How to run (in-container — copy/paste recipe that works)

The benchmark **must** run inside the compose network and reach BOTH Kafka
(`tbnet`) and CKAN (`ckannet`); it also needs the exporter's export volume. Host
`docker` is Docker Desktop/WSL — make sure Docker Desktop is running.

```bash
cd /home/wenze/thesisCode
docker compose stop simulator          # remove uncontrolled background load

# 1. clean env file (do NOT use --env-file .env: inline `# comments` break it)
KEY=$(grep -E '^CKAN_API_KEY=' .env | head -1 | cut -d= -f2- | sed -E 's/[[:space:]]*#.*$//; s/^"//; s/"$//' | xargs)
printf 'TB_HOST=http://thingsboard:8080\nTB_USER=tenant@thingsboard.org\nTB_PASS=tenant\nTB_MQTT_HOST=thingsboard\nTB_MQTT_PORT=1883\nCKAN_URL=http://ckan:5000\nEXPORT_DIR=/app/exports\nCKAN_API_KEY=%s\n' "$KEY" > /tmp/benchenv

# 2. create on tbnet, attach ckannet, start.  paho-mqtt is missing from the image →
#    install to a mounted target (NOT kafka-python: it shadows the image's working one)
docker rm -f t4bench 2>/dev/null
docker create --name t4bench --network thesiscode_tbnet --user 1000:1000 \
  --env-file /tmp/benchenv -v "$PWD":/work -w /work \
  -v thesiscode_telemetry_exports:/app/exports \
  -e HOME=/work -e PYTHONPATH=/work/.benchdeps \
  thesiscode-fair-bridge-etl:latest \
  sh -c "pip install --no-cache-dir --quiet --target=/work/.benchdeps paho-mqtt && \
    python -u evaluation/throughput_four.py --bootstrap-server kafka:9092 \
      --repetitions 3 --results-dir evaluation/results/thesis_throughput4"
docker network connect thesiscode_ckannet t4bench
docker start t4bench                   # detached; ~3–4 h for the full 10×3 ramp

# watch progress
docker logs --tail 30 t4bench
```

Smoke test first (≈1 min): same command but
`--steps 100 --dwell-s 15 --repetitions 1 --device-count 5 --drain-timeout-s 180`.

Names are fixed by compose project `thesiscode`: networks `thesiscode_tbnet` /
`thesiscode_ckannet`, volume `thesiscode_telemetry_exports`, image
`thesiscode-fair-bridge-etl:latest`, exporter container
`thesiscode-fair-bridge-telemetry-exporter-1`.

## 7. How to plot (on the host, needs matplotlib)

```bash
python3 evaluation/plot4throughput.py            # -> figures/throughput4_vs_offered_load.png
python3 evaluation/plot_end_to_end_throughput.py # -> figures/end_to_end_throughput_vs_offered_load.png
```

## 8. How to clean up afterwards (so light `devices.bash` sims start clean)

```bash
# CKAN datasets + TB devices + export dirs with the -bench- marker (dry-run first)
docker create --name t4clean --network thesiscode_tbnet --user 1000:1000 \
  --env-file /tmp/benchenv -v "$PWD":/work -w /work \
  -v thesiscode_telemetry_exports:/app/exports -e HOME=/work -e PYTHONPATH=/work/.benchdeps \
  thesiscode-fair-bridge-etl:latest python evaluation/cleanup_benchmark_data.py --apply
docker network connect thesiscode_ckannet t4clean && docker start -a t4clean && docker rm -f t4clean
```
Caveats: `cleanup_benchmark_data.py` only matches `-bench-` (a separate `e2e-tput`
naming exists). **Export-dir removal fails as uid 1000** (dirs owned by the
exporter) — remove them as root:
`docker exec -u 0 thesiscode-fair-bridge-telemetry-exporter-1 sh -c "cd /app/exports && find . -maxdepth 1 -mindepth 1 -type d -name '*-bench-*' -exec rm -rf {} +"`.
Verify Kafka lag is 0:
`docker exec thesiscode-kafka-1 /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fair-bridge-telemetry-exporter`.
Finally remove `/tmp/benchenv` (holds the API key) and `.benchdeps/`.

## 9. Key results (2026-06-19 run, 20 devices, 3 reps)

- **Scales to ≥ 800 msg/s with no knee.** All 30 rep-runs drained to zero (800
  drained in 17–51 s). Max end-to-end ≈ 762 msg/s. Capacity C* is **above 800**;
  this ramp is a lower bound.
- T1/T2/T3 track y=x; **T4 (CKAN) sits at ~0.80–0.93 of offered** — the gap is the
  drain tail in the makespan, not a rate drop.
- **Broad dip in the T4/offered ratio over 200–400 (min ~0.80), recovering at
  500+.** It tracks the drain tail (peaks ~90 s at 200–400, drops to ~30 s at
  500+). Most consistent with **CKAN warm-up** confounded with the ascending order,
  **amplified by the exporter re-uploading the whole growing CSV every flush** (see
  §10). It appeared in earlier runs too and under the old windowed metric → it's a
  real publish-stage effect, not noise. Cannot fully separate warm-up from a load
  threshold without a controlled re-run.

## 10. Known issue that shapes the results — exporter coupling

`fair-bridge/telemetry_exporter.py` runs in a **single loop**: it consumes Kafka +
writes CSV, and every 30 s **blocks** to upload to CKAN **serially**, **re-uploading
the entire growing CSV** for each resource (`resource_patch(upload=...)`,
lines ~278–289). This (a) builds Kafka lag during each flush, and (b) makes the
publish cost grow with accumulated rows → long drain tails → T4 below y=x. See the
decoupling proposal at `fair-bridge/EXPORTER_DECOUPLING_PROPOSAL.md`.

## 11. How to extend / open questions

- **Find the real C\*:** push the ramp higher (`--steps ...,1000,1200,1500`) until a
  step fails to drain — that point is the capacity/knee.
- **Disentangle the 200–400 dip:** re-run with **randomised/interleaved step order**
  (or pre-warm CKAN, or log per-flush upload latency). If the dip follows run-order
  → warm-up; if it follows absolute load → a real load effect. Also vary
  `--device-count` (10 vs 40): if the dip tracks per-resource rate it's a
  per-resource CKAN/datastore limit.
- **Validate the exporter fix:** apply the decoupling MVP, re-run, check T4 moves
  toward y=x and the dip flattens.

## 12. Gotchas (things that already bit us)

- Old **windowed metric exceeded y=x**; replaced by makespan. Don't reintroduce a guard.
- `--env-file .env` breaks on inline `#` comments (e.g. `TB_MQTT_PORT`). Use a clean env.
- The etl image lacks `paho-mqtt`; install to a `--target` dir; **don't** install
  `kafka-python` there (shadows the image's working one).
- TB/CKAN are on different networks → attach **both** `tbnet` and `ckannet`.
- The figures published in the thesis used **20 devices** (not 100).
