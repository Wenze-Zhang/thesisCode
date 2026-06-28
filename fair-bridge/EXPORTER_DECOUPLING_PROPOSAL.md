# Telemetry exporter — decoupling CSV write from CKAN upload (MVP proposal)

> **Status (2026-06-19):** Option A (background uploader thread) **and** the
> parallel-upload add-on are **implemented** in `telemetry_exporter.py`
> (`uploader_loop`, thread-local `_ckan_client`, parallel `flush_dirty_csvs`,
> `EXPORT_UPLOAD_WORKERS`). Not yet rebuilt/redeployed or re-benchmarked.
> **Incremental upload is deferred** — it is not a safe drop-in (see the note at
> the end of the Option A section).

## Problem

`telemetry_exporter.py:main()` is a **single loop**:

```python
while True:
    records = consumer.poll(...)        # consume Kafka validated
    # append_telemetry_to_csv(...)      # write CSV, add to `dirty`
    consumer.commit(...)
    if time() - last_flush >= 30:
        dirty = flush_dirty_csvs(ckan, dirty)   # <-- BLOCKS: serial CKAN upload
        last_flush = time()
```

Every 30 s the loop **blocks** while it uploads to CKAN **serially**, so during
each flush no Kafka polling happens → consumer lag builds → end-to-end (T4)
throughput drops below `y = x` and the drain tail balloons (the throughput
benchmark's 200–400 msg/s dip). Goal: the consume path must never wait on CKAN.

Two shapes were considered: **(A) in-process background uploader thread**, and
**(B) a second service decoupled via a Kafka topic**. Recommendation below: do **A**
first (it is the MVP and directly fixes the measured problem); keep **B** for later
horizontal scaling.

---

## MVP — Option A: background uploader thread

Keep one process. The consume loop only writes CSV + marks the resource dirty; a
**daemon thread** owns the 30 s flush and the CKAN uploads. The on-disk CSVs + the
`dirty` set already are the queue — no new infra, no new topic.

### Change set (small)

1. Guard `dirty` with a `threading.Lock`.
2. Move the flush into a thread; use a **claim → upload → re-queue-failures** pattern
   so nothing is lost across the concurrent writer.
3. Consume loop: drop the flush block; just `with lock: dirty.add(...)`.

```python
import threading

def uploader_loop(ckan, dirty, lock, stop, flush_interval):
    last_flush = 0.0
    while not stop.is_set():
        if time.time() - last_flush >= flush_interval:
            with lock:                 # atomically claim the current dirty set
                claimed = set(dirty)
                dirty.clear()
            if claimed:
                remaining = flush_dirty_csvs(ckan, claimed)   # slow CKAN I/O, off the consume path
                if remaining:          # re-queue only the failures
                    with lock:
                        dirty |= remaining
            last_flush = time.time()
        stop.wait(0.5)

def main():
    ...
    dirty = discover_pending_csvs(export_dir)
    lock = threading.Lock()
    stop = threading.Event()
    threading.Thread(target=uploader_loop,
                     args=(ckan, dirty, lock, stop, flush_interval),
                     daemon=True).start()
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        offsets = {}
        for tp, batch in records.items():
            for msg in batch:
                try:
                    slug, day, path = append_telemetry_to_csv(msg.value or {}, export_dir, header_cache)
                    with lock:
                        dirty.add((slug, day, path))
                except Exception:
                    log.exception("CSV write failed at %s on %s; not committing past it", msg.offset, tp)
                    break
                offsets[tp] = OffsetAndMetadata(msg.offset + 1, None)
        if offsets:
            consumer.commit(offsets)    # never blocked by CKAN anymore
```

### Why claim-then-clear is correct under concurrency

- We `clear()` the dirty set at claim time. While the upload runs, the consume loop
  keeps adding any resource that receives new rows back into `dirty` (they are not in
  `claimed`), so they get uploaded next interval — no rows are dropped, including the
  ones that arrive *during* an upload.
- Failed uploads are merged back (`dirty |= remaining`), preserving the existing
  at-least-once retry behaviour.
- `dirty` stays bounded by the number of distinct `(slug, day)` resources, not by
  message volume; the CSV files on disk are the durable buffer.

### What this fixes / doesn't

- **Fixes:** the head-of-line blocking → no consumer-lag build-up during flushes →
  shorter drain tail → T4 closer to `y = x`, and the 200–400 dip should flatten.
- **Does not fix:** the per-flush cost still grows because each flush re-uploads the
  **whole** CSV (see optional add-ons). Decoupling alone removes the *stall*, not the
  *growth*.

### Effort & risk
~30 lines, no new services/topics/migrations. Risk is low; the only subtlety is the
lock + claim pattern above. Validate by re-running the throughput benchmark and
checking T4 vs `y = x` and the drain tail.

---

## Optional add-ons (orthogonal, after the MVP)

1. **Parallel uploads inside a flush** — replace the serial `for` in
   `flush_dirty_csvs` with a small `concurrent.futures.ThreadPoolExecutor`
   (e.g. 4–8 workers). Cuts the flush wall-time when many resources are dirty.
2. **Incremental upload instead of full-file re-upload** — the real cost driver.
   Track the last-uploaded row count/offset per `(slug, day)` and push only new rows
   via the CKAN **DataStore** API (`datastore_upsert`) rather than re-`resource_patch`
   the growing file each time. Bigger change (file resource → datastore), so keep it
   as a follow-up, not MVP.

---

## Option B (for later): decouple via a Kafka topic + separate uploader service

Writer consumes `tb.telemetry.validated` → writes CSV → publishes `(slug, day)` to a
new compacted topic `tb.telemetry.export-dirty`. A separate **uploader** service (own
consumer group) consumes that topic and uploads to CKAN.

- **Pros:** full process isolation; durable queue across restarts; horizontally
  scalable (run N uploaders, partitioned by dataset); writer never touches CKAN.
- **Cons:** new topic + new container/service + new consumer group + dedup of the
  `(slug, day)` events; more ops surface. Over-engineered unless a single uploader
  thread genuinely can't keep up.

**Recommendation:** ship **Option A** now (MVP, fixes the benchmark's stall with
minimal change); add the parallel/incremental upload if the publish stage is still
the bottleneck; move to **Option B** only when you need to scale uploads across
multiple instances.
