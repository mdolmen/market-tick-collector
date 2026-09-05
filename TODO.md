# Market Tick Collector — TODO

Rationale, decisions and design notes: [NOTES.md](NOTES.md)

Decided in the notes and assumed throughout: three venues (Binance, Coinbase, Kraken),
full-depth diff channels, one book per `(venue, symbol)` held in a `dict`, reader per
connection into a bounded ring buffer, periodic metrics push, cold rebuild on restart,
ClickHouse as the primary sink.

---

## Phase 0 · Walking skeleton

- [x] uv project as a `data-pipeline-core` consumer: `collector/`, ruff + `mypy --strict`
- [x] Binance spot, one symbol, full-depth diffs: connect, buffer, splice, apply
- [x] Land normalized level rows — one row per price level, not per frame — as a Parquet dataset
- [x] Dump raw frames verbatim to `.jsonl`, feeding the decode benchmark and Phase 1's replay
- [x] Benchmark decode early: `json` vs `orjson` vs `msgspec.json` with a typed schema
- [x] Measure msg/s and **in-process** p99 — receive → book applied → row built — and commit it
- [x] Write the architecture prediction down now, so Phase 9 can prove it wrong

Receive-to-disk p99 moved to Phase 7: one `sink.write()` at the end of a bounded run lands
every row at once, so the number would describe the run's shape and not the pipeline's. The
`ServiceApp` stub moved to Phase 4, where the real one is already listed — a bounded source
is a legal `Source` and `WorkerApp` ran it untouched, so Phase 0 needed **zero** SDK changes.

## Phase 1 · Capture & replay harness

- [ ] Capture raw frames verbatim to disk via the SDK's raw-landing pattern
- [ ] Replay from disk at 1× / 10× / 100×, excluding the socket path
- [ ] Fault injection: dropped frames, reordering, duplicates, gaps, bursts, clock jitter
- [ ] Deterministic seeding so any replay run is byte-reproducible
- [ ] Regression test: replay a known session, assert the resulting book matches
- [ ] Report the replay ceiling separately from any live capture rate, always labelled

## Phase 2 · Venue adapters

- [ ] Verify each venue's live channel surface before writing its adapter
- [ ] Fill the Coinbase row of the venue-limits table: cap, rate limits, disconnect policy
- [ ] Binance adapter: overlapping ranges, out-of-band REST snapshot, `U == prev_u + 1`
- [ ] Coinbase adapter: in-band snapshot, monotonic sequence
- [ ] Kraken adapter: in-band snapshot, CRC32 over the top 10, one depth per symbol
- [ ] Adapters expose only `in_sequence` / `gap_detected` / `snapshot_required` upward
- [ ] Nothing downstream of an adapter may learn which venue a record came from
- [ ] Resolve the symbol set: base assets overlapping all three venues
- [ ] Symbol normalization table — consumer-side business logic, never in the SDK
- [ ] Prices as integer ticks or `Decimal`; never float, anywhere
- [ ] Capture `exchange_ts`, `receive_ts`, `monotonic_ts`, one unit (ns) throughout
- [ ] Measure clock skew per venue and export it as a metric

## Phase 3 · Connection supervision & sharding

- [ ] Shard symbols across connections; size shards by recovery time, not by venue cap
- [ ] Bin-pack shards by measured message rate — BTC and ETH must not share a socket
- [ ] Batch SUBSCRIBE frames; the outbound limit is 5 msg/s on Binance spot
- [ ] Per-connection backoff, liveness and resubscribe; a failure never crosses connections
- [ ] Stagger connection opens so Binance's 24h expiry never synchronises the shards
- [ ] Make-before-break at ~23h: open the replacement, bootstrap it, then cut over
- [ ] Stagger the REST snapshot storm after any multi-book recovery, against the rate limit
- [ ] Measure convergence after the venue's own daily disconnect; report it separately

## Phase 4 · SDK extension: `ServiceApp` + streaming primitives

- [ ] ClickHouse spike **first** — the sink contract below is designed for its insert path
- [ ] Load a day of Phase 1 capture; pick the MergeTree sort key and partitioning
- [ ] One row per price level at 10⁷/day makes that ordering load-bearing, so measure it
- [ ] Measure insert throughput at realistic batch sizes; that number sets the flush triggers
- [ ] `ServiceApp` in `data-pipeline-core`: run until stopped, health endpoint, graceful drain
- [ ] Periodic metrics push — `WorkerApp` pushes in the `finally` of `run()`, useless here
- [ ] Batch-oriented `Sink`: size/time flush triggers, defined fate for the partial batch
- [ ] Extract backoff out of `HttpClient._backoff` into one policy both transports use
- [ ] Connection supervisor primitive: N connections, per-connection health, no shared fate
- [ ] Bounded queue primitive with high and low watermarks, not a single threshold
- [ ] Checkpoint protocol over an opaque token — this consumer's answer is "nothing"
- [ ] New series `messages_dropped_total`, `queue_depth`, drop reason — a deliberate §8 change
- [ ] Label them `queue="ring"|"batch"`; `stage` is already taken by the SDK and frozen
- [ ] Benchmark reader-thread + ring buffer against a pure-asyncio receiver; keep the numbers

## Phase 5 · Backpressure

- [ ] Never block the socket read — the reader drains at line rate under all conditions
- [ ] Ring buffer drops whole frames only; a partial frame is undetectable corruption
- [ ] Emit every drop as a `gap` control record so recovery and replay both see it
- [ ] Shed whole symbols under sustained pressure, never scattered messages
- [ ] Batch queue spills to disk and must never push back on the ring
- [ ] Measure peak arrival, p99 burst duration and sustained service rate first
- [ ] Size each queue as min(burst floor, latency ceiling); floor above ceiling = fix capacity
- [ ] Powers of two for the ring so the index wraps with a mask
- [ ] Load past the drop threshold deliberately; document the policy and the measured number

## Phase 6 · Book reconstruction & consistency

- [ ] One book per `(venue, symbol)`, a `dict` keyed by price (decided)
- [ ] Benchmark it against sorted-array and ticks-from-mid at a realistic read:write ratio
- [ ] Measure the hot read — best bid/ask — not just diff application
- [ ] Bootstrap splice: buffer first, snapshot second, find the event straddling `lastUpdateId`
- [ ] Distinguish snapshot-too-old (refetch) from buffer-behind (keep waiting)
- [ ] Sizes are absolute set-to-value, never increments; zero is the only deletion signal
- [ ] `(venue, symbol)` state machine: DISCONNECTED → BUFFERING → SYNCING → LIVE ⇄ GAPPED
- [ ] Mark the book untrusted on gap *before* repairing it, with both edges in the data
- [ ] Emit `snapshot` and `gap` as control records so replay reproduces the transitions
- [ ] Cold rebuild on restart — no durable book state, the venue is the source of truth
- [ ] Exercise recovery with the fault injector; assert convergence after re-snapshot

## Phase 7 · Storage

- [ ] `BatchSink[ArrowBatch]` contract in `data-pipeline-core`
- [ ] Arrow `RecordBatch` accumulation, flush on size or time
- [ ] Receive-to-disk p50 / p90 / p99 — deferred from Phase 0, meaningless before this sink
- [ ] Pin the landed column schema: dlt drops an all-null column, so files in one dataset
      disagree and a naive per-file read fails (seen in Phase 0 on `exchange_ts`)
- [ ] ClickHouse as the primary sink
- [ ] Rotating Parquet on GCS partitioned by `date/symbol` as the archive tier
- [ ] Retention and tiering rule sized for 10⁷ rows/day
- [ ] Idempotent writes across restart via deterministic ids — a replayed batch cannot duplicate

## Phase 8 · Reconciliation

- [ ] Oracle 1: reconstructed book vs venue REST snapshot, periodic
- [ ] Oracle 2: venue's own depth-limited top-N, continuous, on audited symbols
- [ ] Align oracle 2 by update id, never by clock; keep a ring of recent top-N versions
- [ ] Compare the top N−1 levels to avoid the truncation boundary artifact
- [ ] Oracle 3: replay harness injected faults; target detection of 100%
- [ ] Kraken CRC32 validated inline as a fourth, venue-native check
- [ ] Break classification: missing, duplicate, value mismatch, timing
- [ ] Configurable tolerance rules, break report, idempotent re-run
- [ ] Report every oracle's break count separately, never merged into one number

## Phase 9 · Benchmark harness & profiling

- [ ] Committed, reproducible benchmark suite
- [ ] Sustained throughput and burst capacity, live rate and replay ceiling kept apart
- [ ] Receive-to-disk p50 / p90 / p99
- [ ] Memory allocation and GC pause impact during volume spikes
- [ ] Compare the result against the Phase 0 prediction, whichever way it went
- [ ] Flamegraph before and after one profiling-driven optimisation

## Phase 10 · Read layer

- [ ] Research access API: point-in-time correct reads, returns Arrow/Polars, hides partitioning
- [ ] Query surface: DuckDB over the lake, or ClickHouse SQL

## Docs

- [ ] README leading with numbers
- [ ] Architecture diagram
- [ ] Flamegraphs
- [ ] Trade-off justifications, each with the measurement behind it
- [ ] The normalization boundary, and the one place it leaks

## Stretch — only if the core lands

- [ ] nanobind/pybind11 hot-path kernel, only after profiling proves the hotspot
