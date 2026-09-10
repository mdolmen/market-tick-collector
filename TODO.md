# Market Tick Collector — TODO

## Phase 0 · Walking skeleton

- [x] uv project as a `data-pipeline-core` consumer: `collector/`, ruff + `mypy --strict`
- [x] Binance spot, one symbol, full-depth diffs: connect, buffer, splice, apply
- [x] Land normalized level rows — one row per price level, not per frame — as a Parquet dataset
- [x] Dump raw frames verbatim to `.jsonl`, feeding the decode benchmark and Phase 1's replay
- [x] Benchmark decode early: `json` vs `orjson` vs `msgspec.json` with a typed schema
- [x] Measure msg/s and **in-process** p99 — receive → book applied → row built — and commit it
- [x] Write the architecture prediction down now, so Phase 7 can prove it wrong

Receive-to-disk p99 moved to Phase 7: one `sink.write()` at the end of a bounded run lands
every row at once, so the number would describe the run's shape and not the pipeline's. The
`ServiceApp` stub moved to Phase 4, where the real one is already listed — a bounded source
is a legal `Source` and `WorkerApp` ran it untouched, so Phase 0 needed **zero** SDK changes.

## Phase 1 · Capture & replay harness

- [x] Capture raw frames verbatim to disk via the SDK's raw-landing pattern
- [x] Replay from disk at 1× / 10× / 100×, excluding the socket path
- [x] Fault injection: dropped frames, reordering, duplicates, gaps, bursts, clock jitter
- [x] Deterministic seeding so any replay run is byte-reproducible
- [x] Regression test: replay a known session, assert the resulting book matches
- [x] Report the replay ceiling separately from any live capture rate, always labelled

## Phase 2 · Venue adapters

- [x] Verify each venue's live channel surface before writing its adapter
- [x] Fill the Coinbase row of the venue-limits table with what a probe can establish;
      the cap and the rate limit are not among them and bind in Phase 3
- [x] `VenueAdapter` / `VenueTransport` protocols; Binance refactored onto them, behaviour
      unchanged and the Phase 1 byte-reproducibility test untouched
- [x] Binance adapter: overlapping ranges, out-of-band REST snapshot, `U == prev_u + 1`
- [x] Coinbase adapter: in-band snapshot, `sequence_num`, Advanced Trade `level2`
- [x] Raise `max_size` on the socket — an in-band snapshot exceeds the 1 MiB default
- [x] Adapters expose only `in_sequence` / `gap_detected` / `snapshot_required` upward
- [x] `classify` returns frame / snapshot / control; control is landed, not dropped, because
      a venue may number it in the same sequence as its book messages
- [x] `advance()` moves the cursor past a message that yields no `Update`
- [x] Repair on an in-band venue is a resubscribe, on the same trigger as a REST refetch
- [x] Nothing downstream of an adapter may learn which venue a record came from
- [x] Resolve the symbol set: base assets overlapping all three venues
- [x] Symbol normalization table — consumer-side business logic, never in the SDK
- [x] Prices as integer ticks or `Decimal`; never float, anywhere
- [x] One project-wide `SCALE = 8`; all three venues measured at eight places
- [x] Capture `exchange_ts`, `receive_ts`, `monotonic_ts`, one unit (ns) throughout
- [x] Parse RFC3339 fractions directly — `fromisoformat` truncates ns to µs in silence
- [x] Measure the venue-to-local clock difference per venue and commit the number
- [x] Export it as a metric — needs `RunContext` to carry one; shipped in Phase 4 as
      `venue_clock_difference_seconds`, see `collector/metrics.py`

## Phase 2.5 · Kraken

- [x] Kraken adapter: in-band snapshot, one depth per symbol
- [x] CRC32 over the top 10, validated inline, latching `snapshot_required` on mismatch
- [x] Decode with `parse_float=str` — and for every venue, not just this one: `scaled_int` refuses `1e-08`, so a float kills the run rather than drifting
- [x] The token is already at the instrument's precision; no `price_precision` table needed
- [x] No full-depth channel exists — depth is 10/25/100/500/1000. Running the 1000 ceiling
- [x] Trim the checksum view: the venue leaves levels behind, 1000 → 1040 a side in 60s
- [x] A second depth per symbol is refused — Phase 8's Oracle 2 does not exist here
- [ ] ~~Re-run the Phase 6 book benchmark with the checksum in the loop~~ — premise wrong. The CRC reads the venue's decimal strings and `Book` has none, so it never touches that structure. See Phase 6 and `bench/checksum.py`

## Phase 3 · Connection supervision & sharding

- [x] ~~Shard symbols across connections; size shards by recovery time, not by venue cap~~ —
      sized by blast radius instead. Recovery time was measured and does not scale with
      shard size, so it cannot set one; see `NOTES.md`
- [x] Bin-pack shards by measured message rate — BTC and ETH must not share a socket
- [x] Batch SUBSCRIBE frames; the outbound limit is 5 msg/s on Binance spot
- [x] Per-connection backoff, liveness and resubscribe; a failure never crosses connections
- [x] Stagger connection opens so Binance's 24h expiry never synchronises the shards
- [ ] Make-before-break at ~23h — moved to Phase 4; break-before-make ships here
- [x] Stagger the REST snapshot storm after any multi-book recovery, against the rate limit
- [x] Demultiplex a shard's records into one book per symbol
- [x] Judge connection-scoped continuity per connection, not per book

## Phase 4 · SDK extension: `ServiceApp` + streaming primitives

- [ ] ~~Make-before-break at ~23h~~ — skipped; break-before-make measured 0 gaps in Phase 3
- [x] ClickHouse spike **first** — the sink contract below is designed for its insert path
- [x] Load a day of Phase 1 capture; pick the MergeTree sort key and partitioning
- [x] One row per price level at 10⁷/day makes that ordering load-bearing, so measure it
- [x] Measure insert throughput at realistic batch sizes; that number sets the flush triggers
- [x] `ServiceApp` in `data-pipeline-core`: run until stopped, health endpoint, graceful drain
- [x] Periodic metrics push — `WorkerApp` pushes in the `finally` of `run()`, useless here
- [x] Batch-oriented `Sink`: size/time flush triggers, defined fate for the partial batch
- [x] `RunContext` carries a metrics handle, and the registry a consumer can add its own to
- [x] Clock-difference histogram labelled by venue, deferred from Phase 2
- [x] `price_ticks` needs 128 bits; `Int64` cannot hold a tick at `SCALE = 8`
- [x] Connection supervisor primitive: N connections, per-connection health, no shared fate
- [x] New series `messages_dropped_total`, `queue_depth`, drop reason — a deliberate §8 change
- [x] ~~Label them `queue="ring"|"batch"`~~ — decided against; see `NOTES.md`
- [x] Drop the bounded-queue and checkpoint items from `data-pipeline-core`'s TODO too

## Phase 5 · Backpressure — cut

- [x] Never block the socket read — `ShardSupervisor._enqueue`, shipped in Phase 3
- [x] Drops whole frames only; a partial frame is undetectable corruption
- [x] Every drop counted and latched as a repair, visible to recovery and replay

## Phase 6 · Book reconstruction & consistency

- [x] One book per `(venue, symbol)`, a `dict` keyed by price (decided)
- [x] The checksum stays out of it: it reads decimal strings, `Book` holds ticks. Kraken's per-frame ordered read is on the adapter's own view, already measured
- [x] Bootstrap splice: buffer first, snapshot second, find the event straddling `lastUpdateId`
- [x] Distinguish snapshot-too-old (refetch) from buffer-behind (keep waiting)
- [x] Sizes are absolute set-to-value, never increments; zero is the only deletion signal
- [x] `(venue, symbol)` state machine: BUFFERING → SYNCING → LIVE, gap returns to BUFFERING.
- [x] Mark the book untrusted on gap *before* repairing it, with both edges in the data
- [x] Emit `snapshot` and `gap` as control records so replay reproduces the transitions
- [x] Cold rebuild on restart — no durable book state, the venue is the source of truth
- [x] Exercise recovery with the fault injector; assert convergence after re-snapshot
- [x] Oracle: reconstructed book vs venue REST snapshot, periodic, Binance only —
      the other two have no id-alignable independent read. `snapshot_interval_s`
      already lands the snapshots it reads
- [x] Compare only where the book is `live`; an untrusted book measures where it stopped
- [x] Report its break count, and the Kraken CRC32 break count, as two numbers
- [ ] Run it against a live session and commit the divergence rate

## Phase 7 · Storage

- [ ] `BatchSink[ArrowBatch]` contract in `data-pipeline-core`
- [ ] Arrow `RecordBatch` accumulation, flush on size or time
- [ ] Receive-to-disk p50 / p90 / p99 — deferred from Phase 0, meaningless before this sink
- [ ] Sustained throughput and burst capacity at full shard scale, from the same run
- [ ] Live rate and replay ceiling reported apart, both labelled
- [ ] Compare against the Phase 0 architecture prediction, whichever way it went
- [ ] Pin the landed column schema: dlt drops an all-null column, so files in one dataset
      disagree and a naive per-file read fails (seen in Phase 0 on `exchange_ts`)
- [x] ClickHouse as the primary sink
- [ ] Rotating Parquet on GCS partitioned by `date/symbol` as the archive tier
- [ ] Retention and tiering rule sized for 10⁹ rows/day
- [ ] Idempotent writes across restart via deterministic ids — a replayed batch cannot duplicate

## Phase 8 · Reconciliation — cut

- [x] Kraken CRC32 as a venue-native check — built in Phase 2.5; the REST oracle is Phase 6

## Phase 9 · Read layer

- [ ] `DatasetReader` protocol in `data-pipeline-core`: returns Arrow, hides partitioning
- [ ] ClickHouse implementation, sized against the `ORDER BY` chosen in Phase 4
- [ ] Parquet/DuckDB implementation over the GCS archive tier
- [ ] Consumer-side helper: one symbol over one time window, returning Arrow.

## Docs

- [ ] README leading with numbers
- [ ] Architecture diagram
- [ ] The normalization boundary, and the one place it leaks
- [ ] The SDK diff: what `ServiceApp` forced, and which abstractions survived intact
