# market-tick-collector

High-volume L2 order book capture from public crypto exchange websockets, and the second
consumer of [`data-pipeline-core`](../data-pipeline-core).

This is not a crypto project. Crypto because free high-volume L2 data. No trading, no strategy, no signal. It is a data platform, and the interesting problems are book reconstruction, sequence gaps, backpressure and reconciliation.

## In numbers

The write path is complete: three venues, sharded across supervised connections, books
reconstructed from diffs and landed in ClickHouse through an Arrow batch sink. Phase 9's read
layer was cut for want of a caller (`NOTES.md` § *The read layer*), so the way out of the
table is SQL or `tools/book.py`.

**One live run — Kraken, 185 symbols at depth 1000, 300s, every book live at exit.**
2,048,719 rows across 142 flushes; the server's own `count()` agrees exactly. Zero gaps, zero
crossed books, zero checksum breaks.

| | |
|---|---|
| Sustained | **6,852 rows/s** |
| Burst, busiest arrival second | **41,576 rows/s** (6.1× sustained — the bootstrap storm) |
| Receive-to-disk | p50 **1,195 ms**, p90 **2,042 ms**, p99 **2,286 ms** |

Latency is the batch, not the write: the flush trigger is 2.0s and the insert itself is
45.7 ms. **Binance's p99 on a comparable run is 13,878 ms**, and that number measures snapshot
latency rather than write latency — a book still waiting on its out-of-band REST snapshot
buffers frames that keep their arrival clock, so the splice lands them with the whole wait
attached. The same percentile means different things on two venues, which is why it is
reported per venue and never averaged.

**Two rates, and they are never merged.** The live figure is bounded by what the venue sends;
the ceiling is bounded by this code. Both from the same 200,000-record Kraken corpus, socket
excluded:

| | rows/s | µs/row |
|---|---|---|
| decode → book apply → row built | 85,236 | 11.73 |
| the same, with Arrow and the sink in the loop | **62,618** | 15.97 |

The sink costs 26.5% of the ceiling. Conversion (0.66 µs/row) and insert (0.91 µs/row) account
for only 1.57 µs of the 4.24 µs it adds — **the parts measured apart do not add up to the
whole**, and the remainder is the streaming path itself.

**The insert path**, 400,000 rows in 8 batches of 50,000: Arrow **1,093,040 rows/s** against
131,900 for the row-oriented path — 8.3×, and irrelevant to the running system at 70×
headroom over live arrival. The sort key `(venue, symbol, receive_ts, seq)` was chosen by
measurement too: 258 MB against 344 MB over ten million identical rows, and 16,385 rows read
against 458,752 for a one-symbol, one-minute window.

**Correctness has its own numbers, and there are two of them.** The REST oracle diffs the
reconstructed book against Binance's own periodic snapshot, id-aligned: **45 comparisons, 45
clean, 0 diverging levels in 446,830 compared**, with 3 comparisons unaligned (93.8%
coverage). Kraken's CRC32 over the venue's top ten is a second, independent check, and it
broke **zero** times across the 185-symbol run. The two are reported apart because they are
different claims.

The throughput and correctness numbers were predicted in writing *before* the runs and graded
afterwards, including where the prediction lost: the Arrow ratio was under-called by a factor
of two, and the sink's cost to the ceiling was predicted under 20% against a measured 26.5%.
`DEVELOPMENT.md` has each prediction, its grade, the predictions that turned out ungradeable,
and the two benchmark defects that nearly shipped a 6.7× overstatement as a committed number.

## Architecture

```mermaid
flowchart TB
    WS["venue websockets<br/>binance · coinbase · kraken"]
    SUP["ShardSupervisor<br/>N connections, per-connection backoff and liveness<br/>bounded queue — a full queue drops, it never blocks a socket read"]
    SRC["FrameSource<br/>frames, plus the REST snapshot where the venue needs one"]
    RAW[("raw landing<br/>JSONL, verbatim, file:// or gs://")]
    AD["VenueAdapter — classify, parse, emit Update<br/>exposes only in_sequence / gap_detected / snapshot_required"]
    RT["BookRouter<br/>one BookTransform per venue+symbol"]
    BT["BookTransform<br/>BUFFERING → SYNCING → LIVE<br/>bootstrap splice; a gap marks the book untrusted before repair"]
    BK["Book<br/>two dicts keyed by integer ticks"]
    SINK["arrow_batching_sink<br/>flush at 50,000 rows or 2.0s"]
    CH[("ClickHouse MergeTree<br/>ORDER BY venue, symbol, receive_ts, seq")]
    PQ[("Parquet via dlt<br/>file:// or gs://")]

    WS --> SUP --> SRC
    SRC -- "capture" --> RAW
    RAW -. "replay — cold, no socket, faults optional" .-> AD
    SRC --> AD
    AD ==> RT
    RT --> BT
    BT <--> BK
    BT --> SINK
    SINK --> CH
    SINK --> PQ
```

The double line is the normalization boundary. Everything below it has been handed normalized
records and cannot tell which venue produced them. Everything above it — including the adapter
itself, which is the one thing allowed to know — is the venue's problem.

Two properties fall out of where that line sits. The raw landing is captured *above* it, so a
session is stored as the venue sent it and a parser bug never costs data — and a replay
re-enters at the same point, driving the *same* book code a socket drives, which is what makes
fault injection meaningful rather than a second implementation being tested. And the fault
injector perturbs capture records without having heard of either venue, which is why it worked
unchanged on the second dialect and then the third.

## The normalization boundary, and the one place it leaks

An adapter's only job is turning one venue's frames into the normalized record model. The
book, the router, the batching sink, the oracle and the metrics never learn which venue a
record came from except as a label. `tests/test_boundary.py` asserts that statically rather
than trusting it.

**The leak is real and it is confined.** Sequencing semantics genuinely differ per venue, and
no amount of modelling makes them the same: Binance chains overlapping `[U, u]` ranges and
bootstraps from an out-of-band REST snapshot; Coinbase chains a single per-connection
`sequence_num` and sends its snapshot in band; Kraken numbers *nothing at all*. That difference
stays inside the adapter, behind three booleans — `in_sequence`, `gap_detected`,
`snapshot_required` — and nothing downstream can ask a fourth question. Naming the exception
is the design; pretending it does not exist would have put venue conditionals in the book.

Kraken is what made the contract earn its keep. With no sequence of any kind the chain rule is
trivially true and proves nothing, so a CRC32 over the venue's own top ten is the only
integrity signal there is — which is why the adapter and the checksum shipped in one commit. It
costs 8.5 µs a message at depth 1000, against 73.9 µs done the obvious way (`bench/checksum.py`).

Adding it found a bug in the two venues already shipped: a snapshot arriving at a healthy book
was ignored as redundant, which holds for Binance's REST read and does not hold for one
obtained by resubscribing. 43 crossed books in a replay whose every checksum passed.
`DEVELOPMENT.md` § Phase 2.5.

## Running it

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+. The SDK is picked up from
`../data-pipeline-core` as an editable path, so the two repos sit side by side.

```bash
uv sync
```

Watch it work, no storage needed — level rows go to stdout:

```bash
MTC_DURATION_S=30 MTC_OUTPUT=console MTC_SNAPSHOT_LIMIT=20 \
    uv run python -m collector.main
```

Land a Parquet dataset:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file://$PWD/data/l2" \
MTC_DURATION_S=60 \
    uv run python -m collector.main
```

The run logs its own row count; check it against what landed:

```bash
uv run python -c "import pyarrow.dataset as ds; \
    print(ds.dataset('data/l2/l2/levels', format='parquet').to_table().num_rows)"
```

Land in ClickHouse instead — a whole venue's shard, which is what the numbers above were
measured on. `compose.yaml` brings up the local node the sink was measured against; it is not
a deployment:

```bash
docker compose up -d
MTC_VENUE=kraken MTC_SYMBOLS='*' MTC_OUTPUT=clickhouse MTC_DURATION_S=300 \
    uv run python -m collector.main
```

`MTC_MODE=service` runs the same wiring with no end to it — the SDK's `ServiceApp`, with a
health endpoint and a graceful drain — instead of for a fixed duration.

### Seeing a book

The book is in-memory and dies with the run, so read it back out of what landed:

```bash
uv run python -m tools.book --depth 10
```

It replays the rows — ordered by the venue's `seq`, not by our clock — into the same `Book`
the collector used. The level counts it prints should match the ones the run logged at exit;
that they do is the standing check that the landed rows are enough to reproduce book state.

### Configuration

Environment only, `MTC_` prefixed — there are no command-line flags.

| | | |
|---|---|---|
| `MTC_VENUE` | `binance` | `binance`, `coinbase` or `kraken`; a venue is a process |
| `MTC_SYMBOL` | `BTCUSDT` | one symbol, in the venue's own spelling |
| `MTC_SYMBOLS` | unset | the shard: comma-separated, or `*` for every overlapping base |
| `MTC_DURATION_S` | `60` | how long the bounded run lasts |
| `MTC_OUTPUT` | `parquet` | `parquet`, `console` or `clickhouse` |
| `MTC_MODE` | `collect` | `collect`, `capture`, `replay` or `service` |
| `MTC_MAX_SYMBOLS_PER_SHARD` | `30` | blast radius of losing one connection, as a symbol count |
| `MTC_DEPTH_INTERVAL_MS` | `100` | diff channel cadence (Binance) |
| `MTC_SNAPSHOT_LIMIT` | `5000` | REST snapshot depth (Binance) |
| `MTC_DEPTH` | `1000` | book depth (Kraken — the venue has no full-depth channel) |
| `MTC_SNAPSHOT_INTERVAL_S` | `300` | refresh the snapshot this often even when healthy |
| `MTC_CLICKHOUSE_DSN` | `http://mtc:mtc@localhost:8123/mtc` | the node `compose.yaml` brings up |
| `MTC_RAW_CHANNEL` | `<venue>-depth` | capture/replay channel name |
| `MTC_REPLAY_SPEED` | unset | `1`/`10`/`100`; unset means the ceiling |
| `MTC_FAULTS` | unset | `drop,reorder,duplicate,clock_jitter,burst` |
| `MTC_FAULT_SEED` | `0` | makes any fault run reproducible |
| `MTC_LOG_FORMAT` | `json` | `console` for readable local output |

`collector/settings.py` is the full list, and each field carries the reasoning for its default.

The destination bucket stays dlt-native config (`DESTINATION__FILESYSTEM__BUCKET_URL`), so
the same code targets a local `file://` directory or `gs://` without a change.

### Capture and replay

Land a session verbatim — every frame plus the REST snapshots it took, so the capture is a
sufficient description of the session on its own:

```bash
RAW_BUCKET_URL="file://$PWD/data/capture" MTC_MODE=capture \
MTC_DURATION_S=120 MTC_SNAPSHOT_INTERVAL_S=30 \
    uv run python -m collector.main
```

Replay it cold — no socket, no REST, no clock reads:

```bash
RAW_BUCKET_URL="file://$PWD/data/capture" MTC_MODE=replay MTC_OUTPUT=console \
    uv run python -m collector.main
```

Break it on the way through. Faults perturb the record stream, never the book, so the adapter
cannot tell an injected fault from a real one:

```bash
RAW_BUCKET_URL="file://$PWD/data/capture" MTC_MODE=replay \
MTC_FAULTS=drop,reorder,duplicate MTC_FAULT_SEED=7 \
    uv run python -m collector.main
```

Same capture and same seed replays byte-identical; a different seed does not.

The other venue is the same three commands with one variable changed, which is the whole
claim the adapter boundary makes:

```bash
RAW_BUCKET_URL="file://$PWD/data/capture" \
MTC_VENUE=coinbase MTC_SYMBOL=BTC-USD MTC_MODE=capture \
MTC_DURATION_S=40 MTC_SNAPSHOT_INTERVAL_S=10 \
    uv run python -m collector.main
```

On Coinbase the snapshot arrives in band, so `MTC_SNAPSHOT_INTERVAL_S` drives an
unsubscribe/subscribe pair rather than a REST fetch — the venue never re-sends one unasked.

## Venue reconnaissance

Before an adapter is written, the venue's live channel is *looked at* rather than recalled:

```bash
uv run python -m tools.probe coinbase --duration 30   # message shapes, clocks, precision
uv run python -m tools.symbols                        # the overlapping base assets
```

`DEVELOPMENT.md` § Phase 2 records what the survey found, including the two feeds Coinbase
publishes that differ in whether a lost message is detectable at all.

## Benchmarks

Every number in this README comes from one of these, and an uncommitted number is not a claim:

```bash
uv run python -m bench.storage  kraken data/capture/kraken-p3  # ceiling with the sink in the loop
uv run python -m bench.clickhouse                              # insert paths, sort key, footprint
uv run python -m bench.replay   data/capture/binance-depth     # the replay ceiling, sink excluded
uv run python -m bench.decode   data/btcusdt-frames.jsonl      # json vs orjson vs msgspec
uv run python -m bench.checksum tests/fixtures/kraken_book_capture.jsonl
uv run python -m bench.cadence  data/btcusdt-frames.jsonl      # why the live p99 is wake-up cost
uv run python -m bench.volume   data/capture/kraken-p3         # rows/day at full symbol scale
```

`bench.storage` and `bench.clickhouse` need the local node (`docker compose up -d`).

## The SDK diff

[`data-pipeline-core`](../data-pipeline-core) was built for a batch consumer: one-shot jobs
that fetch, transform, write and exit. This repo is the second consumer and it streams, which
is the entire reason it exists — **when the SDK does not fit, the SDK changes**, and a
workaround here would have proved nothing. What follows is what that cost, including the part
that reads badly.

**What `ServiceApp` forced was the run loop, not the contracts.** `Source` was already allowed
to yield forever, `Transform` was already lazy, and `Sink.write` already took a stream rather
than a list — so an endless generator was a legal `Source` before any of this was written.
What actually broke was start → work → push → exit: metrics pushed once in a `finally` means
*never* for a process that runs for days; a job that either exited 0 or did not gives an
orchestrator nothing to probe; and a shutdown that does not drain loses the batch still open.
Three real defects, zero protocol changes. That the pipeline contracts survived a consumer
they were not designed for is the strongest evidence the repo produced about them.

**What did break was the batch sink, and it broke twice on the same surface.** `BatchSink`
shipped in Phase 4 as `BatchSink[RecordT]` with `write_batch(Sequence[RecordT])` — a
row-oriented insert path that cannot express a columnar one, because a `pa.RecordBatch` is not
a sequence of records under any spelling. Parameterising by the *batch* instead of the record
fixed it, and made both `BatchSink[Sequence[LevelRow]]` and `BatchSink[pa.RecordBatch]` legal
with one set of flush triggers. That is a second SemVer-major event on the public contract, and
it is the honest cost of having designed the first one against a single consumer's first
destination.

Also promoted, each because something here could not be written without it: `RunContext.metrics`
(nothing inside a `Source` could publish a series, so a consumer could measure something and
had nowhere to put it), `ConnectionSupervisor` (N connections behind the one-source contract,
no shared fate), `queue_depth` and `messages_dropped_total{reason}` as a deliberate widening of
the frozen metric surface, and `dlt_sink(columns=...)` to pin a landed schema — dlt infers per
load, so a load where a nullable column happens to be entirely null does not land that column
and the dataset stops being readable as one thing.

**What stayed here, and would have been the easy mistake:** symbol normalization, the venue
tables and caps, the sequencing dialects, Kraken's checksum, the REST oracle, which streams
shard onto which socket. All of it looks reusable and none of it is generic.

**Two gaps are open and neither is fixed.** A `Sink` never sees a `RunContext`, so it is the
one slot in the pipeline that can measure something and has nowhere to publish it — Phase 7 hit
this measuring receive-to-disk, which is a *sink* quantity by construction, and worked around
it by reporting at exit instead of exporting a histogram. Closing it changes `Sink.write`, a
third major event on the same surface, and it waits until something is actually asking to
scrape the number. The second is that the SDK can write a curated dataset and cannot read one
back; the `DatasetReader` that would have closed it was cut, because neither consumer has a
caller for it and building one anyway would have been the thing this section exists to argue
against.

## Development

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy                # strict
uv run pytest
```

Tests replay a recorded Binance bootstrap — twelve consecutive frames with a live REST
snapshot taken part-way through — so the splice branches are exercised against real sequence
ranges rather than invented ones. The recovery path is reached through the fault injector: a
venue will not drop packets on request, and a fault asserted by hand tests the assertion.

The Coinbase suite runs the *same* injector over a stream numbered the way that venue numbers
it — every message on the connection, acks included. That the injector needed no change to
work on a second dialect is the actual evidence the boundary holds; it perturbs capture
records and has never heard of either venue.

One caveat the fault tests state rather than hide: a fault landing on an already-untrusted
book opens no second interval, so the honest denominator for a detection rate is faults
injected against a *trusted* book.
