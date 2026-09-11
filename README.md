# market-tick-collector

High-volume L2 order book capture from public crypto exchange websockets, and the second
consumer of [`data-pipeline-core`](../data-pipeline-core).

This is not a crypto project. Crypto because free high-volume L2 data. No trading, no strategy, no signal. Its aim is to be a realistic use case to extract features to go in the SDK mentioned above.

## In numbers

**Three venues** — Binance spot, Coinbase Advanced Trade, Kraken v2.

**188 symbols** — every base asset quoted against all three venues' USD-ish market, resolved
against their live instrument lists rather than hand-listed. Binance and Coinbase run all 188;
Kraken runs 185, because `BONK`, `PEPE` and `SHIB` quote to nine decimal places there and this
project is fixed at eight.

**Full depth on two of the three.** Binance streams full-depth diffs at 100 ms, bootstrapped
from a `limit=5000` REST snapshot; Coinbase sends its snapshot in band on `level2`, ~4.9 MB
for BTC-USD. Kraken has no full-depth channel at all — `depth` is one of 10/25/100/500/1000 —
so its book is a top-1000 one by construction, running at the venue's ceiling.

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

Land in ClickHouse instead — a whole venue's shard. `compose.yaml` brings up the local node the sink was measured against; it is not a deployment:

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

Before an adapter is written, the venue's live channel is looked at:

```bash
uv run python -m tools.probe coinbase --duration 30   # message shapes, clocks, precision
uv run python -m tools.symbols                        # the overlapping base assets
```

## Benchmarks

Throughput, latency and footprint numbers live in `DEVELOPMENT.md`, each beside the prediction
it was graded against. These are what produce them — an uncommitted number is not a claim:

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

[`data-pipeline-core`](../data-pipeline-core) was built for one-shot batch jobs. This repo
streams, so what did not fit was changed in the SDK rather than worked around here — while
venue logic, symbol normalization and the venue caps stayed out of it. What landed:

| | |
|---|---|
| `ServiceApp` | run until stopped, health endpoint, periodic metrics push, graceful drain |
| `BatchSink[BatchT]` | parameterized by the *batch*, not the record, so a `pa.RecordBatch` is expressible. Re-parameterized from `BatchSink[RecordT]` — a second breaking change on the same surface |
| `arrow_batching_sink` | flush on rows or seconds, closing into a `pa.RecordBatch` |
| `ConnectionSupervisor` | N connections behind the one-source contract, per-connection health, no shared fate |
| `RunContext.metrics` | the run's registry, so a `Source` can publish its own series |
| `queue_depth`, `messages_dropped_total{reason}` | a deliberate widening of the frozen metric surface |
| `dlt_sink(columns=...)` | pin the landed schema, so a dataset's files agree |

**The protocols themselves were never touched.** `Source` was already allowed to yield forever
and `Sink.write` already took a stream, so an endless generator was a legal `Source` before any
of this existed. What broke was the run loop, not the contract.

**Two gaps left open.** A `Sink` never sees a `RunContext`, so it is the one slot that can
measure something and has nowhere to publish it. And the SDK can write a curated dataset and
cannot read one back — the `DatasetReader` that would have closed it was cut, neither consumer
having a caller for it.

## Development

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy                # strict
uv run pytest
```
