# market-tick-collector

High-volume L2 order book capture from public crypto exchange websockets, and the second
consumer of [`data-pipeline-core`](../data-pipeline-core).

This is not a crypto project. Crypto because free high-volume L2 data. No trading, no strategy, no signal. It is a data platform, and the interesting problems are book reconstruction, sequence gaps, backpressure and reconciliation.

## Status

**Phase 1 — capture & replay harness.** One venue (Binance spot), one symbol, full-depth
diffs, bootstrapped over the REST snapshot splice, applied into a book, landed as a Parquet
dataset. The collector is split along the SDK's ingest/transform boundary, so a session can
be captured verbatim and replayed from disk — with faults injected — through the *same* book
code a socket drives.

Two numbers, and they are never merged:

| | |
|---|---|
| **Live capture rate** — bounded by the venue, not by this code | 10.0 frames/s on `btcusdt@depth@100ms` |
| **Replay ceiling** — decode → book apply → row built, excluding the socket *and* the sink | **37,425 frames/s**, 563,088 rows/s |

The second answers "how fast is the code" and is not a capture rate. `DEVELOPMENT.md` has the
breakdown, the prediction that was written before the run, and where it was wrong.

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
| `MTC_SYMBOL` | `BTCUSDT` | one symbol, Binance spot |
| `MTC_DURATION_S` | `60` | how long the bounded run lasts |
| `MTC_OUTPUT` | `parquet` | `parquet` or `console` |
| `MTC_DEPTH_INTERVAL_MS` | `100` | diff channel cadence |
| `MTC_SNAPSHOT_LIMIT` | `5000` | REST snapshot depth |
| `MTC_SNAPSHOT_INTERVAL_S` | `300` | land a snapshot this often even when healthy |
| `MTC_MODE` | `collect` | `collect`, `capture` or `replay` |
| `MTC_RAW_CHANNEL` | `binance-depth` | capture/replay channel name |
| `MTC_REPLAY_SPEED` | unset | `1`/`10`/`100`; unset means the ceiling |
| `MTC_FAULTS` | unset | `drop,reorder,duplicate,clock_jitter,burst` |
| `MTC_FAULT_SEED` | `0` | makes any fault run reproducible |
| `MTC_LOG_FORMAT` | `json` | `console` for readable local output |

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

## Benchmarks

```bash
uv run python -m bench.replay  data/capture/binance-depth  # the replay ceiling
uv run python -m bench.decode  data/btcusdt-frames.jsonl   # json vs orjson vs msgspec
uv run python -m bench.cadence data/btcusdt-frames.jsonl   # why the live p99 is wake-up cost
```

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

One caveat the fault tests state rather than hide: a fault landing on an already-untrusted
book opens no second interval, so the honest denominator for a detection rate is faults
injected against a *trusted* book.
