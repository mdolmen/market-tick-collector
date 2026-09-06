# market-tick-collector

High-volume L2 order book capture from public crypto exchange websockets, and the second
consumer of [`data-pipeline-core`](../data-pipeline-core).

This is not a crypto project. Crypto because free high-volume L2 data. No trading, no strategy, no signal. It is a data platform, and the interesting problems are book reconstruction, sequence gaps, backpressure and reconciliation.

## Status

**Phase 0 — walking skeleton.** One venue (Binance spot), one symbol, full-depth diffs,
bootstrapped over the REST snapshot splice, applied into a book, landed as a Parquet dataset,
for a bounded duration.

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

### Configuration

Environment only, `MTC_` prefixed — there are no command-line flags.

| | | |
|---|---|---|
| `MTC_SYMBOL` | `BTCUSDT` | one symbol, Binance spot |
| `MTC_DURATION_S` | `60` | how long the bounded run lasts |
| `MTC_OUTPUT` | `parquet` | `parquet` or `console` |
| `MTC_DEPTH_INTERVAL_MS` | `100` | diff channel cadence |
| `MTC_SNAPSHOT_LIMIT` | `5000` | REST snapshot depth |
| `MTC_RAW_FRAMES_PATH` | unset | dump frames verbatim as JSONL |
| `MTC_LOG_FORMAT` | `json` | `console` for readable local output |

The destination bucket stays dlt-native config (`DESTINATION__FILESYSTEM__BUCKET_URL`), so
the same code targets a local `file://` directory or `gs://` without a change.

## Benchmarks

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file://$PWD/data/l2" \
MTC_DURATION_S=300 MTC_RAW_FRAMES_PATH=data/btcusdt-frames.jsonl \
    uv run python -m collector.main

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
snapshot taken part-way through — so the splice branches and the gap-recovery path are
exercised against real sequence ranges rather than invented ones. A live run never produces a
gap; until the Phase 1 fault injector exists, those tests are the only reach into that path.
