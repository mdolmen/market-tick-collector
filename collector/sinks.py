"""Project sinks.

``ConsoleSink`` prints each record as one JSON line. Wire a run to it instead
of the storage sink to eyeball level rows before committing them to Parquet.

``ClickHouseSink`` is the curated destination, and the reason Phase 4 measured
before it wrote anything: the table below is the sort key `bench/clickhouse.py`
chose and the batch size is the one it timed. It takes Arrow batches, which is
what the spike measured and what the SDK's `arrow_batching_sink` accumulates;
the row-oriented path it replaced survives in `bench/storage.py` as the arm
that measurement is quoted against.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping

import clickhouse_connect
import pyarrow as pa
from data_pipeline_core import WriteResult, deterministic_id

from collector.model import ARROW_SCHEMA, CLOCKS

# The DDL the spike settled on. `(venue, symbol, receive_ts, seq)` beat arrival
# order on both counts that matter — 258 MB against 344 MB over ten million
# identical rows, and 16,385 rows read against 458,752 for a one-symbol,
# one-minute window, which is the read Phase 10's access API is built around.
#
# `price_ticks` and `size_lots` are `Decimal(38, 0)` — a 128-bit integer — and
# not `Int64`, because `SCALE = 8` puts the Int64 ceiling at about 9.2e10 and
# Kraken's depth-1000 `BCH/USD` book carries a real ask at 886,110,000,000.00.
# Python's unbounded int hid that until a columnar type was involved.
#
# Daily partitions because retention and the Phase 7 archive tier drop a day at
# a time. Not partitioned by venue: `venue` already leads the sort key, so
# adding it to the partition expression only multiplies parts.
SCHEMA = """
CREATE TABLE IF NOT EXISTS {table} (
    venue        LowCardinality(String),
    symbol       LowCardinality(String),
    seq          Int64,
    exchange_ts  Nullable(DateTime64(9, 'UTC')),
    receive_ts   DateTime64(9, 'UTC'),
    monotonic_ts Int64,
    action       LowCardinality(String),
    side         LowCardinality(Nullable(String)),
    price_str    Nullable(String),
    price_ticks  Nullable(Decimal(38, 0)),
    size_str     Nullable(String),
    size_lots    Nullable(Decimal(38, 0))
) ENGINE = MergeTree
PARTITION BY toDate(receive_ts)
ORDER BY (venue, symbol, receive_ts, seq)
SETTINGS non_replicated_deduplication_window = {dedup_window}
"""

# The clocks as the table declares them. The batch arrives in the model's own
# unit — integer nanoseconds — and is cast here, at the one edge that wants a
# timestamp; see `collector/model.py` on why the accumulated batch does not
# carry one. `DateTime64(9)` is stored as int64 ns, so the cast is a reinterpret
# rather than a conversion.
INSERT_SCHEMA = pa.schema(
    [
        field.with_type(pa.timestamp("ns", tz="UTC")) if field.name in CLOCKS else field
        for field in ARROW_SCHEMA
    ]
)


class ConsoleSink:
    """Print each record as one JSON line; for dev/visual confirmation."""

    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        count = 0
        for record in records:
            print(json.dumps(record, ensure_ascii=False, default=str))
            count += 1
        print(f"[console-sink] {count} record(s)")
        return WriteResult(row_count=count)


class ClickHouseSink:
    """A ``BatchSink`` over one ClickHouse table, fed Arrow batches.

    **The clocks go over as integer nanoseconds**, which is the model's unit
    everywhere and exactly how `DateTime64(9)` is stored. Handing the driver
    `datetime` objects instead would round-trip through microseconds and lose
    three digits in silence — the same loss `fromisoformat` inflicts, which is
    why Phase 2 parses RFC3339 fractions by hand rather than using it.

    **A replayed batch does not duplicate, and the mechanism is a token rather
    than a merge.** The obvious answer — `ReplacingMergeTree` on the sort key —
    is wrong here: `(venue, symbol, receive_ts, seq)` is not row-unique, since
    one frame emits every level it touched under one `seq`, so collapsing on it
    would delete real rows. Extending the key to make it unique would discard
    the layout Phase 4 measured. So dedup happens a level up, on the insert:
    each batch carries a token derived from its own contents, and a server that
    has seen that token in the partition discards the block.

    The guarantee that buys, stated exactly: **replaying the same rows in the
    same batches is a no-op.** Two things bound it. The token is content-derived,
    so identical rows must re-batch identically — true for a replay at a row
    trigger, false for a live process that died mid-batch and re-split on
    restart. And the server remembers only `dedup_window` recent blocks per
    partition, so the window has to exceed the batches one partition's replay
    produces; the default is sized for that below.

    **`WriteResult` counts rows sent, not rows kept.** A discarded block is
    invisible in the insert response, so `records_written_total` over a replay
    of already-landed data reads as though it wrote them. The alternative is a
    count query per batch on the hot path, which costs more than the number is
    worth — but the series means "rows handed to the destination", and a replay
    is the one case where that differs from what the table gained.
    """

    def __init__(self, dsn: str, table: str, *, dedup_window: int) -> None:
        self._client = clickhouse_connect.get_client(dsn=dsn)
        self._table = table
        self._client.command(
            SCHEMA.format(table=table, dedup_window=dedup_window),
        )

    def write_batch(self, batch: pa.RecordBatch) -> WriteResult:
        table = pa.Table.from_batches([batch]).cast(INSERT_SCHEMA)
        self._client.insert_arrow(
            self._table,
            table,
            settings={"insert_deduplication_token": _batch_token(batch)},
        )
        return WriteResult(row_count=batch.num_rows)

    def close(self) -> None:
        self._client.close()


def _batch_token(batch: pa.RecordBatch) -> str:
    """A dedup token that depends only on what the batch contains.

    The first and last `(venue, symbol, seq)` plus the row count. Not a hash of
    every row: the token only has to separate this batch from a *different*
    one, and the server compares the block's own checksum as well — a token
    collision between two genuinely different blocks does not merge them. Not
    the run id or a clock either, which is the whole point; either would make
    every replay mint fresh tokens and dedup nothing.
    """
    first, last = 0, batch.num_rows - 1
    return deterministic_id(
        batch.column("venue")[first],
        batch.column("symbol")[first],
        batch.column("seq")[first],
        batch.column("venue")[last],
        batch.column("symbol")[last],
        batch.column("seq")[last],
        batch.num_rows,
    )
