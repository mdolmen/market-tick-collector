"""Project sinks.

``ConsoleSink`` prints each record as one JSON line. Wire a run to it instead
of the storage sink to eyeball level rows before committing them to Parquet.

``ClickHouseSink`` is the curated destination, and the reason Phase 4 measured
before it wrote anything: the table below is the sort key `bench/clickhouse.py`
chose and the batch size is the one it timed.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence

import clickhouse_connect
from data_pipeline_core import WriteResult

from collector.model import LevelRow

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
"""

_COLUMNS = (
    "venue",
    "symbol",
    "seq",
    "exchange_ts",
    "receive_ts",
    "monotonic_ts",
    "action",
    "side",
    "price_str",
    "price_ticks",
    "size_str",
    "size_lots",
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
    """A ``BatchSink`` over one ClickHouse table.

    Row-oriented, which is not where this ends up: Phase 7 replaces the body of
    ``write_batch`` with the Arrow path the spike actually measured, and adds
    the rotating-Parquet archive tier beside it. What this proves now is the
    contract — that a batch-shaped destination fits behind ``batching_sink``
    without the apps knowing — and that the DDL above accepts real rows.

    **The clocks go over as integer nanoseconds**, which is the model's unit
    everywhere and exactly how `DateTime64(9)` is stored. Handing the driver
    `datetime` objects instead would round-trip through microseconds and lose
    three digits in silence — the same loss `fromisoformat` inflicts, which is
    why Phase 2 parses RFC3339 fractions by hand rather than using it.
    """

    def __init__(self, dsn: str, table: str) -> None:
        self._client = clickhouse_connect.get_client(dsn=dsn)
        self._table = table
        self._client.command(SCHEMA.format(table=table))

    def write_batch(self, records: Sequence[LevelRow]) -> WriteResult:
        rows = [
            [row[column] for column in _COLUMNS]  # type: ignore[literal-required]
            for row in records
        ]
        self._client.insert(self._table, rows, column_names=list(_COLUMNS))
        return WriteResult(row_count=len(rows))

    def close(self) -> None:
        self._client.close()
