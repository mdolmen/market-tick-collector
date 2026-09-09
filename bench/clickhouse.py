"""What the ClickHouse insert path costs, and which sort key to pay it with.

    docker compose up -d
    uv run python -m bench.clickhouse kraken data/capture/kraken-p3

`TODO.md` Phase 4 puts this **before** the batch `Sink` contract, because the
contract's flush triggers are a property of the insert path and not a taste
decision. Two questions, both answered against a real capture:

1. **Which `ORDER BY`** — the reads Phase 10 has to serve are "one symbol over
   a time window" and "a whole day, aggregated by symbol". A sort key is the
   only index a MergeTree has, so this is the schema decision that matters.
2. **What batch size** — the rows/s a batch buys, *and* the parts it creates.
   Parts are usually the binding constraint: ClickHouse merges in the
   background and a stream of small inserts outruns the merges long before it
   outruns the disk.

**The corpus is real and the volume is not.** Rows come from replaying a Phase 3
capture through `BookRouter` — 184 genuine symbols, genuine price series,
genuine per-symbol rate skew. That corpus is then tiled forward in time to
reach the load size, so absolute bytes/row are optimistic: a tiled day repeats
one price series and compresses better than a real one would. The *ranking* of
two sort keys over identical rows survives that; the bytes/row figure does not
and is not a claim about a day of storage.

**One venue per run**, so `venue` has cardinality one in the loaded table.
`symbol` is the column that actually discriminates in both candidates, so this
costs the comparison nothing — but it does mean the leading `venue` column
compresses to nothing, and the byte totals should be read with that in mind.

A third candidate, `(venue, symbol, side, price_ticks, receive_ts)`, is not
measured. It clusters a symbol's rows by price level, which is the shape that
would compress the price columns hardest — and it is still wrong twice over:
`side` and `price_ticks` are null on control rows, so it needs
`allow_nullable_key`, and it scatters any time-range read across the whole
partition. Not worth a load to find that out.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from bench.volume import sessions
from collector import adapters
from collector.main import resolve_shards
from collector.model import LevelRow
from collector.settings import CollectorSettings

# Rows loaded per sort-key candidate. Two orders of magnitude below the ~10⁹
# rows/day `bench/volume.py` measures, and deliberately so: this sizes a
# schema on a laptop, it does not rehearse a day.
_LOAD_ROWS = 10_000_000

# Rows read out of the capture before tiling starts. Large enough that a tile
# spans minutes of real book activity rather than seconds of one burst.
_CORPUS_ROWS = 1_000_000

# The insert sizes the flush trigger gets chosen from, and how many rows each
# one is measured over.
_BATCH_SIZES = (10_000, 50_000, 250_000, 1_000_000)
_THROUGHPUT_ROWS = 2_000_000

# The batch the sort-key candidates are loaded with. Not under test here; it
# only has to be big enough not to dominate what is.
_LOAD_BATCH = 250_000

_CANDIDATES = {
    "arrival": "(receive_ts, venue, symbol)",
    "instrument_time": "(venue, symbol, receive_ts, seq)",
}

# `LowCardinality(String)` rather than `Enum8` for the four small-alphabet
# columns. An enum is a schema migration every time a venue, a symbol or an
# action is added; LowCardinality gets the same dictionary encoding with none
# of that, and the difference at this row count is noise.
_COLUMNS = """
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
"""

# **`Int64` cannot hold a scaled tick, and this is where that was found.**
# `SCALE = 8` puts the ceiling at (2^63 - 1)/10^8, about 9.2e10, and Kraken's
# depth-1000 `BCH/USD` book carries an ask at 886,110,000,000.00 — a junk order
# parked far from the touch, entirely real, and 8.9e19 once scaled. Python's
# int does not care and every test to date used one symbol near the touch, so
# nothing had reached a columnar type before now.
#
# `Decimal(38, 0)` is a 128-bit integer with a decimal face: exact, wide enough
# for any price a venue can quote at this scale, and it costs 16 bytes a value
# instead of 8. The alternative — keep `Int64` and refuse the row — would mean
# one junk order on one symbol killing the run, which is the trade `scaled_int`
# already refuses to make in the other direction.
_TICKS = pa.decimal128(38, 0)

# Arrow holds the clocks as int64 nanoseconds — the model's own unit — and they
# are cast to a timestamp only when a batch is handed to the server. Tiling is
# integer addition on those columns, so doing it in the native unit keeps the
# arithmetic exact and the cast zero-copy.
_CLOCKS = ("exchange_ts", "receive_ts", "monotonic_ts")
_SCHEMA = pa.schema(
    [
        ("venue", pa.string()),
        ("symbol", pa.string()),
        ("seq", pa.int64()),
        ("exchange_ts", pa.int64()),
        ("receive_ts", pa.int64()),
        ("monotonic_ts", pa.int64()),
        ("action", pa.string()),
        ("side", pa.string()),
        ("price_str", pa.string()),
        ("price_ticks", _TICKS),
        ("size_str", pa.string()),
        ("size_lots", _TICKS),
    ]
)


def corpus(venue: str, path: Path, limit: int) -> pa.Table:
    """Replay a capture into an Arrow table of real level rows."""
    settings = CollectorSettings(venue=venue, symbols="*")
    router, _ = adapters.build_router(settings, resolve_shards(settings))
    ctx = RunContext.create(source_name="bench", http=cast(HttpClient, None))

    rows: list[LevelRow] = []
    for session in sessions(path):
        for record in session:
            rows.extend(router.transform(record, ctx))
            if len(rows) >= limit:
                return pa.Table.from_pylist(cast(list[dict[str, Any]], rows), _SCHEMA)
    return pa.Table.from_pylist(cast(list[dict[str, Any]], rows), _SCHEMA)


def _shift(table: pa.Table, tile: int, span_ns: int, seq_span: int) -> pa.Table:
    """One tile of the corpus, moved forward in time and in sequence.

    Every clock moves by the same delta so the three stay consistent with each
    other, and `seq` moves past the corpus's own range so a tiled row is never
    confused with the row it was copied from.
    """
    if tile == 0:
        return table
    for column in _CLOCKS:
        index = table.schema.get_field_index(column)
        shifted = pc.add(table.column(column), pa.scalar(tile * span_ns, pa.int64()))
        table = table.set_column(index, column, shifted)
    index = table.schema.get_field_index("seq")
    seq = pc.add(table.column("seq"), pa.scalar(tile * seq_span, pa.int64()))
    return table.set_column(index, "seq", seq)


def batches(table: pa.Table, total: int, size: int) -> Iterator[pa.Table]:
    """Tile the corpus forward until `total` rows have been yielded."""
    receive = table.column("receive_ts")
    span_ns = int(pc.max(receive).as_py() - pc.min(receive).as_py()) + 1
    seq_span = int(pc.max(table.column("seq")).as_py()) + 1

    held: list[pa.Table] = []
    rows = emitted = 0
    tile = 0
    while emitted < total:
        held.append(_shift(table, tile, span_ns, seq_span))
        rows += table.num_rows
        tile += 1
        while rows >= size and emitted < total:
            combined = pa.concat_tables(held)
            take = min(size, total - emitted)
            yield _for_insert(combined.slice(0, take))
            held = [combined.slice(take)]
            rows -= take
            emitted += take


def _for_insert(table: pa.Table) -> pa.Table:
    """Cast the clocks to the timestamps the table's columns are declared as."""
    stamp = pa.timestamp("ns", tz="UTC")
    for column in _CLOCKS:
        index = table.schema.get_field_index(column)
        field = table.schema.field(index).with_type(stamp)
        table = table.set_column(index, field, table.column(column).cast(stamp))
    return table


def create(client: Client, name: str, order_by: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {name}")
    client.command(
        f"CREATE TABLE {name} ({_COLUMNS}) ENGINE = MergeTree "
        f"PARTITION BY toDate(receive_ts) ORDER BY {order_by}"
    )


def load(client: Client, name: str, table: pa.Table, total: int, size: int) -> float:
    """Insert `total` rows in batches of `size`; return seconds spent inserting."""
    elapsed = 0.0
    for batch in batches(table, total, size):
        started = time.monotonic()
        client.insert_arrow(name, batch)
        elapsed += time.monotonic() - started
    return elapsed


def _row(client: Client, sql: str) -> Sequence[Any]:
    """One row of a one-row query, which is all this bench ever asks for."""
    return client.query(sql).result_rows[0]


def parts(client: Client, name: str) -> int:
    """Active parts, which is a merge-pressure reading and not a part count.

    Background merges start during the load, so this is what survived them
    rather than what the inserts created. That is the quantity worth knowing —
    a batch size whose parts merge away as fast as it makes them is a batch
    size the server is keeping up with.
    """
    return int(
        _row(
            client,
            "SELECT count() FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{name}' AND active",
        )[0]
    )


def footprint(client: Client, name: str) -> tuple[int, int]:
    """Compressed and uncompressed bytes of a table's active parts."""
    row = _row(
        client,
        "SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) "
        f"FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{name}' AND active",
    )
    return int(row[0] or 0), int(row[1] or 0)


def read_rows(client: Client, sql: str) -> tuple[int, float]:
    """Rows the server had to read to answer, and how long it took.

    Rows read is the number that says whether the sort key did any work.
    Wall time on a warm single-node laptop mostly measures the page cache.
    """
    started = time.monotonic()
    result = client.query(sql)
    elapsed = time.monotonic() - started
    return int(result.summary.get("read_rows", 0)), elapsed


def probe(client: Client, name: str, venue: str, symbol: str) -> None:
    """The two reads Phase 10's access API is built around."""
    window = _row(
        client,
        f"SELECT min(receive_ts) + INTERVAL 60 SECOND, min(receive_ts) FROM {name}",
    )
    queries = {
        "one symbol, one minute": (
            f"SELECT count() FROM {name} WHERE venue = '{venue}' "
            f"AND symbol = '{symbol}' AND receive_ts BETWEEN '{window[1]}' "
            f"AND '{window[0]}'"
        ),
        "whole load, by symbol": f"SELECT symbol, count() FROM {name} GROUP BY symbol",
    }
    for label, sql in queries.items():
        rows, elapsed = read_rows(client, sql)
        print(f"    {label:<24} {rows:>12,} rows read   {elapsed * 1000:7.0f} ms")


def sort_keys(client: Client, table: pa.Table, venue: str, symbol: str) -> None:
    print(f"\nsort key — {_LOAD_ROWS:,} rows each, identical rows, same partitioning")
    for name, order_by in _CANDIDATES.items():
        create(client, name, order_by)
        load(client, name, table, _LOAD_ROWS, _LOAD_BATCH)
        client.command(f"OPTIMIZE TABLE {name} FINAL")
        compressed, raw = footprint(client, name)
        print(f"\n  {name}  ORDER BY {order_by}")
        print(
            f"    {'on disk':<24} {compressed / 1e6:>9,.0f} MB   "
            f"{compressed / _LOAD_ROWS:.1f} bytes/row, {raw / compressed:.1f}x"
        )
        probe(client, name, venue, symbol)


def throughput(client: Client, table: pa.Table) -> None:
    print(f"\ninsert — {_THROUGHPUT_ROWS:,} rows per batch size, fresh table each")
    print(f"  {'batch':>9}  {'rows/s':>10}  {'merged to':>9}  {'rows/part':>10}")
    for size in _BATCH_SIZES:
        name = f"ins_{size}"
        create(client, name, _CANDIDATES["instrument_time"])
        elapsed = load(client, name, table, _THROUGHPUT_ROWS, size)
        count = parts(client, name)
        print(
            f"  {size:>9,}  {_THROUGHPUT_ROWS / elapsed:>10,.0f}  {count:>6,}  "
            f"{_THROUGHPUT_ROWS // max(count, 1):>10,}"
        )
        client.command(f"DROP TABLE {name}")


def main(argv: Sequence[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <venue> <capture-dir-or-file>", file=sys.stderr)
        return 2
    venue, path = argv[1], Path(argv[2])
    table = corpus(venue, path, _CORPUS_ROWS)
    if not table.num_rows:
        print("capture produced no rows", file=sys.stderr)
        return 1
    symbol = str(table.column("symbol")[0])
    print(f"corpus     {table.num_rows:,} real rows, tiled to {_LOAD_ROWS:,}")

    client = get_client(host="localhost", port=8123, username="mtc", password="mtc")
    try:
        sort_keys(client, table, venue, symbol)
        throughput(client, table)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
