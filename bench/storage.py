"""The replay ceiling with the sink in the loop, and what the sink costs.

    docker compose up -d
    uv run python -m bench.storage kraken data/capture/kraken-p3

`bench/replay.py` measures decode → book apply → `LevelRow` built and says, in
its own docstring, that the "decode → book → Arrow → sink" claim in `NOTES.md`
§ *What "done" has to be able to say* is **not yet earned** because the sink is
excluded. This is the file that earns it: the same pipeline with the curated
destination attached, so the difference between the two numbers is the sink.

Two arms, and they answer different questions.

**The ceiling** is the whole path, unthrottled, with ClickHouse at the end. It
is bounded by this code and is *not* a capture rate — `NOTES.md` § *Two
numbers, never merged*. The live rate comes from a live run and is reported by
`collector/main.py` at exit; the two are never added, averaged or quoted as
one.

**The insert comparison** is Arrow against the row-oriented path the Phase 7
sink replaced. That path is not dead code kept for sentiment: the contract
change in `data-pipeline-core` was justified partly on the insert path being
faster, and a justification with no number behind it is the thing this repo
exists to not do. The row-oriented body lives here, in the arm that measures
it, and nowhere else.

**Neither arm is a storage-volume claim.** Rows come from a real capture and
land in a real table, but the corpus is minutes of one venue; bytes on disk and
rows per day are `bench/volume.py` and `bench/clickhouse.py`, which already
label their own extrapolations.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from data_pipeline_core import RunContext, arrow_batching_sink
from data_pipeline_core.ingestion.http import HttpClient

from bench.volume import sessions
from collector import adapters
from collector.capture import CaptureRecord
from collector.main import resolve_shards
from collector.model import ARROW_SCHEMA, LevelRow
from collector.router import BookRouter
from collector.settings import CollectorSettings
from collector.sinks import INSERT_SCHEMA, SCHEMA, ClickHouseSink

# Capture records held before anything is timed. Large enough that the run
# spans minutes of real book activity rather than one burst, small enough to
# hold in memory beside the rows they expand into.
_CAPTURE_RECORDS = 200_000

# The flush triggers `NOTES.md` § *Flush triggers* settled on. Not under test —
# Phase 4 measured every size from 10k to 1M at ≥881k rows/s — so the ceiling is
# reported at the size the collector actually runs.
_MAX_ROWS = 50_000
_MAX_SECONDS = 2.0

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


def capture(path: Path, limit: int) -> list[CaptureRecord]:
    """The capture itself, held in memory so every arm replays the same input.

    **Records, not rows.** An earlier cut of this bench built the level rows
    once and timed the sink over them, then printed the result as a "decode →
    book → Arrow → sink" ceiling. It was not: the decode and the book had
    already happened outside the timer, so the number described the sink alone
    while claiming the pipeline. The corpus has to start where the pipeline's
    input does, which is a capture record.
    """
    records: list[CaptureRecord] = []
    for session in sessions(path):
        for record in session:
            records.append(record)
            if len(records) >= limit:
                return records
    return records


def _router(venue: str) -> tuple[BookRouter, RunContext]:
    """A fresh router per arm: the books must not start already built."""
    settings = CollectorSettings(venue=venue, symbols="*")
    router, _ = adapters.build_router(settings, resolve_shards(settings))
    return router, RunContext.create(source_name="bench", http=cast(HttpClient, None))


def rows_from(records: Sequence[CaptureRecord], venue: str) -> list[LevelRow]:
    """The level rows a capture yields, for the arms that time only the sink."""
    router, ctx = _router(venue)
    return [row for record in records for row in router.transform(record, ctx)]


class RowOrientedSink:
    """The insert path Phase 7 replaced, kept as the arm it is compared against.

    Verbatim from `collector/sinks.py` before the Arrow rewrite: a list of
    lists, transposed into columns by the driver. It takes the Arrow batch and
    converts *back* to rows, which costs it something the original did not pay —
    so the ratio this produces is, if anything, generous to Arrow. Said out loud
    rather than left for a reader to notice.
    """

    def __init__(self, client: Client, table: str) -> None:
        self._client = client
        self._table = table

    def write_batch(self, batch: pa.RecordBatch) -> Any:
        rows = [list(row.values()) for row in batch.to_pylist()]
        self._client.insert(self._table, rows, column_names=list(_COLUMNS))
        return None


def _create(client: Client, table: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {table}")
    client.command(SCHEMA.format(table=table, dedup_window=0))


def ceiling(records: Sequence[CaptureRecord], venue: str, client: Client) -> None:
    """The whole path twice: without the sink, then with it.

    Both arms replay the same records through the same transform, so the delta
    between them is the sink and nothing else. Reporting only the second number
    would leave "what did the sink cost?" to be answered by comparing against
    `bench/replay.py`, which runs a different venue over a different corpus.
    """
    warmup = records[: min(len(records) // 10, 2_000)]

    router, ctx = _router(venue)
    for record in warmup:
        for _ in router.transform(record, ctx):
            pass
    router, ctx = _router(venue)
    bare_rows = 0
    started = time.monotonic()
    for record in records:
        for _ in router.transform(record, ctx):
            bare_rows += 1
    bare = time.monotonic() - started

    _create(client, "bench_ceiling")
    sink = ClickHouseSink(_dsn(), "bench_ceiling", dedup_window=0)
    writer = arrow_batching_sink(
        sink, schema=ARROW_SCHEMA, max_rows=_MAX_ROWS, max_seconds=_MAX_SECONDS
    )
    router, ctx = _router(venue)
    writer.write(row for record in warmup for row in router.transform(record, ctx))

    router, ctx = _router(venue)
    started = time.monotonic()
    result = writer.write(
        row for record in records for row in router.transform(record, ctx)
    )
    full = time.monotonic() - started
    sink.close()

    print(f"\nreplay ceiling — {len(records):,} records -> {bare_rows:,} rows")
    print("               excludes the socket path; both arms, same corpus\n")
    print(f"  {'arm':<28} {'rows/s':>12}  {'µs/row':>8}")
    print(
        f"  {'decode -> book -> row':<28} {bare_rows / bare:>12,.0f}  "
        f"{bare / bare_rows * 1e6:>8.2f}"
    )
    print(
        f"  {'+ Arrow + sink':<28} {result.row_count / full:>12,.0f}  "
        f"{full / result.row_count * 1e6:>8.2f}"
    )
    # Both framings, because they are different numbers and either alone
    # invites the other to be assumed: throughput falls by one fraction while
    # per-row time rises by a larger one.
    bare_rate, full_rate = bare_rows / bare, result.row_count / full
    drop = (bare_rate - full_rate) / bare_rate * 100
    print(f"\n  ceiling drops {drop:.1f}% in rows/s")
    print(f"  per-row time rises {(full - bare) / bare * 100:.1f}%")
    print("\n  Not a capture rate. The live rate is a different number and is")
    print("  reported by `collector.main` at the end of a live run.")


def inserts(rows: Sequence[LevelRow], client: Client, table: str) -> None:
    """Arrow against the row-oriented path, on identical batches."""
    batches = _batches(rows)
    total = sum(batch.num_rows for batch in batches)
    print(f"\ninsert path — {total:,} rows in {len(batches)} batches of {_MAX_ROWS:,}")
    print(f"  {'path':<16} {'rows/s':>12}  {'ms/batch':>10}")

    for label, factory in (
        ("arrow", lambda: ClickHouseSink(_dsn(), table, dedup_window=0)),
        ("row-oriented", lambda: RowOrientedSink(client, table)),
    ):
        _create(client, table)
        sink = factory()
        sink.write_batch(batches[0])  # warm the connection, discard
        _create(client, table)

        started = time.monotonic()
        for batch in batches:
            sink.write_batch(batch)
        elapsed = time.monotonic() - started
        print(
            f"  {label:<16} {total / elapsed:>12,.0f}  "
            f"{elapsed / len(batches) * 1000:>10.1f}"
        )


def conversion(rows: Sequence[LevelRow]) -> None:
    """What `from_pylist` costs, reported apart from the insert it feeds.

    `arrow_batching_sink`'s docstring says a column-wise builder is more code
    for a saving that has to be measured against the insert rather than
    assumed. This is that measurement, and it is where the answer changes if it
    ever changes.
    """
    chunk = list(rows[:_MAX_ROWS])
    pa.RecordBatch.from_pylist(chunk, schema=ARROW_SCHEMA)  # warm

    started = time.monotonic()
    pa.RecordBatch.from_pylist(chunk, schema=ARROW_SCHEMA)
    elapsed = time.monotonic() - started
    print(f"\nbatch conversion — dict rows -> pa.RecordBatch, {len(chunk):,} rows")
    print(f"  rows/s       {len(chunk) / elapsed:,.0f}")
    print(f"  µs/row       {elapsed / len(chunk) * 1e6:.2f}")


def _batches(rows: Sequence[LevelRow]) -> list[pa.RecordBatch]:
    return [
        pa.RecordBatch.from_pylist(
            cast(list[dict[str, Any]], list(rows[start : start + _MAX_ROWS])),
            schema=ARROW_SCHEMA,
        )
        for start in range(0, len(rows) - _MAX_ROWS + 1, _MAX_ROWS)
    ]


def _dsn() -> str:
    return CollectorSettings().clickhouse_dsn


def main(argv: Sequence[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <venue> <capture-dir-or-file>", file=sys.stderr)
        return 2
    venue, path = argv[1], Path(argv[2])

    records = capture(path, _CAPTURE_RECORDS)
    rows = rows_from(records, venue)
    if len(rows) < _MAX_ROWS * 2:
        print(f"capture produced only {len(rows)} rows", file=sys.stderr)
        return 1
    print(f"corpus         {len(records):,} capture records from {path}")
    print(f"               {len(rows):,} level rows")
    print(
        f"               INSERT_SCHEMA clocks: {INSERT_SCHEMA.field('receive_ts').type}"
    )

    client = get_client(dsn=_dsn())
    try:
        ceiling(records, venue, client)
        inserts(rows, client, "bench_insert")
        conversion(rows)
    finally:
        for table in ("bench_ceiling", "bench_insert"):
            client.command(f"DROP TABLE IF EXISTS {table}")
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
