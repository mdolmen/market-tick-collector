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
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

import clickhouse_connect
import pyarrow as pa
from data_pipeline_core import BatchSink, Sink, WriteResult, deterministic_id

from collector.model import ARROW_SCHEMA, CLOCKS, LevelRow

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


@dataclass(frozen=True)
class WidenedTickSink:
    """Hand the Parquet tier its 128-bit ticks as ``Decimal``, not as ``int``.

    **dlt refuses a Python int wider than 64 bits, at extract, before any
    column hint is consulted** — `TypeError: Integer exceeds 64-bit range`. So
    declaring `price_ticks` as `decimal(38, 0)` in `DLT_COLUMNS` fixes the
    column's *type* and not this: the value never reaches the column. A
    `Decimal` of the same magnitude lands exactly.

    That makes it a real defect on the default output rather than a limit worth
    stating: `SCALE = 8` puts the int64 ceiling at ~9.2e10, and Kraken's
    depth-1000 `BCH/USD` book carries an ask at 886,110,000,000.00 — so
    `MTC_OUTPUT=parquet` over Kraken's full symbol set crashes the run on a
    single junk order. The ClickHouse tier never had the problem, because Arrow
    carries the int straight into a `decimal128(38, 0)`.

    A wrapper rather than a ``Transform``, because the widened row is no longer
    a ``LevelRow`` — the model says those two fields are ``int`` and it is right
    to. The conversion belongs at the boundary into dlt, which is the only
    thing that wants it, and the stream stays lazy through it.

    Applied only on the Parquet path, and per row, which is a cost this does
    not pay anywhere else — the curated tier is ClickHouse and Phase 0 measured
    `Decimal` construction at four times the JSON decode. The two tick columns
    only.
    """

    inner: Sink[Mapping[str, object]]

    def write(self, records: Iterable[LevelRow]) -> WriteResult:
        return self.inner.write(_widened(records))


def _widened(records: Iterable[LevelRow]) -> Iterator[Mapping[str, object]]:
    for row in records:
        ticks, lots = row["price_ticks"], row["size_lots"]
        if ticks is None and lots is None:
            yield row
            continue
        yield {
            **row,
            "price_ticks": None if ticks is None else Decimal(ticks),
            "size_lots": None if lots is None else Decimal(lots),
        }


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


class TimedBatchSink:
    """Wraps a ``BatchSink``, timing what its batches waited for.

    Two quantities, and they are different questions. **Receive-to-disk** is per
    row — `now - monotonic_ts` once the insert returns — and is dominated by how
    long the row's batch stayed open, which is what `NOTES.md` § *Flush
    triggers* means by calling the time trigger a latency setting. **Throughput**
    is per flush: rows and the wall clock they landed at, which is what a
    sustained rate and a burst are computed from afterwards.

    **Both rates are bucketed by when a row was *received*, not by when its
    batch landed**, and the first cut of this got it wrong in a way worth
    keeping on the page. Attributing a whole batch to its flush second reported
    a burst of exactly 50,000 rows/s on a 300s Kraken run — which is
    `batch_max_rows`, not a property of the feed. A batch that took six seconds
    to fill had all six seconds of arrivals counted into the instant it was
    written. Every row carries `monotonic_ts`, so the arrival second is right
    there and the flush time is simply the wrong clock to ask.

    **Live modes only.** A replay's `monotonic_ts` came out of a capture file
    written days ago, so the latency here would be the age of the corpus under
    a label that reads like a pipeline measurement. `collector/main.py` wires
    this for `collect` and `service`, never for `replay`.

    **Reported at exit rather than exported as a series, and the reason is a
    seam rather than a preference.** `Source.fetch(ctx)` and
    `Transform.transform(record, ctx)` both receive a `RunContext`;
    `Sink.write(records)` does not, and the run's registry is built inside
    `build_runtime`. So a sink is the one slot in the SDK that can measure
    something and has nowhere to publish it — which is the gap
    `RunContext.metrics` was added to close for the other two. Recorded in
    `NOTES.md` § *The SDK gaps*; closing it means changing `Sink.write`, which
    is a second SemVer-major event and wants a dashboard asking for it first.
    """

    def __init__(self, inner: BatchSink[pa.RecordBatch]) -> None:
        self._inner = inner
        # Per-row receive-to-disk, in seconds. At ~12,000 rows/s a ten-minute
        # run holds ~7M floats — roughly 60 MB, which is affordable for a bench
        # run and is why this is wired by mode rather than always.
        self.latencies: list[float] = []
        # Rows per arrival second, keyed by `monotonic_ts // 1e9`. A counter
        # rather than a list of timestamps: a 300s run has 300 keys whatever
        # the rate, so this stays flat where holding every arrival would not.
        self.arrivals: Counter[int] = Counter()
        self.flushes = 0

    def write_batch(self, batch: pa.RecordBatch) -> WriteResult:
        result = self._inner.write_batch(batch)
        landed = time.monotonic_ns()
        for received in batch.column("monotonic_ts").to_pylist():
            self.latencies.append((landed - received) / 1e9)
            self.arrivals[received // 1_000_000_000] += 1
        self.flushes += 1
        return result

    def close(self) -> None:
        self._inner.close()  # type: ignore[attr-defined]

    def summary(self) -> dict[str, float]:
        """Receive-to-disk percentiles, and the two rates, from one run.

        Sustained is rows over the span they *arrived* in — not the run's wall
        clock, which includes the subscribe and bootstrap before the first row
        existed. Burst is the busiest single arrival second. Both are live-run
        figures and neither is the replay ceiling.
        """
        if self.flushes < 2 or not self.arrivals:
            return {}
        rows = sum(self.arrivals.values())
        span = max(self.arrivals) - min(self.arrivals) + 1
        ordered = sorted(self.latencies)
        return {
            "flushes": self.flushes,
            "rows": rows,
            "sustained_rows_s": rows / span,
            "burst_rows_s": float(max(self.arrivals.values())),
            "receive_to_disk_p50_ms": _percentile(ordered, 0.50) * 1000,
            "receive_to_disk_p90_ms": _percentile(ordered, 0.90) * 1000,
            "receive_to_disk_p99_ms": _percentile(ordered, 0.99) * 1000,
        }


def _percentile(ordered: Sequence[float], fraction: float) -> float:
    """Nearest-rank on an already-sorted sequence; 0.0 when there is nothing."""
    if not ordered:
        return 0.0
    rank = min(int(fraction * len(ordered)), len(ordered) - 1)
    return ordered[rank]


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
