"""The Parquet tier: what lands, and that a junk order no longer kills the run.

Two defects, both found in Phase 7 and both invisible until a real symbol set
reached them. `DLT_COLUMNS` pins a schema dlt would otherwise infer per load,
and `WidenedTickSink` gets a 128-bit tick past dlt's extract step.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from data_pipeline_core import dlt_sink

from collector.model import DLT_COLUMNS, LevelRow
from collector.sinks import WidenedTickSink

# Kraken's depth-1000 `BCH/USD` book carries a real ask here — a junk order far
# from the touch, 8.9e19 once scaled at `SCALE = 8`, and past the int64 ceiling
# of ~9.2e10 that `bench/clickhouse.py` found the hard way.
_JUNK_ORDER_TICKS = 886_110_000_000_00000000


@pytest.fixture
def bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    made = tmp_path / "bucket"
    made.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", made.as_uri())
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return made


def _row(*, ticks: int) -> LevelRow:
    return LevelRow(
        venue="kraken",
        symbol="BCH/USD",
        seq=1,
        # Null, as every Binance REST snapshot row is: the column Phase 0 lost.
        exchange_ts=None,
        receive_ts=1_757_000_000_123_456_789,
        monotonic_ts=5,
        action="snapshot",
        side="ask",
        price_str="886110000000.00",
        price_ticks=ticks,
        size_str="2.0",
        size_lots=200_000_000,
    )


def _landed(bucket: Path, dataset: str) -> Path:
    files = [f for f in (bucket / dataset).rglob("*.parquet") if "levels" in f.parts]
    assert files, f"no parquet landed for {dataset}"
    return files[0]


def _sink(dataset: str) -> WidenedTickSink:
    return WidenedTickSink(
        dlt_sink(
            dataset=dataset,
            destination="filesystem",
            table_name="levels",
            columns=DLT_COLUMNS,
        )
    )


def test_the_pinned_schema_lands_every_column(bucket: Path) -> None:
    _sink("pinned").write([_row(ticks=150_000_000)])

    landed = pq.read_schema(_landed(bucket, "pinned"))
    assert set(DLT_COLUMNS) <= set(landed.names)


def test_an_all_null_column_is_dropped_without_the_pin(bucket: Path) -> None:
    """The Phase 0 failure, reproduced — so the fix is not asserting itself."""
    dlt_sink(dataset="loose", destination="filesystem", table_name="levels").write(
        [_row(ticks=150_000_000)]
    )

    assert "exchange_ts" not in pq.read_schema(_landed(bucket, "loose")).names


def test_the_tick_columns_are_128_bit(bucket: Path) -> None:
    """`bigint` would be a 64-bit Parquet column — the ceiling, one tier over."""
    _sink("wide").write([_row(ticks=150_000_000)])

    landed = pq.read_schema(_landed(bucket, "wide"))
    assert str(landed.field("price_ticks").type) == "decimal128(38, 0)"
    assert str(landed.field("size_lots").type) == "decimal128(38, 0)"


def test_a_junk_order_lands_instead_of_failing_the_run(bucket: Path) -> None:
    """dlt rejects an int this wide at extract, before any hint is consulted."""
    _sink("junk").write([_row(ticks=_JUNK_ORDER_TICKS)])

    table = pq.read_table(_landed(bucket, "junk"))
    assert table.column("price_ticks").to_pylist()[0] == Decimal(_JUNK_ORDER_TICKS)


def test_a_control_row_with_no_ticks_passes_through(bucket: Path) -> None:
    """A `gap` row has null level fields; the widening must not invent values."""
    row = _row(ticks=150_000_000)
    row["action"] = "gap"
    row["side"] = None
    row["price_str"] = None
    row["price_ticks"] = None
    row["size_str"] = None
    row["size_lots"] = None

    _sink("control").write([row])

    table = pq.read_table(_landed(bucket, "control"))
    assert table.column("price_ticks").to_pylist() == [None]
    assert table.column("size_lots").to_pylist() == [None]


def test_nanoseconds_survive_the_parquet_round_trip(bucket: Path) -> None:
    _sink("clock").write([_row(ticks=150_000_000)])

    table = pq.read_table(_landed(bucket, "clock"))
    assert table.column("receive_ts").to_pylist()[0] == 1_757_000_000_123_456_789
