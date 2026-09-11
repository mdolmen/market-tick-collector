"""The curated sink against a real node: `docker compose up -d` to run these.

Skipped when nothing answers on the configured DSN, because the alternative —
mocking `clickhouse_connect` — would assert that this code calls the driver the
way this code calls the driver. Every claim here is about what the *server*
does with what it is sent: that nanoseconds survive a `DateTime64(9)` round
trip, that an all-null column still lands as a column, and that a replayed
batch is discarded. A fake cannot answer any of the three.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from data_pipeline_core import arrow_batching_sink

from collector.model import ARROW_SCHEMA, LevelRow
from collector.settings import CollectorSettings
from collector.sinks import ClickHouseSink

_DSN = CollectorSettings().clickhouse_dsn
_TABLE = "test_levels"

# A nanosecond value whose last three digits are non-zero, so a round trip
# through microseconds — the loss `fromisoformat` inflicts and the reason the
# clocks stay integers — shows up as a changed number rather than as nothing.
_RECEIVE_NS = 1_757_000_000_123_456_789


def _reachable() -> bool:
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(dsn=_DSN)
    except Exception:
        return False
    try:
        client.command("SELECT 1")
        return True
    except Exception:
        return False
    finally:
        client.close()


pytestmark = pytest.mark.skipif(
    not _reachable(), reason=f"no ClickHouse at {_DSN}; `docker compose up -d`"
)


def _rows(count: int, *, action: str = "set") -> list[LevelRow]:
    """Level rows with the control-row fields null, as a `gap` row has them."""
    level = action not in ("gap",)
    return [
        LevelRow(
            venue="kraken",
            symbol="BTC/USD",
            seq=i,
            # Null on every row: the column dlt would have dropped, and the
            # shape Binance's REST snapshot actually produces.
            exchange_ts=None,
            receive_ts=_RECEIVE_NS + i,
            monotonic_ts=i,
            action=action,  # type: ignore[typeddict-item]
            side="bid" if level else None,
            price_str="1.5" if level else None,
            price_ticks=150_000_000 if level else None,
            size_str="2.0" if level else None,
            size_lots=200_000_000 if level else None,
        )
        for i in range(count)
    ]


@pytest.fixture
def sink() -> Iterator[ClickHouseSink]:
    made = ClickHouseSink(_DSN, _TABLE, dedup_window=1000)
    made._client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    made.close()
    made = ClickHouseSink(_DSN, _TABLE, dedup_window=1000)
    yield made
    made._client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    made.close()


def _count(sink: ClickHouseSink) -> int:
    return int(sink._client.query(f"SELECT count() FROM {_TABLE}").result_rows[0][0])


def test_an_arrow_batch_lands(sink: ClickHouseSink) -> None:
    written = arrow_batching_sink(
        sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60
    ).write(_rows(250))

    assert written.row_count == 250
    assert _count(sink) == 250


def test_nanoseconds_survive_the_round_trip(sink: ClickHouseSink) -> None:
    """The whole reason the clocks stay integers until this one cast."""
    arrow_batching_sink(sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60).write(
        _rows(1)
    )

    landed = sink._client.query(
        f"SELECT toUnixTimestamp64Nano(receive_ts) FROM {_TABLE}"
    ).result_rows[0][0]
    assert landed == _RECEIVE_NS


def test_an_all_null_column_lands_as_nulls_not_as_nothing(sink: ClickHouseSink) -> None:
    arrow_batching_sink(sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60).write(
        _rows(10)
    )

    nulls = sink._client.query(
        f"SELECT countIf(exchange_ts IS NULL) FROM {_TABLE}"
    ).result_rows[0][0]
    assert nulls == 10


def test_a_control_row_lands_with_no_price_level(sink: ClickHouseSink) -> None:
    """A `gap` row describes no level, so its level columns are null."""
    arrow_batching_sink(sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60).write(
        _rows(5, action="gap")
    )

    row = sink._client.query(
        f"SELECT countIf(side IS NULL AND price_ticks IS NULL) FROM {_TABLE}"
    ).result_rows[0][0]
    assert row == 5


def test_replaying_the_same_batches_does_not_duplicate(sink: ClickHouseSink) -> None:
    """The Phase 7 idempotency claim, and the shape it is claimed in.

    The same rows through the same row trigger re-batch identically, so every
    token repeats and every block is discarded. Two `write` calls rather than
    one, because that is what a re-run is.
    """
    rows = _rows(250)
    writer = arrow_batching_sink(
        sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60
    )

    writer.write(rows)
    writer.write(rows)

    assert _count(sink) == 250


def test_a_different_batching_of_the_same_rows_does_duplicate(
    sink: ClickHouseSink,
) -> None:
    """The bound on the claim above, asserted so it cannot be overstated.

    The token is derived from the batch's own edges and size, so re-splitting
    the identical rows mints different tokens and the server has no way to know
    it has seen these rows. This is the live-restart case: a process that died
    mid-batch resumes on a different boundary. It is a real limit, not a bug —
    a per-row key would fix it and would cost the measured sort key.
    """
    rows = _rows(250)

    arrow_batching_sink(sink, schema=ARROW_SCHEMA, max_rows=100, max_seconds=60).write(
        rows
    )
    arrow_batching_sink(sink, schema=ARROW_SCHEMA, max_rows=125, max_seconds=60).write(
        rows
    )

    assert _count(sink) == 500
