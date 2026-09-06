"""The Coinbase dialect, and the two bugs its differences from Binance caused.

`test_faults.py` proves the fault machinery over Binance; this proves the same
machinery over a venue whose snapshot arrives in band and whose sequence counts
every message on the connection. That the `FaultInjector` needs no change to
work here is the actual evidence the normalization boundary landed — it
perturbs `CaptureRecord`s and has never heard of either venue.
"""

from __future__ import annotations

import json
from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.base import BootstrapOutcome
from collector.adapters.coinbase import CoinbaseAdapter, _ns
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.replay import FaultConfig, FaultInjector
from collector.transform import BookTransform
from tests.conftest import coinbase_capture, coinbase_control, coinbase_frame

_FRAMES = 60
_SNAPSHOT_EVERY = 10
_STREAM = "level2:BTC-USD"


def _run(records: list[CaptureRecord]) -> tuple[list[LevelRow], BookTransform]:
    transform = BookTransform(symbol="BTC-USD", adapter=CoinbaseAdapter())
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    rows = [row for record in records for row in transform.transform(record, ctx)]
    return rows, transform


def _session() -> list[CaptureRecord]:
    return coinbase_capture(count=_FRAMES, snapshot_every=_SNAPSHOT_EVERY)


# --- parsing ----------------------------------------------------------------


def test_offer_becomes_ask_at_the_boundary() -> None:
    # The venue's word is "offer"; the model has exactly one pair of sides, and
    # a downstream consumer must never see a venue's vocabulary.
    adapter = CoinbaseAdapter()
    update = adapter.parse_frame(
        coinbase_frame(sequence=7, index=1), receive_ts=0, monotonic_ts=0
    )

    assert len(update.bids) == 1
    assert len(update.asks) == 1


def test_a_single_sequence_number_is_a_range_of_one() -> None:
    adapter = CoinbaseAdapter()
    update = adapter.parse_frame(
        coinbase_frame(sequence=7, index=1), receive_ts=0, monotonic_ts=0
    )

    assert update.first_seq == update.final_seq == 7
    assert adapter.sequence_ids(coinbase_frame(sequence=7, index=1)) == (7, 7)


def test_nanoseconds_survive_the_clock_conversion() -> None:
    # `datetime.fromisoformat` parses this happily and silently drops the last
    # three digits; the model's contract is ns throughout, so it is parsed here.
    assert _ns("2026-09-06T10:00:00.123456789Z") % 1_000_000_000 == 123_456_789
    assert _ns("2026-09-06T10:00:00.000000001Z") % 1_000_000_000 == 1
    # Fewer digits than nanoseconds pad rather than shift.
    assert _ns("2026-09-06T10:00:00.5Z") % 1_000_000_000 == 500_000_000


def test_the_clock_is_read_as_utc_not_local_time() -> None:
    # A bare timestamp through `fromisoformat` is interpreted in the machine's
    # zone, which would skew every exchange_ts by the local offset — silently,
    # and differently in a container than on a laptop.
    assert _ns("1970-01-01T00:00:00.000000000Z") == 0


def test_control_traffic_is_classified_apart_from_the_book() -> None:
    adapter = CoinbaseAdapter()

    assert adapter.classify(_STREAM, coinbase_control(2)) == "control"
    assert adapter.classify(_STREAM, coinbase_frame(3, 0)) == "frame"


# --- the sequence counts every message --------------------------------------


def test_an_ack_advances_the_cursor_so_the_next_frame_is_not_a_gap() -> None:
    """The bug this exists to prevent: acks share the book's sequence, so a
    transform that ignored them would report a gap on the very next frame."""
    rows, transform = _run(_session())

    assert transform.gaps == 0
    assert transform.bootstraps == 1
    assert transform.live
    assert [row for row in rows if row["action"] == "gap"] == []


def test_a_healthy_book_still_advances_over_an_in_band_snapshot() -> None:
    """A snapshot at a live book is ignored for rows but not for sequencing —
    it holds a sequence number, and skipping it strands the next frame."""
    _, transform = _run(_session())

    # Six snapshots land in a 60-frame session, five of them at a healthy book.
    assert transform.bootstraps == 1
    assert transform.gaps == 0


def test_a_lost_ack_is_detected_like_any_other_lost_message() -> None:
    # Coinbase's guarantee is over the connection, not over book messages: a
    # missing ack means *something* was lost, and that something might have
    # been book state. The ack has to stand alone for this to be visible — one
    # next to a snapshot is undetectable, and rightly so, because the snapshot
    # re-establishes the cursor regardless.
    records = coinbase_capture(
        count=_FRAMES, snapshot_every=_SNAPSHOT_EVERY, ack_before=25
    )
    lone_ack = next(r for r in records if r["kind"] == "control" and r["seq"] > 25)
    without_it = [r for r in records if r["seq"] != lone_ack["seq"]]

    _, kept = _run(records)
    _, lost = _run(without_it)

    assert kept.gaps == 0
    assert lost.gaps == 1


# --- the fault battery, unchanged from Binance ------------------------------


def test_a_stale_snapshot_is_refused_rather_than_used() -> None:
    """The bug that produced crossed books: the transform keeps the last
    snapshot it saw, and a gap much later must not rebuild from it."""
    adapter = CoinbaseAdapter()
    update = adapter.parse_frame(
        coinbase_frame(sequence=500, index=0), receive_ts=0, monotonic_ts=0
    )
    snapshot = adapter.parse_snapshot(
        json.loads(json.dumps(coinbase_frame(sequence=10, index=0))),
        receive_ts=0,
        monotonic_ts=0,
    )

    result = adapter.bootstrap([update], snapshot)

    assert result.outcome is BootstrapOutcome.SNAPSHOT_TOO_OLD


def test_a_dropped_run_is_detected_and_the_book_reconverges() -> None:
    injector = FaultInjector(FaultConfig(seed=3, drop=0.02, drop_run=3))
    faulted = list(injector(_session()))

    rows, transform = _run(faulted)

    assert injector.injected, "the seed injected nothing; pick another"
    assert transform.gaps >= 1
    # Every gap closed, and the book is trusted at the end of the run.
    assert transform.bootstraps > transform.gaps
    assert transform.live
    assert transform.crossed == 0
    assert len([row for row in rows if row["action"] == "gap"]) == transform.gaps


def test_the_injector_needs_no_knowledge_of_the_venue() -> None:
    # It perturbs capture records and leaves every non-frame record alone, so
    # the in-band snapshot and the acks pass through untouched.
    records = _session()
    injector = FaultInjector(FaultConfig(seed=1, drop=1.0, drop_run=1))

    faulted = list(injector(records))

    survivors = [r for r in faulted if r["kind"] != "frame"]
    originals = [r for r in records if r["kind"] != "frame"]
    assert survivors == originals
