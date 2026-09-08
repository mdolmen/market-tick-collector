"""The Coinbase dialect, and the two bugs its differences from Binance caused.

`test_faults.py` proves the fault machinery over Binance; this proves the same
machinery over a venue whose snapshot arrives in band and whose sequence counts
every message on the connection. That the `FaultInjector` needs no change to
work here is the actual evidence the normalization boundary landed — it
perturbs `CaptureRecord`s and has never heard of either venue.
"""

from __future__ import annotations

from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters import build_router
from collector.adapters.base import BootstrapOutcome
from collector.adapters.coinbase import CoinbaseAdapter, _ns
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.replay import FaultConfig, FaultInjector
from collector.settings import CollectorSettings
from collector.transform import BookTransform
from tests.conftest import (
    coinbase_capture,
    coinbase_control,
    coinbase_frame,
    coinbase_snapshot,
)

_FRAMES = 60
_SNAPSHOT_EVERY = 10
_STREAM = "level2:BTC-USD"


def _run(
    records: list[CaptureRecord], products: tuple[str, ...] = ("BTC-USD",)
) -> tuple[list[LevelRow], BookTransform]:
    """Through a `BookRouter`, because continuity here is the connection's.

    `sequence_num` counts every message on the socket, so no single book can
    tell a lost message from a neighbour having spoken — and a book that is
    buffering never advances the cursor at all. The router watches every record
    and is the only thing that can judge it, which is why even a one-product
    Coinbase connection needs one. Returns the first product's book, which is
    what every assertion below is about.
    """
    settings = CollectorSettings(venue="coinbase", symbols=",".join(products))
    router, _ = build_router(settings, [products])
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    rows = [row for record in records for row in router.transform(record, ctx)]
    return rows, router.books[f"level2:{products[0]}"]


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
    assert transform.live
    assert [row for row in rows if row["action"] == "gap"] == []


def test_a_healthy_book_rebuilds_from_an_in_band_snapshot() -> None:
    """Not ignored — and this assertion is the other way round than it was.

    It read `bootstraps == 1`: a snapshot at a live book was taken to be
    redundant, worth advancing the cursor over and nothing more. That is true
    of a snapshot read *alongside* an uninterrupted stream, which is Binance's
    REST call and is not this. Here the only way to obtain one is
    `resubscribe_frames`, and the unsubscribe/subscribe pair stops the diffs:
    any level deleted in that gap is missing from the new snapshot and never
    arrives as a delete. The sequence stays unbroken across the pair — measured
    in Phase 2 — so nothing marks the loss and the book stays plausible.

    Phase 2.5 found it on Kraken as 43 crossed books in a replay whose every
    checksum passed. It was here the whole time and no run was long enough to
    show it: the snapshot interval defaults to 300s.
    """
    _, transform = _run(_session())

    # Six snapshots land in a 60-frame session, and every one of them rebuilds.
    assert transform.bootstraps == 6
    assert transform.gaps == 0
    assert transform.live
    assert transform.crossed == 0


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


def test_a_snapshot_from_before_a_gap_is_refused_rather_than_used() -> None:
    """The bug that produced crossed books: the transform keeps the last
    snapshot it saw, and a gap much later must not rebuild from it.

    Driven through `sequence_broke` rather than by handing `bootstrap` a
    distant pair directly. Until Phase 3 the rule was sequence adjacency, so
    any gap in the numbers looked stale and constructing one by hand was
    enough. It is now "did the connection break after this snapshot", which is
    what adjacency was standing in for and is the only version that survives
    several products on one socket — so the test has to actually break the
    connection, and only something watching the whole connection can.
    """
    adapter = CoinbaseAdapter()
    snapshot = adapter.parse_snapshot(
        coinbase_snapshot(sequence=10), receive_ts=0, monotonic_ts=0
    )
    adapter.bootstrapped(snapshot)
    update = adapter.parse_frame(
        coinbase_frame(sequence=500, index=0), receive_ts=0, monotonic_ts=0
    )

    adapter.sequence_broke(update.first_seq)  # 490 missing messages

    assert adapter.snapshot_required()
    assert adapter.bootstrap([update], snapshot).outcome is (
        BootstrapOutcome.SNAPSHOT_TOO_OLD
    )


def test_a_snapshot_is_not_stale_merely_because_others_share_the_socket() -> None:
    """The Phase 3 finding, as the complement of the test above.

    With several products on one connection a product's own next frame is
    several `sequence_num` past its snapshot with nothing missing at all —
    measured 2026-09-08, three products, 5802 messages, no break. Under the old
    adjacency rule that read as stale, and every book on a multiplexed
    connection was permanently `SNAPSHOT_TOO_OLD`.
    """
    adapter = CoinbaseAdapter()
    snapshot = adapter.parse_snapshot(
        coinbase_snapshot(sequence=10), receive_ts=0, monotonic_ts=0
    )
    adapter.bootstrapped(snapshot)
    # Two other products spoke in between, so this product's next frame is 13.
    for sequence in (11, 12, 13):
        update = adapter.parse_frame(
            coinbase_frame(sequence=sequence, index=0), receive_ts=0, monotonic_ts=0
        )
        assert not adapter.gap_detected(update)
        adapter.accept(update)

    assert adapter.bootstrap([update], snapshot).outcome is BootstrapOutcome.READY


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
