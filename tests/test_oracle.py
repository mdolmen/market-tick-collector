"""The oracle: the reconstructed book against the venue's own snapshot.

Everything else in this suite asks whether the collector noticed something go
wrong. This asks the question none of those can — whether a book that noticed
nothing is *right*. The failure mode it exists for is `NOTES.md` § *The diff
stream*: absolute sizes applied as increments drift slowly and stay plausible,
with no gap, no sequence break and no failing checksum to find them by.

So `test_drift_with_no_gap_is_a_break` is the test this module is for. Every
other one is about the opposite risk, which is the larger one in practice: a
comparison taken carelessly manufactures breaks the collector never committed,
and a divergence rate made of artifacts is worse than no divergence rate.
"""

from __future__ import annotations

import json
from typing import Any, cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.binance import BinanceAdapter
from collector.adapters.coinbase import CoinbaseAdapter
from collector.capture import CaptureRecord
from collector.transform import BookTransform
from tests.conftest import (
    capture,
    coinbase_capture,
    synthetic_capture,
    synthetic_frame,
    synthetic_snapshot,
)


def _ctx() -> RunContext:
    return RunContext.create(source_name="test", http=cast(HttpClient, None))


def run(
    records: list[CaptureRecord], transform: BookTransform | None = None
) -> BookTransform:
    """Drive a capture through a book, discarding the rows.

    The oracle lands nothing, so what it did is in the counters rather than in
    the output — which is the property `BookTransform._compare` is claiming.
    """
    book = transform or BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = _ctx()
    for record in records:
        list(book.transform(record, ctx))
    return book


def _before_the_second_snapshot(records: list[CaptureRecord]) -> int:
    """The record after which the book is live and a comparison is imminent.

    By position rather than by a hardcoded index, so the fixture can grow a
    record without silently moving what these tests mutate.
    """
    snapshots = [i for i, record in enumerate(records) if record["kind"] == "snapshot"]
    return snapshots[1] - 1


# --- the comparison itself --------------------------------------------------


def test_a_clean_reconstruction_matches_the_venues_snapshot() -> None:
    """The baseline, and the one that makes every other number here readable.

    A snapshot describes the book at *its* position while the transform holds
    the book at a later one, so this passes only because the comparison rolls
    the snapshot forward first. Without that it would report the frames that
    arrived during the fetch as breaks, on every venue, forever.
    """
    transform = run(synthetic_capture(count=60, snapshot_every=20))
    summary = transform.summary()

    assert summary["oracle_comparisons"] == 2
    assert summary["oracle_clean"] == 2
    assert summary["oracle_levels_broken"] == 0
    assert summary["oracle_unaligned"] == 0
    # A comparison over nothing would satisfy every assertion above.
    assert cast(int, summary["oracle_levels_compared"]) > 0


def test_drift_with_no_gap_is_a_break() -> None:
    """A level nothing on the stream touched, and only the oracle can see it.

    The mutation stands in for the increment bug: it moves one size without
    breaking the chain, so the sequence stays healthy, the book stays live and
    every other counter in the run reads clean. It goes in immediately before
    the snapshot because this synthetic stream rewrites the same five prices
    round-robin — drift planted earlier is overwritten by the venue's own next
    update, which is a real property of a busy book and not a fixture quirk.
    """
    records = synthetic_capture(count=60, snapshot_every=20)
    transform = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = _ctx()

    for index, record in enumerate(records):
        list(transform.transform(record, ctx))
        if index == _before_the_second_snapshot(records):
            price, size = next(iter(transform.book.bids.items()))
            transform.book.apply("bid", price, size + 1)

    summary = transform.summary()
    assert summary["gaps"] == 0
    assert summary["crossed_books"] == 0
    assert summary["oracle_comparisons"] == 2
    assert summary["oracle_levels_broken"] == 1
    assert summary["oracle_clean"] == 1


def test_a_level_outside_the_snapshots_depth_is_not_a_break() -> None:
    """A REST depth response is the top N, and asserts nothing below that.

    The book is full-depth and legitimately holds levels the snapshot never
    described. Counting those as breaks would report a divergence rate that is
    really a depth mismatch, and it would grow with the length of the run.

    Paired deliberately: the same planted level *inside* the band is a break,
    so this cannot pass by comparing nothing at all. It is found by *both*
    comparisons, because this price is not one the round-robin stream ever
    rewrites — drift nothing heals is reported every time it is looked for,
    which is what makes the count a rate rather than an event.
    """
    records = synthetic_capture(count=60, snapshot_every=20)
    drift_at = _before_the_second_snapshot(records)

    def planted(below: bool) -> dict[str, Any]:
        transform = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
        ctx = _ctx()
        for index, record in enumerate(records):
            list(transform.transform(record, ctx))
            if index == drift_at:
                floor = min(transform.book.bids)
                transform.book.apply("bid", floor - 1 if below else floor + 1, 7)
        return transform.summary()

    assert planted(below=True)["oracle_levels_broken"] == 0
    assert planted(below=True)["oracle_clean"] == 2
    assert planted(below=False)["oracle_levels_broken"] == 2


# --- refusing to compare ----------------------------------------------------


def test_a_snapshot_ahead_of_the_book_is_unaligned_not_broken() -> None:
    """Rolling forward only rolls one way, and the other way is not guessed.

    A snapshot the book has not reached yet describes a future, and every level
    the stream is about to change reads as a break. That is the artifact
    `NOTES.md` § *Validating against the venue's own top-N* warns about, and
    the honest answer is to lose the sample rather than to invent the number.
    """
    frames = [json.dumps(synthetic_frame(index)) for index in range(20)]
    records = capture(
        snapshots={0: synthetic_snapshot(0), 20: synthetic_snapshot(60)},
        frames=frames,
    )

    summary = run(records).summary()

    assert summary["oracle_unaligned"] == 1
    assert summary["oracle_comparisons"] == 0
    assert summary["oracle_levels_broken"] == 0


def test_an_untrusted_book_is_not_compared() -> None:
    """`TODO.md` § *Phase 6*: an untrusted book measures where it stopped.

    Between the gap and the repair the book is deliberately frozen, so a
    snapshot diffed against it reports every update lost in the interval — an
    interval the run already knows about and already reports, as a gap and as
    an untrusted-frame count. Counting it twice, the second time as book
    error, is double-counting the one failure the collector handled correctly.
    """
    records = synthetic_capture(count=40, snapshot_every=20)
    frames = [record for record in records if record["kind"] == "frame"]
    dropped = frames[10]
    gapped = [record for record in records if record["seq"] != dropped["seq"]]

    intact = run(records).summary()
    broken = run(gapped).summary()

    # The snapshot the intact run compares against is the one the gapped run
    # spends on the repair, so this pair is the whole assertion.
    assert intact["gaps"] == 0
    assert intact["oracle_comparisons"] == 1
    assert broken["gaps"] == 1
    assert broken["bootstraps"] == 2
    assert broken["oracle_comparisons"] == 0
    assert broken["oracle_unaligned"] == 0


def test_a_superseding_snapshot_is_never_compared() -> None:
    """The venue's answer, not ours — and on Coinbase the answer is no.

    A snapshot that exists only because `resubscribe_frames` asked for it
    arrived through an interrupted stream, so every level deleted while
    unsubscribed is absent from it without ever arriving as a delete. Diffing
    against that measures the interruption the measurement itself caused. The
    gate is `snapshot_supersedes`, which the transform already consults, so no
    venue is named downstream of the boundary to enforce it.
    """
    transform = run(
        coinbase_capture(count=40, snapshot_every=20),
        BookTransform(symbol="BTC-USD", adapter=CoinbaseAdapter()),
    )
    summary = transform.summary()

    assert summary["bootstraps"] == 2, "the snapshots did arrive and did supersede"
    assert summary["oracle_comparisons"] == 0
    assert summary["oracle_unaligned"] == 0
