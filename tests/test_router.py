"""Demux: many symbols on one connection, one book each.

The property that decides whether the router is correct is an equivalence.
Replaying a shard's capture must give each symbol **the same book** it would
have got from a connection carrying only that symbol. If that holds, sharding
is invisible to everything downstream; if it does not, no amount of supervisor
work matters, because the books are wrong.

It is worth stating why that is not circular. The single-symbol path is the one
Phases 0 to 2.5 built and tested against recorded venue sessions, so it is the
reference. This asks whether multiplexing changed any answer.

Coinbase carries the phase here. Its `sequence_num` counts the connection, so
its equivalence *cannot* be established by feeding a lone product's records to
a lone adapter — that stream has holes in it by construction. The comparison
below is against the capture the venue would have sent had only that product
been subscribed, which is what `coinbase_capture(products=...)` builds.
"""

from __future__ import annotations

from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters import build_router
from collector.adapters.base import VenueAdapter
from collector.adapters.coinbase import CoinbaseAdapter
from collector.book import Book
from collector.capture import CaptureRecord
from collector.settings import CollectorSettings
from collector.transform import BookTransform
from tests.conftest import coinbase_capture

_PRODUCTS = ("BTC-USD", "ETH-USD", "SOL-USD")


def _ctx() -> RunContext:
    # http is None: reaching for it would mean the replay path did I/O, which
    # is the property that makes a capture replayable cold.
    return RunContext.create(source_name="test", http=cast(HttpClient, None))


def _sides(book: Book) -> tuple[dict[int, int], dict[int, int]]:
    return dict(book.bids), dict(book.asks)


def _drive_router(records: list[CaptureRecord], products: tuple[str, ...]) -> object:
    settings = CollectorSettings(venue="coinbase", symbols=",".join(products))
    router, _ = build_router(settings, [products])
    for record in records:
        for _ in router.transform(record, _ctx()):
            pass
    return router


def test_a_shards_books_match_replaying_each_symbol_alone() -> None:
    """The equivalence, over all three products at once."""
    shared = coinbase_capture(count=40, snapshot_every=10, products=_PRODUCTS)
    router = _drive_router(shared, _PRODUCTS)

    for product in _PRODUCTS:
        alone = coinbase_capture(count=40, snapshot_every=10, products=(product,))
        solo = BookTransform(symbol=product, adapter=_lone_adapter())
        for record in alone:
            for _ in solo.transform(record, _ctx()):
                pass

        book = router.books[f"level2:{product}"]  # type: ignore[attr-defined]
        assert _sides(book._book) == _sides(solo._book), product
        assert book.live is solo.live, product
        assert book.gaps == solo.gaps, product
        assert book.bootstraps == solo.bootstraps, product


def test_every_book_on_the_shard_actually_converges() -> None:
    """The equivalence would also hold if all four books were equally broken.

    This is the assertion that says they are not, and it is the one that fails
    against the pre-Phase-3 `snapshot_stale`: measured against a live
    three-product capture, two books bootstrapped once then gapped forever and
    the third never bootstrapped at all.
    """
    records = coinbase_capture(count=40, snapshot_every=10, products=_PRODUCTS)
    router = _drive_router(records, _PRODUCTS)
    summary = router.summary()  # type: ignore[attr-defined]

    assert summary["symbols"] == len(_PRODUCTS)
    assert summary["live_at_exit"] == len(_PRODUCTS), "every book ends trusted"
    assert summary["gaps"] == 0, "nothing was actually lost in this capture"
    assert summary["crossed_books"] == 0
    assert summary["unrouted"] == 0
    assert summary["rows"] > 0


def test_control_traffic_advances_the_cursor_without_reaching_a_book() -> None:
    """An ack is numbered like a book message and belongs to no book.

    Routed to a symbol it would advance that book's cursor twice; dropped, the
    next frame on the connection looks like a gap. Neither happens.
    """
    records = coinbase_capture(
        count=30, snapshot_every=10, ack_before=12, products=_PRODUCTS
    )
    assert any(r["stream"].startswith("control:") for r in records), (
        "the ack must be tagged with the connection it arrived on"
    )

    router = _drive_router(records, _PRODUCTS)
    summary = router.summary()  # type: ignore[attr-defined]
    assert summary["gaps"] == 0
    assert summary["live_at_exit"] == len(_PRODUCTS)


def test_a_tag_nobody_subscribed_to_is_counted_not_swallowed() -> None:
    records = coinbase_capture(count=10, snapshot_every=5, products=("BTC-USD",))
    stray = dict(records[-1])
    stray["stream"] = "level2:NOPE-USD"
    router = _drive_router([*records, cast(CaptureRecord, stray)], ("BTC-USD",))

    assert router.unrouted == 1  # type: ignore[attr-defined]


def _lone_adapter() -> VenueAdapter:
    return CoinbaseAdapter()
