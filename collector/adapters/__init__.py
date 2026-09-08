"""Venue adapters. An adapter's only job is one venue's frames → the model.

Everything venue-shaped lives behind this boundary: the frame dialect, the
bootstrap, and the sequencing rule. Nothing downstream — book, sink, metrics —
may learn which venue a record came from except as a label. If it needs to, the
boundary is in the wrong place.

``build`` is the one place a venue name becomes a venue, and it is the only
place in the project that names them all. Everything else takes a
``VenueAdapter`` or a ``Venue`` and cannot tell which it was handed.
"""

from __future__ import annotations

from collections.abc import Sequence

from collector.adapters.base import Venue
from collector.adapters.binance import BinanceAdapter
from collector.adapters.coinbase import CoinbaseAdapter
from collector.adapters.kraken import KrakenAdapter
from collector.router import BookRouter
from collector.settings import CollectorSettings
from collector.transform import BookTransform


def build(settings: CollectorSettings) -> Venue:
    """The adapter for the configured venue, wired from settings.

    Two instances of this normally exist per run — the capture source holds
    one and the book transform another — because each keeps its own sequence
    cursor for its own decision. See ``collector/source.py``.
    """
    if settings.venue == "binance":
        return BinanceAdapter(
            depth_interval_ms=settings.depth_interval_ms,
            snapshot_limit=settings.snapshot_limit,
            **_endpoints(settings, "ws_url", "rest_url"),
        )
    if settings.venue == "coinbase":
        return CoinbaseAdapter(**_endpoints(settings, "ws_url"))
    if settings.venue == "kraken":
        return KrakenAdapter(depth=settings.depth, **_endpoints(settings, "ws_url"))
    raise ValueError(f"no adapter for venue {settings.venue!r}")


def build_router(
    settings: CollectorSettings, symbols: Sequence[str]
) -> tuple[BookRouter, Venue]:
    """A router for one shard, and the transport that feeds it.

    The adapters are keyed by `sequence_key`, which is what makes a
    connection-scoped venue share a single one across every book on the shard
    and a symbol-scoped venue give each book its own. No caller has to know
    which kind it got — that is the whole point of the key — but this function
    does, because it is the one place a venue is named at all.

    The transport is a *separate* instance from any of them, keeping the two
    Phase 2 rules intact: the source's cursor and the transform's cursor are
    different decisions over one venue rule and must not share state.
    """
    template = build(settings)
    sequencers: dict[str, Venue] = {}
    books: dict[str, BookTransform] = {}
    for symbol in symbols:
        key = template.sequence_key(symbol)
        if key not in sequencers:
            sequencers[key] = build(settings)
        books[template.stream_tag(symbol)] = BookTransform(
            symbol=symbol, adapter=sequencers[key]
        )
    router = BookRouter(books, tuple(sequencers.values()))
    return router, template


def _endpoints(settings: CollectorSettings, *names: str) -> dict[str, str]:
    """Only the overrides that were actually set; the adapter owns the rest."""
    values = {name: getattr(settings, name) for name in names}
    return {name: value for name, value in values.items() if value}
