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

from collector.adapters.base import Venue
from collector.adapters.binance import BinanceAdapter
from collector.settings import CollectorSettings


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
            **_endpoints(settings),
        )
    raise ValueError(f"no adapter for venue {settings.venue!r}")


def _endpoints(settings: CollectorSettings) -> dict[str, str]:
    """Only the overrides that were actually set; the adapter owns the rest."""
    overrides = {"ws_url": settings.ws_url, "rest_url": settings.rest_url}
    return {name: value for name, value in overrides.items() if value}
