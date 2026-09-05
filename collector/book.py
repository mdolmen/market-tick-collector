"""The reconstructed L2 book: one ``dict`` per side, keyed by integer ticks.

``dict`` is the decision recorded in ``NOTES.md`` § *Book representation*, taken
against a sorted array and a fixed-width ticks-from-mid array — and it is a
decision Phase 6 benchmarks rather than assumes. What that benchmark has to
measure is the *read*: ``best_bid_ask`` below is an O(n) scan of every level,
while applying a diff is a point mutation and is cheap under all three
candidates. Benchmarking the write picks the wrong structure.
"""

from __future__ import annotations

from collector.model import Side


class Book:
    """One venue-symbol's price levels. Sizes are absolute, zero deletes."""

    __slots__ = ("asks", "bids")

    def __init__(self) -> None:
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}

    def apply(self, side: Side, price_ticks: int, size_lots: int) -> None:
        """Set a level to an absolute size; zero is the only deletion signal.

        Absolute set-to-value, *never* an increment. Applied as increments the
        book drifts slowly and stays plausible, which is the worst failure mode
        available here — and the one nothing but the top-N oracle would catch.
        """
        if size_lots < 0:
            raise ValueError(f"negative size {size_lots} at {price_ticks} ({side})")
        levels = self.bids if side == "bid" else self.asks
        if size_lots == 0:
            levels.pop(price_ticks, None)
        else:
            levels[price_ticks] = size_lots

    def best_bid_ask(self) -> tuple[int | None, int | None]:
        """Top of book, in ticks. O(n) over a plain dict — see the module note."""
        return (
            max(self.bids) if self.bids else None,
            min(self.asks) if self.asks else None,
        )

    def clear(self) -> None:
        """Drop all state. A re-bootstrap rebuilds cold from a fresh snapshot."""
        self.bids.clear()
        self.asks.clear()
