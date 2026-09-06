"""Rebuild a book from landed rows and print it.

    uv run python -m tools.book                      # top 10 of the default dataset
    uv run python -m tools.book --depth 25
    uv run python -m tools.book path/to/levels --depth 5

The collector's book is in-memory and dies with the run, so this reads it back
out of what landed. That it works at all is the point: `NOTES.md` claims the
normalized rows are sufficient to reproduce book state, and replaying them into
the same `Book` the collector used either reproduces the level counts the run
reported or the claim is wrong. It is the cheapest standing check on that, and
Phase 1's replay harness is the same idea with faults injected.

Rows are ordered by `seq` — the venue's own update id, not our clock. Two
sockets or two files mean two delivery paths, and ordering by arrival time
would reproduce that skew rather than the book.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import cast

import pyarrow.dataset as ds

from collector.book import Book
from collector.model import SCALE, Side

DEFAULT_DATASET = "data/l2/l2/levels"


def _load(path: str) -> tuple[Book, dict[int, tuple[str, str]], int, int]:
    table = ds.dataset(path, format="parquet").to_table()
    table = table.sort_by([("seq", "ascending")])
    columns = [
        table[name].to_pylist()
        for name in (
            "action",
            "side",
            "price_ticks",
            "size_lots",
            "price_str",
            "size_str",
        )
    ]

    book = Book()
    # Ticks are the book key; the venue's own strings are what a human reads.
    labels: dict[int, tuple[str, str]] = {}
    gaps = 0
    for action, side, ticks, lots, price_str, size_str in zip(*columns, strict=True):
        if action == "gap":
            gaps += 1
            continue
        book.apply(cast(Side, side), ticks, lots)
        labels[ticks] = (price_str, size_str)
    return book, labels, gaps, table.num_rows


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.book",
        description="Replay landed level rows into a book and print the top of it.",
    )
    parser.add_argument("dataset", nargs="?", default=DEFAULT_DATASET)
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args(argv)

    book, labels, gaps, rows = _load(args.dataset)
    bid, ask = book.best_bid_ask()
    print(
        f"{rows} rows, {gaps} gap(s) -> "
        f"{len(book.bids)} bid / {len(book.asks)} ask levels"
    )
    if bid is None or ask is None:
        print("one side is empty; nothing to show")
        return 1
    scale = 10**SCALE
    print(f"spread {(ask - bid) / scale:g}, mid {(ask + bid) / 2 / scale:g}\n")

    bids = sorted(book.bids, reverse=True)[: args.depth]
    asks = sorted(book.asks)[: args.depth]
    print(f"{'bid size':>16} {'bid':>16}  |  {'ask':<16} {'ask size':<16}")
    print("-" * 72)
    for bid_ticks, ask_ticks in zip(bids, asks, strict=False):
        bid_price, bid_size = labels[bid_ticks]
        ask_price, ask_size = labels[ask_ticks]
        print(f"{bid_size:>16} {bid_price:>16}  |  {ask_price:<16} {ask_size:<16}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
