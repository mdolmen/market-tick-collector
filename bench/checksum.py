"""What a venue-native checksum costs per message, and why it is not free.

`NOTES.md` § *Book representation* settled on a `dict` and made that verdict
conditional on one thing: a rolling-checksum venue recomputes over the top N on
*every* update, which turns the ordered top-N read from an occasional oracle
cost into a per-frame one — the read a plain `dict` is worst at. Kraken is that
venue, so this is the measurement that condition asked for.

**It answers the question and moves it.** The checksum needs the top ten *as
the venue spelled them*, and `collector.book.Book` is keyed by integer ticks
and holds no strings, so it cannot serve the read at all. The pressure lands on
the adapter's own view instead, and `Book`'s representation is untouched by it.
That is the phase's finding, and this bench is the number behind it.

Three implementations of one function, over a real landed session:

    naive     sort the whole side by price, per message, per side
    heap      the same over integer keys, with each level's token cached
    cached    rebuild the window only when a write could have moved it

All three are checked against the venue's own checksum on every message, so a
faster one that is wrong reports as wrong rather than as fast.

    uv run python -m bench.checksum tests/fixtures/kraken_book_capture.jsonl
"""

from __future__ import annotations

import heapq
import sys
import time
import zlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from collector.adapters.kraken import KrakenAdapter, checksum_token
from collector.capture import decode

_TOP = 10
_SCALE = 8


def _book_messages(path: Path) -> list[dict[str, Any]]:
    """Every book message in a landed capture, decoded once, in arrival order."""
    messages: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        payload = decode(decode(line)["payload"])
        if payload.get("channel") == "book":
            messages.append(payload)
    return messages


def _ticks(value: str) -> int:
    whole, _, fraction = value.partition(".")
    return int(whole + fraction.ljust(_SCALE, "0"))


def _crc(parts: Sequence[str]) -> int:
    return zlib.crc32("".join(parts).encode()) & 0xFFFFFFFF


# --- the three implementations ----------------------------------------------


def _naive(messages: Sequence[dict[str, Any]]) -> tuple[float, int]:
    """Sort the whole side by price on every message. The obvious one."""
    bids: dict[str, str] = {}
    asks: dict[str, str] = {}
    elapsed = 0.0
    matched = 0
    for payload in messages:
        entry = payload["data"][0]
        if payload["type"] == "snapshot":
            bids.clear()
            asks.clear()
        started = time.perf_counter_ns()
        for book, levels in ((bids, entry["bids"]), (asks, entry["asks"])):
            for level in levels:
                if _ticks(level["qty"]) == 0:
                    book.pop(level["price"], None)
                else:
                    book[level["price"]] = level["qty"]
        parts = [
            checksum_token(p) + checksum_token(asks[p])
            for p in sorted(asks, key=float)[:_TOP]
        ] + [
            checksum_token(p) + checksum_token(bids[p])
            for p in sorted(bids, key=float, reverse=True)[:_TOP]
        ]
        computed = _crc(parts)
        elapsed += time.perf_counter_ns() - started
        matched += computed == int(entry["checksum"])
    return elapsed, matched


def _heap(messages: Sequence[dict[str, Any]]) -> tuple[float, int]:
    """Integer keys and a cached token per level, but no window reuse."""
    bids: dict[int, str] = {}
    asks: dict[int, str] = {}
    elapsed = 0.0
    matched = 0
    for payload in messages:
        entry = payload["data"][0]
        if payload["type"] == "snapshot":
            bids.clear()
            asks.clear()
        started = time.perf_counter_ns()
        for book, levels in ((bids, entry["bids"]), (asks, entry["asks"])):
            for level in levels:
                ticks = _ticks(level["price"])
                if _ticks(level["qty"]) == 0:
                    book.pop(ticks, None)
                else:
                    book[ticks] = checksum_token(level["price"]) + checksum_token(
                        level["qty"]
                    )
        parts = [asks[t] for t in heapq.nsmallest(_TOP, asks)]
        parts += [bids[t] for t in heapq.nlargest(_TOP, bids)]
        computed = _crc(parts)
        elapsed += time.perf_counter_ns() - started
        matched += computed == int(entry["checksum"])
    return elapsed, matched


def _cached(messages: Sequence[dict[str, Any]]) -> tuple[float, int]:
    """The shipped one: `KrakenAdapter.observe`, not a copy of it.

    The point of measuring the real object rather than a transcription is the
    same as `bench/cadence.py`'s: a benchmark of a copy stops describing the
    collector the first time the collector changes.
    """
    adapter = KrakenAdapter(depth=1000)
    elapsed = 0.0
    matched = 0
    for payload in messages:
        started = time.perf_counter_ns()
        consistent = adapter.observe(payload)
        elapsed += time.perf_counter_ns() - started
        matched += consistent
    return elapsed, matched


def _report(
    label: str,
    run: Callable[[Sequence[dict[str, Any]]], tuple[float, int]],
    messages: Sequence[dict[str, Any]],
) -> None:
    elapsed, matched = run(messages)
    verdict = "ok" if matched == len(messages) else f"WRONG ({matched})"
    print(
        f"{label:<38}{elapsed / len(messages) / 1000:>12.2f}"
        f"{len(messages):>10}{verdict:>12}"
    )


def main(argv: Sequence[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <landed-kraken-capture.jsonl>", file=sys.stderr)
        return 2
    messages = _book_messages(Path(argv[1]))
    if not messages:
        print("no book messages in the corpus", file=sys.stderr)
        return 1

    _cached(messages)  # warm the interpreter, discard

    header = f"{'':<38}{'µs/message':>12}{'messages':>10}{'checksum':>12}"
    print(header)
    print("-" * len(header))
    _report("naive: sort the side, per message", _naive, messages)
    _report("heap: int keys, token cached", _heap, messages)
    _report("cached: rebuilt only when it can move", _cached, messages)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
