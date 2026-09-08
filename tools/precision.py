"""Find symbols a venue quotes with more precision than `SCALE` can hold.

    uv run python -m tools.precision kraken --depth 1000 --duration 120
    uv run python -m tools.precision binance --duration 120

Phase 2 committed one project-wide `SCALE = 8` and recorded it as "all three
venues measured at eight places". That was measured over a handful of symbols.
Across all 188 it does not hold: Kraken quotes `BONK/USD` at nine places
(`price=0.000138167`) in the deep levels of a depth-1000 book, and
`collector.model.scaled_int` refuses to truncate a price rather than silently
losing a digit — which is the right call and kills the run.

This is how the exclusion list in `collector/symbols.py` is re-derived, and it
has to be re-run whenever a venue lists something new, because a sub-cent asset
is exactly the kind of thing that gets listed.

**It reads the depth the collector will actually subscribe.** The extra digits
live at the bottom of the book, not at the touch: the same 188 symbols at
Kraken depth 10 produce nothing over eight places at all. A shallow probe would
report the set is clean and be wrong.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterator, Sequence

from websockets.sync.client import connect

from collector.adapters import build
from collector.adapters.base import Venue
from collector.capture import decode
from collector.model import SCALE
from collector.settings import CollectorSettings
from collector.symbols import OVERLAPPING_BASES, native

_RECV_SLICE_S = 2.0
_CLOSE_TIMEOUT_S = 2.0
_MAX_MESSAGE_BYTES = 64 * 1024 * 1024


def _places(token: object) -> int:
    """Decimal places in a venue's own token, without parsing it as a number."""
    text = str(token)
    return len(text.partition(".")[2]) if "." in text else 0


def _tokens(payload: dict[str, object]) -> Iterator[tuple[str, str, str]]:
    """`(symbol, field, token)` for every price and size in one message.

    Deliberately venue-shaped and deliberately *here*: it reads all three
    dialects' level shapes, which is exactly what an adapter exists to stop
    happening in `collector/`. A tool may, because its whole job is to look at
    a venue before the adapter's assumptions are trusted.
    """
    data = payload.get("data")
    if isinstance(data, list):  # kraken
        for entry in data:
            symbol = str(entry.get("symbol", ""))
            for side in ("bids", "asks"):
                for level in entry.get(side, []):
                    yield symbol, "price", str(level["price"])
                    yield symbol, "qty", str(level["qty"])
        return
    events = payload.get("events")
    if isinstance(events, list):  # coinbase
        for event in events:
            symbol = str(event.get("product_id", ""))
            for update in event.get("updates", []):
                yield symbol, "price", str(update["price_level"])
                yield symbol, "qty", str(update["new_quantity"])
        return
    symbol = str(payload.get("s", ""))  # binance
    for side in ("b", "a"):
        levels = payload.get(side)
        if not isinstance(levels, list):
            continue
        for price, size in levels:
            yield symbol, "price", str(price)
            yield symbol, "qty", str(size)


def probe(
    venue: Venue, symbols: Sequence[str], *, duration_s: float
) -> dict[str, tuple[int, str]]:
    """The worst precision seen per symbol, as `(places, token)`."""
    worst: dict[str, tuple[int, str]] = {}
    with connect(
        venue.ws_url(symbols),
        close_timeout=_CLOSE_TIMEOUT_S,
        max_size=_MAX_MESSAGE_BYTES,
    ) as ws:
        for frame in venue.subscribe_frames(symbols):
            ws.send(frame)
        deadline = time.monotonic() + duration_s
        while time.monotonic() < deadline:
            try:
                message = ws.recv(timeout=_RECV_SLICE_S)
            except TimeoutError:
                continue
            text = message if isinstance(message, str) else message.decode()
            payload = decode(text)
            if venue.stream_of(payload) is None:
                continue
            for symbol, field, token in _tokens(payload):
                places = _places(token)
                if places > worst.get(symbol, (-1, ""))[0]:
                    worst[symbol] = (places, f"{field}={token}")
    return worst


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.precision",
        description=f"Find symbols quoted past SCALE={SCALE}.",
    )
    parser.add_argument("venue", choices=("binance", "coinbase", "kraken"))
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--depth", type=int, default=1000)
    parser.add_argument("--per-connection", type=int, default=0)
    args = parser.parse_args(argv)

    settings = CollectorSettings(venue=args.venue, depth=args.depth)
    venue = build(settings)
    symbols = [native(base, args.venue) for base in OVERLAPPING_BASES]
    size = args.per_connection or venue.max_symbols_per_connection
    worst: dict[str, tuple[int, str]] = {}
    for start in range(0, len(symbols), size):
        worst |= probe(venue, symbols[start : start + size], duration_s=args.duration)

    over = {s: v for s, v in worst.items() if v[0] > SCALE}
    print(f"\n=== {args.venue}: {len(worst)} symbols seen, SCALE={SCALE} ===")
    if not over:
        print("none exceed it")
    for symbol, (places, token) in sorted(over.items(), key=lambda kv: -kv[1][0]):
        print(f"  {symbol:<16} {places} places   {token}")
    print(f"\n{len(over)} symbol(s) cannot be represented; exclude them")
    if over:
        print("  " + json.dumps(sorted(over)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
