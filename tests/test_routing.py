"""`stream_of`: which of a socket's many symbols a message is about.

One socket per symbol needs no demultiplexer, which is why this member did not
exist until Phase 3. One socket per *shard* does, and `stream` was reserved for
exactly this in Phase 1 (`collector/capture.py`), so the routing question is
answerable without a format change — but only if every adapter agrees on what
the tag is and on when there isn't one.

Two properties, over all three dialects:

- a book message routes to `stream_tag(symbol)`, the same tag the source stamps
- connection-wide traffic routes to `None`, because it belongs to no book

The second is the one worth asserting. A subscribe ack carries no symbol and
must not be attributed to whichever book happened to be first on the socket; on
Coinbase it is *numbered* in the same sequence as book messages, so routing it
to a symbol would move that symbol's cursor twice and gap the next frame.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from collector.adapters.binance import BinanceAdapter
from collector.adapters.coinbase import CoinbaseAdapter
from collector.adapters.kraken import KrakenAdapter
from tests.conftest import (
    coinbase_control,
    coinbase_frame,
    coinbase_snapshot,
    kraken_control,
    synthetic_frame,
)

_FIXTURES = Path(__file__).parent / "fixtures"


def test_binance_routes_a_frame_by_its_symbol() -> None:
    adapter = BinanceAdapter(depth_interval_ms=100)
    assert adapter.stream_of(synthetic_frame(0)) == "btcusdt@depth@100ms"


def test_binance_routes_a_subscribe_ack_nowhere() -> None:
    # The shape a `SUBSCRIBE` frame gets back, which the URL-path subscription
    # never produced and Phase 3's batched subscribe does.
    adapter = BinanceAdapter()
    assert adapter.stream_of({"result": None, "id": 1}) is None


def test_coinbase_routes_book_messages_by_product() -> None:
    adapter = CoinbaseAdapter()
    assert adapter.stream_of(coinbase_frame(2, 0)) == "level2:BTC-USD"
    assert adapter.stream_of(coinbase_snapshot(1)) == "level2:BTC-USD"


def test_coinbase_routes_a_numbered_ack_nowhere() -> None:
    """The ack sits *inside* the sequence, and still belongs to no book."""
    adapter = CoinbaseAdapter()
    assert adapter.stream_of(coinbase_control(2)) is None


def test_kraken_routes_book_messages_by_symbol() -> None:
    adapter = KrakenAdapter(depth=10)
    session = _kraken_live_messages()
    book = [m for m in session if m.get("channel") == "book"]
    assert book, "the landed session carried no book message"
    assert {adapter.stream_of(m) for m in book} == {"book:BTC/USD"}


def test_kraken_routes_heartbeats_and_status_nowhere() -> None:
    adapter = KrakenAdapter(depth=10)
    assert adapter.stream_of(kraken_control(0)) is None
    off_channel = [
        m for m in _kraken_live_messages() if m.get("channel") not in (None, "book")
    ]
    assert off_channel, "the landed session carried no control traffic"
    assert all(adapter.stream_of(m) is None for m in off_channel)


def _kraken_live_messages() -> list[dict[str, Any]]:
    """The Phase 2.5 landed session — a real socket, not a builder.

    `stream_of` reads a field the fixture builders also write, so a synthetic
    payload would only prove they agree with each other.
    """
    lines = (_FIXTURES / "kraken_book_capture.jsonl").read_text().splitlines()
    return [json.loads(json.loads(line)["payload"]) for line in lines if line.strip()]
