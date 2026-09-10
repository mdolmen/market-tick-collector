"""Shared fixtures: the recorded Binance bootstrap, in both shapes.

The fixtures are a real session — twelve consecutive ``depthUpdate`` frames
with a live ``GET /api/v3/depth`` taken part-way through, exactly as the
collector does it. Everything downstream of the split needs it as *capture
records*, so building those is here rather than copied into three test modules.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from collector.adapters import binance
from collector.capture import CaptureRecord, Kind, control_stream

_FIXTURES = Path(__file__).parent / "fixtures"

STREAM = binance.stream_name("BTCUSDT", 100)

# One tick of the capture clock per record. Real captures carry real clocks;
# these only have to be monotonic and reproducible, and a fixed step makes an
# assertion about which record a fault landed on readable.
_STEP_NS = 100_000_000


@pytest.fixture(scope="session")
def frames() -> list[str]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    return [line for line in lines if line.strip()]


@pytest.fixture(scope="session")
def snapshot_payload() -> dict[str, Any]:
    text = (_FIXTURES / "binance_depth_snapshot.json").read_text()
    return cast(dict[str, Any], json.loads(text))


def capture(
    *, snapshots: dict[int, dict[str, Any]], frames: list[str]
) -> list[CaptureRecord]:
    """Build a capture: frames in order, with snapshots spliced in by position.

    ``snapshots`` maps "before frame index N" to the payload landed there, so a
    test says where a snapshot arrived rather than assembling the list by hand.
    """
    records: list[CaptureRecord] = []

    def append(kind: str, stream: str, payload: dict[str, Any] | str) -> None:
        seq = len(records) + 1
        records.append(
            CaptureRecord(
                stream=stream,
                kind=cast(Any, kind),
                seq=seq,
                receive_ts=1_700_000_000_000_000_000 + seq * _STEP_NS,
                monotonic_ts=seq * _STEP_NS,
                payload=payload if isinstance(payload, str) else json.dumps(payload),
            )
        )

    for index, text in enumerate(frames):
        if index in snapshots:
            append("snapshot", binance.REST_DEPTH_STREAM, snapshots[index])
        append("frame", STREAM, text)
    if len(frames) in snapshots:
        append("snapshot", binance.REST_DEPTH_STREAM, snapshots[len(frames)])
    return records


# --- synthetic sessions ----------------------------------------------------
#
# Fault behaviour is entirely about sequencing, and the recorded session is
# twelve frames long with the snapshot near the end — so almost every frame in
# it is discarded by the splice and a fault there tests nothing. These build a
# session of any length with snapshots wherever they are wanted. The recorded
# fixture stays for the splice itself, where real ranges are the point.

_BASE_ID = 1_000_000
_IDS_PER_FRAME = 10
_BID_TICKS = 50_000_00000000
_TICK = 1_00000000


def synthetic_frame(index: int) -> dict[str, Any]:
    """Frame ``index``: a contiguous id range and one level a side."""
    first = _BASE_ID + index * _IDS_PER_FRAME
    price = _BID_TICKS + (index % 5) * _TICK
    return {
        "e": "depthUpdate",
        "E": 1_700_000_000_000 + index,
        "s": "BTCUSDT",
        "U": first,
        "u": first + _IDS_PER_FRAME - 1,
        "b": [[f"{price / 10**8:.8f}", f"{1 + index % 3}.00000000"]],
        "a": [[f"{(price + _TICK * 10) / 10**8:.8f}", "2.00000000"]],
    }


def synthetic_snapshot(before_index: int) -> dict[str, Any]:
    """A snapshot current as of just before frame ``before_index``.

    ``lastUpdateId`` sits one below that frame's ``U``, so the frame straddles
    it and the splice starts exactly there.

    **The levels are the book those frames actually built.** They are replayed
    from ``synthetic_frame`` onto the opening state rather than invented, which
    keeps every snapshot after the first agreeing with the stream it is spliced
    into. A builder that emitted one fixed level a side produces a capture no
    live session could ever land, and `tests/test_oracle.py` — which exists to
    diff a snapshot against the reconstruction — would read the fixture's own
    disagreement as book drift.
    """
    bids = {f"{_BID_TICKS / 10**8:.8f}": "1.00000000"}
    asks = {f"{(_BID_TICKS + _TICK * 10) / 10**8:.8f}": "2.00000000"}
    for index in range(before_index):
        frame = synthetic_frame(index)
        for levels, key in ((bids, "b"), (asks, "a")):
            for price, size in frame[key]:
                levels[price] = size
    return {
        "lastUpdateId": _BASE_ID + before_index * _IDS_PER_FRAME - 1,
        "bids": [[price, size] for price, size in bids.items()],
        "asks": [[price, size] for price, size in asks.items()],
    }


def synthetic_capture(*, count: int, snapshot_every: int = 0) -> list[CaptureRecord]:
    """``count`` chained frames, with a snapshot at 0 and every ``n`` after."""
    positions = {0}
    if snapshot_every > 0:
        positions |= set(range(snapshot_every, count, snapshot_every))
    return capture(
        snapshots={index: synthetic_snapshot(index) for index in sorted(positions)},
        frames=[json.dumps(synthetic_frame(i)) for i in range(count)],
    )


# --- Coinbase --------------------------------------------------------------
#
# The other dialect, and the one the boundary is actually tested by. Two things
# the Binance builders above have no reason to model and this one must:
# ``sequence_num`` runs across *every* message on the connection, control
# records included, and the snapshot occupies a number of its own rather than
# arriving out of band. A builder that numbered only the frames would produce a
# capture no live session could ever emit, and the tests over it would prove
# nothing about the adapter.

_CB_SYMBOL = "BTC-USD"
_CB_STREAM = f"level2:{_CB_SYMBOL}"
_CB_BID_TICKS = 79_850_00000000
_CB_TICK = 1_00000000


def _cb_time(sequence: int) -> str:
    """A distinct RFC3339 stamp per message, with a nanosecond fraction."""
    minutes, seconds = divmod(sequence, 60)
    return f"2026-09-06T10:{minutes:02d}:{seconds:02d}.123456789Z"


def _cb_level(side: str, ticks: int, quantity: str, sequence: int) -> dict[str, Any]:
    return {
        "side": side,
        "event_time": _cb_time(sequence),
        "price_level": f"{ticks / 10**8:.8f}",
        "new_quantity": quantity,
    }


def coinbase_frame(sequence: int, index: int) -> dict[str, Any]:
    """One ``update`` message: one level a side, at its own sequence number."""
    price = _CB_BID_TICKS + (index % 5) * _CB_TICK
    return _cb_book(
        sequence,
        "update",
        [
            _cb_level("bid", price, f"{1 + index % 3}.00000000", sequence),
            _cb_level("offer", price + _CB_TICK * 10, "2.00000000", sequence),
        ],
    )


def coinbase_snapshot(sequence: int) -> dict[str, Any]:
    """A ``snapshot`` message. In band, so it consumes a sequence number."""
    return _cb_book(
        sequence,
        "snapshot",
        [
            _cb_level("bid", _CB_BID_TICKS, "1.00000000", sequence),
            _cb_level("offer", _CB_BID_TICKS + _CB_TICK * 10, "2.00000000", sequence),
        ],
    )


def coinbase_control(sequence: int) -> dict[str, Any]:
    """A subscriptions ack — no book state, and numbered like everything else."""
    return {
        "channel": "subscriptions",
        "timestamp": _cb_time(sequence),
        "sequence_num": sequence,
        "events": [{"subscriptions": {"level2": [_CB_SYMBOL]}}],
    }


def _cb_book(
    sequence: int, event_type: str, updates: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "channel": "l2_data",
        "timestamp": _cb_time(sequence),
        "sequence_num": sequence,
        "events": [{"type": event_type, "product_id": _CB_SYMBOL, "updates": updates}],
    }


def coinbase_capture(
    *,
    count: int,
    snapshot_every: int = 0,
    ack_before: int | None = None,
    products: Sequence[str] = (_CB_SYMBOL,),
) -> list[CaptureRecord]:
    """``count`` frames per product, with an ack-then-snapshot pair at 0 and ``n``.

    The ack before each snapshot is what a resubscribe actually looks like on
    the wire, and it is there so the tests exercise a control record sitting
    inside the sequence rather than a tidier stream than the venue sends.

    ``ack_before`` places one *standalone* ack in front of that frame, away
    from any snapshot. Losing an ack next to a snapshot is undetectable and
    should be — the snapshot re-establishes the cursor either way — so it takes
    a lone one to show that the sequence covers control traffic at all.

    ``products`` round-robins several products over **one shared
    `sequence_num`**, which is what the venue actually sends: measured
    2026-09-08 over a 120s three-product subscribe, 5802 messages numbered
    0 → 5801 with no break and one `events` entry each. Merging two separately
    built single-product captures would instead produce two interleaved
    sequences, which is a stream no live connection can emit — and it is
    precisely the shape that hides the bug this builder exists to expose.
    """
    positions = {0}
    if snapshot_every > 0:
        positions |= set(range(snapshot_every, count, snapshot_every))

    # Control traffic names the connection it arrived on, exactly as
    # `FrameSource` tags it — the shard is the first symbol on it.
    control = control_stream(products[0].upper())
    records: list[CaptureRecord] = []
    sequence = 0

    def append(kind: Kind, payload: dict[str, Any], stream: str) -> None:
        nonlocal sequence
        seq = len(records) + 1
        records.append(
            CaptureRecord(
                stream=stream,
                kind=kind,
                seq=seq,
                receive_ts=1_700_000_000_000_000_000 + seq * _STEP_NS,
                monotonic_ts=seq * _STEP_NS,
                payload=json.dumps(payload),
            )
        )
        sequence += 1

    for index in range(count):
        for product in products:
            stream = f"level2:{product}"
            if index in positions:
                append("control", coinbase_control(sequence), control)
                append("snapshot", _cb_at(coinbase_snapshot(sequence), product), stream)
            if index == ack_before:
                append("control", coinbase_control(sequence), control)
            append("frame", _cb_at(coinbase_frame(sequence, index), product), stream)
    return records


def _cb_at(payload: dict[str, Any], product: str) -> dict[str, Any]:
    """The same message, relabelled for one product."""
    events = [{**event, "product_id": product} for event in payload["events"]]
    return {**payload, "events": events}


# --- Kraken ----------------------------------------------------------------
#
# The third dialect, and the builders have to work harder than either of the
# other two for one reason: **there is no sequence to fake**. A Binance or
# Coinbase session is made consistent by numbering it correctly; a Kraken
# session is made consistent by *computing the venue's checksum*, because that
# is the only thing an adapter here can check. A builder that wrote an
# arbitrary `checksum` would produce a session the adapter rejects on message
# one, and the fault battery over it would prove nothing.
#
# So these keep a small book of their own and emit the CRC32 the venue would
# have sent. That makes them the one fixture builder in this file that
# duplicates logic under test — deliberately, and by a different route: the
# adapter maintains an incremental top-10 window, this recomputes from scratch.
# `test_kraken.py` also checks both against a landed live session, which is the
# only thing that proves either of them right.

_KR_SYMBOL = "BTC/USD"
_KR_STREAM = f"book:{_KR_SYMBOL}"
_KR_BID = 79_450_00000000
_KR_TICK = 10000000  # 0.1, the venue's price precision for this pair
_KR_LEVELS = 12  # more than the checksum's ten, so the window can move


def _kr_price(ticks: int) -> str:
    """Ticks to the venue's own spelling: one decimal place for this pair."""
    return f"{ticks / 10**8:.1f}"


def _kr_qty(units: int) -> str:
    return f"{units / 10**8:.8f}"


def _kr_time(index: int) -> str:
    minutes, seconds = divmod(index, 60)
    return f"2026-09-07T08:{minutes:02d}:{seconds:02d}.123456Z"


class KrakenSession:
    """A consistent Kraken session: a book, and the checksums it implies.

    ``symbol`` because a shard puts several on one connection, and each keeps
    its own book and its own checksum — the venue sends one `data[]` entry per
    message whatever is subscribed (measured 2026-09-08, 25635 of 25635).
    """

    def __init__(self, symbol: str = _KR_SYMBOL) -> None:
        self.symbol = symbol
        self.bids: dict[int, str] = {}
        self.asks: dict[int, str] = {}
        for depth in range(_KR_LEVELS):
            self.bids[_KR_BID - depth * _KR_TICK] = _kr_qty(100000 + depth)
            self.asks[_KR_BID + (depth + 1) * _KR_TICK] = _kr_qty(200000 + depth)

    def checksum(self) -> int:
        """Asks best-first then bids best-first, ten a side — recomputed whole."""
        parts: list[str] = []
        for ticks in sorted(self.asks)[:10]:
            parts.append(_kr_token(_kr_price(ticks)) + _kr_token(self.asks[ticks]))
        for ticks in sorted(self.bids, reverse=True)[:10]:
            parts.append(_kr_token(_kr_price(ticks)) + _kr_token(self.bids[ticks]))
        return zlib.crc32("".join(parts).encode()) & 0xFFFFFFFF

    def snapshot(self, index: int) -> dict[str, Any]:
        return self._message(
            "snapshot",
            index,
            [
                {"price": _kr_price(t), "qty": q}
                for t, q in sorted(self.bids.items(), reverse=True)
            ],
            [{"price": _kr_price(t), "qty": q} for t, q in sorted(self.asks.items())],
        )

    def update(self, index: int) -> dict[str, Any]:
        """Move one bid level, the way a real diff message does."""
        ticks = _KR_BID - (index % _KR_LEVELS) * _KR_TICK
        qty = _kr_qty(100000 + index % 7)
        self.bids[ticks] = qty
        return self._message(
            "update", index, [{"price": _kr_price(ticks), "qty": qty}], []
        )

    def _message(
        self,
        kind: str,
        index: int,
        bids: list[dict[str, str]],
        asks: list[dict[str, str]],
    ) -> dict[str, Any]:
        return {
            "channel": "book",
            "type": kind,
            "data": [
                {
                    "symbol": self.symbol,
                    "bids": bids,
                    "asks": asks,
                    "checksum": self.checksum(),
                    "timestamp": _kr_time(index),
                }
            ],
        }


def _kr_token(value: str) -> str:
    return value.replace(".", "").lstrip("0") or "0"


def kraken_control(index: int) -> dict[str, Any]:
    """A heartbeat — outside any sequence, because there is no sequence."""
    return {"channel": "heartbeat"}


def kraken_capture(
    *, count: int, snapshot_every: int = 0, symbol: str = _KR_SYMBOL
) -> list[CaptureRecord]:
    """`count` consistent updates, with a snapshot at 0 and every `n` after.

    Unlike the other two builders there is nothing to keep chained here. What
    is kept correct is the checksum on every message, which is the only claim
    a Kraken capture makes about itself.
    """
    positions = {0}
    if snapshot_every > 0:
        positions |= set(range(snapshot_every, count, snapshot_every))

    session = KrakenSession(symbol)
    control = control_stream(symbol.upper())
    records: list[CaptureRecord] = []

    def append(kind: Kind, payload: dict[str, Any], stream: str = "") -> None:
        seq = len(records) + 1
        records.append(
            CaptureRecord(
                stream=stream or f"book:{symbol}",
                kind=kind,
                seq=seq,
                receive_ts=1_700_000_000_000_000_000 + seq * _STEP_NS,
                monotonic_ts=seq * _STEP_NS,
                payload=json.dumps(payload),
            )
        )

    for index in range(count):
        if index in positions:
            append("control", kraken_control(index), control)
            append("snapshot", session.snapshot(index))
        append("frame", session.update(index))
    return records
