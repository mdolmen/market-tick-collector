"""Coinbase Advanced Trade: in-band snapshot, one sequence per message.

The second row of `NOTES.md` § *Sequencing dialects*: the venue sends the
snapshot on the socket rather than over REST, and numbers messages one at a
time instead of covering a range. Both differences are confined here, and the
`in_sequence` / `gap_detected` / `snapshot_required` contract reads the same
upward as Binance's.

**The feed matters more than the venue does.** The obvious public endpoint,
`ws-feed.exchange.coinbase.com` channel `level2_batch`, sends `l2update`
messages carrying `changes`, `product_id`, `time` and `type` — and no sequence
number of any kind. An adapter on it cannot detect a lost message at all, so
the book would drift silently while staying plausible. Advanced Trade's
`level2` carries `sequence_num` and is what this module speaks. The two are
otherwise similar enough to pick the wrong one by accident, which is why this
paragraph is here.

Three things the Phase 2 probe established that are not guessable:

- **`sequence_num` counts every message on the connection**, subscription acks
  included, so control traffic is landed and advances the cursor.
- **Sides are `bid` and `offer`**, not bid and ask.
- **The snapshot is ~4.9 MB for BTC-USD**, well past the `websockets` 1 MiB
  default frame cap.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from collector.adapters.base import (
    BootstrapResult,
    Level,
    Snapshot,
    SnapshotRequest,
    Update,
    bootstrap_by_sequence,
)
from collector.capture import Kind
from collector.model import SCALE, scaled_int

VENUE = "coinbase"

_WS_URL = "wss://advanced-trade-ws.coinbase.com"
_CHANNEL = "level2"

# The `sequence_key` every symbol on a connection shares. Not a symbol, and
# deliberately not spellable as one, so it can never collide with a product id.
_CONNECTION = "<connection>"
_BOOK_CHANNEL = "l2_data"

_NS_DIGITS = 9
_NS_PER_S = 1_000_000_000


def _ns(value: str) -> int:
    """RFC3339 to integer nanoseconds.

    Hand-split rather than `datetime.fromisoformat`, which parses this shape
    happily and then **silently truncates to microseconds** — the probe saw
    seven, eight and nine fractional digits on the envelope clock, so three of
    them would vanish without any error to notice. The model's contract is one
    unit throughout, and ns is that unit.

    The seconds half still goes through `fromisoformat`, which is C and cheap;
    the fraction is digits and is faster handled here than by any date library.
    """
    head, _, fraction = value.rstrip("Z").partition(".")
    # The offset is explicit because `fromisoformat` on a bare timestamp
    # assumes *local* time, which would skew every clock by the machine's UTC
    # offset — silently, and differently on a laptop and in a container.
    seconds = int(datetime.fromisoformat(f"{head}+00:00").timestamp())
    return seconds * _NS_PER_S + int(fraction.ljust(_NS_DIGITS, "0")[:_NS_DIGITS])


def _levels(
    updates: Sequence[Mapping[str, Any]],
) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """Split one message's flat update list into the two sides.

    Coinbase interleaves both sides in a single list and names the ask side
    `offer`; the model has one canonical pair of sides, and translating here is
    exactly what an adapter is for.
    """
    bids: list[Level] = []
    asks: list[Level] = []
    for update in updates:
        price = update["price_level"]
        size = update["new_quantity"]
        level = Level(
            price_str=price,
            price_ticks=scaled_int(price, SCALE),
            size_str=size,
            size_lots=scaled_int(size, SCALE),
        )
        (bids if update["side"] == "bid" else asks).append(level)
    return tuple(bids), tuple(asks)


class CoinbaseAdapter:
    """The sequencing dialect, and the only place it is allowed to live."""

    venue = VENUE

    # Measured 2026-09-08: 30 accepted, 31 answered `too many L2 streams
    # requested in a single session`. The smallest cap of the three by 30x.
    max_symbols_per_connection = 30

    def __init__(self, *, ws_url: str = _WS_URL) -> None:
        self._ws_url = ws_url
        self._prev_seq: int | None = None
        self._snapshot_required = True
        # The sequence at which this connection last lost a message. Below
        # every real sequence until one does, so a snapshot taken on a
        # connection that has never broken is never stale. See `snapshot_stale`.
        self._last_break_seq = -1

    # --- transport ---------------------------------------------------------

    def ws_url(self, symbols: Sequence[str]) -> str:
        return self._ws_url

    def subscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """One frame for the whole shard: `product_ids` is already a list.

        Batching is the venue's own model here rather than a workaround, which
        is what the Phase 2 probe established.
        """
        return (self._frame("subscribe", symbols),)

    def resubscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """Unsubscribe then subscribe: the only way to ask for a snapshot."""
        return (self._frame("unsubscribe", symbols), self._frame("subscribe", symbols))

    def stream_tag(self, symbol: str) -> str:
        return f"{_CHANNEL}:{symbol.upper()}"

    def snapshot_request(self, symbol: str) -> SnapshotRequest | None:
        """None: the snapshot arrives on the socket, so there is nothing to fetch."""
        return None

    def _frame(self, action: str, symbols: Sequence[str]) -> str:
        return json.dumps(
            {
                "type": action,
                "product_ids": [symbol.upper() for symbol in symbols],
                "channel": _CHANNEL,
            },
            separators=(",", ":"),
        )

    # --- parsing -----------------------------------------------------------

    def classify(self, stream: str, payload: Mapping[str, Any]) -> Kind:
        """By channel, then by the event type nested one level inside it."""
        if payload["channel"] != _BOOK_CHANNEL:
            return "control"
        return "snapshot" if self._event(payload)["type"] == "snapshot" else "frame"

    def stream_of(self, payload: Mapping[str, Any]) -> str | None:
        """From the event's `product_id`; `None` for anything off the book channel.

        Reads the first event rather than going through `_event`, which is the
        guard and not a reader: routing has to work on a message this adapter
        is about to reject, so that the rejection names the stream it came from.
        """
        if payload.get("channel") != _BOOK_CHANNEL:
            return None
        events = payload["events"]
        return self.stream_tag(str(events[0]["product_id"])) if events else None

    def sequence_key(self, symbol: str) -> str:
        """One key for the whole connection, whatever the symbol.

        `sequence_num` counts every message on the socket — book messages for
        all products and the acks between them — so the thing that can lose a
        message is the connection, not the product. Measured over a 120s
        three-product subscribe in Phase 3: 5802 messages, 0 → 5801, no break,
        and BTC-USD held only 2229 of them.
        """
        return _CONNECTION

    def sequence_ids(self, payload: Mapping[str, Any]) -> tuple[int, int]:
        """One number per message, so the range is a point."""
        sequence = int(payload["sequence_num"])
        return sequence, sequence

    def snapshot_seq(self, payload: Mapping[str, Any]) -> int:
        return int(payload["sequence_num"])

    def parse_frame(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Update:
        sequence = int(payload["sequence_num"])
        bids, asks = _levels(self._event(payload)["updates"])
        return Update(
            first_seq=sequence,
            final_seq=sequence,
            # The envelope clock, not the per-level `event_time`: one message
            # carries one `exchange_ts`, and the envelope is the one with
            # nanosecond resolution.
            exchange_ts=_ns(payload["timestamp"]),
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=bids,
            asks=asks,
        )

    def parse_snapshot(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Snapshot:
        bids, asks = _levels(self._event(payload)["updates"])
        return Snapshot(
            final_seq=int(payload["sequence_num"]),
            exchange_ts=_ns(payload["timestamp"]),
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=bids,
            asks=asks,
        )

    def _event(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """The single book event inside the envelope.

        Every message in the probe carried exactly one, and the venue's own
        model is one product per event; more than one would mean this adapter
        is silently dropping book state, so it says so rather than taking the
        first and hoping.
        """
        events = payload["events"]
        if len(events) != 1:
            raise ValueError(f"expected one event, got {len(events)}")
        first: Mapping[str, Any] = events[0]
        return first

    # --- sequencing --------------------------------------------------------

    def chains(self, prev_final_seq: int, first_seq: int) -> bool:
        """`seq == prev_seq + 1`, over every message on the connection."""
        return first_seq == prev_final_seq + 1

    def snapshot_stale(self, first_buffered_seq: int, snapshot_seq: int) -> bool:
        """The connection broke after this snapshot, so it describes a stream
        we have not followed continuously since.

        An in-band snapshot cannot be stale *on arrival*, but the transform
        holds on to the last one it saw and a gap ten seconds later is judged
        against that. Returning False unconditionally — which this did at
        first — rebuilds the book from a snapshot the stream has long overrun,
        silently dropping every update in between, and showed up in Phase 2 as
        crossed books under fault injection.

        **The rule was `first_buffered_seq > snapshot_seq + 1` until Phase 3,
        and that was adjacency standing in for continuity.** The two are the
        same thing only when the connection carries one product. With several,
        `sequence_num` counts the whole socket, so a product's own next frame
        is several numbers past its snapshot with nothing missing at all —
        measured, 2026-09-08: three products, 5802 messages, no break, and
        BTC-USD held 2229 of them. Under the old rule every book on a
        multiplexed connection was permanently `SNAPSHOT_TOO_OLD`: two of three
        products bootstrapped once and then gapped forever, and the third never
        bootstrapped at all.

        Asking when the *connection* last broke is what adjacency was always
        approximating, and it is exact at every product count. The Phase 2
        property is preserved exactly — a gap after the snapshot still
        invalidates it — because that gap is what moves the break.
        """
        return self._last_break_seq > snapshot_seq

    def bootstrap(
        self, buffered: Sequence[Update], snapshot: Snapshot
    ) -> BootstrapResult:
        """The shared sequence walk: no straddle, because the range is a point.

        Coinbase numbers messages one at a time, so the snapshot boundary falls
        *between* messages rather than inside one. That makes the walk simpler
        than Binance's but not different, which is why both call the same
        function.
        """
        return bootstrap_by_sequence(self, buffered, snapshot)

    def bootstrapped(self, snapshot: Snapshot) -> None:
        """The cursor is the snapshot's own sequence.

        Unlike Binance, which resets to `None` because its straddling frame
        legitimately starts before the snapshot position, the next message here
        must be exactly `snapshot_seq + 1`. Resetting to `None` would waive the
        one check that makes the repair verifiable.
        """
        self._prev_seq = snapshot.final_seq
        self._snapshot_required = False

    def in_sequence(self, update: Update) -> bool:
        if self._prev_seq is None:
            return True
        return self.chains(self._prev_seq, update.first_seq)

    def gap_detected(self, update: Update) -> bool:
        if self.in_sequence(update):
            return False
        self._snapshot_required = True
        # Where the continuity broke, which is what `snapshot_stale` judges a
        # snapshot's age against. Recorded at the *update that revealed* the
        # break: everything numbered at or before it was still followed.
        self._last_break_seq = update.first_seq
        return True

    def snapshot_required(self) -> bool:
        """Repaired by a resubscribe, since the venue sends no snapshot unasked."""
        return self._snapshot_required

    def accept(self, update: Update) -> None:
        self._prev_seq = update.final_seq

    def advance(self, payload: Mapping[str, Any]) -> None:
        """Acks and snapshots are numbered too, so they move the cursor."""
        self._prev_seq = int(payload["sequence_num"])

    def observe(self, payload: Mapping[str, Any]) -> bool:
        """The venue publishes no checksum, so `chains` is the whole of it."""
        return True

    def snapshot_supersedes(self) -> bool:
        """Yes: a snapshot only ever arrives because `resubscribe_frames` asked.

        The unsubscribe/subscribe pair keeps the *sequence* unbroken — measured
        in Phase 2 — which is exactly what makes ignoring the snapshot unsafe:
        a level deleted while unsubscribed is absent from the new snapshot and
        never arrives as a delete, so nothing in the sequence marks the loss.
        """
        return True
