"""Kraken v2: in-band snapshot, no sequence at all, a CRC32 over the top 10.

The third row of `NOTES.md` § *Sequencing dialects*, and the one the whole
`in_sequence` / `gap_detected` / `snapshot_required` contract was shaped by.
Binance and Coinbase differ in *how* they number messages; this venue does not
number them. Its `data[]` entries are `symbol`, `bids`, `asks`, `checksum` and
`timestamp`, and the checksum is the entire integrity story.

That is why Phase 2 deferred it. An adapter here without the CRC would answer
`in_sequence` with `True` forever and let the book drift silently while staying
plausible — the failure mode `NOTES.md` § *Steady state* calls the worst
available. The checksum is not a supplementary check on this venue; it is the
only one, so it ships in the same commit as the adapter or neither ships.

Four things the Phase 2.5 probe established that are not guessable, all of them
measured over a landed 60s BTC/USD session at depth 1000 (5129 book messages):

- **The source token *is* the checksum input.** Kraken sends price and qty as
  JSON numbers already padded to the instrument's precision — `79496.8`,
  `0.00550000` — so the CRC needs nothing but the token with its `.` and its
  leading zeros removed. Recomputing it that way matched the venue on 5129 of
  5129 messages. The alternative reading, that the tokens must be re-padded
  from `price_precision` / `qty_precision` off the `instrument` channel, would
  have cost a second committed data artifact and is simply not needed.
- **The token only survives `parse_float=str`**, which `collector.capture`
  now does for every venue. See the note there; it is the project's oldest rule
  and this is merely the venue that made it visible.
- **There is no full-depth channel.** `depth` is one of 10/25/100/500/1000 and
  1000 is the ceiling, so `NOTES.md`'s "full-depth diff channels" is false here
  and this venue's book is a top-1000 one by construction. The checksum covers
  the top 10 whatever depth is subscribed.
- **One depth per symbol per connection.** Subscribing BTC/USD at 1000 and
  again at 10 on one socket answers the second with
  `{"error":"Already subscribed","success":false}`, so Phase 8's Oracle 2 — the
  venue's own depth-limited top-N alongside full depth — does not exist for
  Kraken on a single connection. The CRC is its oracle.

**The `timestamp` was considered as a sequence and rejected.** It is present on
snapshots and updates alike and was strictly monotone and unique across all
5129 messages, so it would have worked. It is still a *clock*, and this project
does not order books by clocks — `NOTES.md` § *Reconciliation* says "align by
update id, never by clock" and `collector.model` says wall clocks can step. A
rule that holds for 60 seconds is not a guarantee, and two messages sharing a
microsecond would silently discard one. So the ordering below is a counter of
our own, and it is honest about proving nothing.
"""

from __future__ import annotations

import json
import zlib
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

VENUE = "kraken"

_WS_URL = "wss://ws.kraken.com/v2"
_CHANNEL = "book"

# The venue checksums this many levels a side whatever depth is subscribed.
_CHECKSUM_LEVELS = 10

_NS_DIGITS = 9
_NS_PER_S = 1_000_000_000


def _ns(value: str) -> int:
    """RFC3339 to integer nanoseconds — the same hand-split as Coinbase's.

    Kraken's clock is microseconds where Coinbase's is nanoseconds, so
    `fromisoformat` would not truncate this one today. It is parsed the same
    way regardless: the model's contract is one unit throughout, and a venue
    that adds three digits later must not silently start losing them.
    """
    head, _, fraction = value.rstrip("Z").partition(".")
    seconds = int(datetime.fromisoformat(f"{head}+00:00").timestamp())
    return seconds * _NS_PER_S + int(fraction.ljust(_NS_DIGITS, "0")[:_NS_DIGITS])


def checksum_token(value: str) -> str:
    """One value's contribution to the CRC input: no `.`, no leading zeros.

    `45285.2` becomes `452852` and `0.00100000` becomes `100000`. The venue
    sends both already padded to the instrument's precision, so this reads the
    token and never reformats it — which is the whole reason the decode has to
    preserve it.
    """
    return value.replace(".", "").lstrip("0") or "0"


def _levels(
    raw: Sequence[Mapping[str, Any]],
) -> tuple[Level, ...]:
    """One side of one message. Named fields, and both forms of each value."""
    return tuple(
        Level(
            price_str=level["price"],
            price_ticks=scaled_int(level["price"], SCALE),
            size_str=level["qty"],
            size_lots=scaled_int(level["qty"], SCALE),
        )
        for level in raw
    )


class _Side:
    """One side of the checksum view: the top levels, ordered on demand.

    Not the reconstructed book. `collector.book.Book` is that, it is keyed by
    integer ticks and it holds no strings, so it cannot produce the thing a CRC
    needs — the top ten levels *as the venue spelled them*. This holds exactly
    that, bounded by the subscribed depth, and it is the price of the third
    dialect row (`NOTES.md` § *The checksum needs a book of its own*).

    **Why it is not a plain `dict` scanned per message.** The checksum makes an
    ordered top-N read a *per-frame* cost, which is the read `NOTES.md`
    § *Book representation* names as the one a `dict` is worst at. From
    `bench/checksum.py` over a landed 5129-message session at depth 1000, per
    book message:

        sort the side, per message              73.9 us   <- the obvious one
        int keys, token cached per level        26.0 us
        rebuilt only when it can move            8.5 us   <- this

    The last one is this class, and the last figure is the whole of `observe`
    rather than this class alone — it includes building the normalized levels,
    which the other two rows do not do. Almost nothing moves the top ten: the
    session's median message touches **one** level, and only 18.5% of sides
    needed a rebuild at all. So the window is cached alongside the price that
    bounds it, and a message that cannot have disturbed it does not pay for
    one.
    """

    __slots__ = ("_bound", "_cached", "_descending", "_dirty", "_levels", "_window")

    def __init__(self, *, descending: bool) -> None:
        # ticks -> that level's finished checksum fragment. Built at apply
        # time because a level is written once and read on every rebuild.
        self._levels: dict[int, str] = {}
        self._descending = descending
        self._cached: tuple[str, ...] = ()
        # The top window's keys, and the worst price in it. Together they are
        # the whole of "could this update have moved the top ten".
        self._window: frozenset[int] = frozenset()
        self._bound: int | None = None
        self._dirty = True

    def clear(self) -> None:
        self._levels.clear()
        self._cached = ()
        self._window = frozenset()
        self._bound = None
        self._dirty = True

    def apply(self, level: Level) -> None:
        if not self._dirty and self._disturbs(level.price_ticks):
            self._dirty = True
        if level.size_lots == 0:
            self._levels.pop(level.price_ticks, None)
        else:
            self._levels[level.price_ticks] = checksum_token(
                level.price_str
            ) + checksum_token(level.size_str)

    def _disturbs(self, price_ticks: int) -> bool:
        """Could a write at this price change the top ten?

        Three ways, and missing the third is a checksum that passes while the
        window is stale: the level is already in the window, it is good enough
        to enter it, or the window is not full — in which case any level at all
        belongs in it.
        """
        if self._bound is None or len(self._cached) < _CHECKSUM_LEVELS:
            return True
        if price_ticks in self._window:
            return True
        return (
            price_ticks >= self._bound
            if self._descending
            else price_ticks <= self._bound
        )

    def top(self) -> tuple[str, ...]:
        """The top ten levels' checksum fragments, best price first."""
        if not self._dirty:
            return self._cached
        keys = sorted(self._levels, reverse=self._descending)[:_CHECKSUM_LEVELS]
        self._cached = tuple(self._levels[key] for key in keys)
        self._window = frozenset(keys)
        self._bound = keys[-1] if keys else None
        self._dirty = False
        return self._cached

    def trim(self, depth: int, *, above: int) -> None:
        """Drop levels the venue's own window no longer holds, once there are
        more than `above` of them.

        A memory bound, not a correctness one — the CRC only ever reads ten
        levels. It is needed because **the venue does not delete every level
        that falls out of its depth window**: the landed session's book grew
        from 1000 to 1040 levels a side in sixty seconds while every one of its
        5129 checksums still matched. Left alone that grows without limit over
        a run measured in days.

        Amortised against `above` rather than run per message, because it walks
        the whole side. Safe because our extra levels are exactly the ones the
        venue has already dropped, and they are the worst ones: keeping the
        best `depth` keeps everything the venue still holds.
        """
        if len(self._levels) <= above:
            return
        keep = sorted(self._levels, reverse=self._descending)[:depth]
        self._levels = {key: self._levels[key] for key in keep}


class KrakenAdapter:
    """The sequencing dialect, and the only place it is allowed to live."""

    venue = VENUE

    # 200 symbols, documented; 188 verified at depth 1000 on one socket
    # 2026-09-08 at 2188 msg/s, which is the whole overlapping set.
    max_symbols_per_connection = 200

    def __init__(self, *, ws_url: str = _WS_URL, depth: int = 1000) -> None:
        self._ws_url = ws_url
        self._depth = depth
        self._bids = _Side(descending=True)
        self._asks = _Side(descending=False)
        # Ours, not the venue's. See `observe`.
        self._seq = 0
        self._prev_seq: int | None = None
        self._snapshot_required = True
        # Trimming is amortised: it walks the whole side, so it runs when the
        # drift has built up rather than on every message.
        self._trim_at = depth + depth // 10

    # --- transport ---------------------------------------------------------

    def ws_url(self, symbols: Sequence[str]) -> str:
        return self._ws_url

    def subscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """One frame for the shard: `params.symbol` is already a list.

        The venue answers with one ack and one snapshot **per symbol**, not one
        of each for the frame — measured over a three-symbol subscribe in
        Phase 3, which returned three of each.
        """
        return (self._frame("subscribe", symbols),)

    def resubscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """Unsubscribe then subscribe — the Coinbase pattern, same reason.

        The venue never re-sends a snapshot unasked, so a book that has failed
        its checksum can only be repaired by asking for one.
        """
        return (self._frame("unsubscribe", symbols), self._frame("subscribe", symbols))

    def stream_tag(self, symbol: str) -> str:
        return f"{_CHANNEL}:{symbol.upper()}"

    def snapshot_request(self, symbol: str) -> SnapshotRequest | None:
        """None: the snapshot arrives on the socket, so there is nothing to fetch."""
        return None

    def _frame(self, action: str, symbols: Sequence[str]) -> str:
        return json.dumps(
            {
                "method": action,
                "params": {
                    "channel": _CHANNEL,
                    "symbol": [symbol.upper() for symbol in symbols],
                    "depth": self._depth,
                },
            },
            separators=(",", ":"),
        )

    # --- parsing -----------------------------------------------------------

    def classify(self, stream: str, payload: Mapping[str, Any]) -> Kind:
        """By channel, then by the message's own type.

        Everything that is not the book channel is control: `heartbeat`,
        `status`, and the `method` acks — including the failed ones, which
        carry `error` and no `channel` at all.
        """
        if payload.get("channel") != _CHANNEL:
            return "control"
        return "snapshot" if payload["type"] == "snapshot" else "frame"

    def stream_of(self, payload: Mapping[str, Any]) -> str | None:
        """From the entry's `symbol`; `None` off the book channel.

        Reads `data[0]` rather than going through `_entry`, for the reason the
        Coinbase one does: routing must survive a message the guard rejects.
        """
        if payload.get("channel") != _CHANNEL:
            return None
        data = payload["data"]
        return self.stream_tag(str(data[0]["symbol"])) if data else None

    def sequence_key(self, symbol: str) -> str:
        """The symbol: the CRC32 is over *this* book's top ten and no other."""
        return symbol.upper()

    def sequence_ids(self, payload: Mapping[str, Any]) -> tuple[int, int]:
        """The counter as it stands. A read, never a write — see `observe`."""
        return self._seq, self._seq

    def snapshot_seq(self, payload: Mapping[str, Any]) -> int:
        return self._seq

    def parse_frame(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Update:
        entry = self._entry(payload)
        return Update(
            first_seq=self._seq,
            final_seq=self._seq,
            exchange_ts=_ns(entry["timestamp"]),
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=_levels(entry["bids"]),
            asks=_levels(entry["asks"]),
        )

    def parse_snapshot(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Snapshot:
        entry = self._entry(payload)
        return Snapshot(
            final_seq=self._seq,
            # Unlike Binance's REST depth response, this one carries a clock.
            exchange_ts=_ns(entry["timestamp"]),
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=_levels(entry["bids"]),
            asks=_levels(entry["asks"]),
        )

    def _entry(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """The single book entry inside the envelope.

        One subscription is one symbol, so more than one entry would mean this
        adapter is silently dropping book state. It says so instead.
        """
        data = payload["data"]
        if len(data) != 1:
            raise ValueError(f"expected one book entry, got {len(data)}")
        first: Mapping[str, Any] = data[0]
        return first

    # --- sequencing --------------------------------------------------------

    def observe(self, payload: Mapping[str, Any]) -> bool:
        """Count the message, then check the venue's CRC32 over the top ten.

        **The counter is ours.** The venue numbers nothing, so this numbers its
        book messages in arrival order — which makes `bootstrap_by_sequence`
        and the shared staleness rule work unchanged, and proves *exactly
        nothing* about whether a message was lost. `chains` over it is
        trivially true, which is the honest shape of this venue: sequencing is
        not evidence here, the checksum is.

        Both drivers call this once per book message on their own instance —
        the source to decide whether to resubscribe, the transform to decide
        whether the book is trustworthy — so the parse members below are reads
        of the counter rather than writes to it, and stay callable twice.

        While the book is already untrusted the view is not maintained, and
        this answers True: the repair is a fresh snapshot that replaces the
        view wholesale, so applying messages to one known to be wrong buys a
        failing checksum per message and nothing else. False is the *edge*,
        reported once, exactly as the contract asks.
        """
        self._seq += 1
        if payload["type"] == "snapshot":
            self._bids.clear()
            self._asks.clear()
            self._apply(payload)
            # A snapshot is self-describing: it carries the checksum of the
            # book it *is*, so a corrupt one is caught before anything is
            # rebuilt from it rather than one message later.
            matched = self._matches(payload)
            self._snapshot_required = not matched
            return matched
        if self._snapshot_required:
            return True
        self._apply(payload)
        if self._matches(payload):
            return True
        self._snapshot_required = True
        return False

    def _apply(self, payload: Mapping[str, Any]) -> None:
        entry = self._entry(payload)
        for side, raw in ((self._bids, entry["bids"]), (self._asks, entry["asks"])):
            for level in _levels(raw):
                side.apply(level)
            side.trim(self._depth, above=self._trim_at)

    def _matches(self, payload: Mapping[str, Any]) -> bool:
        """Asks best-first, then bids best-first, CRC32, unsigned 32-bit."""
        parts = self._asks.top() + self._bids.top()
        computed = zlib.crc32("".join(parts).encode()) & 0xFFFFFFFF
        return computed == int(self._entry(payload)["checksum"])

    def chains(self, prev_final_seq: int, first_seq: int) -> bool:
        """`seq == prev_seq + 1` over a counter we assigned ourselves.

        Which means it is always true, and that is the point rather than a
        defect. It is written out instead of returning `True` so that the two
        callers keep asking one venue rule one way, and so that the reason this
        venue needs `observe` is legible right here: the chain holds and tells
        you nothing.
        """
        return first_seq == prev_final_seq + 1

    def snapshot_stale(self, first_buffered_seq: int, snapshot_seq: int) -> bool:
        """The buffer starts past the snapshot, so updates between are lost.

        Coinbase's rule unchanged, and it applies for the same reason: the
        transform keeps the last snapshot it saw and judges a break ten seconds
        later against it, which without this rebuilds from a snapshot the
        stream has long overrun.
        """
        return first_buffered_seq > snapshot_seq + 1

    def bootstrap(
        self, buffered: Sequence[Update], snapshot: Snapshot
    ) -> BootstrapResult:
        """The shared sequence walk, over the counter `observe` assigns."""
        return bootstrap_by_sequence(self, buffered, snapshot)

    def snapshot_supersedes(self) -> bool:
        """Yes, and here it is not even subtle: the book is depth-limited.

        A snapshot only arrives because `resubscribe_frames` asked, and the
        unsubscribe/subscribe pair interrupts the diff stream. Everything that
        left the top-1000 window in that gap is absent from the new snapshot
        and is never sent as a delete — and with no sequence at all, nothing
        marks the loss. The checksum would not catch it either while the
        staleness sits below the top ten, which is exactly how it stays
        plausible.
        """
        return True

    def bootstrapped(self, snapshot: Snapshot) -> None:
        """The cursor is the snapshot's own position in the arrival order."""
        self._prev_seq = snapshot.final_seq
        self._snapshot_required = False

    def in_sequence(self, update: Update) -> bool:
        if self._prev_seq is None:
            return True
        return self.chains(self._prev_seq, update.first_seq)

    def gap_detected(self, update: Update) -> bool:
        """The checksum's verdict, not the chain's.

        This is the whole difference between the third dialect row and the
        other two. On Binance and Coinbase the chain rule detects the loss and
        sets the latch; here the chain rule cannot detect anything, so the
        latch is set by `observe` and read back out here.
        """
        return self._snapshot_required

    def sequence_broke(self, seq: int) -> None:
        """A no-op: the CRC32 is over this book alone.

        A neighbour's break says nothing about whether this book still
        matches the venue's checksum, which is the only thing that can.
        """

    def snapshot_required(self) -> bool:
        """Repaired by a resubscribe, since the venue sends no snapshot unasked."""
        return self._snapshot_required

    def accept(self, update: Update) -> None:
        self._prev_seq = update.final_seq

    def advance(self, payload: Mapping[str, Any]) -> None:
        """No-op: control traffic sits outside the counter `observe` keeps.

        Kraken's heartbeats, status frames and acks are not numbered by the
        venue and not counted by us, so losing one is invisible — as it should
        be, because on this venue losing a *book* message is invisible to
        sequencing too. That is what the checksum is for.
        """
