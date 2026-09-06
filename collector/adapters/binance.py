"""Binance spot: full-depth diffs, out-of-band REST snapshot, chained ranges.

The row of ``NOTES.md`` § *Sequencing dialects* this venue occupies: a frame
batches several internal updates and therefore carries a *range* ``[U, u]``,
successive frames chain as ``U == prev_u + 1``, and the snapshot arrives over
REST rather than on the socket. Coinbase chains single sequence numbers and
Kraken revalidates with a rolling checksum; all three answer the same
``in_sequence`` / ``gap_detected`` / ``snapshot_required`` contract, and that
is the whole of the difference downstream is allowed to see.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from collector.model import scaled_int

VENUE = "binance"

# The stream tag a captured REST depth snapshot carries. Binance's snapshot
# arrives out of band, so it has no websocket stream name of its own.
REST_DEPTH_STREAM = "rest:depth"

# Binance spot quotes no finer than eight decimal places on either price or
# quantity, so one fixed exponent scales both exactly. ``scaled_int`` raises if
# a venue ever exceeds it, which turns a silent truncation into a failed run.
PRICE_SCALE = 8
SIZE_SCALE = 8

_MS_TO_NS = 1_000_000


@dataclass(frozen=True, slots=True)
class Level:
    """One price level, both forms — see ``model`` on why there are two."""

    price_str: str
    price_ticks: int
    size_str: str
    size_lots: int


@dataclass(frozen=True, slots=True)
class DepthEvent:
    """One ``depthUpdate`` frame: a range of updates, not a single one."""

    first_id: int  # U
    final_id: int  # u
    exchange_ts: int
    receive_ts: int
    monotonic_ts: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


@dataclass(frozen=True, slots=True)
class Snapshot:
    """A REST depth snapshot. Binance sends no clock of its own with it."""

    last_update_id: int
    receive_ts: int
    monotonic_ts: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


class SpliceOutcome(StrEnum):
    """What the buffer and the snapshot say about each other."""

    READY = "ready"
    SNAPSHOT_TOO_OLD = "snapshot_too_old"
    BUFFER_BEHIND = "buffer_behind"


@dataclass(frozen=True, slots=True)
class SpliceResult:
    outcome: SpliceOutcome
    # Index of the straddling event in the buffer; everything from here on is
    # applied. Meaningless unless the outcome is READY.
    start: int = 0


def stream_name(symbol: str, interval_ms: int) -> str:
    """Binance's raw-stream name for a full-depth diff channel."""
    return f"{symbol.lower()}@depth@{interval_ms}ms"


def chains(prev_final_id: int, first_id: int) -> bool:
    """``U == prev_u + 1`` — the chain rule, with exactly one definition.

    Two callers ask different questions of it. ``BinanceDepthAdapter`` asks *is
    the book still trustworthy*; the capture source asks *did the socket skip,
    so should I fetch a fresh snapshot*. They are genuinely different decisions
    — one is about state, the other about I/O — but they are the same venue
    rule, and a venue rule gets one home (``CLAUDE.md``, the normalization
    boundary).
    """
    return first_id == prev_final_id + 1


def chain_ids(payload: Mapping[str, Any]) -> tuple[int, int]:
    """``(U, u)`` out of a raw frame, without building the record model.

    The capture path needs the ids to know when to snapshot and nothing else;
    constructing ``Level`` tuples there would put the whole normalization cost
    on a worker whose only job is landing bytes.
    """
    return int(payload["U"]), int(payload["u"])


def snapshot_too_old(first_buffered_id: int, last_update_id: int) -> bool:
    """The snapshot predates the buffer, so updates between them are lost.

    Shared by ``splice`` (which classifies) and the capture source (which
    decides to refetch), for the same one-definition reason as ``chains``.
    """
    return first_buffered_id > last_update_id + 1


def _levels(raw: Sequence[Sequence[str]]) -> tuple[Level, ...]:
    return tuple(
        Level(
            price_str=price,
            price_ticks=scaled_int(price, PRICE_SCALE),
            size_str=size,
            size_lots=scaled_int(size, SIZE_SCALE),
        )
        for price, size in raw
    )


def parse_event(
    payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
) -> DepthEvent:
    """Parse one ``depthUpdate`` frame. Timestamps land in ns here, once."""
    return DepthEvent(
        first_id=int(payload["U"]),
        final_id=int(payload["u"]),
        exchange_ts=int(payload["E"]) * _MS_TO_NS,
        receive_ts=receive_ts,
        monotonic_ts=monotonic_ts,
        bids=_levels(payload["b"]),
        asks=_levels(payload["a"]),
    )


def parse_snapshot(
    payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
) -> Snapshot:
    """Parse a ``GET /api/v3/depth`` response."""
    return Snapshot(
        last_update_id=int(payload["lastUpdateId"]),
        receive_ts=receive_ts,
        monotonic_ts=monotonic_ts,
        bids=_levels(payload["bids"]),
        asks=_levels(payload["asks"]),
    )


def splice(buffered: Sequence[DepthEvent], last_update_id: int) -> SpliceResult:
    """Locate the snapshot inside the buffered stream. ``NOTES.md`` steps 3-6.

    The snapshot position ``S`` lands *inside* a frame rather than on a
    boundary, because a frame covers a range. So the event to start from is the
    one whose range straddles ``S``: ``U <= S + 1 <= u``.

    The two failure branches are not the same failure, and this is the whole
    reason the function returns three outcomes instead of a bool:

    - ``SNAPSHOT_TOO_OLD`` — the buffer starts *ahead* of ``S + 1``, so updates
      between the snapshot and the buffer are already lost. Refetch.
    - ``BUFFER_BEHIND`` — every buffered event is in the past. The snapshot is
      fine and simply has not been reached yet. Keep buffering.

    Collapsing both into "retry" refetches on the second case, which is a
    bootstrap loop that occasionally spins.
    """
    for index, event in enumerate(buffered):
        if event.final_id <= last_update_id:
            continue  # entirely in the past — discard
        if snapshot_too_old(event.first_id, last_update_id):
            return SpliceResult(SpliceOutcome.SNAPSHOT_TOO_OLD)
        return SpliceResult(SpliceOutcome.READY, index)
    return SpliceResult(SpliceOutcome.BUFFER_BEHIND)


class BinanceDepthAdapter:
    """The sequencing dialect, and the only place it is allowed to live."""

    venue = VENUE

    def __init__(self) -> None:
        self._prev_final_id: int | None = None
        self._snapshot_required = True

    def bootstrapped(self) -> None:
        """A snapshot has been spliced in; the next event is the straddler.

        The cursor resets to ``None`` rather than to ``lastUpdateId``: the
        straddler legitimately starts *before* ``S + 1``, so the chain rule
        below would reject the one event the splice just proved correct.
        """
        self._prev_final_id = None
        self._snapshot_required = False

    def in_sequence(self, event: DepthEvent) -> bool:
        """``U == prev_u + 1``. Ranges chain; single sequence numbers do not."""
        if self._prev_final_id is None:
            return True
        return chains(self._prev_final_id, event.first_id)

    def gap_detected(self, event: DepthEvent) -> bool:
        """The inverse, and it latches ``snapshot_required``."""
        if self.in_sequence(event):
            return False
        self._snapshot_required = True
        return True

    def snapshot_required(self) -> bool:
        """Binance repairs a gap only by refetching the out-of-band snapshot.

        Distinct from ``gap_detected`` because it is a latch: it stays true for
        the whole untrusted interval, not just for the frame that broke the
        chain. A venue that revalidates with a checksum can set it without any
        gap at all, which is why the two are separate questions.
        """
        return self._snapshot_required

    def accept(self, event: DepthEvent) -> None:
        """Advance the cursor past an event that has been applied."""
        self._prev_final_id = event.final_id
