"""The normalization boundary, as a type.

`CLAUDE.md` calls this boundary the design: an adapter's only job is turning
one venue's frames into the normalized record model, and nothing downstream —
book, batching, sink, recon, metrics — may learn which venue a record came from
except as a label. Phase 0 and 1 stated that and did not enforce it; the book
called `binance.parse_event` directly. This module is where it becomes
checkable, and `tests/test_boundary.py` is what checks it.

Two protocols rather than one, because there are two audiences and only one of
them is allowed to see the venue's I/O:

- `VenueAdapter` is pure — parsing and sequencing, no network. The book
  transform is typed against this alone, which is what lets a replay drive the
  same object from disk. Adding a method here that does I/O breaks replay.
- `VenueTransport` is connection concerns, and only the capture source sees it.

**The sequencing dialect is the one honest exception** (`NOTES.md`
§ *Sequencing dialects*). Binance chains overlapping `[U, u]` ranges and
bootstraps from an out-of-band REST snapshot; Coinbase chains a single
`sequence_num` and sends its snapshot in band. Both answer `in_sequence` /
`gap_detected` / `snapshot_required`, and that uniformity is the whole of what
downstream is allowed to see.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from collector.capture import Kind


@dataclass(frozen=True, slots=True)
class Level:
    """One price level, in both forms — see `collector.model` on why two."""

    price_str: str
    price_ticks: int
    size_str: str
    size_lots: int


@dataclass(frozen=True, slots=True)
class Update:
    """One book-mutating frame, normalized.

    `first_seq` and `final_seq` are a *range* because a venue may batch several
    internal updates into one message: Binance's `[U, u]` covers many ids, and
    the snapshot position can land inside a frame rather than on a boundary. A
    venue that numbers messages one at a time sets both to the same value, and
    the chain rule reads identically either way.
    """

    first_seq: int
    final_seq: int
    # Null where the venue sends no clock of its own with the message. The
    # other two are ours, and `monotonic_ts` is the only safe basis for a
    # duration — the wall clocks can step.
    exchange_ts: int | None
    receive_ts: int
    monotonic_ts: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


@dataclass(frozen=True, slots=True)
class Snapshot:
    """A full book state to bootstrap from, however the venue delivered it."""

    final_seq: int
    exchange_ts: int | None
    receive_ts: int
    monotonic_ts: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


class BootstrapOutcome(StrEnum):
    """What the buffer and the snapshot say about each other."""

    READY = "ready"
    SNAPSHOT_TOO_OLD = "snapshot_too_old"
    BUFFER_BEHIND = "buffer_behind"


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    outcome: BootstrapOutcome
    # Index of the first applicable event in the buffer; everything from here
    # on is applied. Meaningless unless the outcome is READY.
    start: int = 0


@dataclass(frozen=True, slots=True)
class SnapshotRequest:
    """An out-of-band snapshot fetch, for a venue that does not send one.

    `stream` is the tag the resulting capture record carries, so a landed file
    can tell a REST snapshot from a socket frame without parsing either.
    """

    stream: str
    url: str
    params: Mapping[str, str | int]


class VenueAdapter(Protocol):
    """Parsing and sequencing for one venue. No I/O, ever.

    Two instances of the same adapter normally exist per run: the transform
    holds one to answer *is the book still trustworthy*, and the capture source
    holds another to answer *did the socket skip, so should I fetch*. They are
    different decisions over the same venue rule, and keeping the rule in one
    class is what stops them drifting apart.
    """

    venue: str

    def classify(self, stream: str, payload: Mapping[str, Any]) -> Kind | None:
        """Frame, snapshot, or neither.

        `None` is control traffic — subscription acks, heartbeats, status —
        which is dropped rather than landed. Binance never produces any;
        Coinbase and Kraken both do, which is why it is in the contract.

        This runs at capture time, because `FaultInjector` reads `kind` off
        disk to guarantee it never drops a snapshot, so the landed value has
        to be right. The transform re-derives it and asserts agreement, so a
        replay does not inherit a capture-time mistake in silence.
        """
        ...

    def sequence_ids(self, payload: Mapping[str, Any]) -> tuple[int, int]:
        """`(first, final)` out of a raw payload, without building the model.

        The capture source needs the ids to know when to snapshot and nothing
        else; constructing `Level` tuples there would put the whole
        normalization cost on a worker whose only job is landing bytes.
        """
        ...

    def snapshot_seq(self, payload: Mapping[str, Any]) -> int:
        """A snapshot's sequence, without building the model — see above.

        Worth its own member rather than reading `parse_snapshot(...).final_seq`
        in the source: a depth-limited snapshot is 5000 levels on Binance, and
        constructing all of them to read one integer is the exact cost the
        capture worker exists not to pay.
        """
        ...

    def chains(self, prev_final_seq: int, first_seq: int) -> bool:
        """The venue's chain rule, with exactly one definition.

        Two callers ask different questions of it. The transform asks *is the
        book still trustworthy*; the capture source asks *did the socket skip,
        so should I fetch a fresh snapshot*. Genuinely different decisions —
        one about state, the other about I/O — over one venue rule, and a venue
        rule gets one home.
        """
        ...

    def snapshot_stale(self, first_buffered_seq: int, snapshot_seq: int) -> bool:
        """The snapshot predates the buffer, so updates between them are lost.

        Shared by `bootstrap`, which classifies it, and the capture source,
        which refetches on it. Always false for a venue whose snapshot arrives
        in band, since there is no window for one to open in.
        """
        ...

    def parse_frame(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Update:
        """One book-mutating message. Timestamps land in ns here, once."""
        ...

    def parse_snapshot(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Snapshot:
        """One full book state, however the venue delivered it."""
        ...

    def bootstrap(
        self, buffered: Sequence[Update], snapshot: Snapshot
    ) -> BootstrapResult:
        """Locate the snapshot inside the buffered stream.

        Three outcomes rather than a bool because the two failures are not the
        same failure: an old snapshot needs a refetch, a young one needs
        patience, and collapsing them gives a bootstrap loop that occasionally
        spins. A venue whose snapshot arrives in band on the same socket can
        only ever return READY, and says so by returning it.
        """
        ...

    def in_sequence(self, update: Update) -> bool:
        """Does this update follow the last one accepted?"""
        ...

    def gap_detected(self, update: Update) -> bool:
        """The inverse, and it latches `snapshot_required`."""
        ...

    def snapshot_required(self) -> bool:
        """Whether the book needs a fresh snapshot before it can be trusted.

        Distinct from `gap_detected` because it is a latch: it stays true for
        the whole untrusted interval, not just the frame that broke the chain.
        A venue that revalidates with a checksum can set it with no gap at all,
        which is why the two are separate questions.
        """
        ...

    def bootstrapped(self) -> None:
        """A snapshot has been spliced in; reset the sequence cursor."""
        ...

    def accept(self, update: Update) -> None:
        """Advance the cursor past an update that has been applied."""
        ...


class VenueTransport(Protocol):
    """How to connect to one venue and, if it needs one, how to fetch a
    snapshot. Only the capture source sees this."""

    def ws_url(self, symbol: str) -> str:
        """The socket to open. Some venues encode the subscription here."""
        ...

    def subscribe_frames(self, symbol: str) -> tuple[str, ...]:
        """Frames to send once connected, in order.

        Empty for a venue that subscribes through the URL path. Kept as a
        sequence because a venue may need a separate frame per channel, and
        because `NOTES.md` § *Connection supervision* batches subscriptions
        against an outbound rate limit.
        """
        ...

    def stream_tag(self, symbol: str) -> str:
        """The `stream` a socket message's capture record carries."""
        ...

    def snapshot_request(self, symbol: str) -> SnapshotRequest | None:
        """The out-of-band fetch, or `None` when the venue sends its own.

        `None` selects the in-band path in the capture source: subscribe,
        receive, classify, and never call `ctx.http` at all.
        """
        ...


class Venue(VenueAdapter, VenueTransport, Protocol):
    """Both halves, which is what one concrete adapter implements.

    Only the capture source asks for this. The book transform takes a
    `VenueAdapter`, deliberately the narrower of the two, so that no amount of
    later drift can put a socket or an HTTP call on the replay path.
    """
