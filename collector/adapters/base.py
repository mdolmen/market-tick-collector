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
`sequence_num` and sends its snapshot in band; Kraken numbers *nothing* and
publishes a CRC32 over its own top 10 instead. All three answer `in_sequence` /
`gap_detected` / `snapshot_required`, and that uniformity is the whole of what
downstream is allowed to see.

The third row is what made the contract earn its keep. Two venues that both
prove continuity from a sequence can share one shape by accident; a venue where
the chain rule is *trivially true and proves nothing* cannot. It is why
`observe` exists, and why `snapshot_required` is a latch asked separately from
`gap_detected` rather than a return value of it.
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

    def classify(self, stream: str, payload: Mapping[str, Any]) -> Kind:
        """Frame, snapshot, or control.

        `control` is a subscription ack, heartbeat or status message: no book
        state, no rows. It is still landed, because a venue may count it in the
        same sequence it counts book messages — Coinbase does — and a capture
        missing it replays as a gap that never happened.

        This runs at capture time, because `FaultInjector` reads `kind` off
        disk to know what it may perturb, so the landed value has to be right.
        """
        ...

    def stream_of(self, payload: Mapping[str, Any]) -> str | None:
        """The `stream` tag this message belongs to, or `None` for the connection.

        The demultiplexer for a socket carrying many symbols, and the reason
        `capture.py` reserved `stream` in Phase 1 rather than adding a field
        later. `None` means the message is about the connection rather than any
        one symbol — a subscription ack, a heartbeat, a status frame — and it
        is the source's job to give those a tag of their own.

        It returns the *tag*, not the symbol, because `record["stream"]` is what
        every downstream demux reads. Returning a symbol would make the hot path
        `stream_tag(stream_of(payload))`, which is two calls and two concepts
        where one will do. `stream_tag` still exists for the other direction:
        building the tag from a symbol we already hold.

        Cheap on purpose, like `sequence_ids` and for the same reason — the
        capture source calls it on every message and must not pay for the
        record model to route one.
        """
        ...

    def sequence_key(self, symbol: str) -> str:
        """Whose sequence this symbol's messages belong to.

        The answer is the symbol itself on a venue that numbers each book
        independently — Binance's `[U, u]` are order-book ids and Kraken's
        checksum is per symbol, so two symbols on one socket prove nothing
        about each other. It is a **single shared constant** on a venue that
        numbers the *connection*: Coinbase counts every message on the socket,
        acks included, so with fifty products on it no product's messages are
        adjacent to each other and a per-symbol cursor would report a gap on
        essentially every frame.

        This exists so that nothing has to branch on which. A caller keys its
        sequence state by this, and a connection-scoped venue collapses to one
        entry without a conditional anywhere — the aliasing *is* the venue
        fact. A boolean would put an `if` at every call site, and each of those
        is a place the venue identity leaks back in past the boundary that
        `tests/test_boundary.py` guards.

        It follows that a break is scoped the same way. One lost message on
        Coinbase means every book on that connection is untrusted, because the
        sequence cannot say which product the missing message was about. That
        is a real cost of multiplexing on that venue and the reason
        `NOTES.md` § *Connection supervision* sizes shards by blast radius.
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
        which repairs on it.

        It is tempting to think an in-band snapshot can never be stale, since
        there is no fetch window for updates to be lost in. That is wrong, and
        wrong in a way that corrupts a book rather than failing loudly: the
        transform keeps the *last* snapshot it saw, and when a gap opens much
        later that snapshot is far behind the buffer. Rebuilding from it
        silently discards every update in between. The test that catches it is
        a crossed book.
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
        same failure: an old snapshot needs a fresh one, a young one needs
        patience, and collapsing them gives a bootstrap loop that occasionally
        spins. Both venues so far delegate to `bootstrap_by_sequence`; it stays
        a protocol member because a checksum venue would not.
        """
        ...

    def observe(self, payload: Mapping[str, Any]) -> bool:
        """The venue's own integrity check over the book after this message.

        True when it passed, and true unconditionally on a venue that
        publishes no such check — which is both of the first two dialect rows:
        they prove continuity from the sequence alone, and the sequence is
        already in `chains`.

        False is returned **once, on the message that broke it**, not for the
        whole untrusted interval — the same distinction `gap_detected` and
        `snapshot_required` already draw, and for the same reason. A caller
        that acted on it every message would ask for a repair again before the
        first one could arrive.

        The third row has no sequence to prove anything with. Kraken numbers
        nothing and instead publishes a CRC32 over its own top 10 on every
        message, so the only way to know the book is still right is to keep an
        ordered view of the top levels, apply the message to it, and recompute.
        That is what a checksum venue does here, latching `snapshot_required`
        when the two disagree.

        **Called once per book message, in arrival order, by both drivers.**
        The capture source calls it to decide whether to ask for a repair; the
        transform calls it to decide whether the book is still trustworthy —
        the same two questions over one venue rule that `chains` already
        serves, and the reason each of them holds its own adapter instance.

        It is also why `snapshot_required` is a separate question from
        `gap_detected`: this can fail with the message sequence intact, and on
        a venue with no sequence it is the *only* thing that can fail.
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

    def snapshot_supersedes(self) -> bool:
        """Does a snapshot from this venue *replace* the book, or describe it?

        False where a snapshot is a read taken alongside a diff stream that
        never stopped — Binance's REST depth call, which costs the socket
        nothing. Such a snapshot at a healthy book is redundant, and rebuilding
        from it would emit a full book's worth of rows to arrive exactly where
        the book already is.

        True where the *only* way to obtain one is to ask the venue, and asking
        means unsubscribe then subscribe — which interrupts the diff stream.
        A level deleted while unsubscribed is simply absent from the new
        snapshot; the venue never sends a delete for it, and no sequence is
        skipped, so a book that keeps its old state stays plausible and is
        wrong. That is the failure `NOTES.md` § *Steady state* calls the worst
        available, and it is why this question exists rather than the transform
        assuming either answer.
        """
        ...

    def bootstrapped(self, snapshot: Snapshot) -> None:
        """A snapshot has been applied; set the sequence cursor from it.

        Takes the snapshot because the two dialects want opposite things from
        it. Binance must forget the cursor entirely — its straddling frame
        legitimately begins *before* the snapshot position, so the chain rule
        would reject the one frame the bootstrap just proved correct. Coinbase
        must set the cursor *to* the snapshot's own sequence, because the next
        message has to be exactly one past it and dropping that check would
        waive the only proof the repair worked.
        """
        ...

    def accept(self, update: Update) -> None:
        """Advance the cursor past an update that has been applied."""
        ...

    def advance(self, payload: Mapping[str, Any]) -> None:
        """Advance the cursor past a message that yields no `Update`.

        Control traffic and in-band snapshots both land here, because both can
        occupy a sequence number without describing a book mutation this
        adapter will ever be handed as an `Update`. Where a venue numbers them
        and this is not called, the *next* frame fails the chain rule, asks for
        a repair, and the repair's own snapshot fails it again — a loop that
        costs a full snapshot every time round.

        A no-op wherever the venue's snapshot arrives out of band or its
        control traffic sits outside the sequence, which is Binance on both
        counts.
        """
        ...


class VenueTransport(Protocol):
    """How to connect to one venue and, if it needs one, how to fetch a
    snapshot. Only the capture source sees this."""

    def ws_url(self, symbols: Sequence[str]) -> str:
        """The socket to open for this shard. Some venues encode it here."""
        ...

    def subscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """Frames to send once connected, in order.

        Empty for a venue that subscribes through the URL path. Kept as a
        sequence because a venue may need a separate frame per channel, and
        because `NOTES.md` § *Connection supervision* batches subscriptions
        against an outbound rate limit.

        **Takes the whole shard, and batching is the point.** Binance allows
        five outbound frames a second, so subscribing four hundred symbols one
        at a time would take eighty seconds during which the book is neither
        bootstrapped nor abandoned. Every venue here can name many symbols in
        one frame — Coinbase in `product_ids`, Kraken in `params.symbol`,
        Binance in the URL path — so a shard costs at most one frame and the
        rate limit stops being a constraint rather than being managed.
        """
        ...

    def stream_tag(self, symbol: str) -> str:
        """The `stream` a socket message's capture record carries."""
        ...

    def resubscribe_frames(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """Frames that make an in-band venue send a fresh snapshot.

        Takes a sequence for symmetry with `subscribe_frames`, but the caller
        passes only the symbols that actually need repairing — a repair is
        per-symbol, and unsubscribing a healthy book to fix a broken one would
        turn one untrusted book into a shard's worth.

        The in-band counterpart of an out-of-band REST refetch, and the source
        makes the same decision for both — only the action differs. Coinbase
        never re-sends a snapshot spontaneously, so a book that has gone
        untrusted can only be repaired by asking for one; unsubscribe followed
        by subscribe does it, and the connection's sequence continues unbroken
        across the pair (measured, Phase 2). Empty for a venue with an
        out-of-band snapshot, which refetches instead.
        """
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


def bootstrap_by_sequence(
    adapter: VenueAdapter, buffered: Sequence[Update], snapshot: Snapshot
) -> BootstrapResult:
    """The bootstrap for any venue whose messages carry monotone ids.

    Shared because both dialects reduce to it, for reasons that only look
    different. Binance's frames cover a *range*, so the snapshot position falls
    inside one and the straddler is the first frame whose range ends past it.
    Coinbase numbers messages one at a time, so the range is a point and the
    same walk lands on the first message after the snapshot. Writing it twice
    would be two copies of one rule.

    What stays per-venue is `snapshot_stale`, and the three outcomes exist
    because its two failures are not the same failure: a snapshot the buffer
    has already overrun needs a fresh one, a snapshot the buffer has not
    reached yet needs patience. Collapsing them into "retry" spins.
    """
    for index, update in enumerate(buffered):
        if update.final_seq <= snapshot.final_seq:
            continue  # entirely in the past — discard
        if adapter.snapshot_stale(update.first_seq, snapshot.final_seq):
            return BootstrapResult(BootstrapOutcome.SNAPSHOT_TOO_OLD)
        return BootstrapResult(BootstrapOutcome.READY, index)
    return BootstrapResult(BootstrapOutcome.BUFFER_BEHIND)
