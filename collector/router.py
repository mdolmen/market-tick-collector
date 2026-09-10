"""One shard's records in, every symbol's book out.

`BookTransform` rebuilds one book and has never read `record["stream"]` — it
assumes everything it is handed is its symbol's. That was true while a
connection carried one symbol. This is the demultiplexer that keeps it true
now: one transform per stream tag, each fed only its own records.

**Two adapters per shard, not two per symbol, where the venue numbers the
connection.** A transform's adapter answers *is this book still trustworthy*,
and on a venue whose `sequence_num` counts the whole socket that question is
not per-symbol: a lost message could have been about any product on it. So
`collector.adapters.build_router` keys adapters by `VenueAdapter.sequence_key`,
exactly as the capture source keys its cursors, and a connection-scoped venue
collapses to one adapter shared by every transform on the shard. The
consequences are the ones the venue imposes rather than ones chosen here:

- Every message on the connection advances that one cursor, whichever book it
  was about, which is the only way the chain rule can be right at all.
- A gap latches `snapshot_required` for the whole shard, so every book on it
  goes untrusted together. That is the blast radius `NOTES.md`
  § *Connection supervision* sizes shards against, and it is real: the sequence
  cannot say which product the missing message described.

Control traffic belongs to the connection and to no book, so it is applied to
each distinct adapter once rather than handed to a transform. Handing it to one
would advance a shared cursor correctly and a per-symbol cursor wrongly;
handing it to all would advance a shared cursor N times. Neither is right, and
`advance` is a no-op on a venue that leaves control outside its sequence.

Venue-neutral throughout: it dispatches on an opaque tag and holds adapters it
cannot name. `tests/test_boundary.py` enforces that.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from data_pipeline_core import RunContext

from collector.adapters.base import VenueAdapter
from collector.capture import CONTROL_STREAM, CaptureRecord, payload_of
from collector.model import LevelRow
from collector.transform import BookTransform


@dataclass
class Gate:
    """One connection's sequence cursor, and the books that share its fate."""

    adapter: VenueAdapter
    books: tuple[BookTransform, ...]
    prev: int | None = None


class BookRouter:
    """A `Transform` over a shard: dispatch each record to its symbol's book.

    `books` maps a stream tag to the transform that owns it. `sequencers` are
    the shard's *distinct* adapters — one per `sequence_key`, so one in total
    on a connection-scoped venue — and exist only so control traffic can
    advance a cursor that belongs to no book. Both come from
    `collector.adapters.build_router`, which is the one place allowed to know
    which venue this is.
    """

    def __init__(
        self,
        books: Mapping[str, BookTransform],
        routes: Mapping[str, BookTransform],
        sequencers: Sequence[VenueAdapter],
        *,
        gates: Mapping[str, Gate] | None = None,
        shard_of: Mapping[str, str] | None = None,
    ) -> None:
        # `books` is one entry per symbol and is what `summary` reports;
        # `routes` may hold the same transform under several tags, because a
        # venue whose snapshot arrives out of band lands it under one of its
        # own. Two dicts rather than one so a book cannot be counted twice.
        self.books = dict(books)
        self._routes = dict(routes)
        self._sequencers = tuple(sequencers)
        # One gate per *connection*, present only where the venue numbers the
        # connection rather than each book. See `_check_connection`.
        self._gates = dict(gates or {})
        self._shard_of = dict(shard_of or {})
        self.unrouted = 0
        self.connection_gaps = 0

    def transform(self, record: CaptureRecord, ctx: RunContext) -> Iterator[LevelRow]:
        stream = record["stream"]
        shard = self._shard_of.get(stream)
        if shard is not None:
            yield from self._check_connection(shard, record, ctx)
        if stream.startswith(CONTROL_STREAM):
            # Connection-wide: no book, but it may occupy a sequence number.
            payload = payload_of(record)
            for sequencer in self._sequencers:
                sequencer.advance(payload)
            return
        book = self._routes.get(stream)
        if book is None:
            # A tag nobody subscribed to. Counted rather than dropped silently
            # or raised on: the capture is still the record of what arrived,
            # and a run that quietly discarded records would be lying about it.
            self.unrouted += 1
            return
        yield from book.transform(record, ctx)

    def _check_connection(
        self, shard: str, record: CaptureRecord, ctx: RunContext
    ) -> Iterator[LevelRow]:
        """Continuity of one *connection*, judged on every record it carried.

        This cannot live in a `BookTransform`, and that is the whole reason the
        router owns it. A transform only sees its own symbol's records, so on a
        venue that numbers the connection it cannot tell a lost message from a
        neighbour simply having spoken. Worse, a book that is *buffering* never
        calls `accept`, so a shared cursor would stall behind it and every
        later message would look like a gap — measured on a live five-product
        run: every book untrusted at exit.

        **Per shard, not per run.** Two connections to the same venue have
        entirely independent sequence spaces, so one gate across both reads as
        a break on nearly every record — also measured, on the same run split
        across three shards. A book record names its shard through its symbol;
        an ack names it through `control_stream`.

        On a break every book *on that connection* goes untrusted together,
        because the sequence cannot say which product the missing message
        described. That blast radius is real and is what `NOTES.md` sizes
        Coinbase's shards against.
        """
        gate = self._gates[shard]
        first, final = gate.adapter.sequence_ids(payload_of(record))
        if gate.prev is not None and not gate.adapter.chains(gate.prev, first):
            self.connection_gaps += 1
            for book in gate.books:
                book.adapter.sequence_broke(first)
            for book in gate.books:
                yield from book.mark_gapped(ctx, first)
        gate.prev = final

    def summary(self) -> dict[str, Any]:
        """The shard's totals, plus the per-symbol detail that explains them.

        `BookTransform.summary()` is flat kwargs for one log line, which stops
        working at a shard's worth of books. Totals lead because that is what a
        run is judged on; `books` keeps every symbol's own numbers so a single
        sick one is still findable.
        """
        books = {tag: book.summary() for tag, book in self.books.items()}

        def total(name: str) -> int:
            return sum(cast(int, s[name]) for s in books.values())

        summary: dict[str, Any] = {
            name: total(name)
            for name in (
                "frames",
                "rows",
                "gaps",
                "bootstraps",
                "crossed_books",
                "untrusted_frames",
                # `oracle_comparisons` is the denominator the rest of them are
                # only readable against, so it totals alongside them.
                "oracle_comparisons",
                "oracle_clean",
                "oracle_unaligned",
                "oracle_levels_compared",
                "oracle_levels_broken",
            )
        }
        summary["symbols"] = len(self.books)
        summary["live_at_exit"] = sum(1 for s in books.values() if s["live_at_exit"])
        summary["unrouted"] = self.unrouted
        summary["books"] = books
        return summary
