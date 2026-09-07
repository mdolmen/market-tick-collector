"""``BookTransform`` — capture records in, normalized level rows out.

This is the whole of the book: the bootstrap, the state machine and the row
construction, lifted out of ``source.py`` so that **the live path and the
replay path run the same object**. Phase 0 had them fused, which meant a replay
harness would have been a second implementation of the thing it was supposed to
be testing.

**It has no idea which venue it is reconstructing.** Everything venue-shaped
reaches it through a ``VenueAdapter``, and the only trace of a venue in the
rows it emits is the ``venue`` label the adapter supplies. That is the
normalization boundary of ``CLAUDE.md``, and ``tests/test_boundary.py`` is what
keeps it from eroding back.

It does no I/O either, and that is the load-bearing property. A snapshot
arrives as a record on the stream rather than as a ``ctx.http`` call, so a
replay bootstraps cold from disk with no network at all. The cost is that this
class cannot ask for a snapshot when it wants one — it waits for the capture to
contain the next one, and ``FrameSource`` is what guarantees the capture does.
See that module on why both sides consult the chain rule.

The state machine of ``NOTES.md`` § *The state machine is the artifact*, with
the capture record kinds that drive each edge:

    BUFFERING --(snapshot record)--> SYNCING --(bootstrap READY)--> LIVE
       ^                                                             |
       +----------------(gap: chain broken on a frame)---------------+

No latency accounting lives here. In a replay ``monotonic_ts`` is the *capture*
clock, so ``now - event.monotonic_ts`` would measure the age of the recording;
the driver that owns the wall clock times its own loop instead (``bench/replay.py``).
"""

from __future__ import annotations

from collections.abc import Iterator

from data_pipeline_core import RunContext

from collector.adapters.base import (
    BootstrapOutcome,
    Level,
    Snapshot,
    Update,
    VenueAdapter,
)
from collector.book import Book
from collector.capture import CaptureRecord, payload_of
from collector.model import LevelRow, Side

# How often the book invariant (best bid < best ask) is checked. Both reads are
# O(n) over a dict, so checking every frame would show up in the very numbers
# this transform exists to produce.
_SANITY_EVERY = 100


class BookTransform:
    """Rebuild one symbol's book from captured frames and emit level rows."""

    def __init__(self, *, symbol: str, adapter: VenueAdapter) -> None:
        self.symbol = symbol.upper()

        self._adapter = adapter
        self._book = Book()
        self._buffered: list[Update] = []
        self._snapshot: Snapshot | None = None
        self._live = False

        self.frames = 0
        self.rows = 0
        self.gaps = 0
        self.bootstraps = 0
        self.crossed = 0
        # Frames that arrived while the book was untrusted — buffered, not
        # applied. The untrusted *interval* is what convergence is measured
        # over (``NOTES.md`` § *The state machine is the artifact*), and a
        # count of gaps says nothing about how long they lasted.
        self.untrusted_frames = 0
        # One `receive_ts - exchange_ts` per frame that carried a venue clock.
        # A plain list because every run in this phase is bounded; a
        # long-running collector needs a histogram at the metric surface
        # instead, which is Phase 4's deliberate §8 change. See `summary`.
        self._skews: list[int] = []

    # --- the Transform contract --------------------------------------------

    def transform(self, record: CaptureRecord, ctx: RunContext) -> Iterator[LevelRow]:
        """One capture record in, zero or more level rows out.

        Zero is the common case while buffering: nothing may be applied until
        the bootstrap has located the snapshot inside the stream, and then a
        whole bootstrap's worth of rows leaves at once. A control record always
        yields zero — it carries no book state — but it is not free, because a
        venue may number it in the same sequence as its book messages.
        """
        if record["kind"] == "control":
            self._adapter.advance(payload_of(record))
            return
        # Every book message, in arrival order, before anything is decided
        # about it. On a venue that publishes an integrity token this is where
        # it is checked, and on a venue with no sequence it is the only thing
        # that can tell the book it has gone wrong — so it must run whether the
        # book is live, buffering or already untrusted.
        self._adapter.observe(payload_of(record))
        if record["kind"] == "snapshot":
            yield from self._on_snapshot(record, ctx)
        else:
            yield from self._on_frame(record, ctx)

    # --- edges -------------------------------------------------------------

    def _on_snapshot(
        self, record: CaptureRecord, ctx: RunContext
    ) -> Iterator[LevelRow]:
        """A snapshot repairs an untrusted book, and is ignored by a trusted one.

        The source lands snapshots periodically as well as on demand, so most
        of them arrive at a book that is perfectly healthy. Rebuilding from
        those would cost a full snapshot's worth of rows — 10⁴ at the real
        depth limit — to arrive exactly where the book already is, and would
        reset the untrusted-interval bookkeeping that convergence is measured
        with. They are Oracle 1's data and a repair held in reserve.

        When the book *is* untrusted the buffer is deliberately not cleared:
        ``bootstrap`` discards whatever is entirely in the past by itself, and a
        frame straddling the new snapshot is exactly what it is looking for.
        """
        payload = payload_of(record)
        self._snapshot = self._adapter.parse_snapshot(
            payload,
            receive_ts=record["receive_ts"],
            monotonic_ts=record["monotonic_ts"],
        )
        # Even an ignored snapshot moves the cursor where the venue numbered
        # it, which an in-band venue does. Skipping this makes the next frame
        # look like a skip, and the "repair" is another snapshot that does the
        # same thing again.
        self._adapter.advance(payload)
        if self._live:
            return
        yield from self._try_bootstrap(ctx)

    def _on_frame(self, record: CaptureRecord, ctx: RunContext) -> Iterator[LevelRow]:
        event = self._adapter.parse_frame(
            payload_of(record),
            receive_ts=record["receive_ts"],
            monotonic_ts=record["monotonic_ts"],
        )
        self.frames += 1
        if event.exchange_ts is not None:
            self._skews.append(event.receive_ts - event.exchange_ts)

        if not self._live:
            self.untrusted_frames += 1
            self._buffered.append(event)
            yield from self._try_bootstrap(ctx)
            return

        if self._adapter.gap_detected(event):
            # Mark the book untrusted *before* repairing it: convergence is
            # only measurable if the untrusted interval has both edges in the
            # data. The next snapshot record closes it.
            self.gaps += 1
            ctx.logger.warning("gap detected", first_seq=event.first_seq)
            self._live = False
            self._buffered = [event]
            yield self._gap_row(event)
            return

        yield from self._apply(event)

    def _try_bootstrap(self, ctx: RunContext) -> Iterator[LevelRow]:
        """Locate the snapshot inside the buffer, and bootstrap if it is there.

        The two failure branches get genuinely different treatment, which is
        the point of the whole routine — but here neither one can act, because
        a transform cannot fetch. ``SNAPSHOT_TOO_OLD`` waits for the next
        snapshot record (the source fetches it on the same condition) and
        ``BUFFER_BEHIND`` waits for more frames.
        """
        if self._snapshot is None:
            return
        result = self._adapter.bootstrap(self._buffered, self._snapshot)
        if result.outcome is BootstrapOutcome.SNAPSHOT_TOO_OLD:
            ctx.logger.warning(
                "snapshot too old, waiting for the next one",
                snapshot_seq=self._snapshot.final_seq,
                buffer_first_seq=self._buffered[0].first_seq,
            )
            return
        if result.outcome is not BootstrapOutcome.READY:
            return

        self.bootstraps += 1
        self._book.clear()
        self._adapter.bootstrapped(self._snapshot)
        ctx.logger.info(
            "bootstrapped",
            snapshot_seq=self._snapshot.final_seq,
            buffered=len(self._buffered),
            discarded=result.start,
        )
        yield from self._snapshot_rows(self._snapshot)
        applicable = self._buffered[result.start :]
        self._buffered = []
        self._live = True
        for event in applicable:
            yield from self._apply(event)

    def _apply(self, event: Update) -> Iterator[LevelRow]:
        rows = self.level_rows(event)
        self._adapter.accept(event)
        self.rows += len(rows)
        if self.frames % _SANITY_EVERY == 0:
            bid, ask = self._book.best_bid_ask()
            if bid is not None and ask is not None and bid >= ask:
                self.crossed += 1
        yield from rows

    # --- rows --------------------------------------------------------------

    def level_rows(self, event: Update) -> list[LevelRow]:
        """One frame's worth of rows, and the book write that goes with them.

        Public because it is the per-frame normalization cost on its own, and
        ``bench/cadence.py`` measures exactly that — over the collector's real
        path rather than a copy of it.
        """
        sides: tuple[tuple[Side, tuple[Level, ...]], ...] = (
            ("bid", event.bids),
            ("ask", event.asks),
        )
        rows: list[LevelRow] = []
        for side, levels in sides:
            for level in levels:
                self._book.apply(side, level.price_ticks, level.size_lots)
                rows.append(
                    LevelRow(
                        venue=self._adapter.venue,
                        symbol=self.symbol,
                        seq=event.final_seq,
                        exchange_ts=event.exchange_ts,
                        receive_ts=event.receive_ts,
                        monotonic_ts=event.monotonic_ts,
                        action="delete" if level.size_lots == 0 else "set",
                        side=side,
                        price_str=level.price_str,
                        price_ticks=level.price_ticks,
                        size_str=level.size_str,
                        size_lots=level.size_lots,
                    )
                )
        return rows

    def _snapshot_rows(self, snapshot: Snapshot) -> Iterator[LevelRow]:
        sides: tuple[tuple[Side, tuple[Level, ...]], ...] = (
            ("bid", snapshot.bids),
            ("ask", snapshot.asks),
        )
        for side, levels in sides:
            for level in levels:
                self._book.apply(side, level.price_ticks, level.size_lots)
                self.rows += 1
                yield LevelRow(
                    venue=self._adapter.venue,
                    symbol=self.symbol,
                    seq=snapshot.final_seq,
                    # Binance's REST depth response carries no clock of its own.
                    exchange_ts=None,
                    receive_ts=snapshot.receive_ts,
                    monotonic_ts=snapshot.monotonic_ts,
                    action="snapshot",
                    side=side,
                    price_str=level.price_str,
                    price_ticks=level.price_ticks,
                    size_str=level.size_str,
                    size_lots=level.size_lots,
                )

    def _gap_row(self, event: Update) -> LevelRow:
        """A control record: it describes no price level, so those fields are null."""
        self.rows += 1
        return LevelRow(
            venue=self._adapter.venue,
            symbol=self.symbol,
            seq=event.first_seq,
            exchange_ts=event.exchange_ts,
            receive_ts=event.receive_ts,
            monotonic_ts=event.monotonic_ts,
            action="gap",
            side=None,
            price_str=None,
            price_ticks=None,
            size_str=None,
            size_lots=None,
        )

    # --- the numbers -------------------------------------------------------

    @property
    def book(self) -> Book:
        """The reconstructed book, for a caller that wants to assert on it."""
        return self._book

    @property
    def live(self) -> bool:
        """Whether the book is trusted right now.

        A run that ends untrusted has not converged, whatever its gap count
        says — the last interval simply never closed. Comparing such a book
        against anything measures where it stopped, not whether it recovers.
        """
        return self._live

    def skew_ms(self) -> dict[str, float]:
        """Venue clock to our clock, in milliseconds, per percentile.

        **This is not clock offset.** It is `receive_ts - exchange_ts`, which
        is one-way network delay *plus* the offset between the venue's clock
        and ours, and the two are not separable without a round-trip estimate
        this collector never makes. Reporting it as "clock skew" and leaving it
        there would be a number that looks more precise than it is; it is a
        useful upper bound on delay and a useful drift alarm, and nothing more.

        It also inherits our own clock discipline: `receive_ts` is a wall clock
        and an NTP step lands in this distribution as a spike no venue caused.

        In a replay the values are the *capture's* skew, not the replay's,
        because both clocks are read from the record. That is the intended
        behaviour — it makes the number a property of the session rather than
        of when it happened to be reprocessed.
        """
        if not self._skews:
            return {}
        ordered = sorted(self._skews)

        def at(quantile: float) -> float:
            index = min(len(ordered) - 1, int(quantile * len(ordered)))
            return round(ordered[index] / 1e6, 3)

        return {"p50": at(0.5), "p90": at(0.9), "p99": at(0.99)}

    def summary(self) -> dict[str, object]:
        """Counters and the book at exit. Throughput belongs to the driver."""
        bid, ask = self._book.best_bid_ask()
        return {
            "venue": self._adapter.venue,
            "skew_ms": self.skew_ms(),
            "symbol": self.symbol,
            "frames": self.frames,
            "rows": self.rows,
            "bootstraps": self.bootstraps,
            "gaps": self.gaps,
            "untrusted_frames": self.untrusted_frames,
            "live_at_exit": self._live,
            "crossed_books": self.crossed,
            "bid_levels": len(self._book.bids),
            "ask_levels": len(self._book.asks),
            "best_bid_ticks": bid,
            "best_ask_ticks": ask,
        }
