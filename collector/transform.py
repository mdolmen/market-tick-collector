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
from collector.metrics import (
    checksum_breaks,
    clock_difference,
    oracle_comparisons,
    oracle_level_breaks,
)
from collector.model import LevelRow, Side
from collector.oracle import BookOracle

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
        self._oracle = BookOracle()
        self._buffered: list[Update] = []
        self._snapshot: Snapshot | None = None
        self._live = False

        self.frames = 0
        self.rows = 0
        self.gaps = 0
        self.bootstraps = 0
        self.crossed = 0
        # Frames the venue's own integrity token rejected, kept apart from
        # `gaps` — see `transform`. Zero on a venue that publishes no token.
        self.checksum_breaks = 0
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
        consistent = self._adapter.observe(payload_of(record))
        if record["kind"] == "frame" and self._live and not consistent:
            # Counted here and nowhere else. The break reaches the book through
            # `gap_detected`, which lands it in `gaps` alongside sequence
            # breaks — and the two are different evidence about different
            # things, so `TODO.md` § *Phase 6* asks for them as two numbers.
            # Read before the frame is handled, because handling it is what
            # ends the live interval this break was found in.
            self.checksum_breaks += 1
            checksum_breaks(ctx.metrics.registry).labels(
                venue=self._adapter.venue
            ).inc()
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

        **Which snapshots may be ignored is the venue's answer, not ours.**
        The paragraph above holds only where a snapshot is a read taken
        alongside a diff stream that never stopped. Where the only way to get
        one is to unsubscribe and subscribe, that read *interrupted* the
        stream, and every level deleted during the gap is absent from the new
        snapshot without ever arriving as a delete. Ignoring such a snapshot
        leaves those levels in the book forever — no gap, no sequence break,
        no failing checksum while the staleness sits below the top ten. It was
        found in Phase 2.5 as 43 crossed books in a 60-second Kraken replay
        whose every checksum passed, and it applies to Coinbase identically;
        it had simply never been exercised, because the snapshot interval
        defaults to 300s and no test run lasted that long.
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
            if not self._adapter.snapshot_supersedes():
                # The snapshot this book did not need is the one the oracle
                # wants: an independent read of a stream that never stopped.
                # Where it *does* supersede there is nothing to compare, and
                # the paragraph above is the reason — that read interrupted
                # the diff stream, so a diff against it measures the
                # interruption rather than the reconstruction.
                self._compare(ctx)
                return
            # Re-bootstrap from it. The buffer goes because it describes a
            # stream this snapshot has replaced, not one it continues.
            self._live = False
            self._buffered = []
            self._oracle.reset()
        yield from self._try_bootstrap(ctx)

    def _on_frame(self, record: CaptureRecord, ctx: RunContext) -> Iterator[LevelRow]:
        event = self._adapter.parse_frame(
            payload_of(record),
            receive_ts=record["receive_ts"],
            monotonic_ts=record["monotonic_ts"],
        )
        self.frames += 1
        if event.exchange_ts is not None:
            difference = event.receive_ts - event.exchange_ts
            self._skews.append(difference)
            # Exported as well as summarised: `skew_ms` is one distribution per
            # run and a service has no end of run to report at.
            clock_difference(ctx.metrics.registry).labels(
                venue=self._adapter.venue
            ).observe(difference / 1e9)

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
            self._oracle.reset()
            yield self._gap_row(event)
            return

        yield from self._apply(event)

    def mark_gapped(self, ctx: RunContext, seq: int) -> Iterator[LevelRow]:
        """Something outside this book decided the stream lost a message.

        The connection did, on a venue that numbers the connection rather than
        each book: one lost message there could have been about any symbol on
        the socket, so every book on it goes untrusted together. `BookRouter`
        is what calls this, and `NOTES.md` § *Connection supervision* is where
        that blast radius is the reason shards are sized.

        Everything after the decision is the same as a gap this book found
        itself — untrusted first, then repair, with both edges of the interval
        in the data so convergence stays measurable.
        """
        if not self._live:
            return
        self.gaps += 1
        ctx.logger.warning("gap on the connection", symbol=self.symbol, seq=seq)
        self._live = False
        self._buffered = []
        self._oracle.reset()
        yield self._gap_row_at(seq)

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
        self._oracle.reset()
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

    def _compare(self, ctx: RunContext) -> None:
        """Hand the ignored snapshot to the oracle and export its verdict.

        Yields nothing, which is the point: this changes no book state and
        lands no row. It is a measurement taken beside the pipeline rather than
        a step in it, and a run with the oracle removed would emit byte-identical
        output.
        """
        assert self._snapshot is not None  # only reached with one in hand
        broken = self._oracle.compare(self._snapshot, self._book)
        registry = ctx.metrics.registry
        result = "unaligned" if broken is None else "broken" if broken else "clean"
        oracle_comparisons(registry).labels(
            venue=self._adapter.venue, result=result
        ).inc()
        if not broken:
            return
        oracle_level_breaks(registry).labels(venue=self._adapter.venue).inc(broken)
        ctx.logger.warning(
            "book diverged from the venue's snapshot",
            symbol=self.symbol,
            snapshot_seq=self._snapshot.final_seq,
            levels=broken,
        )

    def _apply(self, event: Update) -> Iterator[LevelRow]:
        rows = self.level_rows(event)
        self._adapter.accept(event)
        # Held for the next oracle comparison: a snapshot arrives describing a
        # position the book has already passed, and these are what close the
        # distance. See `collector.oracle` on why it rolls this way.
        self._oracle.applied(event)
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
        return self._gap_row_at(
            event.first_seq,
            exchange_ts=event.exchange_ts,
            receive_ts=event.receive_ts,
            monotonic_ts=event.monotonic_ts,
        )

    def _gap_row_at(
        self,
        seq: int,
        *,
        exchange_ts: int | None = None,
        receive_ts: int = 0,
        monotonic_ts: int = 0,
    ) -> LevelRow:
        """The same row, for a gap decided without an `Update` to hand.

        A connection-level break is found by the router while inspecting a raw
        payload, so there is no parsed event to take the clocks from.
        """
        self.rows += 1
        return LevelRow(
            venue=self._adapter.venue,
            symbol=self.symbol,
            seq=seq,
            exchange_ts=exchange_ts,
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            action="gap",
            side=None,
            price_str=None,
            price_ticks=None,
            size_str=None,
            size_lots=None,
        )

    # --- the numbers -------------------------------------------------------

    @property
    def adapter(self) -> VenueAdapter:
        """The sequencer this book judges itself by.

        Exposed for `BookRouter`, which has to reach it on a connection-level
        break: the break is found while inspecting a raw payload, so there is
        no `Update` to carry the news in.
        """
        return self._adapter

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
            "checksum_breaks": self.checksum_breaks,
            **self._oracle.summary(),
            "bid_levels": len(self._book.bids),
            "ask_levels": len(self._book.asks),
            "best_bid_ticks": bid,
            "best_ask_ticks": ask,
        }
