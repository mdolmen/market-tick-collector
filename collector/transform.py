"""``BinanceBookTransform`` — capture records in, normalized level rows out.

This is the whole of the book: the splice, the state machine, the adapter and
the row construction, lifted out of ``source.py`` so that **the live path and
the replay path run the same object**. Phase 0 had them fused, which meant a
replay harness would have been a second implementation of the thing it was
supposed to be testing.

It does no I/O, and that is the load-bearing property. A snapshot arrives as a
record on the stream rather than as a ``ctx.http`` call, so a replay
bootstraps cold from disk with no network at all. The cost is that this class
cannot ask for a snapshot when it wants one — it waits for the capture to
contain the next one, and ``BinanceFrameSource`` is what guarantees the capture
does. See that module on why both sides consult the chain rule.

The state machine of ``NOTES.md`` § *The state machine is the artifact*, with
the capture record kinds that drive each edge:

    BUFFERING --(snapshot record)--> SYNCING --(splice READY)--> LIVE
       ^                                                          |
       +---------------(gap: chain broken on a frame)-------------+

No latency accounting lives here. In a replay ``monotonic_ts`` is the *capture*
clock, so ``now - event.monotonic_ts`` would measure the age of the recording;
the driver that owns the wall clock times its own loop instead (``bench/replay.py``).
"""

from __future__ import annotations

from collections.abc import Iterator

from data_pipeline_core import RunContext

from collector.adapters import binance
from collector.adapters.binance import (
    BinanceDepthAdapter,
    DepthEvent,
    Level,
    Snapshot,
    SpliceOutcome,
)
from collector.book import Book
from collector.capture import CaptureRecord, payload_of
from collector.model import LevelRow, Side

# How often the book invariant (best bid < best ask) is checked. Both reads are
# O(n) over a dict, so checking every frame would show up in the very numbers
# this transform exists to produce.
_SANITY_EVERY = 100


class BinanceBookTransform:
    """Rebuild one symbol's book from captured frames and emit level rows."""

    def __init__(self, *, symbol: str) -> None:
        self.symbol = symbol.upper()

        self._adapter = BinanceDepthAdapter()
        self._book = Book()
        self._buffered: list[DepthEvent] = []
        self._snapshot: Snapshot | None = None
        self._live = False

        self.frames = 0
        self.rows = 0
        self.gaps = 0
        self.bootstraps = 0
        self.crossed = 0

    # --- the Transform contract --------------------------------------------

    def transform(
        self, record: CaptureRecord, ctx: RunContext
    ) -> Iterator[LevelRow]:
        """One capture record in, zero or more level rows out.

        Zero is the common case while buffering: nothing may be applied until
        the splice has located the snapshot inside the stream, and then a whole
        bootstrap's worth of rows leaves at once.
        """
        if record["kind"] == "snapshot":
            yield from self._on_snapshot(record, ctx)
        else:
            yield from self._on_frame(record, ctx)

    # --- edges -------------------------------------------------------------

    def _on_snapshot(
        self, record: CaptureRecord, ctx: RunContext
    ) -> Iterator[LevelRow]:
        """A snapshot record always (re)starts a bootstrap.

        Uniform rather than conditional on the current state: a fresh snapshot
        is authoritative, so splicing against it is correct whether it arrived
        because the run just started, because the last one was too old, or
        because the socket skipped. The buffer is deliberately *not* cleared —
        ``splice`` discards whatever is entirely in the past by itself, and a
        frame straddling the new snapshot is exactly what it is looking for.
        """
        self._snapshot = binance.parse_snapshot(
            payload_of(record),
            receive_ts=record["receive_ts"],
            monotonic_ts=record["monotonic_ts"],
        )
        self._live = False
        yield from self._try_splice(ctx)

    def _on_frame(self, record: CaptureRecord, ctx: RunContext) -> Iterator[LevelRow]:
        event = binance.parse_event(
            payload_of(record),
            receive_ts=record["receive_ts"],
            monotonic_ts=record["monotonic_ts"],
        )
        self.frames += 1

        if not self._live:
            self._buffered.append(event)
            yield from self._try_splice(ctx)
            return

        if self._adapter.gap_detected(event):
            # Mark the book untrusted *before* repairing it: convergence is
            # only measurable if the untrusted interval has both edges in the
            # data. The next snapshot record closes it.
            self.gaps += 1
            ctx.logger.warning("gap detected", first_id=event.first_id)
            self._live = False
            self._buffered = [event]
            yield self._gap_row(event)
            return

        yield from self._apply(event)

    def _try_splice(self, ctx: RunContext) -> Iterator[LevelRow]:
        """Locate the snapshot inside the buffer, and bootstrap if it is there.

        The two failure branches get genuinely different treatment, which is
        the point of the whole routine — but here neither one can act, because
        a transform cannot fetch. ``SNAPSHOT_TOO_OLD`` waits for the next
        snapshot record (the source fetches it on the same condition) and
        ``BUFFER_BEHIND`` waits for more frames.
        """
        if self._snapshot is None:
            return
        result = binance.splice(self._buffered, self._snapshot.last_update_id)
        if result.outcome is SpliceOutcome.SNAPSHOT_TOO_OLD:
            ctx.logger.warning(
                "snapshot too old, waiting for the next one",
                last_update_id=self._snapshot.last_update_id,
                buffer_first_id=self._buffered[0].first_id,
            )
            return
        if result.outcome is not SpliceOutcome.READY:
            return

        self.bootstraps += 1
        self._book.clear()
        self._adapter.bootstrapped()
        ctx.logger.info(
            "bootstrapped",
            last_update_id=self._snapshot.last_update_id,
            buffered=len(self._buffered),
            discarded=result.start,
        )
        yield from self._snapshot_rows(self._snapshot)
        applicable = self._buffered[result.start :]
        self._buffered = []
        self._live = True
        for event in applicable:
            yield from self._apply(event)

    def _apply(self, event: DepthEvent) -> Iterator[LevelRow]:
        rows = self.level_rows(event)
        self._adapter.accept(event)
        self.rows += len(rows)
        if self.frames % _SANITY_EVERY == 0:
            bid, ask = self._book.best_bid_ask()
            if bid is not None and ask is not None and bid >= ask:
                self.crossed += 1
        yield from rows

    # --- rows --------------------------------------------------------------

    def level_rows(self, event: DepthEvent) -> list[LevelRow]:
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
                        venue=binance.VENUE,
                        symbol=self.symbol,
                        seq=event.final_id,
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
                    venue=binance.VENUE,
                    symbol=self.symbol,
                    seq=snapshot.last_update_id,
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

    def _gap_row(self, event: DepthEvent) -> LevelRow:
        """A control record: it describes no price level, so those fields are null."""
        self.rows += 1
        return LevelRow(
            venue=binance.VENUE,
            symbol=self.symbol,
            seq=event.first_id,
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

    def summary(self) -> dict[str, object]:
        """Counters and the book at exit. Throughput belongs to the driver."""
        bid, ask = self._book.best_bid_ask()
        return {
            "symbol": self.symbol,
            "frames": self.frames,
            "rows": self.rows,
            "bootstraps": self.bootstraps,
            "gaps": self.gaps,
            "crossed_books": self.crossed,
            "bid_levels": len(self._book.bids),
            "ask_levels": len(self._book.asks),
            "best_bid_ticks": bid,
            "best_ask_ticks": ask,
        }
