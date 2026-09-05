"""``BinanceDepthSource`` — one symbol, full-depth diffs, for a bounded run.

Bounded is the point. ``WorkerApp`` runs a single pass and calls
``Sink.write()`` exactly once, so a source that returns after a fixed duration
fits the existing SDK contract *untouched* — Phase 0 needs no change to
``data-pipeline-core`` at all. The ``ServiceApp`` that a real always-on
collector demands is Phase 4's problem, and deferring it is what keeps this
phase small enough to de-risk the architecture rather than build it.

Rows are yielded lazily: the run loop hands the iterable straight to the sink,
so memory stays flat and only the book itself is resident.

Two things here that Phase 5 replaces rather than extends. The socket's read
buffer is whatever ``websockets`` provides (a small internal queue), not the
explicit bounded ring with drop-and-emit-a-gap semantics that the backpressure
design calls for. And a slow sink can therefore still reach back to the socket,
which is exactly the coupling that design exists to break. At one symbol for
sixty seconds neither bites; at the real symbol count both do.
"""

from __future__ import annotations

import json
import time
from collections.abc import Generator, Iterator
from contextlib import ExitStack
from typing import IO

from data_pipeline_core import RunContext
from websockets.sync.client import ClientConnection, connect

from collector.adapters import binance
from collector.adapters.binance import (
    BinanceDepthAdapter,
    DepthEvent,
    Level,
    Snapshot,
    SpliceOutcome,
)
from collector.book import Book
from collector.model import LevelRow, Side

# A refetch that keeps landing behind the buffer means something is wrong with
# the venue or the clock, not with our patience. Fail the run rather than spin.
_MAX_SNAPSHOT_REFETCHES = 5

# How often the book invariant (best bid < best ask) is checked. Both reads are
# O(n) over a dict, so checking every frame would show up in the very number
# this run exists to commit.
_SANITY_EVERY = 100

# Cap on a single blocking recv, so the duration and SIGTERM are both honoured
# promptly even on a silent socket.
_RECV_SLICE_S = 1.0

# The library default is 10s, which a venue that has nothing more to say spends
# in full — a dead tail longer than a short run.
_CLOSE_TIMEOUT_S = 2.0


class BinanceDepthSource:
    """Bootstrap a book over the splice, then stream diffs until time is up."""

    name = "binance-depth"

    def __init__(
        self,
        *,
        symbol: str,
        duration_s: float,
        ws_url: str,
        rest_url: str,
        snapshot_limit: int,
        depth_interval_ms: int,
        raw_frames_path: str | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self._duration_s = duration_s
        self._ws_url = ws_url.rstrip("/")
        self._rest_url = rest_url
        self._snapshot_limit = snapshot_limit
        self._depth_interval_ms = depth_interval_ms
        self._raw_frames_path = raw_frames_path

        self._adapter = BinanceDepthAdapter()
        self._book = Book()
        self._apply_ns: list[int] = []
        self._frames = 0
        self._rows = 0
        self._gaps = 0
        self._bootstraps = 0
        self._crossed = 0

    def fetch(self, ctx: RunContext) -> Iterator[LevelRow]:
        stream = binance.stream_name(self.symbol, self._depth_interval_ms)
        with ExitStack() as stack:
            raw = (
                stack.enter_context(open(self._raw_frames_path, "w"))
                if self._raw_frames_path
                else None
            )
            # Socket first, snapshot second. The other order loses every diff
            # that lands during the fetch and starts the book silently corrupt.
            ws = stack.enter_context(
                connect(f"{self._ws_url}/{stream}", close_timeout=_CLOSE_TIMEOUT_S)
            )
            ctx.logger.info("connected", stream=stream, deadline_s=self._duration_s)
            # Clock starts after the handshake, so the measured window is the
            # duration that was asked for rather than that minus a connect.
            started = time.monotonic()
            deadline = started + self._duration_s
            while not self._expired(ctx, deadline):
                if not (yield from self._bootstrap(ws, ctx, raw, deadline)):
                    break
                yield from self._stream(ws, ctx, raw, deadline)
            self._report(ctx, time.monotonic() - started)

    # --- bootstrap ---------------------------------------------------------

    def _bootstrap(
        self,
        ws: ClientConnection,
        ctx: RunContext,
        raw: IO[str] | None,
        deadline: float,
    ) -> Generator[LevelRow, None, bool]:
        """Buffer, snapshot, splice. Returns False if the run ended first.

        The two failure branches of the splice get genuinely different
        treatment here — that asymmetry is the point of the whole routine.
        """
        buffered: list[DepthEvent] = []
        snapshot = self._fetch_snapshot(ctx)
        refetches = 0

        while True:
            result = binance.splice(buffered, snapshot.last_update_id)
            if result.outcome is SpliceOutcome.READY:
                break
            if result.outcome is SpliceOutcome.SNAPSHOT_TOO_OLD:
                refetches += 1
                if refetches > _MAX_SNAPSHOT_REFETCHES:
                    raise RuntimeError(
                        f"snapshot still behind the buffer after {refetches} refetches"
                    )
                ctx.logger.warning(
                    "snapshot too old, refetching",
                    last_update_id=snapshot.last_update_id,
                    buffer_first_id=buffered[0].first_id,
                    attempt=refetches,
                )
                snapshot = self._fetch_snapshot(ctx)
                continue
            # BUFFER_BEHIND: the snapshot is fine and simply has not been
            # reached yet. Refetching here is what makes this loop spin.
            event = self._recv(ws, ctx, raw, deadline)
            if event is None:
                return False
            buffered.append(event)

        self._bootstraps += 1
        self._book.clear()
        self._adapter.bootstrapped()
        ctx.logger.info(
            "bootstrapped",
            last_update_id=snapshot.last_update_id,
            buffered=len(buffered),
            discarded=result.start,
        )
        yield from self._snapshot_rows(snapshot)
        for event in buffered[result.start :]:
            yield from self._apply(event)
        return True

    def _fetch_snapshot(self, ctx: RunContext) -> Snapshot:
        """Through ``ctx.http``, so it inherits retry, backoff and the breaker.

        The websocket gets none of that, and that asymmetry is the argument for
        the connection supervisor in Phase 3.
        """
        response = ctx.http.get(
            self._rest_url,
            params={"symbol": self.symbol, "limit": self._snapshot_limit},
        )
        if response.status_code != 200:
            raise RuntimeError(f"depth snapshot returned {response.status_code}")
        return binance.parse_snapshot(
            response.json(),
            receive_ts=time.time_ns(),
            monotonic_ts=time.monotonic_ns(),
        )

    # --- steady state ------------------------------------------------------

    def _stream(
        self,
        ws: ClientConnection,
        ctx: RunContext,
        raw: IO[str] | None,
        deadline: float,
    ) -> Iterator[LevelRow]:
        """Apply diffs until the clock runs out or the sequence breaks."""
        while True:
            event = self._recv(ws, ctx, raw, deadline)
            if event is None:
                return
            if self._adapter.gap_detected(event):
                # Mark the book untrusted *before* repairing it: convergence is
                # only measurable if the untrusted interval has both edges in
                # the data. The caller re-bootstraps, which emits the far edge.
                self._gaps += 1
                ctx.logger.warning("gap detected", first_id=event.first_id)
                yield self._gap_row(event)
                return
            yield from self._apply(event)

    def _recv(
        self,
        ws: ClientConnection,
        ctx: RunContext,
        raw: IO[str] | None,
        deadline: float,
    ) -> DepthEvent | None:
        """Next frame, or None once the duration is up or a SIGTERM arrived."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or ctx.should_stop():
                return None
            try:
                message = ws.recv(timeout=min(remaining, _RECV_SLICE_S))
            except TimeoutError:
                continue
            receive_ts = time.time_ns()
            monotonic_ts = time.monotonic_ns()
            text = message if isinstance(message, str) else message.decode()
            if raw is not None:
                raw.write(text + "\n")
            self._frames += 1
            # stdlib json on purpose: it is the baseline bench/decode.py
            # measures the alternatives against.
            return binance.parse_event(
                json.loads(text), receive_ts=receive_ts, monotonic_ts=monotonic_ts
            )

    def _apply(self, event: DepthEvent) -> Iterator[LevelRow]:
        rows = self._level_rows(event)
        self._adapter.accept(event)
        # Measured here rather than after the yields: the in-process path is
        # decode → book apply → row built, and anything past this point is the
        # sink's time, not ours. Receive-to-disk needs the batching sink and is
        # Phase 7's number.
        self._apply_ns.append(time.monotonic_ns() - event.monotonic_ts)
        self._rows += len(rows)
        if self._frames % _SANITY_EVERY == 0:
            bid, ask = self._book.best_bid_ask()
            if bid is not None and ask is not None and bid >= ask:
                self._crossed += 1
        yield from rows

    # --- rows --------------------------------------------------------------

    def _level_rows(self, event: DepthEvent) -> list[LevelRow]:
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
                self._rows += 1
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
        self._rows += 1
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

    def _expired(self, ctx: RunContext, deadline: float) -> bool:
        return ctx.should_stop() or time.monotonic() >= deadline

    def _report(self, ctx: RunContext, elapsed_s: float) -> None:
        """The two Phase 0 numbers, logged so they can be committed to NOTES.

        Throughput and *in-process* p99 — deliberately not receive-to-disk. With
        one ``sink.write()`` at the end of a bounded run every row lands at
        once, so that number would describe the run's shape rather than the
        pipeline's. It waits for the batching sink.
        """
        latencies = sorted(self._apply_ns)
        bid, ask = self._book.best_bid_ask()
        ctx.logger.info(
            "phase 0 baseline",
            symbol=self.symbol,
            elapsed_s=round(elapsed_s, 3),
            frames=self._frames,
            frames_per_s=round(self._frames / elapsed_s, 1) if elapsed_s else 0.0,
            rows=self._rows,
            rows_per_s=round(self._rows / elapsed_s, 1) if elapsed_s else 0.0,
            apply_p50_us=_percentile_us(latencies, 0.50),
            apply_p90_us=_percentile_us(latencies, 0.90),
            apply_p99_us=_percentile_us(latencies, 0.99),
            bootstraps=self._bootstraps,
            gaps=self._gaps,
            crossed_books=self._crossed,
            bid_levels=len(self._book.bids),
            ask_levels=len(self._book.asks),
            best_bid_ticks=bid,
            best_ask_ticks=ask,
        )


def _percentile_us(sorted_ns: list[int], q: float) -> float | None:
    """Nearest-rank percentile of a sorted ns list, reported in microseconds."""
    if not sorted_ns:
        return None
    index = min(len(sorted_ns) - 1, int(q * len(sorted_ns)))
    return round(sorted_ns[index] / 1000, 1)
