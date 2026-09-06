"""``BinanceFrameSource`` — the socket and the REST fetch, and nothing else.

An ingest worker in the SDK's sense (``ARCHITECTURE.md`` § *Two archetypes*):
it produces the venue's bytes wrapped in a ``CaptureRecord`` and never builds a
book. Wired to ``raw_landing_sink`` it is a capture run; wired to a curated
sink with ``BinanceBookTransform`` it is the Phase 0 collector, unchanged in
behaviour. The split is what lets a replay drive the *same* transform from
disk instead of from a socket.

Bounded is still the point. ``WorkerApp`` runs a single pass and calls
``Sink.write()`` exactly once, so a source that returns after a fixed duration
fits the existing SDK contract untouched. The ``ServiceApp`` a real always-on
collector demands is Phase 4's problem.

**Why this class watches the sequence at all.** It must not build a book, but
it must know when to fetch a snapshot — because a capture that lacks a snapshot
at every point one is needed cannot be replayed cold, and the transform (which
does no I/O) can never ask for one. So both sides consult
``binance.chains``, and they ask it different questions: this one asks *did the
socket skip, so should I fetch*, the transform asks *is the book still
trustworthy*. One venue rule, one definition, two decisions. The alternative —
a back-channel from transform to source — does not exist in the ``Transform``
protocol and would put I/O back in the transform, which is what breaks replay.

Two things here that Phase 5 replaces rather than extends. The socket's read
buffer is whatever ``websockets`` provides (a small internal queue), not the
explicit bounded ring with drop-and-emit-a-gap semantics that the backpressure
design calls for. And a slow sink can therefore still reach back to the socket,
which is exactly the coupling that design exists to break.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

from data_pipeline_core import RunContext
from websockets.sync.client import ClientConnection, connect

from collector.adapters import binance
from collector.capture import CaptureRecord, Kind

# A refetch that keeps landing behind the buffer means something is wrong with
# the venue or the clock, not with our patience. Fail the run rather than spin.
_MAX_SNAPSHOT_REFETCHES = 5

# Cap on a single blocking recv, so the duration and SIGTERM are both honoured
# promptly even on a silent socket.
_RECV_SLICE_S = 1.0

# The library default is 10s, which a venue that has nothing more to say spends
# in full — a dead tail longer than a short run.
_CLOSE_TIMEOUT_S = 2.0


class BinanceFrameSource:
    """Stream one symbol's diff frames, with a snapshot whenever one is due."""

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
        snapshot_interval_s: float = 0.0,
    ) -> None:
        self.symbol = symbol.upper()
        self._duration_s = duration_s
        self._ws_url = ws_url.rstrip("/")
        self._rest_url = rest_url
        self._snapshot_limit = snapshot_limit
        self._snapshot_interval_s = snapshot_interval_s
        self._stream = binance.stream_name(self.symbol, depth_interval_ms)

        self._seq = 0
        self._frames = 0
        self._snapshots = 0
        # The last received frame's ``u``, and the first ``U`` seen since the
        # most recent snapshot was fetched — the two ids the fetch decision
        # needs, and the only sequence state this class keeps.
        self._prev_final_id: int | None = None
        self._first_id_since_snapshot: int | None = None
        self._last_update_id: int | None = None
        self._skipped = False
        self._refetches = 0
        self._last_snapshot_at = 0.0

    def fetch(self, ctx: RunContext) -> Iterator[CaptureRecord]:
        with connect(
            f"{self._ws_url}/{self._stream}", close_timeout=_CLOSE_TIMEOUT_S
        ) as ws:
            ctx.logger.info(
                "connected", stream=self._stream, deadline_s=self._duration_s
            )
            # Clock starts after the handshake, so the measured window is the
            # duration that was asked for rather than that minus a connect.
            started = time.monotonic()
            deadline = started + self._duration_s
            # Socket first, snapshot second. The other order loses every diff
            # that lands during the fetch and starts the book silently corrupt.
            yield self._snapshot_record(ctx)
            while True:
                frame = self._recv(ws, ctx, deadline)
                if frame is None:
                    break
                yield frame
                if self._snapshot_due(ctx):
                    yield self._snapshot_record(ctx)
            self._report(ctx, time.monotonic() - started)

    # --- receiving ---------------------------------------------------------

    def _recv(
        self, ws: ClientConnection, ctx: RunContext, deadline: float
    ) -> CaptureRecord | None:
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
            self._frames += 1
            # stdlib json on purpose: it is the baseline bench/decode.py
            # measures the alternatives against. Only the two chain ids are
            # read — the record model is the transform's cost, not the
            # capture worker's.
            first_id, final_id = binance.chain_ids(json.loads(text))
            # Judged against the *previous* frame, so it has to happen here,
            # before the cursor moves.
            self._skipped = self._prev_final_id is not None and not binance.chains(
                self._prev_final_id, first_id
            )
            self._prev_final_id = final_id
            if self._first_id_since_snapshot is None:
                self._first_id_since_snapshot = first_id
            return self._record(
                stream=self._stream,
                kind="frame",
                receive_ts=receive_ts,
                monotonic_ts=monotonic_ts,
                payload=text,
            )

    # --- deciding when to snapshot -----------------------------------------

    def _snapshot_due(self, ctx: RunContext) -> bool:
        """True when the capture needs a fresh snapshot to stay replayable.

        Two conditions, and they are the two failure branches of the splice
        seen from the I/O side. A snapshot that predates the first frame since
        it was fetched has already lost updates and no amount of further
        buffering fixes it. A break in the chain means the socket skipped, and
        the book downstream is about to go untrusted with nothing to repair it.
        """
        if self._last_update_id is None or self._first_id_since_snapshot is None:
            return False
        if binance.snapshot_too_old(
            self._first_id_since_snapshot, self._last_update_id
        ):
            self._refetches += 1
            if self._refetches > _MAX_SNAPSHOT_REFETCHES:
                raise RuntimeError(
                    f"snapshot still behind the stream after "
                    f"{self._refetches} refetches"
                )
            ctx.logger.warning(
                "snapshot too old, refetching",
                last_update_id=self._last_update_id,
                buffer_first_id=self._first_id_since_snapshot,
                attempt=self._refetches,
            )
            return True
        # The snapshot caught up, so the refetch budget is for the next stall
        # rather than for the whole run.
        self._refetches = 0
        if self._skipped:
            ctx.logger.warning("socket skipped, fetching a snapshot")
            return True
        return self._interval_elapsed()

    def _interval_elapsed(self) -> bool:
        """Periodic snapshots, for two reasons that are not about this phase.

        A fault injected into a *recording* removes frames; it cannot conjure
        the repair snapshot a live source would have fetched. So without
        periodic snapshots a fault-injected replay can detect a gap and never
        converge, and convergence is half of what the harness exists to prove.

        The second reason is Phase 8's Oracle 1 — reconstructed book against
        the venue's periodic REST snapshot — which needs exactly this in the
        capture and would otherwise force a second pass over the venue.
        """
        if self._snapshot_interval_s <= 0:
            return False
        return time.monotonic() - self._last_snapshot_at >= self._snapshot_interval_s

    # --- fetching ----------------------------------------------------------

    def _snapshot_record(self, ctx: RunContext) -> CaptureRecord:
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
        payload: dict[str, Any] = response.json()
        self._snapshots += 1
        self._last_update_id = int(payload["lastUpdateId"])
        self._first_id_since_snapshot = None
        self._last_snapshot_at = time.monotonic()
        return self._record(
            stream=binance.REST_DEPTH_STREAM,
            kind="snapshot",
            receive_ts=time.time_ns(),
            monotonic_ts=time.monotonic_ns(),
            payload=json.dumps(payload, separators=(",", ":")),
        )

    def _record(
        self,
        *,
        stream: str,
        kind: Kind,
        receive_ts: int,
        monotonic_ts: int,
        payload: str,
    ) -> CaptureRecord:
        self._seq += 1
        return CaptureRecord(
            stream=stream,
            kind=kind,
            seq=self._seq,
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            payload=payload,
        )

    # --- the numbers -------------------------------------------------------

    def _report(self, ctx: RunContext, elapsed_s: float) -> None:
        ctx.logger.info(
            "capture finished",
            symbol=self.symbol,
            stream=self._stream,
            elapsed_s=round(elapsed_s, 3),
            frames=self._frames,
            frames_per_s=round(self._frames / elapsed_s, 1) if elapsed_s else 0.0,
            snapshots=self._snapshots,
        )
