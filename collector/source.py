"""``FrameSource`` — the socket and, where a venue needs one, the REST fetch.

An ingest worker in the SDK's sense (``ARCHITECTURE.md`` § *Two archetypes*):
it produces the venue's bytes wrapped in a ``CaptureRecord`` and never builds a
book. Wired to ``raw_landing_sink`` it is a capture run; wired to a curated
sink with ``BookTransform`` it is the Phase 0 collector, unchanged in
behaviour. The split is what lets a replay drive the *same* transform from
disk instead of from a socket.

Bounded is still the point. ``WorkerApp`` runs a single pass and calls
``Sink.write()`` exactly once, so a source that returns after a fixed duration
fits the existing SDK contract untouched. The ``ServiceApp`` a real always-on
collector demands is Phase 4's problem.

**Two snapshot paths, and the venue picks which.** ``snapshot_request()``
returning a request means the snapshot arrives out of band over REST, and this
class has to decide when to fetch one — Binance. Returning ``None`` means the
venue sends its own on the socket, and this class fetches nothing at all and
merely classifies what arrives — Coinbase. That is the only branch in here, and
it is a genuine transport difference rather than a venue special case.

**Why this class watches the sequence at all**, on the out-of-band path. It
must not build a book, but it must know when to fetch a snapshot — because a
capture that lacks a snapshot at every point one is needed cannot be replayed
cold, and the transform (which does no I/O) can never ask for one. So both
sides consult the adapter's ``chains``, and they ask it different questions:
this one asks *did the socket skip, so should I fetch*, the transform asks *is
the book still trustworthy*. One venue rule, one definition, two decisions. The
alternative — a back-channel from transform to source — does not exist in the
``Transform`` protocol and would put I/O back in the transform, which is what
breaks replay.

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

from collector.adapters.base import Venue
from collector.capture import CaptureRecord, Kind, decode

# A refetch that keeps landing behind the buffer means something is wrong with
# the venue or the clock, not with our patience. Fail the run rather than spin.
_MAX_SNAPSHOT_REFETCHES = 5

# Cap on a single blocking recv, so the duration and SIGTERM are both honoured
# promptly even on a silent socket.
_RECV_SLICE_S = 1.0

# The library default is 10s, which a venue that has nothing more to say spends
# in full — a dead tail longer than a short run.
_CLOSE_TIMEOUT_S = 2.0

# ``websockets`` caps a message at 1 MiB by default and closes the connection
# when one exceeds it. A venue that sends its snapshot in band goes straight
# through that — Coinbase's BTC-USD book measured ~4.9 MB in Phase 2's probe,
# and the first run died 0.7s in with `1009 (message too big)`. Raised rather
# than disabled: unbounded lets a venue drive our allocator.
_MAX_MESSAGE_BYTES = 16 * 1024 * 1024


class FrameSource:
    """Stream one symbol's frames, with a snapshot whenever one is due."""

    def __init__(
        self,
        *,
        venue: Venue,
        symbol: str,
        duration_s: float,
        snapshot_interval_s: float = 0.0,
    ) -> None:
        self._venue = venue
        self.symbol = symbol.upper()
        self.name = f"{venue.venue}-depth"
        self._duration_s = duration_s
        self._snapshot_interval_s = snapshot_interval_s
        self._stream = venue.stream_tag(self.symbol)
        self._snapshot_request = venue.snapshot_request(self.symbol)

        self._seq = 0
        self._frames = 0
        self._snapshots = 0
        self._control = 0
        # The last received frame's final id, and the first id seen since the
        # most recent snapshot was fetched — the two the fetch decision needs,
        # and the only sequence state this class keeps. Both stay None on a
        # venue whose snapshot arrives in band, where there is no fetch to
        # decide about.
        self._prev_final_seq: int | None = None
        self._first_seq_since_snapshot: int | None = None
        self._snapshot_seq: int | None = None
        self._skipped = False
        # Which of the two checks in ``_track`` said so. Logged rather than
        # acted on — the repair is the same either way — but a run that
        # resubscribes needs to say whether the sequence or the venue's own
        # checksum called it, because on some venues only one of them can.
        self._skip_reason = ""
        self._refetches = 0
        self._resubscribes = 0
        self._last_snapshot_at = 0.0

    def fetch(self, ctx: RunContext) -> Iterator[CaptureRecord]:
        with connect(
            self._venue.ws_url(self.symbol),
            close_timeout=_CLOSE_TIMEOUT_S,
            max_size=_MAX_MESSAGE_BYTES,
        ) as ws:
            for frame in self._venue.subscribe_frames(self.symbol):
                ws.send(frame)
            ctx.logger.info(
                "connected", stream=self._stream, deadline_s=self._duration_s
            )
            # Clock starts after the handshake, so the measured window is the
            # duration that was asked for rather than that minus a connect.
            started = time.monotonic()
            deadline = started + self._duration_s
            # Socket first, snapshot second. The other order loses every diff
            # that lands during the fetch and starts the book silently corrupt.
            # In-band venues get this ordering from the venue for free.
            if self._snapshot_request is not None:
                yield self._snapshot_record(ctx)
            while True:
                record = self._recv(ws, ctx, deadline)
                if record is None:
                    break
                yield record
                if not self._snapshot_due(ctx):
                    continue
                # One decision, two actions. Out of band we fetch and land the
                # result ourselves; in band we ask the venue and its answer
                # arrives as an ordinary record a few messages later.
                if self._snapshot_request is not None:
                    yield self._snapshot_record(ctx)
                else:
                    self._resubscribe(ws, ctx)
            self._report(ctx, time.monotonic() - started)
            self._assert_the_subscription_took()

    def _assert_the_subscription_took(self) -> None:
        """A run that received no book message at all did not run.

        Venue-neutral on purpose, because the way this goes wrong is not.
        Phase 2.5 subscribed Kraken to ``XBT/USD`` — the spelling
        ``collector/symbols.py`` had committed — and the venue answered
        ``{"error":"Currency pair not supported","success":false}`` on a socket
        that then stayed open and silent. The run connected, landed two control
        records, reported ``frames: 0`` and exited **zero**. Nothing in the
        pipeline objected, because every stage after this one is built to
        tolerate a quiet stream.

        A rejected subscription looks different on every venue and is a
        different shape again on each, so matching the ack would be three
        venue-specific parsers. Not receiving a single book message in the
        whole window is the one symptom they share, and it is not something a
        working subscription does — even an illiquid symbol gets the in-band
        snapshot, and an out-of-band venue gets the REST one.
        """
        if self._frames or self._snapshots:
            return
        raise RuntimeError(
            f"no book message on {self._stream!r} in {self._duration_s:.0f}s "
            f"({self._control} control record(s)) — the subscription was "
            f"almost certainly rejected; check the venue's spelling of "
            f"{self.symbol!r}"
        )

    def _resubscribe(self, ws: ClientConnection, ctx: RunContext) -> None:
        """Ask an in-band venue for a fresh snapshot, and land nothing yet."""
        frames = self._venue.resubscribe_frames(self.symbol)
        if not frames:
            return
        for frame in frames:
            ws.send(frame)
        self._resubscribes += 1
        self._last_snapshot_at = time.monotonic()
        # The skip has been acted on. Leaving it latched asks again on the very
        # next record, before the answer has had time to arrive.
        self._skipped = False
        ctx.logger.info("resubscribed for a snapshot", stream=self._stream)

    # --- receiving ---------------------------------------------------------

    def _recv(
        self, ws: ClientConnection, ctx: RunContext, deadline: float
    ) -> CaptureRecord | None:
        """Next record, or None once the duration is up or a SIGTERM arrived.

        Control traffic is landed like everything else. It carries no book
        state, so dropping it looks free — but Coinbase numbers acks in the
        same sequence as book messages, and a capture missing one replays as a
        gap that never happened. ``FaultInjector`` already leaves every
        non-frame record alone, so landing it costs nothing downstream.
        """
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
            # stdlib json on purpose: it is the baseline bench/decode.py
            # measures the alternatives against, and the only one of the three
            # that can keep a venue's number as its source token at all. Only
            # the ids are read here — the record model is the transform's cost,
            # not the capture worker's.
            payload = decode(text)
            kind = self._venue.classify(self._stream, payload)
            self._track(kind, payload)
            return self._record(
                stream=self._stream,
                kind=kind,
                receive_ts=receive_ts,
                monotonic_ts=monotonic_ts,
                payload=text,
            )

    def _track(self, kind: Kind, payload: Any) -> None:
        """Advance the snapshot-decision state.

        Both paths track the sequence, because both need to know when the
        socket skipped — the out-of-band one to refetch, the in-band one to
        resubscribe. Only the action differs, which is why the tracking is
        shared and the branch is one line in ``fetch``.
        """
        if kind == "snapshot":
            self._venue.observe(payload)
            self._snapshots += 1
            self._snapshot_seq = self._venue.snapshot_seq(payload)
            self._first_seq_since_snapshot = None
            self._last_snapshot_at = time.monotonic()
            # An in-band snapshot occupies a sequence number of its own, so it
            # moves the cursor. Without this the frame after every snapshot
            # fails the chain rule, which asks for another snapshot, which is a
            # resubscribe loop — 42 of them in a 25s run before this line.
            self._prev_final_seq = self._venue.snapshot_seq(payload)
            self._skipped = False
            return
        if kind == "control":
            self._control += 1
            # Numbered like anything else on a venue that counts them, so it
            # moves the cursor or the next frame looks like it skipped.
            self._prev_final_seq = self._venue.sequence_ids(payload)[1]
            return
        self._frames += 1
        # Two ways one frame can tell this class the stream broke, and a venue
        # normally has only one of them. The chain rule is the sequence's
        # verdict; ``observe`` is the venue's own, and on a venue that numbers
        # nothing it is the only one that ever fires — ``chains`` there is
        # trivially true, so without this the source would never ask for a
        # repair and the capture would carry no snapshot at the one point a
        # replay needs one.
        consistent = self._venue.observe(payload)
        first_seq, final_seq = self._venue.sequence_ids(payload)
        # Judged against the *previous* frame, so it has to happen before the
        # cursor moves.
        broke_chain = self._prev_final_seq is not None and not self._venue.chains(
            self._prev_final_seq, first_seq
        )
        self._skipped = broke_chain or not consistent
        self._skip_reason = "sequence" if broke_chain else "checksum"
        self._prev_final_seq = final_seq
        if self._first_seq_since_snapshot is None:
            self._first_seq_since_snapshot = first_seq

    # --- deciding when to snapshot -----------------------------------------

    def _snapshot_due(self, ctx: RunContext) -> bool:
        """True when the capture needs a fresh snapshot to stay replayable.

        Two conditions, and they are the two failure branches of the bootstrap
        seen from the I/O side. A snapshot that predates the first frame since
        it was fetched has already lost updates and no amount of further
        buffering fixes it. A break in the chain means the socket skipped, and
        the book downstream is about to go untrusted with nothing to repair it.

        On an in-band venue ``snapshot_stale`` is always false — there is no
        fetch window for updates to be lost in — so only the skip and the
        interval can fire, and both resolve to a resubscribe.
        """
        if self._snapshot_seq is None or self._first_seq_since_snapshot is None:
            return False
        if self._venue.snapshot_stale(
            self._first_seq_since_snapshot, self._snapshot_seq
        ):
            self._refetches += 1
            if self._refetches > _MAX_SNAPSHOT_REFETCHES:
                raise RuntimeError(
                    f"snapshot still behind the stream after "
                    f"{self._refetches} refetches"
                )
            ctx.logger.warning(
                "snapshot too old, refetching",
                snapshot_seq=self._snapshot_seq,
                buffer_first_seq=self._first_seq_since_snapshot,
                attempt=self._refetches,
            )
            return True
        # The snapshot caught up, so the refetch budget is for the next stall
        # rather than for the whole run.
        self._refetches = 0
        if self._skipped:
            ctx.logger.warning(
                "stream broke, fetching a snapshot", reason=self._skip_reason
            )
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
        request = self._snapshot_request
        assert request is not None  # only the out-of-band path gets here
        response = ctx.http.get(request.url, params=dict(request.params))
        if response.status_code != 200:
            raise RuntimeError(f"depth snapshot returned {response.status_code}")
        payload: dict[str, Any] = response.json()
        self._snapshots += 1
        self._snapshot_seq = self._venue.snapshot_seq(payload)
        self._first_seq_since_snapshot = None
        self._last_snapshot_at = time.monotonic()
        return self._record(
            stream=request.stream,
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
            venue=self._venue.venue,
            symbol=self.symbol,
            stream=self._stream,
            elapsed_s=round(elapsed_s, 3),
            frames=self._frames,
            frames_per_s=round(self._frames / elapsed_s, 1) if elapsed_s else 0.0,
            snapshots=self._snapshots,
            control=self._control,
            resubscribes=self._resubscribes,
        )
