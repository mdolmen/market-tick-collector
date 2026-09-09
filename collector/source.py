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
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from data_pipeline_core import RunContext
from websockets.sync.client import ClientConnection, connect

from collector.adapters.base import Venue
from collector.capture import CaptureRecord, Kind, control_stream, decode

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

# How long after subscribing to check that every symbol on the shard actually
# got one. A rejection comes back in well under a second on all three venues;
# this is generous so a slow first snapshot is never mistaken for one.
_SUBSCRIBE_GRACE_S = 15.0

# A connection that has produced no *book* message for this long is treated as
# dead and reconnected. `websockets` runs its own ping/pong and raises when a
# peer stops answering, so what is left is a socket the venue keeps alive while
# sending nothing useful.
#
# **Book messages, not any message, and that distinction was measured.** A
# 188-symbol Kraken capture across seven shards had three of them deliver every
# symbol's snapshot and then stop: 11, 26 and 28 records where their neighbours
# carried 44,000 to 58,000. Nothing reconnected, because Kraken heartbeats once
# a second on every connection and a watchdog counting those sees a healthy
# socket forever. Counting only frames and snapshots is what makes the failure
# visible at all.
#
# The window is generous against the rate table: the quietest measured symbol
# runs at 0.03 msg/s, so a shard of thirty is expected to speak far inside a
# minute even when every symbol on it is illiquid.
_LIVENESS_TIMEOUT_S = 60.0


class _Pace(Protocol):
    """Just enough of a pacer for the source to hold one."""

    def acquire(self) -> None: ...


class _NoPace:
    """The default, for a venue with nothing to pace or a source run alone."""

    def acquire(self) -> None:
        return None


@dataclass(slots=True)
class _SymbolState:
    """The snapshot decision for one symbol, and nothing else.

    Phase 0 through 2.5 held these as scalars on the source, because a
    connection carried one symbol and the two were the same thing. A shard
    makes them different things: the socket is shared, the decision is not.
    Every field here was one of those scalars, moved rather than invented.
    """

    # The last received frame's final id, and the first id seen since the most
    # recent snapshot — the two the fetch decision needs. Both stay None on a
    # venue whose snapshot arrives in band, where there is no fetch to decide.
    prev_final_seq: int | None = None
    first_seq_since_snapshot: int | None = None
    snapshot_seq: int | None = None
    skipped: bool = False
    # Which of the two checks in ``_track`` said so. Logged rather than acted
    # on — the repair is the same either way — but a run that resubscribes
    # needs to say whether the sequence or the venue's own checksum called it,
    # because on some venues only one of them can.
    skip_reason: str = ""
    refetches: int = 0
    last_snapshot_at: float = 0.0


class FrameSource:
    """Stream a shard's frames, with a snapshot whenever one is due.

    One connection, many symbols, and one ``_SymbolState`` each. The venue
    rules are unchanged — every decision below is the one Phase 2 made, asked
    per symbol instead of once.
    """

    def __init__(
        self,
        *,
        venue: Callable[[], Venue],
        symbols: Sequence[str],
        duration_s: float,
        snapshot_interval_s: float = 0.0,
        subscribe_grace_s: float = _SUBSCRIBE_GRACE_S,
        liveness_timeout_s: float = _LIVENESS_TIMEOUT_S,
    ) -> None:
        """`venue` is a *factory*, because a shard needs more than one.

        One instance is the transport — the URL, the subscribe frames — and is
        stateless. The others are sequencers, one per `sequence_key`, and they
        are emphatically not: Kraken keeps an ordered top-ten view per book to
        recompute the venue's CRC32 over, and Coinbase keeps the connection's
        cursor. Sharing one across a shard mixes two books into one checksum
        view, which fails on every message and asks for a repair that cannot
        help. That was a live-run failure, not a hypothetical:
        `snapshot for DOGE/USD still behind the stream after 6 refetches`.

        It generalises the rule `adapters/base.py` already states — the source
        and the transform each hold their own instance because they ask
        different questions of one venue rule. A shard just makes "their own"
        mean one per sequence rather than one per process.
        """
        self._venue = venue()
        self.symbols = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if not self.symbols:
            raise ValueError("a shard needs at least one symbol")
        # Names this connection in the tag its control traffic lands under, so
        # a run with several shards can tell their sequences apart. The first
        # symbol is stable, unique across a plan (a symbol is on one shard) and
        # already in the capture, so it costs no new identifier.
        self.shard_id = self.symbols[0]
        self._control_stream = control_stream(self.shard_id)
        self.name = f"{self._venue.venue}-depth"
        self._duration_s = duration_s
        self._snapshot_interval_s = snapshot_interval_s
        self._subscribe_grace_s = subscribe_grace_s
        self._liveness_timeout_s = liveness_timeout_s
        # Tag to symbol, which is the direction the receive path needs:
        # ``stream_of`` hands back a tag and the state is keyed by symbol.
        self._symbol_of = {self._venue.stream_tag(s): s for s in self.symbols}
        self._requests = {s: self._venue.snapshot_request(s) for s in self.symbols}
        # A venue's snapshot arrives out of band or in band; it is a property
        # of the venue, so asking any one symbol answers for the shard.
        self._out_of_band = self._requests[self.symbols[0]] is not None
        # Keyed by ``sequence_key``, not by symbol. On a venue that numbers
        # each book independently that is one state per symbol; on one that
        # numbers the connection every symbol maps to the same entry and the
        # dict collapses to a single state. No branch here says which — the
        # aliasing is the venue's answer. See ``VenueAdapter.sequence_key``.
        self._keys = {
            symbol: self._venue.sequence_key(symbol) for symbol in self.symbols
        }
        self._states = {key: _SymbolState() for key in self._keys.values()}
        # One sequencer per key, alongside the state it owns. A venue that
        # numbers the connection collapses to a single entry, exactly as the
        # state does, because it is the same key.
        self._sequencers = {key: venue() for key in self._states}
        # Book messages seen per *symbol*, which is a different question from
        # the sequence and stays per-symbol even where the sequence does not.
        # It answers "did this subscription take", and on a connection-scoped
        # venue the shared cursor cannot: one product speaking would vouch for
        # forty-nine that never did.
        self._heard = dict.fromkeys(self.symbols, 0)

        self._seq = 0
        self._control = 0
        self._resubscribes = 0
        # The venue's own spacing for out-of-band fetches, and the object that
        # enforces it. The supervisor replaces this with one shared across
        # every shard, because the budget is per IP rather than per socket.
        self.snapshot_interval_s = self._venue.snapshot_interval_s()
        self.pacer: _Pace = _NoPace()
        # Set by the supervisor to hand the fetch to a thread of its own. Left
        # None, this class fetches inline, which is right for a single source
        # driven directly (a test, `tools/recovery.py`) and wrong for a shard —
        # see `_snapshot_due_action`.
        self.submit_snapshot: Callable[[FrameSource, str], None] | None = None
        # Symbols with a fetch already in flight, so a busy stream cannot ask
        # for the same snapshot a hundred times while the first is pending.
        self._pending: set[str] = set()
        # When this connection last produced a frame or a snapshot. Reset at
        # every connect; see `_LIVENESS_TIMEOUT_S`.
        self._last_book_at = 0.0

    def dropped(self, stream: str) -> None:
        """A record for this stream was dropped before it could be landed.

        Called by the supervisor when the queue is full. It latches the
        symbol's skip so the existing `_snapshot_due` machinery lands a repair
        snapshot, because a dropped record is a hole the capture otherwise has
        no repair for — and a capture that cannot be replayed cold is the one
        thing this class exists to prevent.

        Deliberately *not* a counter of its own. The reason a drop matters here
        is that the book downstream is about to be wrong, and the existing
        repair path already knows how to say so.
        """
        symbol = self._symbol_of.get(stream)
        if symbol is None:
            return
        state = self._states[self._keys[symbol]]
        state.skipped = True
        state.skip_reason = "dropped"

    def fetch(
        self, ctx: RunContext, *, until: float | None = None
    ) -> Iterator[CaptureRecord]:
        """Stream this shard until `until` (a `time.monotonic` value).

        The deadline is a parameter rather than only a constructor setting so
        the supervisor can call this again after a reconnect and reuse the
        whole of connect → subscribe → check → bootstrap, instead of
        reimplementing any of it.
        """
        with connect(
            self._venue.ws_url(self.symbols),
            close_timeout=_CLOSE_TIMEOUT_S,
            max_size=_MAX_MESSAGE_BYTES,
        ) as ws:
            # One frame for the shard, so the outbound rate limit does not
            # bind. See ``VenueTransport.subscribe_frames``.
            for frame in self._venue.subscribe_frames(self.symbols):
                ws.send(frame)
            ctx.logger.info(
                "connected",
                venue=self._venue.venue,
                symbols=len(self.symbols),
                deadline_s=self._duration_s,
            )
            # Clock starts after the handshake, so the measured window is the
            # duration that was asked for rather than that minus a connect.
            started = time.monotonic()
            # The liveness clock starts at the subscribe, not at the last
            # message, so a connection that never delivers one is caught too.
            self._last_book_at = started
            deadline = started + self._duration_s if until is None else until
            checked_subscription = False
            while True:
                received = self._recv(ws, ctx, deadline)
                if received is None:
                    break
                record, symbol = received
                yield record
                if not checked_subscription and (
                    time.monotonic() - started >= self._subscribe_grace_s
                ):
                    self._assert_the_subscription_took(ctx)
                    checked_subscription = True
                if symbol is None or not self._snapshot_due(ctx, symbol):
                    continue
                # One decision, two actions. Out of band we fetch and land the
                # result ourselves; in band we ask the venue and its answer
                # arrives as an ordinary record a few messages later.
                if self._out_of_band:
                    fetched = self._snapshot_due_action(ctx, symbol)
                    if fetched is not None:
                        yield fetched
                else:
                    self._resubscribe(ws, ctx, symbol)
            if not checked_subscription:
                self._assert_the_subscription_took(ctx)
            self._report(ctx, time.monotonic() - started)

    def _assert_the_subscription_took(self, ctx: RunContext) -> None:
        """A shard that heard nothing at all did not subscribe.

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
        venue-specific parsers. Hearing nothing at all is the one symptom they
        share, and it is not something a working subscription does.

        **Checked once, shortly after subscribing, rather than at the end.**
        Phase 2.5 raised this at the deadline, which was free when a run was
        one symbol: nothing had been landed yet, because ``WorkerApp`` calls
        ``Sink.write`` once and ``raw_landing_sink`` writes one file at the end
        of it. At shard scale that is the opposite of free — one bad subscribe
        would throw away a ten-minute capture. A rejection is answered within a
        second or two, so the grace window catches it just as reliably.

        **The shard, not each symbol on it, and that is a correction.** This
        first asked every symbol to have spoken by the grace point, which the
        rate table then disproved: the quietest measured symbols run at 0.16
        msg/s on Binance and 0.03 on Coinbase — one message every six seconds
        and every half minute. Worse, an out-of-band venue fetches a symbol's
        first snapshot only once its first frame arrives, so a quiet symbol
        produces *nothing* until it trades. A 188-symbol Binance capture died
        23 seconds into a four-minute run with all seven shards raising, and
        landed 70 records.

        So silence per symbol is reported and never fatal, and only a shard
        that heard nothing whatsoever fails the run. A batched subscribe is
        accepted or rejected as a whole on every venue here, which is what
        makes the shard the right unit to check.
        """
        heard = sum(self._heard.values())
        silent = sorted(symbol for symbol, count in self._heard.items() if not count)
        if heard:
            if silent:
                # Expected on an illiquid symbol inside a short window, and the
                # only evidence available if one was quietly dropped. Named
                # either way; the run is the wrong place to decide which.
                ctx.logger.info(
                    "symbols still silent", venue=self._venue.venue, symbols=silent
                )
            return
        raise RuntimeError(
            f"no book message at all on {self._venue.venue} in "
            f"{self._subscribe_grace_s:.0f}s across {len(self.symbols)} symbol(s) "
            f"({self._control} control record(s)) — the subscription was almost "
            f"certainly rejected; check the venue's spelling of {list(self.symbols)!r}"
        )

    def _snapshot_due_action(
        self, ctx: RunContext, symbol: str
    ) -> CaptureRecord | None:
        """Fetch the snapshot, or hand the fetch to whoever owns that.

        **The fetch must not happen on this thread when a shard is running**,
        and that is a guardrail rather than a preference. `ctx.http` retries
        with backoff, and the pacer that keeps the venue's request budget adds
        seconds more — all of it with the socket unread. Measured: a
        188-symbol Binance capture spent so long paced inside this call that
        the liveness watchdog decided the feed was dead, and the run logged
        **25 reconnects** across seven shards and landed 3638 records. Each
        reconnect re-subscribed and re-fetched, which made it worse.

        So the supervisor sets `submit_snapshot` and the fetch happens on a
        thread of its own; the record reaches the sink through the same queue
        as everything else, a little later than the frame that triggered it.
        Inline stays the default for a source driven on its own.
        """
        if self.submit_snapshot is None:
            return self.fetch_snapshot(ctx, symbol)
        if symbol in self._pending:
            return None
        self._pending.add(symbol)
        self.submit_snapshot(self, symbol)
        return None

    def _resubscribe(self, ws: ClientConnection, ctx: RunContext, symbol: str) -> None:
        """Ask an in-band venue for a fresh snapshot, and land nothing yet.

        Only the symbol that needs repairing. Unsubscribing the whole shard to
        fix one book would send every other book on it untrusted, which is the
        blast radius sharding exists to bound.
        """
        frames = self._venue.resubscribe_frames([symbol])
        if not frames:
            return
        for frame in frames:
            ws.send(frame)
        state = self._states[symbol]
        self._resubscribes += 1
        state.last_snapshot_at = time.monotonic()
        # The skip has been acted on. Leaving it latched asks again on the very
        # next record, before the answer has had time to arrive.
        state.skipped = False
        ctx.logger.info("resubscribed for a snapshot", symbol=symbol)

    # --- receiving ---------------------------------------------------------

    def _recv(
        self, ws: ClientConnection, ctx: RunContext, deadline: float
    ) -> tuple[CaptureRecord, str | None] | None:
        """Next record and the symbol it is about, or None once time is up.

        The symbol comes back alongside because ``fetch`` needs it to ask the
        right snapshot question, and re-deriving it there would route every
        message twice.

        Control traffic is landed like everything else. It carries no book
        state, so dropping it looks free — but Coinbase numbers acks in the
        same sequence as book messages, and a capture missing one replays as a
        gap that never happened. ``FaultInjector`` already leaves every
        non-frame record alone, so landing it costs nothing downstream.
        """
        while True:
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0 or ctx.should_stop():
                return None
            if (
                self._liveness_timeout_s > 0
                and now - self._last_book_at > self._liveness_timeout_s
            ):
                # An `OSError`, so the supervisor treats it as the transport
                # failure it is and reconnects this shard alone.
                raise ConnectionError(
                    f"no book message on {self._venue.venue} in "
                    f"{self._liveness_timeout_s:.0f}s across "
                    f"{len(self.symbols)} symbol(s) "
                    f"({self._control} control record(s) in that time); "
                    f"the socket is alive but the feed is not"
                )
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
            stream = self._venue.stream_of(payload)
            # A tag we never subscribed to is the venue answering about
            # something we did not ask for. Landed under its own tag rather
            # than attributed to a symbol, so the capture stays honest.
            symbol = self._symbol_of.get(stream) if stream is not None else None
            kind = self._venue.classify(stream or self._control_stream, payload)
            if kind != "control":
                # Only book traffic counts as alive; see `_LIVENESS_TIMEOUT_S`.
                # Instance state, not a local: `_recv` returns on *every*
                # message, so a local was reset by each heartbeat and the
                # watchdog could only ever measure one call. That is why a
                # stalled shard went 150s without reconnecting.
                self._last_book_at = time.monotonic()
            self._track(kind, payload, symbol)
            return (
                self._record(
                    stream=stream or self._control_stream,
                    kind=kind,
                    receive_ts=receive_ts,
                    monotonic_ts=monotonic_ts,
                    payload=text,
                ),
                symbol,
            )

    def _track(self, kind: Kind, payload: Any, symbol: str | None) -> None:
        """Advance the snapshot-decision state for the symbol this is about.

        Both paths track the sequence, because both need to know when the
        socket skipped — the out-of-band one to refetch, the in-band one to
        resubscribe. Only the action differs, which is why the tracking is
        shared and the branch is one line in ``fetch``.
        """
        if kind == "control":
            self._control += 1
            # Numbered like anything else on a venue that counts them, so it
            # moves the cursor or the next frame looks like it skipped. There
            # is no symbol on a control message, so this moves every distinct
            # cursor — which is one of them on the only venue that numbers
            # control, and on the others is the cursor's own current value.
            for key, state in self._states.items():
                state.prev_final_seq = self._sequencers[key].sequence_ids(payload)[1]
            return
        if symbol is None:
            return
        key = self._keys[symbol]
        state = self._states[key]
        sequencer = self._sequencers[key]
        self._heard[symbol] += 1
        if kind == "snapshot":
            sequencer.observe(payload)
            state.snapshot_seq = sequencer.snapshot_seq(payload)
            state.first_seq_since_snapshot = None
            state.last_snapshot_at = time.monotonic()
            # An in-band snapshot occupies a sequence number of its own, so it
            # moves the cursor. Without this the frame after every snapshot
            # fails the chain rule, which asks for another snapshot, which is a
            # resubscribe loop — 42 of them in a 25s run before this line.
            state.prev_final_seq = sequencer.snapshot_seq(payload)
            state.skipped = False
            return
        # Two ways one frame can tell this class the stream broke, and a venue
        # normally has only one of them. The chain rule is the sequence's
        # verdict; ``observe`` is the venue's own, and on a venue that numbers
        # nothing it is the only one that ever fires — ``chains`` there is
        # trivially true, so without this the source would never ask for a
        # repair and the capture would carry no snapshot at the one point a
        # replay needs one.
        consistent = sequencer.observe(payload)
        first_seq, final_seq = sequencer.sequence_ids(payload)
        # Judged against the *previous* frame, so it has to happen before the
        # cursor moves.
        broke_chain = state.prev_final_seq is not None and not sequencer.chains(
            state.prev_final_seq, first_seq
        )
        state.skipped = broke_chain or not consistent
        state.skip_reason = "sequence" if broke_chain else "checksum"
        state.prev_final_seq = final_seq
        if state.first_seq_since_snapshot is None:
            state.first_seq_since_snapshot = first_seq

    # --- deciding when to snapshot -----------------------------------------

    def _snapshot_due(self, ctx: RunContext, symbol: str) -> bool:
        """True when this symbol needs a fresh snapshot to stay replayable.

        Three conditions, and the first two are the failure branches of the
        bootstrap seen from the I/O side. A snapshot that predates the first
        frame since it was fetched has already lost updates and no amount of
        further buffering fixes it. A break in the chain means the socket
        skipped, and the book downstream is about to go untrusted with nothing
        to repair it.

        The third is the *initial* snapshot on an out-of-band venue, and it is
        deliberately lazy: due once this symbol's first frame has arrived, not
        at subscribe. "Socket first, snapshot second" is why — the other order
        loses every diff that lands during the fetch — but at shard scale
        fetching before the read loop would also mean a hundred blocking REST
        calls with the socket unattended, which is how a connection dies. One
        fetch per symbol, triggered by that symbol's own first frame, keeps the
        ordering guarantee and spreads the storm across arrivals.

        On an in-band venue ``snapshot_stale`` is always false — there is no
        fetch window for updates to be lost in — so only the skip and the
        interval can fire, and both resolve to a resubscribe.
        """
        if symbol in self._pending:
            # A fetch is already in flight for this symbol. Nothing it could
            # say has arrived yet, so asking again can only re-answer the same
            # question with the same state — and the refetch budget below is
            # spent in milliseconds rather than over the stalls it exists for.
            # Inline fetching hid this: the state was always current by the
            # time the next frame arrived. Measured once the fetch moved to its
            # own thread: five identical "snapshot too old" warnings 20ms apart
            # on the same unchanged pair of sequence numbers, then a dead shard.
            return False
        key = self._keys[symbol]
        state = self._states[key]
        if state.first_seq_since_snapshot is None:
            return False
        if state.snapshot_seq is None:
            # In band, the venue sends it unasked and this must not pre-empt
            # it; out of band, nobody else will.
            return self._out_of_band
        if self._sequencers[key].snapshot_stale(
            state.first_seq_since_snapshot, state.snapshot_seq
        ):
            state.refetches += 1
            if state.refetches > _MAX_SNAPSHOT_REFETCHES:
                raise RuntimeError(
                    f"snapshot for {symbol} still behind the stream after "
                    f"{state.refetches} refetches"
                )
            ctx.logger.warning(
                "snapshot too old, refetching",
                symbol=symbol,
                snapshot_seq=state.snapshot_seq,
                buffer_first_seq=state.first_seq_since_snapshot,
                attempt=state.refetches,
            )
            return True
        # The snapshot caught up, so the refetch budget is for the next stall
        # rather than for the whole run.
        state.refetches = 0
        if state.skipped:
            ctx.logger.warning(
                "stream broke, fetching a snapshot",
                symbol=symbol,
                reason=state.skip_reason,
            )
            return True
        return self._interval_elapsed(state)

    def _interval_elapsed(self, state: _SymbolState) -> bool:
        """Periodic snapshots, for two reasons that are not about this phase.

        A fault injected into a *recording* removes frames; it cannot conjure
        the repair snapshot a live source would have fetched. So without
        periodic snapshots a fault-injected replay can detect a gap and never
        converge, and convergence is half of what the harness exists to prove.

        The second reason is Phase 6's oracle — reconstructed book against
        the venue's periodic REST snapshot — which needs exactly this in the
        capture and would otherwise force a second pass over the venue.
        """
        if self._snapshot_interval_s <= 0:
            return False
        return time.monotonic() - state.last_snapshot_at >= self._snapshot_interval_s

    # --- fetching ----------------------------------------------------------

    def fetch_snapshot(self, ctx: RunContext, symbol: str) -> CaptureRecord:
        """Through ``ctx.http``, so it inherits retry, backoff and the breaker.

        The websocket gets none of that, and that asymmetry is the argument for
        the connection supervisor in Phase 3.

        **Called on the snapshot thread when a shard is running**, so the
        blocking here costs no socket reads. `_snapshot_due_action` explains
        what happened when it did not. The state it writes belongs to one
        symbol and is read by the reader thread; the writes are single
        assignments of immutable values, which is what makes that safe.
        """
        request = self._requests[symbol]
        assert request is not None  # only the out-of-band path gets here
        self._pending.discard(symbol)
        # Before the call, not after: the venue prices the request itself, and
        # exceeding its budget is answered with a ban rather than a rejection.
        self.pacer.acquire()
        response = ctx.http.get(request.url, params=dict(request.params))
        if response.status_code != 200:
            raise RuntimeError(f"depth snapshot returned {response.status_code}")
        payload: dict[str, Any] = response.json()
        key = self._keys[symbol]
        state = self._states[key]
        self._heard[symbol] += 1
        state.snapshot_seq = self._sequencers[key].snapshot_seq(payload)
        state.first_seq_since_snapshot = None
        state.last_snapshot_at = time.monotonic()
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
        heard = sum(self._heard.values())
        # Silence after the grace window is legitimate — an illiquid symbol
        # that has already sent its snapshot owes nothing more — but it is
        # still worth naming, because it is also what a shard looks like when
        # one symbol has quietly stopped.
        quiet = sorted(symbol for symbol, n in self._heard.items() if not n)
        ctx.logger.info(
            "capture finished",
            venue=self._venue.venue,
            symbols=len(self.symbols),
            elapsed_s=round(elapsed_s, 3),
            book_messages=heard,
            messages_per_s=round(heard / elapsed_s, 1) if elapsed_s else 0.0,
            control=self._control,
            resubscribes=self._resubscribes,
            quiet=quiet,
        )
