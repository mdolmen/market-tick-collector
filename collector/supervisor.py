"""One venue's shards, on the SDK's connection supervisor.

`NOTES.md` § *Connection supervision*: N connections, each with its own
backoff, liveness and resubscribe, none of them allowed to take down the others
or the shared book state. That machinery is `ConnectionSupervisor` in
`data-pipeline-core` — the reader threads, the bounded queue that drops rather
than blocking, the per-connection failure counts and the single drain. It went
there because none of it knows what a venue is, and this class is what is left
once it has gone: the parts that do.

**What stays here, and why each is business logic.** The pacer, because a
venue's REST budget is per IP and the number comes from its published weights.
The snapshot thread, because an out-of-band bootstrap is a venue's answer to
"how do I start a book" and two of three venues answer differently. The `seq`
stamp, because it is this capture format's arrival order and what makes
`FaultInjector` reproducible. And the repair latch on a drop, because a dropped
record is a hole only the venue's snapshot can fill.

**The drop policy is the SDK's now, and the guarantee is unchanged.** A full
queue drops, counts on `messages_dropped_total`, and calls back here so the
affected symbol is marked for a repair snapshot — which is strictly cheaper
than stalling a reader and losing the connection along with every book on it.

**Rotation is break-before-make.** At `rotate_after_s` a shard closes and
reconnects, and normal recovery runs. Phase 3 measured that gap at zero lost
frames, which is why Phase 4's make-before-break was skipped rather than built:
the cutover would need the replacement connection's records tagged apart from
the old one's — a capture format change, for a 23h trigger no bounded run can
exercise.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator, Sequence
from queue import Empty, Queue

from data_pipeline_core import ConnectionSupervisor, RunContext
from websockets.exceptions import WebSocketException

from collector.capture import CaptureRecord
from collector.source import FrameSource

# How long the snapshot thread waits on an empty request queue before looking
# at the clock again.
_SNAPSHOT_SLICE_S = 0.2

# Transport failures, which reconnect this shard alone. Anything else — a
# `RuntimeError` from the subscription guard, a price `SCALE` refuses — is a
# data or configuration error that reconnecting cannot fix, and the supervisor
# stops that shard loudly instead.
_TRANSPORT_ERRORS = (WebSocketException, OSError, TimeoutError)


class _Pacer:
    """A minimum interval between snapshot fetches, shared by every shard.

    A venue's REST budget is per IP, so pacing each connection separately would
    let N shards together exceed it by a factor of N — which is exactly how a
    188-symbol Binance run earned a `418` and landed 56 records in two minutes.
    One pacer for the run, a lock, and a next-allowed timestamp.

    Blocking is correct here and is not the guardrail violation it looks like.
    It blocks the snapshot thread, which exists precisely so that no reader
    thread ever waits on HTTP.
    """

    def __init__(self, interval_s: float) -> None:
        self._interval = interval_s
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self._interval
        if wait:
            time.sleep(wait)


class ShardSupervisor:
    """Run one venue's shards concurrently and yield their records in order.

    A `Source` in the SDK's sense: `WorkerApp` calls `fetch` once, consumes it
    lazily through the sink, and never learns that anything was threaded.
    """

    def __init__(
        self,
        *,
        sources: Sequence[FrameSource],
        duration_s: float,
        stagger_s: float = 0.0,
        queue_maxsize: int = 10_000,
        rotate_after_s: float = 0.0,
        backoff_base_s: float = 0.5,
    ) -> None:
        if not sources:
            raise ValueError("a supervisor needs at least one shard")
        self._sources = tuple(sources)
        self.name = f"{sources[0].name}-shards"
        self._duration_s = duration_s
        self._supervisor: ConnectionSupervisor[CaptureRecord] = ConnectionSupervisor(
            self._sources,
            queue_maxsize=queue_maxsize,
            stagger_seconds=stagger_s,
            rotate_after_seconds=rotate_after_s,
            backoff_base_seconds=backoff_base_s,
            duration_seconds=duration_s,
            retry_on=_TRANSPORT_ERRORS,
            on_drop=self._repair,
        )
        # One for the run: the venue's REST budget is per IP, not per socket.
        self._pacer = _Pacer(self._sources[0].snapshot_interval_s)
        # Out-of-band snapshot fetches, taken off the reader threads entirely.
        self._snapshots: Queue[tuple[FrameSource, str]] = Queue()
        for source in self._sources:
            source.pacer = self._pacer
            source.submit_snapshot = self._submit_snapshot
        self._stop = threading.Event()
        self._seq = 0

        self.snapshot_failures = 0

    # --- what the SDK counts, under the names this project reports ----------

    @property
    def dropped(self) -> int:
        return self._supervisor.dropped

    @property
    def reconnects(self) -> int:
        return self._supervisor.reconnects

    @property
    def rotations(self) -> int:
        return self._supervisor.rotations

    @property
    def fatal(self) -> int:
        """Shards that stopped for a reason no reconnect can fix."""
        return self._supervisor.fatal

    @property
    def failures(self) -> list[int]:
        """Per shard, so "a failure never crosses connections" is checkable
        rather than asserted: a fault on shard 2 must leave 1 and 3 at zero."""
        return self._supervisor.failures

    # --- the Source contract -----------------------------------------------

    def fetch(self, ctx: RunContext) -> Iterator[CaptureRecord]:
        deadline = time.monotonic() + self._duration_s
        # Logged before the run rather than beside a failure: without it no
        # per-shard number — `failures_per_shard`, a fatal shard's index — can
        # be read back to the symbols it is about.
        ctx.logger.info(
            "shards starting",
            venue=self._sources[0].name,
            shards={i: list(s.symbols) for i, s in enumerate(self._sources)},
        )
        snapshots = threading.Thread(
            target=self._run_snapshots,
            args=(ctx, deadline),
            name="snapshots",
            daemon=True,
        )
        snapshots.start()
        records = self._supervisor.fetch(ctx)
        try:
            for record in records:
                # Stamped here, on one thread, so arrival order is a total
                # order across shards and every seq is unique in the landed
                # file.
                self._seq += 1
                record["seq"] = self._seq
                yield record
        finally:
            # Reached on a sink failure too, where only the generator's
            # `finally` runs. Without it the snapshot thread outlives the run.
            self._stop.set()
            records.close()
            snapshots.join(timeout=5.0)
            self._report(ctx)

    # --- out-of-band snapshots ----------------------------------------------

    def _submit_snapshot(self, source: FrameSource, symbol: str) -> None:
        """Queued from a reader thread, served by the snapshot thread."""
        self._snapshots.put((source, symbol))

    def _run_snapshots(self, ctx: RunContext, deadline: float) -> None:
        """One thread for every out-of-band fetch the run makes.

        It exists so no reader thread ever blocks on HTTP. `ctx.http` retries
        with backoff and the pacer adds the venue's own required spacing on
        top, which together stalled a reader long enough for the liveness
        watchdog to declare the feed dead: 25 reconnects across seven shards
        on a 188-symbol Binance capture, each one re-fetching what it had just
        abandoned.

        One thread rather than a pool, because the pacer would serialise a
        pool anyway — the venue's budget is per IP.
        """
        while not self._stopping(deadline):
            try:
                source, symbol = self._snapshots.get(timeout=_SNAPSHOT_SLICE_S)
            except Empty:
                continue
            self._pacer.acquire()
            if self._stopping(deadline):
                return
            try:
                record = source.fetch_snapshot(ctx, symbol)
            except Exception as error:
                # A failed snapshot leaves that book untrusted and nothing
                # else; the symbol's skip stays latched, so the next frame
                # asks again. Killing the run over one of them would throw
                # away every other book on every other shard.
                self.snapshot_failures += 1
                ctx.logger.warning(
                    "snapshot fetch failed",
                    symbol=symbol,
                    error=f"{type(error).__name__}: {error}",
                )
                continue
            # The same queue and the same drop policy as a live frame: a
            # second queue here would mean a second policy and a second count.
            self._supervisor.offer(ctx, record)

    # --- drops --------------------------------------------------------------

    def _repair(self, record: CaptureRecord) -> None:
        """Latch a repair on the shard that owns a dropped record's stream.

        A dropped record is a hole the capture has no repair for, which is what
        makes a capture unreplayable. Latching the symbol's skip makes the
        existing snapshot machinery land one.

        Offered to every shard because a stream belongs to exactly one of them
        and `FrameSource.dropped` ignores a stream it does not own — cheaper
        than maintaining a second index of who holds what, on a path that by
        construction runs rarely.
        """
        for source in self._sources:
            source.dropped(record["stream"])

    # --- waiting ------------------------------------------------------------

    def _stopping(self, deadline: float) -> bool:
        return self._stop.is_set() or time.monotonic() >= deadline

    def _report(self, ctx: RunContext) -> None:
        health = self._supervisor.health()
        ctx.logger.info(
            "shards finished",
            shards=health.connections,
            records=health.records,
            dropped=health.dropped,
            snapshot_failures=self.snapshot_failures,
            fatal_shards=health.fatal,
            reconnects=health.reconnects,
            rotations=health.rotations,
            failures_per_shard=list(health.failures),
        )
