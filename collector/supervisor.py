"""N connections, one record stream, and no shared fate between them.

`NOTES.md` § *Connection supervision*: N connections, each with its own
backoff, liveness and resubscribe, none of them allowed to take down the others
or the shared book state. This is that, as a `Source` — so `WorkerApp` runs a
sharded capture with no change to the SDK's one-source contract.

**One reader thread per shard, one queue, one drain.** The drain is the
generator `WorkerApp` consumes, so the whole pipeline downstream stays
single-threaded and lazy exactly as it was. The threads exist because a socket
read blocks and N of them cannot be served from one loop without an event loop
this project deliberately does not have on the data path (`NOTES.md`
§ *Receiver architecture*).

**The enqueue never blocks the socket read.** `CLAUDE.md` calls that a
guardrail, not an aspiration: a feed has no flow control, so stalling the
reader closes the receive window, fills the venue's send buffer and gets the
connection dropped — which costs a re-bootstrap of every book on it. So a full
queue *drops*, counts, and latches a repair for the affected symbol, which is
strictly cheaper than losing the connection.

What Phase 5 still owns and this deliberately does not do: sizing the queue
from a measurement, high and low watermarks rather than one threshold, shedding
whole symbols under sustained pressure, the power-of-two ring with a masked
index, and emitting each drop as a `gap` control record so a replay reproduces
it. This is the smallest thing that does not violate the guardrail.

**`seq` is stamped on the drain, not by the readers.** `CaptureRecord.seq` is
the capture's own arrival order (`collector/capture.py`), and it is what makes
a reordering fault expressible and `FaultInjector` reproducible. Two shards
numbering independently would produce duplicate seqs in one file and quietly
void both properties.

**Rotation is break-before-make, and the gap is the point.** At
`rotate_after_s` a shard closes and reconnects, and normal recovery runs. That
costs a real gap and measures it, which is the number Phase 4's make-before-break
has to beat. Doing the cutover properly needs the replacement connection's
records tagged apart from the old one's — a capture format change to land
something whose 23h trigger cannot be exercised under a bounded run at all.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator, Sequence
from queue import Empty, Full, Queue

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.backoff import Backoff
from websockets.exceptions import WebSocketException

from collector.capture import CaptureRecord
from collector.source import FrameSource

# How long the drain waits on an empty queue before looking at the clock again.
# Short enough that shutdown is prompt, long enough not to spin.
_DRAIN_SLICE_S = 0.2

# A reconnect sleeps in slices so a SIGTERM during a long backoff is honoured
# rather than waited out.
_BACKOFF_SLICE_S = 0.5


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
        self._stagger_s = stagger_s
        self._rotate_after_s = rotate_after_s
        self._backoff_base_s = backoff_base_s
        self._queue: Queue[CaptureRecord] = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._seq = 0

        self.dropped = 0
        self.reconnects = 0
        self.rotations = 0
        # Per shard, so "a failure never crosses connections" is checkable
        # rather than asserted: a fault on shard 2 must leave 1 and 3 at zero.
        self.failures: list[int] = [0] * len(self._sources)

    # --- the Source contract -----------------------------------------------

    def fetch(self, ctx: RunContext) -> Iterator[CaptureRecord]:
        deadline = time.monotonic() + self._duration_s
        threads = [
            threading.Thread(
                target=self._run_shard,
                args=(index, source, ctx, deadline),
                name=f"shard-{index}",
                daemon=True,
            )
            for index, source in enumerate(self._sources)
        ]
        for thread in threads:
            thread.start()
        try:
            yield from self._drain(ctx, deadline, threads)
        finally:
            # Reached on a sink failure too, where only the generator's
            # `finally` runs. Without it the readers outlive the run.
            self._stop.set()
            for thread in threads:
                thread.join(timeout=5.0)
            self._report(ctx)

    def _drain(
        self, ctx: RunContext, deadline: float, threads: list[threading.Thread]
    ) -> Iterator[CaptureRecord]:
        while True:
            try:
                record = self._queue.get(timeout=_DRAIN_SLICE_S)
            except Empty:
                if time.monotonic() >= deadline or ctx.should_stop():
                    return
                if not any(thread.is_alive() for thread in threads):
                    return
                continue
            # Stamped here, on one thread, so arrival order is a total order
            # across shards and every seq is unique in the landed file.
            self._seq += 1
            record["seq"] = self._seq
            yield record

    # --- one shard ----------------------------------------------------------

    def _run_shard(
        self, index: int, source: FrameSource, ctx: RunContext, deadline: float
    ) -> None:
        """Connect, read, and reconnect on failure — this shard only.

        Nothing here touches another shard's state, which is the whole of "a
        failure never crosses connections". The only shared objects are the
        queue and the stop event, and neither carries a shard's identity.
        """
        self._wait(index * self._stagger_s, deadline)
        backoff = Backoff(base_seconds=self._backoff_base_s)
        attempt = 0
        while not self._stopping(deadline):
            until = deadline
            if self._rotate_after_s > 0:
                until = min(deadline, time.monotonic() + self._rotate_after_s)
            try:
                for record in source.fetch(ctx, until=until):
                    self._offer(record, source)
            except (WebSocketException, OSError, TimeoutError) as error:
                # Transport only. A `RuntimeError` from the subscription guard
                # is a configuration mistake and reconnecting cannot fix it, so
                # it is left to propagate and fail the run.
                self.failures[index] += 1
                self.reconnects += 1
                attempt += 1
                ctx.logger.warning(
                    "shard connection failed, backing off",
                    shard=index,
                    venue=source.name,
                    attempt=attempt,
                    error=f"{type(error).__name__}: {error}",
                )
                self._wait(backoff.delay(attempt - 1), deadline)
                continue
            # A clean return means the window closed: either the run is over,
            # or this was a rotation and the shard reconnects immediately.
            attempt = 0
            if self._stopping(deadline):
                return
            self.rotations += 1
            ctx.logger.info("rotating shard connection", shard=index)

    def _offer(self, record: CaptureRecord, source: FrameSource) -> None:
        """Enqueue without ever blocking the read. See the module docstring."""
        try:
            self._queue.put_nowait(record)
        except Full:
            self.dropped += 1
            # A dropped record is a hole the capture has no repair for, which
            # is what makes a capture unreplayable. Latching the symbol's skip
            # makes the existing snapshot machinery land one.
            source.dropped(record["stream"])

    # --- waiting ------------------------------------------------------------

    def _stopping(self, deadline: float) -> bool:
        return self._stop.is_set() or time.monotonic() >= deadline

    def _wait(self, seconds: float, deadline: float) -> None:
        """Sleep in slices, so a stop during a long backoff is prompt."""
        until = min(time.monotonic() + seconds, deadline)
        while time.monotonic() < until and not self._stop.is_set():
            self._stop.wait(min(_BACKOFF_SLICE_S, until - time.monotonic()))

    def _report(self, ctx: RunContext) -> None:
        ctx.logger.info(
            "shards finished",
            shards=len(self._sources),
            records=self._seq,
            dropped=self.dropped,
            reconnects=self.reconnects,
            rotations=self.rotations,
            failures_per_shard=self.failures,
        )
