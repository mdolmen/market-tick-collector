"""Replay a capture from disk, optionally with faults, into the same book.

The point of the whole harness is that nothing downstream of here knows it is
being replayed: ``ReplaySource`` yields the identical ``CaptureRecord`` stream
``FrameSource`` produced, so ``BookTransform`` is the object
under test rather than a copy of it.

**Faults perturb the transport, never the book.** Every one of them is a
transformation of the record iterator — remove a record, swap two, repeat one,
move a clock. The adapter and the book see a stream that is merely wrong, in
the ways a real network is wrong, and have no way to tell an injected fault
from a genuine one. That is what makes the Phase 8 claim "detected 100% of
injected faults" mean something; if a fault were injected by reaching into the
book, the claim would only say the test and the code agree.

**Snapshots are never dropped, reordered or duplicated.** Losing the snapshot
makes the rest of the session unrecoverable by construction, which tests
nothing about book reconstruction and everything about arithmetic.

**Two speeds, and they are two different claims.** Paced replay divides the
capture's own inter-arrival gaps by a factor and reproduces the arrival
*shape*, which is what fault injection needs to be realistic. Unthrottled
replay reproduces nothing and answers "how fast is the code" — the replay
ceiling of ``NOTES.md`` § *Two numbers, never merged*, which is never reported
as a capture rate.
"""

from __future__ import annotations

import random
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass

from data_pipeline_core import RunContext, Source, raw_landing_source

from collector.capture import CaptureRecord

FAULT_NAMES = ("drop", "reorder", "duplicate", "clock_jitter", "burst")

# Rates a named fault turns on. Deliberately coarse: they exist so a run can be
# asked for from the environment, and a test that wants a specific rate builds
# a ``FaultConfig`` directly.
_DEFAULT_DROP = 0.02
_DEFAULT_DROP_RUN = 3
_DEFAULT_REORDER = 0.02
_DEFAULT_DUPLICATE = 0.02
_DEFAULT_JITTER_NS = 5_000_000
_DEFAULT_BURST = 0.1


@dataclass(frozen=True, slots=True)
class FaultConfig:
    """What to break, how often, and the seed that makes it reproducible."""

    seed: int = 0
    # Probability that a frame begins a dropped run, and how long that run is.
    # A run rather than a single frame because a real loss is bursty, and
    # because one dropped frame and ten are the same gap to the chain rule.
    drop: float = 0.0
    drop_run: int = _DEFAULT_DROP_RUN
    reorder: float = 0.0
    duplicate: float = 0.0
    # Uniform ± perturbation of ``receive_ts`` only. Never ``exchange_ts`` and
    # never a venue id: the fault is *our* clock being wrong, and the book must
    # not notice, because nothing in it may sequence by clock.
    clock_jitter_ns: int = 0
    # Probability of collapsing one inter-arrival gap to zero. Handled by the
    # pacer, not the injector — it changes arrival timing and no record
    # content, so it is meaningless in an unthrottled replay, which is already
    # maximal burst.
    burst: float = 0.0

    @classmethod
    def from_names(cls, names: str, *, seed: int) -> FaultConfig | None:
        """Build a config from a comma-separated list, or None if empty."""
        wanted = {name.strip() for name in names.split(",") if name.strip()}
        if not wanted:
            return None
        unknown = wanted - set(FAULT_NAMES)
        if unknown:
            raise ValueError(f"unknown fault(s) {sorted(unknown)}, want {FAULT_NAMES}")
        return cls(
            seed=seed,
            drop=_DEFAULT_DROP if "drop" in wanted else 0.0,
            reorder=_DEFAULT_REORDER if "reorder" in wanted else 0.0,
            duplicate=_DEFAULT_DUPLICATE if "duplicate" in wanted else 0.0,
            clock_jitter_ns=_DEFAULT_JITTER_NS if "clock_jitter" in wanted else 0,
            burst=_DEFAULT_BURST if "burst" in wanted else 0.0,
        )


class FaultInjector:
    """Breaks a record stream, and keeps the denominator of what it broke."""

    def __init__(self, config: FaultConfig) -> None:
        self._config = config
        self._random = random.Random(config.seed)
        # (fault, capture seq) per injection. Oracle 3's break rate needs a
        # denominator that came from the injector rather than from the
        # detector, or it is grading its own homework. Clock jitter is counted
        # apart from it on purpose: it is a perturbation nothing is *supposed*
        # to detect, so putting it in the denominator would guarantee a
        # detection rate below 100% and make the number meaningless.
        self.injected: list[tuple[str, int]] = []
        self.jittered = 0

    def __call__(self, records: Iterable[CaptureRecord]) -> Iterator[CaptureRecord]:
        config = self._config
        source = iter(records)
        held: CaptureRecord | None = None
        while True:
            record = held if held is not None else next(source, None)
            held = None
            if record is None:
                return
            if record["kind"] != "frame":
                yield record
                continue

            if config.drop and self._random.random() < config.drop:
                self.injected.append(("drop", record["seq"]))
                held = self._skip(source, config.drop_run - 1)
                continue

            if config.reorder and self._random.random() < config.reorder:
                following = next(source, None)
                if following is not None and following["kind"] == "frame":
                    self.injected.append(("reorder", record["seq"]))
                    yield self._jitter(following)
                    yield self._jitter(record)
                    continue
                held = following

            if config.duplicate and self._random.random() < config.duplicate:
                self.injected.append(("duplicate", record["seq"]))
                yield self._jitter(record)

            yield self._jitter(record)

    def _skip(
        self, source: Iterator[CaptureRecord], count: int
    ) -> CaptureRecord | None:
        """Discard up to ``count`` more frames; hand back a snapshot untouched."""
        for _ in range(count):
            record = next(source, None)
            if record is None or record["kind"] != "frame":
                return record
        return None

    def _jitter(self, record: CaptureRecord) -> CaptureRecord:
        jitter = self._config.clock_jitter_ns
        if not jitter:
            return record
        self.jittered += 1
        offset = self._random.randint(-jitter, jitter)
        return CaptureRecord(
            stream=record["stream"],
            kind=record["kind"],
            seq=record["seq"],
            receive_ts=record["receive_ts"] + offset,
            monotonic_ts=record["monotonic_ts"],
            payload=record["payload"],
        )


def pace(
    records: Iterable[CaptureRecord],
    *,
    speed: float,
    burst: float = 0.0,
    seed: int = 0,
) -> Iterator[CaptureRecord]:
    """Reproduce the capture's arrival shape, divided by ``speed``.

    The gaps come from ``monotonic_ts``, which is the only clock in the record
    that cannot step. ``burst`` collapses a gap to zero without touching the
    record, so a burst run and a clean run land byte-identical rows.
    """
    chance = random.Random(seed)
    previous: int | None = None
    for record in records:
        arrived = record["monotonic_ts"]
        if previous is not None:
            delay = (arrived - previous) / speed / 1e9
            if delay > 0 and not (burst and chance.random() < burst):
                time.sleep(delay)
        previous = arrived
        yield record


class ReplaySource:
    """Reads a landed capture back, through the SDK's own raw-landing reader."""

    name = "replay"

    def __init__(
        self,
        *,
        channel: str,
        bucket_url: str | None = None,
        speed: float | None = None,
        faults: FaultConfig | None = None,
    ) -> None:
        self._reader: Source[CaptureRecord] = raw_landing_source(
            channel, bucket_url=bucket_url
        )
        self._speed = speed
        self._faults = faults
        self.injector = FaultInjector(faults) if faults is not None else None

    def fetch(self, ctx: RunContext) -> Iterator[CaptureRecord]:
        records: Iterable[CaptureRecord] = self._reader.fetch(ctx)
        if self.injector is not None:
            records = self.injector(records)
        if self._speed is not None:
            records = pace(
                records,
                speed=self._speed,
                burst=self._faults.burst if self._faults else 0.0,
                seed=self._faults.seed if self._faults else 0,
            )
        ctx.logger.info(
            "replaying",
            speed="max" if self._speed is None else self._speed,
            faults=None if self._faults is None else asdict(self._faults),
        )
        for record in records:
            if ctx.should_stop():
                return
            yield record
