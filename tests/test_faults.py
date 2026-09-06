"""Fault injection: what each fault is supposed to prove, asserted.

The faults are the only way to reach the recovery path — a venue will not drop
packets on request — so these are the tests Phase 6's convergence claim and
Phase 8's Oracle 3 will both be built on. Each one names the property it
establishes, because a fault asserting only "something changed" tests the
injector rather than the book.

Two things learned building them, both worth stating rather than working
around:

**A fault that lands on an already-untrusted book opens no new interval.** So
the honest denominator for a detection rate is faults injected against a
*trusted* book, not faults injected. These tests place one fault at a known
position with the book live, which measures that exactly; the randomised
injector is exercised for its own mechanics further down.

**A recording cannot conjure the repair snapshot a live source would have
fetched.** That is why the captures carry periodic snapshots, and why the
source grew ``snapshot_interval_s``.
"""

from __future__ import annotations

from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.binance import BinanceAdapter
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.replay import FaultConfig, FaultInjector
from collector.transform import BookTransform
from tests.conftest import synthetic_capture

# A session long enough that a fault can sit well inside a live stretch, with
# snapshots often enough that a repair arrives before the session ends.
_FRAMES = 60
_SNAPSHOT_EVERY = 10
# A frame index comfortably after a snapshot and before the next: the book is
# live here, which is the only state in which a fault is detectable at all.
_TARGET = 22


def _session() -> list[CaptureRecord]:
    return synthetic_capture(count=_FRAMES, snapshot_every=_SNAPSHOT_EVERY)


def _frame_positions(records: list[CaptureRecord]) -> list[int]:
    return [i for i, record in enumerate(records) if record["kind"] == "frame"]


def _at(records: list[CaptureRecord], frame_index: int) -> int:
    """Position in the record list of the nth *frame*."""
    return _frame_positions(records)[frame_index]


def _run(records: list[CaptureRecord]) -> tuple[list[LevelRow], BookTransform]:
    transform = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    rows = [row for record in records for row in transform.transform(record, ctx)]
    return rows, transform


def _book(transform: BookTransform) -> tuple[dict[int, int], dict[int, int]]:
    return dict(transform.book.bids), dict(transform.book.asks)


def _gap_rows(rows: list[LevelRow]) -> list[LevelRow]:
    return [row for row in rows if row["action"] == "gap"]


# --- the clean baseline -----------------------------------------------------


def test_a_clean_session_bootstraps_once_and_never_gaps() -> None:
    """Periodic snapshots must not disturb a healthy book, or nothing below
    can distinguish a repair from routine noise."""
    rows, clean = _run(_session())

    assert clean.gaps == 0
    assert clean.bootstraps == 1
    assert clean.live
    assert _gap_rows(rows) == []
    # One snapshot's rows, not one per periodic snapshot.
    assert sum(row["action"] == "snapshot" for row in rows) == 2


# --- one fault, placed, with the book live ----------------------------------


def test_a_dropped_run_is_detected_and_the_book_reconverges() -> None:
    """The gap machinery is the drop machinery — one path, exercised here.

    ``TODO.md`` lists "dropped frames" and "sequence gaps" as separate faults.
    On Binance they are one mechanism: a removed frame *is* a chain break.
    """
    clean_rows, clean = _run(_session())

    records = _session()
    start = _at(records, _TARGET)
    faulted_records = records[:start] + records[start + 2 :]
    rows, faulted = _run(faulted_records)

    # Detected: exactly one untrusted interval, opened in the landed data.
    assert faulted.gaps == 1
    assert len(_gap_rows(rows)) == 1
    # Repaired: the next periodic snapshot closed it, so the interval has both
    # edges in the data and convergence is measurable.
    assert faulted.bootstraps == clean.bootstraps + 1
    # Converged: the interval closed, and the book ends where the clean replay
    # ends. Both halves are needed — a run that ends untrusted has a book that
    # says where it stopped, not whether it recovers.
    assert faulted.live
    assert faulted.untrusted_frames > clean.untrusted_frames
    assert _book(faulted) == _book(clean)
    assert rows != clean_rows


def test_a_reorder_breaks_the_chain_and_recovers() -> None:
    records = _session()
    first = _at(records, _TARGET)
    second = _at(records, _TARGET + 1)
    records[first], records[second] = records[second], records[first]

    _, clean = _run(_session())
    rows, faulted = _run(records)

    assert faulted.gaps == 1
    assert len(_gap_rows(rows)) == 1
    assert _book(faulted) == _book(clean)


def test_a_duplicate_frame_is_reported_as_a_gap() -> None:
    """A finding, not a feature — the prediction is in ``DEVELOPMENT.md``.

    ``in_sequence`` is ``U == prev_u + 1``, so a redelivered frame — whose
    ``U`` is at or behind the cursor — fails the chain rule exactly the way a
    genuine loss does. Venues do redeliver, so this is a full re-bootstrap for
    a book that was never wrong: the recovery is unnecessary, not incorrect.
    Pinned here so that changing it is a decision rather than an accident.
    """
    records = _session()
    position = _at(records, _TARGET)
    records.insert(position + 1, records[position])

    _, clean = _run(_session())
    rows, faulted = _run(records)

    assert faulted.gaps == 1
    assert len(_gap_rows(rows)) == 1
    # Wrong for the right reason: the book stays correct throughout, because
    # the repair fixes something that was not broken.
    assert _book(faulted) == _book(clean)


def test_clock_jitter_leaves_the_book_untouched() -> None:
    """Nothing sequences by clock, and this is what says so.

    The rows differ — ``receive_ts`` is a column — but no venue id moved, so
    the book, the sequencing and every price level are identical. A collector
    that ordered by arrival time would fail here and nowhere else.
    """
    clean_rows, clean = _run(_session())

    injector = FaultInjector(FaultConfig(seed=11, clock_jitter_ns=50_000_000))
    rows, faulted = _run(list(injector(_session())))

    assert injector.jittered > 0
    # Jitter is not in the detectable denominator: nothing is meant to catch it.
    assert injector.injected == []

    assert faulted.gaps == 0
    assert _book(faulted) == _book(clean)
    assert len(rows) == len(clean_rows)
    for faulty, clean_row in zip(rows, clean_rows, strict=True):
        assert {k: v for k, v in faulty.items() if k != "receive_ts"} == {
            k: v for k, v in clean_row.items() if k != "receive_ts"
        }
    assert rows != clean_rows


# --- the randomised injector's own mechanics --------------------------------


def test_every_frame_is_faulted_at_a_rate_of_one() -> None:
    """The rates are real probabilities, so 1.0 has to hit every frame."""
    records = _session()
    frames = len(_frame_positions(records))

    injector = FaultInjector(FaultConfig(seed=1, duplicate=1.0))
    out = list(injector(records))

    assert len(injector.injected) == frames
    assert len(_frame_positions(out)) == frames * 2


def test_snapshots_are_never_dropped_reordered_or_duplicated() -> None:
    """Losing the snapshot tests arithmetic, not book reconstruction."""
    records = _session()
    snapshots = [r for r in records if r["kind"] == "snapshot"]

    injector = FaultInjector(
        FaultConfig(seed=1, drop=0.5, drop_run=4, reorder=0.5, duplicate=0.5)
    )
    survived = [r for r in injector(records) if r["kind"] == "snapshot"]

    assert survived == snapshots


def test_a_randomised_run_detects_every_fault_it_lands_on_a_live_book() -> None:
    """Oracle 3's shape, with the denominator stated honestly.

    Not every injected fault is detectable: one landing on an already-untrusted
    book opens no second interval. So the claim is bounded on both sides —
    every gap traces to a fault, and no live-book fault passes unnoticed.
    """
    injector = FaultInjector(FaultConfig(seed=3, drop=0.1, drop_run=2))
    rows, faulted = _run(list(injector(_session())))

    assert injector.injected, "the fault never fired; the test would be vacuous"
    assert 0 < faulted.gaps <= len(injector.injected)
    assert len(_gap_rows(rows)) == faulted.gaps
