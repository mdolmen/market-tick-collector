"""The bootstrap splice and the gap rule, against a recorded Binance bootstrap.

The fixtures are a real session: twelve consecutive ``depthUpdate`` frames with
a live ``GET /api/v3/depth`` taken part-way through, exactly as the collector
does it. The recorded snapshot happens to have landed on a frame boundary, so
the interior-straddle case moves the snapshot position within the same recorded
frames rather than inventing new ones — the ranges stay real either way.
"""

from __future__ import annotations

import json
from itertools import pairwise
from pathlib import Path

import pytest

from collector.adapters.binance import (
    BinanceDepthAdapter,
    DepthEvent,
    Snapshot,
    SpliceOutcome,
    parse_event,
    parse_snapshot,
    splice,
)

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def frames() -> list[DepthEvent]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    return [
        parse_event(json.loads(line), receive_ts=index, monotonic_ts=index)
        for index, line in enumerate(lines)
        if line.strip()
    ]


@pytest.fixture(scope="module")
def snapshot() -> Snapshot:
    payload = json.loads((_FIXTURES / "binance_depth_snapshot.json").read_text())
    return parse_snapshot(payload, receive_ts=0, monotonic_ts=0)


def test_the_recording_is_a_contiguous_chain(frames: list[DepthEvent]) -> None:
    # If this fails the fixture is not a clean capture and every other
    # assertion below is measuring the wrong thing.
    for previous, event in pairwise(frames):
        assert event.first_id == previous.final_id + 1


# --- branch 1 + 2: discard the past, start from the straddler ----------------


def test_ready_discards_the_past_and_starts_at_the_straddler(
    frames: list[DepthEvent], snapshot: Snapshot
) -> None:
    result = splice(frames, snapshot.last_update_id)

    assert result.outcome is SpliceOutcome.READY
    straddler = frames[result.start]
    assert straddler.first_id <= snapshot.last_update_id + 1 <= straddler.final_id
    # Everything before it is entirely in the past and was discarded.
    assert all(e.final_id <= snapshot.last_update_id for e in frames[: result.start])


def test_ready_when_the_snapshot_lands_inside_a_frame(
    frames: list[DepthEvent],
) -> None:
    # A frame batches several internal updates, so the snapshot position
    # normally falls *within* a range rather than on its edge. Here: strictly
    # inside the last recorded frame.
    inside = frames[-1]
    last_update_id = (inside.first_id + inside.final_id) // 2
    assert inside.first_id < last_update_id < inside.final_id

    result = splice(frames, last_update_id)

    assert result.outcome is SpliceOutcome.READY
    assert frames[result.start] is inside


# --- branch 3: the buffer starts ahead of the snapshot ----------------------


def test_snapshot_too_old_when_the_buffer_starts_ahead(
    frames: list[DepthEvent],
) -> None:
    # Updates between the snapshot and the first buffered frame are already
    # lost, so no amount of further buffering helps: refetch the snapshot.
    result = splice(frames, frames[0].first_id - 2)

    assert result.outcome is SpliceOutcome.SNAPSHOT_TOO_OLD


def test_a_snapshot_exactly_one_before_the_buffer_is_not_too_old(
    frames: list[DepthEvent],
) -> None:
    # The boundary: S + 1 == U means the first frame continues the snapshot
    # with nothing missing between them.
    result = splice(frames, frames[0].first_id - 1)

    assert result.outcome is SpliceOutcome.READY
    assert result.start == 0


# --- branch 4: the buffer has not reached the snapshot yet ------------------


def test_buffer_behind_when_every_frame_is_in_the_past(
    frames: list[DepthEvent],
) -> None:
    # The snapshot is fine; the buffer simply has not caught up. Refetching
    # here is what makes a bootstrap loop spin.
    result = splice(frames, frames[-1].final_id)

    assert result.outcome is SpliceOutcome.BUFFER_BEHIND


def test_buffer_behind_on_an_empty_buffer() -> None:
    assert splice([], 1).outcome is SpliceOutcome.BUFFER_BEHIND


# --- the gap rule: U == prev_u + 1 -----------------------------------------


def test_consecutive_frames_are_in_sequence(frames: list[DepthEvent]) -> None:
    adapter = BinanceDepthAdapter()
    adapter.bootstrapped()

    for event in frames:
        assert not adapter.gap_detected(event)
        adapter.accept(event)

    assert not adapter.snapshot_required()


def test_a_skipped_frame_is_a_gap_and_latches_snapshot_required(
    frames: list[DepthEvent],
) -> None:
    adapter = BinanceDepthAdapter()
    adapter.bootstrapped()
    adapter.accept(frames[0])

    assert adapter.gap_detected(frames[2])
    assert adapter.snapshot_required()


def test_snapshot_required_stays_latched_past_the_frame_that_broke_the_chain(
    frames: list[DepthEvent],
) -> None:
    adapter = BinanceDepthAdapter()
    adapter.bootstrapped()
    adapter.accept(frames[0])
    adapter.gap_detected(frames[2])
    adapter.accept(frames[2])

    # frames[3] chains onto frames[2] perfectly, so it is *in sequence* — but
    # the book is still untrusted until a fresh snapshot is spliced in.
    assert not adapter.gap_detected(frames[3])
    assert adapter.snapshot_required()


def test_a_fresh_adapter_requires_a_snapshot() -> None:
    assert BinanceDepthAdapter().snapshot_required()


def test_the_straddler_is_not_judged_by_the_chain_rule(
    frames: list[DepthEvent],
) -> None:
    # The splice validated it, and its U legitimately starts before S + 1, so
    # the chain rule would reject the one event already proven correct.
    adapter = BinanceDepthAdapter()
    adapter.bootstrapped()

    assert not adapter.gap_detected(frames[5])
    assert not adapter.snapshot_required()
