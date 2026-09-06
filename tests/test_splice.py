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

from collector.adapters.base import BootstrapOutcome, BootstrapResult, Snapshot, Update
from collector.adapters.binance import BinanceAdapter

_FIXTURES = Path(__file__).parent / "fixtures"

# Binance forgets the cursor on bootstrap, so which snapshot it is handed makes
# no difference — but the contract takes one, because Coinbase's does.
_SNAPSHOT = Snapshot(
    final_seq=0,
    exchange_ts=None,
    receive_ts=0,
    monotonic_ts=0,
    bids=(),
    asks=(),
)


def splice(buffered: list[Update], last_update_id: int) -> BootstrapResult:
    """`bootstrap` against a bare sequence id.

    The adapter takes the whole `Snapshot` because that is what the transform
    holds; these tests care only about where the id falls in the buffer, so
    they wrap it rather than build a book state they never read.
    """
    snapshot = Snapshot(
        final_seq=last_update_id,
        exchange_ts=None,
        receive_ts=0,
        monotonic_ts=0,
        bids=(),
        asks=(),
    )
    return BinanceAdapter().bootstrap(buffered, snapshot)


@pytest.fixture(scope="module")
def frames() -> list[Update]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    adapter = BinanceAdapter()
    return [
        adapter.parse_frame(json.loads(line), receive_ts=index, monotonic_ts=index)
        for index, line in enumerate(lines)
        if line.strip()
    ]


@pytest.fixture(scope="module")
def snapshot() -> Snapshot:
    payload = json.loads((_FIXTURES / "binance_depth_snapshot.json").read_text())
    return BinanceAdapter().parse_snapshot(payload, receive_ts=0, monotonic_ts=0)


def test_the_recording_is_a_contiguous_chain(frames: list[Update]) -> None:
    # If this fails the fixture is not a clean capture and every other
    # assertion below is measuring the wrong thing.
    for previous, event in pairwise(frames):
        assert event.first_seq == previous.final_seq + 1


# --- branch 1 + 2: discard the past, start from the straddler ----------------


def test_ready_discards_the_past_and_starts_at_the_straddler(
    frames: list[Update], snapshot: Snapshot
) -> None:
    result = splice(frames, snapshot.final_seq)

    assert result.outcome is BootstrapOutcome.READY
    straddler = frames[result.start]
    assert straddler.first_seq <= snapshot.final_seq + 1 <= straddler.final_seq
    # Everything before it is entirely in the past and was discarded.
    assert all(e.final_seq <= snapshot.final_seq for e in frames[: result.start])


def test_ready_when_the_snapshot_lands_inside_a_frame(
    frames: list[Update],
) -> None:
    # A frame batches several internal updates, so the snapshot position
    # normally falls *within* a range rather than on its edge. Here: strictly
    # inside the last recorded frame.
    inside = frames[-1]
    last_update_id = (inside.first_seq + inside.final_seq) // 2
    assert inside.first_seq < last_update_id < inside.final_seq

    result = splice(frames, last_update_id)

    assert result.outcome is BootstrapOutcome.READY
    assert frames[result.start] is inside


# --- branch 3: the buffer starts ahead of the snapshot ----------------------


def test_snapshot_too_old_when_the_buffer_starts_ahead(
    frames: list[Update],
) -> None:
    # Updates between the snapshot and the first buffered frame are already
    # lost, so no amount of further buffering helps: refetch the snapshot.
    result = splice(frames, frames[0].first_seq - 2)

    assert result.outcome is BootstrapOutcome.SNAPSHOT_TOO_OLD


def test_a_snapshot_exactly_one_before_the_buffer_is_not_too_old(
    frames: list[Update],
) -> None:
    # The boundary: S + 1 == U means the first frame continues the snapshot
    # with nothing missing between them.
    result = splice(frames, frames[0].first_seq - 1)

    assert result.outcome is BootstrapOutcome.READY
    assert result.start == 0


# --- branch 4: the buffer has not reached the snapshot yet ------------------


def test_buffer_behind_when_every_frame_is_in_the_past(
    frames: list[Update],
) -> None:
    # The snapshot is fine; the buffer simply has not caught up. Refetching
    # here is what makes a bootstrap loop spin.
    result = splice(frames, frames[-1].final_seq)

    assert result.outcome is BootstrapOutcome.BUFFER_BEHIND


def test_buffer_behind_on_an_empty_buffer() -> None:
    assert splice([], 1).outcome is BootstrapOutcome.BUFFER_BEHIND


# --- the gap rule: U == prev_u + 1 -----------------------------------------


def test_consecutive_frames_are_in_sequence(frames: list[Update]) -> None:
    adapter = BinanceAdapter()
    adapter.bootstrapped(_SNAPSHOT)

    for event in frames:
        assert not adapter.gap_detected(event)
        adapter.accept(event)

    assert not adapter.snapshot_required()


def test_a_skipped_frame_is_a_gap_and_latches_snapshot_required(
    frames: list[Update],
) -> None:
    adapter = BinanceAdapter()
    adapter.bootstrapped(_SNAPSHOT)
    adapter.accept(frames[0])

    assert adapter.gap_detected(frames[2])
    assert adapter.snapshot_required()


def test_snapshot_required_stays_latched_past_the_frame_that_broke_the_chain(
    frames: list[Update],
) -> None:
    adapter = BinanceAdapter()
    adapter.bootstrapped(_SNAPSHOT)
    adapter.accept(frames[0])
    adapter.gap_detected(frames[2])
    adapter.accept(frames[2])

    # frames[3] chains onto frames[2] perfectly, so it is *in sequence* — but
    # the book is still untrusted until a fresh snapshot is spliced in.
    assert not adapter.gap_detected(frames[3])
    assert adapter.snapshot_required()


def test_a_fresh_adapter_requires_a_snapshot() -> None:
    assert BinanceAdapter().snapshot_required()


def test_the_straddler_is_not_judged_by_the_chain_rule(
    frames: list[Update],
) -> None:
    # The splice validated it, and its U legitimately starts before S + 1, so
    # the chain rule would reject the one event already proven correct.
    adapter = BinanceAdapter()
    adapter.bootstrapped(_SNAPSHOT)

    assert not adapter.gap_detected(frames[5])
    assert not adapter.snapshot_required()
