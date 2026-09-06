"""The book transform, driven off the recorded session as capture records.

These were ``test_source.py`` before the book moved out of the source. They
assert the *shape* of what lands — which is exactly what a replay depends on,
and now they are driven through the same door a replay uses.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from itertools import groupby
from typing import Any, cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.binance import BinanceAdapter
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.transform import BookTransform
from tests.conftest import capture


def run(records: list[CaptureRecord]) -> tuple[list[LevelRow], BookTransform]:
    transform = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    rows = [row for record in records for row in transform.transform(record, ctx)]
    return rows, transform


def action_blocks(rows: list[LevelRow]) -> list[str]:
    """Collapse the row stream to the order its record kinds appeared in."""
    kinds: Iterator[str] = (
        "level" if row["action"] in ("set", "delete") else row["action"] for row in rows
    )
    return [kind for kind, _ in groupby(kinds)]


def test_bootstrap_lands_the_snapshot_then_applies_from_the_straddler(
    frames: list[str], snapshot_payload: dict[str, Any]
) -> None:
    rows, _ = run(capture(snapshots={0: snapshot_payload}, frames=frames))

    assert action_blocks(rows) == ["snapshot", "level"]

    snapshot_rows = [row for row in rows if row["action"] == "snapshot"]
    assert len(snapshot_rows) == len(snapshot_payload["bids"]) + len(
        snapshot_payload["asks"]
    )
    assert {row["seq"] for row in snapshot_rows} == {snapshot_payload["lastUpdateId"]}
    # Binance sends no clock with a REST snapshot; the column stays null rather
    # than borrowing our own receive time and calling it the venue's.
    assert all(row["exchange_ts"] is None for row in snapshot_rows)

    # Only the straddler and beyond are applied. In this recording that is the
    # last frame, so exactly one frame's worth of levels follows.
    applied = {row["seq"] for row in rows if row["action"] in ("set", "delete")}
    assert applied == {json.loads(frames[-1])["u"]}


def test_a_gap_lands_its_control_row_before_the_repair(
    frames: list[str], snapshot_payload: dict[str, Any]
) -> None:
    # Bootstrap off the recording, then replay an old frame: its U no longer
    # chains, so the sequence is broken. The capture carries the second
    # snapshot the source would have fetched on the same condition.
    replayed = json.loads(frames[5])
    resume_at = json.loads(frames[6])
    second = {**snapshot_payload, "lastUpdateId": resume_at["U"] - 1}
    replayed_session = [*frames, frames[5], *frames[6:]]

    rows, transform = run(
        capture(
            snapshots={0: snapshot_payload, len(frames) + 1: second},
            frames=replayed_session,
        )
    )

    # The untrusted interval has both edges in the data: the gap opens it and
    # the re-snapshot closes it. Convergence is not measurable otherwise.
    assert action_blocks(rows) == ["snapshot", "level", "gap", "snapshot", "level"]
    assert transform.gaps == 1
    assert transform.bootstraps == 2

    gap_rows = [row for row in rows if row["action"] == "gap"]
    assert len(gap_rows) == 1
    gap = gap_rows[0]
    assert gap["seq"] == replayed["U"]
    # A control record describes no price level.
    assert gap["side"] is None
    assert gap["price_str"] is None
    assert gap["price_ticks"] is None
    assert gap["size_str"] is None
    assert gap["size_lots"] is None


def test_a_stale_snapshot_waits_for_the_next_one_rather_than_bootstrapping(
    frames: list[str], snapshot_payload: dict[str, Any]
) -> None:
    # A snapshot from before the stream starts has already lost updates, so no
    # amount of further buffering fixes it. The transform cannot fetch — it
    # waits for the source's refetch to arrive on the stream.
    stale = {**snapshot_payload, "lastUpdateId": json.loads(frames[0])["U"] - 5}

    stalled, transform = run(capture(snapshots={0: stale}, frames=frames))
    assert stalled == []
    assert transform.bootstraps == 0

    rows, transform = run(
        capture(snapshots={0: stale, 1: snapshot_payload}, frames=frames)
    )
    assert action_blocks(rows) == ["snapshot", "level"]
    assert transform.bootstraps == 1


def test_every_row_carries_both_forms_of_price_and_size(
    frames: list[str], snapshot_payload: dict[str, Any]
) -> None:
    rows, _ = run(capture(snapshots={0: snapshot_payload}, frames=frames))

    for row in rows:
        if row["action"] == "gap":
            continue
        assert isinstance(row["price_str"], str)
        assert isinstance(row["price_ticks"], int)
        assert isinstance(row["size_str"], str)
        assert isinstance(row["size_lots"], int)
        # A delete is signalled by zero, and only by zero.
        assert (row["action"] == "delete") == (row["size_lots"] == 0)


def test_the_rows_carry_the_capture_clocks_not_the_wall_clock(
    frames: list[str], snapshot_payload: dict[str, Any]
) -> None:
    """The whole of byte-reproducibility: no clock is read during processing."""
    records = capture(snapshots={0: snapshot_payload}, frames=frames)
    rows, _ = run(records)

    captured = {record["receive_ts"] for record in records}
    assert {row["receive_ts"] for row in rows} <= captured
