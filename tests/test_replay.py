"""Replay: cold start from disk, and byte-reproducibility.

The claim under test is the one the whole harness rests on — that a landed
capture is a sufficient description of a session, so replaying it rebuilds the
same book with no network and no clock reads. If that is false, every number
the replay ceiling produces describes something other than the collector.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.binance import BinanceAdapter
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.replay import FaultConfig, ReplaySource
from collector.transform import BookTransform
from tests.conftest import synthetic_capture

CHANNEL = "test-depth"


def land(records: list[CaptureRecord], tmp_path: Path) -> str:
    """Write a capture where ``raw_landing_source`` expects to find one."""
    directory = tmp_path / CHANNEL
    directory.mkdir(parents=True, exist_ok=True)
    lines = "".join(json.dumps(record) + "\n" for record in records)
    (directory / "capture.jsonl").write_text(lines)
    return f"file://{tmp_path}"


def replay(
    bucket_url: str, *, faults: FaultConfig | None = None
) -> tuple[list[LevelRow], BookTransform, ReplaySource]:
    source = ReplaySource(channel=CHANNEL, bucket_url=bucket_url, faults=faults)
    transform = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    rows = [row for r in source.fetch(ctx) for row in transform.transform(r, ctx)]
    return rows, transform, source


def test_a_landed_capture_replays_cold_with_no_network(tmp_path: Path) -> None:
    records = synthetic_capture(count=40, snapshot_every=10)
    rows, transform, _ = replay(land(records, tmp_path))

    # ``http`` is None above: any I/O at all would have raised. The book is
    # built entirely from what the capture carried.
    assert transform.bootstraps >= 1
    assert transform.gaps == 0
    assert transform.frames == 40
    bid, ask = transform.book.best_bid_ask()
    assert bid is not None and ask is not None and bid < ask
    assert rows


def test_replay_matches_the_same_records_in_memory(tmp_path: Path) -> None:
    """Going through the SDK's raw landing changes nothing about the result."""
    records = synthetic_capture(count=40, snapshot_every=10)

    direct = BookTransform(symbol="BTCUSDT", adapter=BinanceAdapter())
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    expected = [row for r in records for row in direct.transform(r, ctx)]

    landed, _, _ = replay(land(records, tmp_path))
    assert landed == expected


def test_the_same_seed_replays_byte_identical(tmp_path: Path) -> None:
    bucket = land(synthetic_capture(count=60, snapshot_every=10), tmp_path)
    faults = FaultConfig(seed=7, drop=0.1, reorder=0.1, duplicate=0.1)

    first, _, first_source = replay(bucket, faults=faults)
    second, _, second_source = replay(bucket, faults=faults)

    assert first == second
    assert first_source.injector is not None
    assert second_source.injector is not None
    assert first_source.injector.injected == second_source.injector.injected
    # Faults that never fire would make the equality above vacuous.
    assert first_source.injector.injected


def test_a_different_seed_replays_differently(tmp_path: Path) -> None:
    bucket = land(synthetic_capture(count=60, snapshot_every=10), tmp_path)
    first, _, _ = replay(
        bucket, faults=FaultConfig(seed=7, drop=0.1, reorder=0.1, duplicate=0.1)
    )
    second, _, _ = replay(
        bucket, faults=FaultConfig(seed=8, drop=0.1, reorder=0.1, duplicate=0.1)
    )

    assert first != second
