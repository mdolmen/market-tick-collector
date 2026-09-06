"""Shared fixtures: the recorded Binance bootstrap, in both shapes.

The fixtures are a real session — twelve consecutive ``depthUpdate`` frames
with a live ``GET /api/v3/depth`` taken part-way through, exactly as the
collector does it. Everything downstream of the split needs it as *capture
records*, so building those is here rather than copied into three test modules.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from collector.adapters import binance
from collector.capture import CaptureRecord

_FIXTURES = Path(__file__).parent / "fixtures"

STREAM = binance.stream_name("BTCUSDT", 100)

# One tick of the capture clock per record. Real captures carry real clocks;
# these only have to be monotonic and reproducible, and a fixed step makes an
# assertion about which record a fault landed on readable.
_STEP_NS = 100_000_000


@pytest.fixture(scope="session")
def frames() -> list[str]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    return [line for line in lines if line.strip()]


@pytest.fixture(scope="session")
def snapshot_payload() -> dict[str, Any]:
    text = (_FIXTURES / "binance_depth_snapshot.json").read_text()
    return cast(dict[str, Any], json.loads(text))


def capture(
    *, snapshots: dict[int, dict[str, Any]], frames: list[str]
) -> list[CaptureRecord]:
    """Build a capture: frames in order, with snapshots spliced in by position.

    ``snapshots`` maps "before frame index N" to the payload landed there, so a
    test says where a snapshot arrived rather than assembling the list by hand.
    """
    records: list[CaptureRecord] = []

    def append(kind: str, stream: str, payload: dict[str, Any] | str) -> None:
        seq = len(records) + 1
        records.append(
            CaptureRecord(
                stream=stream,
                kind=cast(Any, kind),
                seq=seq,
                receive_ts=1_700_000_000_000_000_000 + seq * _STEP_NS,
                monotonic_ts=seq * _STEP_NS,
                payload=payload if isinstance(payload, str) else json.dumps(payload),
            )
        )

    for index, text in enumerate(frames):
        if index in snapshots:
            append("snapshot", binance.REST_DEPTH_STREAM, snapshots[index])
        append("frame", STREAM, text)
    if len(frames) in snapshots:
        append("snapshot", binance.REST_DEPTH_STREAM, snapshots[len(frames)])
    return records
