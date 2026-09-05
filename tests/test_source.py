"""The source's orchestration, driven off the recorded session.

What a live run does not exercise: a sequence gap. Binance does not drop frames
on request, so the recovery path — mark untrusted, re-snapshot, converge — is
only reachable here until the Phase 1 fault injector exists. These tests assert
the *shape* of what lands, which is what replay later depends on.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from itertools import groupby
from pathlib import Path
from typing import Any, cast

import pytest
from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector import source as source_module
from collector.model import LevelRow
from collector.source import BinanceDepthSource

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def frames() -> list[str]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    return [line for line in lines if line.strip()]


@pytest.fixture(scope="module")
def snapshot_payload() -> dict[str, Any]:
    text = (_FIXTURES / "binance_depth_snapshot.json").read_text()
    return cast(dict[str, Any], json.loads(text))


class _FakeSocket:
    """Replays recorded frames, then behaves like a silent-but-open socket."""

    def __init__(self, messages: list[str]) -> None:
        self._messages = list(messages)
        self._index = 0

    def recv(self, timeout: float | None = None) -> str:
        if self._index >= len(self._messages):
            # Nothing more to say. Burn the caller's timeout the way a real
            # quiet socket would, so the run reaches its deadline instead of
            # spinning.
            time.sleep(timeout or 0)
            raise TimeoutError
        message = self._messages[self._index]
        self._index += 1
        return message

    def __enter__(self) -> _FakeSocket:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _FakeResponse:
    status_code = 200

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeHttp:
    """Serves one snapshot payload per call, repeating the last one."""

    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self._payloads = payloads
        self.calls = 0

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        payload = self._payloads[min(self.calls, len(self._payloads) - 1)]
        self.calls += 1
        return _FakeResponse(payload)


def _run(
    monkeypatch: pytest.MonkeyPatch,
    messages: list[str],
    payloads: list[dict[str, Any]],
    duration_s: float = 0.2,
) -> list[LevelRow]:
    socket = _FakeSocket(messages)
    monkeypatch.setattr(
        source_module, "connect", lambda *args, **kwargs: socket, raising=True
    )
    http = _FakeHttp(payloads)
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, http))
    source = BinanceDepthSource(
        symbol="BTCUSDT",
        duration_s=duration_s,
        ws_url="wss://example.invalid/ws",
        rest_url="https://example.invalid/depth",
        snapshot_limit=20,
        depth_interval_ms=100,
    )
    return list(source.fetch(ctx))


def _action_blocks(rows: list[LevelRow]) -> list[str]:
    """Collapse the row stream to the order its record kinds appeared in."""
    kinds: Iterator[str] = (
        "level" if row["action"] in ("set", "delete") else row["action"] for row in rows
    )
    return [kind for kind, _ in groupby(kinds)]


def test_bootstrap_lands_the_snapshot_then_applies_from_the_straddler(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    rows = _run(monkeypatch, frames, [snapshot_payload])

    assert _action_blocks(rows) == ["snapshot", "level"]

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
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    # Bootstrap off the recording, then replay an old frame: its U no longer
    # chains, so the sequence is broken.
    replayed = json.loads(frames[5])
    resume_at = json.loads(frames[6])
    second_snapshot = {**snapshot_payload, "lastUpdateId": resume_at["U"] - 1}

    rows = _run(
        monkeypatch,
        [*frames, frames[5], *frames[6:]],
        [snapshot_payload, second_snapshot],
    )

    # The untrusted interval has both edges in the data: the gap opens it and
    # the re-snapshot closes it. Convergence is not measurable otherwise.
    assert _action_blocks(rows) == ["snapshot", "level", "gap", "snapshot", "level"]

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


def test_a_stale_snapshot_is_refetched_rather_than_waited_out(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    # A snapshot from before the buffer starts has already lost updates, so no
    # amount of further buffering fixes it. The second serve is usable.
    stale = {**snapshot_payload, "lastUpdateId": json.loads(frames[0])["U"] - 5}
    socket = _FakeSocket(frames)
    monkeypatch.setattr(
        source_module, "connect", lambda *args, **kwargs: socket, raising=True
    )
    http = _FakeHttp([stale, snapshot_payload])
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, http))
    source = BinanceDepthSource(
        symbol="BTCUSDT",
        duration_s=0.2,
        ws_url="wss://example.invalid/ws",
        rest_url="https://example.invalid/depth",
        snapshot_limit=20,
        depth_interval_ms=100,
    )

    rows = list(source.fetch(ctx))

    assert http.calls == 2
    assert _action_blocks(rows) == ["snapshot", "level"]


def test_every_row_carries_both_forms_of_price_and_size(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    rows = _run(monkeypatch, frames, [snapshot_payload])

    for row in rows:
        if row["action"] == "gap":
            continue
        assert isinstance(row["price_str"], str)
        assert isinstance(row["price_ticks"], int)
        assert isinstance(row["size_str"], str)
        assert isinstance(row["size_lots"], int)
        # A delete is signalled by zero, and only by zero.
        assert (row["action"] == "delete") == (row["size_lots"] == 0)
