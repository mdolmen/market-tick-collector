"""The frame source: what it lands, and when it decides to fetch a snapshot.

The source builds no book, so there is nothing here about levels. What it owes
the rest of the pipeline is a capture that can be replayed *cold* — which means
a snapshot at every point one is needed, because the transform does no I/O and
can never ask for one itself.
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

import pytest
from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector import source as source_module
from collector.adapters import binance
from collector.capture import CaptureRecord, payload_of
from collector.source import BinanceFrameSource


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
) -> tuple[list[CaptureRecord], _FakeHttp]:
    socket = _FakeSocket(messages)
    monkeypatch.setattr(
        source_module, "connect", lambda *args, **kwargs: socket, raising=True
    )
    http = _FakeHttp(payloads)
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, http))
    source = BinanceFrameSource(
        symbol="BTCUSDT",
        duration_s=duration_s,
        ws_url="wss://example.invalid/ws",
        rest_url="https://example.invalid/depth",
        snapshot_limit=20,
        depth_interval_ms=100,
    )
    return list(source.fetch(ctx)), http


def _kinds(records: list[CaptureRecord]) -> list[str]:
    return [record["kind"] for record in records]


def test_the_snapshot_comes_first_and_every_frame_lands_verbatim(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    records, _ = _run(monkeypatch, frames, [snapshot_payload])

    # Socket first, snapshot second — but the snapshot is the first *record*,
    # because it describes the state the frames after it build on.
    assert _kinds(records) == ["snapshot", *["frame"] * len(frames)]
    assert records[0]["stream"] == binance.REST_DEPTH_STREAM
    assert {record["stream"] for record in records[1:]} == {
        binance.stream_name("BTCUSDT", 100)
    }

    # Verbatim: the payload round-trips to the same object the venue sent, and
    # the frames are in arrival order with a contiguous capture seq.
    assert [payload_of(r) for r in records[1:]] == [json.loads(t) for t in frames]
    assert [record["seq"] for record in records] == list(range(1, len(records) + 1))


def test_a_stale_snapshot_is_refetched_rather_than_waited_out(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    # A snapshot from before the stream starts has already lost updates, so no
    # amount of further waiting fixes it.
    stale = {**snapshot_payload, "lastUpdateId": json.loads(frames[0])["U"] - 5}

    records, http = _run(monkeypatch, frames, [stale, snapshot_payload])

    assert http.calls == 2
    # The refetch lands after the frame that proved the first one too old, so
    # a replay sees the same evidence in the same order the source did.
    assert _kinds(records)[:3] == ["snapshot", "frame", "snapshot"]


def test_a_socket_skip_fetches_a_snapshot_so_the_capture_stays_replayable(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    # Drop a frame from the middle: the chain breaks, and without a fresh
    # snapshot in the capture the book downstream could never recover.
    skipped = [*frames[:5], *frames[6:]]
    # The refetch lands *after* the frame that broke the chain, so it has to be
    # current for the frame that follows it — a snapshot that is merely newer
    # than the break is still too old, and the source will keep refetching.
    resumed = {**snapshot_payload, "lastUpdateId": json.loads(frames[7])["U"] - 1}

    records, http = _run(monkeypatch, skipped, [snapshot_payload, resumed])

    assert http.calls == 2
    assert _kinds(records) == [
        "snapshot",
        *["frame"] * 5,
        # frames[6] broke the chain: it lands, then the snapshot follows it.
        "frame",
        "snapshot",
        *["frame"] * (len(skipped) - 6),
    ]


def test_a_clean_session_never_refetches(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    _, http = _run(monkeypatch, frames, [snapshot_payload])
    assert http.calls == 1
