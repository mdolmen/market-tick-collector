"""The frame source: what it lands, and when it decides to fetch a snapshot.

The source builds no book, so there is nothing here about levels. What it owes
the rest of the pipeline is a capture that can be replayed *cold* — which means
a snapshot at every point one is needed, because the transform does no I/O and
can never ask for one itself.
"""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from typing import Any, cast

import pytest
from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector import source as source_module
from collector.adapters import binance
from collector.adapters.binance import BinanceAdapter
from collector.capture import CaptureRecord, payload_of
from collector.source import FrameSource


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
    symbols: Sequence[str] = ("BTCUSDT",),
) -> tuple[list[CaptureRecord], _FakeHttp]:
    socket = _FakeSocket(messages)
    monkeypatch.setattr(
        source_module, "connect", lambda *args, **kwargs: socket, raising=True
    )
    http = _FakeHttp(payloads)
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, http))
    source = FrameSource(
        venue=lambda: BinanceAdapter(
            depth_interval_ms=100,
            snapshot_limit=20,
            ws_url="wss://example.invalid/ws",
            rest_url="https://example.invalid/depth",
        ),
        symbols=symbols,
        duration_s=duration_s,
    )
    return list(source.fetch(ctx)), http


class _QuietVenue(BinanceAdapter):
    """An in-band venue whose every message is control — a rejected ack.

    Subclassed rather than written out because the guard under test is
    venue-neutral: what matters is that nothing classifies as a book message,
    not which venue failed to send one.
    """

    def classify(self, stream: str, payload: Any) -> Any:
        return "control"

    def snapshot_request(self, symbol: str) -> Any:
        return None

    def sequence_ids(self, payload: Any) -> tuple[int, int]:
        return 0, 0


def _kinds(records: list[CaptureRecord]) -> list[str]:
    return [record["kind"] for record in records]


def test_the_snapshot_follows_the_first_frame_and_every_frame_lands_verbatim(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    """Socket first, snapshot second — and in Phase 3 that is literal.

    Phase 0 fetched before the read loop, which honoured the same rule because
    the subscription was already open. A shard cannot: fetching for a hundred
    symbols before reading leaves the socket unattended through a hundred
    retrying HTTP calls. So the fetch is triggered by each symbol's own first
    frame instead, which is the same guarantee arrived at one symbol at a time
    — the frame is buffered, the snapshot lands behind it, and the transform
    splices exactly as before.
    """
    records, _ = _run(monkeypatch, frames, [snapshot_payload])

    assert _kinds(records) == ["frame", "snapshot", *["frame"] * (len(frames) - 1)]
    assert records[1]["stream"] == binance.rest_depth_stream("BTCUSDT")
    assert {r["stream"] for i, r in enumerate(records) if i != 1} == {
        binance.stream_name("BTCUSDT", 100)
    }

    # Verbatim: the payload round-trips to the same object the venue sent, and
    # the frames are in arrival order with a contiguous capture seq.
    landed = [r for i, r in enumerate(records) if i != 1]
    assert [payload_of(r) for r in landed] == [json.loads(t) for t in frames]
    assert [record["seq"] for record in records] == list(range(1, len(records) + 1))


def test_a_shard_tags_every_symbols_snapshot_apart(
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    snapshot_payload: dict[str, Any],
) -> None:
    """Two symbols on one socket, and two distinguishable REST snapshots.

    The tag was a single constant until Phase 3, which was enough while a
    connection carried one book. It is not enough now: a replay reading a
    shard's capture has to know which book each snapshot describes, and the
    only thing on the record that can say so is `stream`.
    """
    eth = [json.dumps({**json.loads(text), "s": "ETHUSDT"}) for text in frames[:3]]
    records, http = _run(
        monkeypatch,
        [*frames[:3], *eth],
        [snapshot_payload, snapshot_payload],
        symbols=("BTCUSDT", "ETHUSDT"),
    )

    assert http.calls == 2, "one initial snapshot per symbol, not one per shard"
    snapshots = [r["stream"] for r in records if r["kind"] == "snapshot"]
    assert snapshots == [
        binance.rest_depth_stream("BTCUSDT"),
        binance.rest_depth_stream("ETHUSDT"),
    ]
    assert {r["stream"] for r in records if r["kind"] == "frame"} == {
        binance.stream_name("BTCUSDT", 100),
        binance.stream_name("ETHUSDT", 100),
    }


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
    assert _kinds(records)[:4] == ["frame", "snapshot", "frame", "snapshot"]


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
        # frames[0] lands, then the initial snapshot it triggered.
        "frame",
        "snapshot",
        *["frame"] * 4,
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


def test_a_rejected_subscription_fails_the_run_instead_of_going_quiet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Phase 2.5 failure, as an assertion.

    Kraken was subscribed to `XBT/USD` — the spelling `collector/symbols.py`
    had committed — and answered `success: false` on a socket that stayed open
    and silent. The run connected, landed the ack, reported `frames: 0` and
    exited zero. A capture with no book message in it is not a quiet market,
    it is a broken subscription, and it has to say so.
    """
    socket = _FakeSocket([json.dumps({"result": {}, "error": "nope"})])
    monkeypatch.setattr(
        source_module, "connect", lambda *args, **kwargs: socket, raising=True
    )
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, _FakeHttp([])))
    source = FrameSource(
        venue=_QuietVenue,
        symbols=["XBT/USD"],
        duration_s=0.2,
    )

    with pytest.raises(RuntimeError, match="no book message"):
        list(source.fetch(ctx))
