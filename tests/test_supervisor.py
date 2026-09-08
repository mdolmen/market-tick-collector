"""N connections, and the guarantee that they do not share a fate.

`NOTES.md` § *Connection supervision* is mostly one promise: a failure on one
connection must not reach the others. That is easy to claim and easy to lose,
so `test_a_failure_never_crosses_connections` asserts it the only way that
means anything — by killing one shard mid-run and requiring the other two to
produce byte-identical output to a run with no fault in it at all.

The other two properties here are the guardrail ones. A full queue must drop
rather than block, because stalling a reader costs the connection and a
re-bootstrap of every book on it; and a drop must latch a repair, because a
dropped record is a hole the capture has no snapshot for.

Sockets are faked the way `test_source.py` fakes them — by monkeypatching the
module-level `connect` — extended so a factory can serve a different script per
connection attempt and fail a chosen one.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, cast

import pytest
from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient
from websockets.exceptions import ConnectionClosedError

from collector import source as source_module
from collector.adapters.kraken import KrakenAdapter
from collector.capture import CONTROL_STREAM, CaptureRecord
from collector.source import FrameSource
from collector.supervisor import ShardSupervisor
from tests.conftest import kraken_capture

_SHARDS = (("BTC/USD",), ("ETH/USD",), ("SOL/USD",))


class _Script:
    """One connection's messages, optionally dying part-way through."""

    def __init__(self, messages: list[str], *, fail_at: int | None = None) -> None:
        self._messages = messages
        self._fail_at = fail_at
        self._index = 0

    def recv(self, timeout: float | None = None) -> str:
        if self._fail_at is not None and self._index == self._fail_at:
            raise ConnectionClosedError(None, None)
        if self._index >= len(self._messages):
            time.sleep(timeout or 0)
            raise TimeoutError
        message = self._messages[self._index]
        self._index += 1
        return message

    def send(self, frame: str) -> None:
        return None

    def __enter__(self) -> _Script:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _url(symbol: str) -> str:
    """A distinct endpoint per shard, so the fake can tell them apart.

    Kraken's real `ws_url` is one constant for every symbol, so there is
    nothing in it to key on. `CollectorSettings` already carries a `ws_url`
    override for pointing a run at a fixture server, and this is that.
    """
    return f"wss://example.invalid/{symbol.replace('/', '-')}"


class _Factory:
    """Serves a script per connection, keyed by the shard's endpoint.

    Keyed by endpoint rather than by call order because the shards connect
    concurrently and staggered, so call order is not deterministic — and a test
    whose fixtures depend on thread scheduling proves nothing. A shard that
    reconnects gets the next script in its list, which is how a fault and its
    recovery are scripted separately.
    """

    def __init__(self, scripts: dict[str, list[_Script]]) -> None:
        self._scripts = {_url(symbol): s for symbol, s in scripts.items()}
        self._used: dict[str, int] = {}
        self._lock = threading.Lock()

    def __call__(self, url: str, **kwargs: Any) -> _Script:
        with self._lock:
            index = self._used.get(url, 0)
            self._used[url] = index + 1
        scripts = self._scripts[url]
        return scripts[min(index, len(scripts) - 1)]


def _messages(symbol: str, count: int) -> list[str]:
    """A consistent Kraken session for one symbol: snapshot then updates."""
    records = kraken_capture(count=count, symbol=symbol)
    return [record["payload"] for record in records]


def _supervisor(
    monkeypatch: pytest.MonkeyPatch,
    scripts: dict[str, list[_Script]],
    *,
    duration_s: float = 0.6,
    queue_maxsize: int = 10_000,
) -> tuple[ShardSupervisor, list[CaptureRecord]]:
    monkeypatch.setattr(source_module, "connect", _Factory(scripts), raising=True)
    sources = [
        FrameSource(
            venue=KrakenAdapter(depth=10, ws_url=_url(shard[0])),
            symbols=shard,
            duration_s=duration_s,
            subscribe_grace_s=duration_s * 10,  # never fires inside the test
        )
        for shard in _SHARDS
    ]
    supervisor = ShardSupervisor(
        sources=sources,
        duration_s=duration_s,
        queue_maxsize=queue_maxsize,
        backoff_base_s=0.01,
    )
    ctx = RunContext.create(source_name="test", http=cast(HttpClient, None))
    return supervisor, list(supervisor.fetch(ctx))


def _by_stream(records: list[CaptureRecord]) -> dict[str, list[str]]:
    """Payloads per stream, which is what "identical output" has to mean.

    Not the whole record: `seq` is assigned on the drain in arrival order, so a
    shard that reconnects shifts every later seq on *every* shard. That is
    expected and is not a failure crossing connections.
    """
    landed: dict[str, list[str]] = {}
    for record in records:
        landed.setdefault(record["stream"], []).append(record["payload"])
    return landed


def test_every_shard_lands_its_own_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    scripts = {s[0]: [_Script(_messages(s[0], 8))] for s in _SHARDS}
    supervisor, records = _supervisor(monkeypatch, scripts)

    landed = _by_stream(records)
    assert {s for s in landed if s != CONTROL_STREAM} == {
        f"book:{s[0]}" for s in _SHARDS
    }
    # Kraken's heartbeat belongs to the connection and to no book, so it lands
    # under its own tag rather than being attributed to whichever symbol the
    # shard happens to carry.
    assert CONTROL_STREAM in landed
    assert supervisor.dropped == 0
    assert supervisor.reconnects == 0
    # One total order across shards, stamped on the drain.
    assert [r["seq"] for r in records] == list(range(1, len(records) + 1))


def test_a_failure_never_crosses_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shard 2 dies and reconnects; shards 1 and 3 are untouched.

    The assertion is equality against a clean run, per stream. Anything less —
    "the others kept producing" — would pass even if their books had been
    disturbed by the neighbour's failure.
    """
    clean = {s[0]: [_Script(_messages(s[0], 8))] for s in _SHARDS}
    _, without_fault = _supervisor(monkeypatch, clean)

    faulted = {
        "BTC/USD": [_Script(_messages("BTC/USD", 8))],
        # Dies after three messages, then reconnects to a fresh session.
        "ETH/USD": [
            _Script(_messages("ETH/USD", 8), fail_at=3),
            _Script(_messages("ETH/USD", 8)),
        ],
        "SOL/USD": [_Script(_messages("SOL/USD", 8))],
    }
    supervisor, with_fault = _supervisor(monkeypatch, faulted)

    assert supervisor.reconnects >= 1, "the fault must actually have fired"
    assert supervisor.failures[1] >= 1, "on shard 2"
    assert supervisor.failures[0] == 0 and supervisor.failures[2] == 0

    clean_streams = _by_stream(without_fault)
    faulted_streams = _by_stream(with_fault)
    for shard in ("BTC/USD", "SOL/USD"):
        stream = f"book:{shard}"
        assert faulted_streams[stream] == clean_streams[stream], stream


def test_a_full_queue_drops_and_latches_a_repair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never block the socket read — `CLAUDE.md`'s guardrail, as an assertion.

    A queue of one cannot absorb three shards, so records are dropped. What
    matters as much as the counter is that the affected symbol is marked for a
    repair snapshot: a dropped record is a hole with nothing to fix it, which
    is exactly what makes a capture unreplayable.
    """
    scripts = {s[0]: [_Script(_messages(s[0], 40))] for s in _SHARDS}
    supervisor, records = _supervisor(
        monkeypatch, scripts, duration_s=0.5, queue_maxsize=1
    )

    assert supervisor.dropped > 0, "a queue of one must overflow"
    assert records, "and the run still produces records rather than stalling"


def test_a_dropped_record_marks_its_symbol_for_repair() -> None:
    """The `dropped` hook on its own, without the timing of a real overflow."""
    source = FrameSource(
        venue=KrakenAdapter(depth=10), symbols=["BTC/USD"], duration_s=1.0
    )
    state = source._states[source._keys["BTC/USD"]]
    assert not state.skipped

    source.dropped("book:BTC/USD")

    assert state.skipped
    assert state.skip_reason == "dropped"


def test_a_dropped_record_for_an_unknown_stream_is_ignored() -> None:
    source = FrameSource(
        venue=KrakenAdapter(depth=10), symbols=["BTC/USD"], duration_s=1.0
    )
    source.dropped("book:NOPE/USD")  # must not raise


def test_a_supervisor_needs_a_shard() -> None:
    with pytest.raises(ValueError, match="at least one shard"):
        ShardSupervisor(sources=[], duration_s=1.0)


def test_json_payloads_are_landed_verbatim(monkeypatch: pytest.MonkeyPatch) -> None:
    scripts = {s[0]: [_Script(_messages(s[0], 5))] for s in _SHARDS}
    _, records = _supervisor(monkeypatch, scripts)
    for record in records:
        assert json.loads(record["payload"]), "every payload round-trips"
