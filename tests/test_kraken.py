"""The Kraken dialect: no sequence, and a CRC32 that is the whole of the proof.

`test_coinbase.py` proves the machinery over a venue whose snapshot arrives in
band and whose sequence counts every message. This proves it over a venue that
**numbers nothing**, where the chain rule is trivially true and the only thing
that can detect a lost message is the venue's own checksum.

Two of these tests carry the phase. `test_the_checksum_matches_the_live_venue`
runs a landed session from the real socket through the adapter — the only thing
that proves the CRC recipe rather than proving the fixture builder agrees with
itself. `test_a_dropped_frame_is_caught_by_the_checksum_alone` asserts the chain
rule stayed silent while the book still went untrusted, which is the property
that would have been silently absent had Kraken shipped in Phase 2.
"""

from __future__ import annotations

import json
import zlib
from pathlib import Path
from typing import Any, cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.adapters.base import BootstrapOutcome
from collector.adapters.kraken import KrakenAdapter, checksum_token
from collector.capture import CaptureRecord, payload_of
from collector.model import LevelRow
from collector.replay import FaultConfig, FaultInjector
from collector.transform import BookTransform
from tests.conftest import kraken_capture

_FIXTURES = Path(__file__).parent / "fixtures"
_FRAMES = 60
_SNAPSHOT_EVERY = 10
_STREAM = "book:BTC/USD"


def _ctx() -> RunContext:
    return RunContext.create(source_name="test", http=cast(HttpClient, None))


def _run(records: list[CaptureRecord]) -> tuple[list[LevelRow], BookTransform]:
    transform = BookTransform(symbol="BTC/USD", adapter=KrakenAdapter(depth=1000))
    ctx = _ctx()
    rows = [row for record in records for row in transform.transform(record, ctx)]
    return rows, transform


def _session() -> list[CaptureRecord]:
    return kraken_capture(count=_FRAMES, snapshot_every=_SNAPSHOT_EVERY)


def _live_capture() -> list[CaptureRecord]:
    """A real socket session, landed by `tools.probe` and re-landed as a capture."""
    lines = (_FIXTURES / "kraken_book_capture.jsonl").read_text().splitlines()
    return [cast(CaptureRecord, json.loads(line)) for line in lines if line.strip()]


# --- the checksum, against the venue itself ---------------------------------


def test_the_checksum_matches_the_live_venue() -> None:
    """The phase's load-bearing test: the recipe, on bytes the venue sent.

    Everything else here can pass while the CRC recipe is wrong, because every
    other fixture is one this repo computed. This one is not.
    """
    adapter = KrakenAdapter(depth=1000)
    records = _live_capture()
    checked = 0

    for record in records:
        if record["kind"] == "control":
            continue
        assert adapter.observe(payload_of(record)), (
            f"checksum failed on capture seq {record['seq']}"
        )
        checked += 1

    # A checksum that is never checked passes trivially, so assert the count.
    assert checked == 301
    assert not adapter.snapshot_required()


def test_the_live_session_rebuilds_without_a_single_gap() -> None:
    rows, transform = _run(_live_capture())

    assert transform.gaps == 0
    assert transform.bootstraps == 1
    assert transform.live
    assert transform.crossed == 0
    assert rows


def test_the_cached_window_agrees_with_a_full_recompute() -> None:
    """The top ten is cached and rebuilt only when a write could have moved it.

    That optimisation is worth 8.7x (`bench/checksum.py`: 73.9us -> 8.5us a
    message at depth 1000) and it is exactly the kind that stays correct for a
    thousand messages and then is not. So it is checked against the naive
    recomputation on every message of the live session, which is the thing it
    replaced.
    """
    adapter = KrakenAdapter(depth=1000)
    bids: dict[str, str] = {}
    asks: dict[str, str] = {}

    for record in _live_capture():
        if record["kind"] == "control":
            continue
        payload = payload_of(record)
        entry = payload["data"][0]
        if payload["type"] == "snapshot":
            bids.clear()
            asks.clear()
        for book, levels in ((bids, entry["bids"]), (asks, entry["asks"])):
            for level in levels:
                if float(level["qty"]) == 0:
                    book.pop(level["price"], None)
                else:
                    book[level["price"]] = level["qty"]

        naive = [
            checksum_token(p) + checksum_token(asks[p])
            for p in sorted(asks, key=float)[:10]
        ] + [
            checksum_token(p) + checksum_token(bids[p])
            for p in sorted(bids, key=float, reverse=True)[:10]
        ]
        expected = zlib.crc32("".join(naive).encode()) & 0xFFFFFFFF

        assert adapter.observe(payload)
        assert expected == int(entry["checksum"])


def test_the_token_drops_the_point_and_the_leading_zeros() -> None:
    # The venue's two documented examples, and the all-zero case a deleted
    # level produces — which must not become the empty string.
    assert checksum_token("45285.2") == "452852"
    assert checksum_token("0.00100000") == "100000"
    assert checksum_token("0.00000000") == "0"


# --- no sequence, and what follows from it ----------------------------------


def test_the_chain_rule_is_true_and_proves_nothing() -> None:
    """The honest shape of this venue, asserted so it cannot be mistaken.

    `chains` runs over a counter this adapter assigned itself in arrival
    order, so it holds for every pair of consecutive messages including the
    ones either side of a loss. That is why `observe` exists.
    """
    adapter = KrakenAdapter()

    assert adapter.chains(41, 42)
    assert not adapter.chains(41, 43)

    # Over a real session the counter never skips, whatever happened on the
    # wire, because it counts what arrived rather than what was sent.
    records = [r for r in _live_capture() if r["kind"] != "control"]
    previous: int | None = None
    for record in records:
        payload = payload_of(record)
        adapter.observe(payload)
        first, final = adapter.sequence_ids(payload)
        if previous is not None:
            assert adapter.chains(previous, first)
        previous = final


def test_a_dropped_frame_is_caught_by_the_checksum_alone() -> None:
    """The bug this phase exists to prevent, stated as an assertion.

    Drop one book message and the book must go untrusted — and it must do so
    *without* the chain rule ever objecting, because on this venue the chain
    rule cannot. Shipping the adapter without the CRC would leave this test
    green on the gap count and false on the mechanism, which is why the
    checksum and the adapter had to land together.
    """
    records = _session()
    frames = [r for r in records if r["kind"] == "frame"]
    dropped = frames[len(frames) // 2]
    without_it = [r for r in records if r["seq"] != dropped["seq"]]

    intact_rows, intact = _run(records)
    _, broken = _run(without_it)

    assert intact.gaps == 0, "the builder's own session must be consistent"
    assert intact_rows
    assert broken.gaps >= 1

    # And the chain rule stayed silent throughout the broken run.
    adapter = KrakenAdapter()
    previous: int | None = None
    for record in without_it:
        if record["kind"] == "control":
            continue
        payload = payload_of(record)
        adapter.observe(payload)
        first, final = adapter.sequence_ids(payload)
        if previous is not None:
            assert adapter.chains(previous, first), (
                "the chain rule fired; this venue's chain rule cannot detect a loss"
            )
        previous = final


def test_control_traffic_is_classified_apart_from_the_book() -> None:
    adapter = KrakenAdapter()
    book = {"channel": "book", "type": "update", "data": []}

    assert adapter.classify(_STREAM, book) == "frame"
    assert adapter.classify(_STREAM, {**book, "type": "snapshot"}) == "snapshot"
    assert adapter.classify(_STREAM, {"channel": "heartbeat"}) == "control"
    assert adapter.classify(_STREAM, {"channel": "status", "type": "update"}) == (
        "control"
    )
    # A failed ack carries no `channel` at all, so `classify` must not assume one.
    assert (
        adapter.classify(
            _STREAM,
            {"method": "subscribe", "success": False, "error": "Already subscribed"},
        )
        == "control"
    )


def test_control_traffic_does_not_move_the_counter() -> None:
    # Kraken numbers nothing, so a lost heartbeat is invisible — as it must be,
    # since a lost book message is invisible to sequencing here too.
    adapter = KrakenAdapter()
    before = adapter.sequence_ids({})

    adapter.advance({"channel": "heartbeat"})

    assert adapter.sequence_ids({}) == before


# --- the fault battery, unchanged from the other two dialects ---------------


def test_a_stale_snapshot_is_refused_rather_than_used() -> None:
    """Coinbase's crossed-book lesson, which applies here for the same reason:
    the transform keeps the last snapshot it saw and must not rebuild from one
    the stream has long overrun."""
    adapter = KrakenAdapter()
    records = [r for r in _session() if r["kind"] != "control"]
    snapshot_payload = payload_of(records[0])
    adapter.observe(snapshot_payload)
    snapshot = adapter.parse_snapshot(snapshot_payload, receive_ts=0, monotonic_ts=0)
    for record in records[1:]:
        adapter.observe(payload_of(record))
    late = adapter.parse_frame(payload_of(records[-1]), receive_ts=0, monotonic_ts=0)

    result = adapter.bootstrap([late], snapshot)

    assert result.outcome is BootstrapOutcome.SNAPSHOT_TOO_OLD


def test_a_dropped_run_is_detected_and_the_book_reconverges() -> None:
    injector = FaultInjector(FaultConfig(seed=3, drop=0.02, drop_run=3))
    faulted = list(injector(_session()))

    rows, transform = _run(faulted)

    assert injector.injected, "the seed injected nothing; pick another"
    assert transform.gaps >= 1
    assert transform.bootstraps > transform.gaps
    assert transform.live
    assert transform.crossed == 0
    assert len([row for row in rows if row["action"] == "gap"]) == transform.gaps


def test_the_injector_needs_no_knowledge_of_the_venue() -> None:
    records = _session()
    injector = FaultInjector(FaultConfig(seed=1, drop=1.0, drop_run=1))

    faulted = list(injector(records))

    survivors = [r for r in faulted if r["kind"] != "frame"]
    originals = [r for r in records if r["kind"] != "frame"]
    assert survivors == originals


# --- the shadow book -------------------------------------------------------


def test_the_view_is_trimmed_without_disturbing_the_top() -> None:
    """The venue does not delete every level leaving its depth window — the
    landed session grew from 1000 to 1040 levels a side in sixty seconds while
    every checksum still matched. Trimming bounds that, and must never drop a
    level the venue still holds."""
    adapter = KrakenAdapter(depth=1000)
    records = [r for r in _live_capture() if r["kind"] != "control"]

    for record in records:
        assert adapter.observe(payload_of(record))

    # Still consistent with the venue after the whole session, which is the
    # only statement worth making about a trim.
    assert not adapter.snapshot_required()


def test_a_shallow_book_still_checksums() -> None:
    """Fewer than ten levels a side: the window is not full, so every write
    has to be treated as one that could move it."""
    adapter = KrakenAdapter(depth=10)
    bid = {"price": "100.0", "qty": "1.00000000"}
    ask = {"price": "101.0", "qty": "2.00000000"}
    # Asks first, then bids — one level each, so the whole CRC input is these.
    crc_input = "".join(
        checksum_token(level["price"]) + checksum_token(level["qty"])
        for level in (ask, bid)
    )
    assert crc_input == "10102000000001000100000000"
    payload: dict[str, Any] = {
        "channel": "book",
        "type": "snapshot",
        "data": [
            {
                "symbol": "BTC/USD",
                "bids": [bid],
                "asks": [ask],
                "checksum": zlib.crc32(crc_input.encode()) & 0xFFFFFFFF,
                "timestamp": "2026-09-07T08:00:00.000000Z",
            }
        ],
    }

    assert adapter.observe(payload)

    # And the side order is load-bearing: bids first is a different checksum.
    assert crc_input != "".join(
        checksum_token(level["price"]) + checksum_token(level["qty"])
        for level in (bid, ask)
    )
