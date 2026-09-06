"""The replay ceiling: how fast the code is, with the socket taken out.

    uv run python -m bench.replay data/capture/binance-depth

``NOTES.md`` § *Two numbers, never merged* is the whole reason this file
exists. The live capture rate is bounded by the venue and is not a statement
about this code; the number here is bounded by this code and is not a capture
rate. Reporting either one without its label is the mistake the discipline
exists to prevent.

**What the headline number covers**, exactly, and nothing more: capture record
→ ``parse_event`` → book apply → ``LevelRow`` built. It excludes the socket, and
it excludes the sink — the Arrow batch sink is Phase 7, so the "through decode
→ book → Arrow → sink" claim in ``NOTES.md`` is *not yet earned* and this must
not be quoted as if it were.

The reader's own cost is reported beside it rather than folded in. Decoding the
capture envelope is what a replay pays to stand in for a socket, so counting it
against the collector would understate the collector; hiding it entirely would
overstate what the harness can drive. Both numbers, both labelled — the same
rule one level down.

Deliberately not routed through ``WorkerApp``: dlt and the metrics push are not
part of the claim being made.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector.capture import CaptureRecord
from collector.transform import BinanceBookTransform

# Enough of a run to leave the interpreter warm and the branch predictors
# settled before anything is timed.
_WARMUP = 200


def _load(path: Path) -> list[CaptureRecord]:
    """Read a capture, whether it landed as one file or a directory of them."""
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    records: list[CaptureRecord] = []
    for file in files:
        for line in file.read_text().splitlines():
            if line.strip():
                records.append(cast(CaptureRecord, json.loads(line)))
    return records


def _drive(records: Sequence[CaptureRecord]) -> tuple[int, int, float]:
    """One unthrottled pass. Returns (frames, rows, elapsed seconds)."""
    transform = BinanceBookTransform(symbol="BTCUSDT")
    ctx = RunContext.create(source_name="bench", http=cast(HttpClient, None))
    rows = 0
    started = time.monotonic()
    for record in records:
        for _ in transform.transform(record, ctx):
            rows += 1
    elapsed = time.monotonic() - started
    return transform.frames, rows, elapsed


def _decode(lines: Sequence[str]) -> float:
    """What the replay pays to stand in for a socket."""
    started = time.monotonic()
    for line in lines:
        json.loads(line)
    return time.monotonic() - started


def main(argv: Sequence[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <capture-dir-or-file>", file=sys.stderr)
        return 2
    records = _load(Path(argv[1]))
    if not records:
        print("capture is empty", file=sys.stderr)
        return 1
    snapshots = sum(record["kind"] == "snapshot" for record in records)
    if not snapshots:
        print("capture carries no snapshot, so no book can be built", file=sys.stderr)
        return 1

    _drive(records[:_WARMUP])  # warm the interpreter, discard
    frames, rows, elapsed = _drive(records)
    if not frames:
        print("capture carries no frames", file=sys.stderr)
        return 1
    envelope_s = _decode([json.dumps(record) for record in records])

    print(f"corpus         {len(records)} records, {snapshots} snapshot(s)")
    print(f"               {frames} frames -> {rows} rows\n")
    print("replay ceiling — decode -> book apply -> row built")
    print("               excludes the socket path AND the sink\n")
    print(f"  frames/s     {frames / elapsed:,.0f}")
    print(f"  rows/s       {rows / elapsed:,.0f}")
    print(f"  µs/frame     {elapsed / frames * 1e6:.1f}")
    print("\n  envelope decode, the replay's own overhead, reported apart:")
    print(f"  µs/record    {envelope_s / len(records) * 1e6:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
