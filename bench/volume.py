"""How many rows a day of capture actually is, split by what produced them.

    uv run python -m bench.volume kraken data/capture/kraken-p3

`TODO.md` Phase 4 sizes the ClickHouse schema against "10⁷ rows/day". That
figure predates Phase 3 and was written when a run carried one symbol; the
sort key, the partition granularity and the retention rule all rest on it, so
it gets measured before any DDL is written rather than after.

**Snapshot rows are reported apart from diff rows, and the split is the whole
point.** A bootstrap emits the entire book — 2000 rows a symbol at Kraken's
depth 1000, so ~370k rows to bring 185 books up on one connection — and a
capture that reconnected a few times is mostly bootstrap by row count. Dividing
total rows by the capture's span therefore measures the session's *shape*, not
the venue's steady-state rate. Only the diff rows extrapolate to a day; the
snapshot rows extrapolate to how often the process expects to re-bootstrap,
which is a different question with a different answer.

The number this prints is still an extrapolation from a few minutes and is
labelled as one. It is a size estimate for a schema decision, not a capture
rate, and it is not the same claim as `bench/replay.py`'s ceiling.
"""

from __future__ import annotations

import sys
from collections import Counter
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import cast

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.http import HttpClient

from collector import adapters
from collector.capture import CaptureRecord, decode
from collector.main import resolve_shards
from collector.settings import CollectorSettings

_SECONDS_PER_DAY = 86_400


def sessions(path: Path) -> Iterator[Iterator[CaptureRecord]]:
    """Stream a capture one landed file at a time.

    A generator rather than `bench/replay.py`'s list: a Phase 3 capture is
    200 MB of JSONL and materialising it costs more memory than the machine
    running the bench has to spare. Nothing here needs two passes.

    **One file is one session, and they are not concatenated.** A channel
    accumulates a file per run, so the first and last record of the *directory*
    span every idle hour between them — `binance-p3` reads as 6439 seconds of
    capture for 289k records that arrived in three short bursts. Spans are
    therefore summed per file, which is also what makes a rate out of several
    short runs mean anything.
    """
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    for file in files:
        with file.open() as handle:
            yield (cast(CaptureRecord, decode(line)) for line in handle if line.strip())


def measure(venue: str, path: Path) -> tuple[Counter[str], Counter[str], int, float]:
    """Replay a capture and count rows by action and by symbol.

    Returns the action counts, the per-symbol counts, the record count and the
    capture's own span in seconds — the span from the records' `receive_ts`,
    not from the wall clock, so an unthrottled replay measures the session it
    is reading rather than itself.
    """
    settings = CollectorSettings(venue=venue, symbols="*")
    router, _ = adapters.build_router(settings, resolve_shards(settings))
    # No `HttpClient`: nothing on the replay path fetches, and a book that
    # wanted a snapshot mid-replay would be reading the capture's own.
    ctx = RunContext.create(source_name="bench", http=cast(HttpClient, None))

    actions: Counter[str] = Counter()
    symbols: Counter[str] = Counter()
    frames = 0
    span_ns = 0
    for session in sessions(path):
        first = last = 0
        for record in session:
            frames += 1
            last = record["receive_ts"]
            if not first:
                first = last
            for row in router.transform(record, ctx):
                actions[row["action"]] += 1
                symbols[row["symbol"]] += 1
        span_ns += last - first
    return actions, symbols, frames, span_ns / 1e9


def report(
    venue: str, actions: Counter[str], symbols: Counter[str], span_s: float
) -> None:
    total = sum(actions.values())
    diffs = actions["set"] + actions["delete"]
    per_day = diffs / span_s * _SECONDS_PER_DAY if span_s > 0 else 0.0
    print(f"\n{venue}  {span_s:.0f}s of capture, {len(symbols)} symbols")
    for action, count in actions.most_common():
        print(f"  {action:<9} {count:>10,}  {count / total:6.1%}")
    print(f"  {'diff/s':<9} {diffs / span_s:>10,.0f}")
    print(f"  {'diff/day':<9} {per_day:>10,.0f}   extrapolated, not measured")
    if symbols:
        symbol, count = symbols.most_common(1)[0]
        print(f"  busiest   {symbol} at {count / span_s:,.0f} rows/s")


def main(argv: Sequence[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <venue> <capture-dir-or-file>", file=sys.stderr)
        return 2
    venue, path = argv[1], Path(argv[2])
    actions, symbols, frames, span_s = measure(venue, path)
    if not frames or span_s <= 0:
        print("capture is empty or spans no time", file=sys.stderr)
        return 1
    print(f"corpus     {frames:,} records")
    report(venue, actions, symbols, span_s)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
