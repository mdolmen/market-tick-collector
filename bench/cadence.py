"""Why the live in-process p99 is not a property of the pipeline.

The Phase 0 run reports an in-process p50 around 200µs per frame. The decode
benchmark says the same work costs single-digit µs. Both are correct, and the
difference is the *cadence*: a 100ms channel leaves the process idle between
frames, and the first work after each wake-up runs on a core that has been
allowed to go quiet. A tight benchmark loop never pays that; a live collector
pays it on every frame.

This runs the collector's real per-frame path — the same ``parse_event`` and
row construction the source uses, not a copy — twice over one corpus: back to
back, then with the channel's own gap between frames. The gap between those two
numbers is the wake-up cost, and it is the reason the *live* latency figure
must not be quoted as what the code costs. The replay ceiling in Phase 1 is the
number that answers that, which is exactly why NOTES keeps the two apart.

    uv run python -m bench.cadence data/btcusdt-frames.jsonl
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from collector.adapters.binance import BinanceAdapter
from collector.transform import BookTransform

# Enough frames to characterise a percentile without spending len(corpus) *
# gap seconds on the idle pass.
IDLE_SAMPLE = 150


def _measure(frames: Sequence[str], gap_s: float) -> list[int]:
    adapter = BinanceAdapter()
    transform = BookTransform(symbol="BTCUSDT", adapter=adapter)
    latencies: list[int] = []
    for text in frames:
        if gap_s:
            time.sleep(gap_s)
        started = time.monotonic_ns()
        event = adapter.parse_frame(json.loads(text), receive_ts=0, monotonic_ts=0)
        # The collector's own row construction, deliberately not a copy of it.
        transform.level_rows(event)
        latencies.append(time.monotonic_ns() - started)
    return latencies


def _report(label: str, latencies: list[int]) -> None:
    ordered = sorted(latencies)

    def at(q: float) -> float:
        return ordered[min(len(ordered) - 1, int(q * len(ordered)))] / 1000

    print(
        f"{label:<30}{at(0.50):>10.1f}{at(0.90):>10.1f}{at(0.99):>10.1f}"
        f"{len(ordered):>10}"
    )


def main(argv: Sequence[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <captured-frames.jsonl>", file=sys.stderr)
        return 2
    frames = [line for line in Path(argv[1]).read_text().splitlines() if line.strip()]
    if not frames:
        print("corpus is empty", file=sys.stderr)
        return 1

    gap_s = 0.1  # the 100ms channel this corpus was captured from
    _measure(frames[:200], 0)  # warm the interpreter, discard

    header = f"{'':<30}{'p50 µs':>10}{'p90 µs':>10}{'p99 µs':>10}{'frames':>10}"
    print(header)
    print("-" * len(header))
    _report("back to back", _measure(frames, 0))
    _report(f"{gap_s * 1000:.0f}ms idle between", _measure(frames[:IDLE_SAMPLE], gap_s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
