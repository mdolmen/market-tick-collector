"""json vs orjson vs msgspec.json over a captured frame corpus.

Committed and reproducible, because an uncommitted number is not a claim:

    MTC_DURATION_S=300 MTC_RAW_FRAMES_PATH=data/btcusdt-frames.jsonl \\
        uv run python -m collector.main
    uv run python bench/decode.py data/btcusdt-frames.jsonl

It measures two things, not one. *Decode* is the question the phase asked —
JSON parsing was expected to dominate a high-rate diff stream. *Decode +
model* is what the collector actually does per frame: decode, then scale every
price and size through ``Decimal`` into the integer book key. Reporting only
the first would answer a question the pipeline never asks, and the two can
rank differently — which is the whole reason to run this in Phase 0 rather
than assume it in Phase 9.

Nothing here is zero-copy. ``json``, ``orjson`` and ``msgspec`` all allocate;
saying otherwise would need a ``memoryview`` over the recv buffer and a native
extension. What is reported is what was measured.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import msgspec
import orjson

from collector.adapters.binance import PRICE_SCALE, SIZE_SCALE
from collector.model import scaled_int

REPEATS = 5


class DepthFrame(msgspec.Struct):
    """The typed schema — msgspec's advantage is skipping the generic dict."""

    U: int
    u: int
    E: int
    b: list[tuple[str, str]]
    a: list[tuple[str, str]]


def _scale(payload: Any) -> int:
    """The per-level work the collector does after any decoder returns."""
    total = 0
    for price, size in (*payload["b"], *payload["a"]):
        total += scaled_int(price, PRICE_SCALE) + scaled_int(size, SIZE_SCALE)
    return total


def _scale_typed(frame: DepthFrame) -> int:
    total = 0
    for price, size in (*frame.b, *frame.a):
        total += scaled_int(price, PRICE_SCALE) + scaled_int(size, SIZE_SCALE)
    return total


def _time(work: Callable[[], object], corpus_size: int) -> tuple[float, float]:
    """Best-of-``REPEATS`` wall time over the corpus → (µs/frame, frames/s)."""
    best = min(_one_pass(work) for _ in range(REPEATS))
    per_frame_s = best / corpus_size
    return per_frame_s * 1e6, 1 / per_frame_s


def _one_pass(work: Callable[[], object]) -> float:
    started = time.perf_counter()
    work()
    return time.perf_counter() - started


def main(argv: Sequence[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <captured-frames.jsonl>", file=sys.stderr)
        return 2
    raw = [
        line.encode() for line in Path(argv[1]).read_text().splitlines() if line.strip()
    ]
    if not raw:
        print("corpus is empty", file=sys.stderr)
        return 1

    typed = msgspec.json.Decoder(DepthFrame)
    untyped = msgspec.json.Decoder()

    cases: list[tuple[str, Callable[[], object], Callable[[], object]]] = [
        (
            "json (stdlib)",
            lambda: [json.loads(frame) for frame in raw],
            lambda: [_scale(json.loads(frame)) for frame in raw],
        ),
        (
            "orjson",
            lambda: [orjson.loads(frame) for frame in raw],
            lambda: [_scale(orjson.loads(frame)) for frame in raw],
        ),
        (
            "msgspec (untyped)",
            lambda: [untyped.decode(frame) for frame in raw],
            lambda: [_scale(untyped.decode(frame)) for frame in raw],
        ),
        (
            "msgspec (typed)",
            lambda: [typed.decode(frame) for frame in raw],
            lambda: [_scale_typed(typed.decode(frame)) for frame in raw],
        ),
    ]

    decoded = [typed.decode(frame) for frame in raw]
    levels = sum(len(frame.b) + len(frame.a) for frame in decoded)
    print(
        f"corpus: {len(raw)} frames, {levels} levels, "
        f"{sum(len(frame) for frame in raw) / 1024:.0f} KiB, best of {REPEATS}\n"
    )
    header = (
        f"{'decoder':<20}{'decode µs':>12}{'frames/s':>14}"
        f"{'+model µs':>12}{'frames/s':>14}"
    )
    print(header)
    print("-" * len(header))
    for name, decode_only, decode_and_model in cases:
        decode_us, decode_rate = _time(decode_only, len(raw))
        model_us, model_rate = _time(decode_and_model, len(raw))
        print(
            f"{name:<20}{decode_us:>12.1f}{decode_rate:>14,.0f}"
            f"{model_us:>12.1f}{model_rate:>14,.0f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
