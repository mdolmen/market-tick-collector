"""Look at a venue's live book channel before writing an adapter for it.

    uv run python -m tools.probe binance --duration 30
    uv run python -m tools.probe coinbase --land
    uv run python -m tools.probe kraken --subscribe '{"method": ...}'

Phase 2's first item, and it gates the rest: the sequencing dialect, the
timestamp format, the decimal precision and the set of non-book frames an
adapter has to ignore are all *observed*, not recalled. Writing the Coinbase
and Kraken adapters from memory of the documentation is how a book ends up
silently wrong, which is the one failure mode this project exists not to have.

Deliberately venue-generic and deliberately outside `collector/`. It knows a
URL and a subscribe frame per venue and nothing else — no parsing, no
classification, no record model. Classification is the very thing its output
decides, so a probe that assumed it would be assuming the answer.

The landed envelope is a `CaptureRecord` **without `kind`**, for that reason:
at probe time no adapter exists to say whether a message is a frame or a
snapshot. Once one does, the same file re-lands as a real capture and becomes
a test fixture.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from data_pipeline_core import raw_landing_sink
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection, connect

# Same two as `collector/source.py`: a bounded recv so the duration is honoured
# on a silent socket, and a short close so a finished probe does not hang.
_RECV_SLICE_S = 1.0
_CLOSE_TIMEOUT_S = 2.0

# `websockets` defaults to a 1 MiB frame cap, and a venue that sends its
# snapshot in band blows straight through it — Coinbase's BTC-USD book is
# ~1 MB and the library kills the connection mid-snapshot. Raised rather than
# disabled: unbounded means a venue can drive our allocator.
_MAX_MESSAGE_BYTES = 16 * 1024 * 1024

# One example per distinct message shape is the point of the report; the whole
# of a 5000-level snapshot is not.
_EXAMPLE_CHARS = 700

# Keys venues use to tag a message's type. Any of them found in a payload goes
# into that payload's shape signature, so `type=snapshot` and `type=update`
# count as two shapes rather than one.
_TAG_KEYS = ("type", "channel", "e", "method", "event")


@dataclass(frozen=True, slots=True)
class Probe:
    """A venue's public book channel: where to connect, what to say."""

    venue: str
    url: str
    # Sent in order once connected. Empty for a venue that encodes the
    # subscription in the URL path, which is itself a difference worth seeing.
    subscribe: tuple[Mapping[str, Any], ...] = ()


PROBES: dict[str, Probe] = {
    "binance": Probe(
        venue="binance",
        url="wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
    ),
    # Advanced Trade, not the older Exchange feed this first pointed at. Phase 2
    # probed `level2_batch` on `ws-feed.exchange.coinbase.com` while deciding,
    # `CoinbaseAdapter` settled on Advanced Trade `level2`, and the table was
    # never moved across — so the default probe spoke a protocol no adapter in
    # this project speaks, and answered a subscription with
    # `{"type":"error","reason":"No channels provided"}`. A probe whose default
    # target is not the thing being built is worse than no default.
    "coinbase": Probe(
        venue="coinbase",
        url="wss://advanced-trade-ws.coinbase.com",
        subscribe=(
            {
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channel": "level2",
            },
        ),
    ),
    "kraken": Probe(
        venue="kraken",
        url="wss://ws.kraken.com/v2",
        subscribe=(
            {
                "method": "subscribe",
                "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10},
            },
        ),
    ),
}


# --- collecting -------------------------------------------------------------


def collect(probe: Probe, *, duration_s: float) -> Iterator[dict[str, object]]:
    """Stream one venue's messages for `duration_s`, verbatim.

    A `ConnectionClosed` inside the window is not an error here — it is the
    disconnect behaviour the probe was sent to observe, so it is reported and
    the run ends rather than reconnecting.
    """
    seq = 0
    with connect(
        probe.url, close_timeout=_CLOSE_TIMEOUT_S, max_size=_MAX_MESSAGE_BYTES
    ) as ws:
        for frame in probe.subscribe:
            ws.send(json.dumps(frame))
        print(f"connected to {probe.url} ({len(probe.subscribe)} subscribe frame(s))")
        started = time.monotonic()
        deadline = started + duration_s
        while True:
            try:
                text = _recv(ws, deadline)
            except ConnectionClosed as closed:
                print(
                    f"venue closed the socket after "
                    f"{time.monotonic() - started:.1f}s: {closed}"
                )
                return
            if text is None:
                return
            seq += 1
            yield {
                "stream": probe.venue,
                "seq": seq,
                "receive_ts": time.time_ns(),
                "monotonic_ts": time.monotonic_ns(),
                "payload": text,
            }


def _recv(ws: ClientConnection, deadline: float) -> str | None:
    """Next message as text, or None once the window is up."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            message = ws.recv(timeout=min(remaining, _RECV_SLICE_S))
        except TimeoutError:
            continue
        return message if isinstance(message, str) else message.decode()


# --- reporting --------------------------------------------------------------


def shape(payload: Any) -> str:
    """A message's signature: its type tags, then its top-level keys.

    Two messages share a shape when an adapter would treat them the same way,
    which is what makes the counts below mean anything. Nested tags are part of
    that: Coinbase's Advanced Trade feed puts `snapshot` and `update` inside
    `events[]` under one `channel=l2_data` envelope, so a top-level-only
    signature would report one shape where an adapter sees two.
    """
    if not isinstance(payload, dict):
        return f"<{type(payload).__name__}>"
    tags = [
        f"{key}={payload[key]}"
        for key in _TAG_KEYS
        if isinstance(payload.get(key), str)
    ]
    tags += [
        f"{key}[].{tag}"
        for key, value in payload.items()
        if isinstance(value, list)
        for tag in sorted(_nested_tags(value))
    ]
    keys = ",".join(sorted(payload))
    return f"{' '.join(tags)} {{{keys}}}" if tags else f"{{{keys}}}"


def _nested_tags(items: Sequence[Any]) -> set[str]:
    """Distinct `type=…` tags among a list's dict members."""
    return {
        f"{key}={item[key]}"
        for item in items
        if isinstance(item, dict)
        for key in _TAG_KEYS
        if isinstance(item.get(key), str)
    }


def max_decimals(value: Any, best: tuple[int, str] = (0, "")) -> tuple[int, str]:
    """Deepest fraction seen in any plain decimal token, and the token itself.

    Settles the one project-wide `SCALE`: `scaled_int` refuses to truncate, so
    a venue quoting finer than the chosen scale fails every run rather than
    corrupting a book key. Better to learn that here than in production.

    Reads tokens, not values, because `report` decodes with `parse_float=str`
    — see there for why that is not a detail.
    """
    if isinstance(value, str):
        whole, dot, fraction = value.partition(".")
        if dot and (whole.lstrip("-") + fraction).isdigit():
            return max(best, (len(fraction), value))
        return best
    if isinstance(value, dict):
        for item in value.values():
            best = max_decimals(item, best)
    elif isinstance(value, list):
        for item in value:
            best = max_decimals(item, best)
    return best


def report(records: Sequence[Mapping[str, object]], *, venue: str) -> None:
    """Shape counts with one example each, plus the decimal depth."""
    shapes: dict[str, tuple[int, str]] = {}
    deepest = (0, "")
    for record in records:
        text = str(record["payload"])
        # `parse_float=str` keeps every number as its exact source token. Not a
        # reporting nicety: Kraken quotes price and qty as JSON *numbers*, so a
        # plain `json.loads` rounds a book key through a float before anything
        # can object, and the decimal depth this function measures would read
        # as zero because no price was ever a string.
        payload = json.loads(text, parse_float=str)
        key = shape(payload)
        count, example = shapes.get(key, (0, text))
        shapes[key] = (count + 1, example)
        deepest = max_decimals(payload, deepest)

    elapsed = _elapsed_s(records)
    print(f"\n=== {venue}: {len(records)} messages in {elapsed:.1f}s ===")
    if not records:
        print("nothing received — check the URL and the subscribe frame")
        return
    print(f"{len(records) / elapsed:.1f} msg/s, {len(shapes)} distinct shape(s)")
    print(
        f"deepest fraction: {deepest[0]} decimal place(s)"
        f"{f' — {deepest[1]!r}' if deepest[1] else ''}\n"
    )
    for key, (count, example) in sorted(shapes.items(), key=lambda i: -i[1][0]):
        print(f"--- {count:>6} x  {key}")
        print(f"       {_truncate(example)}\n")


def _elapsed_s(records: Sequence[Mapping[str, object]]) -> float:
    if len(records) < 2:
        return 1.0
    span = int(str(records[-1]["monotonic_ts"])) - int(str(records[0]["monotonic_ts"]))
    return max(span / 1e9, 1e-9)


def _truncate(text: str) -> str:
    if len(text) <= _EXAMPLE_CHARS:
        return text
    return f"{text[:_EXAMPLE_CHARS]}… (+{len(text) - _EXAMPLE_CHARS} chars)"


# --- entry point ------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.probe",
        description="Observe a venue's live book channel before adapting it.",
    )
    parser.add_argument("venue", choices=sorted(PROBES))
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument(
        "--url", help="override the endpoint, to try an alternative feed"
    )
    parser.add_argument(
        "--subscribe",
        action="append",
        default=[],
        metavar="JSON",
        help=(
            "replace the subscribe frame(s); repeat to send several. "
            "Two book subscriptions in one run is how the 'one depth per "
            "symbol' limit gets confirmed rather than assumed."
        ),
    )
    parser.add_argument(
        "--land",
        action="store_true",
        help="also land the messages verbatim, for use as a test fixture",
    )
    parser.add_argument(
        "--channel", help="raw-landing channel (default: <venue>-probe)"
    )
    parser.add_argument("--bucket-url", help="raw-landing bucket (default: the SDK's)")
    args = parser.parse_args(argv)

    probe = PROBES[args.venue]
    if args.url:
        probe = Probe(probe.venue, args.url, probe.subscribe)
    if args.subscribe:
        frames = tuple(json.loads(frame) for frame in args.subscribe)
        probe = Probe(probe.venue, probe.url, frames)

    records = list(collect(probe, duration_s=args.duration))
    report(records, venue=args.venue)

    if args.land and records:
        channel = args.channel or f"{args.venue}-probe"
        result = raw_landing_sink(channel, bucket_url=args.bucket_url).write(records)
        print(f"landed {result.row_count} record(s) to channel {channel!r}")
    return 0 if records else 1


if __name__ == "__main__":
    raise SystemExit(main())
