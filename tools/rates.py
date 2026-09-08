"""Measure how much traffic each symbol actually produces, per venue.

    uv run python -m tools.rates kraken --duration 300
    uv run python -m tools.rates binance --duration 300 --write

`NOTES.md` § *Connection supervision* says to bin-pack shards by message rate
rather than by symbol count, because BTC and ETH on one socket defeats the
point. That instruction is unusable without a rate per symbol, and a rate
nobody can re-derive is not a claim — so this measures one, on one connection
per venue, and `--write` commits the result to `collector/rates.py` the way
`tools/symbols.py` stands behind `collector/symbols.py`.

**One connection for all 188 on purpose.** The numbers have to be comparable
across symbols, and two connections measured at different times are not: a
burst on one is an hour the other did not see. One socket for the whole set
makes every symbol's count a share of the same window. It also happens to be
the most direct test of the venue's cap, since it subscribes far past any shard
size this project will ever run.

**What this does not measure.** Rates are not stationary — one piece of news
moves a single symbol by an order of magnitude for an hour — so the honest
claim from a run of this is *"no shard exceeded X msg/s during the measurement
window"*, never *"shards are balanced"*. Run it long, run it more than once,
and treat a shard plan as sized rather than proven.

**On Kraken the number depends on `--depth`, and the shard plan has to use the
depth it will run.** The venue emits a message when a level inside the
subscribed window moves, so a deeper book is a busier stream for the same
market. A 30s check at depth 10 ranked ADA above BTC — top-of-book churn, not
volume — and that ordering is not the one a depth-1000 run would produce.
Measure at the depth the collector will subscribe, or the bin-packing is
solving a different problem.

Silence is a result too, and it is reported rather than dropped: a symbol with
zero messages is either genuinely illiquid or was never subscribed, and the
second is the failure `collector/source.py` grew `_assert_the_subscription_took`
to catch. Both need a name in the output.
"""

from __future__ import annotations

import argparse
import importlib.util
import time
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from websockets.sync.client import connect

from collector.adapters import build
from collector.adapters.base import Venue
from collector.capture import decode
from collector.settings import CollectorSettings
from collector.symbols import OVERLAPPING_BASES, native

# The same two as `collector/source.py`: a bounded recv so the duration is
# honoured on a silent socket, and a short close so a finished run does not
# hang on a venue with nothing left to say.
_RECV_SLICE_S = 1.0
_CLOSE_TIMEOUT_S = 2.0

# An in-band snapshot for one symbol is ~4.9 MB on Coinbase; 188 of them arrive
# on the same socket at subscribe. The cap is per message, not per run, so this
# only has to clear the largest single one.
_MAX_MESSAGE_BYTES = 32 * 1024 * 1024

_MODULE = Path(__file__).parent.parent / "collector" / "rates.py"

_HEADER = '''"""Measured message rate per symbol, per venue. Generated — do not edit.

Regenerate with `uv run python -m tools.rates <venue> --write`. The rate is
messages per second on the venue's book channel, counted over one connection
carrying every symbol in `collector/symbols.py`, so the numbers are shares of
one window rather than of several.

`collector/shard.py` reads this to bin-pack shards by traffic instead of by
symbol count. A base missing from a venue's table has never been measured
there; the sharder treats that as the set's median rather than as free, so a
new listing cannot quietly land on the busiest socket.

One venue is measured at a time and each is dated separately, because the
rate is only comparable *within* a window. Nothing here licenses comparing a
base's rate across venues — that is two markets, the same way
`collector/symbols.py` says two quotes are two instruments.
"""

from __future__ import annotations

from typing import Final
'''


def measure(
    venue: Venue, symbols: Sequence[str], *, duration_s: float
) -> tuple[dict[str, int], float]:
    """Count book messages per symbol over one connection. Returns (counts, elapsed).

    Seeded with every subscribed tag at zero, so a symbol that never spoke is a
    zero in the result rather than a missing key — silence is the answer to a
    question this asks, not an absence of one.
    """
    counts = {venue.stream_tag(symbol): 0 for symbol in symbols}
    started = time.monotonic()
    for tag in _stream(venue, symbols, duration_s=duration_s):
        # An unknown tag means the venue answered about something we did not
        # ask for, which is worth seeing rather than silently bucketing.
        counts[tag] = counts.get(tag, 0) + 1
    return counts, time.monotonic() - started


def _stream(
    venue: Venue, symbols: Sequence[str], *, duration_s: float
) -> Iterator[str]:
    """Every book message's stream tag, until the duration is up."""
    with connect(
        venue.ws_url(symbols),
        close_timeout=_CLOSE_TIMEOUT_S,
        max_size=_MAX_MESSAGE_BYTES,
    ) as ws:
        for frame in venue.subscribe_frames(symbols):
            ws.send(frame)
        deadline = time.monotonic() + duration_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                message = ws.recv(timeout=min(remaining, _RECV_SLICE_S))
            except TimeoutError:
                continue
            text = message if isinstance(message, str) else message.decode()
            tag = venue.stream_of(decode(text))
            # None is connection-wide traffic — an ack, a heartbeat, a status
            # frame. It belongs to no symbol and must not inflate one.
            if tag is not None:
                yield tag


def report(
    venue_name: str, counts: Mapping[str, int], elapsed_s: float, *, top: int = 15
) -> None:
    total = sum(counts.values())
    silent = sorted(tag for tag, n in counts.items() if n == 0)
    print(f"\n=== {venue_name}: {total} messages in {elapsed_s:.1f}s ===")
    print(
        f"{total / elapsed_s:.1f} msg/s over {len(counts)} symbols, "
        f"{len(counts) - len(silent)} of them heard from\n"
    )
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    for tag, n in ranked[:top]:
        share = 100 * n / total if total else 0.0
        print(f"  {tag:<28} {n / elapsed_s:>8.2f} msg/s  {share:>5.1f}%")
    if len(ranked) > top:
        rest = sum(n for _, n in ranked[top:])
        print(
            f"  {'(' + str(len(ranked) - top) + ' more)':<28} "
            f"{rest / elapsed_s:>8.2f} msg/s  "
            f"{100 * rest / total if total else 0:>5.1f}%"
        )
    head = sum(n for _, n in ranked[:10])
    print(f"\ntop 10 carry {100 * head / total if total else 0:.1f}% of the traffic")
    if silent:
        print(f"\nsilent ({len(silent)}): {', '.join(silent)}")


def _existing() -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    """The committed tables and their provenance lines, or two empty dicts.

    A venue at a time is the only way to measure — one connection, one window
    — so writing has to merge rather than replace, or measuring Kraken would
    silently delete Binance.
    """
    if not _MODULE.exists():
        return {}, {}
    spec = importlib.util.spec_from_file_location("collector._rates_current", _MODULE)
    if spec is None or spec.loader is None:  # pragma: no cover — unreachable for a path
        return {}, {}
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rates = getattr(module, "RATES", {})
    measured = getattr(module, "MEASURED", {})
    return dict(rates), dict(measured)


def _render(tables: Mapping[str, dict[str, float]], measured: Mapping[str, str]) -> str:
    lines = [_HEADER, "RATES: Final[dict[str, dict[str, float]]] = {"]
    for venue_name in sorted(tables):
        lines.append(f"    # measured {measured.get(venue_name, 'date unknown')}")
        lines.append(f'    "{venue_name}": {{')
        table = tables[venue_name]
        for base, rate in sorted(table.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f'        "{base}": {rate:.4f},')
        lines.append("    },")
    lines.append("}")
    lines.append("")
    lines.append("MEASURED: Final[dict[str, str]] = {")
    for venue_name in sorted(measured):
        lines.append(f'    "{venue_name}": "{measured[venue_name]}",')
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.rates",
        description="Measure per-symbol message rate on one connection.",
    )
    parser.add_argument("venue", choices=("binance", "coinbase", "kraken"))
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument(
        "--depth", type=int, default=1000, help="Kraken book depth; ignored elsewhere"
    )
    parser.add_argument(
        "--bases",
        help="comma-separated base assets; default is every overlapping base",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help=f"emit {_MODULE.relative_to(_MODULE.parent.parent)}",
    )
    args = parser.parse_args(argv)

    bases = (
        tuple(b.strip().upper() for b in args.bases.split(","))
        if args.bases
        else OVERLAPPING_BASES
    )
    venue = build(CollectorSettings(venue=args.venue, depth=args.depth))
    symbols = [native(base, args.venue) for base in bases]
    tag_to_base = {venue.stream_tag(native(base, args.venue)): base for base in bases}

    counts, elapsed = measure(venue, symbols, duration_s=args.duration)
    report(args.venue, counts, elapsed)

    if args.write:
        rates = {
            tag_to_base[tag]: n / elapsed
            for tag, n in counts.items()
            if tag in tag_to_base
        }
        tables, measured = _existing()
        tables[args.venue] = rates
        measured[args.venue] = (
            f"{datetime.now(UTC).date()} over {elapsed:.0f}s "
            f"on one connection, {len(rates)} symbols"
            + (f", depth {args.depth}" if args.venue == "kraken" else "")
        )
        _MODULE.write_text(_render(tables, measured))
        print(f"\nwrote {_MODULE} ({len(rates)} symbols for {args.venue})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
