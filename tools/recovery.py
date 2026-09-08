"""How long a shard takes to get every book trusted after connecting.

    uv run python -m tools.recovery kraken --sizes 5,10,20
    uv run python -m tools.recovery binance --sizes 5,10,20 --repeat 3

`NOTES.md` § *Connection supervision* says to size shards by recovery time
rather than by the venue's published cap, and calls that inversion the point
worth saying out loud. It is unusable without a number, and this is the number:
the wall-clock from opening a connection to the last of its books reaching
LIVE, at several shard sizes.

**The cost model is not the same on all three venues, and the output says so
rather than averaging them into one figure.** Binance repairs out of band, so
recovery is N REST calls against a request-rate limit and should grow roughly
linearly in N. Coinbase and Kraken repair by resubscribing, so recovery is N
in-band snapshots arriving down the socket — bandwidth, not request rate, and
Coinbase's BTC-USD snapshot alone is ~4.9 MB. Two different slopes for the same
axis, and `collector/shard.py` divides the budget by whichever applies.

**Measured at several sizes on purpose.** `shard_size` divides a budget by a
per-symbol cost, which assumes the cost per symbol is constant. That assumption
is exactly what more than one size tests: if the per-symbol figure falls as N
grows, a fixed divisor is over-conservative and the shards are smaller than
they need to be; if it rises, the budget is being blown.

This is a cold bootstrap rather than a *re*-bootstrap, which is the honest
approximation available under a bounded run. A reconnect differs only in that
the books already exist; the venue-side cost — the snapshots and the fetches —
is what dominates and is the same either way.
"""

from __future__ import annotations

import argparse
import statistics
import time
from collections.abc import Sequence

from data_pipeline_core import RunContext
from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.http import HttpClient

from collector import adapters
from collector.settings import CollectorSettings
from collector.source import FrameSource
from collector.symbols import OVERLAPPING_BASES, native

# A shard that has not converged by now is not going to inside a useful budget,
# and reporting the timeout is more honest than reporting the deadline.
_TIMEOUT_S = 120.0


def time_to_live(
    settings: CollectorSettings, symbols: Sequence[str], ctx: RunContext
) -> tuple[float, int]:
    """Seconds until every book on the shard is LIVE, and how many made it."""
    router, _ = adapters.build_router(settings, [tuple(symbols)])
    source = FrameSource(
        venue=lambda: adapters.build(settings),
        symbols=symbols,
        duration_s=_TIMEOUT_S,
        subscribe_grace_s=_TIMEOUT_S,
    )
    started = time.monotonic()
    for record in source.fetch(ctx):
        for _ in router.transform(record, ctx):
            pass
        live = sum(1 for book in router.books.values() if book.live)
        if live == len(symbols):
            return time.monotonic() - started, live
    return time.monotonic() - started, sum(
        1 for book in router.books.values() if book.live
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.recovery",
        description="Measure a shard's time to a fully trusted book set.",
    )
    parser.add_argument("venue", choices=("binance", "coinbase", "kraken"))
    parser.add_argument(
        "--sizes", default="5,10,20", help="comma-separated shard sizes to try"
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--depth", type=int, default=1000)
    args = parser.parse_args(argv)

    sizes = [int(n) for n in args.sizes.split(",")]
    bases = OVERLAPPING_BASES
    print(f"\n=== {args.venue}: time to every book LIVE ===")
    print(f"{'symbols':>8}  {'seconds':>8}  {'per symbol':>11}  {'live':>6}")
    print("-" * 40)
    for size in sizes:
        symbols = [native(base, args.venue) for base in bases[:size]]
        settings = CollectorSettings(
            venue=args.venue,
            depth=args.depth,
            symbols=",".join(symbols),
        )
        runs: list[float] = []
        live = 0
        for _ in range(args.repeat):
            ctx = RunContext.create(source_name="recovery", http=_http())
            elapsed, live = time_to_live(settings, symbols, ctx)
            runs.append(elapsed)
        median = statistics.median(runs)
        note = "" if live == size else f"  <- only {live} converged"
        print(f"{size:>8}  {median:>8.2f}  {median / size:>11.3f}{note}")
    print(
        "\nper-symbol is the divisor `shard_size` uses; set it as "
        "MTC_PER_SYMBOL_RECOVERY_S"
    )
    return 0


def _http() -> HttpClient:
    """A real client: Binance's recovery *is* the REST fetch being measured.

    Wired with a live circuit breaker rather than `None`, because the REST
    limit is exactly what this is measuring against and a run that trips it
    should say so rather than keep hammering.
    """
    settings = CollectorSettings()
    return HttpClient(
        source="recovery",
        settings=settings,
        breaker=CircuitBreaker(
            "recovery",
            threshold=settings.circuit_breaker_threshold,
            cooldown_seconds=settings.circuit_breaker_cooldown_seconds,
        ),
    )


if __name__ == "__main__":
    raise SystemExit(main())
