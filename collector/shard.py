"""Which symbols go on which connection. One function, and deliberately so.

`NOTES.md` § *Connection supervision* argues the whole of this phase and then
reduces to two instructions: bin-pack by measured message rate rather than by
symbol count, and size the shard by recovery time rather than by the venue's
published cap. Both fit in `plan_shards` below, which is fifteen lines of
longest-processing-time-first greedy and has no state, no I/O and no venue.

**The measurement made this simpler than the design expected, and said so.**
The argument for bin-packing was "BTC and ETH on one socket defeats it". At the
measured distribution that problem does not exist: over all 188 symbols the
busiest single one is 2.2% to 4.9% of its venue's traffic and the top ten carry
18.7% to 32.9%, so any shard of a few dozen lands within a few percent of the
mean whichever way it is packed. Bin-packing stays the default because it costs
fifteen lines and is robust to a distribution that *becomes* skewed — one piece
of news does that to one symbol for an hour — but the honest claim for it today
is insurance, not balance. `NOTES.md` holds the table.

**Two caps, and the minimum wins.** The venue's own limit on symbols per
connection is an upper bound; the recovery budget is usually the tighter one,
which is the inversion `NOTES.md` calls the point worth saying out loud. That
holds on two venues of three: Binance carries the whole set against a cap of
1024 and Kraken against 200. It fails on Coinbase, whose `level2` cap is 30,
and where seven connections are forced before any budget is applied. So this
takes the minimum rather than assuming which constraint binds.

Nothing here knows a venue. It takes numbers and returns groups of the strings
it was given.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from statistics import median

# What a shard's books are allowed to take to come back after a reconnect. The
# blast radius of one connection dying, expressed as time rather than as a
# symbol count — `NOTES.md` § *Connection supervision*. It is a policy choice,
# not a measurement; `tools/recovery.py` measures the per-symbol cost that
# turns it into a symbol count.
DEFAULT_RECOVERY_BUDGET_S = 5.0


def shard_size(
    *, venue_cap: int, recovery_budget_s: float, per_symbol_recovery_s: float
) -> int:
    """The most symbols one connection may carry, by the tighter of two rules.

    `per_symbol_recovery_s` is what a symbol costs to bring back — N REST calls
    against a request-rate limit on an out-of-band venue, in-band snapshot
    bandwidth on a venue that repairs by resubscribing. Zero or less means it
    has not been measured yet, and the venue cap is then the only bound; that
    is a weaker plan and the caller should say so rather than pretend.
    """
    if per_symbol_recovery_s <= 0:
        return max(1, venue_cap)
    by_recovery = int(recovery_budget_s / per_symbol_recovery_s)
    return max(1, min(venue_cap, by_recovery))


def plan_shards(
    symbols: Sequence[str],
    rates: Mapping[str, float],
    *,
    max_per_shard: int,
) -> tuple[tuple[str, ...], ...]:
    """Group `symbols` into shards of at most `max_per_shard`, packed by rate.

    Longest-processing-time-first: take the busiest symbol still unplaced and
    put it on the least-loaded shard that has room. Separating the two busiest
    symbols is a consequence of the sort rather than a rule about them.

    The shard *count* falls out of `max_per_shard` rather than being a second
    knob, which removes the only way this could fail — a fixed count and a
    per-shard cap can be jointly unsatisfiable, and there is then nothing
    sensible to do about it.

    A symbol with no measured rate is charged the median of those that have
    one, not zero. A new listing is unmeasured precisely because it is new, and
    treating it as free is how it lands on the busiest socket.

    Deterministic: ties break on the symbol, then on the shard index, so the
    same inputs always give the same plan and a plan can be committed.
    """
    if max_per_shard < 1:
        raise ValueError(f"max_per_shard must be positive, got {max_per_shard}")
    if not symbols:
        return ()
    known = [rates[s] for s in symbols if s in rates]
    default = median(known) if known else 0.0

    count = -(-len(symbols) // max_per_shard)
    shards: list[list[str]] = [[] for _ in range(count)]
    load = [0.0] * count
    for symbol in sorted(symbols, key=lambda s: (-rates.get(s, default), s)):
        index = min(
            (i for i in range(count) if len(shards[i]) < max_per_shard),
            key=lambda i: (load[i], i),
        )
        shards[index].append(symbol)
        load[index] += rates.get(symbol, default)
    return tuple(tuple(shard) for shard in shards)
