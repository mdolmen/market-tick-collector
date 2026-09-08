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
    *,
    venue_cap: int,
    recovery_budget_s: float,
    per_symbol_recovery_s: float,
    blast_radius: int = 0,
) -> int:
    """The most symbols one connection may carry, by the tightest rule that binds.

    Three bounds and the minimum wins: the venue's own cap, the blast radius
    accepted, and a recovery budget divided by what a symbol costs to bring
    back. `blast_radius` and `per_symbol_recovery_s` are both optional; zero
    means that bound is not being applied.

    **The recovery bound is measured and does not bind, which is the opposite
    of what `NOTES.md` predicted.** `tools/recovery.py` over 5, 10 and 20
    symbols, 2026-09-08:

        kraken     3.95s   9.65s  15.43s
        coinbase  11.23s  14.38s  16.80s
        binance   17.83s  42.81s  18.66s

    The per-symbol figure *falls* as the shard grows — 3.57s to 0.93s on
    Binance — so it is not a constant to divide by. The reason is that a
    symbol's first snapshot is only fetched once its first frame arrives, so
    the time to a fully trusted shard is set by the **quietest** symbol on it
    rather than by the sum of the work: a `max`, not a `sum`. Two consequences,
    both against the design's expectations:

    - Dividing a budget by a per-symbol cost is the wrong shape, and it stays
      here only because a venue with a genuinely linear repair cost would need
      it. Leave `per_symbol_recovery_s` at zero unless a measurement supports
      it.
    - `NOTES.md`'s illustrative 5s budget is not achievable at *any* shard
      size, including one: the floor is a quiet symbol's first message.

    So the constraint that actually binds is the blast radius — how many books
    may go untrusted at once — and on Coinbase the venue cap of 30 gets there
    first anyway.
    """
    bounds = [venue_cap]
    if blast_radius > 0:
        bounds.append(blast_radius)
    if per_symbol_recovery_s > 0:
        bounds.append(int(recovery_budget_s / per_symbol_recovery_s))
    return max(1, min(bounds))


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
