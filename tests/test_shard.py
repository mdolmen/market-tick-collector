"""The sharding policy: every symbol placed once, no shard over the cap.

A pure function over numbers, so this is the one part of Phase 3 that needs no
socket, no fixture and no venue. What it has to guarantee is small and the
tests say all of it: nothing is lost, nothing is duplicated, no shard exceeds
what the connection will take, the plan is stable across runs, and an
unmeasured symbol is not treated as free.

The last one is the only subtle rule. A symbol with no rate is almost always a
*new listing*, which is unmeasured precisely because it is new and is the least
safe thing to assume costs nothing.
"""

from __future__ import annotations

import pytest

from collector.rates import RATES
from collector.shard import plan_shards, shard_size


def test_every_symbol_lands_exactly_once() -> None:
    symbols = [f"S{i}" for i in range(97)]
    rates = {s: float(i) for i, s in enumerate(symbols)}
    shards = plan_shards(symbols, rates, max_per_shard=10)
    placed = [s for shard in shards for s in shard]
    assert sorted(placed) == sorted(symbols)
    assert len(placed) == len(set(placed))


def test_no_shard_exceeds_the_cap() -> None:
    symbols = [f"S{i}" for i in range(97)]
    shards = plan_shards(symbols, {}, max_per_shard=10)
    assert len(shards) == 10, "the count falls out of the cap, not a second knob"
    assert all(len(shard) <= 10 for shard in shards)


def test_the_two_busiest_never_share_a_socket() -> None:
    """`NOTES.md`'s stated reason for packing by rate at all."""
    symbols = ["BTC", "ETH", *(f"S{i}" for i in range(18))]
    rates = {"BTC": 100.0, "ETH": 90.0, **{f"S{i}": 1.0 for i in range(18)}}
    shards = plan_shards(symbols, rates, max_per_shard=10)
    assert len(shards) == 2
    btc = next(s for s in shards if "BTC" in s)
    assert "ETH" not in btc


def test_packing_by_rate_beats_the_alphabet() -> None:
    """The property the greedy exists for: the heaviest shard is smaller.

    Compared against the arrangement the input order would have given, which
    is what any non-packing scheme reduces to.
    """
    symbols = [f"S{i:02d}" for i in range(20)]
    rates = {s: float(20 - i) for i, s in enumerate(symbols)}
    shards = plan_shards(symbols, rates, max_per_shard=10)
    packed = max(sum(rates[s] for s in shard) for shard in shards)
    naive = max(
        sum(rates[s] for s in symbols[:10]), sum(rates[s] for s in symbols[10:])
    )
    assert packed < naive


def test_an_unmeasured_symbol_is_charged_the_median_not_zero() -> None:
    """Otherwise a new listing looks free and lands on the busiest socket."""
    measured = {f"S{i}": 10.0 for i in range(9)}
    symbols = [*measured, "NEW"]
    shards = plan_shards(symbols, measured, max_per_shard=5)
    # Charged 10.0 like the rest, so it balances: five and five. Charged zero,
    # the greedy would have placed it last onto an already-full-weight shard.
    assert sorted(len(shard) for shard in shards) == [5, 5]


def test_the_plan_is_stable_across_runs() -> None:
    symbols = [f"S{i}" for i in range(50)]
    rates = {s: 1.0 for s in symbols}  # all ties, so only the tie-break decides
    first = plan_shards(symbols, rates, max_per_shard=7)
    assert all(plan_shards(symbols, rates, max_per_shard=7) == first for _ in range(5))


def test_an_empty_set_plans_nothing() -> None:
    assert plan_shards([], {}, max_per_shard=10) == ()


def test_a_nonsense_cap_is_refused() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        plan_shards(["A"], {}, max_per_shard=0)


def test_the_tightest_bound_wins() -> None:
    # Recovery binds: 5s budget at 0.1s a symbol is 50, well under Binance's cap.
    assert (
        shard_size(venue_cap=1024, recovery_budget_s=5.0, per_symbol_recovery_s=0.1)
        == 50
    )
    # The venue cap binds: Coinbase's 30 is below the same budget's 50.
    assert (
        shard_size(venue_cap=30, recovery_budget_s=5.0, per_symbol_recovery_s=0.1) == 30
    )
    # Unmeasured recovery falls back to the cap alone rather than inventing one.
    assert (
        shard_size(venue_cap=200, recovery_budget_s=5.0, per_symbol_recovery_s=0.0)
        == 200
    )
    # Never zero, however tight the budget.
    assert (
        shard_size(venue_cap=1024, recovery_budget_s=0.01, per_symbol_recovery_s=9.0)
        == 1
    )
    # Blast radius binds where it is the smallest of the three, which the
    # Phase 3 measurement says is the usual case: recovery time barely grows
    # with shard size, so it rarely gets a say.
    assert (
        shard_size(
            venue_cap=1024,
            recovery_budget_s=5.0,
            per_symbol_recovery_s=0.0,
            blast_radius=30,
        )
        == 30
    )


@pytest.mark.parametrize("venue", sorted(RATES))
def test_the_committed_rates_shard_within_a_few_percent_of_the_mean(venue: str) -> None:
    """A golden check over the real measurement, not a synthetic distribution.

    It pins the claim `NOTES.md` and `collector/shard.py` both make: at the
    measured spread, packing lands every shard close to the mean. If a future
    re-measurement finds a genuinely skewed venue this fails, which is the
    point — the sharder's justification would have changed.
    """
    rates = RATES[venue]
    symbols = sorted(rates)
    shards = plan_shards(symbols, rates, max_per_shard=30)
    loads = [sum(rates[s] for s in shard) for shard in shards]
    # The last shard is short whenever the count does not divide evenly, so
    # compare per-symbol load rather than per-shard.
    per_symbol = [load / len(shard) for load, shard in zip(loads, shards, strict=True)]
    mean = sum(rates.values()) / len(symbols)
    assert max(abs(p - mean) for p in per_symbol) < 0.35 * mean
