"""The symbol rules, and the distinction they exist to protect.

Nothing here touches a venue: `tools/symbols.py` re-resolves the set against
live instrument lists, and this asserts the properties that must hold whatever
the set currently is.
"""

from __future__ import annotations

import pytest

from collector.symbols import (
    OVERLAPPING_BASES,
    QUOTE,
    base_of,
    canonical,
    native,
)

VENUES = tuple(QUOTE)


@pytest.mark.parametrize("venue", VENUES)
def test_native_round_trips_through_base_of(venue: str) -> None:
    for base in OVERLAPPING_BASES:
        assert base_of(native(base, venue), venue) == base


def test_each_venue_spells_it_its_own_way() -> None:
    # Bitcoin is the case that has been wrong twice. An intersection over base
    # *keys* dropped it silently in Phase 2; the exception table added to fix
    # that spelled it `XBT/USD`, which is Kraken's **v1** naming and which the
    # v2 socket rejects outright. The separator rule alone is the whole rule.
    assert {v: native("BTC", v) for v in VENUES} == {
        "binance": "BTCUSDT",
        "coinbase": "BTC-USD",
        "kraken": "BTC/USD",
    }
    assert native("DOGE", "kraken") == "DOGE/USD"


def test_no_venue_needs_an_alias_and_the_table_still_works() -> None:
    # The table is empty, so this asserts the machinery rather than an entry:
    # an unaliased code has to round-trip, or removing the last alias would
    # have quietly removed the mechanism with it.
    assert base_of("BTC/USD", "kraken") == "BTC"
    assert base_of("DOGE/USD", "kraken") == "DOGE"
    assert native(base_of("SOL/USD", "kraken"), "kraken") == "SOL/USD"


def test_binance_and_coinbase_btc_are_not_the_same_instrument() -> None:
    # The quote differs, so these are two markets and two prices. An id that
    # collapsed them would invite a cross-venue comparison this project does
    # not support and has no way to make correct.
    assert canonical("BTC", "binance") != canonical("BTC", "coinbase")
    assert canonical("BTC", "binance") == "BTC-USDT"
    assert canonical("BTC", "coinbase") == "BTC-USD"


def test_the_same_base_is_one_base_across_venues() -> None:
    # The overlap is on the base asset, which is the thing that genuinely is
    # shared — `NOTES.md` § *Venues and channels*.
    assert base_of("BTCUSDT", "binance") == base_of("BTC/USD", "kraken") == "BTC"


@pytest.mark.parametrize(
    ("symbol", "venue"),
    [
        ("BTC-USD", "binance"),  # another venue's spelling
        ("BTCEUR", "binance"),  # another quote
        ("USDT", "binance"),  # quote with no base in front of it
        ("BTCUSDT", "kraken"),
    ],
)
def test_base_of_refuses_a_symbol_that_is_not_this_venues(
    symbol: str, venue: str
) -> None:
    # Returning a plausible string here is how a book gets filed under the
    # wrong instrument, which nothing downstream could ever detect.
    with pytest.raises(ValueError, match="symbol"):
        base_of(symbol, venue)


def test_the_committed_set_is_sorted_and_unique() -> None:
    # It is generated; this catches a hand-edit that a regeneration would then
    # silently revert.
    assert list(OVERLAPPING_BASES) == sorted(set(OVERLAPPING_BASES))
    assert "BTC" in OVERLAPPING_BASES
