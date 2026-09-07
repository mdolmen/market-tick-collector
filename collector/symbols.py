"""Symbols: the canonical id, and each venue's own spelling of it.

Consumer-side business logic, and deliberately not in the SDK — venue caps,
venue symbols and venue quirks are this project's problem, not a pipeline
framework's (`CLAUDE.md`).

**`BTCUSDT` and `BTC-USD` are not the same instrument.** They share a base
asset and differ in the quote, and their prices are two different numbers about
two different markets. The canonical id therefore carries both halves, and the
overlap between venues is computed on the *base* asset alone — which is what
`NOTES.md` § *Venues and channels* means by "all the overlapping base assets".
Nothing here licenses comparing a price across venues; this project does not do
that, and the naming is careful so that a later reader is not tempted.

The mapping is a rule plus a two-entry exception table, not a table of every
symbol. The rule reproduces each venue's own spelling for every overlapping
base but two, so a hand-maintained table would be hundreds of rows of data
whose only contribution is opportunities to be wrong.

**The two exceptions are Kraken's, and finding them was not free.** Kraken
still uses the pre-ISO codes `XBT` for bitcoin and `XDG` for dogecoin. An
earlier version of this module had no exception table and its generator
reported zero failures — because `BTC` was never in the resolved set to fail
on. Kraken lists `XBT/USD`, the intersection quietly dropped bitcoin, and the
rule looked perfect precisely because the hardest case had been excluded from
the sample. The test that caught it asserts `BTC` is in the set.

Regenerate with `uv run python -m tools.symbols`; last resolved 2026-09-06
against live instrument lists.
"""

from __future__ import annotations

from typing import Final

# The quote asset each venue's USD-ish market is denominated in. Binance spot
# has no USD market; USDT is the counterpart, and it is a different instrument.
QUOTE: Final[dict[str, str]] = {
    "binance": "USDT",
    "coinbase": "USD",
    "kraken": "USD",
}

# How each venue spells `BASE + QUOTE`.
_SEPARATOR: Final[dict[str, str]] = {
    "binance": "",
    "coinbase": "-",
    "kraken": "/",
}

# Where a venue's code for an asset is not the canonical one. Kraken predates
# the ISO-ish names the other two use and never migrated.
_ALIASES: Final[dict[str, dict[str, str]]] = {
    "kraken": {"BTC": "XBT", "DOGE": "XDG"},
}
_CANONICAL: Final[dict[str, dict[str, str]]] = {
    venue: {code: base for base, code in aliases.items()}
    for venue, aliases in _ALIASES.items()
}

# Base assets quoted against the venue's USD-ish asset on all three venues.
# Kraken was in the intersection before it was adapted, so that the set did not
# move when Phase 2.5 landed it. It did not.
OVERLAPPING_BASES: Final[tuple[str, ...]] = (
    "1INCH",
    "2Z",
    "AAVE",
    "ACH",
    "ADA",
    "AERO",
    "AI",
    "ALGO",
    "ALICE",
    "ALLO",
    "ALT",
    "ANKR",
    "APE",
    "API3",
    "APT",
    "ARB",
    "ARKM",
    "ARPA",
    "ASTER",
    "ATOM",
    "AUCTION",
    "AUDIO",
    "AVAX",
    "AVNT",
    "AXS",
    "BAND",
    "BAT",
    "BCH",
    "BERA",
    "BICO",
    "BIGTIME",
    "BIO",
    "BLUR",
    "BNB",
    "BNT",
    "BONK",
    "BREV",
    "BTC",
    "C98",
    "CAKE",
    "CELR",
    "CFG",
    "CHIP",
    "CHZ",
    "COMP",
    "COOKIE",
    "COTI",
    "COW",
    "CRV",
    "CTSI",
    "CVC",
    "CVX",
    "DASH",
    "DOGE",
    "DOLO",
    "DOT",
    "EGLD",
    "EIGEN",
    "ENA",
    "ENS",
    "ESP",
    "ETC",
    "ETH",
    "ETHFI",
    "EUL",
    "FET",
    "FIDA",
    "FIL",
    "FLOKI",
    "FLOW",
    "G",
    "GMT",
    "GNO",
    "GRT",
    "GTC",
    "HBAR",
    "ICP",
    "IMX",
    "INJ",
    "JASMY",
    "JTO",
    "KAITO",
    "KAT",
    "KAVA",
    "KERNEL",
    "KMNO",
    "KNC",
    "KSM",
    "LAYER",
    "LDO",
    "LINEA",
    "LINK",
    "LPT",
    "LQTY",
    "LTC",
    "MANA",
    "MASK",
    "ME",
    "MEGA",
    "MET",
    "METIS",
    "MINA",
    "MORPHO",
    "NEAR",
    "NMR",
    "OGN",
    "ONDO",
    "OP",
    "OPN",
    "ORCA",
    "OSMO",
    "PAXG",
    "PENDLE",
    "PENGU",
    "PEPE",
    "PLUME",
    "PNUT",
    "POL",
    "POWR",
    "PROVE",
    "PUMP",
    "PYTH",
    "QI",
    "QNT",
    "RAD",
    "RARE",
    "RAY",
    "RE",
    "RED",
    "RENDER",
    "REQ",
    "REZ",
    "RLC",
    "ROBO",
    "RPL",
    "RSR",
    "S",
    "SAND",
    "SAPIEN",
    "SEI",
    "SENT",
    "SHIB",
    "SIGN",
    "SKY",
    "SNX",
    "SOL",
    "SPELL",
    "SPK",
    "STG",
    "STRK",
    "STX",
    "SUI",
    "SUPER",
    "SUSHI",
    "SXT",
    "SYRUP",
    "T",
    "TAO",
    "TIA",
    "TNSR",
    "TREE",
    "TRUMP",
    "TURBO",
    "UMA",
    "UNI",
    "USD1",
    "USDS",
    "VET",
    "VIRTUAL",
    "VTHO",
    "W",
    "WAL",
    "WCT",
    "WIF",
    "WLD",
    "WLFI",
    "XLM",
    "XPL",
    "XRP",
    "XTZ",
    "YB",
    "YFI",
    "ZAMA",
    "ZEC",
    "ZK",
    "ZKP",
    "ZRO",
    "ZRX",
)


def canonical(base: str, venue: str) -> str:
    """The project-wide id for one venue's market in `base`.

    Carries the quote asset because the quote is part of what the instrument
    *is*: `BTC-USDT` on Binance and `BTC-USD` on Coinbase are two markets, and
    an id that called them both `BTC` would invite a comparison this project
    does not support.
    """
    return f"{base.upper()}-{QUOTE[venue]}"


def native(base: str, venue: str) -> str:
    """The venue's own symbol for `base` against its USD-ish quote."""
    code = _ALIASES.get(venue, {}).get(base.upper(), base.upper())
    return f"{code}{_SEPARATOR[venue]}{QUOTE[venue]}"


def base_of(native_symbol: str, venue: str) -> str:
    """The canonical base asset back out of a venue-native symbol.

    The inverse of `native`, and it refuses rather than guesses: a symbol from
    the wrong venue, or against another quote, is a caller error and silently
    returning a plausible string is how a book ends up filed under the wrong
    instrument.
    """
    suffix = f"{_SEPARATOR[venue]}{QUOTE[venue]}"
    symbol = native_symbol.upper()
    if not symbol.endswith(suffix) or len(symbol) == len(suffix):
        raise ValueError(f"{native_symbol!r} is not a {venue} {QUOTE[venue]} symbol")
    code = symbol[: -len(suffix)]
    return _CANONICAL.get(venue, {}).get(code, code)
