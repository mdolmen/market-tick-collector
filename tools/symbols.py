"""Resolve the base assets that all three venues quote, and check the rule.

    uv run python -m tools.symbols            # report only
    uv run python -m tools.symbols --check    # non-zero if the set has moved

`collector/symbols.py` commits the resolved set, because a number nobody can
re-derive is not a claim. This is how it is re-derived. It also re-checks the
thing that makes the module a *rule* rather than a table: that
`symbols.native()` reproduces every venue's own spelling for every base in the
set. The day that stops being true, the exception belongs in `symbols.py` and
this is what says so.

Venues list and delist constantly, so a moved set is news rather than a
failure — `--check` exists for a scheduled run, not for CI on every commit.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from collections.abc import Sequence
from typing import Any

from websockets.sync.client import connect

from collector.symbols import OVERLAPPING_BASES, QUOTE, native

_TIMEOUT_S = 30
_RECV_SLICE_S = 1.0
_CLOSE_TIMEOUT_S = 2.0
# The instrument snapshot is ~1400 pairs and exceeds the library's 1 MiB cap,
# the same way an in-band book snapshot does.
_MAX_MESSAGE_BYTES = 32 * 1024 * 1024

_BINANCE = "https://api.binance.com/api/v3/exchangeInfo"
# The Exchange products endpoint is public and lists the same instrument
# universe as Advanced Trade, whose own products call wants credentials.
_COINBASE = "https://api.exchange.coinbase.com/products"
# Kraken's instrument list comes off the v2 socket rather than REST — see
# `kraken_pairs`, and `collector/symbols.py` for what reading REST cost.
_KRAKEN_WS = "wss://ws.kraken.com/v2"


def _get(url: str) -> Any:
    request = urllib.request.Request(
        url, headers={"User-Agent": "market-tick-collector"}
    )
    with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
        return json.load(response)


def binance_pairs() -> dict[str, str]:
    """Base asset → native symbol, for live spot markets against the quote."""
    payload = _get(_BINANCE)
    return {
        symbol["baseAsset"]: symbol["symbol"]
        for symbol in payload["symbols"]
        if symbol["status"] == "TRADING" and symbol["quoteAsset"] == QUOTE["binance"]
    }


def coinbase_pairs() -> dict[str, str]:
    return {
        product["base_currency"]: product["id"]
        for product in _get(_COINBASE)
        if product.get("status") == "online"
        and not product.get("trading_disabled")
        and product["quote_currency"] == QUOTE["coinbase"]
    }


def kraken_pairs() -> dict[str, str]:
    """From the v2 socket's own `instrument` channel, not from REST.

    This read REST `AssetPairs.wsname` until Phase 2.5, on the stated grounds
    that `wsname` "is the spelling the websocket API accepts". It is not:
    `wsname` is **v1** naming and reports `XBT/USD` and `XDG/USD`, which the v2
    socket this project speaks rejects with `Currency pair not supported`. That
    produced a two-entry exception table in `collector/symbols.py` describing a
    protocol nobody here speaks, and a Kraken run that connected, subscribed,
    received nothing and exited clean.

    So the authority is the venue telling us what it will accept, on the same
    connection the collector uses. Nothing else is one.
    """
    suffix = f"/{QUOTE['kraken']}"
    pairs: dict[str, str] = {}
    for symbol in _kraken_v2_symbols():
        if symbol.endswith(suffix):
            pairs[symbol[: -len(suffix)]] = symbol
    return pairs


def _kraken_v2_symbols() -> list[str]:
    """Every symbol in the v2 `instrument` snapshot. Public, no credentials."""
    with connect(
        _KRAKEN_WS, max_size=_MAX_MESSAGE_BYTES, close_timeout=_CLOSE_TIMEOUT_S
    ) as ws:
        ws.send(
            json.dumps({"method": "subscribe", "params": {"channel": "instrument"}})
        )
        deadline = time.monotonic() + _TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                message = ws.recv(timeout=_RECV_SLICE_S)
            except TimeoutError:
                continue
            payload = json.loads(message)
            if payload.get("channel") == "instrument" and payload.get("type") == (
                "snapshot"
            ):
                pairs: list[dict[str, Any]] = payload["data"]["pairs"]
                return [pair["symbol"] for pair in pairs]
    raise RuntimeError("no instrument snapshot from the Kraken v2 socket")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.symbols",
        description="Resolve the overlapping base assets across the venues.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the live set differs from the committed one",
    )
    args = parser.parse_args(argv)

    venues = {
        "binance": binance_pairs(),
        "coinbase": coinbase_pairs(),
        "kraken": kraken_pairs(),
    }
    listed = {venue: set(pairs.values()) for venue, pairs in venues.items()}
    for venue, pairs in venues.items():
        print(f"{venue:<10} {len(pairs):>5} {QUOTE[venue]} markets")

    # Matched on the *symbol* `native()` produces, not on the venue's own base
    # key. Intersecting base keys is what hid Kraken's XBT: bitcoin simply
    # never appeared as "BTC" there, so it dropped out of the set instead of
    # showing up as a mapping failure. Going through `native()` means the
    # alias table is on the path and an unmapped code fails loudly.
    candidates = set(venues["binance"]) | set(venues["coinbase"])
    resolved = tuple(
        sorted(
            base
            for base in candidates
            if all(native(base, venue) in listed[venue] for venue in venues)
        )
    )
    print(f"\noverlapping base assets: {len(resolved)}")

    added = sorted(set(resolved) - set(OVERLAPPING_BASES))
    dropped = sorted(set(OVERLAPPING_BASES) - set(resolved))
    print(f"\nvs committed: +{len(added)} -{len(dropped)}")
    if added:
        print(f"   added:   {added}")
    if dropped:
        print(f"   dropped: {dropped}")

    if not args.check:
        return 0
    return 1 if (added or dropped) else 0


if __name__ == "__main__":
    raise SystemExit(main())
