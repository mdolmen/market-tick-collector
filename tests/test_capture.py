"""The decoder never produces a float.

`CLAUDE.md`'s oldest rule is that prices are never floats, and until Phase 2.5
no venue could break it: Binance and Coinbase both quote decimal strings, so
`json.loads` had nothing to convert. Kraken quotes JSON *numbers*, which turns
the rule from a convention into something the decode has to enforce.

These assert the enforcement, not the venue — `collector.capture` is downstream
of the boundary and must not know one venue from another.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from collector.capture import CaptureRecord, decode, payload_of
from collector.model import SCALE, scaled_int


def _floats(value: Any) -> list[float]:
    """Every float anywhere in a decoded payload."""
    if isinstance(value, float):
        return [value]
    if isinstance(value, dict):
        return [f for item in value.values() for f in _floats(item)]
    if isinstance(value, list):
        return [f for item in value for f in _floats(item)]
    return []


def test_a_json_number_decodes_to_its_source_token() -> None:
    # The Kraken shape, written out rather than imported: the point is that
    # `capture` handles it without knowing whose shape it is.
    text = '{"price":79496.8,"qty":0.00550000}'

    payload = decode(text)

    assert payload == {"price": "79496.8", "qty": "0.00550000"}
    assert _floats(payload) == []


def test_trailing_zeros_survive_the_decode() -> None:
    # Not cosmetic: the trailing zeros are part of the token a venue checksum
    # is computed over, and `float` discards them before anything can object.
    assert decode('{"qty":0.00000000}')["qty"] == "0.00000000"
    assert json.loads('{"qty":0.00000000}')["qty"] == 0.0


def test_a_dust_sized_quantity_would_kill_the_run_as_a_float() -> None:
    """The failure that is worse than the checksum one, because it is total.

    `repr` of a small float is exponent notation, and `scaled_int` refuses
    that outright — so without the token a live run does not drift, it dies.
    """
    token = decode('{"qty":0.00000001}')["qty"]
    assert token == "0.00000001"
    assert scaled_int(token, SCALE) == 1

    as_float = json.loads('{"qty":0.00000001}')["qty"]
    with pytest.raises(ValueError, match="not a plain decimal string"):
        scaled_int(repr(as_float), SCALE)


def test_integers_are_left_alone() -> None:
    # `parse_float` is not consulted for a bare integer, and must not be: a
    # sequence number and a checksum are integers and are read as such.
    payload = decode('{"sequence_num":41,"checksum":3218708950}')

    assert payload["sequence_num"] == 41
    assert payload["checksum"] == 3218708950


def test_payload_of_reads_a_record_through_the_same_decoder() -> None:
    record = CaptureRecord(
        stream="book:BTC/USD",
        kind="frame",
        seq=1,
        receive_ts=0,
        monotonic_ts=0,
        payload='{"price":45285.2}',
    )

    assert payload_of(record)["price"] == "45285.2"


def test_a_non_string_payload_is_refused() -> None:
    with pytest.raises(TypeError, match="expected a string"):
        payload_of({"payload": {"price": "1.0"}})
