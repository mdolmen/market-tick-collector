"""The one piece of logic in the record model: exact decimal → integer scaling.

``scaled_int`` shuffles digits rather than going through ``Decimal``, because
Phase 0 measured the decimal path at 2x the cost and scaling dominates the
per-frame work. ``Decimal`` therefore stays here as the oracle: it is the
definition of the right answer, and the fast path is checked against it over
every value in the recorded session rather than over hand-picked examples.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from collector.model import scaled_int

_FIXTURES = Path(__file__).parent / "fixtures"


def _decimal_scaled(value: str, scale: int) -> int:
    """The implementation this replaced — the authority it must agree with."""
    scaled = Decimal(value).scaleb(scale)
    as_int = int(scaled)
    if scaled != as_int:
        raise ValueError(f"{value!r} needs more than {scale} decimal places")
    return as_int


def _recorded_values() -> list[str]:
    lines = (_FIXTURES / "binance_depth_frames.jsonl").read_text().splitlines()
    values: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        frame = json.loads(line)
        for price, size in (*frame["b"], *frame["a"]):
            values.extend((price, size))
    return values


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("1", 100_000_000),
        ("64150.01000000", 6_415_001_000_000),
        ("0.00000001", 1),
        ("-1.5", -150_000_000),
        ("+1.5", 150_000_000),
        (".5", 50_000_000),
    ],
)
def test_scaled_int_is_exact_within_the_scale(value: str, expected: int) -> None:
    assert scaled_int(value, 8) == expected


def test_scaled_int_agrees_with_decimal_on_every_recorded_value() -> None:
    values = _recorded_values()
    assert values, "the recording should carry price levels"
    for value in values:
        assert scaled_int(value, 8) == _decimal_scaled(value, 8), value


@pytest.mark.parametrize("value", ["1E-8", "1e-8", "", "abc", "1.2.3", "nan", "1 000"])
def test_scaled_int_rejects_what_it_cannot_parse_exactly(value: str) -> None:
    # The trade for dropping Decimal is generality, so anything outside a plain
    # decimal string is refused rather than parsed approximately.
    with pytest.raises(ValueError):
        scaled_int(value, 8)


def test_scaled_int_refuses_to_truncate() -> None:
    # Truncating here would collapse two distinct price levels onto one book
    # key and leave a book that is wrong but plausible. Fail the run instead.
    with pytest.raises(ValueError, match="more than 8 decimal places"):
        scaled_int("0.000000001", 8)


def test_scaled_int_does_not_round_trip_through_float() -> None:
    # 0.1 + 0.2 is the canonical float failure; the decimal path must not care.
    assert scaled_int("0.30000000", 8) == 30_000_000
