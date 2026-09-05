"""The one piece of logic in the record model: exact decimal → integer scaling."""

from __future__ import annotations

import pytest

from collector.model import scaled_int


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("1", 100_000_000),
        ("64150.01000000", 6_415_001_000_000),
        ("0.00000001", 1),
    ],
)
def test_scaled_int_is_exact_within_the_scale(value: str, expected: int) -> None:
    assert scaled_int(value, 8) == expected


def test_scaled_int_refuses_to_truncate() -> None:
    # Truncating here would collapse two distinct price levels onto one book
    # key and leave a book that is wrong but plausible. Fail the run instead.
    with pytest.raises(ValueError, match="more than 8 decimal places"):
        scaled_int("0.000000001", 8)


def test_scaled_int_does_not_round_trip_through_float() -> None:
    # 0.1 + 0.2 is the canonical float failure; the decimal path must not care.
    assert scaled_int("0.30000000", 8) == 30_000_000
