"""The normalized record model — one row per price level, never one per frame.

The shape is fixed in ``NOTES.md`` § *Scope*. Two rules it encodes:

**Prices are never floats.** A row carries the venue's own decimal string —
lossless, and the input a venue checksum is computed over — *and* an integer
scaled form, which is what an orderable book key and a columnar sink both want.
Sizes get the same pair for the same reasons. A single ``Decimal`` column would
have been the obvious third option; Parquet needs an explicit precision and
scale for one, so the str + int pair is the lower-risk choice at this phase.

**Control events are records.** ``action`` carries ``snapshot`` and ``gap``
alongside the book mutations, so replaying landed rows reproduces the state
transitions rather than only their effects. A ``gap`` row describes no price
level, so its level fields are null.

``ARROW_SCHEMA`` is the same model in the columnar spelling the batch sink
closes into. Stated twice, and a test asserts the two agree field for field —
the alternative is a second definition that drifts.
"""

from __future__ import annotations

from typing import Literal, TypedDict

import pyarrow as pa

Action = Literal["set", "delete", "snapshot", "gap"]
Side = Literal["bid", "ask"]

# One scale for every venue, not one per adapter. `price_ticks` is meaningless
# downstream if its exponent depends on a venue the consumer is not allowed to
# know, so a per-venue scale would put the venue back on the wrong side of the
# normalization boundary. Eight is what Phase 2's probe measured on all three
# venues, on both price and size, and `scaled_int` refuses to truncate — so a
# venue that ever quotes finer fails the run instead of corrupting a book key.
SCALE = 8


class LevelRow(TypedDict):
    """One price level, or one control event, normalized across venues.

    Nothing downstream of an adapter may learn which venue a record came from
    beyond the ``venue`` label — that boundary is the design.
    """

    venue: str
    symbol: str
    # The venue's own final update id for the frame this row came from.
    seq: int
    # All three clocks in nanoseconds, converted at the adapter. ``exchange_ts``
    # is null where the venue's payload carries none of its own (Binance's REST
    # depth snapshot doesn't). ``monotonic_ts`` is the only safe basis for a
    # duration; the other two are wall clocks and can step.
    exchange_ts: int | None
    receive_ts: int
    monotonic_ts: int
    action: Action
    side: Side | None
    price_str: str | None
    price_ticks: int | None
    size_str: str | None
    size_lots: int | None


# **`Int64` cannot hold a scaled tick, and `bench/clickhouse.py` is where that
# was found.** `SCALE = 8` puts the ceiling at (2^63 - 1)/10^8, about 9.2e10,
# and Kraken's depth-1000 `BCH/USD` book carries an ask at 886,110,000,000.00 —
# a junk order parked far from the touch, entirely real, and 8.9e19 once
# scaled. Python's int does not care and every test to date used one symbol
# near the touch, so nothing had reached a columnar type before then.
#
# `Decimal(38, 0)` is a 128-bit integer with a decimal face: exact, wide enough
# for any price a venue can quote at this scale, and it costs 16 bytes a value
# instead of 8. The alternative — keep `Int64` and refuse the row — would mean
# one junk order on one symbol killing the run, which is the trade `scaled_int`
# already refuses to make in the other direction.
TICKS = pa.decimal128(38, 0)

# The clocks stay int64 nanoseconds here — the model's own unit — and are cast
# to a timestamp only where a batch meets a destination that wants one. Keeping
# the accumulated batch in the native unit means any arithmetic on it is exact,
# and the cast at the edge is zero-copy. Declaring `timestamp` here instead
# would move the conversion into `from_pylist`, which takes `datetime` objects
# and would round-trip the run's nanoseconds through microseconds — the same
# silent three-digit loss `fromisoformat` inflicts, and the reason Phase 2
# parses RFC3339 fractions by hand.
CLOCKS = ("exchange_ts", "receive_ts", "monotonic_ts")

ARROW_SCHEMA = pa.schema(
    [
        ("venue", pa.string()),
        ("symbol", pa.string()),
        ("seq", pa.int64()),
        ("exchange_ts", pa.int64()),
        ("receive_ts", pa.int64()),
        ("monotonic_ts", pa.int64()),
        ("action", pa.string()),
        ("side", pa.string()),
        ("price_str", pa.string()),
        ("price_ticks", TICKS),
        ("size_str", pa.string()),
        ("size_lots", TICKS),
    ]
)


# The same model a third time, in dlt's hint spelling, for the Parquet tier.
# **Pinned because dlt infers per load**, and a load in which `exchange_ts` is
# entirely null — which is every Binance REST snapshot — does not land the
# column at all. Phase 0 hit exactly that: files in one dataset disagreed about
# their width and reading the directory as one dataset failed on a file that
# was individually valid.
#
# `price_ticks` and `size_lots` are decimals here for the reason `TICKS` gives
# above, and it bites harder on this tier: dlt's `bigint` is a 64-bit Parquet
# column, so Kraken's 8.9e19 tick would overflow it. Python's int reaching a
# fixed-width type is the same discovery in a second place. Declaring the
# column is not the whole fix — see `collector.sinks.WidenedTickSink`, because
# dlt refuses the wide int before the hint is ever consulted.


def _hint(data_type: str, *, nullable: bool, **extra: object) -> dict[str, object]:
    """One hint dict, built fresh — never a shared constant.

    dlt writes the column's own `name` into the hint it is handed, so two
    columns sharing a dict end up claiming the same name and dlt merges them
    into one. It warns, and then lands a table quietly missing columns.
    """
    return {"data_type": data_type, "nullable": nullable, **extra}


DLT_COLUMNS: dict[str, dict[str, object]] = {
    "venue": _hint("text", nullable=False),
    "symbol": _hint("text", nullable=False),
    "seq": _hint("bigint", nullable=False),
    "exchange_ts": _hint("bigint", nullable=True),
    "receive_ts": _hint("bigint", nullable=False),
    "monotonic_ts": _hint("bigint", nullable=False),
    "action": _hint("text", nullable=False),
    "side": _hint("text", nullable=True),
    "price_str": _hint("text", nullable=True),
    "price_ticks": _hint("decimal", nullable=True, precision=38, scale=0),
    "size_str": _hint("text", nullable=True),
    "size_lots": _hint("decimal", nullable=True, precision=38, scale=0),
}


def scaled_int(value: str, scale: int) -> int:
    """Scale a venue's decimal string to an exact integer, or refuse.

    Raises rather than truncating: a venue quoting finer than ``scale`` would
    otherwise collapse two distinct price levels onto one book key, and the
    resulting book stays plausible while being wrong — the failure mode this
    project exists to not have. Loud is cheap; silent drift is not.

    Digit-shuffling rather than ``Decimal(value).scaleb(scale)``, which is what
    this was until Phase 0 measured scaling at 4x the cost of the JSON decode:
    235ns per value through ``Decimal`` against 198ns here, so ~15% off the
    per-frame path. Modest, and deliberately so — the same parse without the
    two validating lines below runs at 115ns, and buying that last 40% would
    mean accepting ``"1_0"`` as ten. Not on a book key.

    The trade taken is generality instead: ``Decimal`` accepts exponent
    notation and this does not, so anything that is not a plain decimal string
    is refused rather than parsed approximately. No venue in the planned set
    quotes that way, and one that did would need a conversion in its adapter —
    where venue dialects belong anyway.
    """
    sign = -1 if value.startswith("-") else 1
    whole, _, fraction = value.lstrip("+-").partition(".")
    if len(fraction) > scale:
        raise ValueError(f"{value!r} needs more than {scale} decimal places")
    if not (whole + fraction).isdigit():
        raise ValueError(f"{value!r} is not a plain decimal string")
    return sign * int(whole + fraction.ljust(scale, "0"))
