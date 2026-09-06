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
"""

from __future__ import annotations

from typing import Literal, TypedDict

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
