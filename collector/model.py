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

from decimal import Decimal
from typing import Literal, TypedDict

Action = Literal["set", "delete", "snapshot", "gap"]
Side = Literal["bid", "ask"]


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
    """
    scaled = Decimal(value).scaleb(scale)
    as_int = int(scaled)
    if scaled != as_int:
        raise ValueError(f"{value!r} needs more than {scale} decimal places")
    return as_int
