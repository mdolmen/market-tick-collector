"""The capture envelope — what lands on disk, and what a replay reads back.

One record per received artifact, landed through the SDK's ``raw_landing_sink``
and read back through ``raw_landing_source``. Four decisions are encoded here.

**``payload`` is an opaque string, not a nested object.** Verbatim means
verbatim: re-encoding the venue's JSON would lose key order and number
formatting, and a capture that cannot reproduce the bytes cannot back a
checksum claim later.

**Both clocks are captured, and a replay reads them from here.** Stamping
``time.time_ns()`` during a replay makes every run produce different rows, so
"byte-reproducible" would be unachievable by construction. The clocks belong to
the moment of capture, not to the moment of processing.

**``stream`` tags every record.** Phase 1 writes one value. Phase 8's
depth-limited oracle needs the diff stream and the venue's own top-N
interleaved in arrival order in one file; with the tag reserved now, adding
that subscription is a subscription change rather than a format migration.

**``seq`` is the capture's own counter**, not a venue id. It is arrival order,
which is what makes a reordering fault expressible — and it is the only
ordering a replay can trust before anything has been parsed.

**Control traffic is landed, not discarded.** A subscription ack carries no
book state and produces no rows, so dropping it looks free. It is not: Coinbase
numbers *every* message on a connection, acks included, so a capture missing
one has a hole in the sequence and replays as a phantom gap. Measured, not
assumed — Phase 2's probe found the ack at ``sequence_num`` 2 sitting between
two book messages at 1 and 3, with no break in 317 messages. A venue whose
control traffic sits outside its sequence simply never emits this kind.
"""

from __future__ import annotations

from collections.abc import Mapping
from json import JSONDecoder
from typing import Any, Literal, TypedDict

Kind = Literal["frame", "snapshot", "control"]

# **The decoder never produces a float.** ``parse_float=str`` hands back the
# venue's own source token instead of a float, everywhere, for every venue.
#
# It reads as a Kraken concern and is not one. Kraken quotes price and qty as
# JSON *numbers* where the other two quote strings, so it is the venue that
# makes the difference visible — but the rule it makes visible is the project's
# oldest one, ``CLAUDE.md`` § *Prices are never floats*. A decoder that returns
# floats simply had no venue to break on until now.
#
# Two things break without it, and the second is the one worth knowing:
#
# - The token is what a venue checksum is computed over, so a float round-trip
#   breaks Kraken's CRC32 before it can validate anything.
# - ``repr`` of a small float is exponent notation — ``0.00000001`` comes back
#   as ``1e-08`` — and ``scaled_int`` refuses that outright. So the failure is
#   not a silently wrong book; it is a dead run, on the first dust-sized order.
#
# One decoder, at module scope, because ``json.loads(s, parse_float=str)``
# bypasses the module's cached default decoder and builds a fresh
# ``JSONDecoder`` on every call — a per-message cost on the hot path.
_DECODER = JSONDecoder(parse_float=str)


class CaptureRecord(TypedDict):
    """One captured artifact, venue-neutral. The payload inside is not."""

    stream: str
    kind: Kind
    seq: int
    receive_ts: int
    monotonic_ts: int
    payload: str


def payload_of(record: Mapping[str, object]) -> dict[str, Any]:
    """Decode a record's payload back into the venue's own object."""
    payload = record["payload"]
    if not isinstance(payload, str):
        raise TypeError(f"payload is {type(payload).__name__}, expected a string")
    return decode(payload)


def decode(text: str) -> dict[str, Any]:
    """One venue message, as the venue's own object and its own tokens.

    The single decode in the project, shared by the capture source and the
    replay path so that a frame parses identically live and from disk. See
    ``_DECODER`` on why it is not ``json.loads``.
    """
    decoded: dict[str, Any] = _DECODER.decode(text)
    return decoded
