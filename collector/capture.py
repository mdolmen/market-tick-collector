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
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal, TypedDict

Kind = Literal["frame", "snapshot"]


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
    decoded: dict[str, Any] = json.loads(payload)
    return decoded
