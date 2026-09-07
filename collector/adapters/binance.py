"""Binance spot: full-depth diffs, out-of-band REST snapshot, chained ranges.

The row of `NOTES.md` § *Sequencing dialects* this venue occupies: a frame
batches several internal updates and therefore carries a *range* `[U, u]`,
successive frames chain as `U == prev_u + 1`, and the snapshot arrives over
REST rather than on the socket. Coinbase chains single sequence numbers and
sends its snapshot in band; both answer the `in_sequence` / `gap_detected` /
`snapshot_required` contract in `adapters.base`, and that is the whole of the
difference downstream is allowed to see.

Every socket message on this venue is a `depthUpdate` — the probe found no
control traffic at all — so `classify` never returns `control` here. The
snapshot is told apart by its stream tag rather than by its content, because
it did not arrive on the socket in the first place.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from collector.adapters.base import (
    BootstrapResult,
    Level,
    Snapshot,
    SnapshotRequest,
    Update,
    bootstrap_by_sequence,
)
from collector.capture import Kind
from collector.model import SCALE, scaled_int

VENUE = "binance"

# The stream tag a captured REST depth snapshot carries. Binance's snapshot
# arrives out of band, so it has no websocket stream name of its own.
REST_DEPTH_STREAM = "rest:depth"

_WS_URL = "wss://stream.binance.com:9443/ws"
_REST_URL = "https://api.binance.com/api/v3/depth"

_MS_TO_NS = 1_000_000


def stream_name(symbol: str, interval_ms: int) -> str:
    """Binance's raw-stream name for a full-depth diff channel."""
    return f"{symbol.lower()}@depth@{interval_ms}ms"


def _levels(raw: Sequence[Sequence[str]]) -> tuple[Level, ...]:
    return tuple(
        Level(
            price_str=price,
            price_ticks=scaled_int(price, SCALE),
            size_str=size,
            size_lots=scaled_int(size, SCALE),
        )
        for price, size in raw
    )


class BinanceAdapter:
    """The sequencing dialect, and the only place it is allowed to live."""

    venue = VENUE

    def __init__(
        self,
        *,
        depth_interval_ms: int = 100,
        snapshot_limit: int = 5000,
        ws_url: str = _WS_URL,
        rest_url: str = _REST_URL,
    ) -> None:
        self._depth_interval_ms = depth_interval_ms
        self._snapshot_limit = snapshot_limit
        self._ws_url = ws_url.rstrip("/")
        self._rest_url = rest_url
        self._prev_final_id: int | None = None
        self._snapshot_required = True

    # --- transport ---------------------------------------------------------

    def ws_url(self, symbol: str) -> str:
        return f"{self._ws_url}/{self.stream_tag(symbol)}"

    def subscribe_frames(self, symbol: str) -> tuple[str, ...]:
        """None: Binance takes the subscription in the URL path."""
        return ()

    def resubscribe_frames(self, symbol: str) -> tuple[str, ...]:
        """None: a stale book here is repaired by refetching over REST."""
        return ()

    def stream_tag(self, symbol: str) -> str:
        return stream_name(symbol, self._depth_interval_ms)

    def snapshot_request(self, symbol: str) -> SnapshotRequest | None:
        return SnapshotRequest(
            stream=REST_DEPTH_STREAM,
            url=self._rest_url,
            params={"symbol": symbol.upper(), "limit": self._snapshot_limit},
        )

    # --- parsing -----------------------------------------------------------

    def classify(self, stream: str, payload: Mapping[str, Any]) -> Kind:
        """By stream tag: the snapshot never came over the socket.

        Never `control`: every message on a raw depth stream is a
        `depthUpdate`, which the Phase 2 probe confirmed over a live session.
        """
        return "snapshot" if stream == REST_DEPTH_STREAM else "frame"

    def stream_of(self, payload: Mapping[str, Any]) -> str | None:
        """From `s`, the symbol the venue stamps on every `depthUpdate`.

        `None` covers the subscribe ack — `{"result":null,"id":1}` — which the
        URL-path subscription never produced and a `SUBSCRIBE` frame does.
        """
        symbol = payload.get("s")
        return None if symbol is None else self.stream_tag(str(symbol))

    def sequence_ids(self, payload: Mapping[str, Any]) -> tuple[int, int]:
        return int(payload["U"]), int(payload["u"])

    def snapshot_seq(self, payload: Mapping[str, Any]) -> int:
        return int(payload["lastUpdateId"])

    def parse_frame(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Update:
        return Update(
            first_seq=int(payload["U"]),
            final_seq=int(payload["u"]),
            exchange_ts=int(payload["E"]) * _MS_TO_NS,
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=_levels(payload["b"]),
            asks=_levels(payload["a"]),
        )

    def parse_snapshot(
        self, payload: Mapping[str, Any], *, receive_ts: int, monotonic_ts: int
    ) -> Snapshot:
        """`GET /api/v3/depth`. Binance sends no clock of its own with it."""
        return Snapshot(
            final_seq=int(payload["lastUpdateId"]),
            exchange_ts=None,
            receive_ts=receive_ts,
            monotonic_ts=monotonic_ts,
            bids=_levels(payload["bids"]),
            asks=_levels(payload["asks"]),
        )

    # --- sequencing --------------------------------------------------------

    def chains(self, prev_final_seq: int, first_seq: int) -> bool:
        """`U == prev_u + 1` — the chain rule, with one definition."""
        return first_seq == prev_final_seq + 1

    def snapshot_stale(self, first_buffered_seq: int, snapshot_seq: int) -> bool:
        return first_buffered_seq > snapshot_seq + 1

    def bootstrap(
        self, buffered: Sequence[Update], snapshot: Snapshot
    ) -> BootstrapResult:
        """Locate the snapshot inside the buffered stream. `NOTES.md` steps 3-6.

        The snapshot position `S` lands *inside* a frame rather than on a
        boundary, because a frame covers a range. So the event to start from is
        the one whose range straddles `S`: `U <= S + 1 <= u`.

        The two failure branches are not the same failure, and this is the
        whole reason it returns three outcomes instead of a bool:

        - `SNAPSHOT_TOO_OLD` — the buffer starts *ahead* of `S + 1`, so updates
          between the snapshot and the buffer are already lost. Refetch.
        - `BUFFER_BEHIND` — every buffered event is in the past. The snapshot
          is fine and simply has not been reached yet. Keep buffering.

        Collapsing both into "retry" refetches on the second case, which is a
        bootstrap loop that occasionally spins.
        """
        return bootstrap_by_sequence(self, buffered, snapshot)

    def bootstrapped(self, snapshot: Snapshot) -> None:
        """A snapshot has been spliced in; the next event is the straddler.

        The cursor resets to `None` rather than to `lastUpdateId`: the
        straddler legitimately starts *before* `S + 1`, so the chain rule
        would reject the one event the bootstrap just proved correct. The
        snapshot is therefore unused here, and is in the signature for
        Coinbase, whose boundary falls between messages rather than inside one.
        """
        self._prev_final_id = None
        self._snapshot_required = False

    def advance(self, payload: Mapping[str, Any]) -> None:
        """No-op: no control traffic, and the REST snapshot is outside the chain."""

    def observe(self, payload: Mapping[str, Any]) -> bool:
        """The venue publishes no checksum, so `chains` is the whole of it."""
        return True

    def snapshot_supersedes(self) -> bool:
        """No: the REST snapshot is read alongside a socket that never stopped."""
        return False

    def in_sequence(self, update: Update) -> bool:
        """`U == prev_u + 1`. Ranges chain; single sequence numbers do not."""
        if self._prev_final_id is None:
            return True
        return self.chains(self._prev_final_id, update.first_seq)

    def gap_detected(self, update: Update) -> bool:
        """The inverse, and it latches `snapshot_required`."""
        if self.in_sequence(update):
            return False
        self._snapshot_required = True
        return True

    def snapshot_required(self) -> bool:
        """Binance repairs a gap only by refetching the out-of-band snapshot.

        Distinct from `gap_detected` because it is a latch: it stays true for
        the whole untrusted interval, not just for the frame that broke the
        chain. A venue that revalidates with a checksum can set it without any
        gap at all, which is why the two are separate questions.
        """
        return self._snapshot_required

    def accept(self, update: Update) -> None:
        """Advance the cursor past an update that has been applied."""
        self._prev_final_id = update.final_seq
