"""Collector configuration — this project's fields on top of the SDK's.

Inherits the SDK plumbing knobs (logging, HTTP, metrics push) and adds the
collector's own. Read from the environment with an ``MTC_`` prefix, e.g.
``MTC_SYMBOL=ETHUSDT MTC_DURATION_S=300``. The destination bucket stays
dlt-native config (``DESTINATION__FILESYSTEM__BUCKET_URL``).
"""

from __future__ import annotations

from typing import Literal

from data_pipeline_core import Settings
from pydantic_settings import SettingsConfigDict


class CollectorSettings(Settings):
    model_config = SettingsConfigDict(
        env_prefix="MTC_", env_file=".env", extra="ignore"
    )

    # One venue, one symbol, one bounded run. Several venues at once needs the
    # connection supervisor (Phase 3) and ``ServiceApp`` (Phase 4); until then
    # a venue is a process, which keeps ``WorkerApp``'s one-source contract
    # untouched.
    venue: Literal["binance", "coinbase", "kraken"] = "binance"
    symbol: str = "BTCUSDT"
    duration_s: float = 60.0

    # Binance's full-depth diff channel. 100ms rather than the 1000ms default:
    # volume is the reason this project exists.
    depth_interval_ms: int = 100
    snapshot_limit: int = 5000

    # Kraken's book channel is depth-limited — 10/25/100/500/1000 and nothing
    # else — so unlike the other two there is no full-depth subscription to
    # ask for. 1000 is the venue's ceiling and the closest this project gets
    # to its own assumption. The checksum covers the top 10 regardless.
    depth: int = 1000

    # How often to land a snapshot even when the sequence is healthy. It makes
    # a capture recoverable from an *injected* fault (which removes frames but
    # cannot conjure the repair snapshot a live source would have fetched), and
    # it is what Phase 8's periodic REST oracle reads. Zero disables it.
    snapshot_interval_s: float = 300.0

    # Endpoints live on the adapter, which is where a venue's transport
    # belongs; these override them, for pointing a run at a testnet or a
    # recorded fixture server. Empty means "use the adapter's own".
    ws_url: str = ""
    rest_url: str = ""

    dataset: str = "l2"
    table_name: str = "levels"
    destination: str = "filesystem"

    # "console" prints the level rows instead of landing them — for eyeballing
    # a live run before wiring storage.
    output: Literal["parquet", "console"] = "parquet"

    # Which of the three wirings of the same two pieces to run:
    #   collect  socket -> book -> level rows -> curated sink (the Phase 0 path)
    #   capture  socket -> raw landing, verbatim, no book at all
    #   replay   raw landing -> book -> level rows, no socket
    mode: Literal["collect", "capture", "replay"] = "collect"

    # Where a capture lands and a replay reads from. The bucket falls back to
    # the SDK's own RAW_BUCKET_URL when unset, which is how a local
    # file:// dir and a gs:// bucket stay the same code.
    #
    # One channel per venue, defaulted from ``venue`` when left empty. That is
    # why ``CaptureRecord`` carries no venue field: the channel already says,
    # a replay resolves its adapter from the same setting, and adding one would
    # have been a format change for information the path already holds.
    raw_channel: str = ""
    raw_bucket_url: str | None = None

    # Paced replay: the capture's own inter-arrival gaps divided by this. None
    # means unthrottled — the replay *ceiling*, which is a different number and
    # is never reported as a capture rate.
    replay_speed: float | None = None

    # Comma-separated fault names (see collector.replay.FAULT_NAMES) and the
    # seed that makes any run of them reproducible. The seed is reported, so a
    # break can always be re-created exactly.
    faults: str = ""
    fault_seed: int = 0
