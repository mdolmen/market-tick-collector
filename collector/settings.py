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

    # One venue, one symbol, one bounded run — the whole of Phase 0's scope.
    symbol: str = "BTCUSDT"
    duration_s: float = 60.0

    # Binance's full-depth diff channel. 100ms rather than the 1000ms default:
    # volume is the reason this project exists.
    depth_interval_ms: int = 100
    snapshot_limit: int = 5000

    # How often to land a snapshot even when the sequence is healthy. It makes
    # a capture recoverable from an *injected* fault (which removes frames but
    # cannot conjure the repair snapshot a live source would have fetched), and
    # it is what Phase 8's periodic REST oracle reads. Zero disables it.
    snapshot_interval_s: float = 300.0

    ws_url: str = "wss://stream.binance.com:9443/ws"
    rest_url: str = "https://api.binance.com/api/v3/depth"

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
    raw_channel: str = "binance-depth"
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
