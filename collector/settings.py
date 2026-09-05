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

    ws_url: str = "wss://stream.binance.com:9443/ws"
    rest_url: str = "https://api.binance.com/api/v3/depth"

    dataset: str = "l2"
    table_name: str = "levels"
    destination: str = "filesystem"

    # "console" prints the level rows instead of landing them — for eyeballing
    # a live run before wiring storage.
    output: Literal["parquet", "console"] = "parquet"

    # Dump every received frame verbatim to this path as JSONL. Feeds
    # bench/decode.py and seeds Phase 1's replay input. Off by default: the tap
    # sits inside the measured in-process path, so a baseline run leaves it
    # unset and the corpus is captured by a separate run.
    raw_frames_path: str | None = None
