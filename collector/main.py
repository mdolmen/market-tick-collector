"""Entry point — three wirings of the same two pieces, nothing else to write.

    MTC_SYMBOL=BTCUSDT MTC_DURATION_S=60 uv run python -m collector.main

Configuration comes from ``CollectorSettings`` (``MTC_*`` env vars, or a
``.env``); structured logging, the resilience stack behind ``ctx.http``,
graceful shutdown and the metrics push all come from ``WorkerApp``.

    MTC_MODE=collect     socket -> book -> level rows -> curated sink
    MTC_MODE=capture     socket -> raw landing, verbatim, no book
    MTC_MODE=replay      raw landing -> book -> level rows, no socket

``collect`` and ``replay`` differ only in where the records come from — the
same ``BinanceBookTransform`` instance type builds the book either way, which
is what makes a replay a regression test of the collector rather than of a
parallel implementation.

Key knobs:

    MTC_SYMBOL           one symbol, Binance spot        (BTCUSDT)
    MTC_DURATION_S       how long a bounded live run lasts (60)
    MTC_OUTPUT           parquet | console               (parquet)
    MTC_RAW_CHANNEL      capture/replay channel name     (binance-depth)
    MTC_RAW_BUCKET_URL   where it lands (else RAW_BUCKET_URL)
    MTC_REPLAY_SPEED     1 / 10 / 100, or unset for the ceiling
    MTC_FAULTS           drop,reorder,duplicate,clock_jitter,burst
    MTC_FAULT_SEED       makes any fault run reproducible (0)

``output=parquet`` lands a dlt dataset *directory* of Parquet files under the
configured destination, not a single file.
"""

from __future__ import annotations

from collections.abc import Mapping

from data_pipeline_core import (
    Sink,
    WorkerApp,
    dlt_sink,
    raw_landing_sink,
)
from data_pipeline_core.runtime.logging import get_logger

from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.replay import FaultConfig, ReplaySource
from collector.settings import CollectorSettings
from collector.sinks import ConsoleSink
from collector.source import BinanceFrameSource
from collector.transform import BinanceBookTransform


def _frame_source(settings: CollectorSettings) -> BinanceFrameSource:
    return BinanceFrameSource(
        symbol=settings.symbol,
        duration_s=settings.duration_s,
        ws_url=settings.ws_url,
        rest_url=settings.rest_url,
        snapshot_limit=settings.snapshot_limit,
        depth_interval_ms=settings.depth_interval_ms,
        snapshot_interval_s=settings.snapshot_interval_s,
    )


def _row_sink(settings: CollectorSettings) -> Sink[LevelRow]:
    if settings.output == "console":
        return ConsoleSink()
    return dlt_sink(
        dataset=settings.dataset,
        destination=settings.destination,
        table_name=settings.table_name,
    )


def build_capture_app(
    settings: CollectorSettings,
) -> WorkerApp[CaptureRecord, CaptureRecord]:
    """Ingest worker: the venue's bytes to raw landing, and no book at all."""
    sink: Sink[Mapping[str, object]] = raw_landing_sink(
        settings.raw_channel, bucket_url=settings.raw_bucket_url
    )
    return WorkerApp(_frame_source(settings), sink, settings=settings)


def build_collect_app(
    settings: CollectorSettings, transform: BinanceBookTransform
) -> WorkerApp[CaptureRecord, LevelRow]:
    """The Phase 0 path: socket to level rows, with the book in between."""
    return WorkerApp(
        _frame_source(settings),
        _row_sink(settings),
        transform=transform,
        settings=settings,
    )


def build_replay_app(
    settings: CollectorSettings, transform: BinanceBookTransform
) -> WorkerApp[CaptureRecord, LevelRow]:
    """The same transform, fed from disk instead of from a socket."""
    source = ReplaySource(
        channel=settings.raw_channel,
        bucket_url=settings.raw_bucket_url,
        speed=settings.replay_speed,
        faults=FaultConfig.from_names(settings.faults, seed=settings.fault_seed),
    )
    return WorkerApp(
        source, _row_sink(settings), transform=transform, settings=settings
    )


def main() -> int:
    settings = CollectorSettings()
    if settings.mode == "capture":
        return build_capture_app(settings).run()

    # The transform is held here rather than inside the builder because its
    # counters and the book it ends with are the run's actual result, and
    # ``WorkerApp.run()`` returns only an exit code.
    transform = BinanceBookTransform(symbol=settings.symbol)
    build = build_collect_app if settings.mode == "collect" else build_replay_app
    code = build(settings, transform).run()
    get_logger().info("book", mode=settings.mode, **transform.summary())
    return code


if __name__ == "__main__":
    raise SystemExit(main())
