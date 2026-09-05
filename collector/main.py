"""Entry point — wired through the SDK, nothing else to write.

    MTC_SYMBOL=BTCUSDT MTC_DURATION_S=60 uv run python -m collector.main

Configuration comes from ``CollectorSettings`` (``MTC_*`` env vars, or a
``.env``); structured logging, the resilience stack behind ``ctx.http``,
graceful shutdown and the metrics push all come from ``WorkerApp``. Key knobs:

    MTC_SYMBOL           one symbol, Binance spot        (BTCUSDT)
    MTC_DURATION_S       how long the bounded run lasts  (60)
    MTC_OUTPUT           parquet | console               (parquet)
    MTC_RAW_FRAMES_PATH  dump frames verbatim as JSONL   (unset)

``output=parquet`` lands a dlt dataset *directory* of Parquet files under the
configured destination, not a single file.
"""

from __future__ import annotations

from data_pipeline_core import Sink, WorkerApp, dlt_sink

from collector.model import LevelRow
from collector.settings import CollectorSettings
from collector.sinks import ConsoleSink
from collector.source import BinanceDepthSource


def build_app(settings: CollectorSettings) -> WorkerApp[LevelRow, LevelRow]:
    source = BinanceDepthSource(
        symbol=settings.symbol,
        duration_s=settings.duration_s,
        ws_url=settings.ws_url,
        rest_url=settings.rest_url,
        snapshot_limit=settings.snapshot_limit,
        depth_interval_ms=settings.depth_interval_ms,
        raw_frames_path=settings.raw_frames_path,
    )
    sink: Sink[LevelRow] = (
        ConsoleSink()
        if settings.output == "console"
        else dlt_sink(
            dataset=settings.dataset,
            destination=settings.destination,
            table_name=settings.table_name,
        )
    )
    return WorkerApp(source=source, sink=sink, settings=settings)


def main() -> int:
    return build_app(CollectorSettings()).run()


if __name__ == "__main__":
    raise SystemExit(main())
