"""Entry point — four wirings of the same two pieces, nothing else to write.

    MTC_SYMBOL=BTCUSDT MTC_DURATION_S=60 uv run python -m collector.main

Configuration comes from ``CollectorSettings`` (``MTC_*`` env vars, or a
``.env``); structured logging, the resilience stack behind ``ctx.http``,
graceful shutdown and the metrics push all come from the SDK's app.

    MTC_MODE=collect     socket -> book -> level rows -> curated sink
    MTC_MODE=capture     socket -> raw landing, verbatim, no book
    MTC_MODE=replay      raw landing -> book -> level rows, no socket
    MTC_MODE=service     the collect wiring, run until stopped

``collect`` and ``replay`` differ only in where the records come from — the
same ``BookRouter`` builds the books either way, which is what makes a replay a
regression test of the collector rather than of a parallel implementation.

A venue is still a process, and now a process is many connections.
``MTC_SYMBOLS`` names the set, ``collector/shard.py`` packs it onto as many
sockets as the venue's cap and the recovery budget allow, and
``ShardSupervisor`` runs them concurrently behind the one-source contract —
which is therefore still untouched.

**``service`` is the same three objects as ``collect``.** ``ServiceApp`` swaps
the run loop, not the pipeline: the supervisor already stops on
``ctx.should_stop`` and already yields forever, so it was a legal service source
before there was a service to run it. What ``collect`` cannot do is stop on a
signal without losing the partial batch, push metrics more than once, or answer
a health probe — and none of those are properties of the source.

Key knobs:

    MTC_VENUE            binance | coinbase | kraken     (binance)
    MTC_SYMBOL           one symbol, venue-native form   (BTCUSDT)
    MTC_SYMBOLS          a set: comma list, or * for all (falls back to SYMBOL)
    MTC_DURATION_S       how long a bounded live run lasts (60)
    MTC_SHARD_STAGGER_S  delay between shard opens        (1)
    MTC_QUEUE_MAXSIZE    records buffered before dropping (10000)
    MTC_ROTATE_AFTER_S   reconnect each shard this often  (0, off)
    MTC_DEPTH            Kraken book depth, 10..1000     (1000)
    MTC_OUTPUT           parquet | console | clickhouse  (parquet)
    MTC_CLICKHOUSE_DSN   where the curated rows go
    MTC_HEALTH_PORT      service mode's probe endpoint    (8080)
    MTC_RAW_CHANNEL      capture/replay channel name     (<venue>-depth)
    MTC_RAW_BUCKET_URL   where it lands (else RAW_BUCKET_URL)
    MTC_REPLAY_SPEED     1 / 10 / 100, or unset for the ceiling
    MTC_FAULTS           drop,reorder,duplicate,clock_jitter,burst
    MTC_FAULT_SEED       makes any fault run reproducible (0)

``output=parquet`` lands a dlt dataset *directory* of Parquet files under the
configured destination, not a single file.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

from data_pipeline_core import (
    ServiceApp,
    Sink,
    WorkerApp,
    arrow_batching_sink,
    dlt_sink,
    raw_landing_sink,
)
from data_pipeline_core.runtime.logging import get_logger

from collector import adapters
from collector.capture import CaptureRecord
from collector.model import ARROW_SCHEMA, DLT_COLUMNS, LevelRow
from collector.rates import RATES
from collector.replay import FaultConfig, ReplaySource
from collector.router import BookRouter
from collector.settings import CollectorSettings
from collector.shard import plan_shards, shard_size
from collector.sinks import ClickHouseSink, ConsoleSink, WidenedTickSink
from collector.source import FrameSource
from collector.supervisor import ShardSupervisor
from collector.symbols import tradable


def _channel(settings: CollectorSettings) -> str:
    """One raw-landing channel per venue, so a replay knows what it is reading."""
    return settings.raw_channel or f"{settings.venue}-depth"


def resolve_symbols(settings: CollectorSettings) -> tuple[str, ...]:
    """The shard, in the venue's own spelling.

    ``*`` means every overlapping base; a comma list means those; empty means
    the single ``symbol``, which is what every invocation before Phase 3 used.
    """
    if not settings.symbols:
        return (settings.symbol,)
    if settings.symbols.strip() == "*":
        return tradable(settings.venue)
    return tuple(s.strip() for s in settings.symbols.split(",") if s.strip())


def resolve_shards(settings: CollectorSettings) -> tuple[tuple[str, ...], ...]:
    """The symbol set, packed onto as many connections as it needs.

    The shard size is the tighter of the venue's own cap and the recovery
    budget, and the packing is by measured message rate — see
    ``collector/shard.py``. One symbol still gives exactly one shard of one,
    so nothing about a single-symbol run has changed.
    """
    symbols = resolve_symbols(settings)
    venue = adapters.build(settings)
    size = shard_size(
        venue_cap=venue.max_symbols_per_connection,
        recovery_budget_s=settings.recovery_budget_s,
        per_symbol_recovery_s=settings.per_symbol_recovery_s,
        blast_radius=settings.max_symbols_per_shard,
    )
    return plan_shards(symbols, RATES.get(settings.venue, {}), max_per_shard=size)


def _duration(settings: CollectorSettings) -> float:
    """How long a run lasts — and a service does not.

    ``duration_s`` bounds every other mode; in ``service`` the run ends on a
    signal instead, so the deadline has to be one that never arrives. Passing
    the configured value would make a service stop after a minute, and passing
    zero would make it stop at once.
    """
    return math.inf if settings.mode == "service" else settings.duration_s


def _shard_sources(settings: CollectorSettings) -> list[FrameSource]:
    return [
        FrameSource(
            # A factory: one shard needs a transport plus a sequencer per
            # `sequence_key`, and those must not share state.
            venue=lambda: adapters.build(settings),
            symbols=shard,
            duration_s=_duration(settings),
            snapshot_interval_s=settings.snapshot_interval_s,
            subscribe_grace_s=settings.subscribe_grace_s,
            liveness_timeout_s=settings.liveness_timeout_s,
        )
        for shard in resolve_shards(settings)
    ]


def _source(settings: CollectorSettings) -> ShardSupervisor:
    """Always the supervisor, even for one shard.

    A single-shard run through it is the same records in the same order with
    one extra thread and one queue hop, so keeping a separate un-supervised
    path would be a second code path to keep correct for no behavioural gain.
    """
    return ShardSupervisor(
        sources=_shard_sources(settings),
        duration_s=_duration(settings),
        stagger_s=settings.shard_stagger_s,
        queue_maxsize=settings.queue_maxsize,
        rotate_after_s=settings.rotate_after_s,
    )


def _row_sink(settings: CollectorSettings) -> Sink[LevelRow]:
    if settings.output == "console":
        return ConsoleSink()
    if settings.output == "clickhouse":
        # The flush triggers are SDK settings with defaults measured in
        # `bench/clickhouse.py`; the sink itself only knows how to write a
        # batch it is handed. The schema is this project's and is pinned, so
        # every batch is the same shape whatever the rows happened to contain.
        return arrow_batching_sink(
            ClickHouseSink(
                settings.clickhouse_dsn,
                settings.clickhouse_table,
                dedup_window=settings.clickhouse_dedup_window,
            ),
            schema=ARROW_SCHEMA,
            max_rows=settings.batch_max_rows,
            max_seconds=settings.batch_max_seconds,
        )
    return WidenedTickSink(
        dlt_sink(
            dataset=settings.dataset,
            destination=settings.destination,
            table_name=settings.table_name,
            # Pinned, not inferred: a load whose `exchange_ts` is entirely null
            # would otherwise land without the column. See `collector/model.py`.
            columns=DLT_COLUMNS,
        )
    )


def build_capture_app(
    settings: CollectorSettings,
) -> WorkerApp[CaptureRecord, CaptureRecord]:
    """Ingest worker: the venue's bytes to raw landing, and no book at all."""
    sink: Sink[Mapping[str, object]] = raw_landing_sink(
        _channel(settings), bucket_url=settings.raw_bucket_url
    )
    return WorkerApp(_source(settings), sink, settings=settings)


def build_collect_app(
    settings: CollectorSettings, router: BookRouter
) -> WorkerApp[CaptureRecord, LevelRow]:
    """The Phase 0 path: socket to level rows, with the books in between."""
    return WorkerApp(
        _source(settings),
        _row_sink(settings),
        transform=router,
        settings=settings,
    )


def build_service_app(
    settings: CollectorSettings, router: BookRouter
) -> ServiceApp[CaptureRecord, LevelRow]:
    """The collect wiring with no end to it: run until stopped, then drain."""
    return ServiceApp(
        _source(settings),
        _row_sink(settings),
        transform=router,
        settings=settings,
    )


def build_replay_app(
    settings: CollectorSettings, router: BookRouter
) -> WorkerApp[CaptureRecord, LevelRow]:
    """The same router, fed from disk instead of from a socket."""
    source = ReplaySource(
        channel=_channel(settings),
        bucket_url=settings.raw_bucket_url,
        speed=settings.replay_speed,
        faults=FaultConfig.from_names(settings.faults, seed=settings.fault_seed),
    )
    return WorkerApp(source, _row_sink(settings), transform=router, settings=settings)


def main() -> int:
    settings = CollectorSettings()
    if settings.mode == "capture":
        return build_capture_app(settings).run()

    # The router is held here rather than inside the builder because its
    # counters and the books it ends with are the run's actual result, and
    # ``run()`` returns only an exit code.
    router, _ = adapters.build_router(settings, resolve_shards(settings))
    if settings.mode == "service":
        code = build_service_app(settings, router).run()
    elif settings.mode == "collect":
        code = build_collect_app(settings, router).run()
    else:
        code = build_replay_app(settings, router).run()
    get_logger().info("books", mode=settings.mode, **router.summary())
    return code


if __name__ == "__main__":
    raise SystemExit(main())
