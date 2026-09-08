"""Entry point — three wirings of the same two pieces, nothing else to write.

    MTC_SYMBOL=BTCUSDT MTC_DURATION_S=60 uv run python -m collector.main

Configuration comes from ``CollectorSettings`` (``MTC_*`` env vars, or a
``.env``); structured logging, the resilience stack behind ``ctx.http``,
graceful shutdown and the metrics push all come from ``WorkerApp``.

    MTC_MODE=collect     socket -> book -> level rows -> curated sink
    MTC_MODE=capture     socket -> raw landing, verbatim, no book
    MTC_MODE=replay      raw landing -> book -> level rows, no socket

``collect`` and ``replay`` differ only in where the records come from — the
same ``BookRouter`` builds the books either way, which is what makes a replay a
regression test of the collector rather than of a parallel implementation.

A venue is still a process, and now a process is many connections.
``MTC_SYMBOLS`` names the set, ``collector/shard.py`` packs it onto as many
sockets as the venue's cap and the recovery budget allow, and
``ShardSupervisor`` runs them concurrently behind ``WorkerApp``'s one-source
contract — which is therefore still untouched. Several *venues* at once remains
a Phase 4 problem, because that needs ``ServiceApp``.

Key knobs:

    MTC_VENUE            binance | coinbase | kraken     (binance)
    MTC_SYMBOL           one symbol, venue-native form   (BTCUSDT)
    MTC_SYMBOLS          a set: comma list, or * for all (falls back to SYMBOL)
    MTC_DURATION_S       how long a bounded live run lasts (60)
    MTC_SHARD_STAGGER_S  delay between shard opens        (1)
    MTC_QUEUE_MAXSIZE    records buffered before dropping (10000)
    MTC_ROTATE_AFTER_S   reconnect each shard this often  (0, off)
    MTC_DEPTH            Kraken book depth, 10..1000     (1000)
    MTC_OUTPUT           parquet | console               (parquet)
    MTC_RAW_CHANNEL      capture/replay channel name     (<venue>-depth)
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

from collector import adapters
from collector.capture import CaptureRecord
from collector.model import LevelRow
from collector.rates import RATES
from collector.replay import FaultConfig, ReplaySource
from collector.router import BookRouter
from collector.settings import CollectorSettings
from collector.shard import plan_shards, shard_size
from collector.sinks import ConsoleSink
from collector.source import FrameSource
from collector.supervisor import ShardSupervisor
from collector.symbols import OVERLAPPING_BASES, native


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
        return tuple(native(base, settings.venue) for base in OVERLAPPING_BASES)
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
    )
    return plan_shards(symbols, RATES.get(settings.venue, {}), max_per_shard=size)


def _shard_sources(settings: CollectorSettings) -> list[FrameSource]:
    return [
        FrameSource(
            # A factory: one shard needs a transport plus a sequencer per
            # `sequence_key`, and those must not share state.
            venue=lambda: adapters.build(settings),
            symbols=shard,
            duration_s=settings.duration_s,
            snapshot_interval_s=settings.snapshot_interval_s,
            subscribe_grace_s=settings.subscribe_grace_s,
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
        duration_s=settings.duration_s,
        stagger_s=settings.shard_stagger_s,
        queue_maxsize=settings.queue_maxsize,
        rotate_after_s=settings.rotate_after_s,
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
    # ``WorkerApp.run()`` returns only an exit code.
    router, _ = adapters.build_router(settings, resolve_shards(settings))
    build = build_collect_app if settings.mode == "collect" else build_replay_app
    code = build(settings, router).run()
    get_logger().info("books", mode=settings.mode, **router.summary())
    return code


if __name__ == "__main__":
    raise SystemExit(main())
