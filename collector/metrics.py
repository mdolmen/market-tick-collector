"""This project's own series, on the registry the run already pushes.

`data-pipeline-core`'s standard series are frozen by policy (`ARCHITECTURE.md`
§8) because the Grafana dashboards over them are shared across consumers. This
is the other half of that arrangement: a consumer's business metrics live in the
consumer, and reach the exporter through `ctx.metrics.registry` — the registry
the run actually drains — rather than a second one nothing pushes.

**The venue label is why this is not in the SDK.** `venue` is a business
dimension of this project; a `venue` label on `StandardMetrics` would put crypto
exchanges in the observability contract every other consumer shares. Phase 2
measured this quantity and had nowhere to publish it, and the fix was to make
the registry reachable, not to widen the standard surface.
"""

from __future__ import annotations

from weakref import WeakKeyDictionary

from prometheus_client import CollectorRegistry, Counter, Histogram

# Seconds. The quantity is one-way delay plus clock offset, which on a public
# venue over the internet lands in single-digit to low-hundreds of
# milliseconds; the buckets bracket that and leave headroom for the drift this
# metric exists to make visible.
_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

# One histogram per registry. A `Transform` sees the registry once per record
# and prometheus_client refuses a duplicate registration, so the series is
# created on first use and looked up after. Weak keys so a test that builds a
# registry per case does not accumulate them for the life of the process.
_HISTOGRAMS: WeakKeyDictionary[CollectorRegistry, Histogram] = WeakKeyDictionary()


def clock_difference(registry: CollectorRegistry) -> Histogram:
    """The venue-to-local clock difference histogram, labelled by venue.

    **Not clock skew, and the name says so.** It is `receive_ts - exchange_ts`:
    one-way network delay *plus* the offset between the venue's clock and ours,
    inseparable without a round-trip estimate this collector never makes. See
    `BookTransform.skew_ms` — same quantity, same caveat, one reported per run
    and the other exported continuously.
    """
    histogram = _HISTOGRAMS.get(registry)
    if histogram is None:
        histogram = Histogram(
            "venue_clock_difference_seconds",
            "Venue-to-local clock difference: one-way delay plus clock offset.",
            ["venue"],
            buckets=_BUCKETS,
            registry=registry,
        )
        _HISTOGRAMS[registry] = histogram
    return histogram


# The same first-use registration, for the series that are counters. Keyed by
# metric name within a registry so one dictionary serves all of them.
_COUNTERS: WeakKeyDictionary[CollectorRegistry, dict[str, Counter]] = (
    WeakKeyDictionary()
)


def _counter(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: tuple[str, ...],
) -> Counter:
    counters = _COUNTERS.setdefault(registry, {})
    counter = counters.get(name)
    if counter is None:
        counter = Counter(name, documentation, list(labels), registry=registry)
        counters[name] = counter
    return counter


def oracle_comparisons(registry: CollectorRegistry) -> Counter:
    """Periodic snapshots diffed against the reconstructed book, by outcome.

    One series rather than three because the denominator and the outcomes have
    to be read together: `NOTES.md` § *Validating against the venue's own
    top-N* asks for a break count *with a denominator*, and a bare break count
    is the shape it rejects. `unaligned` is coverage lost rather than a book
    found wrong — see `collector.oracle` — and stays a third value here so it
    can never be mistaken for either.
    """
    return _counter(
        registry,
        "book_oracle_comparisons_total",
        "Reconstructed book against the venue's own snapshot, by outcome.",
        ("venue", "result"),
    )


def oracle_level_breaks(registry: CollectorRegistry) -> Counter:
    """Diverging price levels, which is the magnitude of the count above.

    Separate because one snapshot disagreeing by a single level and one
    disagreeing by a thousand are the same event and very different news.
    """
    return _counter(
        registry,
        "book_oracle_level_breaks_total",
        "Price levels where the reconstructed book and the snapshot disagree.",
        ("venue",),
    )


def checksum_breaks(registry: CollectorRegistry) -> Counter:
    """Frames whose venue-published integrity token disagreed with our view.

    **Never merged with the oracle's count**, which is `TODO.md` § *Phase 6*
    asking for two numbers: they bound different things. The checksum is
    continuous, venue-native and covers the top ten; the oracle is periodic,
    independent and covers the depth a REST read returns. Adding them would
    produce a number that bounds neither.

    Zero on a venue that publishes no token, which is most of them.
    """
    return _counter(
        registry,
        "venue_checksum_breaks_total",
        "Frames whose venue-published integrity token disagreed with our view.",
        ("venue",),
    )
