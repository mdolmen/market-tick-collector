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

from prometheus_client import CollectorRegistry, Histogram

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
