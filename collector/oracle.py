"""The reconstructed book against the venue's own snapshot of it.

The only independent answer this project has to *is the book correct?*. Every
other check is internal: the sequence says a message was lost, the venue's
checksum says the top ten disagree, the fault injector says the detector fires
on faults the harness itself chose. None of them can see a book that drifts
while staying plausible — sizes applied as increments rather than as absolute
sets, which `NOTES.md` § *The diff stream* names as the worst failure available
here precisely because nothing else catches it.

**It costs no I/O.** The snapshots are already in the capture: the source lands
one every ``snapshot_interval_s`` whether the book needs it or not, and
``BookTransform`` already ignores the ones that arrive at a healthy book. This
reads those. So the oracle runs unchanged over a replay, and its number is
reproducible from a landed file rather than only from a live session.

**It knows no venue**, and cannot: it sees a ``Snapshot``, some ``Update``s and
a ``Book``, all of them past the normalization boundary. Whether a venue's
snapshot is comparable at all is the adapter's answer, given upstream through
``snapshot_supersedes`` — a snapshot that only exists because the stream was
interrupted to ask for it measures the interruption, not the reconstruction.

Two things make the comparison honest, and without either one the number is an
artifact of how it was taken:

**Align by update id, never by clock.** When a snapshot record reaches the
transform the book has already applied the frames that arrived during the REST
round trip, so the two describe different moments. Diffing them there measures
the round trip. The fix is to roll the *snapshot* forward to where the book is
— re-applying the retained frames the snapshot predates — which works only
because sizes are absolute set-to-value, the same property that lets the
bootstrap apply a straddling frame. Where the roll-forward cannot be done the
comparison is skipped and counted; a skipped comparison is never a break.

**Judge only the depth both sides cover.** A REST depth response is the top
``snapshot_limit`` levels a side, while the diff stream is full-depth, so the
book legitimately holds levels the snapshot never described — and the *book*
was bootstrapped from a truncated response too, so it is missing levels the
snapshot describes and it was never given. Comparison is confined to where
both are authoritative, and the claim it backs is therefore divergence *within
that depth*, which is what the summary says. `bootstrapped` carries the second
half of that, and the measurement that found it.

The work is one pass over the snapshot's levels and one over the book's, once
per snapshot interval — 10⁴ dict operations every 300 seconds, against a frame
path doing that many per second. It does not need to be cheaper than it is.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence

from collector.adapters.base import Level, Snapshot, Update
from collector.book import Book

# Applied frames kept back for the roll-forward. It has to cover everything the
# book applied while a snapshot was in flight: one REST round trip against a
# 100ms diff channel is a handful of frames, and this is three orders of
# magnitude of headroom for a slow fetch, a retry or a paced one. Cheap, since
# an `Update` holds the frame's own levels rather than a copy of the book.
_RETAIN = 4096


def _levels(levels: Sequence[Level]) -> dict[int, int]:
    """A snapshot's side as the book holds it: ticks to lots, no zeroes."""
    return {level.price_ticks: level.size_lots for level in levels if level.size_lots}


def _roll(levels: dict[int, int], updates: Iterable[Level]) -> None:
    """One frame's worth of the same absolute-set rule ``Book.apply`` uses."""
    for level in updates:
        if level.size_lots == 0:
            levels.pop(level.price_ticks, None)
        else:
            levels[level.price_ticks] = level.size_lots


class BookOracle:
    """One book's divergence from the venue's periodic snapshots of it."""

    def __init__(self, *, retain: int = _RETAIN) -> None:
        self._retain = retain
        # A deque rather than a list: this is written once per applied frame,
        # on the path every other number in the run is measured over.
        self._recent: deque[Update] = deque()
        # The `final_seq` of the newest frame dropped from the ring. Alignment
        # needs every frame the book applied after the snapshot's position, so
        # this is what says whether one of them is already gone.
        self._evicted: int | None = None
        # The depth the book was bootstrapped with, which bounds what it can be
        # held to. See `bootstrapped`. `None` until the first one arrives, and
        # nothing is compared before that.
        self._floor: int | None = None
        self._ceiling: int | None = None

        # Snapshots actually diffed — the denominator, and the reason `clean`
        # is reportable as a rate rather than as a bare count.
        self.comparisons = 0
        self.clean = 0
        # Snapshots that arrived at a live book and could not be aligned to it.
        # Reported rather than folded into either of the two above: it is
        # coverage lost, not a book that was found wrong.
        self.unaligned = 0
        self.levels_compared = 0
        self.levels_broken = 0

    # --- the transform's side ----------------------------------------------

    def applied(self, event: Update) -> None:
        """A frame the book has just applied, held for the next roll-forward."""
        if len(self._recent) == self._retain:
            self._evicted = self._recent.popleft().final_seq
        self._recent.append(event)

    def reset(self) -> None:
        """The book was rebuilt, so what it applied before says nothing about it.

        Called wherever the book is cleared or goes untrusted. Keeping the ring
        across a re-bootstrap would roll a snapshot forward with frames applied
        to a book that no longer exists.
        """
        self._recent.clear()
        self._evicted = None

    def bootstrapped(self, snapshot: Snapshot) -> None:
        """The book was just built from this, which is the extent of its knowledge.

        **Measured, not assumed** — Phase 6's first live run predicted zero
        breaks and found 173 in 40,001 levels, every one of them one-sided and
        ranked in the deepest 1.5% of the band. See `DEVELOPMENT.md` § *Grading
        the prediction*.

        The cause is here rather than in the book. A bootstrap seeds from a
        `limit`-truncated response, so the book begins knowing nothing beyond
        that snapshot's worst price a side. The diff stream teaches it a deeper
        level only when that level *changes*; one sitting untouched below the
        cut is absent from the book for as long as it stays untouched. When the
        market later moves, a fresh snapshot's band reaches into that region
        and every untouched level in it reads as a break the collector never
        committed.

        So authority is bounded on both ends, and the comparison uses whichever
        of the two bounds is the narrower. It does not widen as the run goes on:
        a diff at a deep price says what *that* level is now, and nothing about
        its neighbours.
        """
        self.reset()
        bids = _levels(snapshot.bids)
        asks = _levels(snapshot.asks)
        self._floor = min(bids) if bids else None
        self._ceiling = max(asks) if asks else None

    # --- the comparison -----------------------------------------------------

    def compare(self, snapshot: Snapshot, book: Book) -> int | None:
        """Diverging levels, or ``None`` when the two could not be aligned.

        The caller has already established that this book is live and that this
        venue's snapshot is an independent read. What is left is arithmetic.
        """
        if not self._alignable(snapshot):
            self.unaligned += 1
            return None

        bids = _levels(snapshot.bids)
        asks = _levels(snapshot.asks)
        # Before the roll-forward: the band is the depth the *venue* answered
        # with, and a frame that deletes the outermost level does not shrink
        # what the snapshot was authoritative about.
        #
        # Narrowed to what the book was ever given, which is the other half of
        # the same truncation — `bootstrapped` is where that is argued. The
        # narrower of the two bounds wins on each side, so the comparison only
        # ever covers depth both of them cover.
        floor = _bid_floor(min(bids) if bids else None, self._floor)
        ceiling = _ask_ceiling(max(asks) if asks else None, self._ceiling)

        for event in self._recent:
            if event.final_seq <= snapshot.final_seq:
                # Wholly reflected in the snapshot already. The frame that
                # straddles its position is not, and is re-applied entire —
                # absolute sets make that exact rather than approximate.
                continue
            _roll(bids, event.bids)
            _roll(asks, event.asks)

        compared, broken = 0, 0
        for expected, actual, edge, inside in (
            (bids, book.bids, floor, _at_or_above),
            (asks, book.asks, ceiling, _at_or_below),
        ):
            if edge is None:
                # The venue returned nothing on this side, so it asserts
                # nothing about it. An empty book side is not evidence either.
                continue
            prices = {p for p in expected if inside(p, edge)}
            prices |= {p for p in actual if inside(p, edge)}
            compared += len(prices)
            broken += sum(1 for p in prices if expected.get(p) != actual.get(p))

        self.comparisons += 1
        if not broken:
            self.clean += 1
        self.levels_compared += compared
        self.levels_broken += broken
        return broken

    def _alignable(self, snapshot: Snapshot) -> bool:
        """Whether the book's position is known and reachable from this snapshot.

        Three ways it is not, and all three are silence rather than error. The
        ring is empty, so nothing says where the book is. The snapshot is ahead
        of the book, which rolls the wrong way and cannot be undone. Or a frame
        the roll-forward needs has already left the ring.
        """
        if not self._recent:
            return False
        if self._recent[-1].final_seq < snapshot.final_seq:
            return False
        return self._evicted is None or snapshot.final_seq >= self._evicted

    # --- the numbers -------------------------------------------------------

    def summary(self) -> dict[str, int]:
        """Prefixed, because these live alongside the transform's own counters.

        ``clean`` over ``comparisons`` is the headline — how many independent
        reads the reconstruction matched exactly — and ``levels_broken`` over
        ``levels_compared`` is its magnitude when it did not. Deliberately not
        a classification of the breaks: `NOTES.md` § *Phase 8 — cut to one
        oracle* cut that, and a count with a denominator is the claim.
        """
        return {
            "oracle_comparisons": self.comparisons,
            "oracle_clean": self.clean,
            "oracle_unaligned": self.unaligned,
            "oracle_levels_compared": self.levels_compared,
            "oracle_levels_broken": self.levels_broken,
        }


def _at_or_above(price: int, edge: int) -> bool:
    return price >= edge


def _at_or_below(price: int, edge: int) -> bool:
    return price <= edge


def _bid_floor(snapshot: int | None, book: int | None) -> int | None:
    """The shallower of two bid floors — a higher price bounds a narrower band.

    ``None`` where either side asserts nothing, which is not the same as an
    empty intersection: it means there is no evidence rather than no agreement.
    """
    return None if snapshot is None or book is None else max(snapshot, book)


def _ask_ceiling(snapshot: int | None, book: int | None) -> int | None:
    """The shallower of two ask ceilings — a lower price bounds a narrower band."""
    return None if snapshot is None or book is None else min(snapshot, book)
