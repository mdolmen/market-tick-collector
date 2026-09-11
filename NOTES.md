# Market Tick Collector — design notes

Task list: [TODO.md](TODO.md)

---

## Why this project exists

A **second consumer of `data-pipeline-core`**, chosen for volume.

Target roles (Aug 2026): ABC Arbitrage *Senior Python Developer, Data & Research Platform*;
LMAX *Python Middle Office Developer*; Wintermute *Core Developer (Python)*. Common thread,
from the LMAX posting:

> "building Python tools and services that work with large volumes of financial data, used by
> teams across the business for analysis and reporting. This brings real performance
> challenges around throughput."

`proba-markets-analysis` is well-engineered but produces **0.5 records/sec**. It cannot carry
a throughput claim. Gaps this closes:

1. **Throughput evidence** — the central missing claim for all three roles
2. **Stateful stream processing** — book reconstruction, sequence tracking, gap recovery
3. **async / concurrency, Arrow / columnar** — absent from both existing repos
4. **Proof the SDK generalises** — a service finds leaks poll-and-land never could
5. **Reconciliation** — named by both LMAX and Wintermute, and rare in any portfolio

**Non-goals.** No trading, no strategy, no alpha, no backtesting. This is a data platform.

---

## Scope: generic L2, not crypto

The data is crypto because that is where free high-volume L2 lives. **The project is not
about crypto** and must not drift into it.

**In scope — treated as generic market data:**

- L2 price-level book: snapshot + incremental deltas, per venue per symbol
- Sequence numbers, gap detection, re-snapshot and convergence
- One normalized record model, venue-agnostic, **one row per price level** — not per frame:
  `(venue, symbol, seq, exchange_ts, receive_ts, monotonic_ts, side, price, size, action)`
- `action` carries control events (`snapshot`, `gap`), not only book mutations
- **`price` is never a float** — integer ticks, `Decimal`, or the venue's own decimal string
- Float prices are unstable book keys and break Kraken's string-computed checksum
- One time unit throughout (ns), converted at the adapter; mixed ms/ns is a bug generator
- Trades, if and only if they are needed as a reconciliation oracle

---

## The volume dial — the priority

Volume is the reason this project exists, so it gets decided first and measured continuously.

**Volume = venues × symbols × update rate.** Symbol count is the real knob: a handful of
majors is 10⁴–10⁵ msg/day and proves nothing; a few hundred symbols across several venues is
10⁷+ msg/day. Depth-limited channels (top-N levels, ~100ms coalescing) are far lighter than
full-depth diff streams — that choice is a second knob and it changes the engineering, not
just the number.

**Set a target and write it down before building.** Suggested: **sustained 50k msg/s through
the full path** (decode → book apply → Arrow batch → sink), with the live capture rate
reported separately. A missed target with a profile explaining why is a better artifact than
a met target nobody can reproduce.

### Two numbers, never merged

This is the part most portfolio projects get wrong, so be explicit about it:

1. **Live sustained rate** — bounded by the venues, not by me; report with its symbol set
2. **Replay ceiling** — bounded by my pipeline, and it **excludes the socket path**

The first is honest: real network I/O on a real kernel path, and *not under my control*.
The second is what answers "how fast is your code", which is exactly why it must never be
reported as a capture rate. Two numbers with two labels — "100k msg/s" without saying which
one it is, is the claim an interviewer will take apart.

### The replay harness survives the scope change

Capture raw frames verbatim to disk (the SDK's raw-landing pattern), then replay them at
1× / 10× / 100× with fault injection: dropped frames, reordering, duplicates, sequence gaps,
burst spikes, clock jitter.

It is worth building for three reasons that have nothing to do with Optiq:

- **Reproducible benchmarks** — a live feed is not a controlled experiment; a replay is
- **The gap-recovery path is otherwise untestable** — a venue will not drop packets on request
- Injecting the fault is the only way to assert the book converges after a re-snapshot
- **Regression testing** — replay a known session, assert the resulting book matches

What it no longer is: the centrepiece. It is a test fixture and a benchmark driver, and the
faults it detects are faults it authored — say so.

---

## Book reconstruction

Applying a diff is a dict write. The difficulty is entirely in bootstrap, ordering, and what
happens when a message is lost.

### Bootstrap: the splice

Open the websocket **before** fetching the snapshot. Snapshot-first loses every diff that
lands during the fetch and starts the book silently corrupt.

1. Connect, subscribe, buffer every event — apply nothing
2. `GET /depth` → snapshot with `lastUpdateId = S`, issued through `ctx.http`
3. Discard buffered events entirely in the past: `u <= S`
4. Find the event whose range straddles `S`: `U <= S+1 <= u`
5. Buffer ahead of `S` and no straddle → the snapshot is too old, refetch it
6. Buffer behind `S` and no straddle → the snapshot is fine, keep buffering
7. Apply the straddling event and everything after it

Step 4 is the fiddly one. A single websocket message batches several internal updates, so it
carries a *range* `[U, u]` and the snapshot position lands *inside* a message rather than on
a boundary. The two failure branches are not the same: an old snapshot needs a refetch, a
young one needs patience. Collapsing both into "retry" gives a bootstrap loop that
occasionally spins.

Routing step 2 through `ctx.http` inherits the SDK's retry, jitter, circuit breaker and
status metrics for free. The websocket gets none of them, and that asymmetry is the argument
for § *Connection supervision*.

### Steady state

```python
if event.U != last_u + 1:          # discontinuity
    → GAPPED, tear down, re-bootstrap
for price, size in event.levels:
    if size == 0: book[side].pop(price, None)
    else:         book[side][price] = size   # absolute set, NOT increment
last_u = event.u
```

- **Sizes are absolute, not deltas** — "this level is now 0.4", never "add 0.1"
- Treated as increments the book drifts slowly and stays plausible: the worst failure mode
- That drift is exactly what the top-N oracle below catches, and nothing else does
- **Zero means delete**, and it is the only deletion signal
- An untouched level far from the mid is legitimate, not evidence of a bug

### Sequencing dialects

The per-venue difference confined inside the adapter (§ *Scope*, the one honest exception):

| Class | Bootstrap | Gap detection | Example |
|---|---|---|---|
| Overlapping ranges + out-of-band snapshot | REST fetch, splice above | `U == prev_u + 1` | Binance spot |
| In-band snapshot + monotonic sequence | venue sends snapshot on the socket | `seq == prev_seq + 1` | Coinbase, Bybit |
| In-band snapshot + rolling checksum | venue sends snapshot on the socket | recompute CRC over top-N | Kraken, OKX |

The chosen set is one venue from each row, which is why it is the right set: Binance,
Coinbase and Kraken force `in_sequence` / `gap_detected` / `snapshot_required` to mean three
genuinely different things behind one contract. Kraken's row also validates itself
continuously with no external oracle, so it is the venue that proves the reconstruction
without any help from § *Validating against the venue's own top-N*.

**The row is a property of the feed, not of the venue — DECIDED by measurement.** Phase 2's
probe found Coinbase sitting in two different rows depending on which endpoint is used.
`ws-feed.exchange.coinbase.com` / `level2_batch` sends `l2update` messages with no sequence
number of any kind: it belongs to no row, because an adapter on it cannot detect a lost
message at all. `advanced-trade-ws.coinbase.com` / `level2` carries `sequence_num` on the
envelope and is row two as intended. **Coinbase means Advanced Trade `level2` throughout this
project**, and the reason is written here because the two feeds are otherwise interchangeable
enough to pick the wrong one by accident.

**Kraken is row three and nothing else — DECIDED by measurement, SHIPPED in Phase 2.5.**
Its book messages carry `symbol`, `bids`, `asks`, `checksum`, `timestamp` and **no sequence
at all**. The checksum is not a *supplementary* venue-native check on that venue, as this
table's framing implied; it is the only integrity signal there is. So Kraken did not ship
before its CRC32 — without it `in_sequence` returns `True` unconditionally and the book
drifts silently while staying plausible, which § *Steady state* calls the worst failure mode
available. Both landed in one commit.

**The contract grew one member, `observe` — DECIDED.** The first two rows prove continuity
from a sequence and `chains` already carries that. The third has none, so the venue's own
check had to become a question of its own, asked once per book message by both the capture
source and the transform on their own adapter instances. It returns False on the *edge* — the
message that broke it — leaving `snapshot_required` as the latch for the whole interval,
which is the distinction `gap_detected` and `snapshot_required` already drew.

**Kraken's ordering is a counter of our own — DECIDED, having rejected the alternative.**
The venue's `timestamp` is on every book message and was strictly monotone and unique across
all 5129 messages of a landed session, so it would have worked as a sequence. It is a clock,
and § *Reconciliation* says align by update id and never by clock. A property that holds for
sixty seconds is not a guarantee. So the adapter numbers book messages in arrival order,
`chains` over that is trivially true, and the honesty is the point: sequencing is not
evidence on this venue.

**The decoder never produces a float, for every venue — DECIDED, widened in Phase 2.5.**
Kraken quotes price and qty as JSON numbers, so `json.loads` turns them into floats before
any adapter sees them and `parse_float=str` is what recovers the token. The stated reason was
the checksum, and the stronger one turned up in the code: `repr` of a small float is exponent
notation, `0.00000001` comes back as `1e-08`, and `scaled_int` refuses that outright. The
failure is not a silently wrong book but a dead run on the first dust-sized order — which
makes this a property of the *decode*, not of one adapter, so it lives in
`collector.capture` and applies to all three. Phase 0's decoder ranking does not transfer:
neither `orjson` nor `msgspec` exposes a `parse_float` hook.

**Measured, Phase 2.5:** the token arrives already padded to the instrument's precision, so
the CRC input is the token verbatim and no `price_precision` table is needed.

**A venue's sequence may cover more than its book messages — DECIDED.** Coinbase numbers
*every* message on the connection, subscription acks included. Three consequences, all of
them things the Binance-shaped design got wrong on contact:

- Control traffic is **landed, not discarded**. A capture missing an ack has a hole in the
  sequence and replays as a gap that never happened, so `CaptureRecord.kind` gained `control`.
- An **in-band snapshot occupies a sequence number too**, so it advances the cursor even when
  the book is healthy and the snapshot is otherwise ignored. Missing this is a resubscribe
  loop, each round pulling a 4.9 MB snapshot.
- Both are one question — "does this message move the cursor without being an `Update`" — so
  the adapter answers it with one member, `advance(payload)`.

**A stored in-band snapshot goes stale — DECIDED, after getting it wrong.** The intuition
that an in-band snapshot needs no staleness rule (there is no fetch window, so nothing can be
lost in it) is true on arrival and false afterwards: the transform holds the last snapshot it
saw, and a gap much later is judged against it. Rebuilding from it silently discards every
update in between and leaves a plausible book — the failure mode § *Steady state* names as
the worst available. Both venues use the same rule, `first_buffered > snapshot + 1`, and both
bootstraps reduce to one shared sequence walk.

**Repair on an in-band venue is a resubscribe — DECIDED.** Coinbase never re-sends a snapshot
unasked, so `snapshot_required` drives unsubscribe-then-subscribe: the in-band counterpart of
Binance's REST refetch, on the same trigger, differing only in the action. Measured: the
connection's `sequence_num` continues unbroken across the pair, and a fresh snapshot follows
within a few messages. It also means Phase 1's finding carries over unchanged — recovery
latency is bounded by the snapshot interval, which is a venue property and not a code one.

### The state machine is the artifact

Per `(venue, symbol)`:

```
DISCONNECTED → BUFFERING → SYNCING → LIVE ⇄ GAPPED → BUFFERING
```

Two calls to make explicitly, because the convergence claim depends on both:

- **A gap marks the book untrusted before it repairs it**
- Convergence is measurable only if the untrusted interval starts and ends *in the data*
- **Emit gaps and snapshots as records**, not only as metrics and log lines
- Then replaying landed data reproduces the transitions — the harness tests *this* book

### Validating against the venue's own top-N — DECIDED

Subscribe to the depth-limited channel alongside the full-depth diff stream on a subset of
symbols. The venue's top-N becomes a continuous independent oracle: a break count with a
denominator, at 100ms resolution, rather than a periodic REST diff. Cost is one extra
subscription per audited symbol.

Two ways to produce a break rate that is pure artifact:

- **Align by update id, not by clock** — two sockets mean two independent delivery paths
- Comparing at equal wall-clock measures socket skew and reports it as book error
- Compare their snapshot at id `X` against the book as of applied id `X`
- That needs a small ring of recent top-N versions kept per audited symbol
- Where a venue's depth-limited channel exposes no id, say the comparison is not rigorous
- **Compare the top N−1 levels, not N** — the truncation boundary is an artifact
- A level can sit inside the reconstructed book and outside the venue's cut at one instant

### The REST oracle — DECIDED, and built

Oracle 1 of § *Phase 8 — cut to one oracle*, in `collector/oracle.py`. The section above is
Oracle 2 and stays cut; the two share their honesty rules and nothing else.

**It costs no I/O.** `snapshot_interval_s` already lands a snapshot whether the book needs
one or not, and `BookTransform._on_snapshot` already ignores the ones that arrive at a
healthy book. The oracle reads exactly those. So it runs unchanged over a replay, and its
number is reproducible from a landed file rather than only from a live session — which is
the difference between a benchmark and an anecdote, and the same standard § *Two numbers,
never merged* holds the throughput claim to.

**Roll the snapshot forward; never roll the clock.** The alignment problem of the section
above exists here too, in a different shape: a snapshot record reaches the transform after
the frames that arrived during the REST round trip have already been applied, so the two
describe different moments and diffing them measures the round trip. The ring of recent
*book versions* Oracle 2 needed is not required — a ring of recent *frames* is, and it is
far cheaper. Re-apply the retained frames the snapshot predates onto a copy of it and the
copy lands where the book is. This works only because sizes are absolute set-to-value,
which is the same property that lets the bootstrap apply a straddling frame whole.

Where the roll-forward cannot be done — ring empty, snapshot ahead of the book, or a needed
frame already evicted — the comparison is **skipped and counted as `unaligned`**. Coverage
lost is not a book found wrong, and the two must never share a counter.

**Judge only the depth the response covers.** The § *top N−1* rule, restated for a REST
read: `snapshot_limit` is 5000 and the diff stream is full-depth, so the book legitimately
holds levels the snapshot never described. Comparison is confined to the price span the
snapshot itself asserts. The claim it backs is divergence *within that depth*, and saying
so is the whole of what makes it a claim.

**And only the depth the book was given — MEASURED, and the reason the first prediction was
wrong.** Truncation cuts both ways, and the second way was not predicted. The bootstrap
seeds from a `limit`-truncated response too, so the book starts knowing nothing past *its*
worst price a side, and the diff stream teaches it a deeper level only when that level
changes. A level sitting untouched below the opening cut is absent from the book for as long
as it stays untouched. When the market moves, a later snapshot's band reaches into that
region and asks the book about levels it was never given.

The first live run predicted zero breaks and measured 173 in 40,001 levels over a session
with no gaps — and the signature said which of the two it was without any ambiguity at all:
every break one-sided (the snapshot had a level, the book did not), zero size
disagreements anywhere, and every one of them in the deepest 1.5% of the band. A book that
was actually wrong disagrees about *sizes*, near the mid, on levels it was told about.

So the comparison band is the intersection of the two, and the same capture then reported 0
breaks in 39,828 levels — the denominator falling by exactly the 173 that had been breaking.
That equality is the check that matters: the fix drops levels the book could not know rather
than hiding levels it got wrong. `DEVELOPMENT.md` § *Phase 6* grades the prediction.

The general lesson is the one worth carrying: **an oracle's first job is to be wrong about
nothing.** A divergence rate made of artifacts is worse than no divergence rate, because it
is quotable. Both of the honesty rules above and both halves of the band exist to make the
number small for the right reason, and the run that produced a non-zero number was more
useful than the one that produced zero would have been.

**Only a live book, and only a non-superseding snapshot.** The first is `TODO.md`'s own
condition and `BookTransform.live`'s docstring: an untrusted book says where it stopped, and
diffing it manufactures breaks out of an interval the run already reports as a gap. The
second is the adapter's existing answer, and it is why this venue set gets one oracle rather
than three — see below.

### Why Coinbase has no oracle, and Kraken does not need one — DECIDED

`TODO.md` said "all three venues" and that was wrong. Only Binance has an independent book
read that aligns by id: REST `lastUpdateId` shares a numbering space with the socket's
`[U, u]` range, and the read does not touch the socket.

- **Coinbase.** Advanced Trade's `sequence_num` counts the *connection*, so a second
  connection's snapshot is in a numbering space the first one's book knows nothing about,
  and the Exchange REST book belongs to a different API's sequence entirely. The only
  Coinbase snapshot that aligns is the one `resubscribe_frames` asks for — and
  `CoinbaseAdapter.snapshot_supersedes` is precisely the argument against using it: that
  read interrupts the diff stream, so every level deleted while unsubscribed is absent from
  the snapshot without arriving as a delete. Those read as breaks the measurement itself
  caused. Worse, taking one costs a full re-bootstrap of a ~4.9 MB snapshot and opens an
  untrusted interval, so it would contaminate the convergence numbers sitting beside it.
- **Kraken.** Same shape, and its REST depth carries no sequence at all — there is nothing
  to align on even in principle. It does not need one: the CRC32 validates the top ten
  continuously, which is a stronger claim than a periodic diff over the same levels.

So the reported position is: Binance verified against an independent periodic read, Kraken
by its own checksum, Coinbase unverified against the venue and *stated* as unverified. That
is the § *Re-scope* trade taken again — the smaller claim that is backed beats the larger
one that is not, and this one is defensible out loud in a way "we diffed all three" would
not survive a follow-up question.

**The two numbers stay two numbers.** The oracle's break count bounds periodic, independent
divergence within a REST depth; the checksum's bounds continuous, venue-native divergence
over the top ten. `venue_checksum_breaks_total` exists because the second one previously
did not: `observe`'s verdict reaches the book through `gap_detected`, so a Kraken run
reported it inside `gaps` and it could not be recovered afterwards.

---

## Open decisions

### Venues and channels — DECIDED

Venues: Binance, Coinbase, Kraken.

Symbols: All the overlapping base assets. **188 of them**, resolved 2026-09-06 against live
instrument lists and committed in `collector/symbols.py`, regenerated by `tools/symbols.py`.
The venue-native spelling is a rule (`BASE` + separator + quote) plus two exceptions, both
Kraken's: `XBT` for bitcoin and `XDG` for dogecoin.

`BTCUSDT` and `BTC-USD` are **different instruments** — same base, different quote, two
prices about two markets. The canonical id carries both halves so nothing downstream is
tempted into a cross-venue price comparison this project does not support.

**Sequenced, after the Phase 2 probe:** Binance spot and Coinbase Advanced Trade `level2`
land in Phase 2. Kraken lands in its own phase together with its CRC32, because it has no
other gap detection — see § *Sequencing dialects*. The venue set is unchanged; only its
order is.

### Receiver architecture — DECIDED

Dedicated reader task/thread per connection → bounded ring buffer →
decode/book workers, with asyncio confined to connection management and the control/health
plane.

### Connection supervision — DECIDED

N connections, each with its own backoff, liveness and resubscribe, none of them allowed to
take down the others or the shared book state.

**One stream per symbol: yes.** It is the venue's own unit and it lines up exactly with the
per-`(venue, symbol)` state machine. **One connection per venue: no** — see below.

#### Verified venue limits (checked Sep 2026, re-check before building)

| Venue | Cap per connection | Other limits |
|---|---|---|
| Binance spot | **1024 streams** | 5 outbound msg/s; 300 connects per 5 min per IP; **forced close at 24h** |
| Kraken v2 | **200 symbols** | rate counter 200/s (500 pro), scaled by depth; one depth per symbol |
| Coinbase Advanced Trade | **30 products** | `level2` public, no auth; **snapshot ~4.9 MB in band** |

**The Coinbase cell is now measured, 2026-09-08, and it is the smallest cap by a
factor of thirty.** Binary search over a live subscribe: 30 products accepted, 31
answered `{"type":"error","message":"too many L2 streams requested in a single
session"}` on a socket that then stays open and silent — the same shape of failure
`_assert_the_subscription_took` exists for. 188 symbols therefore needs **seven
connections on Coinbase before recovery time is considered at all**.

The Coinbase row is filled from what the Phase 2 probe actually observed, and the cells it
could not observe say so. A 15-second read of one symbol establishes the channel surface; it
does not establish a connection cap or a rate limit, and writing a number there from
documentation would be exactly the recall this phase's first item exists to avoid. **Both
remain open, and Phase 3 is where they bind** — it is the phase that sizes shards, and it
should measure them rather than inherit a guess from here.

What the probe does establish for Coinbase: no authentication is needed for `level2`,
subscriptions go out as one JSON frame carrying a `product_ids` list (so batching is natural
rather than a workaround), and the in-band snapshot is ~4.9 MB for BTC-USD — which is a
transport constraint in its own right, since it exceeds the `websockets` 1 MiB default frame
cap and kills the connection mid-snapshot until `max_size` is raised.

The "5 incoming messages/sec" is the rate at which *we* may send frames — SUBSCRIBE,
UNSUBSCRIBE, PONG — not market data. Subscribing symbol by symbol would take 80 seconds for
400 symbols, so subscriptions go out batched in one frame or via the combined-stream URL.

#### Why one connection per venue fails

- **It is already impossible** — Kraken caps at 200 symbols and the target is a few hundred
- **No decode parallelism** — one connection is one read loop is one core, under a 50k target
- It also leaves § *Receiver architecture* untestable: one connection cannot feed N readers
- **Head-of-line coupling** — all symbols share one TCP/TLS session and one kernel buffer
- A burst on BTC delays every other symbol and inflates its `receive_ts`, corrupting skew
- **Blast radius** — one disconnect sends every book on that venue to GAPPED at one instant
- All of them then need a REST snapshot at once: a stampede, exactly while degraded
- Recovery time scales with symbol count instead of staying constant
- Binance's 24h force-close makes that a **certainty, not a risk** — a daily venue outage

The opposite extreme is equally wrong: one connection per symbol hits the 300-connects-per-5-
minutes limit on startup, plus a TLS handshake, an FD and a reconnect timer per symbol.

#### The shape that is right

Stream is the *logical* unit, connection is the *transport* unit, and the map between them is
a **sharding policy**. Shard size is the minimum of three constraints:

- The venue cap — 1024 on Binance, 200 on Kraken
- The blast radius accepted — how many books may go untrusted at once
- The decode parallelism needed — connections must be ≥ the readers we intend to run

**The venue cap is never the binding constraint; recovery time is.** If REST snapshots are
limited to roughly 10/s and recovery must finish inside 5 seconds, that is ~50 symbols per
connection — five to twenty times below either cap. Size shards by recovery time and the
published caps never come into it. That inversion is the point worth saying out loud.

Three consequences:

- **Bin-pack by message rate, not symbol count** — BTC and ETH on one socket defeats it
- **Stagger connection opens**, or Binance's 24h expiry synchronises every shard's death
- **Make-before-break at ~23h** — open the replacement, bootstrap it, then cut over
- That converts a guaranteed daily gap into zero gap, at the cost of brief double capacity

**The daily disconnect is a gift.** It is a real, recurring, venue-authored gap-recovery
exercise — the only fault in the system that is not self-injected. Measure convergence
against it and report that number separately from the harness's injected faults.

#### Phase 3 predictions, written before the measurements

`CLAUDE.md` § *Numbers, not adjectives* asks for the prediction first, and a wrong one is
part of the record rather than something to quietly correct. Six, written 2026-09-07 with
nothing measured yet beyond Phase 2's single-symbol probes.

1. **Neither Coinbase nor Kraken batches several symbols into one message.** Kraken v2 sends
   one `data[]` entry per book message and Coinbase Advanced Trade one event per product, so
   the `_entry` / `_event` guards (`adapters/kraken.py:330`, `adapters/coinbase.py:181`) will
   not fire on a multiplexed subscription. This is the prediction with the most riding on it:
   if it is wrong, one capture record covers several streams and the one-tag-per-record model
   in `capture.py` has to grow a fan-out path.
2. **Coinbase's `sequence_num` stays one unbroken chain across a multi-product subscribe.**
   It counts messages on the *connection*, so multiplexing should not perturb it at all.
3. **Message rate across the 188 is heavily skewed** — the top ten bases carry more than half
   the total on every venue, and the tail is close to silent. If the distribution turns out
   flat, bin-packing by rate buys nothing over round-robin and the sharder should say so.
4. **Recovery time, not the venue cap, binds — and by a wide margin.** ~50 symbols per
   connection against a 5s budget, five to twenty times below either published cap.
5. **Kraken at depth 1000 × 188 symbols does not survive**, on the depth-scaled rate counter
   or on in-band snapshot bandwidth at subscribe. Phase 2.5 chose that ceiling for *one*
   symbol; the prediction is that Phase 3 has to lower it, and the measurement is the reason.
6. **Binance is the only venue where rotation is needed.** The 24h force-close is the only
   observed one; the other two cells in the table above are unmeasured, and a venue that does
   not close the socket does not need a replacement opened for it.

#### What the multi-symbol probe found, 2026-09-07

Two of the six predictions above are now settled, both by a 120s three-symbol probe per
venue. Reproduce with:

```
uv run python -m tools.probe coinbase --url wss://advanced-trade-ws.coinbase.com \
  --subscribe '{"type":"subscribe","product_ids":["BTC-USD","ETH-USD","SOL-USD"],"channel":"level2"}' \
  --duration 120 --land --channel coinbase-multi --bucket-url file://data/capture
uv run python -m tools.probe kraken \
  --subscribe '{"method":"subscribe","params":{"channel":"book","symbol":["BTC/USD","ETH/USD","SOL/USD"],"depth":1000}}' \
  --duration 120 --land --channel kraken-multi --bucket-url file://data/capture
```

**Prediction 1 holds: neither venue batches symbols into one message.** Coinbase, 5801 book
messages, every one `len(events) == 1` and never more than one `product_id`. Kraken, 25635
book messages, every one `len(data) == 1` and never more than one `symbol`. The `_entry` and
`_event` guards stay guards, and each takes a symbol to select by rather than growing a
fan-out path.

**Prediction 2 holds, and it is worse news than it sounds.** Coinbase's `sequence_num` ran
0 → 5801 over the three products with **zero breaks**, ack included: one unbroken
connection-wide chain, exactly as predicted. Which means each *product's* messages are
non-adjacent in it — BTC-USD held 2229 of 5802 — and `snapshot_stale` is
`first_buffered_seq > snapshot_seq + 1`. Adjacency was only ever exact at N=1.

Replaying that landed capture per product through today's `BookTransform`, which is what a
shard's router will do:

| product | messages | bootstraps | live at exit | untrusted frames |
|---|---|---|---|---|
| BTC-USD | 2230 | 1, then gapped at record 8 | **no** | 2221 |
| ETH-USD | 1926 | 1, then gapped at record 7 | **no** | 1919 |
| SOL-USD | 1648 | **0 — never bootstrapped** | **no** | 1646 |

Every book on a multiplexed Coinbase connection is dead, by two mechanisms rather than one:
a product whose snapshot arrives before its first frame bootstraps once and then gaps on the
very next one with no way back, and a product whose first frame beats its snapshot never
bootstraps at all. Both reduce to the same cause and the same one-line fix — the connection,
not the symbol, is what can lose a message, so staleness has to be judged against when the
connection last broke rather than against sequence adjacency. See § *Sequencing dialects*.

**Also observed, unpredicted:** three symbols on Kraken at depth 1000 already cost 214.9
msg/s and a 8.8 MB two-minute landing, against Coinbase's 48.8 msg/s.

#### The full-set measurement, 2026-09-08 — and two predictions were wrong

All 188 symbols per venue, ~300s, `uv run python -m tools.rates <venue> --write`, committed
to `collector/rates.py`.

| venue | connections | total | top 1 | **top 10** | median | min |
|---|---|---|---|---|---|---|
| binance | 1 | 371.2 msg/s | 2.7% | **20.1%** | 1.28 | 0.16 |
| coinbase | 7 (capped at 30) | 747.0 msg/s | 2.2% | **18.7%** | 2.63 | 0.03 |
| kraken (depth 1000) | 1 | 2188.2 msg/s | 4.9% | **32.9%** | 5.62 | 0.00 |

**Prediction 3 was wrong, and it is the one that matters.** The claim was that the top ten
bases carry *more than half* the traffic on every venue. They carry 18.7% to 32.9%, and the
busiest single symbol is 2.2% to 4.9% of its venue. The distribution is far flatter than
assumed — there is no BTC-and-ETH-dominate-everything effect to design around.

Two consequences, and the first one contradicts this section's own advice above:

- **"BTC and ETH must not share a socket" was solving a problem that does not exist at this
  scale.** Any shard of ~30 symbols drawn from this distribution lands within a few percent
  of the mean whichever way it is packed. Bin-packing by rate is still the right default —
  it costs fifteen lines and it is robust to a distribution that *becomes* skewed, which one
  news event does — but it must be honest that against round-robin it currently buys almost
  nothing. The measurement, not the intuition, is the thing to cite.
- **A flat tail is the expensive half.** 173 of Coinbase's 188 carry 74% of its traffic
  between them. Shard sizing is therefore governed by symbol *count* far more than by which
  symbols, which is the opposite of what "bin-pack by message rate, not symbol count" implies.

**Prediction 5 was wrong.** Kraken carried all 188 symbols at depth 1000 on one connection,
2188.2 msg/s, every symbol heard from — no rate-counter rejection and no subscribe failure.
The Phase 2.5 depth ceiling survives the full set and does not need lowering. It is still
by far the most expensive venue per symbol: 5.9× Binance's total rate for the same 188.

**Prediction 4 holds on two venues of three, and Coinbase is the exception.** The cap is not
binding on Binance (188 of 1024) or Kraken (188 of 200 — close, but it fits). On Coinbase the
cap is 30 and forces seven connections before any recovery-time budget is applied, so
*"the venue cap is never the binding constraint; recovery time is"* is now known to be a
statement about two venues rather than a general rule. Where a cap is small enough it binds
first, and the sharder has to take the minimum of the two rather than assume which wins.

#### Sharding policy — DECIDED, and three of this section's claims were wrong

Built in Phase 3 as `collector/shard.py`: one pure function, LPT greedy, fifteen lines.
Sort symbols by measured rate descending, put each on the least-loaded shard that has room,
derive the shard count from the size so the packing cannot fail. Deterministic tie-breaks,
so a plan is reproducible and committable.

**Shard size is `min(venue cap, blast radius, recovery budget ÷ per-symbol cost)`.** The
last term is disabled by default, because measuring it disproved it — see below. What binds
in practice is the blast radius, defaulted to 30, which is also Coinbase's cap.

Three things this section asserted that the measurements contradict:

1. **"The venue cap is never the binding constraint; recovery time is."** False on Coinbase,
   whose `level2` cap is 30 and forces seven connections for 188 symbols before any budget
   applies. True in the trivial sense on the other two, where nothing binds but blast radius.
2. **"Bin-pack by message rate, not symbol count — BTC and ETH on one socket defeats it."**
   The distribution is flat: the top ten carry 18.7–32.9% and the busiest single symbol is
   2.2–4.9%. Any shard of a few dozen lands within 0.2 msg/s of the mean however it is
   packed. Bin-packing stays because it costs fifteen lines and survives a distribution that
   *becomes* skewed, but it buys ~nothing today and the code says so.
3. **"If REST snapshots are limited to roughly 10/s and recovery must finish inside 5
   seconds, that is ~50 symbols per connection."** Both halves are wrong. Binance's real
   limit is request *weight*: a full-depth snapshot is 250 of a 6000/min budget, so 24 a
   minute rather than 10 a second — twenty-four times slower than assumed, and exceeding it
   is an **IP ban** (`418`), not a rejected request. And recovery time does not scale with
   symbol count the way the arithmetic assumes; see below.

**Recovery time does not size a shard, and that is the phase's most useful negative result.**
`tools/recovery.py`, time from connect to every book LIVE, 2026-09-08:

| venue | 5 symbols | 10 | 20 | per symbol at 20 |
|---|---|---|---|---|
| kraken | 3.95s | 9.65s | 15.43s | 0.77s |
| coinbase | 11.23s | 14.38s | 16.80s | 0.84s |
| binance | 17.83s | 42.81s | 18.66s | 0.93s |

The per-symbol figure *falls* as the shard grows — 3.57s to 0.93s on Binance — so it is not
a constant that a budget can be divided by. The cause is structural: a book goes live on its
own first frame, so a shard converges at the pace of its **quietest** symbol. That is a
`max`, not a `sum`, and it means the 5s budget above is unreachable at any shard size
including one. Dividing a budget by a per-symbol cost is the wrong shape, and the term
survives in the code only for a venue whose repair cost is genuinely linear.

#### The snapshot storm is a rate limit on *weight*, and it bans rather than throttles

Found by running it: 188 unpaced Binance snapshots landed 56 records in two minutes and
died on `{"code":-1003,"msg":"Way too much request weight used; IP banned until ..."}`. The
weight table for `GET /api/v3/depth` is 5 / 25 / 50 / 250 by requested depth against 6000 a
minute, so full depth is 24 snapshots a minute and 2.5s apart — **~8 minutes to bootstrap
188 symbols**, which is a real cost of full-depth snapshots and an argument for a shallower
`snapshot_limit` that Phase 6 should weigh rather than this phase.

The pacer is one object for the run, not one per connection: the budget is per IP, so pacing
each socket separately exceeds it by a factor of the shard count. The spacing comes from the
adapter, because the weight table is a venue value.

#### One connection per symbol is *also* wrong, for a reason nobody wrote down

`sequence_num` on Coinbase counts every message on the socket. That was known. What was not
is that it makes continuity a property of the **connection**, so no per-symbol object can
judge it: a book only sees its own records, and a book that is *buffering* never advances
the cursor at all, stranding every other book behind it. `BookRouter` owns the check, one
gate per connection — two shards of the same venue have unrelated sequence spaces, so a
single gate across a run reads as a break on nearly every record.

The blast radius follows and is not a choice: one lost message on Coinbase sends every book
on that connection untrusted, because the sequence cannot say which product it described.
That is what makes blast radius the constraint worth setting shard size by.

#### `SCALE = 8` does not hold across the full symbol set — DECIDED, by narrowing

Phase 2 committed one project-wide `SCALE = 8` and recorded it as *"all three venues
measured at eight places"*. That was measured over a handful of symbols. Across all 188 at
the depth the collector subscribes, Kraken quotes three sub-cent assets at **nine**:
`BONK/USD`, `PEPE/USD`, `SHIB/USD` — `price=0.000003072`. Binance and Coinbase quote
nothing past eight.

`scaled_int` refuses to truncate rather than lose a digit silently, which is right, so the
run died — and died *invisibly*, because the exception escaped the reader thread and the
supervisor caught only transport errors. Three of seven Kraken shards were dead while the
summary reported `failures_per_shard` all zero and 163 of 188 streams landed.

**Decided: exclude the three, do not widen `SCALE`.** Widening is a project-wide data-model
change reopening a measured Phase 2 decision, for three memecoins. `collector/symbols.py`
holds the list and `tools/precision.py` derives it — and it must be re-derived after any
relisting, because a sub-cent asset is exactly what gets listed. The extra digits live at
the *bottom* of the book: the same 188 symbols at Kraken depth 10 are entirely clean, so
the list is a function of the subscribed depth.

The supervisor now reports a shard that stopped for a reason no reconnect can fix, which is
what makes the next one visible.

#### Moving the snapshot fetch off the reader thread broke two things first

Both were invisible until a 188-symbol run, and both are worth keeping:

- **The pacer blocked the reader.** Waiting for the venue's request budget with the socket
  unread let the liveness watchdog declare the feed dead: 25 reconnects across seven shards,
  each re-subscribing and re-fetching what it had just abandoned, 3638 records in five
  minutes. The fetch now runs on one thread of its own.
- **The refetch budget was then spent in milliseconds.** With the fetch asynchronous,
  `_snapshot_due` is reachable before the answer arrives, so it re-judged the same unchanged
  sequence numbers and counted each as a refetch — five identical warnings 20ms apart, then
  a dead shard, four of seven. Inline fetching had hidden it because the state was always
  current by the next frame.

#### What Phase 3 actually delivers, measured 2026-09-08/09

Capture of every tradable overlapping symbol, sharded, 150–240s per venue:

| venue | shards | symbols landed | records | drops | reconnects | fatal |
|---|---|---|---|---|---|---|
| binance | 7 | 188/188 | 288,676 | 0 | 0 | 0 |
| coinbase | 7 | 188/188 | 127,728 | 0 | 0 | 0 |
| kraken | 7 | 185/185 | 560,273 | 0 | 0 | 0 |

Replaying those captures back through the router, which is the property that says sharding
is invisible downstream:

| venue | symbols | frames | rows | gaps | crossed | unrouted | live at exit |
|---|---|---|---|---|---|---|---|
| coinbase | 188 | 127,537 | 1,195,352 | 0 | 0 | 0 | 188 |
| kraken | 185 | 560,088 | 921,047 | 0 | 0 | 0 | 184 |

Break-before-make rotation, Kraken, four symbols, two rotations in 90s: **0 gaps**, three
bootstraps per book, ~1 buffered frame each. A clean close is answered with a fresh
snapshot, so a planned rotation costs a re-bootstrap rather than a gap. That is the number
Phase 4's make-before-break has to beat, and the margin is thin.

#### Two halves, and they belong in different repos

1. **Generic:** the supervisor, backoff policy, ping/pong liveness, per-connection health
2. That is the streaming analogue of what `HttpClient` already does per request
3. The concrete SDK move is extracting backoff out of `HttpClient._backoff` for both paths
4. **Consumer:** how many streams fit a socket, which symbols shard where, the venue caps
5. Those are config and adapter values, never SDK constants

### Book representation — DECIDED

NumPy is likely wrong: L2 updates are scalar point-mutations at arbitrary price levels, and
per-element NumPy access from Python is slower than a dict. Real candidates: `dict` keyed by
price, sorted array with binary search + memmove, or a fixed-width array indexed by
ticks-from-mid. Benchmark before committing. Write the prediction down first.

**The read pattern picks the structure, not the write pattern.** Applying a diff is a point
write and is cheap under every candidate. Best bid/ask is the hot read — needed for the top-N
oracle, for latency metrics, for anything downstream — and over a plain `dict` it is an O(n)
scan of every level. Benchmark against a realistic read:write ratio or the answer will be
wrong.

Go for a dict.

**That condition was tested in Phase 2.5 and does not bind — DECIDED.** The worry was that a
rolling-checksum venue recomputes over the top N on every update, turning the ordered top-N
read from an occasional oracle cost into a per-frame one, which is the read a plain `dict` is
worst at. The per-frame cost is real. It does not land here: the CRC is computed over the top
ten *as the venue spelled them*, and `Book` is keyed by integer ticks and holds no strings,
so it cannot serve that read at all. Kraken's adapter keeps its own view for it, bounded by
the subscribed depth, and `Book` never sees a checksum. The dict stands and Phase 6
benchmarks the read:write ratio it always faced.

The cost of that view, from `bench/checksum.py` over a 5129-message session at depth 1000:
73.9µs a message sorting the side per message, 26.0µs over integer keys with each level's
token cached, **8.5µs** rebuilding the window only when a write could have moved it. The
median message touches one level and only 18.5% of sides needed a rebuild.

**A second book per `(venue, symbol)` is the price — DECIDED, and it is a real cost.** It
contradicts "one book per `(venue, symbol)`" above. The alternative was putting the venue's
decimal strings into `Book`, which pushes a venue-shaped need downstream, inflates the
structure Phase 6 is about to benchmark, and leaves the capture source unable to detect a
break at all — the source holds an adapter but never a book, so with the view in the adapter
it can resubscribe on the failing checksum within one message instead of waiting for the
periodic snapshot.

### Backpressure — DECIDED in shape, sizes by measurement

**Never on the socket.** A market data feed has no flow control: the venue does not slow down
for us. Stop reading and the TCP receive window closes, the venue's send buffer fills, and
its answer is to disconnect — costing a full re-bootstrap of every book on that connection.
Blocking upstream is therefore *strictly more expensive* than dropping, and that asymmetry
decides everything below.

```
kernel rcvbuf → reader → [ring buffer] → decode/normalize → book apply → [batch queue] → sink
      1                       2                                               3
```

- **1. Kernel receive buffer** — not a valve, only a size (`SO_RCVBUF`) and an alarm
- It fills before anything downstream notices, so its depth is the earliest warning signal
- **2. Ring buffer, raw frames** — bounded, drop under pressure, never block
- **3. Batch queue, normalized records** — bounded, spill to disk, never push back on 2

**A drop at 2 is a sequence gap, and gaps are already solved here.** It runs the machinery
that exists anyway: `gap_detected` → book untrusted → re-snapshot → converge. It is
indistinguishable from a network drop and it cannot corrupt a book silently. Drop whole
frames only — a partial frame is undetectable corruption — and emit the drop as the same
`gap` control record so replay reproduces it.

**A drop at 3 is a different failure entirely.** The book is in memory and stays correct; what
is lost is landed history, not state. Hence the looser policy: spill rather than drop, and
never block, because blocking here propagates. That is the failure mode to design against —
book-apply blocks on the sink, decode blocks on book-apply, the reader blocks, the kernel
buffer fills, the venue disconnects, and **a slow ClickHouse insert has become a market data
outage**. Raw landing on its own path is the real defence: with raw frames already on disk, a
slow sink can lose nothing, because it can always be replayed.

**Shed whole symbols, not scattered messages.** Scattered drops break every symbol's sequence
at once, so every book goes untrusted together and the re-snapshot stampede arrives — the
same failure connection sharding exists to avoid. Shedding symbol X leaves X untrusted and
every other book perfect. The unit of correctness is the book, so the unit of shedding is the
symbol. 240 correct symbols is a defensible operational state; 300 symbols with holes is not.

#### Sizing the queues

A bounded queue absorbs **variance, not deficit**. If arrival exceeds service on average, no
depth saves it — a bigger queue only delays the drop and adds latency first. So the number
comes from two bounds, and the smaller wins:

- **Burst floor:** `depth ≥ (peak_arrival − service_rate) × burst_duration`
- **Latency ceiling:** `depth ≤ latency_budget × service_rate`

The ceiling is the one people forget. **Queue depth is latency**: a million slots at 50k
msg/s is twenty seconds of staleness, and twenty-second-old book updates are not data worth
having. The p99 receive-to-disk claim in § *What "done" has to be able to say* is the budget,
so it sizes the queue. If the floor exceeds the ceiling that is not a sizing problem, it is a
capacity problem — fix the consumer, never grow the queue.

Three inputs, all measurable, none guessable:

- Peak arrival rate and p99 burst duration — from a capture, not from intuition
- Sustained service rate of the consumer — from the replay harness
- Bytes per slot — depth × record size is a real memory limit in a container

Per queue the answers differ. The ring buffer at 2 has a tight latency budget and should be
sized to swallow a routine burst but not a backlog: absorbing 100ms is obviously worth it,
absorbing 10s is worse than taking the gap. The batch queue at 3 has a loose latency budget
because it serves durability rather than liveness, and once it spills to disk its "size" is
disk, not RAM.

Two details that matter more than they look:

- Powers of two for the ring, so the index wraps with a mask instead of a modulo
- **High and low watermarks, not one threshold** — shed at high, resume at low
- A single threshold flaps: shed at full, resume at 99%, and it oscillates under load
- Sizes are config with measured defaults, never SDK constants — the SDK's own rule

#### The metric surface

`messages_dropped_total`, `queue_depth` and a drop reason label do not exist in the SDK, and
its metric surface is frozen by policy (`ARCHITECTURE.md` §8: names and labels are not to be
renamed or relabelled, because the Grafana dashboards are shared across consumers). Adding
them is a deliberate contract change with a dashboard consequence, not a drive-by commit.

**One collision to avoid.** `StandardMetrics` already uses `stage` to mean ingest-vs-transform
worker, on every series, and that label is frozen. Do not overload it to mean pipeline stage:
use a separate label (`queue="ring"|"batch"`) or the shared dashboards break in a way that
reads like data corruption.

### Metrics for a long-running process — DECIDED

Periodic push.

### Checkpointing — DECIDED

Cold rebuild.

### Storage - DECIDED

ClickHouse.

**Sort key `(venue, symbol, receive_ts, seq)`, partitioned `toDate(receive_ts)`** — measured
in Phase 4 against arrival order over ten million identical rows: 258 MB against 344 MB, and
16,385 rows read against 458,752 for the one-symbol/one-minute read the read layer is built
around — Phase 10 when this was measured, Phase 9 after the re-scope. Daily partitions because retention and the archive tier drop a day at a
time; not partitioned by venue, because `venue` already leads the sort key and adding it to
the partition expression only multiplies parts.

**`price_ticks` and `size_lots` are 128-bit, not `Int64`.** `SCALE = 8` caps Int64 at about
9.2e10 and Kraken's depth-1000 `BCH/USD` book quotes an ask at 886,110,000,000.00 — real,
junk, and 8.9e19 scaled. Python's unbounded int hid it until a fixed-width column existed.
The narrow column would mean one junk order on one symbol killing a run, which is the same
trade `scaled_int` already refuses.

### Idempotent writes — DECIDED, and the guarantee is narrower than the phrase

**An `insert_deduplication_token` per batch, derived from the batch's own contents**, with
`non_replicated_deduplication_window = 30000` on the table. `TODO.md` asked for "idempotent
writes across restart via deterministic ids — a replayed batch cannot duplicate", and the
obvious reading of that is `ReplacingMergeTree` keyed on the sort key. **That reading is
wrong and would lose data.** `(venue, symbol, receive_ts, seq)` is not row-unique: one frame
emits every level it touched under a single `seq`, so collapsing on that key would delete
real rows rather than duplicate ones. Extending the key until it is unique — adding `side`
and `price_ticks` — needs `allow_nullable_key` for the control rows and throws away the
layout Phase 4 measured at 258 MB and 16,385 rows read.

So dedup moves up a level, onto the insert. The token is `deterministic_id` (the SDK's, from
`storage/ids.py`) over the batch's first and last `(venue, symbol, seq)` and its row count —
content, never a clock or a run id, either of which would mint fresh tokens on every replay
and deduplicate nothing.

**What that guarantees, exactly: replaying the same rows in the same batches is a no-op.**
Two bounds, both asserted in `tests/test_clickhouse.py` rather than left in prose:

- The rows must re-batch identically. A replay at a fixed row trigger does. A live process
  that died with a partial batch buffered resumes on a different boundary and **will**
  duplicate — there is a test that shows exactly this, so the limit cannot be quietly
  overstated later.
- The window is per partition and holds the last N blocks. A day is one partition and ~10⁹
  rows, so at the 50,000-row trigger that is ~20,000 batches; 30,000 clears a full-day replay
  with headroom, at a few dozen bytes a block.

One honesty note that falls out of it: **`WriteResult` counts rows sent, not rows kept.** A
discarded block is invisible in the insert response, so `records_written_total` over a replay
of already-landed data reads as though it wrote them.

### The archive tier and retention — CUT

`TODO.md`'s "rotating Parquet on GCS partitioned by `date/symbol`" and "retention and tiering
rule sized for 10⁹ rows/day" are both struck.

The archive tier is business logic — a bucket layout and a partition scheme for this
project's data — and Phase 7's priority was the SDK contract underneath it. Retention goes
with it rather than surviving alone: tiering has nowhere to tier to once the archive is cut,
and a TTL is a deployment concern with no deployment behind it. The sizing input survives in
`bench/volume.py`, which already measures the rows/day any future retention rule would be
set from.

What did *not* get cut with them is the Parquet tier itself. It is still the default output,
it is still what a local run lands, and Phase 7 fixed two real defects in it — see
§ *The Parquet tier had two defects*.

### Flush triggers — DECIDED

**50,000 rows or 2.0 seconds**, and the reasoning inverts what `TODO.md` assumed.

That bullet said insert throughput would set the triggers. It does not. Every batch size
measured — 10k through 1M — sustains at least 881k rows/s, against a measured multi-venue
arrival of ~12,000 rows/s. With 70x headroom everywhere, throughput does not discriminate
between the options at all.

**Latency does.** A batch is buffered rows and buffered rows are staleness, which is the same
argument § *Sizing the queues* makes about queue depth, one stage further down. So the time
trigger is the real setting and the row trigger is only the ceiling that bounds memory
through a burst. At ~12,000 rows/s the time trigger fires first, at ~24k rows and ~43k
inserts a day, well inside what the server merges away.

### The Parquet tier had two defects, and a real symbol set was needed to see either

Both found in Phase 7, both on `MTC_OUTPUT=parquet` — the **default** output — and both
invisible to every run before this one because those ran one symbol near the touch.

**dlt infers the schema per load, so an all-null column does not land.** `exchange_ts` is
null on every Binance REST snapshot row, so a load of nothing but snapshots dropped the
column entirely; files in one dataset then disagree about their width and reading the
directory as one dataset fails on a file that is individually valid. `TODO.md` recorded the
symptom from Phase 0 without a fix. The fix is `dlt_sink(columns=...)`, added to the SDK as a
generic passthrough with the hints staying here — and dlt's own warning names the failure
precisely, which is worth knowing: *"did not receive any data during this load and therefore
could not have their types inferred... will not be materialized in the destination."*

**dlt rejects a Python int wider than 64 bits at extract, before any column hint applies.**
`TypeError: Integer exceeds 64-bit range`. So declaring `price_ticks` as `decimal(38, 0)`
fixes the column's *type* and not this — the value never reaches the column. At `SCALE = 8`
the int64 ceiling is ~9.2e10 and Kraken's depth-1000 `BCH/USD` book carries an ask at
886,110,000,000.00, which is 8.9e19 scaled: **one junk order on one symbol crashed the whole
run.** The ClickHouse tier never had the problem, because Arrow carries the int straight into
a `decimal128(38, 0)` — this is the same Int64 discovery `bench/clickhouse.py` made in
Phase 4, arriving a second time on a second tier, which is the sort of thing that argues for
stating a model's width once and deriving both spellings from it.

The fix is a `Decimal` conversion on the two tick columns, at the boundary into dlt and
nowhere else — a sink wrapper rather than a `Transform`, because the widened row is no longer
a `LevelRow` and the model is right to say those fields are `int`.

**A third, smaller, and the most embarrassing: dlt writes the column's own `name` into the
hint dict it is handed.** The first cut of `DLT_COLUMNS` shared one dict between the three
text columns and one between the two tick columns, as a tidy-looking constant. dlt mutated
them, the columns all claimed the same name, and it merged them — warning loudly and then
landing a table quietly missing columns. Hints are built per column now.

### A day is 10⁹ rows, not 10⁷ — MEASURED

Every "10⁷/day" in this file and in `TODO.md` predates Phase 3 and was written when a run
carried one symbol. Measured across 188 symbols per venue, diff rows only, bootstraps
excluded: binance 9.6e7/day, coinbase 4.8e8/day, kraken 4.7e8/day. Retention, the archive
tier and the read layer all inherit the correction, and none of them has been re-sized yet.

### Make-before-break — SKIPPED, and here is what that leaves open

Not built, and no substitute measurement taken.

The bullet was justified as converting "a guaranteed daily gap into zero gap". Phase 3
measured break-before-make rotation at **0 gaps** — a clean close is answered with a fresh
snapshot, so a planned rotation costs a re-bootstrap and not a gap — which undercuts the
premise. Building the cutover means a capture-format change (records tagged by connection
generation) for a trigger no bounded run reaches.

**The open risk, stated rather than implied.** That 0-gap number is Kraken, four symbols, two
rotations in 90 seconds. Kraken is the in-band venue: a resubscribe gets a snapshot for free.
The venue that actually has the 24h expiry is **Binance**, where a reconnect needs an
out-of-band REST snapshot per symbol paced against a budget — and rotation there has not been
measured at shard scale. A run intended to last past 24h should measure that first.

---

## Technical notes

**Don't claim "zero-copy" in Python.** `struct`, `orjson` and `msgspec` all allocate. Genuine
zero-copy needs `memoryview` over the recv buffer plus a native extension. Say what you
measured, not what you aspired to.

**Decode probably matters more than you think.** JSON decode of a high-rate diff stream may
well dominate the profile. `json` vs `orjson` vs `msgspec.json` with a typed schema is a
cheap, early, high-information benchmark — run it in Phase 0.

**`Record = dict[str, Any]` will not survive the target rate.** This is the concrete tie-in
to the SDK's own note on record representation: the per-record `dict` cost is the wrong shape
somewhere around 10⁵–10⁶ records, and a batch-oriented `Sink` generic over an Arrow
`RecordBatch` is the shape the current contract cannot express. Prove it with a benchmark
before changing the SDK contract.

**nanobind stays in stretch.** One hot-path kernel in C++ or Rust, bound into Python, *after*
profiling proves a real hotspot. Doing it speculatively is worse than not doing it.

**Timestamps are now meaningful.** With a real remote venue there is genuine clock skew:
capture `exchange_ts`, `receive_ts` and a monotonic clock per message, and export measured
skew as a metric. Skew silently corrupts any cross-venue comparison, which is precisely why
measuring it is worth a paragraph in the README.

**Sequencing.** Phase 0 exists to de-risk the architecture while there is still time to
change it. Get one symbol, one venue, end-to-end to a file with a baseline number committed
before building anything else.

---

## What "done" has to be able to say

Not features — **numbers**, since that is the gap this exists to close:

- "N venues, M symbols, X msg/day captured live, sustained Y msg/s, burst Z"
- "replay ceiling of W msg/s through decode → book → Arrow → sink, excluding the socket path"
- "p99 receive-to-disk latency of Z ms"
- The wrong prediction is part of the story, so write the prediction down first
- "dropped R messages; here is the drop policy, and the number it was measured against"
- "reconciled the book against the venue REST snapshot: S breaks over T comparisons"
- *Separately, never merged*: "U Kraken CRC32 breaks over the same session"
- "book converges within K messages of a re-snapshot after an injected gap"
- "the SDK grew V primitives and W stayed untouched; here is the diff"

Three lines were cut here in the September 2026 re-scope, and the reasons are in
§ *Re-scope*: the deliberate-overload backpressure policy, the venue top-N match rate, and
the profiled-hotspot before/after.

**Phase 7 answered the first three, and `DEVELOPMENT.md` § *Phase 7* has them with their
denominators.** Live: Kraken, 185 symbols at depth 1000, 2,048,719 rows in 300s — sustained
6,852 rows/s, burst 41,576 rows/s. Ceiling: 62,618 rows/s through decode → book → Arrow →
sink, socket excluded, against 85,236 rows/s for the same corpus with the sink removed. p99
receive-to-disk 2,286 ms, which is the 2.0s flush trigger plus the close, and is therefore a
*configuration* result more than a code one — a consumer wanting a tighter tail lowers
`batch_max_seconds` and pays in insert count.

The live figure and the ceiling are two numbers and appear here as two. Anyone quoting one
of them as the other is quoting a number this project spent a phase learning not to merge.

A committed benchmark harness with reproducible numbers matters more than any individual
optimisation.

---

## Relationship to the existing repos

- **Reuses:** `data-pipeline-core` — runtime, metrics, storage adapters, resilience
- **Reuses:** `data-pipeline-infra` — Terraform, but needs a *service* module, not a job one
- **Forces changes in:** `data-pipeline-core` — enumerated below
- **Doesn't touch:** `proba-markets-analysis`
- Every leak found here is fixed **in the SDK, not worked around in the consumer**
- That discipline is the entire point of having two consumers

### What the SDK actually is today (v0.2.0, read not assumed)

The starting point, so the gap list below is a diff against reality rather than a memory:

- **Fully synchronous** — no `async def`, no `asyncio`, no websocket dependency in `src/`
- **`WorkerApp.run()` is one pass, then exits** — fetch, optional transform, one write, done
- Its own `ARCHITECTURE.md` §2 says so: "start → work → push metrics → exit"
- **`Sink.write(Iterable) -> WriteResult`** is called exactly once per run
- **Metrics push in the `finally` of `run()`**; no scrape endpoint, no HTTP server at all
- **Resilience is HTTP-shaped** — `HttpClient`, `CircuitBreaker`, `IpGuard`, `ProxyRouter`
- Backoff is welded inside `HttpClient` rather than exposed as a policy
- **No checkpointing** and no notion of durable progress anywhere
- Its `TODO.md` already anticipates an "always-on transform archetype" — this is that consumer

**The contract shape survives; the run loop does not.** `Source.fetch()` returns an
`Iterable`, and an endless generator is a perfectly legal `Source`. What breaks is the
one-pass loop, the single sink call and the push-at-exit metrics — a much better story than
"I rewrote the contract".

### The SDK gaps, in order of how hard they bite

1. **There is no loop** — `ServiceApp`: run until stopped, health endpoint, graceful drain
2. **Metrics never publish** until the process dies (§ *Metrics for a long-running process*)
3. **`Sink` is the wrong shape** — one `write()` per run, not repeated batches with flushes
4. **No checkpointing** (§ *Checkpointing*), and the progress token must stay opaque
5. **Connection supervision**, with backoff lifted out of `HttpClient` into one policy
6. **Backpressure primitives and metrics** (§ *Backpressure*), against a frozen surface
7. **Sync-only, no primitive either way** — whichever receiver shape wins is net-new

Item 3 is the one to think hardest about: it is the SemVer-major public surface, and it
needs size/time flush triggers plus a defined fate for the partial batch at shutdown. One
small point in favour of the reader-thread option in 7 — `Lifecycle` is already a
`threading.Event`, so cooperative shutdown works cross-thread untouched.

This list is a diff against the SDK as found, and stays accurate as one. It is no longer the
work list: 4 is cut outright and 6 shrinks to its metrics half, both in § *Re-scope*. The
gap that list never named — the SDK can write a curated dataset and cannot read one back —
is the read layer's `DatasetReader`.

**Item 3 was only half-fixed in Phase 4, and Phase 7 found the other half.** The batch `Sink`
shipped as `BatchSink[RecordT]` with `write_batch(Sequence[RecordT])`, which is a row-oriented
insert path and cannot express a columnar one: a `pa.RecordBatch` is not a sequence of
records under any spelling. Parameterising by the *batch* rather than the record fixes it —
`BatchSink[Sequence[LevelRow]]` and `BatchSink[pa.RecordBatch]` are both legal and the flush
triggers stay one implementation. Shipped in `data-pipeline-core` v0.3.0, and a second
SemVer-major event on the same surface, which is the honest cost of having designed item 3
against one consumer's first destination.

**And a gap the list still does not name: a `Sink` never sees a `RunContext`.** `Source.fetch`
and `Transform.transform` both take one, so both can publish a series through
`ctx.metrics.registry` — which is the whole point of § *Metrics for a long-running process*.
`Sink.write(records)` takes no context, and the run's registry is built inside
`build_runtime`, so the sink is the one slot that can measure something and has nowhere to
put it. Phase 7 hit this measuring receive-to-disk, which is a *sink* quantity by
construction, and worked around it by reporting at exit from `collector/main.py` rather than
exporting a histogram. Not fixed here: closing it changes `Sink.write`, a third major event
on the same surface, and it should wait until something is actually asking to scrape the
number.

### Where the boundary falls

The SDK's own first principle is "only the generic goes in", and it puts canonical models
and entity resolution explicitly out of scope. Symbol normalization is the crypto version of
"Marseille ↔ OM" — it is *this* repo's business logic, and it does not go in the SDK no
matter how reusable it looks.

| Belongs in `data-pipeline-core` | Stays here |
|---|---|
| `ServiceApp`: run-forever loop, health endpoint, periodic metrics | Snapshot/delta bootstrap and the splice |
| Drop and queue-depth metrics, against a frozen label surface | Per-venue sequencing dialects and gap rules |
| Connection supervisor: reader threads, reconnect, backoff, the bounded queue | Which streams shard onto which socket, and the venue's REST pacing |
| Batch-oriented `Sink` with flush triggers | Symbol normalization and the venue tables |
| `DatasetReader`: returns Arrow, hides partitioning | Checksum validation, the REST oracle |
| — | Point-in-time correct book reads, as-of semantics |

The bounded queue and the checkpoint protocol were in the left column until the September
2026 re-scope removed both; see § *Re-scope*. `DatasetReader` replaced them, and unlike them
it has a second consumer to prove it against.

---

## Interview talking points

1. **Throughput, with two numbers** — what live rate and replay ceiling each bound
2. **Drop policy** — what gives when the queue fills, and why it drops rather than blocks
3. **Gap recovery** — sequence tracking, untrusted book, re-snapshot, convergence
4. **Reconciliation** — one oracle and one venue-native checksum, reported as two numbers
5. **The SDK story** — what `ServiceApp` forced, and which abstractions survived intact
6. **The normalization boundary** — one model, quirks in adapters, one contained leak

The two worth rehearsing are 3 and 5. Gap recovery is a consistency problem with a correct
answer and should be whiteboardable end to end. The SDK story is stronger for its second
half: the `Source`/`Sink` protocols held, an endless generator was already a legal `Source`,
and what broke was the run loop rather than the contract.

Point 4 was "three oracles" before the re-scope. Say "one oracle and a checksum" and say
what each bounds — the smaller claim that is actually backed beats the larger one that is
not, and the cut is defensible out loud (§ *Re-scope*). Point 2 is now the weakest and
should be led with the property rather than the number: the read never blocks, drops are
counted and latch a repair. The sizing measurement behind it was cut, deliberately.


---

## Phase 2.5 additions

### An in-band snapshot at a healthy book must rebuild — DECIDED, after getting it wrong

`BookTransform` ignored a snapshot arriving at a live book, on the reasoning that rebuilding
costs a full book's worth of rows to arrive where the book already is. That holds for a
snapshot read *alongside* a diff stream that never stopped, which is Binance's REST call. It
is false for one obtained by unsubscribing and resubscribing, which is the only way either
in-band venue will send one: every level deleted during that gap is absent from the new
snapshot and never arrives as a delete. No gap, no sequence break, and no failing checksum
while the staleness sits below the top ten.

Found as 43 crossed books in a 60-second Kraken replay in which all 6572 checksums passed —
the book wrong in exactly the region the CRC does not cover. It applied to **Coinbase
identically** and had been there since Phase 2; nothing caught it because
`snapshot_interval_s` defaults to 300 and no test run lasted that long. Two Coinbase tests
asserted `bootstraps == 1` over a session with six snapshots, encoding the bug.

The adapter answers it: `snapshot_supersedes`, False on Binance, True on both in-band venues.

### The venue limits table, corrected

| Venue | Cap per connection | Other limits |
|---|---|---|
| Kraken v2 | **200 symbols** | one depth per symbol per connection; depth ∈ 10/25/100/500/1000 |

**There is no full-depth channel.** `NOTES.md` assumes "full-depth diff channels" throughout
and that is false for this venue: 1000 is the ceiling and its book is a top-1000 one by
construction. Running the ceiling.

**A second depth for a subscribed symbol is refused** — `{"error":"Already subscribed"}` —
so § *Validating against the venue's own top-N* (Phase 8's Oracle 2) does not exist here on
one connection, and the CRC is this venue's only oracle. Whether a second connection can
carry it is Phase 3's question, since that is the phase that decides how many there are.

**Message rate scales hard with depth:** 95.7 msg/s at depth 1000 against 7.1 at depth 10.
That is a shard-sizing input for Phase 3, not a detail.

### Symbol spelling comes from the socket, not from REST — DECIDED

Phase 2 committed `XBT` for bitcoin and `XDG` for dogecoin, from REST `AssetPairs.wsname`.
That is **v1** naming and the v2 socket rejects it: `Currency pair not supported XBT/USD`.
The v2 `instrument` channel lists all 188 committed bases under ISO codes and contains
neither `XBT` nor `XDG`. The mapping is a rule with no exceptions, as the generator always
claimed while reading the wrong list, and `tools/symbols.py` now reads the v2 socket.

The failure was silent: a rejected subscription is an ack with `success: false` on a socket
that stays open, so the run connected, landed two control records, logged `frames: 0` and
exited zero. `FrameSource` now fails a run that received no book message at all — venue-
neutral, because the ack shape is not and that symptom is.

---

## Re-scope — DECIDED, September 2026

The remaining plan was written when the project was "capture L2 well". It is not. It is
**the second consumer of `data-pipeline-core`, and the deliverable is the SDK diff** —
`ServiceApp`, the batch sink, periodic metrics, the connection supervisor: the primitives a
long-running consumer forces out of an SDK built for one-shot Cloud Run jobs. Phases 5, 6
and 8 as written were market-data feature work that would have produced no SDK pressure at
all, and § *Why this project exists* is honest about which of the five gaps still needs
closing: throughput evidence (1), and proof the SDK generalises (4). Not more oracles.

The counterweight, and why this is a re-scope and not a retreat: **nobody hiring for this
work cares about a `ServiceApp` abstraction.** They care whether the book is correct and
whether the claim is measured. Cutting to plumbing alone inverts the value. So two items
survive the cut on their merits, and only two.

### Phase 5 — cut, because the load-bearing half already shipped

`ShardSupervisor._enqueue` is a `put_nowait` onto a bounded `Queue`: full drops the record,
counts it, and latches a repair for the affected symbol. The socket read never blocks and a
drop is never silent. That is the property `CLAUDE.md` names, and it holds today.

The rest of the phase — high and low watermarks, disk spill, symbol shedding, a
power-of-two ring with a masked index — is capacity engineering against a load that has
never been measured, which § 2 of `CLAUDE.md` forbids in as many words. The single
threshold stays a config default until a number argues otherwise, and
`messages_dropped_total` reports what that number would be. Peak arrival rate and p99 burst
duration survive into Phase 7's burst-capacity measurement, where they are throughput
numbers rather than sizing inputs for a queue that will not be built.

**This kills an SDK item, and it is dropped in `data-pipeline-core` too rather than left
orphaned there.** That repo's TODO already defers the bounded-queue primitive to "the phase
that sizes it from a measurement". That phase is gone, so the item goes with it. The
checkpoint protocol goes for the adjacent reason: cold rebuild means this consumer's answer
is "nothing", so there is no second implementation to generalise against, and a protocol
designed from one data point is a guess with a type signature.

### Phase 6 — not a cut, a close-out

Deleting the phase would have deleted the record of work already done. Most of it is
shipped, as a byproduct of Phases 0–3, and was never ticked. Audited item by item against
the code rather than assumed, because the first pass ticked two things it should not have:

**Verified, with the test that establishes it.** `Book`'s absolute set-to-value with
zero-as-the-only-delete (`test_book.py`, nine tests). The bootstrap splice and the
snapshot-too-old / buffer-behind distinction, both branches acted on — the transform waits,
`FrameSource._snapshot_due` refetches — and both covered by `test_splice.py`.
Untrusted-before-repair with both edges of the interval landed, `BookTransform._on_frame`
and `mark_gapped`, with `untrusted_frames` as the measurable width. `snapshot` and `gap`
emitted as control rows. Convergence under the fault injector, asserted as
`_book(faulted) == _book(clean)` on all three venues — every adapter has its own
`test_a_dropped_run_is_detected_and_the_book_reconverges`.

**Over-ticked, now corrected.** The state machine is written as DISCONNECTED → BUFFERING →
SYNCING → LIVE ⇄ GAPPED in two docstrings and in this file. What exists is a `_live` bool
plus `_snapshot is None`: three implicit states, with DISCONNECTED owned by
`ShardSupervisor` and GAPPED not a state at all — a gapped book buffers, which is why the
gap edge and the cold-start edge share `_try_bootstrap`. The behaviour is right and the
diagram oversells it; whether it becomes an enum is now its own item. And **cold rebuild is
true by construction, not by test** — nothing persists book state, so nothing stops someone
adding it. An assertion that the collector has no durable book state is worth the four
lines.

**The read benchmark goes, and it is a real cost.** `book.py`'s docstring called the `dict`
"a decision Phase 6 benchmarks rather than assumes", and `best_bid_ask` is an O(n) scan over
every level; the alternatives — a sorted array, ticks-from-mid — were named and never
measured. So the structure is chosen on reasoning rather than on a number, which is the one
place in this repo where that is true, and the docstring is rewritten to say so plainly
rather than to keep promising a benchmark that is not coming. Argued for and lost: the case
was that a measured trade-off against a written-down prediction is the cheapest credibility
available here. The counter-case is that the read is not on the hot path the project exists
to measure — diff application is, and it is a point mutation under all three candidates.

If it is ever reopened, the thing to measure is `best_bid_ask` at a realistic read:write
ratio. Benchmarking the write picks the wrong structure, which is what the docstring was
right about.

### Phase 8 — cut to one oracle

Oracle 2 (the venue's own top-N, id-aligned, with a ring of recent versions), Oracle 3 as a
separate deliverable, break classification into missing/duplicate/mismatch/timing,
configurable tolerance rules and idempotent re-run: all cut. That is enterprise
reconciliation plumbing, it is genuinely a different project, and Oracle 2 does not exist on
Kraken anyway.

**Oracle 1 moves into Phase 6 and is built.** Without it, "is the reconstructed book
correct?" is answered by Kraken's CRC32 — one venue of three — plus synthetic faults the
harness injected itself. Injected-fault detection proves the detector works on faults it was
told about; it says nothing about Binance or Coinbase against reality. Oracle 1 is a
periodic REST snapshot diffed against the live book, on machinery the bootstrap refetch
already owns — `FrameSource._interval_elapsed` names this oracle as its second reason for
existing, and `snapshot_interval_s` already lands the snapshots it would read. It converts
the repo from "I built a pipeline" to "I built a pipeline and here is its measured
divergence rate", which is the difference between a demo and a claim. Its break count and
the CRC32 break count are reported as two numbers, never merged.

**It compares only where the book is `live`.** `BookTransform.live`'s own docstring is the
reason: an untrusted book says where it stopped, not whether it recovers, so diffing one
against a snapshot manufactures breaks out of an interval that was already known to be open.
The denominator is comparisons attempted against a trusted book — the same honesty the fault
tests already apply to their detection rate.

Oracle 3's substance is not lost: the fault-injection tests exist and pass. What is cut is
promoting them to a headline detection-rate claim, which was always the weakest of the
three because the harness grades its own homework.

### Phase 9 — cut entirely; the numbers move to Phase 7

The benchmark phase was first narrowed to SDK overhead — `batching_sink` against writing to
the `BatchSink` directly, the run loop against driving the transform directly, the periodic
push measured as a stall in the drain — then cut outright. The narrowing was the right
diagnosis and the wrong conclusion: what was left was a phase-shaped wrapper around
measurements that each belong beside the thing they measure.

**So they moved rather than vanished, and the distinction is the whole of it.** § *Why this
project exists* names throughput evidence as gap 1 — the central missing claim for all three
target roles — and a re-scope that dropped it would have removed the reason to build any of
this. Receive-to-disk percentiles were already a Phase 7 item; sustained throughput at shard
scale, the live-versus-replay split and the comparison against the Phase 0 architecture
prediction join them there. One run produces all four, it is the run the batch sink makes
possible, and Phase 7 is now the critical path for that reason.

What is genuinely gone is the SDK's own overhead as a stated number, and it is worth being
clear that this is a loss rather than a tidy-up: "the abstraction costs N% of throughput" is
a claim the SDK story would have been stronger for, and it is the one measurement no other
repo here could have produced. It can be taken while Phase 7's harness is open, since that
harness is where it would run anyway.

Cut with it: the flamegraph-before-and-after, the bounded `scaled_int` cache, and Kraken's
double-parse. All three are micro-optimisations, and the two named ones are guesses at a
hotspot dressed up as tasks. **This is the cut to regret if any is.** "I predicted the
hotspot, I was wrong, here is the before and after" is one of the better stories the project
had. It goes anyway because it is downstream of Phase 7, and a profile of a pipeline whose
sink does not exist profiles the wrong thing. If Phase 7 lands early it comes back.

### The read layer — split at the SDK boundary

Phase 10 in the old numbering; it becomes Phase 9 once the benchmark phase is gone.

The SDK is asymmetric: `Sink` writes, and `staging.py`'s `raw_landing_source` reads back
what `raw_landing_sink` wrote — but only for the raw tier. The curated tier has no read half
at all, in either consumer. That is a real gap and closing it is a legitimate primitive: a
`DatasetReader` protocol returning Arrow and hiding partitioning, with a ClickHouse
implementation against the `ORDER BY` Phase 4 chose and a DuckDB one over the Parquet
archive.

What does *not* go in the SDK is everything that made the original phase interesting to this
repo: point-in-time correct book reads, as-of semantics, snapshot-at-time. That is
market-data semantics and it sits on the same side of the line as symbol normalization — see
§ *Where the boundary falls*, which already puts canonical models out of scope.

**And it is cut here too, not merely relocated.** The consumer-side helper is one read: a
symbol over a time window, returning Arrow. That is not a compromise pick — it is the read
`bench/clickhouse.py` already probes as "one symbol, one minute", and the read the MergeTree
sort key was chosen against in Phase 4. Building as-of semantics on top would be inventing
demand for a query nothing in the repo makes. If a consumer of the data ever needs
snapshot-at-time, it arrives as a requirement with a caller attached, which is the only
condition under which it should have been built anyway.

The `DatasetReader` protocol still has to earn the SDK, and the only test that settles it is
whether `proba-markets-analysis` can read its curated tier through it. Written down here
rather than as a task, since the task list now takes the extraction as decided: if the
betting repo cannot use it, it is not a primitive and it collapses back into this repo
beside the helper.

### What this costs, stated plainly

The § *What "done" has to be able to say* list loses three of its lines: the backpressure
policy under deliberate overload, the top-N match rate, and the profiling story. § *Interview
talking points* loses "three oracles, three different claims" — it is one oracle plus a
venue-native checksum, and it should be said that way rather than padded. Both sections are
edited to match rather than left making claims the plan no longer supports.

What it does not cost is the throughput claim, and that is the line not to cross. Every cut
above was checked against gap 1 of § *Why this project exists*; the moment a re-scope starts
eating the number this repo exists to produce, it has stopped being a re-scope. Phase 7 now
carries that number and is the critical path — more so than 9 or 10, which both sit behind
its sink.

### The queue metrics carry no `queue` label — DECIDED, September 2026

The TODO said `queue_depth` and `messages_dropped_total` would be labelled
`queue="ring"|"batch"`, on the reasoning that a pipeline holds two of them and an operator
needs to know which one is filling. Written when the ring was here and the batch buffer was
still hypothetical; both now exist, and only one of them is a queue in the sense the metric
means. `BatchingSink` accumulates on the calling thread and flushes inline — it has an
occupancy but no producer that can outrun it, and it cannot drop. So the label would have
carried one value on one series and a permanent zero on the other, which is worse than no
label: a dashboard that shows `messages_dropped_total{queue="batch"}` flat at zero for ever
reads as evidence when it is an artifact of the shape.

The counter-argument, and it is a real one: §8 freezes labels, so adding `queue` later is a
breaking change and adding it now would have been free. Rejected because a label that
distinguishes nothing is not free — it is a permanent invitation to read something into it.
The SDK owns exactly one droppable queue, which is `ConnectionSupervisor`'s; if a second
ever appears, so does the label, and the migration is a dashboard edit rather than a lost
year of data.
