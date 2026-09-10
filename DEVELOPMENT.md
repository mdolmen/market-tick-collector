# Development log

Per-phase predictions and the measurements that grade them. Design decisions and their
rationale live in `NOTES.md`; the sequenced task list is `TODO.md`. The rule this file
exists to enforce: **the prediction is written down before the run**, and a wrong prediction
stays on the page.

---

## Phase 0 — walking skeleton

One Binance symbol, full-depth diffs, bootstrapped over the splice, applied into a book,
emitted as normalized level rows, landed as a Parquet dataset, for a bounded duration.

### The prediction

Written **before** the measured runs. What had already been seen when this was written, in
the interest of not overclaiming: a five-second smoke run against a console sink reported an
in-process p50 around 170µs per frame. Nothing else had been measured, and no decoder
comparison had been run at all.

**Decode is not the hotspot.** `NOTES.md` § *Technical notes* says JSON decode "may well
dominate the profile" on a high-rate diff stream. The prediction is that it does not, at this
frame shape: a 100ms Binance diff carries only a handful of price levels, and the per-level
work *after* the decoder returns — two `Decimal` constructions and a `scaleb` each, to reach
the integer book key — costs several times the parse of the whole frame. Concretely:

- `msgspec` typed decodes fastest, `orjson` within ~30% of it, stdlib `json` 3–5× slower
- On decode **+ model** the three converge to within ~30% of each other, because `Decimal`
  dominates and every decoder pays it identically
- So the Phase 9 hotspot is price/size normalization, not JSON, and the fix is to stop going
  through `Decimal` in the hot path rather than to swap decoder

If that is wrong, the interesting version of wrong is that real frames are much wider than
the smoke run suggested — in which case the decoder ranking survives and the per-frame level
count is the number that actually mattered.

**Throughput here proves nothing, and that is fine.** One symbol on a 100ms channel is ~10
frames/s, bounded entirely by the venue rather than by this code. The Phase 0 number is a
floor and a regression tripwire, not a claim. Expect the Phase 1 replay ceiling three or four
orders of magnitude above it, and never report the two as one number.

**Architecture predictions, for Phase 9 to grade:**

- The `dict` book holds up on writes under every load this project reaches. What breaks first
  is `best_bid_ask` — an O(n) scan — and it breaks in Phase 8, when the top-N oracle starts
  reading it continuously, not before. The read:write ratio picks the structure.
- Reader thread + ring buffer beats pure asyncio by **less than 2×**, and most of that margin
  comes from getting decode off the reader thread rather than from the threading itself.
- `Record = dict[str, Any]` becomes the dominant per-row cost somewhere around 10⁵ rows/s,
  which is what forces the Arrow-batch `Sink` — the SemVer-major change in `NOTES.md`
  § *The SDK gaps*.

### The measurement

Apple M-series laptop, Python 3.11, BTCUSDT on `btcusdt@depth@100ms`, `snapshot_limit=5000`.
Reproduce with:

```
DESTINATION__FILESYSTEM__BUCKET_URL="file://$PWD/data/l2" \
MTC_DURATION_S=60 uv run python -m collector.main
uv run python -m bench.decode  data/btcusdt-frames.jsonl
uv run python -m bench.cadence data/btcusdt-frames.jsonl
```

**Baseline — the committed number.** 60s bounded run, no raw tap:

| | |
|---|---|
| Frames | 601 in 60.005s — **10.0 frames/s** |
| Rows | 18,563 — **309 rows/s** |
| In-process latency | p50 **205µs**, p90 343µs, p99 **1273µs** |
| Bootstraps / gaps / crossed books | 1 / 0 / 0 |
| Book at exit | 4,991 bid levels, 5,048 ask levels, spread one tick |

A 300s capture run agreed: 3,000 frames, 10.0 frames/s, 54,056 rows, p50 201µs, p99 1679µs,
zero gaps. 10.0 frames/s is the channel's own rate, so the pipeline kept up with the venue
for the whole window and the number bounds the venue, not this code. The Parquet dataset read
back at exactly 18,563 rows, split 10,000 `snapshot` / 5,891 `set` / 2,672 `delete`.

**Decode.** 3,000 frames, 44,179 levels, 1,660 KiB, best of 5:

| decoder | decode µs | frames/s | + model µs | frames/s |
|---|---|---|---|---|
| `json` (stdlib) | 3.1 | 321,347 | 9.7 | 103,401 |
| `orjson` | 2.1 | 471,936 | 8.6 | 115,867 |
| `msgspec` (untyped) | 2.2 | 460,073 | 8.7 | 115,575 |
| `msgspec` (typed) | **1.7** | **599,057** | 8.6 | 116,765 |

### Grading the prediction

**Right.** Decode is not the hotspot. Scaling into the record model costs 4× the decode on
the fastest decoder (6.9µs vs 1.7µs), and on decode + model the four options collapse into a
13% spread — tighter than the ~30% predicted. Switching decoder buys **13%** end to end;
getting out of `Decimal` is where the room is. The ranking held: `msgspec` typed fastest,
`orjson` 24% behind it, inside the predicted 30%.

**Wrong.** Stdlib `json` was predicted 3–5× slower than `msgspec` typed. It is **1.8×**. The
stdlib is closer to the C decoders on frames this small than the folklore says, which makes
the "keep the stdlib for now" choice in `source.py` cheaper than it looked when it was made.

**Not predicted at all, and the most useful thing here.** The live run reports p50 205µs;
`bench/decode.py` says the same work is single-digit µs. Both are right, and `bench/cadence.py`
shows why — the *same code over the same corpus*:

| | p50 | p90 | p99 |
|---|---|---|---|
| back to back | 12.8µs | 31.5µs | 192.3µs |
| 100ms idle between frames | 235.0µs | 362.2µs | 606.1µs |

An 18× gap, and it reproduces the live figure almost exactly. A 100ms channel leaves the
process idle between frames, and the first work after each wake-up runs on a core that has
been allowed to go quiet. So **the live in-process p99 is mostly wake-up cost, not pipeline
cost** — it will *fall* as symbol count rises and the process stops idling, which is the
opposite of how a latency number normally behaves. Quoting it as what the code costs would be
wrong in a way that happens to flatter. The replay ceiling in Phase 1 is the number that
answers "how fast is your code", which is the § *Two numbers, never merged* discipline
arriving from a direction nobody predicted.

### Acting on it: the scaling path

The finding named a hotspot, so it got fixed rather than filed. `scaled_int` shuffles digits
instead of going through `Decimal`, over the 88,358 price and size strings in the corpus:

| | ns/value | vs. before |
|---|---|---|
| `Decimal(v).scaleb(s)` — before | 235.4 | 1.00x |
| digit shuffle, as shipped | **198.2** | **1.19x** |
| …the same parse without its two validating lines | 115.4 | 2.04x |
| …as shipped, plus a `dict` cache on the string | 37.8 | 6.22x |

End to end that is decode + model from 8.6µs to **7.3µs** per frame on `msgspec` typed —
136,452 frames/s against 116,765, about **15%**. Stdlib `json` moved 9.7µs → 8.5µs.

Two deliberate refusals in that table, both worth more than the milliseconds:

- **The unvalidated parse is 40% faster and was not taken.** Dropping the sign handling and
  the `isdigit` guard gets to 115ns, at the price of `int()` quietly accepting `"1_0"` as ten
  and `"+-1"` as one. That is a silent-corruption path into a *book key*, which is the one
  failure this project is organised around not having. 83ns is not the price of that.
- **The cache is 6.2x and belongs to Phase 9, not here.** It is by far the biggest number on
  the page, because only 15,347 of 88,358 values were distinct. But 6,782 of those were
  distinct *sizes* after five minutes, and that set grows roughly linearly — an unbounded
  cache is a memory leak wearing a speedup's clothes. It needs a bound, an eviction policy and
  a long capture to size them against, which is a Phase 9 optimisation with a real design in
  it, not a Phase 0 one-liner.

What survives from the prediction either way: switching decoder is worth ~13% and this was
worth ~15%, so both are small change next to the wake-up cost above. Neither is where the
throughput story gets written.

### Other findings

- **Phase 0 required zero changes to `data-pipeline-core`.** A bounded source is a legal
  `Source` and `WorkerApp` ran it untouched. The contract survives; the run loop is what
  Phase 4 has to replace.
- **dlt drops an all-null column.** Snapshot rows carry `exchange_ts = None` (Binance sends no
  clock with a REST depth response), so the first Parquet file in the dataset has no
  `exchange_ts` column at all and a later one does. `pyarrow.dataset` unifies the schemas; a
  naive per-file `concat_tables` raises. Deferred to Phase 7, where the sink pins the schema.
- **Receive-to-disk p99 was not measured, deliberately.** One `sink.write()` at the end of a
  bounded run lands every row at once, so the number would describe the run's shape rather
  than the pipeline's. It moves to Phase 7 with the batching sink.
- The `websockets` sync client's internal queue is the only thing between the socket and a
  slow sink right now, so a stalled consumer still reaches back to the socket — the coupling
  Phase 5 exists to break. At one symbol it never bit; the console-sink smoke run made it
  visible immediately, with `receive_ts` values bunching as frames were drained in bursts.

---

## Phase 1 — capture & replay harness

The collector split along the SDK's ingest/transform boundary: `BinanceFrameSource` lands
verbatim capture records, `BinanceBookTransform` turns them into level rows, and a replay
feeds the *same transform* from disk. Faults are injected into the record stream, never into
the book.

### The prediction

Written **before** the measured runs. Nothing had been measured at this point beyond Phase 0.

**The replay ceiling lands between 100k and 140k frames/s.** `bench/decode.py` measured decode
+ model at 8.5µs per frame on stdlib `json` after the `scaled_int` work, which is ~118k
frames/s, and the transform adds a dict write per level plus a `LevelRow` construction per
level on top of that. Those are not free — Phase 0's own prediction list says
`Record = dict[str, Any]` becomes the dominant per-row cost around 10⁵ rows/s, and at ~15
levels per frame this corpus crosses that. So: **frames/s within the decode+model band but
rows/s the number that actually binds**, and the gap between the two is the row model, not
the book.

If that is wrong, the interesting version of wrong is the `dict` book being the cost rather
than the row model — which would move Phase 6's benchmark forward, because it would mean the
write path matters at a volume Phase 0 predicted it would not.

**Three to four orders of magnitude above the live rate.** Phase 0 measured 10.0 frames/s
live, bounded entirely by the venue's 100ms channel. `DEVELOPMENT.md` above predicted the
ceiling three or four orders above that; at 10⁵ frames/s it is four. The two numbers are
never merged, and the ceiling excludes the sink as well as the socket at this phase.

**Wake-up cost disappears.** `bench/cadence.py` measured an 18× gap between back-to-back and
100ms-idle frames — p50 12.8µs against 235.0µs. An unthrottled replay never idles, so the
ceiling should sit near the back-to-back figure and *not* near the live one. If it does, that
retroactively confirms the Phase 0 finding that the live p99 was mostly wake-up cost.

**Fault detection: 100% of drops and reorders landing on a live book, 0% of clock jitter.**
Jitter is the control: nothing in the pipeline may sequence by clock, so a book that changes
under it has a bug that no other test would find.

**A duplicate frame will be reported as a gap, and that is a defect.** `in_sequence` is
`U == prev_u + 1`, so a redelivered frame — whose `U` is at or behind the cursor — fails the
chain rule exactly the way a genuine loss does. Venues redeliver. The prediction is that the
book stays *correct* (the re-bootstrap repairs something unbroken) while the collector pays a
full snapshot for nothing, and that the fault injector is what surfaces it — which is the
argument for having built it.

### The measurement

Apple M-series laptop, Python 3.11, BTCUSDT on `btcusdt@depth@100ms`. A 120s capture:
1,200 frames, 4 snapshots at `snapshot_limit=1000`, 1,204 records, 1.1 MiB, zero socket skips
and zero refetches. Reproduce with:

```
RAW_BUCKET_URL="file://$PWD/data/capture" MTC_MODE=capture \
MTC_DURATION_S=120 MTC_SNAPSHOT_INTERVAL_S=30 MTC_SNAPSHOT_LIMIT=1000 \
  uv run python -m collector.main
RAW_BUCKET_URL="file://$PWD/data/capture" MTC_MODE=replay MTC_OUTPUT=console \
  uv run python -m collector.main
uv run python -m bench.replay data/capture/binance-depth
```

**Cold replay reproduces the session.** 1,200 frames → 18,055 rows, one bootstrap, zero
gaps, zero crossed books, 1,065 bid / 1,109 ask levels, spread one tick. The replay runs with
`http=None`, so any I/O at all would have raised: the capture alone is a sufficient
description of the session, which is the claim the whole harness rests on.

**The replay ceiling — decode → book apply → row built, excluding the socket *and* the sink:**

| | |
|---|---|
| Frames/s | **37,425** |
| Rows/s | **563,088** |
| µs/frame | 26.7 |
| Envelope decode, the replay's own overhead, reported apart | 2.2 µs/record |

Through `ReplaySource` end to end — fsspec read, envelope decode and all — it is **17,948
frames/s**. Both are real; the first says how fast the collector is, the second how fast the
harness can drive it, and merging them would flatter one or the other.

**Where the 26.7µs goes**, same corpus, each stage in isolation:

| | µs/frame | frames/s |
|---|---|---|
| `parse_event` only (payload pre-decoded) | 10.45 | 95,674 |
| `book.apply` only | **0.96** | 1,036,457 |
| `level_rows` = `book.apply` + `LevelRow` construction | 5.69 | 175,612 |
| Full transform through the generator | 26.76 | 37,369 |

**Paced replay.** Same corpus, 120s span, at each speed:

| speed | wall | expected | rows | gaps | book |
|---|---|---|---|---|---|
| 1× | 123.4s | 120.0s | 18,055 | 0 | 1065/1109 |
| 10× | 14.1s | 12.0s | 18,055 | 0 | 1065/1109 |
| 100× | 1.70s | 1.2s | 18,055 | 0 | 1065/1109 |
| max | 0.07s | — | 18,055 | 0 | 1065/1109 |

Identical rows and an identical book at every speed, which is the point: pacing changes when
work happens and nothing about what it produces.

**Fault injection**, seed 7, over the same capture:

| faults | injected | jittered | gaps | bootstraps |
|---|---|---|---|---|
| `drop` | 29 | 0 | 4 | 4 |
| `reorder` | 29 | 0 | 4 | 4 |
| `duplicate` | 29 | 0 | 4 | 4 |
| `clock_jitter` | 0 | 1,200 | **0** | 1 |
| all four | 61 | 1,168 | 5 | 5 |

### Grading the prediction

**Wrong, by 3×.** Predicted 100k–140k frames/s; measured **37.4k**. The band came from
`bench/decode.py`'s 8.5µs decode + model, and the transform turned out to cost three times
that rather than a little more.

**Right about the mechanism, wrong about which part.** The prediction said the book would not
be the cost and the row model would be, and the book is indeed **0.96µs/frame — 3.6% of the
path**, vindicating the Phase 0 call to keep the `dict`. But the biggest single line item was
not predicted at all: **the per-row generator protocol costs ~10.7µs/frame, 40% of the
path**. `transform()` → `_on_frame` → `_apply` → `yield from rows` is resumed once per row at
13.4 rows per frame, and `LevelRow` construction adds 4.7µs on top. Phase 0 predicted
`Record = dict[str, Any]` would dominate "somewhere around 10⁵ rows/s"; it dominates at
5.6×10⁵, so that prediction was right in kind and conservative in threshold. This is the
Arrow-batch `Sink` argument arriving with a number attached, a phase earlier than expected.

**Right: three to four orders above the live rate.** 37,425 against the venue's 10.0
frames/s is 3,742× — and the two are never reported as one number.

**Right: the wake-up cost disappears.** `parse_event` + `level_rows` is 16.1µs here against
`bench/cadence.py`'s 12.8µs p50 back-to-back and 235µs with 100ms idle between frames. The
unthrottled replay sits with the former, nowhere near the latter, which retroactively
confirms the Phase 0 finding that the live p99 was mostly wake-up cost.

**Right: 100% of drops and reorders on a live book, 0% of clock jitter.** Jitter moved
`receive_ts` on all 1,200 frames and produced **zero** gaps and a byte-identical book — the
control worked, and nothing in the pipeline sequences by clock.

**Right, and it is a defect: a duplicate frame is reported as a gap.** 29 duplicates, 4 gaps,
a full re-bootstrap each time, and the book correct throughout. Pinned by
`test_a_duplicate_frame_is_reported_as_a_gap` so that changing it is a decision. The fix is
one branch — `event.final_id <= prev_final_id` → already applied, skip — but it belongs with
the state machine in Phase 6, not bolted on here.

### What the number means, and what it is not

37.4k frames/s is **not a wall**. It is one core, one process, pure Python, with two known
optimisations not yet taken and a third that the profile above just made the case for.
Measured headroom, same corpus:

| | µs/frame | note |
|---|---|---|
| Today | 26.7 | |
| `scaled_int` behind a bounded cache | **−3.9** | measured: 5.60 → 1.70µs/frame, and it is 49% of `parse_event`. 5,661 distinct strings in 32,272 — already on the Phase 9 list, and it needs a size bound before it is a speedup rather than a leak |
| `msgspec` typed instead of stdlib `json` | ≈ −1.4 | Phase 0 measured 3.1 → 1.7µs on the payload decode |
| Batch rows instead of yielding one at a time | ≈ −5 | most of the 10.7µs generator protocol; this is what the Arrow-batch `Sink` does anyway |
| Arrow columnar builders instead of `LevelRow` dicts | ≈ −4 | the 4.7µs of per-row dict construction |

Taking the first three lands near **16µs/frame ≈ 60k frames/s**; all four, near **12µs ≈ 80k**.
None of that is speculative work — every line is already on the phase list for other reasons.

**Against the target and against the plan**, which are two different comparisons:

- `NOTES.md` § *The volume dial* sets **sustained 50k msg/s** through the full path. Today's
  37.4k **misses it by 25%**, and the profile says exactly where the 25% is. That is the
  honest state: a missed target with a reason beats a met target nobody can reproduce.
- The *planned live load* is nowhere near either number. A few hundred symbols across three
  venues on 100ms channels is ~9k frames/s aggregate — **4× under the current ceiling** — and
  10⁷ rows/day is 116 rows/s against a measured 563k. The venue-side volume this project
  plans for is not what the ceiling binds.

So the ceiling is a claim about the code, and the code is currently ~4× faster than the
workload and ~25% slower than the stated stress target. Both of those are worth saying; the
one that would be dishonest is quoting 37.4k as a capture rate.

### Not predicted at all, and the most useful thing here

**With an out-of-band snapshot, one dropped frame costs up to a full snapshot interval of
untrusted book.** Binance repairs a gap only by refetching over REST, and a *recording* cannot
conjure a snapshot the live source never fetched — so recovery waits for the next periodic
one. At 30s intervals that is up to 300 frames. Measured, same capture, varying the drop rate:

| drop rate | injected | gaps | untrusted frames | live at exit | top-N vs clean |
|---|---|---|---|---|---|
| 0.02 | 29 | 4 | **84.1%** | no | — |
| 0.005 | 13 | 4 | 64.2% | no | — |
| 0.002 | 5 | 4 | 63.9% | no | — |
| 0.001 | 2 | 2 | 37.5% | **yes** | top 10 / 50 / 250 all match |

Three things follow, and none of them were on the plan:

- **Convergence is only a claim about a run that ends trusted.** Above 0.001 the session ends
  mid-interval, so its book says where it stopped rather than whether it recovers. Comparing
  it to anything is the mistake `live_at_exit` now exists to prevent — and reporting a
  "book mismatch" from such a run would have been a break that was pure artifact.
- **Recovery latency is bounded by the snapshot interval, not by the pipeline.** That is a
  venue property, not a code property, and it is the argument for Phase 3 sizing shards by
  recovery time rather than by venue cap. It is also the concrete case for Kraken's row of
  the dialect table: an in-band rolling checksum revalidates continuously and never waits.
- **`snapshot_interval_s` is a real tuning knob, and it was not in the plan.** It exists
  because an injected fault cannot conjure its own repair; it turns out to also set the
  worst-case untrusted interval, and Phase 8's Oracle 1 reads the same records.

---

## Phase 2 — venue adapters

### No prediction to grade

Phases 0 and 1 predicted a number and were graded against it. This phase has no performance
claim to make: it moves the venue behind a boundary and adds a second dialect, and the only
number it produces is clock skew. Inventing a prediction to keep the format would be
ceremony. The measurement below is a *survey*, and it was taken before any adapter was
written precisely so the adapters could not be written from memory.

### The channel surface

`uv run python -m tools.probe <venue>`, 12-15s per venue, 2026-09-06. One BTC book channel
each, live, unauthenticated.

| | Binance spot | Coinbase Advanced Trade | Kraken v2 |
|---|---|---|---|
| Endpoint | `stream.binance.com:9443/ws` | `advanced-trade-ws.coinbase.com` | `ws.kraken.com/v2` |
| Subscribe | URL path | JSON frame | JSON frame |
| Snapshot | out of band, REST | **in band**, ~4.9 MB | **in band**, depth-limited |
| Sequencing | range `[U, u]`, chained | `sequence_num`, +1 per message | **none** |
| Revalidation | — | — | CRC32 per update |
| Non-book shapes | 0 | 1 (`subscriptions`) | 3 (`heartbeat`, `status`, ack) |
| Level encoding | `[str, str]` pairs | named fields, strings | named fields, **JSON numbers** |
| Timestamp | `E`, epoch ms | RFC3339, ns in envelope | RFC3339, µs |
| Deepest fraction | 8 | 8 | 8 |
| msg/s observed | 10.1 | 15.0 | 7.1 |

**One project-wide `SCALE = 8` holds.** All three venues quote to eight decimal places and no
further, on both price and size, so a single scale keeps `price_ticks` comparable without a
consumer knowing the venue. `scaled_int` already refuses to truncate, so a venue that ever
exceeds it fails the run rather than corrupting a book key.

### Four things the survey changed

**The Coinbase feed choice decides whether the venue has gap detection at all.** The obvious
public endpoint — `ws-feed.exchange.coinbase.com`, channel `level2_batch` — delivers
`l2update` messages whose fields are `changes`, `product_id`, `time`, `type`. There is no
sequence number anywhere in them. An adapter on that feed cannot detect a dropped message by
any means, and would apply diffs to a book that drifts silently while staying plausible.
Advanced Trade's `level2` carries `sequence_num` on the envelope and is the feed this project
uses. Two endpoints for nominally the same data, and only one of them is adaptable.

**An in-band snapshot blows through the `websockets` default frame cap.** The library caps a
message at 1 MiB and closes the connection when one exceeds it; Coinbase's BTC-USD snapshot
is ~4.9 MB, so the first probe died 0.7s in with `1009 (message too big)` mid-snapshot. This
never surfaced in Phases 0 and 1 because Binance's snapshot arrives over REST and its diff
frames are small. `connect(..., max_size=…)` has to be raised for any in-band venue — raised
rather than disabled, since unbounded lets a venue drive our allocator.

**Kraken quotes prices as JSON numbers.** `{"price":79864.2,"qty":0.00000000}` — not strings.
`json.loads` therefore produces a Python float, and the project's hardest rule is that prices
are never floats. The venue's own decimal token is recoverable only by decoding with
`parse_float=str`; the probe reported *zero* decimal places on Kraken until it did, because no
price on that venue was ever a string. Two consequences worth writing down now: the CRC32
input is the token, not the value, so a float round-trip would break the checksum before it
could validate anything; and `orjson` and `msgspec` offer no `parse_float` hook, so Phase 0's
decoder ranking does not carry over to this venue unchanged.

**`datetime.fromisoformat` silently truncates nanoseconds.** Coinbase's envelope clock has
nine fractional digits (`…:25.933594987Z`) and `fromisoformat` returns a `datetime` with six,
without raising. The model's contract is one unit — ns — throughout, so the adapter parses the
fractional part itself rather than going through `datetime`.

### Kraken does not ship this phase

The plan made this conditional on one question: does Kraken's book channel carry a
per-message sequence? It does not. Its `data[]` entries are `symbol`, `bids`, `asks`,
`checksum`, `timestamp`, and the checksum is the whole of its integrity story.

The CRC32 was deferred from this phase as a Kraken-only concern. With no sequence, deferring
it does not leave Kraken with weaker gap detection — it leaves Kraken with **none**:
`in_sequence` would return `True` unconditionally and the book would drift silently, the
failure mode `NOTES.md` § *Steady state* names as the worst available. So Kraken moves to its
own phase and lands with the checksum as one coherent piece.

Binance and Coinbase already span two of the dialect table's three rows, which is what forces
the `in_sequence` / `gap_detected` / `snapshot_required` contract to mean two genuinely
different things. A third venue with no gap detection would have added a venue and subtracted
a guarantee.

### What the second dialect cost, which is the point of having one

Three bugs reached working code and were caught by measurement rather than by review. All
three are the same shape: an assumption that held for Binance, silently carried into a venue
where it does not.

**An in-band snapshot occupies a sequence number.** Coinbase numbers every message on the
connection — book updates, snapshots and subscription acks alike. The source and the transform
both tracked the sequence across frames only, so the frame *after* every snapshot failed the
chain rule, which asked for a repair, whose snapshot failed it again: **42 resubscribes in a
25-second run**, each pulling a 4.9 MB snapshot. Loud, and therefore cheap.

**A stored snapshot goes stale even when it arrived in band.** This one was not loud. The
reasoning that "an in-band snapshot cannot be stale, there is no fetch window" is true on
arrival and false thereafter: the transform keeps the last snapshot it saw, and a gap thirty
seconds later is judged against it. Rebuilding from it silently discards every update in
between and leaves a plausible-looking book. It showed up only as `crossed_books: 3` under
fault injection — the invariant check that Phase 0 added for no particular reason.

**A bare RFC3339 timestamp parses as local time.** `datetime.fromisoformat("…T10:00:00")`
returns a naive datetime that `.timestamp()` then interprets in the machine's zone, so every
`exchange_ts` would have been wrong by the local UTC offset — correct on a UTC container,
wrong by an hour on the laptop it was written on. Caught by a test asserting the epoch.

The generalisation worth keeping: **the second venue is not twice the work of the first, it is
where the first venue's accidents become visible.** Every one of these was latent in Phase 0's
design and undetectable with one venue in the tree.

### Clock difference per venue: measured here, exported in Phase 4

`TODO.md` asked to measure it *and* export it as a metric. The export half is not reachable
this phase, and the reason is worth recording rather than working around:

`RunContext` carries `run_id`, `logger`, `clock`, `should_stop` and `http` — **no metrics
handle**. `StandardMetrics` is constructed and owned by `WorkerApp`, pushed once at exit, and
its series are labelled `(source, stage)` with no `venue` and no histogram. Nothing inside a
`Source` or a `Transform` can export a series at all. Exporting therefore needs both an SDK
change to `RunContext` and a new series — which is exactly the deliberate §8 metric-surface
change Phase 4 already schedules. Doing it twice is worse than doing it once.

| | p50 | p90 | p99 | frames |
|---|---|---|---|---|
| Binance, live 20s | 113.8 ms | 150.8 ms | 367.5 ms | 199 |
| Coinbase, 40s capture | **−7.4 ms** | 177.6 ms | 432.5 ms | 624 |

**It is not clock skew, and the negative p50 is why that matters.** The quantity is
`receive_ts − exchange_ts`: one-way network delay *plus* the offset between the venue's clock
and ours, and the two are not separable without a round-trip estimate this collector never
makes. A negative median means Coinbase's stamps sit ahead of our clock by more than the
delay — so the number bounds delay from above and flags drift, and claiming anything more
precise from it would be false. It also inherits our own clock discipline: `receive_ts` is a
wall clock, and an NTP step lands in this distribution as a spike no venue caused.

Binance's 113.8 ms median is partly the channel: `E` is the event time and the diff channel
aggregates on a 100 ms cadence, so roughly half a window is built in before any network.

### What the boundary cost per frame: nothing measurable

Putting the venue behind a protocol adds an attribute lookup and a bound-method call to every
parse. `bench/cadence.py`, same corpus and same machine as Phase 0:

| | p50 | p90 | p99 |
|---|---|---|---|
| Phase 0, venue fused into the transform | 12.8 µs | 31.5 µs | 192.3 µs |
| Phase 2, venue behind `VenueAdapter` | 12.2 µs | 29.9 µs | 183.7 µs |

Slightly *faster*, which is noise rather than an improvement — the honest reading is that the
indirection is invisible next to the JSON decode and the per-level scaling that dominate this
path. Worth measuring anyway: "an abstraction is free" is the kind of claim this project is
supposed to check rather than assert.

### Symbols: 188 base assets, and one that nearly went missing

188 base assets are quoted against a USD-ish asset on all three venues (Binance USDT,
Coinbase USD, Kraken USD), resolved against live instrument lists and committed with
`tools/symbols.py` to re-derive them.

The mapping is a rule plus two exceptions rather than a 564-row table. The exceptions are
Kraken's pre-ISO `XBT` and `XDG`, and how they surfaced is the useful part: the first
generator intersected the three venues **on their own base keys** and reported zero mapping
failures. Bitcoin never appears as `BTC` on Kraken, so it dropped out of the intersection
entirely — and the rule scored perfectly *because the one case that would have broken it had
been excluded from the sample*. A validator whose sample is filtered by the thing it is
validating always passes. The generator now matches on the symbol the rule produces, so an
unmapped code fails loudly instead of vanishing.

---

## Phase 2.5 — Kraken

The venue Phase 2 refused to ship. Its book messages carry no sequence of any kind, so an
adapter without the CRC32 would answer `in_sequence` with `True` forever and let the book
drift while staying plausible. Both landed together, as that refusal said they would have to.

### What the probe settled

`uv run python -m tools.probe kraken --duration 60 --land`, depth 1000, BTC/USD, 5129 book
messages. The gate was one question — does recomputing the CRC32 from the raw JSON tokens
reproduce the venue's own `checksum` field — and the answer was 5129 of 5129.

| | |
|---|---|
| Checksum input | the source token, `.` and leading zeros removed |
| Price / qty encoding | JSON numbers, **already at instrument precision** |
| Depth options | 10 / 25 / 100 / 500 / 1000 — no full-depth channel |
| Two depths, one connection | refused: `{"error":"Already subscribed"}` |
| Sequence | none |
| `timestamp` | on every book message, µs, strictly monotone and unique |
| msg/s at depth 1000 | 95.7, against 7.1 at depth 10 in Phase 2 |

Two of those cancelled planned work. Because the token is already padded to the instrument's
precision, the CRC needs no `price_precision` table off the `instrument` channel — the whole
second data artifact that had been budgeted for. And because no book message ever arrived
with a bare integer where a decimal was expected, `parse_float=str` alone recovers every
token; `parse_int` never comes into it.

### The timestamp would have worked, and was rejected

It is on snapshots and updates alike and was strictly monotone and unique across all 5129
messages, so it would have served as the sequence and saved a counter. It is still a clock,
and this project's own rules say to align by update id and never by clock, and that wall
clocks step. A property that holds for sixty seconds is not a guarantee: two messages in one
microsecond would silently discard one. So the ordering is a counter of our own, `chains`
over it is trivially true, and the adapter says so in as many words. Sequencing is not
evidence on this venue; the checksum is.

### The checksum needs a book the reconstruction cannot provide

`Book` is keyed by integer ticks and holds no strings. The CRC is computed over the top ten
levels *as the venue spelled them*, so `Book` cannot serve that read at all — not slowly,
not at all. The adapter keeps its own view instead, bounded by the subscribed depth.

That inverts what `NOTES.md` § *Book representation* expected. Its `dict` verdict was made
conditional on a checksum venue turning the ordered top-N read into a per-frame cost, and the
per-frame cost is real — it simply lands somewhere else, and `Book` never sees it. Phase 6
benchmarks the same read:write ratio it always faced.

Where it does land, from `bench/checksum.py` over the 5129-message session:

| implementation | µs / message |
|---|---|
| sort the side, per message | 73.9 |
| integer keys, token cached per level | 26.0 |
| rebuilt only when a write could move it | **8.5** |

The median message touches **one** level, and only 18.5% of sides needed a rebuild at all —
so the top ten is cached with the price that bounds it and most messages skip the work.
The last row times the whole of `observe`, level construction included, which the other two
do not do. Its correctness is checked against the naive recomputation on every message of a
landed session, because an 8.7x optimisation that is right for a thousand messages and then
is not is worse than no optimisation.

The venue also does not delete every level that leaves its depth window: the book grew from
1000 to 1040 levels a side in sixty seconds while every checksum still matched. The view is
trimmed to bound that. A memory bound, not a correctness one — the CRC reads ten levels.

### Three bugs, and none of them was the checksum

The CRC was the part that worked first. What cost the time was everything around it.

**`XBT/USD` is a protocol nobody here speaks.** The first live run connected, subscribed,
received nothing and exited **zero**. `collector/symbols.py` committed `XBT` for bitcoin and
`XDG` for dogecoin — an exception table added in Phase 2 — and the v2 socket answers
`{"error":"Currency pair not supported XBT/USD","success":false}`. Those spellings come from
REST `AssetPairs.wsname`, which `tools/symbols.py` described as "the spelling the websocket
API accepts". It is **v1** naming. The v2 socket uses ISO codes throughout and its own
`instrument` channel lists all 188 committed bases under them, `XBT` and `XDG` appearing in
neither. So the mapping is a rule with no exceptions after all, exactly as the generator kept
claiming while reading the wrong list — and Phase 2's write-up, which called those exceptions
the useful part of that section, was describing a fix to the wrong layer.

**A rejected subscription is silent.** That run *passed*. The ack is `success: false` on a
socket that stays open, and every stage after the source tolerates a quiet stream, so the
worker logged `frames: 0` and returned success. The guard is venue-neutral because the ack
is not: no book message at all in the whole window is the one symptom a rejected subscription
has on any venue, and no working subscription produces it — even an illiquid symbol gets the
in-band snapshot.

**A snapshot at a healthy book is not always redundant.** This is the one worth having found.
`BookTransform` ignored a snapshot arriving at a live book: rebuilding would cost a full
book's worth of rows to arrive where the book already is. That reasoning holds for a snapshot
read *alongside* a stream that never stopped — Binance's REST call — and fails for one
obtained by unsubscribing and resubscribing, which is the only way either in-band venue will
send one. Levels deleted during that gap are absent from the new snapshot and never arrive as
deletes. No gap, no sequence break, and no failing checksum for as long as the staleness sits
below the top ten.

It surfaced as **43 crossed books in a 60-second replay in which all 6572 checksums passed** —
the book was wrong in exactly the region the CRC does not cover, which is as close to the
worst failure mode in this repo's vocabulary as it gets. Diagnosing it took locating the first
crossed record (2335) against the snapshot positions (3, 917, 2841): the corruption began
after the first resubscribe, not after a gap.

It applies to **Coinbase identically** and had been there since Phase 2. Nothing caught it
because `snapshot_interval_s` defaults to 300 and no test run lasted that long. Two Coinbase
tests asserted `bootstraps == 1` over a session containing six snapshots — they were encoding
the bug. The fix is one question on the adapter, `snapshot_supersedes`, answered False by
Binance and True by both in-band venues.

### Verification

45s live collect, depth 1000: 3271 frames at 72.7/s, 0 gaps, 0 crossed books, live at exit,
best bid/ask 79528.7 / 79528.8. Every checksum validated.

Cold replay of the capture that produced the 43 crossings: **0 crossed**, 11 bootstraps, live
at exit.

Fault replay, drop + reorder, seed 3: 10 gaps, 11 bootstraps, 0 crossed books, and it ends
**untrusted** — correctly. The last gap lands at record 5060 and the last snapshot at 5024,
so nothing in the recording can repair it. That is `_interval_elapsed`'s documented
limitation rather than a convergence failure: an injected fault removes frames from a
recording but cannot conjure the repair snapshot a live source would have fetched. On the
live path the source resubscribes on the failing checksum itself, within one message.

---

## Phase 4 — ClickHouse, the batch sink, and a service to run them

### The prediction, and where it came from

This phase's predictions were not written for it. They were already in `TODO.md`, from
before Phase 3 existed, and both are load-bearing:

- **"One row per price level at 10⁷/day makes that ordering load-bearing, so measure it."**
- **"Measure insert throughput at realistic batch sizes; that number sets the flush
  triggers."**

Both are wrong, in opposite directions, and the second is the more interesting failure.

### A day is 10⁹ rows, not 10⁷

`bench/volume.py` replays a Phase 3 capture through `BookRouter` and counts rows by action.
Snapshot rows are reported apart from diff rows and only the diffs are extrapolated: a
bootstrap emits the whole book — ~370k rows to bring 185 Kraken books up at depth 1000 — so
a capture that reconnected a few times is mostly bootstrap by row count, and dividing total
rows by the span would measure the session's shape rather than the venue's rate.

| venue | span | symbols | set | delete | snapshot | diff rows/s | diff rows/day |
|---|---|---|---|---|---|---|---|
| binance | 717s | 181 | 686,579 | 113,514 | 549,582 | 1,116 | 96,385,181 |
| coinbase | 149s | 188 | 582,490 | 241,346 | 371,516 | 5,535 | 478,194,752 |
| kraken | 149s | 184 | 609,173 | 198,587 | 113,287 | 5,404 | 466,862,749 |

**~10⁹ diff rows/day across the three, two orders of magnitude above the figure the schema
was to be sized against.** The 10⁷ estimate was written when a run carried one symbol; 188
symbols on three venues is simply a different problem, and every downstream number —
retention, the archive tier, the read layer's partition pruning — inherits the correction.

One bug found in writing it, worth keeping because it inflates nothing and deflates a lot:
a raw-landing channel accumulates **one file per run**, so first-to-last across the
directory spans every idle hour between sessions. `binance-p3` read as 6439 seconds of
capture for 289k records that arrived in three short bursts, understating its rate 6x.
Spans are summed per file.

### The sort key: `(venue, symbol, receive_ts, seq)`

`bench/clickhouse.py` loads ten million identical rows — real rows from a Phase 3 capture,
tiled forward in time — into two MergeTrees that differ only in `ORDER BY`, both partitioned
`toDate(receive_ts)`.

| candidate | on disk | bytes/row | one symbol, one minute | whole load, by symbol |
|---|---|---|---|---|
| `(receive_ts, venue, symbol)` | 344 MB | 34.4 | 458,752 rows read | 10,000,000 rows read |
| `(venue, symbol, receive_ts, seq)` | **258 MB** | **25.8** | **16,385 rows read** | 10,000,000 rows read |

Rows read rather than wall time, because on a warm single node wall time mostly measures the
page cache. Instrument-time wins 25% on storage and 28x on the point read, and loses nothing
on the full scan — both read everything, as a full scan should.

The bytes/row figures are **optimistic and not a storage claim**: a tiled day repeats one
price series and compresses better than a real one would. The ranking survives that; the
absolute number does not, and does not belong in the README.

A third candidate, `(venue, symbol, side, price_ticks, receive_ts)`, was dismissed without
loading it. It needs `allow_nullable_key` — `side` and `price_ticks` are null on control
rows — and it scatters every time-range read across the partition.

### `Int64` cannot hold a scaled tick

Found by the loader refusing a row, not by reading the model. `SCALE = 8` puts the Int64
ceiling at about 9.2e10, and Kraken's depth-1000 `BCH/USD` book carries an ask at
**886,110,000,000.00** — a junk order parked far from the touch, entirely real, 8.9e19 once
scaled. Python's int is unbounded and every test to date used one symbol near the touch, so
nothing had put a tick into a fixed-width column before.

`price_ticks` and `size_lots` are `Decimal(38, 0)` — a 128-bit integer — at 16 bytes a value
instead of 8. The alternative was `Int64` plus a refused row, which means one junk order on
one symbol killing a run. `scaled_int` already refuses to truncate rather than corrupt a book
key; widening the column is the same trade on the other side of the pipeline.

### Insert throughput does not set the flush triggers

| batch | rows/s | active parts after | rows/part |
|---|---|---|---|
| 10,000 | 881,063 | 13 | 153,846 |
| 50,000 | 1,154,997 | 8 | 250,000 |
| 250,000 | 1,223,083 | 8 | 250,000 |
| 1,000,000 | 1,173,221 | 2 | 1,000,000 |

The TODO said this number would set the triggers. **It does not, and the reason is the
finding.** The worst batch size measured sustains 881k rows/s against a measured multi-venue
arrival of ~12,000 rows/s — 70x headroom at *every* size, so throughput does not discriminate
between them at all. What does is latency: a batch is buffered rows, and buffered rows are
staleness. The triggers are therefore a latency choice, and `batch_max_rows` is only the
ceiling that bounds memory during a burst.

Defaults: **50,000 rows / 2.0 seconds**. At ~12,000 rows/s the time trigger fires first, at
roughly 24k rows and 43k inserts a day — comfortably inside what the parts column above says
the server merges away.

### Verification

45s live service run, Kraken, four symbols at depth 100, into the local node:

- 22,380 rows in ClickHouse, and `BookRouter` counted **22,380** — the partial batch at drain
  was written exactly once, which is the property the batch contract exists to promise
- 0 drops, 0 reconnects, 0 gaps, 0 crossed books, 4 books live at exit, 0 unrouted
- `/healthz` 200 from the first moment; `/readyz` 200 once records were flowing
- SIGTERM drained rather than truncated
- A second run at a 4s push interval: 6 pushes in 22s — five periodic and one at exit, which
  is the whole point of the archetype

### Two bugs the service surfaced that a bounded run could not

Both were invisible for the same reason: a bounded run always ends.

- **The drain read the stop flag only when the queue was empty.** `ShardSupervisor._drain`
  checked `ctx.should_stop()` inside the `Empty` branch. A bounded run reaches an empty queue
  eventually so it always noticed; a service under sustained load never does, and a SIGTERM
  arriving while records keep coming would never have landed.
- **A service ran for `duration_s` and stopped.** The supervisor's deadline is
  `monotonic() + duration_s`, and the first service run exited immediately because the
  duration passed to it was zero. A service's deadline has to be one that never arrives.

### The connection supervisor moves into the SDK

The phase's fourth primitive, and the one that was deferred twice: `ShardSupervisor` had one
consumer, and a primitive extracted from one consumer is a guess. What settled it is that
the re-scope makes the SDK diff the deliverable — a supervisor that stays here is a
supervisor the SDK never got.

**The seam is "does it know what a venue is".** Everything that does not — N reader threads,
staggered opens, per-connection backoff and failure counts, the bounded queue, the single
drain, the transport-vs-data error split — is `ConnectionSupervisor`. Everything that does
stays: the REST pacer (a venue's budget is per IP and the number comes from its published
weights), the snapshot thread, the `seq` stamp (this capture format's arrival order, and what
makes `FaultInjector` reproducible) and the repair latch on a drop.

`FrameSource` needed **no change at all** to satisfy the new `Connection` protocol —
`fetch(ctx, until=...)` and a `name` were already its shape — which is the evidence the seam
was cut in the right place rather than negotiated into one. `collector/supervisor.py` traded
200 lines for 132 — the threads, the queue, the reconnect loop and the drain out; a pacer, a
snapshot thread and a repair latch in — and `tests/test_supervisor.py` passes **unedited**:
the no-shared-fate test,
the full-queue drop, the busy-drain stop, the liveness reconnects and the loud fatal shard
all still assert exactly what they asserted before.

Three things the extraction changed on purpose:

- **A supervisor with no duration is the default**, rather than a caller's obligation to pass
  `math.inf`. The Phase 4 bug where a service ran for `duration_s` and stopped was a missing
  argument at a call site; it is now the shape of the type.
- **`fetch` is typed as a generator**, so the wrapper can `close()` it and have the reader
  threads stopped and joined where it chooses instead of when the object is collected.
- **The shard-to-symbol map is logged at start.** `failures_per_shard=[0,1,0]` and a fatal
  shard's index were unreadable without it — the old code put the symbols in the fatal log
  line and nowhere else, so the common case (a per-shard count) had nothing to key on.

Two out-of-band paths kept the extraction honest. Snapshot records fetched off the reader
threads go through the supervisor's public `offer` rather than a queue of their own, because
a second queue is a second drop policy and a second counter. And the drop callback takes the
record, not the connection: the snapshot thread enqueues on behalf of the shard that owns the
symbol, not on its own behalf, so a connection-shaped callback would have latched the repair
on the wrong book.

### The drop policy stops being a claim

`queue_depth` and `messages_dropped_total{reason}` are new standard series, and §8 freezes
that surface, so the widening is deliberate — recorded in `ARCHITECTURE.md` and the SDK
changelog. Until now `ShardSupervisor.dropped` was an int in a log line at the end of a run:
the guardrail every one of these documents leads with was, in production, unobservable.

They carry no `queue` label. The TODO said `queue="ring"|"batch"`; `BatchingSink` turns out
to accumulate on the calling thread and flush inline, so it has an occupancy but no producer
that can outrun it and no way to drop. See `NOTES.md` § *The queue metrics carry no `queue`
label*.

Depth is sampled on the drain's `_DRAIN_SLICE_SECONDS` tick rather than set per record —
nothing reads it faster than the metrics push, and the drain is the hot path. Drops are
counted per event, because a drop is rare by construction and an uncounted one is the exact
failure the series exists to catch.

### Verification

Four Kraken symbols at depth 100, into the local ClickHouse node, as a service:

- **190s live run: 85,506 records, 0 drops, 0 reconnects, 0 fatal shards, 0 rotations.**
  4 books live at exit, 0 gaps, 0 crossed books, 0 unrouted.
- **126,982 rows in ClickHouse, and `BookRouter` counted 126,982** — the partial batch at
  drain was written exactly once, still true with the queue on the other side of the SDK
  boundary.
- `/healthz` 200 from the first moment; `/readyz` 503 until records flowed, then 200.
  SIGTERM drained rather than truncated.
- `queue_depth` populated from a live drain (6 at the sample), `messages_dropped_total` 0.

And the drop path, forced rather than waited for — a queue of one against a ~580 msg/s feed,
with a consumer sleeping 2ms a record:

- **1,889 drops, all counted on `messages_dropped_total{reason="queue_full"}`**, against 799
  records landed. The run kept producing rather than stalling, which is the guardrail.
- 696 resubscribes over 14 seconds: every drop latched its symbol for a repair snapshot, and
  the existing recovery machinery acted on every one of them.

---

## Phase 6 — the oracle

`collector/oracle.py`: the reconstructed book diffed against Binance's own periodic REST
snapshot, id-aligned. The reasoning for building it, and for building it on one venue rather
than three, is `NOTES.md` § *The REST oracle* and § *Why Coinbase has no oracle*.

### The prediction

Written before the first live run, and left alone afterwards whichever way it goes.

The reconstruction is exact arithmetic. Sizes are absolute set-to-value, the sequence is
checked frame by frame, and a break in it takes the book untrusted before anything else
happens — so on a session with no gaps there is no mechanism by which an applied frame can
leave the book wrong. The prediction is therefore not "small divergence" but **zero**:

- **Every comparison clean, on a run with no gaps.** Not most of them. A single broken level
  on a clean session means the diff application is wrong, and that is the finding.
- `unaligned` in the low single digits at most, and zero if the ring is generously sized —
  it exists for a snapshot fetch slow enough to outrun 4096 retained frames, which at a
  100ms channel is roughly seven minutes of them.

The way this prediction fails that would *not* mean the book is wrong, and the thing to look
for first if it does:

- **The bootstrap truncation floor.** The book is seeded from a `limit=5000` snapshot, so it
  begins knowing nothing below that snapshot's worst bid. The diff stream teaches it about a
  deeper level only when that level *changes*; one that sat there untouched at bootstrap is
  absent from the book permanently. If the market later widens and a subsequent snapshot's
  band floor drops below the bootstrap floor, that gap between the two is a region the book
  was never authoritative over, and every untouched level in it reads as a break.
- The signature is unmistakable and is what to check: breaks **one-sided** (the snapshot has
  a level, the book does not), **concentrated at the deep end** of the band, and **growing
  with run length**. Breaks near the mid, or two-sided ones, are the book being wrong.
- If that is what shows up, the fix is a comparison change and not a book change: floor the
  band at the bootstrap snapshot's own worst price as well as at the current one, so the
  oracle judges only the depth the book was ever given.

Predicting zero is a deliberate choice of a falsifiable number over a safe one. A prediction
of "under 0.1%" would be unfalsifiable by this run and would have quietly excused exactly the
bug the oracle exists to catch.

### The measurement

Binance spot, BTCUSDT and ETHUSDT on one shard, `snapshot_limit=5000`,
`snapshot_interval_s=45`, captured live and then replayed — so the number below comes from a
landed file and anyone can reproduce it from the same bytes.

**479s live capture: 9,587 records, 20.0 msg/s, 0 drops, 0 reconnects, 0 snapshot failures.**
50 snapshots landed, 2 of them spent on the bootstraps.

Replayed:

- **45 comparisons, 45 clean. 0 diverging levels in 446,830 compared.**
- 0 gaps, 0 crossed books, 2 bootstraps, both books live at exit.
- **3 comparisons unaligned** — 48 snapshots offered, 45 taken, so 93.8% coverage.
- Two replays of the same capture produced identical counters, down to the per-symbol
  breakdown. The oracle reads only what is in the file and touches no clock.

Per symbol, because one healthy book can hide another:

| | BTCUSDT | ETHUSDT |
|---|---|---|
| frames | 4,789 | 4,748 |
| comparisons / clean | 29 / 29 | 16 / 16 |
| levels compared | 288,435 | 158,395 |
| levels broken | 0 | 0 |
| unaligned | 1 | 2 |

**What the 3 unaligned are, measured rather than assumed.** All three are the same condition
— the snapshot was ahead of the book, by 4, 29 and 284 update ids. That is the REST read
seeing the venue's book at a position the 100ms-batched diff stream had not delivered yet,
which is a property of reading two surfaces of one venue and not a fault. Neither of the
other two rejection conditions fired once: nothing was ever evicted from the 4096-frame
ring, so its size is not a limitation at this rate.

Those three are recoverable, and deliberately not recovered: holding the snapshot aside and
comparing once the book reaches its id would take coverage to essentially 100%, at the cost
of carrying a pending snapshot per symbol. 93.8% coverage with a stated reason is worth more
than the extra state, and the reason is the kind that gets asked about.

### Grading the prediction

**Wrong, on the number that was predicted.** Zero was predicted and the first run measured
173 broken levels in 40,001 — on a session with no gaps, which is the case the prediction
called impossible.

**Right on the alternative, including its signature.** The prediction named the bootstrap
truncation floor as the way this could fail without the book being wrong, and said what to
look for: breaks one-sided, concentrated at the deep end, growing with run length. The
measurement was 173 breaks, **100% one-sided** (the snapshot had a level, the book did not),
**zero size disagreements anywhere**, and every one of them ranked between 4927 and 4999 of
5000 — the deepest 1.5% of the band. Bids clean, asks broken, because that is the way the
price had moved.

That signature is what makes this a diagnosis rather than a guess. A book that is genuinely
wrong disagrees about *sizes*, on levels it was told about, near the mid. Not one level in
40,001 did.

The fix was the one the prediction had already named — floor the comparison band at the
bootstrap snapshot's own extent as well as the current one — and on the *same capture* the
number moved from 173 breaks in 40,001 levels to 0 in 39,828. The denominator fell by
exactly 173, the count that had been breaking. That equality is the whole check: the fix
removes levels the book could not know, rather than hiding levels it got wrong.

**What predicting zero bought.** A prediction of "under 0.1%" would have been satisfied by
the 0.43% — near enough to wave through — and the truncation artifact would have shipped
inside the headline number, growing with every hour of run length, unnoticed because it
looked like the small non-zero divergence everyone expects. The falsifiable prediction is
what turned a plausible number into a diagnosis. The run that produced a non-zero number was
worth more than the run that produced zero would have been.

**What the number does and does not claim.** It says: over 446,830 level-comparisons against
Binance's own REST book, at the depth both sides were authoritative over, the reconstruction
did not differ once. It does not say anything about Coinbase, which has no id-alignable
independent read (`NOTES.md` § *Why Coinbase has no oracle*); about Kraken, whose CRC32 is
its own oracle and reported 0 breaks over the Phase 2.5 sessions; about depth beyond the top
5000; or about a run long enough to see a gap — this one had none, so the oracle has not yet
been exercised against a book that recovered.
