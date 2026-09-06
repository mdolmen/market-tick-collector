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
