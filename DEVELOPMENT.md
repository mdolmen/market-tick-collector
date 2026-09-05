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
