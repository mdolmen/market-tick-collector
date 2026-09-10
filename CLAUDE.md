# CLAUDE.md — market-tick-collector

High-volume L2 order book capture from public crypto exchange websockets, and the
second consumer of `data-pipeline-core`. Read `NOTES.md` for the decisions and
their rationale, and `TODO.md` for the sequenced build plan, before making
changes.

These are behavioral guidelines (derived from Karpathy's notes on LLM coding
pitfalls), specialized to this repo. They bias toward caution over speed; use
judgment on trivial tasks.

## 1. Think before coding

- State assumptions explicitly. If a requirement is ambiguous, ask — don't pick
  silently.
- If multiple interpretations exist, surface them. If a simpler approach exists,
  say so and push back when warranted.
- If something is unclear, stop and name what's confusing.

## 2. Simplicity first

- Minimum code that solves the problem. Nothing speculative.
- No features, abstractions, "flexibility", or error handling beyond what was
  asked or what the contract requires.
- If you write 200 lines and it could be 50, rewrite it. Ask: "would a senior
  engineer call this overcomplicated?"

## 3. Surgical changes

- Touch only what the task requires. Don't "improve" adjacent code, comments, or
  formatting; match existing style even if you'd do it differently.
- Clean up orphans **your** changes create (unused imports/vars). Don't delete
  pre-existing dead code — mention it instead.
- Every changed line should trace directly to the request.

## 4. Goal-driven execution

- Turn tasks into verifiable goals: "fix bug" → "write a failing test that
  reproduces it, then make it pass".
- For multi-step work, state a brief plan with a verify step each, then loop until
  green.

---

## Project-specific guardrails

These override the generic guidance where they conflict — they are why this repo
exists.

- **This is not a crypto project.** The data is crypto because that is where free
  high-volume L2 lives. Funding rates, open interest, liquidations, mark price,
  perp-vs-spot semantics, microstructure interpretation, cross-venue arbitrage and
  anything resembling a signal are all out of scope. No trading, no strategy, no
  backtesting. Scope creep toward "and then I predict the price" destroys what
  makes the project credible.
- **The normalization boundary is the design.** An adapter's only job is turning
  one venue's frames into the normalized record model. Nothing downstream — book,
  batching, sink, recon, metrics — may learn which venue a record came from except
  as a label. If downstream code needs to know the venue, the boundary is in the
  wrong place.
- **One honest exception, confined.** Sequencing semantics genuinely differ per
  venue. That difference stays *inside* the adapter, behind a uniform
  `in_sequence` / `gap_detected` / `snapshot_required` contract. See `NOTES.md`
  § *Sequencing dialects*.
- **Leaks get fixed in the SDK, not worked around here.** When `data-pipeline-core`
  doesn't fit, change the SDK. That discipline is the entire point of having a
  second consumer. Equally: venue logic, symbol normalization and venue caps are
  business logic and never go into the SDK.
- **Never block the socket read.** A market data feed has no flow control; stalling
  the reader costs the connection and a re-bootstrap of every book on it.
  Backpressure drops or sheds, and never propagates upstream. See `NOTES.md`
  § *Backpressure*.
- **Prices are never floats.** Integer ticks, `Decimal`, or the venue's own decimal
  string — they are book keys and checksum inputs.
- **Numbers, not adjectives.** Every performance claim ships with a committed,
  reproducible measurement. Live capture rate and replay ceiling are two different
  numbers and are never merged. Write the prediction down *before* profiling; a
  wrong prediction is part of the record, not something to quietly correct.
- **Decisions live in `NOTES.md`.** When something moves from OPEN to DECIDED,
  record it there with the reasoning, then reflect the work in `TODO.md`.
- **Ticking a `TODO.md` box does not rewrite it.** Tick it and leave the line
  alone. The reasoning goes in `NOTES.md` and the measurements in
  `DEVELOPMENT.md`; a plan that grows an explanation every time something ships
  stops being scannable, which is the only thing it is for.

## Conventions

- Python 3.11+, strict type hints; `mypy --strict` and `ruff` must pass.
- Reuse `data-pipeline-core` for runtime, metrics, resilience and storage
  adapters. Don't reimplement plumbing the SDK already owns.
- Outbound HTTP goes through `ctx.http` so it inherits retry, backoff, the circuit
  breaker and the status metrics.
- New mechanisms ship with tests. Recovery and gap paths are exercised through the
  fault injector, not asserted by hand.
- Benchmarks are committed and reproducible. An uncommitted number is not a claim.

## Git

- Commit often. Each commit is self-contained: it builds, it makes sense on its own, and it does one thing. Unrelated cleanups go in their own commit.
- Commit messages in English, following [Conventional Commits](https://www.conventionalcommits.org/) — `<type>(optional scope): description`.
- **Type**: one of `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`, `perf`. An optional scope names the area (e.g. `feat(adapter):`). A breaking change adds `!` before the colon (`feat!:`).
- **Subject**: single line, imperative mood, lower-case after the colon, no trailing period. Keep it tight.
- **Body** (only when needed): blank line after subject, then a short paragraph explaining the *why*. If there are multiple distinct points, use one bullet (`- `) per point instead of prose.
- No `Co-Authored-By` trailers unless explicitly requested.
