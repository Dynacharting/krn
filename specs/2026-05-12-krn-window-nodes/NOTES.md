# Discovery notes — krn-window-nodes

Seeded from Linear ticket ENC-297 (KRN: Window node — buffer ticks within a period, emit reduced batch on boundary). The ticket already framed the problem clearly and offered two design shapes (A: single Window node; B: TumblingWindow + Worker). Discovery focused on three open dimensions.

## Q1 — Where does the reduce live?

Reading `Worker.hpp` revealed the ticket's option (B) didn't actually compose: `Worker` accumulates incoming values *internally* and applies its `Fn` over the accumulator, so feeding a per-boundary `vector<double>` from a paired `TumblingWindow` would treat the vector as one element of `Worker`'s accumulator, not as the bucket to reduce. This split the original (B) into:

- (B′) TumblingWindow + a *new* `VectorReducer(fn)` node — single-purpose, vector-shaped reducer
- (C) Extend `Worker` with a "boundary input" channel — couples Worker to timer semantics

User picked **(B′)** explicitly for modularity.

## Q1.5 — fn registry overlap

Worker's `FunctionMap` is shaped for the variant-typed accumulator (`ArgType(Span<const ArgType>)`); `VectorReducer`'s domain is concretely `double(Span<const double>)`. New `VectorFnMap` registry — accepted as part of the (B′) recommendation.

## Q2 — Scope boundary

User chose **KRN-only**. The saved-chart-slice seed switch (forum's `db/seed/seed.go:candlesV1NEXOSavedScene` flipping back to `pipeline_json` subscriptions that drive the new nodes) is a follow-up proposal — needs its own live-stack verification surface (real per-minute candles painting in the browser).

## Q3 — Empty-bucket behavior

User chose **(a) no emit**. Matches `BucketTime + Aggregate` silence-on-no-data. A chart that wants per-minute bars even on quiet markets is a future "EmitOnEmpty" config addition (one line) if it bites.

## Refinements introduced during discovery

- **Self-contained timer in `TumblingWindow`** rather than composing via a separate `BucketTime` with a "boundary input" channel. Multi-input nodes in KRN today only follow the Aggregate (N-parallel-arrivals) pattern; a "value stream + control signal" shape would be a new edge case in `INode` / `TreeBuilder`. Keeping the timer internal mirrors `BucketTime`'s existing lifecycle and minimizes blast radius.
- **Three building blocks total** (one already exists): `BucketTime` (existing wall-clock tick source, untouched), `TumblingWindow` (new), `VectorReducer` (new). OHLC = four `VectorReducer ← TumblingWindow ← Listener` expressions feeding a Compound buffer in embassy.
