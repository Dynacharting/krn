# SPEC: KRN windowing primitives — TumblingWindow + VectorReducer

**Slug:** krn-window-nodes
**Date:** 2026-05-12
**Status:** Draft
**Author:** ndrandal
**Repo:** krn (sole)

## Problem

KRN has no primitive for "buffer ticks since the last clock boundary, reduce on the boundary." `BucketTime` aligns timer ticks to a wall-clock period; `Aggregate` joins N **parallel** inputs (`inputs:[node, node]`); `Worker` reduces a per-symbol accumulator that's never cleared on a clock boundary. Composing these does not produce per-period reduction over a sequential stream. Surfaced during the saved-chart slice's smoke prep (ENC-292): the audit's "BucketTime + Aggregate + Worker" recommendation was wrong about Aggregate's semantics, and `TreeBuilder` rejects `arity:0` configs anyway. Without a windowing primitive, true minute-bucketed OHLC and per-minute volume sums are inexpressible — the saved-chart slice currently shows running OHLC over `taHistoryMax`, not real per-minute candles.

## Proposed change

Two single-purpose nodes that compose:

- **`TumblingWindow(periodMs, input)`** — accumulates incoming scalars per `(streamKey, field)` into an internal buffer; an internal wall-clock-aligned timer (mirroring `BucketTime`'s alignment) flushes the buffer at every `periodMs` boundary by emitting one `StreamValue{value=vector<double>}` downstream and clearing. Empty bucket = no emit. Self-contained timer (no separate `BucketTime` companion in the JSON) — simpler `TreeBuilder` shape than a multi-input wiring.
- **`VectorReducer(fn, input)`** — receives one `vector<double>` per upstream emit, looks up `fn` in the existing `FunctionMap` registry (whose plain reducers are already shaped `double(const vector<double>&)`; Worker uses the same registry via a `TreeBuilder` adapter), emits one scalar `double` downstream per call. Existing fns (`sum`, `mean`, `max`, `min`, `spread`, …) are usable as-is.

Composed JSON shape (one minute of NEXO highs, reduced to a max):

```json
{ "type": "VectorReducer", "fn": "max",
  "input": { "type": "TumblingWindow", "periodMs": 60000,
             "input": { "type": "Listener", "streamKey": "NEXO", "field": "highPrice" } } }
```

Per-minute OHLC = four such expressions (max for high, min for low, first for open, last for close).

## Scope

- New header + impl: `include/krn/nodes/TumblingWindow.hpp`, `src/nodes/TumblingWindow.cpp`. Lifecycle mirrors `BucketTime` (timer thread, `shared_from_this`, synchronous `shutdown()`, `start()` after construction).
- New header + impl: `include/krn/nodes/VectorReducer.hpp`, `src/nodes/VectorReducer.cpp`. No timer; a thin pass-through transform.
- No new registry. `VectorReducer` resolves its `fn` through the existing `FunctionMap::instance().getFunction(name)` at construction; if `first`/`last` aren't already registered, they get added to `registerBuiltinFunctions()` alongside the existing `sum`/`mean`/`max`/`min`/`spread` set.
- `NodeTypeRegistry` registrations in `src/NodeRegistry.cpp` (`TumblingWindow`, `VectorReducer`) so `TreeBuilder` resolves them from the JSON `type` field.
- Unit tests at `tests/nodes/TumblingWindowTest.cpp` and `tests/nodes/VectorReducerTest.cpp` covering: per-period boundary emit + reset, empty-bucket no-emit, late-arriving values within a bucket all contribute, concurrent `onValue` from multiple threads, shutdown joins the timer cleanly.
- One-line addition to `docs/atomic-keys.md` or a new `docs/window-nodes.md` documenting the JSON shape + the example above.

## Non-goals

- **No changes to `Worker`, `Aggregate`, `BucketTime`.** Their contracts are untouched; this proposal only adds.
- **No new wire shape or `TreeBuilder` mechanism.** The new node types slot into the existing `{type, ...}` JSON pattern — no schema changes, no proto changes, no `treaty` work.
- **No new reducer registry.** `VectorReducer` reuses the existing `FunctionMap` (whose `Func = double(const vector<double>&)` is already vector-shaped — Worker's variant-typed `Fn` is a `TreeBuilder` adaptation, not the registry shape). One source of reducer names; one bind point for new fns.
- **No saved-chart-slice seed switch.** Forum's `db/seed/seed.go:candlesV1NEXOSavedScene` continues to use bare atomic keys (running OHLC over `taHistoryMax`). Switching to `pipeline_json` subscriptions that drive these new nodes is a separate follow-up proposal — that work needs a live-stack verification surface (browser shows real per-minute candles) that's out of scope here.
- **No catalog/editor surfacing in `pipelines-panel-real`.** The web editor's `gma-catalog.ts` doesn't know about `TumblingWindow` or `VectorReducer`. Adding them needs catalog + editor + storage + adapter changes in lockstep — its own ticket per the pipelines-panel-real PLAN's "out of plan" notes.
- **No backfill, no replay.** A node started mid-period buffers from-now; the first emit reflects whatever arrived between construction and the next aligned boundary.

## Acceptance criteria

1. `ctest` passes including ≥ 5 new test cases across `TumblingWindowTest.cpp` + `VectorReducerTest.cpp` (boundary emit, empty-bucket no-emit, in-bucket accumulation, concurrent inputs, shutdown joins timer).
2. A composed pipeline `VectorReducer(max) ← TumblingWindow(periodMs=1000) ← Listener(NEXO,highPrice)` subscribed via WS with a feed pushing 100 ticks/s emits one scalar per second for ≥ 3 consecutive seconds; the value matches the max of the ticks within that boundary (assertable in an integration test).
3. With no upstream values for an entire `periodMs`, no value frame is emitted for that boundary — verified by counting downstream emits over 5 empty boundaries.
4. `TreeBuilder` builds a valid DAG from a JSON config that uses both new node types nested in the example shape; an unknown `fn` for `VectorReducer` produces a build-time error frame, not a silent no-op.
5. `mage Bench` (or the existing `krn_tests` bench harness) confirms `TumblingWindow.onValue` is allocation-bounded by amortized vector growth (no per-tick `new` after the buffer reaches steady-state capacity); `VectorReducer.onValue` is 0 allocs/op for the built-in fns.
6. `shutdown()` on a live `TumblingWindow` joins the timer thread within the existing engine shutdown deadline; verified by a test that constructs, starts, and shuts down 100× back-to-back without thread leaks (`ASAN`/`TSAN` clean if the suite uses them).
7. Documentation: a worked example in `docs/window-nodes.md` (or appended to `docs/atomic-keys.md`) showing the OHLC composition (4 expressions) and the empty-bucket semantics.

## Constraints

- **Performance:** the per-tick path through `TumblingWindow.onValue` should match `Worker.onValue` within margin (single mutex-protected vector append). The per-boundary emit (vector copy + reset) is amortized; allocations there are acceptable since they fire once per `periodMs`, not once per tick.
- **Compatibility:** no wire-protocol changes. New node types are additive in the `TreeBuilder` JSON; existing pipelines continue to build and run unchanged. `treaty` is untouched.
- **Dependencies:** none — internal-only change to krn. Embassy ferrying of `pipeline_json` already supports any node `TreeBuilder` builds; no embassy or forum work here.
- **Deadline:** none. Unblocks the saved-chart-slice follow-up but is not on its critical path (running OHLC works for the demo today).

## Affected systems / callers

- `krn/include/krn/nodes/TumblingWindow.hpp` + `krn/src/nodes/TumblingWindow.cpp` — new.
- `krn/include/krn/nodes/VectorReducer.hpp` + `krn/src/nodes/VectorReducer.cpp` — new.
- `krn/src/core/NodeRegistry.cpp::registerBuiltinNodeTypes` (or `src/core/TreeBuilder.cpp`'s builder-registration block — confirm at implementation time) — two new `NodeTypeRegistry::registerNodeType` entries.
- `krn/src/core/FunctionRegistry.cpp` — add `first` / `last` if they aren't already registered; no other changes.
- `krn/tests/nodes/TumblingWindowTest.cpp` + `tests/nodes/VectorReducerTest.cpp` — new.
- `krn/tests/test_bootstrap.cpp` — verify the new registry installations land in the global gtest Environment.
- `krn/docs/window-nodes.md` (or addendum to `docs/atomic-keys.md`) — new doc.

## Alternatives considered

- **(A) Single `Window(periodMs, fn)` node** — accumulate + reduce in one node. Rejected: bakes the reduce into the windowing, can't reuse the raw bucket for downstream consumers (percentile chart, histogram, raw-vector responder). One node is simpler at the cost of composition.
- **(C) Extend `Worker` with a `boundary` input channel** — paired `BucketTime` fires a "reduce-and-clear" signal. Rejected: expands `Worker`'s contract beyond "reduce my accumulator" and couples it to timer semantics. Existing `Worker` users would need re-validation. Least modular of the three.
- **(B) "TumblingWindow + existing Worker(fn)"** (the original ticket framing) — rejected on closer read of `Worker`: it accumulates incoming values and reduces over the accumulator, so feeding it one `vector<double>` per boundary would treat the vector as one element of its own accumulator, not as the bucket to reduce. Composition only works with a vector-shaped reducer (i.e., `VectorReducer`).
- **Separate `VectorFnMap` registry** — initially considered; rejected during /plan after re-reading `FunctionMap`. The registry's stored `Func` is already `double(const vector<double>&)` — exactly what `VectorReducer` needs. Worker's variant-typed `Fn` is a `TreeBuilder.cpp:fnFromName` adapter, not the registry's native shape. One registry, one source of truth.
- **Compose `TumblingWindow` with an external `BucketTime` via a control-signal input** — would let `BucketTime` own all wall-clock logic. Rejected: requires a multi-input pattern beyond what `INode` / `TreeBuilder` support today (Aggregate's parallel-arrival pattern doesn't fit "value stream + control signal"); larger blast radius than self-contained timer in `TumblingWindow`.

## Risks

- **Timer drift across many `TumblingWindow` instances.** Each instance owns a timer thread. With dozens of subscriptions, that's dozens of threads. *Mitigation:* alignment to wall clock keeps boundaries coincident across instances (no jitter accumulation); the thread-per-window cost mirrors `BucketTime`'s existing pattern, so it's a known shape — but worth measuring once with N=100 instances.
- **Empty-bucket no-emit makes downstream consumers see "stuck" bars in a UI.** A chart that expects one frame per minute will visually freeze during quiet markets. *Mitigation:* documented behavior; a future "EmitOnEmpty" mode is a one-line config addition if charts need it.
- **The vector copy on emit is the hot allocation.** Per-boundary `vector<double>` copy + clear churns the buffer's heap allocation. *Mitigation:* reuse the underlying vector via `std::vector::clear()` (keeps capacity); only the *emitted* `StreamValue.value` carries a fresh vector via move. Steady-state per-tick path stays alloc-free.
- **Concurrent `onValue` from `Dispatcher` worker threads vs. timer-thread emit.** Standard mutex protects the buffer; needs care to not hold the mutex while calling `downstream_->onValue()` to avoid deadlocks if downstream re-enters. *Mitigation:* swap-out-then-emit pattern (move buffer to local, release mutex, then emit downstream).

## Open questions

- Whether `TumblingWindow`'s timer should share the engine `ThreadPool` or own a dedicated `std::thread` like `BucketTime` does. Resolved at implementation time by mirroring `BucketTime` exactly unless profiling says otherwise — the proposal's "minimal blast radius" preference favors the existing pattern.
