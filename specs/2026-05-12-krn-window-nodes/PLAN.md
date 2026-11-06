# PLAN: KRN windowing primitives — TumblingWindow + VectorReducer

**Spec:** [SPEC.md](./SPEC.md)
**Date:** 2026-05-12
**Status:** Draft

## Overview

Two phases — one per node — each shippable on its own. Phase 1 lands `TumblingWindow` (the new primitive: collect a scalar stream, emit `vector<double>` per wall-clock boundary). It is independently useful: any downstream node that handles a vector-typed `StreamValue` can consume it (e.g., a future histogram node, or a `Responder` that ships the raw bucket to the browser). Phase 2 lands `VectorReducer` and the worked composition (`VectorReducer ← TumblingWindow ← Listener`), proving end-to-end that minute-OHLC inputs are now expressible. Each phase ends with `ctest --output-on-failure` green and `mage Bench` showing no regression on the existing hot path.

`registerBuiltinNodeTypes()` lives in `src/core/TreeBuilder.cpp:319` (not a separate `NodeRegistry.cpp` despite the header naming); `BuiltinFunctions.cpp` already registers `sum`/`mean`/`max`/`min`/`first`/`last` so no new function-registry work is needed. Engine sources are picked up via `GLOB_RECURSE` in the root `CMakeLists.txt:113`, so new `.cpp` files don't need explicit CMake edits — but `tests/nodes/CMakeLists.txt` lists test files explicitly and *does* need a one-line addition each phase (used by the alternate non-glob test build).

## Phase 1: TumblingWindow node

**Goal:** A `TumblingWindow(periodMs, input)` node ships and a `ctest` suite proves: it accumulates per `(streamKey, field)`, emits one `StreamValue{value=vector<double>}` per wall-clock boundary, swallows empty buckets, and shuts down its timer cleanly.

**Tasks:**

1. **`include/krn/nodes/TumblingWindow.hpp`** — class declaration mirroring `BucketTime`'s shape: `final` class extending `INode` and `enable_shared_from_this<TumblingWindow>`. Members: `period_`, `pool_`, `downstream_` (single `shared_ptr<INode>`), per-symbol buffer `unordered_map<string, vector<double>>`, `mx_` mutex, `cv_`, `stopping_`/`started_` atomics, `timerThread_`. Public surface: ctor `(period, downstream, pool)`, `start()`, `onValue(StreamValue)` override (the upstream tap), `shutdown() noexcept` override, public-static `nextAlignedAfter` reused from `BucketTime` (or a copy if cross-include is awkward).

2. **`src/nodes/TumblingWindow.cpp`** — implement:
   - `onValue`: under `mx_`, `acc_[sv.symbol].push_back(toDouble(sv.value))`. Use the same `MAX_SYMBOLS` cap as `Worker.cpp:20-25` to refuse unbounded growth.
   - `timerLoop` (mirrors `BucketTime::timerLoop` lines 53-86): `wait_until` the next aligned boundary; on wake, swap-out each symbol's buffer (move-out, leave empty vector with capacity per the spec's risk mitigation), release the lock, then dispatch one `StreamValue{symbol, vector<double>}` per non-empty bucket via `pool_->post([ds, sv]{ ds->onValue(sv); })`. Empty buckets: skip — no emit (per AC-3).
   - `shutdown` / dtor: identical pattern to `BucketTime::shutdown` (set `stopping_`, `cv_.notify_all`, join-or-detach the timer thread).

3. **Register in `TreeBuilder.cpp::registerBuiltinNodeTypes`** — add a `NodeTypeRegistry::registerNodeType("TumblingWindow", builder)` block alongside the existing `BucketTime` registration at line ~374. Builder reads `periodMs` (uint), the recursive `input` child via the existing helper, captures the engine `ThreadPool*`, constructs the node, calls `start()`. Reject `periodMs <= 0` with a clear `runtime_error` (`TreeBuilder` already handles it as a build-time error frame).

4. **`tests/nodes/TumblingWindowTest.cpp`** — five tests, all using a `RecorderNode` (a tiny `INode` impl that captures incoming `StreamValue`s into a thread-safe vector — pattern mirrors `tests/nodes/IntervalTest.cpp` if it has one, otherwise inline):
   - `EmitsOnceAtBoundaryWithAccumulatedValues` — period=100ms, push 5 values within a window, sleep through one boundary, assert recorder got exactly 1 frame and its `value` is `vector<double>{1,2,3,4,5}`.
   - `EmptyBucketDoesNotEmit` — period=100ms, no upstream, sleep through 5 boundaries, assert recorder count == 0.
   - `MultipleSymbolsBucketIndependently` — push values for symbols A and B in same window, assert recorder gets 2 frames per boundary, partitioned by symbol.
   - `ConcurrentOnValueDoesNotLoseUpdates` — 8 threads hammer `onValue` with N=1000 each, sleep through one boundary, assert sum of vector lengths == 8000.
   - `ShutdownJoinsTimerThread` — construct + start + shutdown 100× in a loop; relies on the test binary's `tearDown` not seeing thread leaks (a `ThreadSanitizer` build catches it; otherwise just verifies no hang).

5. **`tests/nodes/CMakeLists.txt`** — append `TumblingWindowTest.cpp` to the explicit list at line ~3 (the root-level `GLOB_RECURSE` would also pick it up, but the per-subdir explicit list is what the alternate test build uses).

6. **`docs/window-nodes.md` (stub)** — create the file with the JSON shape for `TumblingWindow` alone (one paragraph + one example). Phase 2 expands it with the full composition and the OHLC worked example.

**Dependencies:** none — additive change to the engine; `BucketTime`, `ThreadPool`, `INode`, `StreamValue` all exist as-is.

**Verification:**
```sh
cd krn && bash tools/compile.sh build-debug   # configures + builds Debug w/ KRN_BUILD_TESTS=ON
cd build-debug
./krn_tests --gtest_filter='TumblingWindowTest.*' --gtest_brief=1   # 5 tests pass
ctest --output-on-failure                                            # full suite green (regression gate)
```
Note: the test suite is a single CTest target (`krn_tests`) wrapping all
GoogleTest cases — `ctest -R` filters CTest names, not GoogleTest names,
so filter at the GoogleTest layer via `--gtest_filter`.

**Risks:**
- **Per-symbol buffer growth in pathological inputs.** A bad upstream that emits millions of unique symbols would balloon the `acc_` map. *Mitigation:* reuse `Worker.cpp:20-25`'s `MAX_SYMBOLS=10000` cap with the same warn-and-drop log. Same constant, same intent.
- **Lock held while calling `downstream_->onValue` would deadlock if downstream re-enters.** *Mitigation:* per the spec's risks section, move the buffer to a local `vector<double>` under the lock, release the lock, then call downstream. Standard pattern; same shape `BucketTime::timerLoop` uses (it constructs the `StreamValue` after releasing).
- **Test wall-clock dependency makes CI flaky.** Sleeping through a 100ms boundary on a busy CI runner can drift. *Mitigation:* allow `period=50ms`, sleep `period * 3`, assert "≥ 2 boundaries observed" rather than exact count for the timing-sensitive tests; explicit count only for the deterministic value-content tests.

**Rollback:** revert the phase's PR. The new node is only reached via JSON configs that name it — no existing pipeline depends on it, so removal is safe.

---

## Phase 2: VectorReducer + composition + docs

**Goal:** `VectorReducer(fn, input)` ships, the composed `VectorReducer ← TumblingWindow ← Listener` JSON resolves and runs end-to-end, `mage Bench` confirms no per-tick alloc regression on the hot path.

**Tasks:**

1. **`include/krn/nodes/VectorReducer.hpp`** — declare a small `final` class extending `INode`. Members: `fn_` (`krn::Func` from `FunctionMap.hpp:10` — already `double(const vector<double>&)`), `downstream_` (`shared_ptr<INode>`), `stopping_` atomic, `mx_` (only for `downstream_` swap on shutdown). Ctor `(Func, shared_ptr<INode>)`. Override `onValue` (extracts `vector<double>` from the variant `sv.value`, applies `fn_`, emits `StreamValue{symbol, double}`) and `shutdown() noexcept`.

2. **`src/nodes/VectorReducer.cpp`** — implement `onValue`: if `sv.value` doesn't hold `vector<double>`, log at `Warn` (`"VectorReducer: non-vector input dropped"`) and drop. Otherwise apply `fn_` to the held vector, construct `StreamValue{sv.symbol, ArgType{double}}`, snapshot `downstream_` under the lock, release, call downstream. Catches reducer exceptions same way `Worker.cpp:33-41` does (log `Error`, drop the value, leave state consistent).

3. **Register in `TreeBuilder.cpp::registerBuiltinNodeTypes`** — add `NodeTypeRegistry::registerNodeType("VectorReducer", builder)`. Builder reads `fn` string, looks up via `FunctionMap::instance().getFunction(fn)` (throws if unknown — matches `Worker`'s build-time behavior at `TreeBuilder.cpp:148-158`), reads recursive `input`, returns `make_shared<VectorReducer>(std::move(fn), input)`.

4. **`tests/nodes/VectorReducerTest.cpp`** — four tests using the same `RecorderNode` from Phase 1 (move it to a shared `tests/nodes/RecorderNode.hpp` if not already extracted):
   - `MaxOverVectorEmitsScalar` — feed `vector<double>{1, 5, 3}`, fn=`max`, assert recorder got `5.0`.
   - `SumOverVectorEmitsScalar` — same shape, fn=`sum`, assert recorder got `9.0`.
   - `NonVectorInputDropped` — feed `StreamValue{value=int{42}}`, assert recorder got 0 frames.
   - `UnknownFnRejectedAtBuildTime` — call `fnFromName`-equivalent at builder time with `fn="bogus"`, assert it throws (the existing Worker test pattern at `WorkerTest.cpp` likely has a model).

5. **Integration test `tests/nodes/WindowReducerCompositionTest.cpp`** — wire the full composition: `Listener("X", "f") → TumblingWindow(periodMs=100) → VectorReducer("max")`, drive 10 values into the listener within a window, sleep `200ms`, assert the recorder downstream of `VectorReducer` got at least 1 frame whose value equals `max` of the inputs. Also assert `TreeBuilder::buildFromJSON` accepts the equivalent JSON config for the same shape.

6. **`tests/nodes/CMakeLists.txt`** — append `VectorReducerTest.cpp` and `WindowReducerCompositionTest.cpp` to the explicit list.

7. **`docs/window-nodes.md` (full)** — flesh out from Phase 1's stub: JSON shape for both nodes, the OHLC worked example (4 expressions for high/low/open/close), the empty-bucket no-emit semantics, the alignment guarantee inherited from `BucketTime`, and a one-line "follow-up: saved-chart-slice seed switch" pointer.

8. **Bench gate** — run `cd build && ctest -R Bench` (or `mage Bench` if the existing target wraps it) and capture `BenchmarkOrchestratorRouteValue`-equivalent numbers (or whichever KRN bench exists for the per-tick path) before and after; assert no regression. The new nodes don't sit on the per-tick fast path of *existing* pipelines, so this should be a no-op verification — included to defend against accidental coupling.

**Dependencies:** Phase 1 merged.

**Verification:**
```sh
cd krn/build-debug
./krn_tests --gtest_filter='VectorReducerTest.*:WindowReducerCompositionTest.*' --gtest_brief=1   # 6 tests pass
ctest --output-on-failure                                                                          # full suite green

# Benches: configure a Release tree with -DKRN_BUILD_BENCHMARKS=ON and
# run each microbench by name (the combined run can hang with multiple
# TumblingWindow instances + ThreadPool teardowns in one process).
cd ../build-bench
./bench_dispatcher    --benchmark_filter=BM_DispatcherOnTick       --benchmark_min_time=2s
./bench_window_nodes  --benchmark_filter=BM_VectorReducer_Max      --benchmark_min_time=1s
./bench_window_nodes  --benchmark_filter=BM_VectorReducer_Sum      --benchmark_min_time=1s
./bench_window_nodes  --benchmark_filter=BM_TumblingWindow         --benchmark_min_time=1s
```
Note: the gtest suite is a single CTest target (`krn_tests`) wrapping all
GoogleTest cases — `ctest -R` filters CTest names, not GoogleTest names,
so filter at the GoogleTest layer via `--gtest_filter` (same caveat as
Phase 1).

**Risks:**
- **`StreamValue::value` carrying a non-vector variant silently does nothing.** Could mask a wiring bug if a user mis-pairs `VectorReducer` with a non-vector source. *Mitigation:* the `Warn` log on the drop path, plus a one-line note in `docs/window-nodes.md` about the expected upstream shape. A future stricter `TreeBuilder` could validate type compat at build time, but that's out of scope here.
- **The integration test's wall-clock sleep is the same flakiness vector as Phase 1.** *Mitigation:* same approach — generous deadline (200ms for one boundary at period=100ms), assert "at least 1 frame" rather than exact count.
- **`FunctionMap::getFunction` throws on unknown name; `TreeBuilder`'s builder must catch and translate to a `runtime_error`.** *Mitigation:* mirror `TreeBuilder.cpp:156-158`'s `try { ... } catch (...) { throw runtime_error(...) }` shape exactly — `WorkerTest` already covers the "unknown fn" build-time-error path; the `UnknownFnRejectedAtBuildTime` test from task 4 above pins the same for `VectorReducer`.

**Rollback:** revert the Phase 2 PR. `TumblingWindow` from Phase 1 stays — it's still useful for a downstream consumer of `vector<double>`. The composition simply isn't expressible until a follow-up reintroduces `VectorReducer`.

---

## Cross-cutting concerns

- **Migrations:** none. No schema, no wire-protocol, no config-file changes. `krn.conf` is untouched.
- **Observability:** the existing `krn::util::logger` is used for the two new warn/error paths (per-symbol cap, non-vector input, reducer exception). No new metrics — both nodes sit off the smoke's measured surface.
- **Docs:** `docs/window-nodes.md` (new). `CLAUDE.md` Code Conventions section gets a one-liner on the new node types only if Phase 2 surfaces a convention worth pinning (e.g., the empty-bucket no-emit choice) — defer to /verify time.

## Out of plan

- **Saved-chart-slice seed switch.** Forum's `db/seed/seed.go:candlesV1NEXOSavedScene` continues to use bare atomic keys. Switching to `pipeline_json` subscriptions that drive `VectorReducer ← TumblingWindow ← Listener` for real per-minute candles is a follow-up proposal — needs a live-stack verification surface (browser shows real per-minute candles painting) and touches forum + likely customer-layer.
- **Catalog/editor surfacing in `pipelines-panel-real`.** The web editor's `gma-catalog.ts` doesn't know about the new node kinds. Adding them needs catalog + editor + storage + adapter changes in lockstep — its own ticket per the pipelines-panel-real PLAN's "out of plan" notes.
- **`EmitOnEmpty` config.** A future flag for charts that want a frame per boundary even on quiet markets. One-line addition once a real consumer asks.
- **Type-compat validation in `TreeBuilder`.** Catching `VectorReducer ← Listener` (non-vector source) at build time would prevent the runtime drop. Out of scope; covered by the `Warn` log for now.
