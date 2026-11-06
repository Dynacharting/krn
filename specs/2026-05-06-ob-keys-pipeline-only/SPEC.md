# SPEC: enshrine `ob.*` as pipeline-only (not Listener-subscribable)

**Slug:** ob-keys-pipeline-only
**Date:** 2026-05-06
**Status:** Draft
**Author:** ndrandal
**Repos:** `krn` (docs + nodes/Listener), `customer-layer` (apps/web TS types), `forum` (db/seed reshape)
**Linear:** ENC-101

## Problem

A `Listener` node bound to an `ob.*` atomic key (`ob.best.bid.price`,
`ob.spread`, etc.) registers with the Dispatcher but receives **zero
updates**. The PoC validating ENC-94/ENC-99 confirmed this against
feed-sim: 15s of subscription to `ob.best.bid.price` /
`ob.best.ask.price` / `ob.spread` for NEXO + VALT yielded 0 events,
while bare keys (`lastPrice`, `sma_5`) on the same connection fired
normally (170 events). The cause is architectural: only
`connectors/market/src/MarketTA.cpp:400` calls
`Dispatcher::notifyListeners` — `connectors/market/src/ob/ObProvider.cpp`
writes the AtomicStore on book updates but never notifies. The
asymmetry is real and intentional (the OB namespace exists to
express continuously-evolving derived state, not feed events) but
nothing in the system tells a user, an AI-generated pipeline, or
ENC-94's own `docs/atomic-keys.md` that the Listener path is
unavailable for `ob.*`. The result is silent breakage. The seed
pipeline added in mvp-deploy-prep Phase 2 (`forum/db/seed/seed.go:293`,
NEXO bid Listener) is itself an instance of the bug.

## Proposed change

Enshrine `ob.*` as **pipeline-only**: the canonical way to surface
top-of-book values in a chart is `Listener(<bare-key clock>) →
AtomicAccessor(ob.*)` inside a TreeBuilder spec. Make the
asymmetry explicit at every layer that can produce a broken pipeline:

- Documentation that names the rule and shows the canonical pattern.
- A construction-time reject in `krn::nodes::Listener` for any field
  in the `ob.*` namespace, with a clear error message that points
  the developer at the AtomicAccessor pattern.
- A TypeScript-side type narrowing on
  `customer-layer/apps/web/src/types/pipeline-graph.ts` so a
  Listener's `field` cannot be an `ob.*` key at compile time.
- Reshape the seeded NEXO bid pipeline to use the canonical pattern.

This is the lowest-blast-radius option (compared to making
`ObProvider` push events on every book update, which would amplify
message rates and blur the architectural split between feed-event
push and order-book-derived state).

## Scope

- **`krn/docs/atomic-keys.md`** — replace ENC-94's
  "subscribe to these for L2 sources" framing for `ob.*` with the
  push-vs-pull asymmetry: bare keys are push-fed via MarketTA,
  `ob.*` is pull-only via AtomicStore. Document the canonical
  Listener-clock + AtomicAccessor pattern with a worked example
  (the same NEXO/VALT case the PoC ran).
- **`krn/include/krn/nodes/Listener.hpp`** + **`src/nodes/Listener.cpp`** —
  `Listener` constructor (or `start()`) returns / throws / refuses
  registration when `field_` matches the `ob.*` namespace. Surface
  via a `Result`-shaped factory (matches the rest of the codebase's
  `Result.hpp` pattern; see `include/krn/Result.hpp`). The error
  message names the field, points at `docs/atomic-keys.md`, and
  suggests the AtomicAccessor alternative.
- **`krn/src/core/TreeBuilder.cpp`** — wire the new factory so
  a graph that names `ob.*` on a Listener fails to build with the
  same error, before `start()`.
- **`customer-layer/apps/web/src/types/pipeline-graph.ts`** —
  narrow `ListenerNode.config.field` to a non-`ob.*` subset of
  `AtomicKey`. Add a runtime type guard helper
  `isListenerSubscribable(field: AtomicKey): boolean` for callers
  that build pipelines from runtime data (e.g., AI-generated graphs).
- **`forum/db/seed/seed.go`** — reshape the NEXO bid pipeline
  (currently `Listener(ob.best.bid.price)` → AtomicAccessor →
  Responder) to the canonical pattern: `Listener(lastPrice on NEXO)
  → AtomicAccessor(ob.best.bid.price) → Worker(spread fn) →
  Responder`. Update the unit test counts.

## Non-goals

- **No C++ change to `ObProvider` to push events.** The push path
  intentionally carries feed events, not derived order-book state;
  pushing every book update would amplify message rates beyond the
  Dispatcher's design budget and require throttling logic that
  doesn't exist today.
- **No new protocol shape for Interval-based polling on `ob.*`.**
  The canonical pattern (Listener on a high-frequency bare key
  feeding AtomicAccessor) gives the same effective semantics
  without expanding the cloudchannel protocol.
- **No changes to forum's classifier / orchestrator prompt.**
  Post-Phase-6-cutover the classifier is rule-based
  (`forum/internal/orchestrator/classifier.go`), and aibox-mock
  returns canned plans; there is no live LLM prompt today that
  could mis-generate a Listener-on-`ob.*`. When real AI boxes
  replace aibox-mock, the prompt update lands as part of that
  migration, informed by the doc this proposal produces.
- **No krn perf changes.** `ObProvider` stays pull-only and the
  `Listener` reject is a single string-prefix check at construct
  time, so nothing on a per-tick path moves. Nothing in this repo
  measures that today — see **Corrections C1**.
  ~~Hot-path benchmarks (`PackAppend` 0.85 ns/op / 0 allocs;
  `OrchestratorThroughput` ≥40M ops/sec) must remain unchanged.~~
  — **struck 2026-09-19 (ENC-1269): both are *embassy* Go
  benchmarks, not krn ones, and both figures are
  stub-broadcaster numbers. C1.**
- **No backward-compat shim.** No production deployment has a
  Listener on `ob.*` that fires today (it never did); making the
  failure explicit at construct time doesn't break any working
  user pipeline.

## Acceptance criteria

1. **Docs**: `krn/docs/atomic-keys.md` contains a section titled
   "push vs pull" that names the asymmetry, lists the canonical
   pattern, and includes the NEXO/VALT worked example. The misleading
   "subscribe to these for L2 sources" wording is gone.
2. **Listener construct rejects ob.\***: a unit test
   (`src/nodes/Listener_test.cpp` or wherever the existing
   pattern places it) builds a Listener with `field="ob.spread"`
   and asserts the factory returns an error containing the string
   `pipeline-only` and the field name.
3. **TreeBuilder rejects ob.\* listener** at graph-build time
   when a JSON spec names `kind=listener` with `field=ob.best.bid.price`;
   error surfaces in `mage Test` (krn ws tests) without the
   graph reaching `start()`.
4. **TypeScript compile-time guard**: `pnpm --filter
   @customer-layer/web build` fails if a developer assigns
   `{ kind: "listener", config: { field: "ob.best.bid.price", ... } }`
   to a `ListenerNode`. (i.e., `ListenerNode["config"]["field"]`
   excludes `ob.*` template-literal types).
5. **Seed reshape**: `forum/db/seed/seed.go`'s NEXO bid pipeline
   no longer references `ob.*` as a Listener field.
   `TestRunIsIdempotent` and `TestSeededAdminCanLoginViaSymmetricSecret`
   continue to pass; new pipeline graph nodes parse cleanly through
   the existing JSON marshalling.
6. **End-to-end PoC re-run**: against feed-sim, a TreeBuilder spec
   with the canonical pattern (`Listener(NEXO.lastPrice) →
   AtomicAccessor(NEXO.ob.best.bid.price) → Responder`) emits
   non-zero updates within 15s — the same window where ENC-101's
   original repro saw zero. Documented in
   `krn/docs/atomic-keys.md` as a self-contained `apps/poc-client`
   command.
7. ~~**No bench regression**: `mage Bench` on embassy and
   `mage Bench` on krn (if present) report unchanged numbers
   for `PackAppend` / hot-path benchmarks (within ±2%).~~
   — **struck 2026-09-19 (ENC-1269): unrunnable as written.
   krn has no `mage` target and no `PackAppend`; `PackAppend`
   is embassy's. ±2% is also below the run-to-run noise of a
   sub-nanosecond Go microbenchmark. See Corrections C1.**

## Constraints

- **Performance:** No changes to krn hot path; ObProvider stays
  pull-only. Listener construct gains one string-prefix check
  (sub-microsecond).
- **Compatibility:** Listener-on-`ob.*` was always silently broken;
  making it an explicit construct-time error is strictly an
  improvement, not a wire-format or API break. No proto / treaty
  changes.
- **Dependencies:** None. Self-contained within krn +
  customer-layer types + forum seed.
- **Deadline:** None declared. Useful as a follow-up to
  `mvp-deploy-prep` (closes the open assumption in ENC-94's
  `atomic-keys.md`); no calendar pressure.

## Affected systems / callers

- **`krn/include/krn/nodes/Listener.hpp` +
  `src/nodes/Listener.cpp`** — factory or `start()` gains the
  reject path.
- **`krn/src/core/TreeBuilder.cpp`** — surfaces the reject
  during graph build.
- **`krn/docs/atomic-keys.md`** — sharpens ENC-94's doc.
- **`krn/CLAUDE.md`** — one-line cross-reference under "Code
  Conventions" (the document already lists ENC-94's pointer; this
  proposal extends the wording).
- **`customer-layer/apps/web/src/types/pipeline-graph.ts`** —
  narrows `ListenerNode.config.field` and exports
  `isListenerSubscribable`.
- **`forum/db/seed/seed.go`** — reshapes the NEXO bid pipeline.
- **`forum/db/seed/seed_test.go`** — `TestRunIsIdempotent` count
  assertions stay; pipeline structure assertions update to
  reflect the new node count (Listener + AtomicAccessor + Worker
  + Responder = 4 nodes vs current 2).

No `treaty/` changes. No `embassy/` changes (data plane / KRN
client are unaffected).

## Alternatives considered

- **(a) ObProvider emits `Dispatcher::notifyListeners` on book
  updates.** Rejected: order-book updates run at much higher
  cadence than trade events; pushing them would either flood the
  dispatcher (no upstream throttle) or require a new throttle layer
  that doesn't exist today. The architectural split between feed
  events (push) and derived state (pull-only) is intentional and
  documented in ENC-94; option (a) erodes that without a clear
  user-visible win, since the canonical Listener+AtomicAccessor
  pattern gives the same chart-paint behavior at the
  user's chosen clock cadence.
- **(c) New cloudchannel-protocol Interval-based listener for
  `ob.*`.** Rejected: it's strictly a subset of what the existing
  `Interval → AtomicAccessor` pipeline composition already
  expresses. Adding it as a first-class wire shape costs treaty
  + forum + embassy + krn touchpoints to reproduce something
  callers can build today.
- **Status-quo + AI-prompt-only fix.** Rejected: makes the
  failure mode "AI-generated pipelines work, hand-rolled graphs
  silently break". The construct-time reject in `Listener`
  closes the silent path uniformly.

## Risks

- **TreeBuilder error path is poorly surfaced today.** If
  TreeBuilder swallows the new `Result` instead of propagating it
  to the WS client, the user gets a `502` with no detail.
  Mitigation: AC #3 gates the spec on `mage Test` exercising the
  WS error path; if the existing `ClientSession.cpp` catch-all
  drops it, this proposal includes the small surfacing change to
  bubble the message up via `sendError("subscribe", msg)` (the
  same shape as
  `src/server/ClientSession.cpp:355`).
- **TypeScript template-literal exclusion subtly fails for
  `(string & {})`.** Existing `AtomicKey` includes
  `(string & {})` as an escape hatch — a `ListenerNode.config.field`
  typed as the same will accept any string at compile time,
  defeating AC #4. Mitigation: define the listener-subscribable
  type as the explicit non-`ob.*` literal union (drops the
  `(string & {})` escape), forcing call sites that genuinely need
  a runtime-only key to call `isListenerSubscribable` first.
- **Seed reshape changes counts.** `TestRunIsIdempotent` checks
  per-table counts; reshape adds two nodes to the NEXO pipeline.
  Mitigation: the test counts row-counts, not pipeline-node-counts
  (nodes are stored as a JSON column, not a separate table); no
  test should break. Verify in AC #5.

## Open questions

- None at spec time.

---

## Corrections

Struck in place and dated, never deleted — the house convention
(`DynaCharting/LIMITATIONS.md` §C, `HANDOFF-2026-09-16.md`), and the rule
`specs/2026-09-14-krn-flow-control-replay/recheck.sh` is written against
(*"the row struck through — D9: nothing is deleted"*). A reader who
half-remembers the wrong version needs to find the correction, not a silence.

### C1 — this SPEC gated on two embassy Go benchmarks, presented as krn ones

**Struck 2026-09-19 under ENC-1269**, from the ENC-1263 performance-claim
audit (`workspace:specs/2026-09-19-chart-quality-bar/PERF-CLAIMS.md` row B6).
Two places were affected: the *Non-goals* bullet above and acceptance
criterion **#7**.

**What was claimed.** *"Hot-path benchmarks (`PackAppend` 0.85 ns/op / 0
allocs; `OrchestratorThroughput` ≥40M ops/sec) must remain unchanged"*, and
AC#7's ±2% restatement of it.

**Neither benchmark is in this repo.** `BenchmarkPackAppend` is
`embassy/internal/pipeline/binary_test.go:46`; `BenchmarkOrchestratorThroughput`
is `embassy/bench/throughput_bench_test.go:28`. Both are Go. krn is C++20 and
has no Go toolchain, no `mage` target, and no function of either name — so no
krn build, test or CI step ever evaluated this clause. It sat unchallenged
from 2026-05-06.

**Where the numbers came from.** `customer-layer/specs/2026-05-02-go-backend-rewrite/DECISIONS.md:318-322`
(ADR-014, dated 2026-05-03), a five-row table of `embassy` per-op costs
measured against *"current main"* — no machine, no Go version, no `-count`,
no commit. `0.85` is its `BenchmarkPackAppend` row verbatim. `≥40M ops/sec`
is its `BenchmarkOrchestratorThroughput` row (21.6 ns/op → *"≈46M /s"*)
rounded down into a floor. This SPEC is the only other committed home of
either figure.

**What retracted them.**

- `embassy/CLAUDE.md:134-139` — `BenchmarkBroadcastFanout` with **real
  WebSocket clients** attached, 256-byte records: *0 conns 67.1 ns/op 0 allocs;
  1 conn 250.7 ns/op 289 B 1 alloc; **8 conns 2,277 ns/op, 2,458 B, 13
  allocs***. It names the benchmark, the payload, the connection count and
  what it supersedes, and is the maintained embassy number. PERF-CLAIMS B1
  rules it **STANDS** and calls it the house standard for a CPU claim.
- `BenchmarkOrchestratorThroughput` routes into `noopBroadcaster`
  (`embassy/bench/throughput_bench_test.go:13-21`), which discards every frame.
  Its implied rate is an upper bound on **packing + routing in isolation**, and
  says nothing about the data plane: the shipped 8-connection path measures
  ≈0.44M ops/sec, ≈91× below the ≥40M figure this SPEC froze as a system
  constraint. The same benchmark's own doc comment targets *"≥ 100k
  records/sec"* — 400× below what was copied here.
- `workspace:specs/2026-09-14-local-data-plane/SPEC.md` **D2** (ENC-1012,
  2026-08-24) already rules that a stub-broadcaster figure *"must not be cited
  as the data-plane number"*. `noopBroadcaster` is the same construction as the
  `nullBroadcaster` D2 names.

**One thing PERF-CLAIMS B6 and ENC-1269 both got wrong, corrected here.** They
state that `PackAppend` *"has two committed values (0.85 and 2.463 ns/op)"*.
It does not. `2.463 ns/op` is `BenchmarkOrchestratorRouteValue`, not
`PackAppend` — `embassy/CLAUDE.md:138` says so explicitly, and
`encultured-decisions-prompt.md:105-106` is where it is (mis)certified as
*"verified"*. `PackAppend`'s `0.85` has exactly one source, ADR-014, quoted in
exactly two committed places: ADR-014 itself and this SPEC. The defect is that
it carries no conditions, not that it disagrees with itself.

**Not re-measured, deliberately.** A krn design record must not carry an
embassy benchmark number at all — re-measuring would reproduce the
misattribution with fresher digits. The number that belongs here is a **krn**
one, and there is none: `KRN_BUILD_BENCHMARKS` is `OFF` by default
(`CMakeLists.txt:6`), the five suites under `benchmarks/` get no `add_test`
registration, and nothing in CI runs them — so *"no krn perf changes"* is
today an assertion about the diff, not a measurement. **ENC-997** (*"[krn]
Commit a real performance baseline"*) owns closing that with a `BASELINE.md`
and a perf job; this correction discharges only its *"false numbers deleted or
corrected"* clause.

**Same defect, noted but not changed:** AC#3 gates on *"`mage Test` (krn ws
tests)"*. There is no `mage` in this repo either — tests are
`cmake -B build -DKRN_BUILD_TESTS=ON && ctest --test-dir build`. It is not a
performance claim, so it is left standing and recorded here; it belongs to
ENC-997.

**Re-check:**

```bash
# the two figures are struck, not merely gone
grep -n '~~' specs/2026-05-06-ob-keys-pipeline-only/SPEC.md
# neither benchmark exists in this repo, in any language
grep -rn 'PackAppend\|OrchestratorThroughput' --include='*.cpp' --include='*.hpp' \
     --include='*.cc' --include='*.h' . ; echo "exit=$?  (1 = correct: no hits)"
# the maintained embassy number
sed -n '134,139p' ../embassy/CLAUDE.md
```
