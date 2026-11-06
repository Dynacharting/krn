# PLAN: enshrine `ob.*` as pipeline-only

**Spec:** [SPEC.md](./SPEC.md)
**Date:** 2026-05-06
**Status:** Draft

## Overview

Three phases, sliced by which repo owns the work and what's runnable
independently. Phase 1 lands the engine-level enforcement in krn
(Listener factory + TreeBuilder surfacing + docs); it's
self-contained and shippable because no live caller depends on
Listener-on-`ob.*` working today (it never did — that's the bug).
Phase 2 lands the caller-side guards (customer-layer TS narrow,
forum seed reshape) so AI-generated and seeded pipelines respect
the rule. Phase 3 is the end-to-end PoC re-run against feed-sim
that closes AC #6 with a real-world non-zero-update measurement.

Phases 1 and 2 can land in either order from a repo-isolation
standpoint, but Phase 1 first is preferred so the doc the seed
reshape references is already merged.

---

## Phase 1: krn — push-vs-pull asymmetry made loud

**Goal:** A Listener bound to an `ob.*` field fails to construct,
with a clear error pointing at `docs/atomic-keys.md`; the failure
surfaces as a `subscribe` error on the WS client; the doc itself
names the canonical pattern.

**Tasks:**

1. **`include/krn/nodes/Listener.hpp`** — add a static factory
   `Listener::Create(symbol, field, downstream, pool, dispatcher)
   -> krn::Result<std::shared_ptr<Listener>>`. Inside the factory,
   reject any `field` matching `ob.*` (literal prefix check; first
   3 chars `'o','b','.'`). Error message:
   `"listener: field '<field>' is pipeline-only — see
   docs/atomic-keys.md; bind via AtomicAccessor downstream of a
   bare-key Listener clock"`. Keep the public constructor
   accessible for non-rejecting tests but make it private-by-
   convention via doc comment; existing call sites move to the
   factory.
2. **`src/nodes/Listener.cpp`** — implement the factory. The body
   delegates to `std::make_shared<Listener>(...)` after the prefix
   check. Add a one-liner inline helper
   `static bool isPipelineOnlyKey(const std::string& field)`
   colocated in the .cpp (header doesn't need to expose it).
3. **`src/core/TreeBuilder.cpp`** — at lines 285-291 (the
   `buildForRequest`'s Listener construction), call
   `Listener::Create` instead of `make_shared<Listener>` and
   `head->start()`. On `Result::Error`, throw a
   `std::runtime_error` carrying the error message so the existing
   `try { TreeBuilder } catch { sendError("validate", ex.what()) }`
   chain at `ClientSession.cpp:456` propagates it. No
   `ClientSession.cpp` change should be required.
4. **`tests/nodes/ListenerTest.cpp`** — three new test cases:
   `RejectsObNamespaceAtFactory`, `AcceptsBareKeyAtFactory`,
   `RejectsObSpread` (covers 2-char `ob.` prefix exactly + a
   compound `ob.best.bid.price` case + a bare-key happy path). Each
   asserts the `Result` failure mode (or success) and that the
   error message contains the literal string `pipeline-only` and
   the offending field name.
5. **`tests/treebuilder/`** — add `ObListenerRejectTest.cpp` (or
   reuse an existing TreeBuilder test file). Build a request JSON
   with `{"streamKey":"NEXO","field":"ob.best.bid.price"}` and
   assert `buildForRequest` throws / returns an error matching
   `pipeline-only`. Expected: caught upstream by ClientSession's
   subscribe-validate path.
6. **`tests/ws/`** — extend the existing WS subscribe test (or add
   `ObSubscribeRejectTest.cpp` if no good fit) to send a subscribe
   request with `field=ob.spread`, assert the response is a
   `{"type":"error","where":"validate"|"subscribe", "message":...}`
   containing `pipeline-only`.
7. **`docs/atomic-keys.md`** — replace the "Decision rule" section's
   row pattern that implies `ob.*` is subscribable with the
   push-vs-pull rule, plus a new section "Canonical
   `ob.*`-in-a-chart pattern" with a worked NEXO example showing
   `Listener(NEXO.lastPrice) → AtomicAccessor(NEXO.ob.best.bid.price)
   → Worker(diff with bid+ask) → Responder`. Include the exact
   request JSON. Replace the old "Subscribe to these for L2 sources"
   wording for `ob.*` with "Use these via AtomicAccessor; never
   bind a Listener directly".
8. **`CLAUDE.md`** — extend the existing "Atomic-key namespaces —
   bare vs `ob.*` (ENC-94)" bullet with one sentence:
   `Listeners may bind only to bare keys; ob.* is pipeline-only
   (ENC-101). The construct-time reject in nodes/Listener::Create
   surfaces the rule as a subscribe-validate error.`

**Dependencies:** none — self-contained in krn.

**Verification:**

```bash
cd krn && cmake --build build -j --target krn_tests \
  && (cd build && ctest --output-on-failure -R 'ListenerTest|ObListenerReject|ObSubscribeReject')
```
- All three new test cases (`RejectsObNamespaceAtFactory`,
  `AcceptsBareKeyAtFactory`, `RejectsObSpread`) pass.
- The TreeBuilder + WS reject tests pass.
- `ctest --output-on-failure` overall green (no regressions).
- `cmake --build build --target krn_bench &&
  ./build/krn_bench --benchmark_filter='Hot|Pack'` reports
  unchanged numbers within ±2% of the pre-change baseline (capture
  baseline in a comment in the PR body).

**Risks:**

- **Existing constructor used in `TreeBuilder` and tests** has 2
  call sites today (`src/core/TreeBuilder.cpp:286` and the
  ListenerTest constructions). Switching only TreeBuilder leaves
  the test ctor path live. Mitigation: leave the public
  constructor; only TreeBuilder uses the new factory. Test code
  continues to use the constructor for the
  "ForwardsValueToDownstreamViaPool" / "ShutdownStopsPropagation"
  cases that need a Listener with arbitrary fields. The reject
  rule lives in the factory only — this is intentional, so test
  helpers can still build a Listener with `ob.*` to exercise the
  reject path without it triggering at runtime construction.
- **TreeBuilder catches and re-throws** — `buildForRequest` already
  throws `std::runtime_error` on validation; ClientSession's
  `try { TreeBuilder } catch (std::exception& ex)` at line 456
  surfaces via `sendError("validate", ex.what())`. Mitigation:
  AC #3 (Phase 1 task 5/6) verifies the WS round-trip; if the
  `try` happens to be narrower than expected, widen it in a
  one-line follow-up.
- **Prefix check false-positives.** `obesity` would not start with
  `ob.` (3-char prefix including the dot), so the literal
  3-character prefix check is safe. Mitigation: explicit
  3-character check (`'o','b','.'`) — not a `starts_with("ob")`.

**Rollback:** revert the merged PR. The Listener constructor path
is unchanged for non-`ob.*` keys; reverting reinstates the silent-
failure behavior but doesn't break any working pipeline.

---

## Phase 2: customer-layer + forum — caller-side enforcement

**Goal:** TypeScript code that names an `ob.*` key on a Listener
fails to compile; the seeded NEXO bid pipeline uses the canonical
pattern; existing seed tests still pass.

**Tasks:**

1. **`customer-layer/apps/web/src/types/pipeline-graph.ts`** —
   define a `ListenerSubscribableKey` type that excludes the
   `ob.*` template-literal pattern and the `(string & {})` escape:
   ```ts
   type ListenerSubscribableKey =
     Exclude<AtomicKey, `ob.${string}`> & string;
   ```
   Narrow `ListenerNode.config.field` from `string` to
   `ListenerSubscribableKey`. Export
   `isListenerSubscribable(field: string): field is ListenerSubscribableKey`
   helper that returns `!field.startsWith("ob.")`. Add a TSDoc
   comment pointing at krn's `docs/atomic-keys.md`.
2. **`customer-layer/apps/web/src/types/pipeline-graph.test.ts`**
   (new file or extend the existing types tests) — add
   `expectError`-style test that asserts `{ field: "ob.spread" }`
   on a `ListenerNode["config"]` is a TS compile error (use
   `// @ts-expect-error`). Three positive cases: `lastPrice`,
   `px.sma.5`, `bid` (bare keys still accepted).
3. **`forum/db/seed/seed.go`** — reshape the NEXO bid pipeline
   (currently lines 283-308):
   - `n1-listener` → `kind=listener, field=lastPrice` (clock).
   - `n2-accessor-bid` → `kind=atomicAccessor,
     symbol=NEXO, field=ob.best.bid.price`.
   - `n3-accessor-ask` → `kind=atomicAccessor,
     symbol=NEXO, field=ob.best.ask.price`.
   - `n4-aggregate` → `kind=aggregate, arity=2`.
   - `n5-worker-spread` → `kind=worker, fn=spread` (computes
     bid-ask diff).
   - `n6-responder` → `kind=responder, key=2`.
   Edges fan from listener → both accessors; both accessors →
   aggregate; aggregate → worker → responder. Update the pipeline
   name to `"NEXO bid-ask spread"`.
4. **`forum/db/seed/seed_test.go`** — `TestRunIsIdempotent` already
   counts table rows, not pipeline nodes; should stay green. Add
   one new sub-test
   `TestSeededPipelinesAreListenerSubscribable` that loads the
   seeded pipelines via `pool.ListPipelinesByTenant`, JSON-decodes
   the `Nodes` column, and asserts no Listener-kind node has a
   `field` starting with `ob.`. Keeps the rule enforced even if
   the seed code is later edited.
5. **`forum/db/seed/seed.go` ensureSeededPipelinesUnchanged
   (optional)**: if Phase 1 lands first and adds a runtime
   listener-validation in krn, we get a second layer of
   defense. Track via SPEC's risk note; skip if redundant.

**Dependencies:** Phase 1's `docs/atomic-keys.md` should be the
referenced doc in the TSDoc comment + seed `seed.go` comment. If
Phase 1 lands first, link directly. Otherwise, link to the spec
in `customer-layer/specs/...` (cross-repo path is fine).

**Verification:**

```bash
cd customer-layer && pnpm install --frozen-lockfile \
  && pnpm --filter @customer-layer/web build \
  && pnpm --filter @customer-layer/web test
cd forum && go test ./db/seed/...
cd forum && FORUM_DATABASE_URL='postgres://...' mage SeedDB \
  && FORUM_DATABASE_URL='postgres://...' mage SeedDB
```
- Web build succeeds; vitest reports the new `// @ts-expect-error`
  test passes.
- `go test ./db/seed/...` green —
  `TestSeededPipelinesAreListenerSubscribable` finds zero
  `ob.*` listener fields.
- Two consecutive `mage SeedDB` against the same pg are idempotent
  (row counts unchanged on second run, same as today).

**Risks:**

- **`(string & {})` escape hatch in `AtomicKey`.** If we just do
  `Exclude<AtomicKey, ...>` without narrowing the escape, the
  escape's `string` accepts `"ob.spread"` at compile time and
  AC #4 fails. Mitigation: `& string` after the Exclude is
  insufficient; the spec explicitly drops the `(string & {})`
  escape from the listener-subscribable type. A separate
  `ListenerSubscribableKey` type built from the explicit non-`ob.*`
  literal union (no `(string & {})`) is what closes AC #4.
- **Seed reshape changes pipeline node count.** Pipelines store
  nodes as a JSON column (`pipeline_graphs.nodes`), so per-table
  row count is unaffected. `TestRunIsIdempotent`'s table-row check
  stays green. Verified by reading `pipeline_graphs` schema in
  `db/migrations/sqlite/20260502203103_initial.sql`.
- **`worker.fn=spread` may not be a valid `WorkerFn` literal.**
  `spread` IS in the WorkerFn vocabulary (see
  `apps/web/src/types/pipeline-graph.ts:21`) so this is fine.
  Mitigation: AC #5 catches via `pnpm build` (TS would reject an
  invalid `fn`).

**Rollback:** revert the merged PR per repo. The TS narrow is
backward-compatible at the JS-runtime layer (only the type
declaration changes); the seed reshape produces a different
pipeline structure but is idempotent on UUID-key, so re-running
the prior seed against the same DB would skip the existing rows
(no replay needed). For a clean rollback both must revert in
lockstep; if only one reverts, the other still passes its own
tests in isolation.

---

## Phase 3: end-to-end PoC against feed-sim

**Goal:** Empirically demonstrate that the canonical pattern emits
non-zero updates against feed-sim, in the same 15s window where
ENC-101's original PoC saw zero. Document the green run as a
reproducible command in `docs/atomic-keys.md`.

**Tasks:**

1. **`apps/poc-client/`** (or wherever the existing PoC harness
   lives — check `krn/apps/`) — assemble a request JSON with
   the canonical pattern from Phase 1 task 7's example. Wire it
   to the same feed-sim WS endpoint
   (`feed-sim.v3m.xyz` ITCH stream) the original PoC used. Run
   for ≥15 seconds, count update messages, assert ≥1 update.
2. **`docs/atomic-keys.md`** — append a "Reproducing ENC-101's
   regression" section at the bottom: the exact `apps/poc-client`
   invocation + expected output (a non-zero update count). Cite
   the original 0-updates baseline + this run's count.
3. **(Optional) `krn/CHANGELOG.md`** if one exists — add an
   entry referencing ENC-101 closure with the link to the PR.

**Dependencies:** Phase 1 merged (the canonical pattern must be
live in TreeBuilder and the doc); Phase 2 can land in parallel
(its outputs are not on the PoC path).

**Verification:**

```bash
cd krn && cmake --build build -j --target krn_server poc_client \
  && ./build/krn_server &
SERVER_PID=$!
sleep 2
./build/poc_client \
  --ws ws://localhost:4000 \
  --request 'examples/enc-101-canonical.json' \
  --duration 15s \
  --expect-updates-min 1
kill $SERVER_PID
```
- The PoC's update counter is ≥1 and ideally in the same
  order-of-magnitude as `lastPrice`'s rate (the trade-event
  cadence drives the Listener clock; AtomicAccessor reads the
  current `ob.best.bid.price`).
- The output is captured into `docs/atomic-keys.md` so a future
  reader doesn't need feed-sim access to believe the rule.

**Risks:**

- **PoC harness API drift.** If `apps/poc-client` was renamed or
  moved during a recent refactor, the verification command needs
  re-grounding. Mitigation: first task in Phase 3 is to locate the
  harness (`find . -path './build' -prune -o -iname '*poc*' -print`);
  if absent, write a 50-line Go or Python WS client into
  `apps/poc-client/cmd/enc-101-repro/` instead.
- **Feed-sim availability.** `feed-sim.v3m.xyz` is an external
  dependency. Mitigation: cache a 30-second feed-sim transcript
  on disk and replay via `ItchAdapter`'s file-replay mode (the
  existing `krn-common/connectors/market/src/feed/ItchAdapter.cpp`
  supports this — see ENC-99's PoC notes).

**Rollback:** N/A — this phase only adds documentation and a
test artifact. If the PoC fails, the Phase 1+2 code is still
correct (the failure indicates either a feed-sim hiccup or a
real bug uncovered by the e2e check, which would surface as a
follow-up ticket, not a rollback of the rule itself).

---

## Cross-cutting concerns

- **Migrations:** none. No schema changes; the seed reshape produces
  different `pipeline_graphs.nodes` JSON content but the column
  shape is unchanged.
- **Observability:** no new metrics. The construct-time reject
  surfaces via the existing `subscribe`/`validate` WS error path
  + the existing `ws.validate.failed` metric (see
  `src/server/ClientSession.cpp` for the metric registration —
  if it exists). Verify in Phase 1 task 6.
- **Docs:** primary deliverable is the `docs/atomic-keys.md`
  rewrite (Phase 1 task 7 + Phase 3 task 2). Secondary:
  `krn/CLAUDE.md` cross-ref (Phase 1 task 8); customer-layer
  TSDoc comment (Phase 2 task 1); forum seed inline comment
  (Phase 2 task 3).

## Out of plan

- **C++ change to `ObProvider` to push events on book updates.**
  Spec alternative (a). Deferred indefinitely; would require a
  throttle layer that doesn't exist today.
- **New cloudchannel-protocol Interval-based listener for `ob.*`.**
  Spec alternative (c). Deferred — the canonical Listener-clock +
  AtomicAccessor pattern this plan delivers covers the same use
  case without expanding the protocol.
- **AI-prompt update.** No live LLM prompt today (post-cutover
  classifier is rule-based + aibox-mock returns canned plans).
  When real AI boxes replace aibox-mock, the prompt update
  references this plan's `docs/atomic-keys.md` section.
- **Multi-symbol or aggregation-pattern recipes** beyond the
  single NEXO/VALT example. Doc-only follow-up if/when needed.
