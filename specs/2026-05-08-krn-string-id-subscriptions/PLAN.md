# PLAN: krn — string IDs on the WS subscribe protocol

**Spec:** [SPEC.md](./SPEC.md)
**Date:** 2026-05-08
**Status:** Draft

## Overview

Two phases. Phase 1 ships the entire variant<int, string> surgery in
one PR — `RequestKey.hpp` + the type ripple through Responder /
ClientSession.active_ / chains_ / sendFn + parser admitting
`id:"string"` + outbound rendering branch. The variant is atomic:
you can't half-introduce it without leaving call sites uncompilable,
and the wire-shape change (parser accepts new + render branches) is
likewise atomic. Phase 1 ends with smoke.js still green AND a new
gtest fixture exercising the string-keyed round-trip end to end.

Phase 2 backfills the cross-proposal smoke: `embassy-saved-scene-
dispatch` proposal's `tools/saved-scene-smoke.sh` (still pending in
that proposal's phase 3) becomes the green-bar AC. Plus docs +
benchmark verify the hot path didn't regress.

Phasing rationale: the variant refactor in phase 1 is the load-
bearing piece — once it lands, the rest is cleanup. Splitting phase
1 into "type alias" + "ripple" + "wire" sub-phases would create
intermediate states where the code doesn't compile or the wire
contract is half-shipped (e.g., parser accepts strings but renderer
still int-only). One coherent PR, one clear gate.

## Phase 1: variant refactor + wire enablement

**Goal:** A single connected WS client can subscribe with `key:int`
AND a separate WS client can subscribe with `id:"string"` to the
same krn instance; each receives correctly-typed value frames
(`{key:N, ...}` vs `{requestId:"...", ...}`); smoke.js stays
green; the int-keyed unit tests still pass.

**Tasks:**

1. New header `include/krn/server/RequestKey.hpp` defining
   `using RequestKey = std::variant<int, std::string>;` plus a
   `std::hash<RequestKey>` specialization (combines the variant
   index discriminant with the alternative's standard hash so
   `int(5)` and `string("5")` hash to different values). Add free
   helpers: `void writeRequestKeyJSON(rapidjson::Writer&, const
   RequestKey&)` (writes `"key":N` for int, `"requestId":"..."` for
   string), and `std::optional<RequestKey> parseRequestKeyFromObj(
   const rapidjson::Value&)` (reads `key:int | id:int | id:string`,
   rejects a request that supplies both `key` AND `id`).
2. Refactor `include/krn/nodes/Responder.hpp` + `src/nodes/Responder.cpp`:
   - Change ctor parameter from `int key` to `RequestKey key`.
   - Field `key_` becomes `RequestKey`.
   - `send_` typedef changes to
     `std::function<void(const RequestKey&, const StreamValue&)>`.
   - The body's `fn(key_, sv)` call site is unchanged in structure.
   - Single Responder ctor call site outside ClientSession is at
     `tests/connection/...` (verify via
     `grep -rn 'std::make_shared<krn::nodes::Responder>'`); update
     fixture to pass a `RequestKey{1}` literal.
3. Refactor `include/krn/server/ClientSession.hpp:77-78`:
   `unordered_map<int, ...> active_` and
   `unordered_map<int, vector<INode>> chains_` →
   `unordered_map<RequestKey, ...>` (using the new hash). Include
   `RequestKey.hpp`.
4. `src/server/ClientSession.cpp:344-357` — subscribe parser:
   - Replace the int-only key extraction with
     `auto rk = parseRequestKeyFromObj(r); if (!rk) {
     sendError("subscribe", "..."); continue; }` so
     `key:int`, `id:int`, AND `id:"string"` all produce a valid
     `RequestKey`.
   - Update the surrounding code that names `int key` — it now
     names `RequestKey key`.
   - Update map operations at lines 470, 484-489 to use the
     variant key (no code change required if the types line up,
     but verify).
5. `src/server/ClientSession.cpp:387-413` — sendFn lambda + Responder
   construction:
   - Lambda signature: `[weak](const RequestKey& reqKey, const
     krn::StreamValue& sv) { ... }`.
   - JSON emit body: replace `w.Key("key"); w.Int(reqKey);` with
     `writeRequestKeyJSON(w, reqKey);` (writes the appropriate
     field+value pair via std::visit on the variant).
   - Responder ctor call passes `key` (now `RequestKey`).
6. `src/server/ClientSession.cpp:497, 546` — ack/error JSON emit
   sites: same `writeRequestKeyJSON` swap.
7. `src/server/ClientSession.cpp:516-540` — cancel handler:
   - Read `keys: [int, ...]` AND `ids: ["...", ...]` arrays. Both
     present in one payload is an error (`sendError("cancel",
     "specify keys (int) or ids (string), not both")` and return).
   - Build `std::vector<RequestKey>` from whichever is present.
   - The `active_.find(key)` and `chains_.erase(key)` lines work
     against the variant-keyed map unchanged.
8. Build verification: `cmake --build build -t krn_server -j$(nproc)`
   completes clean. `cmake --build build -t krn_tests -j$(nproc)`
   completes clean. Run the existing `krn_tests` binary —
   ALL pre-existing tests pass (regression gate for the variant
   ripple).

**Dependencies:** Phase 1 of `embassy-saved-scene-dispatch`
(merged) is unrelated to this work but provides the operator
visibility that surfaced the bug — useful but not required. No
hard dependencies.

**Verification:**
- `ctest --output-on-failure` (or `krn_tests` binary) passes —
  all pre-existing tests including the int-keyed
  `tests/ws/ClientSessionTest.cpp` cases stay green.
- `node tools/smoke-test/smoke.js --url ws://localhost:4000
   --duration 30` passes ≥22/23 keys with data (matches today's
   baseline).
- Manual probe with a one-liner JS script over WS: send
  `{type:"subscribe", requests:[{id:"r-test-1", streamKey:"NEXO",
  field:"lastPrice"}]}`. Observe a `{type:"subscribed",
  requestId:"r-test-1"}` ack AND ≥1 value frame
  `{requestId:"r-test-1", streamKey:"NEXO", value:<float>}`.

**Risks:**
- **`std::hash<std::variant<int, std::string>>` collision.** A
  naive hash that just XORs the variant's index with the
  alternative's hash collapses int(5) and string("5") onto the
  same bucket. **Mitigation:** the hash specialization
  unconditionally salts on the variant index (e.g., `idx ^
  (alt_hash << 1)`); a unit test in
  `tests/ws/ClientSessionTest.cpp` (or new
  `tests/server/RequestKeyHashTest.cpp`) asserts they differ.
- **Ripple touches more files than the spec named.** `int key`
  may appear elsewhere in the engine — e.g., in test fixtures
  that construct a Responder for unit testing the dispatch
  path. **Mitigation:** task 2's prep step is
  `grep -rn 'std::make_shared<krn::nodes::Responder>\|Responder(' \
   include/ src/ tests/` to enumerate every ctor site; each
  one gets the `RequestKey` upgrade in the same PR.
- **Hot-path overhead.** `std::visit` on the variant inside
  `sendFn`'s emit body adds branch cost on every value update.
  **Mitigation:** the variant lives at the wire boundary only —
  Dispatcher / AtomicStore / TreeBuilder never see it. The
  lambda fires once per value emit (already the slowest part
  of the path because of JSON encoding + WS write). The variant
  visit is cheap relative to the JSON serialization. Verify
  with the existing benchmark in phase 2 task 3.
- **Wire-shape regression silently breaks smoke.js.** The
  smoke.js client parses outgoing value frames as
  `{key:int, ...}` and would break if the int path's renderer
  accidentally emits `{requestId:"5", ...}` instead. **Mitigation:**
  AC-3 hardcodes smoke.js green-bar; the manual probe in
  verification uses a separate string-keyed payload that doesn't
  share state with smoke.js's int-keyed flow.

**Rollback:** Revert the krn PR. Phase 1 is one PR; reverting
restores the int-only behavior. No persistence layer affected; no
schema migration to reverse. Embassy's saved-scene path returns to
silently-dropping per the pre-fix state.

---

## Phase 2: regression tests + cross-proposal smoke + docs + bench

**Goal:** New named tests cover the string-key path; the
embassy-saved-scene-dispatch proposal's `tools/saved-scene-smoke.sh`
runs to completion against a krn build that includes phase 1; the
hot-path benchmark stays within 2% of baseline; the protocol's new
shape is documented.

**Tasks:**

1. Extend `tests/ws/ClientSessionTest.cpp` with three new test
   cases:
   - `TEST(ClientSessionTest, SubscribeWithStringIdEchoesRequestId)`:
     subscribe with `id:"r-NEXO-test"`, send a value via the
     internal Dispatcher, assert outbound JSON contains
     `"requestId":"r-NEXO-test"` (no `"key"` field).
   - `TEST(ClientSessionTest, MixedIntAndStringSubsCoexist)`:
     subscribe with `key:1` AND with `id:"alpha"` on the same
     session; fire two distinct values; assert frame for key:1
     is `{key:1, ...}` and for "alpha" is `{requestId:"alpha",
     ...}`.
   - `TEST(ClientSessionTest, CancelByIdsRemovesStringKeyedSub)`:
     subscribe with `id:"foo"`, cancel with
     `{type:"cancel", ids:["foo"]}`, assert subsequent values
     for that subscription don't fire.
2. New test file
   `tests/server/RequestKeyHashTest.cpp` (or fold into
   `ClientSessionTest.cpp` if the layout convention is
   per-domain): unit tests for the `std::hash<RequestKey>`
   specialization. Three cases: `int(5)` and `string("5")` hash
   differently; same-int / same-string hash equal; both can
   coexist as map keys without overwriting each other.
3. Run `krn` as the existing dev compose service against the
   `embassy-saved-scene-dispatch` proposal's
   `tools/saved-scene-smoke.sh` (which the embassy proposal's
   phase 3 still needs to ship — this task waits on that script's
   existence). Expect: 7 NEXO subscribes log `ws.subscribe` on
   krn; embassy's data-plane WS receives binary frames within
   5s. **If the script doesn't exist yet:** stand up a manual
   smoke equivalent (a bash one-liner that triggers the GET
   `/api/sessions/<candle-id>` and tails `dev_gma_v3_1`'s log)
   and capture its result in this PR's body.
4. Run the existing `tests/dispatch/...` benchmark
   (or `mage Bench` equivalent if it exists for krn) against
   this PR's branch and main. Assert the per-update path's
   p99 / records-per-sec stays within 2% of main's baseline.
   Capture the numbers in the PR body.
5. Update `docs/atomic-keys.md` with a one-paragraph note on the
   subscribe wire format: a request key may be `key:int`,
   `id:int` (legacy fallback), or `id:"string"`. Outbound value /
   ack frames mirror the type via `key:int` or
   `requestId:"string"`.
6. Update `krn/CLAUDE.md`'s "Code Conventions" section with a
   one-line addition: subscribe / cancel / value-emit code paths
   use `RequestKey = std::variant<int, std::string>`; engine
   internals (Dispatcher, AtomicStore, TreeBuilder, Listener)
   stay key-type-agnostic.

**Dependencies:** Phase 1 merged.

**Verification:**
- `ctest -R 'StringId|MixedIntAndString|CancelByIds|RequestKeyHash'
   --output-on-failure` runs all 4 new tests (3 in ClientSessionTest
   + 1 in RequestKeyHash) and passes.
- The cross-proposal smoke (or its manual equivalent in task 3)
  shows 7 NEXO `ws.subscribe` lines on krn + binary frames
  flowing to the browser data WS.
- The benchmark numbers in the PR body show ≤2% delta from
  main's baseline on per-update path.
- `grep -E '\bkey\b.*int|requestId.*string' docs/atomic-keys.md`
  finds the new note.

**Risks:**
- **Cross-proposal smoke isn't ready.** The embassy proposal's
  phase 3 has the smoke script as a Todo. If the script doesn't
  exist when this proposal is ready to verify, task 3 falls back
  to the manual one-liner. **Mitigation:** task 3 explicitly
  documents the fallback so this proposal isn't blocked on the
  embassy proposal's task list ordering.
- **Benchmark regresses despite control-path-only variant.** The
  variant's type discriminant adds one branch per value emit. If
  the JSON encoder allocates differently for the
  `requestId:"string"` path vs `key:N`, the per-emit cost could
  diverge. **Mitigation:** the existing string-keyed path is
  rarer (used only by embassy-style clients); int-keyed
  (smoke.js) path stays byte-for-byte identical, so the benchmark
  on the int side is the load-bearing measurement and that
  shouldn't move.
- **Test fixtures double-define a `RequestKey` literal in a
  way that collides with the production header.** Test fixtures
  may want to construct `RequestKey{42}` and `RequestKey{"foo"}`;
  the variant's implicit conversions can be ambiguous. **Mitigation:**
  in tests, use the explicit alternative tag:
  `RequestKey{std::in_place_index<0>, 42}` /
  `RequestKey{std::in_place_index<1>, "foo"}`. Document the
  pattern in the new test file's preamble.

**Rollback:** Tests + docs are non-runtime; deletion is the
rollback. The bench numbers are in the PR body; if they regress,
the path is to revert phase 1 (above), not phase 2.

---

## Cross-cutting concerns

- **Migrations:** none. No persisted state, no proto changes,
  no schema migrations. The variant lives entirely in C++ memory
  + on the wire.
- **Observability:** the `ws.subscribe` log line in
  `ClientSession.cpp` (search for the existing INFO emit) gets a
  field-name update — for int subs render `key=N`, for string
  subs render `requestId="..."`. Pin this in /plan task 1.4 (or
  a tiny separate task in phase 1) so the operator-grep story
  matches the wire.
- **Docs:** Phase 2 task 5 hits `docs/atomic-keys.md`; task 6
  hits `krn/CLAUDE.md`. No external readme changes.
- **Linear:** create a project for this proposal at /linear sync
  time. ~10 task issues + 7 ACs. Cross-references the
  embassy-saved-scene-dispatch project's phase 2 (which becomes
  unblocked when phase 1 of THIS proposal merges).

## Out of plan

- **Engine-internal type churn.** Dispatcher / AtomicStore /
  TreeBuilder / Listener — all stay int-key-naïve (they don't
  see the request key; they route on `(streamKey, field)`).
- **Stringified-int back-compat shim.** `id:"5"` is the string
  "5", not the integer 5. POLA. Documented in spec non-goals.
- **Mixed cancel payload.** `keys` and `ids` in the same cancel
  message is an explicit error. Documented in spec non-goals.
- **New krn node kinds.** Out of scope.
- **Treaty / proto schema changes.** Embassy's wire shape is
  already string-id-compatible upstream of this proposal.
- **Saved-scene end-to-end smoke as part of THIS proposal's
  green-bar.** That's the embassy proposal's territory — this
  proposal's phase 2 task 3 captures the smoke result, but the
  *script* lives in embassy. If that script slips, the manual
  one-liner stands in.
