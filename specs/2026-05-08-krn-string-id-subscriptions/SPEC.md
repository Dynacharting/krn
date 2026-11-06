# SPEC: krn — string IDs on the WS subscribe protocol

**Slug:** krn-string-id-subscriptions
**Date:** 2026-05-08
**Status:** Draft
**Author:** ndrandal
**Repo:** krn

## Problem

krn's WebSocket subscribe protocol only accepts integer request
keys: `ClientSession::handleSubscribe` at `src/server/ClientSession.cpp:344-357`
reads `key: int` (or fallback `id: int`) and rejects anything else with
`sendError("subscribe", "request missing integer 'key'")`. Internally
the entire request-routing surface — `ClientSession::active_`,
`ClientSession::chains_`, `Responder.key_`, the `sendFn` callback's
signature — is `int`-keyed end to end. Embassy (the on-prem agent)
sends string request IDs derived from forum's saved-scene
InstructionPackages (e.g. `r-NEXO-open`, `r-NEXO-vwap`); krn silently
rejects every one of them. The end-to-end saved-chart-slice path has
been broken at this seam since it was first wired — yesterday's
smoke worked only because `tools/smoke-test/smoke.js` is its own
int-keyed client that never goes through embassy. ADR-001 in
`customer-layer/specs/2026-05-08-embassy-saved-scene-dispatch/DECISIONS.md`
captures the discovery and the chosen fix shape.

## Proposed change

Generalize krn's request-key surface to accept **both** integer
and string keys natively. On subscribe: a request with `key: int` (or
fallback `id: int`) keeps working exactly as it does today (smoke.js
+ any future int-keyed client are unaffected); a request with
`id: "string"` is now a first-class alternative. Internally, the
key-bearing types switch from `int` to `std::variant<int,
std::string>` (typedef `RequestKey`): `ClientSession::active_` /
`chains_` keys, `Responder::key_`, and the `sendFn` callback
signature. On outbound: int-keyed subscriptions continue to emit
`{key: <int>, streamKey, value}` in value frames (and
`{type:"ack", key:<int>, status, reason}` in acks); string-keyed
subscriptions emit `{requestId: "<string>", streamKey, value}`
(matching embassy's existing `inboundProbe.requestId` reader) and
`{type:"ack", requestId: "<string>", status, reason}`. Cancel grows
a parallel `ids: ["..."]` array alongside the existing
`keys: [<int>, ...]`. The variant boundary stays concentrated at
the wire (parse + render) and at the routing maps; the engine
(Dispatcher, AtomicStore, TreeBuilder, Listener) stays
key-type-agnostic.

## Scope

- `src/server/ClientSession.cpp:handleSubscribe` (lines 344-357 +
  the rest of the subscribe path through line 510): accept
  `id: "string"` as a valid request key alongside the existing
  `key: int` / `id: int` paths. Mutual-exclusivity check: a
  request with both `key` and `id` AND both pass type validation
  is an error (`sendError("subscribe", "request must have key
  (int) OR id (int|string), not both")`).
- `include/krn/server/ClientSession.hpp:77-78` — change
  `std::unordered_map<int, …> active_` and `chains_` to
  `std::unordered_map<RequestKey, …>`. Define `RequestKey` as
  `std::variant<int, std::string>` in a new header
  `include/krn/server/RequestKey.hpp` with std::hash specialization
  so `unordered_map` works.
- `include/krn/nodes/Responder.hpp` + `src/nodes/Responder.cpp`:
  add a constructor overload (or change the constructor) accepting
  `RequestKey key` instead of `int key`. The `sendFn` typedef
  changes from `std::function<void(int, const StreamValue&)>` to
  `std::function<void(const RequestKey&, const StreamValue&)>`.
  Internal `key_` field becomes `RequestKey`.
- `src/server/ClientSession.cpp:387-413` — the sendFn lambda signature
  matches the new typedef. The lambda's body branches once on the
  variant: int alternative writes `w.Key("key"); w.Int(N);`; string
  alternative writes `w.Key("requestId"); w.String(s);`. Same one-
  shot dispatch in any other JSON emit site (acks at lines 497, 546).
- `src/server/ClientSession.cpp:516-540` — cancel handler: read
  legacy `keys: [int, …]` AND new `ids: ["…", …]` arrays in the
  same payload (either, both, or neither — neither is a
  no-op). Internally normalize to a `std::vector<RequestKey>` and
  iterate; existing `chains_.erase(key)` works against the
  variant-keyed map.
- New header `include/krn/server/RequestKey.hpp` with the alias and
  the `std::hash<RequestKey>` specialization. Helper free
  functions: `requestKeyToJSON(key, writer, fieldName)` (writes
  the appropriate field+value pair) and `parseRequestKey(json)`
  (returns `optional<RequestKey>`).
- `tests/server/ClientSessionTest.cpp` (or new
  `tests/server/StringIDSubscribeTest.cpp`): subscribe with
  `id: "r-NEXO-open"`, expect ack with `requestId: "r-NEXO-open"`,
  expect value frames with `requestId: "r-NEXO-open"`. Mixed-
  client scenario: one int-keyed sub + one string-keyed sub on
  the same WS session, both produce correctly-shaped frames.
- `tools/smoke-test/smoke.js` — no changes; it must continue to
  work bit-for-bit identical to today as the regression check.
- `docs/atomic-keys.md` (one-paragraph addition or new section): the
  WS subscribe protocol's key field is `key:int | id:int |
  id:string`; outbound mirrors the type via `key:int` or
  `requestId:string`.

## Non-goals

- **No engine-internal type churn.** Dispatcher's per-event routing
  table, AtomicStore's `(streamKey, field)` map, TreeBuilder's
  request JSON parsing, Listener's `(streamKey, field)` binding —
  none of these touch the request key. The variant lives in
  ClientSession + Responder + sendFn only. Engine code remains
  key-type-agnostic.
- **No int-keyed wire change.** Smoke.js subscribes with `key:int`
  today and expects `key:int` back; that contract is preserved
  byte-for-byte.
- **No protobuf / treaty schema change.** Treaty's
  `cloudchannel.v1.GmaSubscriptionRequest` already carries `id`
  as a string; embassy's wire-out shape is already correct upstream
  of this proposal. The fix lives entirely inside krn.
- **No new node kinds.** No new pipeline shapes. The "string-id"
  capability is a request-correlation surface, not a routing
  primitive.
- **No mixed cancel.** A single cancel payload may carry `keys` OR
  `ids`, but not both at the same time. Mixed is an explicit error
  (`sendError("cancel", "specify keys (int) or ids (string), not
  both")`). Keeps the parser tight and the failure modes obvious.
- **No back-compat shim for "id:int".** The existing
  `r["id"].IsInt()` fallback at line 353 is preserved (yesterday's
  smoke uses `key:int`; some clients may still send `id:int`),
  but no new shim translates a stringified-int (`id:"5"`) to an
  integer key. If a client sends `id:"5"`, krn treats it as the
  string "5", not the integer 5. Matches POLA.
- **No sovereignty / instruction lifecycle changes.** Forum's
  ack-tracking, embassy's instruction-handler — both untouched
  by this proposal.

## Acceptance criteria

1. Subscribe with `{type:"subscribe", requests:[{id:"r-NEXO-open",
   streamKey:"NEXO", field:"openPrice"}]}` over WS to
   `ws://localhost:4000` produces a `{type:"subscribed",
   requestId:"r-NEXO-open"}` ack, AND `dev_gma_v3_1`'s log
   contains `ws.subscribe ... requestId=r-NEXO-open` (or
   equivalent — the existing log line learns to render the
   variant; field name TBD at /plan time).
2. The same subscribe receives at least one value frame within 5
   seconds (assuming feed-sim is publishing NEXO data) and the
   frame is `{requestId:"r-NEXO-open", streamKey:"NEXO",
   value:<float>}` (no `key` field).
3. Smoke.js continues to pass: `node tools/smoke-test/smoke.js
   --url ws://localhost:4000 --duration 30` reports PASS,
   ≥22/23 keys with data (matching today's baseline). The wire
   format on smoke.js side is unchanged byte-for-byte.
4. A mixed-client scenario over a single WS connection — one
   `key:1` int sub + one `id:"alpha"` string sub — produces
   correctly-typed value frames per subscription (int sub gets
   `key:1`, string sub gets `requestId:"alpha"`), no
   cross-contamination.
5. Cancel with `{type:"cancel", ids:["r-NEXO-open"]}` returns
   the matching `chains_str_` (or variant-map equivalent) entry
   to nothing; subsequent value updates for that subscription
   stop. Cancel with `{type:"cancel", keys:[1]}` continues to
   work as today.
6. `ctest` / `cmake --build . -t krn_tests` passes including
   ≥3 new test cases covering the string-key path (subscribe,
   value frame, cancel).
7. The end-to-end saved-scene smoke from
   `embassy-saved-scene-dispatch` phase 3 (the
   `tools/saved-scene-smoke.sh` script that proposal still
   needs to ship) passes when run against the krn build from
   this proposal. Cross-proposal smoke; soft-AC here, gated in
   the embassy proposal's plan.

## Constraints

- **Performance:** The variant adds no per-tick cost. `Responder`
  fires `fn(key_, sv)` once per value update; `key_`'s type was
  already passed by value (`int`), now it's a `variant<int,
  string>` (passed by const-ref). Hash on `unordered_map<RequestKey>`
  is a single-discriminant branch — bounded constant overhead. The
  existing zero-allocation hot path stays zero-allocation: no
  string allocation per value emit (the variant's string is
  stored once at subscribe, pointed-to thereafter). Verify with
  `mage Bench`-equivalent — krn's existing benchmarks should not
  regress >2%.
- **Compatibility:**
  - **Smoke.js** wire-byte-compatible: unchanged.
  - **Embassy** wire-compatible without code change: embassy already
    sends `id:"string"` and reads `requestId:"string"`.
  - **Treaty** schema: unchanged.
- **Dependencies:** none — this is a self-contained krn change.
  Embassy's saved-scene fix unblocks once this lands; that's
  tracked in the embassy proposal's phase 2.
- **Build:** C++20, `std::variant` is in standard. No new third-party.
- **Deadline:** none, but blocks the saved-chart-slice demo's
  pixel-painting path indefinitely.

## Affected systems / callers

- `src/server/ClientSession.cpp:344-357` (subscribe parser),
  `:387-413` (sendFn lambda + Responder construction), `:497`
  + `:546` (ack JSON emit), `:516-540` (cancel handler).
- `include/krn/server/ClientSession.hpp:77-78` (active_, chains_).
- `include/krn/nodes/Responder.hpp` + `src/nodes/Responder.cpp`.
- New file: `include/krn/server/RequestKey.hpp` (variant alias,
  hash specialization, helpers).
- New / extended test: `tests/server/StringIDSubscribeTest.cpp`
  (or extension of an existing file).
- `tools/smoke-test/smoke.js` — read-only contract; new test in
  embassy or a new krn test exercises the string path.
- `docs/atomic-keys.md` — wire-format note.

## Alternatives considered

- **C — krn adds an optional `requestId` echo on int-keyed value
  frames; clients that want string IDs send a sidecar string
  alongside the int key.** Smaller (~30 LOC C++), keeps internal
  representation int. Rejected per ADR-001: the "real
  architecture" answer puts the identifier contract on the data
  plane. The two-plane rule favors krn owning the request-key
  surface end to end. Option C is held in reserve as a fallback
  if the variant work blocks for >1 week.
- **Embassy maps string↔int internally** (option B in
  embassy-saved-scene-dispatch ADR-001). Rejected: every
  embassy-like client repeats the same int↔string dance, and the
  per-update int→string lookup violates embassy's zero-alloc hot
  path. Pushes the right concern to the wrong layer.
- **Always-string-internally with original-type-remembered for
  outgoing rendering.** A reasonable middle: smaller blast than
  the variant, single internal type. Rejected because
  `unordered_map<string,…>` lookups have a string-hash cost on the
  request-routing path (per cancel + per value's `chains_`
  retain-step), and because mixing "internally-string-but-we-
  pretend-int-on-the-wire" is a subtle invariant that rots over
  time. Variant makes the type story explicit at every site.

## Risks

- **C++ template / hash boilerplate for variant-keyed maps.**
  `std::hash<std::variant<int, std::string>>` doesn't exist by
  default; we provide a specialization. Wrong-spec'd hash → silent
  collisions → wrong-routed frames. **Mitigation:** the new
  test fixture's mixed-client scenario (int+string subs in the
  same session) catches symmetric collisions; CT also adds a unit
  test that the helper hashes int(5) and string("5") to different
  values.
- **Per-update overhead from variant dispatch.** A `std::visit`
  inside the per-tick path could cost cycles. **Mitigation:** the
  variant lives ONLY at the wire boundary (sendFn body's render
  branch). Engine-internal hot paths (Dispatcher, AtomicStore)
  never see RequestKey. Bench numbers stay within 2% of baseline.
- **Cancel/active_ collision between int(5) and string("5").**
  `unordered_map<RequestKey>` treats these as separate keys (by
  variant index discriminant), so int 5 and string "5" can
  coexist without overwriting. **Mitigation:** explicit unit test.
- **Smoke.js silently breaks on a wire-shape regression.** The
  smoke is the canary. **Mitigation:** AC-3 hardcodes the smoke
  green-bar against today's baseline; CI gates it once smoke runs
  in CI.
- **Outbound `requestId` field name conflicts with existing
  protocol fields.** krn's outgoing frames already use
  `requestId` in nowhere visible from ClientSession.cpp's grep —
  but `ack` frames may already carry a different field. **Mitigation:**
  before /plan, grep `krn/src/` for existing `requestId` usage
  and confirm no conflict.

## Open questions

- Field-name on outgoing log lines (`ws.subscribe ...`): does the
  log render `key=N` for int subs and `requestId="..."` for
  string subs, or unify under one field name? Resolved at /plan.
- Existing `tests/server/` test layout — confirm the right home
  for the new test file (extend existing or new file). Resolved
  at /plan time by reading the dir.
