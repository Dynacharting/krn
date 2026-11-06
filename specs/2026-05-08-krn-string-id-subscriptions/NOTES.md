# NOTES — krn-string-id-subscriptions discovery

## Trigger

Mid-execution finding from
`embassy-saved-scene-dispatch` phase 2 task 1 (per ADR-001 in
that proposal's DECISIONS.md). Phase 1's instrumentation surfaced
that embassy successfully sends 7 subscribe messages per
saved-scene InstructionPackage but krn logs zero `ws.subscribe`
activity — a wire-protocol mismatch, not anything in the original
spec's hypothesis list.

## Live finding

- krn's parser at `src/server/ClientSession.cpp:344-357` accepts
  `key:int` or fallback `id:int`; rejects with
  `sendError("subscribe", "request missing integer 'key'")` and
  `continue` on anything else.
- Embassy sends `{id:"r-NEXO-open", streamKey:..., field:...}` —
  `id` is a string.
- Bidirectional mismatch: krn's value-update frames emit
  `{key:int, streamKey, value}`; embassy's `inboundProbe` reads
  `requestId:"string"`.
- Smoke.js (`tools/smoke-test/smoke.js`) uses int-keyed wire and
  works fine — never goes through embassy. Yesterday's 23/23
  green smoke is unrelated to this codepath.

## Forks resolved

- **Q1 — fix shape (from ADR-001 in embassy-saved-scene-dispatch):**
  Option A (krn accepts string IDs natively). Chosen for
  two-plane-rule alignment: data plane owns the identifier contract.
- **Q2 — internal representation:** Option B (variant<int, string>
  across the int-keyed surface — Responder.key_, ClientSession's
  active_/chains_, sendFn signature). Chosen for clean type
  semantics over parallel paths (option C) or always-string
  with type-remembering (option D).
- **Q3 — outgoing wire field name for string-keyed subs:** Option W
  (`requestId:"string"` on outbound, matching embassy's existing
  `inboundProbe.requestId` reader). Asymmetric with inbound
  (`id` in, `requestId` out) but zero embassy-side change.

## Memory referenced

- Two-plane rule (`feedback_two_plane_rule.md`) — the request-key
  contract belongs to the data plane. krn is the data plane. ✓
- Always-ticket-on-Linear (`feedback_always_ticket_linear.md`) —
  this proposal will sync to Linear after /plan.

## Deferred / out of scope

- Engine-internal type churn (Dispatcher, AtomicStore, TreeBuilder,
  Listener). RequestKey lives at the wire + routing boundary only.
- Treaty / proto schema changes. Embassy's wire shape is already
  string-id-compatible upstream of this proposal.
- Mixed-keys-in-one-cancel-payload. Use either `keys:[int,...]`
  OR `ids:[str,...]`, not both at once.
- Stringified-int back-compat shim. `id:"5"` is the string "5",
  not the integer 5 — POLA.
