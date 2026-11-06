# Discovery notes — ob-keys-pipeline-only

## Origin

ENC-101 ticket: PoC validating ENC-94 / ENC-99 found that
`Listener(ob.best.bid.price)` (and friends) registers with the
Dispatcher but never fires. 15s subscription returned 0 updates
while bare keys + TA derivations on the same connection received
170. Filed with three explicit alternatives:

- (a) Make ObProvider emit `Dispatcher::notifyListeners` on book updates.
- (b) Doc-only — ob.* is pipeline-accessible, not directly Listenable.
- (c) Add Interval-based polling protocol for ob.* fields.

ENC-94's `krn-common/docs/atomic-keys.md` (status: shipped) currently
presents `ob.*` as "subscribe to these for L2 sources" — exactly
the framing ENC-101 found is wrong.

## Key code investigations during discovery

- `connectors/market/src/MarketTA.cpp:400` — the only call site
  of `Dispatcher::notifyListeners`. Bare keys (lastPrice,
  TA derivations) push from here.
- `connectors/market/src/ob/ObProvider.cpp` — zero references to
  `Dispatcher` or `notifyListeners`. Writes the AtomicStore on
  book updates and stops.
- `forum/db/seed/seed.go:293` — only post-cutover reference to
  `ob.*` as a `Listener.config.field`. Added by mvp-deploy-prep
  Phase 2's seed package (this session); is itself an instance
  of the bug ENC-101 describes.
- `forum/internal/orchestrator/classifier.go` — post-cutover
  classifier is rule-based, not LLM-prompt-based; aibox-mock
  returns canned plans. There is no live LLM prompt today that
  could mis-generate listener-on-ob.\*; the prompt-update concern
  from ENC-94 is deferred to "when real AI boxes replace
  aibox-mock."

## Decision

Option (b), with construct-time enforcement so the failure mode
is loud rather than silent. User instruction: "take the approach
that will work and fits best into the current solution."

Architectural fit: the push-vs-pull split between MarketTA and
ObProvider is intentional. Option (b) sharpens that boundary
without expanding the protocol or the C++ hot path.

## Items deferred (out of scope)

- C++ change to ObProvider — alternatives considered, rejected.
- New cloudchannel protocol shape for ob.\* polling — rejected;
  the canonical Listener-clock + AtomicAccessor composition
  already expresses what callers need.
- Real AI-prompt update — deferred until aibox-mock is replaced.

## Phasing intuition (for /plan)

Probably one phase split into two waves of touchpoints:

- **Wave 1 (krn):** docs + Listener/TreeBuilder reject + tests.
  Self-contained in krn repo; verifiable with `mage Test` /
  the existing C++ test harness.
- **Wave 2 (customer-layer + forum):** TS type narrowing + seed
  reshape. Depends on Wave 1's docs but not its code.

Cross-cutting is small. `/plan` may collapse this into a single
phase given total scope.
