# SPEC: document atomic-key namespaces (bare vs ob.*)

**Slug:** atomic-keys-doc · **Date:** 2026-05-01 · **Status:** Approved
**Repo:** krn (+ customer-layer touch) · **Linear:** ENC-94

## Problem

The 2026-05-01 PoC subscribed to bare `bid` / `ask` against the
`feed-sim.v3m.xyz` ITCH feed and got nothing — those fields stay
empty because `MarketTickComputer` only writes them when the feed
payload carries `bid`/`ask` JSON keys directly (pre-aggregated
ticks). For raw L2/L3 sources like ITCH, top-of-book data lives
under the `ob.*` namespace (`ob.best.bid.price`, `ob.spread`, etc.)
served by `ob::Provider`. The split is intentional but undocumented;
nothing tells a user (or the AI compile prompt) which to use when.

## Proposed change

Doc-only. Add `krn/docs/atomic-keys.md` describing the two
namespaces, their sources, the full `ob.*` catalog, and when to
pick which. Surface a pointer from `CLAUDE.md`. Update
`customer-layer/apps/server/src/services/ai-compile-prompt.ts` so
Claude knows to ask for `ob.best.bid.price` against L2 sources
rather than bare `bid`.

## Scope

- New `krn/docs/atomic-keys.md` covering:
  - The two namespaces and the design intent (preserve "feed gave us
    this" vs "we derived this from order-book state").
  - Bare key vocabulary: `lastPrice`, `openPrice`, `highPrice`,
    `lowPrice`, `prevClose`, `mean`, `median`, `volume`, `obv`,
    `vwap`, `bid`, `ask`, `spread`, `timestamp`, plus the TA
    indicators (sma_N / ema_N / rsi_N / macd.* / bb.* / atr.N /
    momentum.N / roc.N / volume_avg.N / volatility_rank).
  - `ob.*` vocabulary: full transcription of
    `ObKeysCatalog::canonicalKeys` so the doc stays the single
    discoverable source.
  - Decision rule: pre-aggregated tick connector ⇒ bare; L2/L3
    ⇒ `ob.*`; never both for the same field.
- `krn/CLAUDE.md` Code Conventions: one bullet pointing at
  `docs/atomic-keys.md`.
- `customer-layer/apps/server/src/services/ai-compile-prompt.ts`:
  rewrite the "Atomic keys directly subscribable in the field"
  section of `SCHEMA_REFERENCE` to:
  - List the bare keys with a one-line "from pre-aggregated feeds"
    annotation.
  - List the `ob.*` keys with "from L2/L3 sources (ITCH, FIX, etc.)".
  - Tell Claude: when the user asks for bid/ask/spread on a feed
    described as "ITCH", "L2", or unspecified, prefer `ob.*`; only
    use bare when the user names a pre-aggregated source.

## Non-goals

- **No code in MarketTickComputer.** No alias path that resolves
  bare `bid` to `ob.best.bid.price`. The namespace split is
  intentional design, not a UX wart.
- **No new atomic keys.** Pure documentation of what already exists.
- **No breaking change to either namespace.** Bare keys still fire
  exactly when they fired before; `ob.*` is unchanged.
- **No automatic schema generation.** `ObKeysCatalog.hpp` is
  authoritative for the `ob.*` list; this doc duplicates it once
  with a note to keep them in sync. Generator tooling is a
  separate ticket if it ever matters.
- **No customer-layer test changes.** The AI compile prompt change
  doesn't break any existing test (tests mock the LLM call).

## Acceptance criteria

1. `krn/docs/atomic-keys.md` exists and contains both vocabulary
   lists plus the decision rule.
2. `krn/CLAUDE.md` has a one-line bullet referencing
   `docs/atomic-keys.md`.
3. `customer-layer/apps/server/src/services/ai-compile-prompt.ts`'s
   `SCHEMA_REFERENCE` mentions `ob.best.bid.price`, `ob.best.ask.price`,
   `ob.spread`, `ob.mid` explicitly.
4. The doc page transcribes every entry in
   `connectors/market/include/krn/ob/ObKeysCatalog.hpp::canonicalKeys`,
   verifiable by spot check.
5. A re-run of the 2026-05-01 PoC client subscribing to
   `(NEXO, ob.best.bid.price)` against feed-sim produces non-zero
   updates in 30 s. (Validates the doc's claim is accurate, not
   just plausible.)

## Constraints

- **Performance:** none — doc.
- **Compatibility:** none broken. Bare `bid`/`ask` continues to
  fire under the same conditions as before.
- **Dependencies:** AC5 reuses ENC-99's PoC client.

## Affected systems

- `krn/docs/atomic-keys.md` (new)
- `krn/CLAUDE.md` (one-line addition)
- `customer-layer/apps/server/src/services/ai-compile-prompt.ts`
  (rewrite the atomic-keys section of SCHEMA_REFERENCE)

## Alternatives considered

- **Wire bid/ask through MarketTickComputer from the OB.** Rejected:
  conflates "feed told us" with "we derived" — exactly the
  distinction the field-map was designed to preserve. Adds a runtime
  dependency from MarketTickComputer to OrderBookManager.
- **Alias `bid` → `ob.best.bid.price` at AtomicAccessor lookup.**
  Rejected: same conflation, with the additional cost of a
  surprising keyspace.
- **Auto-generate the doc from ObKeysCatalog.hpp.** Rejected as
  premature; transcribe once and add a sync note.

## Risks

- **Doc rot.** `ObKeysCatalog.hpp` could grow without the doc
  catching up. Mitigation: a one-line "Keep in sync with
  ObKeysCatalog.hpp" footer at the top of the section, plus the
  AC5 PoC re-run as a coarse smoke.

## Open questions

- None.
