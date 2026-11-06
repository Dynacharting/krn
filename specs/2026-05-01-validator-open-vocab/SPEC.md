# SPEC: JsonValidator open-vocabulary string scan + generic recursion

**Slug:** validator-open-vocab
**Date:** 2026-05-01
**Status:** Approved
**Author:** ndrandal
**Repo:** krn

## Problem

`JsonValidator::validateTree` enumerates a fixed allowlist of string field names (`"type", "symbol", "field", "node", "function", "fn"`) when checking string-length limits, and recurses into a fixed allowlist of structural keys (`"child"`, `"inputs"`, `"stages"`, `"pipeline"`, `"node"`). Connectors that add new node types with new sub-spec keys silently skip validation — both for length and for depth — because the validator doesn't know to look there. Adding `binance.tradeSpec.symbol` today requires editing `JsonValidator.cpp`. The fix is one of the audit's V19 echoes: the engine should treat user JSON generically.

## Proposed change

Replace the closed-vocabulary scan with a generic walk: validate every string member against `MAX_STRING_LEN` regardless of name, and recurse into every nested object or object-array regardless of key, all bounded by the existing `MAX_TREE_DEPTH` and `MAX_ARRAY_SIZE` caps. Behavior on the existing built-in node types is unchanged (every existing test still passes, including the negative tests that assert specific error messages); the validator gains the ability to catch oversize strings on connector-introduced keys without code changes.

## Scope

- Rewrite `JsonValidator::validateTree` (~`src/core/JsonValidator.cpp:64-131`) to:
  - Iterate `v.MemberBegin()..MemberEnd()`. For each member:
    - If string → check length against `MAX_STRING_LEN`. The error message names the offending key.
    - If object → recurse with `depth + 1`.
    - If array → check size against `MAX_ARRAY_SIZE`; iterate; for each element that is an object recurse with `depth + 1`; for each that is a string check length.
    - Else (number, bool, null) → no check.
  - After the member walk, retain the existing `if (v.HasMember("type")) validateNode(v);` call so unknown node types still throw the existing error.
- The hardcoded `stringFields[]` array, the fixed `arrayKeys[]` array, and the explicit `"child"`/`"node"`/`"inputs"` recursion arms are deleted in favor of the generic walk.
- Update `tests/validation/ValidationTest.cpp` (and any other validator tests) to add coverage:
  - Oversize string under a connector-introduced key (e.g. `"trade.notes"`) is rejected with an error mentioning `"trade.notes"`.
  - Oversize array under a connector-introduced key is rejected.
  - Deeply-nested object via a non-builtin key path triggers `MAX_TREE_DEPTH`.
  - Existing positive tests for `child`, `inputs`, `stages`, `pipeline` still pass without modification.
- Verify `MAX_STRING_LEN`, `MAX_ARRAY_SIZE`, `MAX_TREE_DEPTH` constants stay where they are (top of `JsonValidator.cpp`); making them runtime-configurable is out of scope.

## Non-goals

- **Not making the limits runtime-configurable.** `MAX_*` stay compile-time constants; a separate ticket can flip them to `Config` if/when needed.
- **Not adding type-aware schema validation.** The validator continues to reject only on length / depth / array-size / unknown node-type. No "this key must be a number" rules.
- **Not introducing a per-node validation hook.** The audit calls out the open-vocabulary issue specifically; per-type schemas are a much bigger conversation.
- **Not touching `validateNode`.** Its existing `NodeTypeRegistry` lookup is fine; this proposal only changes the structural walk.
- **No changes to `JsonValidator.hpp`'s public surface** beyond what's necessary for the implementation rewrite.

## Acceptance criteria

1. `git grep "stringFields\[\]" src/core/JsonValidator.cpp` returns no hits; the closed allowlist is gone.
2. A spec containing `"trade.notes": "<\(MAX_STRING_LEN+1\) chars\>"` (a key not in the original allowlist) is rejected by `validateTree` with an error message that includes `"trade.notes"`.
3. A spec containing `"trade.legs": [<MAX_ARRAY_SIZE+1 elements>]` is rejected with an error mentioning `"trade.legs"` and the size cap.
4. A spec nested via a non-builtin path `{"foo":{"bar":{"baz":…}}}` deeper than `MAX_TREE_DEPTH` is rejected at the cap.
5. Every existing test in `tests/validation/` (and any other validator test) passes without modification.
6. `validateTree` allocates no per-call vectors / heap state beyond what the existing recursion uses; the rewrite is iterative-on-members, not transformative.
7. `ctest --output-on-failure` green.

## Constraints

- **Performance:** The walk is O(N) over members, same as today. Recursion bounded by `MAX_TREE_DEPTH`. No regression expected; if anything the dispatch becomes slightly cheaper since we drop the per-key `HasMember` calls.
- **Compatibility:** Behavior on every existing built-in node spec is unchanged. The only observable difference is that previously-silent oversize values on connector keys now throw — that's the bug fix.
- **Dependencies:** None.
- **Deadline:** None.

## Affected systems / callers

- `src/core/JsonValidator.cpp` — body rewrite.
- `tests/validation/ValidationTest.cpp` — extend with three cases (oversize string under foreign key, oversize array under foreign key, deep-nested foreign path).

## Alternatives considered

- **Add a `JsonValidator::registerStringField(name)` registry that connectors call.** Rejected: just kicks the closed-vocabulary problem one level up. Connectors shouldn't have to enumerate every key they might use.
- **Keep the allowlist but expand it to include common connector keys.** Rejected: same problem, larger allowlist.
- **Move validation into per-node-type registered validators.** Bigger refactor; the audit wants the open-vocabulary fix specifically and doesn't ask for per-type validation.

## Risks

- **Stricter validation on previously-silent oversize fields could break existing pipelines.** Mitigation: the only such fields are connector-introduced ones; today they bypass validation entirely, so any breakage is uncovering a real overflow bug. The error message names the key for fast diagnosis.
- **Generic recursion could now descend into objects that were intentionally opaque.** Mitigation: there are none today — every "object" inside a tree is itself a node spec. If a future connector wants opaque metadata, it can use a string field (which gets length-checked but not parsed).

## Open questions

- None.
