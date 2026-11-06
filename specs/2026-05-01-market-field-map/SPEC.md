# SPEC: Move SourceProfile to market connector as MarketFieldMap

**Slug:** market-field-map
**Date:** 2026-05-01
**Status:** Approved
**Author:** ndrandal
**Repo:** krn

## Problem

`krn::SourceProfile` lives in the engine (`include/krn/SourceProfile.hpp`) and is embedded in `krn::util::Config` as `cfg.sourceProfile`, but every field on it is market-flavored: `priceFields`, `volumeFields`, `bidFields`, `askFields`, `taEnabled`. The struct's only product consumer is `MarketTA` in the market connector. The engine parses `source.*` INI keys directly. The contract audit calls this out as the residual vocabulary-leak (V6) — engine-level types and config keys that exist solely for one connector. Adding a second connector with its own field-mapping needs means today's design either grows a second engine type or reuses a name that doesn't fit.

## Proposed change

Move the struct out of the engine into the market connector and rename it: `krn::SourceProfile` → `krn::market::MarketFieldMap`, header at `connectors/market/include/krn/market/MarketFieldMap.hpp`. The market connector registers a `ConfigReaderFn` with `ConfigNamespaceRegistry` (built in ENC-30) that owns the new canonical prefix `market.source.*`; `MarketConnector` holds the populated `MarketFieldMap` as a member and threads it into the `MarketTickComputer` factory closure when registering with `EventComputerRegistry`. The engine's `Config` struct loses its `sourceProfile` field and stops parsing `source.*` keys entirely. For one release, the market connector also registers a deprecated `source` reader that forwards to the same handler with a logged warning so existing INI files keep working.

## Scope

- New header `connectors/market/include/krn/market/MarketFieldMap.hpp` containing `krn::market::MarketFieldMap` (same internal field names as today's `SourceProfile`).
- Delete `include/krn/SourceProfile.hpp`. Remove `#include "krn/SourceProfile.hpp"` and the `sourceProfile` member from `krn::util::Config`. Remove the `source.*` parsing branches from `krn::util::Config::loadFromFile`.
- `MarketConnector::registerWith` registers a `ConfigReaderFn` for the `market` namespace (matches keys `market.source.*`), parses the tail into a `MarketFieldMap` member, and registers a deprecated `source` namespace reader that forwards with a `Logger::Warn`. Calls `reg.cfg->loadConfigNamespace()` (or however ENC-30 surfaces post-load dispatch) so any keys already loaded route through.
- `MarketConnector` exposes the populated `MarketFieldMap` to its `tick` `EventComputerRegistry::registerFactory` closure so `MarketTickComputer` is constructed with the map.
- `MarketTA` / `MarketTickComputer` swaps its `SourceProfile _profile` member for `MarketFieldMap _fieldMap` (or whatever name fits — type change only, internal logic unchanged).
- Tests: move `tests/feed/SourceProfileTest.cpp` → `connectors/market/tests/MarketFieldMapTest.cpp` (or under existing `tests/connectors/`), updating includes and namespaces. Same for any `sourceProfile` references in `tests/feed/GapFixesTest.cpp`.
- `src/util/krn.conf` (default INI): rewrite `source.*` keys to `market.source.*`.
- CHANGELOG / `docs/CONNECTOR_REFACTOR.md`: note the rename + deprecation window.

## Non-goals

- **No internal field-name renames.** `priceFields`, `volumeFields`, `bidFields`, `askFields`, `timestampField`, `taEnabled` keep their current names. Disambiguating those (e.g. `tradePriceFields`) is a separate ticket.
- **No new fields on `MarketFieldMap`.** Pure move + rename, not a feature add.
- **Not removing the `taEnabled` toggle** even though it sits in market-flavored territory — that's the existing on/off control and stays.
- **Not implementing ENC-30.** This spec assumes ENC-30 has landed (`ConfigNamespaceRegistry` is wired through `Config::loadFromFile` and dispatches keys to registered readers). If ENC-30 isn't done, this spec's plan blocks.
- **Not auto-migrating user INI files.** Existing files using `source.*` get a deprecation warning at load, not a rewrite. Users update by hand within the one-release window.
- **Not changing the public API of `MarketTickComputer`'s constructor in any way other than the parameter type rename.** Behavior is identical.

## Acceptance criteria

1. `include/krn/SourceProfile.hpp` no longer exists; `connectors/market/include/krn/market/MarketFieldMap.hpp` exists. `git grep -nE "krn::SourceProfile|include.*SourceProfile.hpp"` returns no hits.
2. `krn::util::Config` has no `sourceProfile` field, and `krn::util::Config::loadFromFile` contains no `source.` string literal — the engine no longer recognizes the prefix.
3. `MarketConnector::registerWith` registers a `ConfigReaderFn` for the `market` namespace via `reg.configNs->registerNamespace("market", …)`. A new `MarketConfigDispatchTest` proves that loading a config containing `market.source.priceFields = a,b` populates `MarketConnector`'s field-map member with `{a, b}`.
4. The market `EventComputerRegistry::registerFactory("tick", …)` factory closure consumes the populated `MarketFieldMap` so each `MarketTickComputer` instance reflects per-`MarketConnector` configuration.
5. Loading an INI containing legacy `source.priceFields=…` succeeds, populates the same field-map, and emits exactly one `Logger::Warn` per unique deprecated prefix observed.
6. `ctest --output-on-failure` green: every existing test that referenced `cfg.sourceProfile` (notably `SourceProfileTest`, `GapFixesTest`'s bid/ask/spread case) passes after migration to the new type and namespace.
7. `krn_engine` static lib builds without including any connector header, including the deleted `SourceProfile.hpp` (engine-isolation invariant from connector-lifecycle still holds).

## Constraints

- **Performance:** No change. The map is read once at construction; runtime hot path is unchanged.
- **Compatibility:** Wire format / INI keys: `source.*` continues to work for one release with a deprecation warning, then drops. C++ public type `krn::SourceProfile` is **removed** without an alias — there are no external consumers of the engine library outside this repo, and the removal is what the audit explicitly asks for. The MarketTickComputer constructor parameter changes type but the call sites are inside the connector.
- **Dependencies:** **Hard dependency on ENC-30.** This spec cannot land before ENC-30 (ConfigNamespaceRegistry wiring) — without it, `Config::loadFromFile` can't hand `market.*` keys to a market-side reader.
- **Deadline:** None. Schedule after ENC-30 ships.

## Affected systems / callers

- `include/krn/SourceProfile.hpp` — deleted.
- `include/krn/util/Config.hpp` / `src/util/Config.cpp` — drop `sourceProfile` field + parser branches; drop `#include "krn/SourceProfile.hpp"`.
- `connectors/market/include/krn/market/MarketFieldMap.hpp` — new.
- `connectors/market/include/krn/MarketTA.hpp` / `connectors/market/src/MarketTA.cpp` — switch member type.
- `connectors/market/include/krn/market/MarketConnector.hpp` / `connectors/market/src/MarketConnector.cpp` — own a `MarketFieldMap` member; register `ConfigReaderFn`; thread the map into the tick factory closure.
- `tests/feed/SourceProfileTest.cpp` — move + update.
- `tests/feed/GapFixesTest.cpp` — switch the `cfg.sourceProfile.bidFields = …` setup to populate the new path (likely via the connector / via a test helper that constructs a `MarketFieldMap` directly).
- `src/util/krn.conf` — rewrite default keys.
- `docs/CONNECTOR_REFACTOR.md` and `CLAUDE.md` — note the new location + deprecation.

## Alternatives considered

- **Leave `SourceProfile` in `krn::` and just register a `ConfigReaderFn` that copies engine-side state into a market-side mirror.** Rejected: doesn't actually fix the audit's V6 complaint — engine still defines a market-flavored type. Half measures.
- **Move the type but keep `source.*` config keys as the canonical name.** Rejected: every other connector will want its own namespace too; an unprefixed `source.*` invites future collisions and contradicts the connector-pluggability story we just shipped.
- **Hard-break `source.*` immediately, no deprecation alias.** Rejected: trivial cost (a few lines on the connector for one release) buys a kinder migration for any persisted INI file.
- **Keep `krn::SourceProfile` as a `using` alias for one release for source compat.** Rejected: there are no external consumers of the engine lib, and the audit's whole point is removing the engine-side name. An alias would prolong the cleanup.

## Risks

- **Test fixture churn.** Several tests construct `util::Config` and reach into `cfg.sourceProfile` directly. Mitigation: a small test-only helper that builds a `MarketFieldMap` and threads it the same way the connector does, so test setup is one line.
- **ENC-30 surface area is unknown at spec time.** Mitigation: this spec lists what it expects from `ConfigNamespaceRegistry`; if ENC-30 ends up shipping a different dispatch surface, fold the adjustment into this proposal's plan rather than landing a contradictory shape.
- **Forgetting the deprecation warning.** Mitigation: AC5 makes the warning observable in tests.

## Open questions

- ENC-30's exact post-load dispatch surface — does `Config::loadFromFile` synchronously dispatch each unknown key to `ConfigNamespaceRegistry` as it parses, or does it collect unknowns and flush at the end? The `MarketConnector::registerWith` registration must run *before* the dispatch fires. Resolved during `/plan` once ENC-30 lands.
