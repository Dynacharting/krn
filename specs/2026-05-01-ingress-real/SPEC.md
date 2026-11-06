# SPEC: Make IngressRegistry real

**Slug:** ingress-real
**Date:** 2026-05-01
**Status:** Approved
**Author:** ndrandal
**Repo:** krn

## Problem

`engine::IngressRegistry` is dead scaffolding: it has zero `registerIngress` calls in product code, and `MarketConnector::registerWith` directly news up `FeedServer` + `WsFeedClient` with raw `Dispatcher*` pointers. The audit's V3/V19 thread argues the engine should own the ingress lifecycle uniformly — connectors register *factories*, the engine instantiates and drives them. Today, every new ingress kind (a Coinbase WS client, a file replayer) duplicates the connector-side construction loop and its own ad-hoc start/stop wiring.

## Proposed change

Engine owns ingress. Connectors register named `IngressFactory`s with `IngressRegistry` from `registerWith`. The composition root reads `ingress.N.kind` and `ingress.N.*` from `Config`, looks each name up in `IngressRegistry`, invokes the factory to obtain an `IIngressSource`, and calls `start()` / `stop()` on each as part of the existing IConnector lifecycle hooks (or a new `connectors-stop`-style step). `MarketConnector` registers two factories — `market.feedserver` and `market.wsclient` — and stops constructing those types itself; instead the connector's `start()` becomes a thin shell that no longer needs to touch sockets.

## Scope

- New ingress factory parameter: `IngressFactory` signature changes to take `EngineRegistries&` (so factories can read namespaced config + engine pieces uniformly), instead of `(io_context&, Dispatcher*, Config&)`.
- `MarketConnector::registerWith` registers two named factories on `reg.ingress`:
  - `market.feedserver` — wraps the existing `FeedServer` and adapts its `run()`/`stop()` to `IIngressSource::start()`/`stop()`.
  - `market.wsclient` — wraps `WsFeedClient`. The factory is invoked once per parsed `ingress.N.kind = market.wsclient` block; per-instance config (URL, adapter, symbols) read from `ingress.N.*` keys via the connector's `ConfigReaderFn`.
- New engine-side ingress driver in the composition root (`main.cpp`): after `dispatchPendingKeys`, iterate `cfg.ingress[]`, build each via `IngressRegistry::find(kind)`, push into `std::vector<std::unique_ptr<IIngressSource>>`, start them in registration order, register a single `ingress-stop` shutdown step at priority 35 that stops them in reverse order.
- `Config` gains a strongly typed `ingress` vector of `{kind, name, params: map<string,string>}` populated from `ingress.N.*` keys via either an engine-owned parser or a generic dispatch. Pick the same pattern used for the existing `feeds.N.*` keys to keep things consistent — minor refactor.
- `MarketConnector::start()` no longer constructs or runs `FeedServer` / `WsFeedClient`. Its responsibility shrinks to applying `allowNegativePrices` to the `OrderBookManager` and any other connector-only state. `MarketConnector::stop()` similarly drops feed-server and ws-client teardown — those are now driven by `ingress-stop`.
- Update `krn.conf` defaults: encode the current implicit feed server as `ingress.0.kind = market.feedserver`, `ingress.0.port = 9001` (or read from `feedPort`).
- Update `tests/test_bootstrap.cpp`: registerWith remains; no live ingress sources are constructed because no `ingress.N.*` keys are present in the default test Config.
- New `IngressFactoryDispatchTest`: register a fake factory under a unique name, populate `cfg.ingress[]` synthetically, run the engine-side driver loop (extracted into a helper), assert exactly one `IIngressSource` was built with the expected params and that `start()` / `stop()` were called in order.

## Non-goals

- **Not converting the `WebSocketServer` (the public WS API server) into an `IIngressSource`.** That's a different layer — clients call in, not pushed events. Keep its existing direct construction in `main.cpp`.
- **No automatic reconnection / circuit-breaking** added to the new lifecycle. Whatever `WsFeedClient` does today is preserved verbatim.
- **No support for hot-reloading `ingress.N.*` keys** — the list is read once at boot.
- **Not migrating the `feeds.N.*` config namespace yet.** Existing INI files using `feeds.N.url` keep working via the existing `Config` parser; new canonical is `ingress.N.kind = market.wsclient` with `ingress.N.url = …`. One-release deprecation alias mirrors ENC-35's pattern.
- **Not changing `Dispatcher*` in `IIngressSource`'s factory closures.** Factories receive the dispatcher via `EngineRegistries`; the existing pointer plumbing stays. (`Dispatcher` rename to a more neutral `EventBus` is a separate ticket.)
- **Not implementing `binance.*` or any second-connector ingress.** This proposal proves the path with the existing market sources only.

## Acceptance criteria

1. `engine::IngressRegistry` has at least two registered factories at boot under deterministic names (`market.feedserver`, `market.wsclient`), confirmed by `IngressRegistry::kinds().size() >= 2` after `MarketConnector::registerWith`.
2. `MarketConnector::start()` source contains zero references to `FeedServer::run` and `WsFeedClient::start`. `MarketConnector::stop()` likewise contains no calls into those types.
3. Loading an INI with `ingress.0.kind = market.feedserver, ingress.0.port = 9100` plus `ingress.1.kind = market.wsclient, ingress.1.url = ws://x` produces two `IIngressSource` instances after the engine-side driver runs, both of which receive `start()` exactly once.
4. The new `ingress-stop` shutdown step at priority 35 stops sources in reverse-registration order; verified by a test using two trivial mock `IIngressSource`s recording call order.
5. Existing `krn_server` boot path still listens on the configured `feedPort` end-to-end (smoke test or `ss -tln`).
6. `ctest --output-on-failure` green; no test that was passing before this ticket regresses.
7. `IngressFactory`'s typedef takes a single `EngineRegistries&` argument; the legacy three-arg signature is deleted.

## Constraints

- **Performance:** zero hot-path impact — ingress sources start once and run independently.
- **Compatibility:** `feeds.N.*` keys keep working for one release with a `Logger::Warn` per occurrence; canonical is `ingress.N.*`. The C++ public type `engine::IngressFactory` changes signature — a hard break, no alias (only one in-tree user).
- **Dependencies:** Hard dependency on **ENC-30** (already In Review). Soft dependency on the eventual market `ConfigReaderFn` (introduced via ENC-35) for routing the `ingress.N.*` keys; if ENC-35 lands first this proposal piggybacks on its reader. If not, this proposal adds a parallel reader and ENC-35 absorbs it later.
- **Deadline:** none.

## Affected systems / callers

- `include/krn/engine/IngressRegistry.hpp` — `IngressFactory` signature change.
- `include/krn/engine/IConnector.hpp` — no API change; lifecycle docs gain a one-line note that ingress lifecycle is engine-driven.
- `connectors/market/src/MarketConnector.cpp` — register factories; shrink `start()`/`stop()`.
- `connectors/market/include/krn/server/FeedServer.hpp` + `connectors/market/include/krn/ws/WsFeedClient.hpp` — adapt to `IIngressSource` (small wrapper class or direct inheritance).
- `src/main.cpp` — new engine-side driver loop; new `ingress-stop` step.
- `include/krn/util/Config.hpp` + `src/util/Config.cpp` — `ingress` vector field + parsing.
- `src/util/krn.conf` — default `ingress.*` block.
- `tests/test_bootstrap.cpp` — no change beyond ensuring registerWith still works.
- New `tests/engine/IngressFactoryDispatchTest.cpp`.

## Alternatives considered

- **Option B: connector-owned, IIngressSource as interface only.** Connector keeps building sources directly; `IIngressSource` is just a contract for the existing `start/stop` shape. Rejected: leaves `IngressRegistry` as dead scaffolding (the audit's exact complaint) and offers no path to config-driven ingress addition.
- **Option C: hybrid where `IngressRegistry::registerIngress` immediately constructs a default instance.** Rejected: conflates registration with instantiation, and the registry would need to know which engine handles to pass — the same tangle we're untangling.
- **Keep `IngressFactory(io_context&, Dispatcher*, Config&)` and just add usage.** Rejected: factories will need access to the namespaced config readers, the logger, and the shutdown coordinator — passing a single `EngineRegistries&` matches what every other extension point already does.

## Risks

- **Behavior drift in feed clients.** `WsFeedClient::start` today happens during connector `registerWith`; under this design it's deferred to engine-side driver. If anything was implicitly relying on the eager start, it'll surface. Mitigation: integration smoke test that connects to a mock feed and asserts events arrive.
- **Config parsing creep.** Adding `ingress.*` to `Config` re-introduces market-flavored parsing in the engine — same anti-pattern ENC-30 was fighting. Mitigation: parse `ingress.N.kind` (engine knows the structure) but leave `ingress.N.*` other keys to be retrieved by the factory through `EngineRegistries`'s config-namespace surface, so connectors own their own per-kind keys.

## Open questions

- Exact mechanism for per-ingress config keys: a `std::map<string,string>` blob on each `Config::Ingress{}` entry, or factories read from a `ConfigReaderFn` registered for the kind's namespace? Resolved in `/plan` once the parser shape is confirmed against krn.conf samples.
