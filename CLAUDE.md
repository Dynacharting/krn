# CLAUDE.md — KRN

## Project Overview

KRN is a high-performance C++20 WebSocket server and library for real-time atomic analysis computations over streaming data. Clients submit JSON-encoded request trees that execute nested statistical operations asynchronously.

The codebase is organized as a **domain-agnostic core engine** plus **pluggable connectors**. This repo holds the engine, its composition `krn_host`, the connector-free `krn_engine_server` and the reference plugin. The market connector and `krn_server` live in [krn-common](https://github.com/Dynacharting/krn-common), the data layer in [krn-data](https://github.com/Dynacharting/krn-data); both consume this repo through `FetchContent`. Adding a new data source means writing a new connector with no changes to the engine.

For the deeper architectural picture, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Build & Run

```bash
# Build (Release)
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)

# Build with tests
cmake .. -DCMAKE_BUILD_TYPE=Debug -DKRN_BUILD_TESTS=ON
cmake --build . -j$(nproc)

# Build via script (prefers clang++)
./tools/compile.sh

# Run server: krn_host + the reference plugin (streams REF and SYN)
./build/krn_engine_server                  # compiled defaults (wsPort=8080)
./build/krn_engine_server 9002 my.conf     # argv[1]=wsPort wins over the INI file

# Run tests
cd build && ctest --output-on-failure
```

**An empty `CMAKE_BUILD_TYPE` defaults to `Release` (ENC-1548)** on a single-config generator,
top-level only, so an untyped configure still gets `-O3` and LTO. An explicit
`-DCMAKE_BUILD_TYPE` always wins, and a multi-config generator or a consumer is left alone.

**Consuming the engine (ENC-1553).** Under `FetchContent` / `add_subdirectory` the root
`CMakeLists.txt` defines only the engine library targets (`krn_engine`, `krn_host`,
aliases `krn::krn_engine` / `krn::krn_host`, and the reference connector) — no tests,
server, install rules or isolation gate, whatever the consumer's cache says, and without
writing to that cache. `cmake --install` plus `find_package(krn CONFIG)` gives
`krn::krn_engine` and `krn::krn_host`. Both are pinned by the ctest cases `krn_consumer_fetchcontent_smoke`
(which checks the **committed** `HEAD`, fetched over `file://`) and `krn_consumer_install_smoke`.

## Project Structure

```
include/krn/              # Engine public headers (libkrn_engine)
  engine/                 # Engine-side contracts & registries
                          #   IConnector, EngineRegistries, IEventComputer,
                          #   EventTypeRegistry, EventComputerRegistry,
                          #   NodeTypeRegistry, IngressRegistry,
                          #   ConfigNamespaceRegistry
  nodes/                  # INode, Listener, Worker, Aggregate, Interval,
                          # AtomicAccessor, Responder, GroupSplit
  server/                 # WebSocketServer, ClientSession
  ws/                     # WebSocket bridge/responder
  rt/                     # ThreadPool, SPSCQueue
  runtime/                # ShutdownCoordinator
  util/                   # Config, Logger, Metrics
  atomic/                 # AtomicProviderRegistry
  Dispatcher.hpp          # Generic event-routing hub
  Event.hpp               # Canonical {type, symbol, payload} event (the `symbol`
                          # field is an opaque streamKey internally; WS payloads
                          # use "streamKey" as the JSON key — ENC-50)
  StreamValue.hpp         # ArgType + pipeline value
  AtomicStore.hpp         # Thread-safe (symbol, field) -> ArgType store
  FunctionMap.hpp         # Named worker-function registry
  FunctionRegistry.hpp    # registerBuiltinFunctions()
  NodeRegistry.hpp        # registerBuiltinNodeTypes()
  TreeBuilder.hpp         # JSON → node DAG
  host/Host.hpp           # krn_host: HostOptions, runHost(), hostMain()

src/                      # Engine implementations (mirrors include/ layout)
  host/Host.cpp           # krn_host: the engine composition (ENC-1559)

apps/engine_server/       # krn_engine_server: krn_host + the reference plugin

plugins/reference/        # libkrn_plugin_reference
  include/krn/reference/  # ReferenceComputer, ReferenceConnector, SyntheticConnector
  src/

tests/                    # GoogleTest suites (krn_tests) + scripts/ + consumer/
  engine/                 # Registry tests
  connectors/             # SyntheticConnectorTest
  (core/ dispatch/ feed/ integration/ nodes/ registry/ treebuilder/
   validation/ ws/ … — engine tests)
  consumer/               # FetchContent / find_package smoke projects
  test_bootstrap.cpp      # krn_tests' gtest Environment — builtins only

tools/                    # Python/shell utilities (compile.sh, mapping, todo_scan)
docs/
  ARCHITECTURE.md         # Deeper architecture reference
  CONNECTOR_REFACTOR.md   # Engine/connector split — historical plan
```

## Key Architecture (1-paragraph orientation)

A connector registers itself at boot via `MyConnector::registerWith(EngineRegistries&)`. The `Dispatcher` routes inbound `Event`s by their `type` field to per-dispatcher `IEventComputer` instances (supplied by connectors). Listeners subscribe on `(streamKey, field)` and receive `StreamValue`s from direct event fields (via `Dispatcher`) or from computer-written atomics (via `Dispatcher::notifyListeners`). JSON request trees are built through `TreeBuilder`, which looks up node constructors in `NodeTypeRegistry`; worker math names resolve via `FunctionMap`. The wire-format JSON key is `streamKey` everywhere — no `symbol` alias is accepted (ENC-50).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full picture including the event lifecycle, connector contract, and a step-by-step guide for adding a new connector.

## Tech Stack

- **C++20** (`-DCMAKE_CXX_STANDARD=20`)
- **CMake 3.20+** — produces `libkrn_engine.a`, `libkrn_host.a`, `libkrn_plugin_reference.a`, `krn_engine_server`, `krn_tests`
- **Boost.Asio / Beast** — networking + WebSocket
- **RapidJSON** — JSON parsing and validation
- **GoogleTest** — unit + integration tests

## Code Conventions

- Engine code under `include/krn/` + `src/`; plugin code under `plugins/<name>/`
- Engine CMake target (`krn_engine`) must not depend on any connector headers
- **`krn_engine` must not depend on Arrow, the ClickHouse client, or `krn_data` (ENC-1481).**
  `krn_data` depends on `krn_engine`, never the reverse. This is a shipping constraint, not
  taste — and the constraint is about **`krn_engine`**, not about the product. The
  data-layer SPEC's D14 client profile reads *"live-only **or** reads a central L1 over the
  wire with no local store"*: the **live-only** variant omits the library, so
  `krn_host` and `krn_engine_server` must build and link with no Arrow anywhere,
  while the over-the-wire variant needs Arrow IPC and therefore does ship `krn_data`. Don't
  shorten that to "the client profile omits the library" — that is true of one variant.
  **The engine names no record type** (ENC-1557): `Dispatcher::onRecord` takes a
  `data::SymbolId` and an `engine::FieldSource`, and the record PODs and their adapter are in
  `krn_data` (krn-data's `data/include/krn/data/{TickRecord,RecordFieldSource}.hpp`). Only
  `include/krn/engine/SymbolTable.hpp` is still engine-side, Arrow-free. Every
  way of breaking the invariant still compiles and still passes every other test, so it is
  asserted by `tools/check_engine_isolation.py` as the ctest cases `krn_engine_isolation`
  (checks A–E; check E's market vocabulary is regenerated from git history by `tools/derive_market_vocabulary.py` and has a fixture control; refuses to pass on vacuous input) and `krn_engine_isolation_selftest` (runs
  each predicate against an input that violates it). Run them before claiming a layering
  change is safe; `--selftest` needs no build. **Checks C and D walk the transitive link
  closure**, because `krn_engine_server` links only `krn_host` and the plugin: one
  `target_link_libraries(krn_host PUBLIC krn_data)` breaks the live-only build while the
  server's own link property stays clean. CMake emits a `link.<target>` manifest row per
  first-party target — add one when you add a library target. **The positive control is
  `krn_isolation_control`** (ENC-1507), a scan-only target in the root `CMakeLists.txt` that
  exists in every configuration and is the gate's only control. The gate exits 0 pass,
  1 violation, 2 could not run, and a missing or silent control is 2. Never link the control
  into a real target; the gate fails if anything does.
  **Its roots are `krn_engine`, `krn_host` and `krn_engine_server`** (ENC-1561; krn-common's `krn_server`
  left in ENC-1570); a missing root is a configure error under `KRN_BUILD_TESTS=ON`,
  and no graphed target may reach `krn_connector_market`. `apps/` may include `plugins/*`
  headers but not `connectors/*` ones; with no `connectors/` here that list is empty on purpose.
- Namespace: `krn::` for public types, `krn::engine::` for engine contracts, `krn::market::` / `krn::synthetic::` for connector code
- Globals: `krn::gThreadPool` (shared_ptr<ThreadPool>, set up in `src/host/Host.cpp`)
- Runtime config: INI-style key=value files (krn-common's `conf/krn.conf` is the market example)
- Prefer lock-free / fine-grained locking (shared_mutex, SPSCQueue)
- Test files named `<Component>Test.cpp` under `tests/<category>/`; engine cases build into `krn_tests`, which links no market code; a case that needs the market connector belongs in krn-common (ENC-1562, ENC-1570)
- **A test that writes to a process-global registry must derive its key per invocation (ENC-1102).** `FunctionMap`, `NodeTypeRegistry`, `EventComputerRegistry`, `EventTypeRegistry`, `IngressRegistry`, `ProtocolIngressRegistry`, `AtomicProviderRegistry` and `ConfigNamespaceRegistry` are process-wide singletons with no per-entry removal, and `--gtest_repeat` re-runs every test body in the **same process**. A test that registers a *fixed* name therefore meets, on iteration 2, a registry its own iteration-1 run already populated — `registerNodeType` returns false, `EventComputerRegistry` reports double the factories, `FunctionMap::getAll()` does not grow. Five tests failed exactly this way. Build the name from a file-local `static std::atomic` counter (`uniqueKey()` in `tests/engine/RegistriesTest.cpp`, `uniqueType()` in `tests/engine/EventComputerCacheTest.cpp`) instead. This is a test-lifecycle fix only — the registries' production duplicate semantics (first-wins for node/event/ingress types, append for event computers, replace for functions/providers) are deliberate and unchanged.
- **`--gtest_repeat` must be run against the FULL unsharded binary**, never with `--gtest_filter`. ENC-1065 requires the full binary because cross-suite corruption surfaces dozens of suites later; filtering a repeat run is what hid ENC-1102 for months, since every repeat check in the project happened to be filtered.
- Connectors implement the strict `IConnector` lifecycle: `registerWith()` allocates and registers (no live sockets/timers), `start()` brings sources online, `stop()` noexcept tears down in reverse order. `krn_host` drives all three; never wire your own `ShutdownCoordinator` step from inside a connector.
- **Ingress sources are engine-owned (ENC-31).** Connectors register named factories on `reg.ingress` (e.g. krn-common's `market.feedserver`, `market.wsclient`); `krn_host` reads `cfg.ingress[]` and instantiates them. Adding a new ingress kind is a factory registration + INI edit, not a `krn_host` change. A forum connector row becomes an ingress only if a plugin registered its `protocol` on `reg.protocols` (`ProtocolIngressRegistry`, ENC-1574); an unregistered protocol is logged and the row skipped, with no default kind.
- **WS request keys — int vs string (`RequestKey`).** The subscribe / cancel / value-emit code paths in `ClientSession` and `Responder` use `krn::server::RequestKey = std::variant<int, std::string>` (header at `include/krn/server/RequestKey.hpp`). Inbound subscribe accepts `{key:<int>}`, `{id:<int>}` (legacy), or `{id:"<string>"}`; outbound frames render `"key":<int>` or `"requestId":"<string>"` to mirror. Engine internals (Dispatcher, AtomicStore, TreeBuilder, Listener) stay key-type-agnostic — they route on `(streamKey, field)`, not on the request id. See krn-common's [`docs/atomic-keys.md`](https://github.com/Dynacharting/krn-common/blob/main/docs/atomic-keys.md#subscribe-request-key--int-vs-string-requestkey) §"Subscribe request key — int vs string".
- **Atomic-key namespaces — bare vs `ob.*` (ENC-94, ENC-101).** Two distinct namespaces by source (both written by krn-common's market connector): bare (`bid`, `ask`, `lastPrice`, sma_N, ...) is written by `MarketTickComputer` only when the tick payload carries the field directly (pre-aggregated tick connectors). `ob.*` (`ob.best.bid.price`, `ob.spread`, ...) is computed from `OrderBookManager` state — used for L2/L3 sources (ITCH, FIX). **Listeners may bind only to bare keys; `ob.*` is pipeline-only** — `ob::Provider` never calls `Dispatcher::notifyListeners`, so a `Listener` bound to an `ob.*` field would silently never fire. The reject lives in `nodes::Listener::Create` (the static factory; the public constructor is kept for unit-test fixtures only) and surfaces as a `{"type":"error","where":"build","message":"listener: field '...' is pipeline-only — see docs/atomic-keys.md..."}` WS frame. Canonical pattern for surfacing `ob.*` into a chart: `Listener(<bare-key clock>) → AtomicAccessor(ob.*) → ...` — see krn-common's [`docs/atomic-keys.md`](https://github.com/Dynacharting/krn-common/blob/main/docs/atomic-keys.md) for the worked NEXO example.
- **A timer node posts; it does not call. Drain the pool before you sample (ENC-1340).**
  `BucketTime` and `Interval` hand each tick to the `ThreadPool` with `post()`. `shutdown()`
  joins the timer thread, so after it returns no *new* tick can be posted — but a tick already
  posted is sitting in the queue, unexecuted. A test that samples the child's counter straight
  after `shutdown()` misses it, the pool's worker runs it a moment later, and the test goes red
  with the shutdown having worked perfectly. It is load-sensitive because the only thing
  deciding the outcome is whether the worker got scheduled first, which is why it reads as
  "flaky" rather than "wrong": `BucketTimeTest.ShutdownStopsTicks` failed 8 times in 200 runs
  (4.0%) with a single core oversubscribed, always `after == before + 1`, and 0 times in 200 on
  a quiet box. **`pool.drain()` after `shutdown()`** is the fix and it is deterministic — it
  waits for the queue to empty *and* for in-flight tasks to finish. A longer `sleep` is not a
  fix, it is a wider window. Pair it with a bounded poll for the *first* tick instead of a fixed
  leading sleep, so a slow box delays the test rather than making it assert `0 == 0`.
- **`AtomicStore` readers stand down for a queued writer, and the bound is 1 yield (ENC-1340).**
  `std::shared_mutex` is a reader-preferring `pthread_rwlock_t` on glibc, so overlapping readers
  can starve a writer with no bound. `AtomicStore::set`/`setBatch` publish `_writersQueued`
  before blocking and `get()` yields once while it is non-zero. This was worth 25% of runs of
  `ConcurrencyContentionTest.MultiReaderMultiWriterNoTornReads` going bimodal — 30 of 40 runs at
  ~1.2 s and 10 of 40 from 2.8 s to past a 25 s cap, with 71 s seen uncapped. **Do not raise the
  yield bound to "make it faster".** Higher values look better on wall clock by making the
  readers stop reading: at 64 yields the test finished in 14 ms having performed 2,335 reads
  against 80,000 writes — a concurrency test silently converted into a no-op. The test now
  records its reader throughput and asserts a floor for that reason; the measured sweep is the
  table in `src/core/AtomicStore.cpp`.
- **Two Dispatcher ingress shapes, one body (ENC-1486).** `Dispatcher::onTick(const Event&)`
  (JSON DOM — the legacy TCP feed and `market.wsclient`) and
  `Dispatcher::onRecord(data::SymbolId, const engine::FieldSource&)` (resolved fields, no
  DOM; krn_data builds the source from a `TickRecord` with `RecordFieldSource`) are both
  three-line adapters over the private
  `ingest(symbol, eventType, const engine::FieldSource&)`. The raw-admission ledger, the
  history buffers, `computeAndStoreAtomics`, the `AtomicStore` keys, the computer sets and
  `deliver()`'s ordering are therefore **one** implementation reached two ways — SPEC
  `specs/2026-10-14-krn-data-layer/SPEC.md` D1, and the reason **a graph cannot tell which
  entry point fed it** is structural rather than asserted. Three things follow and each has
  a sharp edge: **(1)** `IEventComputer::compute` takes a `const engine::FieldSource&`, not
  an `Event` — a record overload would be a second copy of every connector's field
  extraction, free to drift; read inputs with `fields.tryGetNumber(name, out)` and a
  computer works on both paths. `computeEvent(const Event&)` is a non-virtual DOM shim for
  direct callers (connector unit tests), not what the Dispatcher calls. **(2)** `onRecord`
  needs `setSymbolTable()`; without a table, or for an id the table never issued, it is a
  **no-op** — a record names only an interned `symbolId`, and routing under `""` would be a
  silent wrong answer. The `symbolId` → ticker resolution is a deliberate heap copy per
  record (`StreamValue` still carries `std::string symbol`); do not remove it without
  ENC-1487's numbers, which measure it as a separate step. **(3)** No event time:
  `header.tsEventNs` is not exposed as a field and the clock nodes are untouched (SPEC D9).
  Mapping table and the reason for each omission: krn-data's `data/include/krn/data/RecordFieldSource.hpp`.
  Pinned by krn-data's `data/tests/RecordIngressParityTest.cpp` (in `krn_data_tests`; the engine-only
  JSON-free and refusal cases are `tests/dispatch/RecordIngressEngineTest.cpp`), which compares deliveries **in
  order** and store contents across the two paths, carries its own negative controls, and
  separately pins the record→field mapping absolutely — the parity half cannot see a bug
  *inside* the mapping, because it derives the equivalent JSON tick through it.
- **Derived-builtin key shape — `atomicKeyNamespaceByField` (ENC-1008).** `Dispatcher::computeAndStoreAtomics` stores FunctionMap builtins (`mean`, `sum`, `stddev`, … — 56 names) under the **bare** function name by default. That is a single per-symbol slot shared by every field, so two fields of one symbol both driving `mean` overwrite each other and one value is silently lost. Setting `atomicKeyNamespaceByField = true` stores them as `<field>.<fn>` (`lastPrice.mean`) so each source field keeps its own. **Default off**, because the key is a client-supplied wire string — `field` in a WS subscribe is read verbatim by `TreeBuilder` for both `Listener` and `AtomicAccessor` — with no version negotiation. Flipping it breaks an `AtomicAccessor` bound to a bare builtin name — loudly for the 53 names nothing else writes, and **silently for `mean`/`median`/`spread`**, which `MarketTickComputer` also writes over price history and which therefore keep resolving to a *different* value. Listener **push is unchanged** in both states, and outbound WS frames carry no key string at all. Both states are pinned by `tests/dispatch/AtomicKeyNamespaceTest.cpp`; the migration checklist is in krn-common's [`docs/atomic-keys.md`](https://github.com/Dynacharting/krn-common/blob/main/docs/atomic-keys.md#derived-atomic-key-shape--atomickeynamespacebyfield-enc-1008).

## Configuration

Two layers, and they intentionally differ — keep them straight:

1. **Compiled-in defaults** (`include/krn/util/Config.hpp`) — what a server
   uses when **no** config file is passed.
2. **Shipped INI** (krn-common's `conf/krn.conf`) — the market example config;
   it overrides some compiled defaults when you pass it as `argv[2]`.

| Key | Compiled default (`Config.hpp`) | `krn.conf` value |
|---|---|---|
| `wsPort` | **8080** | 4000 |
| `threadPoolSize` | 0 (= hardware_concurrency) | 4 |
| `taHistoryMax` | 1000 | 1000 |
| `metricsEnabled` / `metricsIntervalSec` | false / 15 | true / 15 |
| `logLevel` | `info` | `info` |

The market keys (`feedPort`, the TA periods, the legacy feed keys, `allowNegativePrices`) are not engine keys
(ENC-1574); krn-common's connector owns them.

A key that no engine field, `ConfigNamespaceRegistry` reader or `Config::markConsumed()`
call claims is logged once as `WARN config.unclaimed_key key=… file=…` and ignored; startup
never fails on it (ENC-1595). A plugin that re-reads the file marks its keys from a hook.

Other engine keys parsed by `src/util/Config.cpp` (defaults from `Config.hpp`):

- **Memory bounds:** `maxSymbols` (10000), `maxFieldsPerSymbol` (200).
- **Derived-atomic key shape:** `atomicKeyNamespaceByField` (false) — see the
  atomic-key namespaces bullet under *Code Conventions*.
- **Ingress (ENC-31, the current model):** `ingress.N.kind` plus per-entry
  sub-keys (`ingress.N.port`, `ingress.N.url`, `ingress.N.adapter`,
  `ingress.N.symbols`, …). The engine instantiates each entry by the kind a
  plugin registered.
- **forum-driven ingress:** `forumUrl`, `forumTenantId`, `forumAuthToken` —
  when `forumUrl` is set, ingress is pulled from forum instead of the static
  INI list.
- **Connector-namespaced config** (handled by `ConfigNamespaceRegistry`, not the
  engine directly): `market.source.*` → `MarketFieldMap` (legacy bare `source.*`
  is a one-release alias). See krn-common's `docs/feed-adapters.md`.

CLI override order: `argv[1]=wsPort`, `argv[2]=configFile`; krn-common's `krn_server` adds `argv[3]=feedPort`.
Values from argv win over the config file; the config file wins over the
compiled defaults.

## Design records (the umbrella D13 pointers)

Six design records at the **workspace root** decide things about this repo. They are *not* in
this checkout: each path below is relative to the workspace directory that holds `GMA_V3/`
alongside `forum/`, `embassy/` and `treaty/` — e.g. `../specs/<dir>/SPEC.md` from here. No
proposal directory goes into a service repo, so these one-line pointers are the only thing that
makes them reachable from the code they govern (`specs/2026-09-14-gsd-proposal-backfill/SPEC.md`
§D13, §D14). Each line names the path **and what that record decides for GMA_V3 specifically**.
Installed by **ENC-1371**, which supersedes the six one-line tickets ENC-1233, ENC-1166,
ENC-1164, ENC-1163, ENC-1155 and ENC-1152 — one per bullet, in the order they appear.

- **`specs/2026-09-16-krn-code-review-remediation/SPEC.md` — §2 D1–D10, §3, §5 Q1–Q3** (ENC-1233). The only *wholly* GMA_V3 record of the six: all ten locked decisions and all 24 tickets are this repo's, shipped as `34b2cf4` (63 files, +2790/−789) and amended by `63bd705` (ENC-1008, D5's deferred half) and `c80ddad` (ENC-1069, which struck D9's CI half). D1 a trade print is book-neutral **by declaration** and the book is mutated exactly once · D2 one evaluator for the `ob.*` key grammar (`parseObKey` + `ObMaterializer::eval`) · D3 one stream, one strand — an Asio object is touched only from its own executor · D4 the Dispatcher publishes a `CONCURRENCY CONTRACT` in its public header placing thread-safety on `IEventComputer` implementors instead of widening the lock · D5 the atomic key stays flat and the single-field assumption is documented, not encoded — **amended, not reversed**, by `atomicKeyNamespaceByField` · D6 work and memory are bounded by *demand*, before the write, at every independently-constructible layer · D7 a fixed, written lock order, `symbolLock_` → `OrderBook::m_`, never the reverse · D8 one definition per fully-qualified name · D9 a portable, reproducible default build with every departure a named option defaulting off — **the CI-matrix half is struck**, `c80ddad` deleted it · D10 a missing fixture is a `FAIL()`, not a skip. §3 carries the eleventh decision §2 deliberately delegates to it (M14 / ENC-795: an empty post-pull ingress list is fatal, `std::exit(1)`). **Always write `§2 D1–D10`** — a bare `D1–D10` collides with the same review's ten *documentation-finding* ids. Live for anyone editing this tree: §5 **Q1** (the TSan/ASan matrix behind D3/D4/D7 no longer exists), **Q2** (D7's precondition is enforced only by a parenthesis in the lock-order comment in krn-common's `connectors/market/src/book/OrderBookManager.cpp` — the SPEC cites it as `:191`, which has already drifted; the text is *"Delta handlers must not synchronously re-enter a mutation on the same symbol"*), **Q3** (no shared typed-getter rule was installed, so the next feed adapter starts from zero).
- **`specs/2026-09-14-local-data-plane/SPEC.md` — §2 D3–D9, §3** (ENC-1166). Seven of its ten decisions govern this repo and **none of the work is written yet**; §3's GMA_V3 row is the owed-work list. D3 the first crypto source is Bitcoin on-chain metrics through a local `bitcoind` shim, no exchange price feed and Hyperliquid deferred — so **V1 ships with no price data of any kind** · D4 do not emit canonical JSON-ITCH (ENC-1016 superseded); GMA gets a small on-chain adapter instead, which promotes a real adapter selector (krn-common's `connectors/market/src/MarketConnector.cpp:213-214`, ENC-1022) to a hard prerequisite · D5 the local wsserver is architecturally required, not a convenience, and the GMA-side `translate()` is a thin JSON→`TickEvent` map — the existing `market.wsclient` ingress suffices, so ENC-1009 (`external.push`) is explicitly **not** needed · D6 embassy compiles but **GMA carries** the DynaCharting records (~50 lines of route-table consumer, zero scene knowledge; GMA packing natively was rejected) · D7 transport is WSS with Caddy terminating TLS **in front of** this server, which is plaintext and text-framed today · D8 a forum-minted session ticket and **no JWT verification in C++**, plus a second, browser-facing port that must refuse `pipeline` / `stages` / `node` · D9 seeding is GMA-served history-then-live on subscribe with client-side resume-from-last-offset, which makes historical replay load-bearing. Read D8 with §5 **Q4**: its V1 half shipped with Ed25519 verification, expiry handling and a redeemed-`jti` set **in C++** — the thing D8 existed to prevent.
- **`specs/2026-09-14-krn-flow-control-replay/SPEC.md` — §2 D1–D10 (D4 **overturned**, see that document's `## Corrections` C1), §3, §5** (ENC-1164). The other single-repo record — all 18 tickets are `[GMA_V3]` — and the reason its §2 *cites* `docs/ARCHITECTURE.md` §8, `docs/atomic-keys.md` (now krn-common's) and **lines 124–125 of this file** rather than restating them. D1 outbound flow control is coalesce-latest above a 256-frame watermark keyed `(subscription instance id, streamKey)`; credit-based was rejected · D2 at the hard 4,096 bound a value frame displaces the **oldest** queued value, so drops are stale-first at every depth (residual shed-newest is counted as `ws.outbox_shed_newest`) · D3 a single stream can never reach `MAX_OUTBOX_SIZE` — lossless protocol traffic behind a stalled socket is what fills it, which is why the covering test drives the bound with batched `canceled` acks · ~~D4~~ **overturned 2026-09-20**: cross-tick ordering is no longer "not a guarantee", it is one `asio::strand` per request DAG — see `specs/2026-09-20-krn-join-correctness/SPEC.md` D3/D4 · D5 injected values are raw-written to the `AtomicStore` rather than gated on a registered `Listener`, bounded by a separate ledger against the same `maxSymbols` / `maxFieldsPerSymbol` budget · D6 derived atomics namespace as `<field>.<fn>` behind `atomicKeyNamespaceByField`, **defaulting off**, because the key is an inbound client contract stored unmigratably in forum · D7 a timer thread must never own its node (`Interval`, `TumblingWindow`, `BucketTime`) · D8 the two verification rules written at lines 124–125 above · D9 a stale run log is deleted, not regenerated · D10 (ruled, unshipped) do not call the build plan's §8.1 values "atomics". All of §5 is GMA_V3-scoped — **note the extra `Q1b` inside the Q1–Q6 range** — and Q6 records that `argv[3]` (feedPort) is silently ignored when a config file is passed, contradicting the CLI override order documented at the end of *Configuration* above.
- **`specs/2026-09-14-observability-spend-control/SPEC.md` — §3 (the GMA_V3 row) and §5 Q5; it holds *no* §2 decision for this repo** (ENC-1163). D1–D10 are all forum, embassy or workspace `infra/`. What it records here is two **verified absences**, both owned by ENC-931: this server has **no health endpoint of any kind** — it is WebSocket-only, and the stack orchestrator encodes that as an empty `[gma]=` entry, so `stack.sh status` renders a *blank* HEALTH column that reads like "not checked yet" rather than "cannot be checked"; and **`logLevel` is parsed and never applied** — `Logger::setLevel`, `setFile` and `setFormatJson` have zero call sites outside `Logger.cpp`, so GMA is permanently Info-level plaintext stdout with no override. Read that against the `logLevel` row in *Configuration* above, which documents the key as though setting it did something.
- **`specs/2026-09-14-connector-schema-discovery/SPEC.md` — §2 D6, §3, §5 Q3** (ENC-1155). D6 sequences ENC-1022 before ENC-1023 — the adapter selector must become real **before** the connector protocol whitelist widens — and rules that the adapter registry **errors on an unknown adapter name rather than defaulting**; today both branches at krn-common's `connectors/market/src/MarketConnector.cpp:213-214` build an `ItchAdapter`, and `src/util/Config.cpp:240` lets a `krn.conf` entry bypass the whitelist at `src/forum/ConnectorsClient.cpp:222` altogether. D6 is the only decision whose change site is this repo, but **D4** (decided in customer-layer) lands a consequence here: custom connectors are a self-host / fork path, which makes krn-common's `docs/writing-adapters.md` the officially-supported extension path and raises the stakes on D6's throw. §3 also carries ENC-1026 (the bridge into `MarketFieldMap`) and ENC-1028, which implement no `D` and are reachable only through that row. §5 **Q3** is the live trap: `bid`, `ask`, `spread` and `timestamp` are never populated on the only working protocol (krn-common's `connectors/market/src/feed/ItchAdapter.cpp:71-85`), so the mapping layer in krn-common's `connectors/market/src/MarketTA.cpp:352-368` is structurally unreachable. Do not let ENC-1031's `[GMA_V3]` title pull it in — that SPEC's §6 records it as entirely a treaty change.
- **`specs/2026-09-14-deployability/SPEC.md` — §2 D7, D8, §3** (ENC-1152). D8 (locked) is **no server-side CI in any repo, ever**; the local pre-push gate — `ctest` here — is the mechanism, and this repo is where D8 was *executed*: `c80ddad` (ENC-1069) deleted the last working CI in the workspace, so GMA_V3's **zero `.github` files are D8's tree evidence**, a proof that lives in an absence and therefore rots silently. D7 is the only change still owed: build identity from `git describe --tags --always --dirty`, one line per repo, retiring hardcoded phase strings (ENC-916, filed as `forum + embassy + GMA_V3`) — and it is blocked on §5 **Q1**, which measures zero git tags in every repo including this one, so there is nothing for `git describe` to describe. Note the limits: D1/D2 (single-box `m7i.xlarge` EC2 + Caddy + `docker compose`, us-east-2) cite GMA only as *rationale* — a C++ engine computing indicators continuously is sustained CPU by design, which is why burstable `t3` was rejected — and D5's container work is scoped to forum's and embassy's Dockerfiles, so **no decision in this record reaches this repo's root `Dockerfile`**.
