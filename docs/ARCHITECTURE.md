# KRN Architecture

This document is the authoritative reference for how KRN is put together. `CLAUDE.md` is the scannable orientation; this is the deeper picture.

## 1. Design intent

KRN is a **streaming-computation server**: JSON request trees come in over WebSocket, events come in from data feeds, and the node pipeline runs user-specified math on the stream and pushes results back out. The codebase is split into two layers:

- **Engine** (`libkrn_engine`): everything that has no idea what it's processing — node pipeline, event routing, JSON tree builder, WebSocket transport, thread pool, atomic store.
- **Connectors** (here only `libkrn_plugin_reference`): everything domain-specific — feed protocols, per-event computations, custom atomic provider namespaces, specialized node types. The market connector (order book, TA, ITCH, `FeedServer`, `WsFeedClient`) and `krn_server` live in [krn-common](https://github.com/Dynacharting/krn-common); the data layer (`krn_data`, Arrow, ClickHouse, ingest) lives in [krn-data](https://github.com/Dynacharting/krn-data). Both consume this repo through `FetchContent`.

**The core invariant:** adding a new connector touches only the connector's own directory plus one factory in a binary's `HostOptions::connectors`. No engine edits. If a change to the engine is ever required to support a new connector, something went wrong in the extension-point design.

## 2. Library topology

```
┌──────────────────────────────────────────────────────────────┐
│ krn_engine_server  (apps/engine_server/main.cpp)             │
└───────────┬──────────────────────┬───────────────────────────┘
            │ links                │ links
            ▼                      ▼
┌───────────────────────┐ ┌────────────────────────────────────┐
│ libkrn_host           │ │ libkrn_plugin_reference            │
│   src/host/Host.cpp:  │ │   ReferenceConnector +             │
│   the composition     │ │   ReferenceComputer,               │
│   (hostMain/runHost)  │ │   SyntheticConnector               │
└──────┬────────────────┘ └──────┬─────────────────────────────┘
       │ depends on              │ depends on
       ▼                         ▼
┌──────────────────────────────────────────────────────────────┐
│ libkrn_engine                                                │
│   Dispatcher, TreeBuilder, Node pipeline, WebSocketServer,   │
│   AtomicStore, FunctionMap, AtomicProviderRegistry,          │
│   ThreadPool, ShutdownCoordinator, registries under engine/  │
└──────────────────────────────────────────────────────────────┘
```

`krn_host` names no plugin: a binary passes its connectors in `HostOptions` (`include/krn/host/Host.hpp`). `krn_engine_server` is the connector-free server: `krn_host` plus the reference plugin. krn-common's `krn_server` is `krn_host` plus its market connector. Under `FetchContent` / `add_subdirectory` only the library targets are defined (`krn_engine`, `krn_host`, `krn_plugin_reference`); `cmake --install` + `find_package(krn CONFIG)` gives `krn::krn_engine` and `krn::krn_host`.

Engine isolation is enforced twice. CMake include scoping: `krn_engine`'s PUBLIC include path is `include/` only, so an engine `#include` of a plugin header fails to compile. And `tools/check_engine_isolation.py`, run as the ctest cases `krn_engine_isolation` and `krn_engine_isolation_selftest`:

| Check | What it rejects |
|---|---|
| A textual | an engine translation unit including an Arrow / Parquet / ClickHouse / DuckDB header, or a `connectors/*` / `plugins/*` header |
| B include | an in-repo include directory on `krn_engine` other than `include/` or a vendored `third_party/` tree |
| C link | `krn_data` / Arrow / Parquet / ClickHouse anywhere in `krn_engine`'s transitive link closure |
| D profile | the same in the closures of `krn_host`, `krn_engine_server` and `krn_tests`; and `krn_connector_market` reachable from any graphed target |
| E lexical | part 1 (ENC-1578) is the comment/literal stripper over check A's file list; its vocabulary is ENC-1579 |

The positive control is `krn_isolation_control`, a scan-only target whose closure must trip a forbidden pattern. Exit 0 is a verified pass, 1 a violation, 2 could not run (including a missing or silent control).

## 3. Event lifecycle (the hot path)

An `Event` is the canonical ingress unit:

```cpp
struct Event {
  std::string                          symbol;   // opaque stream key
  std::shared_ptr<rapidjson::Document> payload;  // source JSON
  std::string                          type { "tick" };
};
```

Where events come from, end-to-end, using krn-common's market connector as the example (`FeedServer`, `WsFeedClient`, `ItchAdapter` and `MarketTickComputer` are all krn-common's):

```
  TCP socket (port 9001)                External wss:// feed
           │                                    │
           ▼                                    ▼
   FeedServer.handleLine                   WsFeedClient
   (market JSON schema)                    + ItchAdapter.translate()
           │                                    │
           ▼                                    ▼
                      Event{ symbol, payload, type="tick" }
                                  │
                                  ▼
                ┌─── Dispatcher.onTick(event) ───┐
                │   (wraps the DOM in a          │
                │    JsonFieldSource and calls   │
                │    the private ingest(); see   │
                │    "Two ingress shapes" below) │
                │                                │
                │  1. For each IEventComputer c: │
                │       if c.eventType() ==      │
                │          event.type, run       │
                │          c.compute(symbol,     │
                │            fields, ctx)        │
                │                                │
                │     MarketTickComputer:        │
                │       - extract price/vol/bid/ │
                │         ask from the fields    │
                │       - update per-symbol      │
                │         TickEntry history      │
                │       - run computeAll-        │
                │         AtomicValues → store   │
                │       - notifyListeners(       │
                │         symbol, "sma_5", v)    │
                │                                │
                │  2. For each raw payload field │
                │     matching a subscribed      │
                │     (symbol, field) listener:  │
                │       - append to per-field    │
                │         history                │
                │       - run FunctionMap        │
                │         builtins → store       │
                │       - threadPool.post(       │
                │         listener.onValue(sv))  │
                └────────────────────────────────┘
                                  │
                                  ▼
                        Listener.onValue()
                                  │
                                  ▼
                     downstream pipeline nodes
                                  │
                                  ▼
                           Responder → sendFn
                                  │
                                  ▼
                       ClientSession.sendUpdate
                                  │
                                  ▼
                            async_write → WS
```

Key semantics:

- **Type routing.** `Dispatcher` invokes only computers whose `eventType()` matches the event's `type`. This is how the reference and synthetic computers (or market and synthetic ones) coexist on the same dispatcher.
- **TA atomics are not directly subscribable.** `sma_5`, `rsi_14`, etc. are written to `AtomicStore` but delivered to listeners only when a client adds an `AtomicAccessor` node to its pipeline. Subscribing directly on field `sma_5` does nothing by itself.
- **FunctionMap builtins are subscribable.** `mean`, `sum`, `stddev`, etc. are computed per-field per-tick inside `Dispatcher::computeAndStoreAtomics` and fan out to matching listeners. By default they are stored under the **bare** function name — one slot per symbol, shared by every field, so two fields driving the same builtin overwrite each other. Set `atomicKeyNamespaceByField = true` to store them as `<field>.<fn>` instead; Listener push is unchanged either way. See krn-common's [atomic-keys.md](https://github.com/Dynacharting/krn-common/blob/main/docs/atomic-keys.md#derived-atomic-key-shape--atomickeynamespacebyfield-enc-1008) for what flipping it breaks.
- **Per-field raw path.** If a listener subscribed on `(AAPL, lastPrice)` and the payload has `lastPrice`, the dispatcher reads it and delivers directly — no TA involvement.

### Two ingress shapes, one body (ENC-1486)

`Dispatcher` has **two** entry points and **one** implementation behind them:

```cpp
void onTick  (const Event& tick);                 // JSON DOM — the legacy TCP
                                                  //   feed + market.wsclient
void onRecord(data::SymbolId symbolId,            // resolved fields, no DOM
              const engine::FieldSource& fields);
```

`onTick`'s unit of work is a `shared_ptr<rapidjson::Document>`: it walks every
member constructing a `std::string` per member per tick for the raw-admission
ledger, and each computer then probes the DOM by name. For a chart tick that is
irrelevant; at MBO rates — tens of millions of records per symbol-day — it is
the dominant cost, not the math. `onRecord` takes a record's interned
`symbolId` and a `FieldSource` over fields the normalizer already resolved, so
nothing on that path constructs a DOM or a per-field string. The engine names
no record type: `krn::data::TickRecord` and its `RecordFieldSource` are in
`krn_data` (krn-data's `data/include/krn/data/`, ENC-1557).

Both are three-line adapters over the private
`ingest(symbol, eventType, const engine::FieldSource&)`, so the raw-admission
ledger, the history buffers, `computeAndStoreAtomics`, the `AtomicStore` keys,
the computer sets and `deliver()`'s ordering are literally one implementation —
**a graph cannot tell which entry point fed it**, structurally rather than by
intention. `engine::FieldSource` is the field view; `JsonFieldSource` reads a
DOM and `RecordFieldSource` reads a `TickRecord`, and the latter's header
carries the record → engine field mapping and what each omission protects.

Consequences worth knowing before you touch either:

- **`IEventComputer::compute` takes a `FieldSource`, not an `Event`.** A record
  overload would be a second implementation of every connector's field
  extraction, free to drift — which is exactly how a graph would learn to tell
  the paths apart. `computeEvent(const Event&)` is a non-virtual DOM shim for a
  caller that has an `Event` in hand (connector unit tests); the Dispatcher
  does not use it.
- **`onRecord` needs `setSymbolTable()`.** A record names an interned
  `symbolId`; `StreamValue` carries a `std::string symbol`, so the ticker is
  resolved back at this boundary — one heap copy per record, deliberately left
  in place because removing it is a separately measured step of the data-layer
  benchmark. Without a table installed, or for an id the table never issued,
  `onRecord` is a no-op: there is no stream key, and routing under `""` would
  be a silent wrong answer.
- **There is no event time on this path.** `header.tsEventNs` is not exposed as
  a field and the clock nodes are untouched.

Design: `specs/2026-10-14-krn-data-layer/SPEC.md` D1/D3/D9,
krn-data's `docs/data-layer.md` §C2/§C6/§6. Pinned by
krn-data's `data/tests/RecordIngressParityTest.cpp` and
`tests/dispatch/RecordIngressEngineTest.cpp`.

## 4. Engine registries (extension points)

All live under `include/krn/engine/`. Most are header-only static singletons with a mutex-guarded map. Connectors publish here during `registerWith()`.

| Registry | Purpose | Who consumes |
|---|---|---|
| `EventTypeRegistry` | name → `EventSchema` (known fields, dispatchable flag) | Future validator integration |
| `EventComputerRegistry` | event type → list of `IEventComputer` **factories** (not instances — each dispatcher gets a fresh set, for state isolation) | **Live** — `Dispatcher` calls `createAll(type)` on the first event of each type and caches the result per dispatcher |
| `NodeTypeRegistry` | type name → `NodeBuilderFn(spec, defaultSymbol, Deps, downstream) → shared_ptr<INode>` | `TreeBuilder::buildOne` + `JsonValidator::validateNode` |
| `AtomicProviderRegistry` | namespace (e.g. `"ob"`) → `double(symbol, fullKey)` | `AtomicAccessor` when a store lookup misses |
| `FunctionMap` | fn name (e.g. `"mean"`) → `double(vector<double>)` | `TreeBuilder`'s worker builder |
| `IngressRegistry` | kind (e.g. krn-common's `"market.feedserver"`, `"market.wsclient"`) → ingress factory | **Live (ENC-31)** — `krn_host` instantiates each `cfg.ingress[]` entry by kind |
| `ProtocolIngressRegistry` | forum connector protocol → ingress kind (ENC-1574) | **Live** — `ConnectorsClient` maps each forum row; an unregistered protocol is logged and skipped |
| `ConfigNamespaceRegistry` | prefix (e.g. `"market"`) → reader function | **Live (ENC-35)** — populates connector config on `dispatchPendingKeys()` |

`EventTypeRegistry` is the one registry nothing on the hot path consults yet.

## 5. Connector contract

### Interface

```cpp
namespace krn::engine {

struct EngineRegistries {            // include/krn/engine/EngineRegistries.hpp
  const util::Config*      cfg;
  ConfigNamespaceRegistry* configNs;
  rt::ThreadPool*          pool;
  AtomicStore*             store;
  Dispatcher*              dispatcher;
  rt::ShutdownCoordinator* shutdown;
  boost::asio::io_context* io;
  EventTypeRegistry*       events;
  EventComputerRegistry*   computers;
  NodeTypeRegistry*        nodes;
  IngressRegistry*         ingress;
  AtomicProviderRegistry*  providers;
  FunctionMap*             functions;
  util::Logger*            log;
};

class IConnector {
public:
  virtual ~IConnector() = default;
  virtual std::string_view name() const = 0;
  virtual void registerWith(EngineRegistries&) = 0;  // allocate + register; no live sockets/timers
  virtual void start() = 0;                          // bring sources online
  virtual void stop() noexcept = 0;                  // tear down
};

}
```

### Boot sequence (see `src/host/Host.cpp`, `krn::host::runHost`)

1. **Config** — `argv[1]` = wsPort, `argv[2]` = config file; `argv[3]` onward belong to the binary (`HostHooks::applyExtraArgs`).
2. **Engine builtins** — `registerBuiltinFunctions()` populates `FunctionMap`; `registerBuiltinNodeTypes()` populates `NodeTypeRegistry`.
3. **Engine components** — `ThreadPool`, `AtomicStore`, `Dispatcher`, metrics, `io_context`, `WebSocketServer`, with their shutdown steps.
4. **Connector registration** — each `HostOptions::connectors` factory is constructed and `registerWith(regs)`'d, in order.
5. **Ingress** — the forum pull (when `forumUrl` is set), `cfg.dispatchPendingKeys()`, then each `cfg.ingress[]` entry is built by kind from `IngressRegistry` and started.
6. **Connector start** — `start()` in registration order; one `connectors-stop` step (priority 30) calls `stop()` in reverse.
7. **Run** — `ioc.run()` until SIGINT/SIGTERM; `ShutdownCoordinator::stop()` drains registered steps in order.

A binary is a list of factories (`apps/engine_server/main.cpp`):

```cpp
krn::host::HostOptions opts;
opts.programName = "krn_engine_server";
opts.connectors.push_back([] { return std::make_unique<krn::synthetic::ReferenceConnector>(); });
opts.connectors.push_back([] { return std::make_unique<krn::synthetic::SyntheticConnector>(); });
return krn::host::hostMain(argc, argv, opts);
```

### What `ReferenceConnector::registerWith` does

- Registers the `ReferenceComputer` factory for `reference.tick` on `reg.computers`, so each `Dispatcher` gets its own computer.
- Creates its timer on `reg.io`. `start()` arms it: every `tickMs` it emits a `reference.tick` event on stream `REF` whose `sample` field is `sampleBase + n`.

`ReferenceComputer` (`plugins/reference/include/krn/reference/ReferenceComputer.hpp`) is deterministic and history-dependent: per stream it writes the running sum and sum of squares of `sample` to `reference.sum` / `reference.sumsq`, NaN during a 3-event warm-up. Its header declares the output shape so tests assert it by name. `SyntheticConnector` emits `synthetic.tick` on `SYN` and writes `synthetic.sin` / `synthetic.cos`.

## 6. Adding a new connector (cookbook)

Concrete recipe to add a connector named `cool`:

1. **Directory layout**

   ```
   plugins/cool/
     include/krn/cool/CoolConnector.hpp
     src/CoolConnector.cpp
   ```

   A connector can equally live in its own repo that pulls this one in with `FetchContent` and links `krn::krn_host`, as krn-common does.

2. **CMake** — explicit source lists, no globs (ENC-1551):

   ```cmake
   add_library(krn_plugin_cool STATIC
     "${PROJECT_SOURCE_DIR}/plugins/cool/src/CoolConnector.cpp")
   target_include_directories(krn_plugin_cool PUBLIC
     "${PROJECT_SOURCE_DIR}/plugins/cool/include")
   target_link_libraries(krn_plugin_cool PUBLIC krn_engine)
   ```

   The isolation gate picks the new target up from `BUILDSYSTEM_TARGETS`; nothing else to register.

3. **Implement `CoolConnector : public engine::IConnector`.** The `registerWith(EngineRegistries&)` method decides what you publish:

   - **Custom atomic namespace:** `AtomicProviderRegistry::registerNamespace("cool", ...)`.
   - **Custom node type:** `engine::NodeTypeRegistry::registerNodeType("CoolNode", builder)`. JSON trees can now specify `"type":"CoolNode"`.
   - **Per-event computation:** write a class that implements `engine::IEventComputer` — one method, `compute(const std::string& symbol, const engine::FieldSource& fields, ComputeContext&)`; read your inputs through `fields.tryGetNumber(name, out)` and your computer works on BOTH ingress paths with no further work (§3, "Two ingress shapes"). Register a factory with `reg.computers->registerFactory(type, ...)`, or `reg.dispatcher->addComputer(...)` for a per-instance add.
   - **Ingress source:** register a factory on `reg.ingress` and name its kind in `ingress.N.kind`, or build a timer/client against `reg.io` and arm it in `start()`.

4. **Add one factory to the binary's `HostOptions::connectors`:**

   ```cpp
   opts.connectors.push_back([] { return std::make_unique<krn::cool::CoolConnector>(); });
   ```

That's the entire integration. You shouldn't need to touch any file under `include/` or `src/`.

**Reference implementations:**
- `plugins/reference/` — `ReferenceConnector` + `ReferenceComputer` and the sine-wave `SyntheticConnector`. With `tests/connectors/SyntheticConnectorTest.cpp` it is a complete worked example.
- krn-common's `connectors/market/` — full production connector with OB, TA, two feed paths, custom provider namespace.

## 7. Node pipeline

`INode` has two methods:

```cpp
class INode {
  virtual void onValue(const StreamValue& sv) = 0;
  virtual void shutdown() noexcept = 0;
};
```

Built-in node types (all registered by `registerBuiltinNodeTypes()`):

| Type | Role |
|---|---|
| `Listener` | Head of a chain. Subscribes on `(symbol, field)`; `Dispatcher` calls its `onValue` when the field fires. Uses `weak_ptr` downstream to allow the session to drop the chain. |
| `Worker` | Runs a named function (from `FunctionMap`) across its accumulated inputs; emits downstream. |
| `Aggregate` | Fan-in of N input heads into one downstream; emits when all N inputs have reported for a tick cycle. Each declared input is built terminating in its own `InputPort`, so an input is identified by its build-time **port index** — `arity` must equal `inputs.length` and is refused otherwise (ENC-1291, SPEC `specs/2026-09-20-krn-join-correctness/SPEC.md` D2). Until then it counted *values* rather than *inputs* and could complete a tuple from repeated ticks of one input. Correlation is still per `StreamValue::symbol`, so a cross-streamKey join emits nothing — SPEC D1 (`by`) is ENC-1292. |
| `Interval` | Timer wrapper — ticks its downstream every N ms (built on the engine thread pool). |
| `AtomicAccessor` | Pull-style — reads `(symbol, field)` from `AtomicStore` or the `AtomicProviderRegistry` and emits downstream. |
| `Responder` | Tail — writes the value back out to the WS client via a captured send function. |
| `GroupSplit` | Fans a single chain out to per-key child chains (constructed lazily on first key). **JSON wire name retained as `"SymbolSplit"`** for backward compatibility. |
| `Chain` | Sequential composition of stages (pipeline spec). |

### Ownership model

`ClientSession → Listener → ... → Responder → ClientSession` is a cycle. Break rule:

- **Listener** holds its downstream as `weak_ptr<INode>`. Cycle broken here.
- All other nodes hold downstream as `shared_ptr<INode>` (chain keeps itself alive).
- `TreeBuilder::BuiltChain.keepAlive` retains every constructed node so the Listener's `weak_ptr` stays valid for the lifetime of the subscription.
- `ClientSession.chains_[key]` stores the `keepAlive` vector. Cleared on `close()` / `handleCancel()` — that's what actually terminates the chain.

## 8. JSON protocols

### WebSocket (port `cfg.wsPort`) — `ClientSession`

> **Port default:** the compiled-in default `wsPort` is **8080**
> (`include/krn/util/Config.hpp`); `argv[1]` and a config file's `wsPort`
> override it. krn-common's `conf/krn.conf` sets **4000**.

Subscribe (each request lives in a top-level `requests` array):
```json
{"type":"subscribe","requests":[
  {"key": 1, "streamKey": "AAPL", "field": "lastPrice",
   "pipeline": [{"type":"Worker","fn":"mean"}]}
]}
```
- `streamKey` (string) is **required** — there is **no `symbol` fallback**
  (ENC-50). A request missing `streamKey` is rejected with
  `{"type":"error","where":"subscribe","message":"request missing 'streamKey' string"}`.
- The per-request identifier is `key` (int) **or** `id` (int|string), not both.
  `pipeline` overrides `node` if both present; pipeline is built in reverse order.
- `field` is the triggering event field.

Server replies (the id field mirrors the input — `key` for int subs,
`requestId` for string subs):
```json
{"type": "subscribed", "key": 1}
{"type": "update",     "key": 1, "streamKey": "AAPL", "value": 187.34}
{"type": "update",     "key": 2, "streamKey": "AAPL", "value": 187.34, "bucketStartMs": 1789879372000}
```

**`bucketStartMs` — the bar this value belongs to (ENC-1280).** Present only
when the subscription's pipeline is clocked by a wall-clock-aligned timer node
(`TumblingWindow` or `BucketTime`). It is the **epoch-ms start of the bucket
that closed when the value was produced**, taken from the very boundary those
nodes already align their `wait_until` deadline to — so it carries exactly the
alignment they guarantee and nothing else. Properties a consumer may rely on:

- always a JSON **integer** (rapidjson `Int64`), never a double — a current ms
  epoch needs 41 bits and would not survive a `float32` lane;
- always a whole multiple of the node's `periodMs`, so two subscriptions of the
  same period agree on bar identity regardless of when either connected;
- **bucket-derived, never a dispatch-time `time.Now()`** — re-dispatching an
  unchanged pipeline reproduces the same value for the same bar;
- **absent, not `0`**, when the stream is not bucketed. `0` is the internal
  "no bucket identity" sentinel (`StreamValue::bucketStartMs`) and would read as
  a 1970 epoch on the wire, so the key is omitted instead. An un-bucketed
  `update` frame therefore has exactly the four keys it always had.

Delivery is lossy (see flow control below) and quiet buckets emit nothing, so a
consumer must key off `bucketStartMs` rather than counting frames: bars arrive
in increasing order but not necessarily consecutively. See
`specs/2026-09-20-timestamps-on-the-wire/SPEC.md` D7/D9/D10 for the downstream
contract this feeds.

The wire key is `streamKey` everywhere; the engine is stream-neutral and the
internal `Event::symbol` field is just an opaque stream key (see §3). No
`symbol` alias is accepted on the wire.

**Outbound flow control — `update` frames are lossy under backlog (ENC-996).**
`ClientSession` keeps one outbound queue per session, and frames on it are of
two kinds:

| kind | frames | policy |
|---|---|---|
| lossless | `subscribed`, `canceled`, `error` | never dropped, never reordered |
| coalescable | `update` | latest-value-wins once the queue is a backlog |

Once the queue exceeds `COALESCE_WATERMARK` (256 frames), a new `update` for a
`(subscription, streamKey)` pair that already has one pending **replaces** it
rather than queueing behind it. Below the watermark the queue is a plain
lossless FIFO, so a consumer that keeps up receives every value.

At `MAX_OUTBOX_SIZE` (4096 frames) the queue is out of memory budget and cannot
append at all. A newer `update` then **displaces the oldest pending `update` in
the whole outbox** — the same newer-supersedes-older trade coalescing makes, but
across keys rather than within one (ENC-1072). A lossless frame likewise evicts
the oldest pending `update`.

Consequences for clients:
- **Intermediate values can vanish.** A client that must see every tick (running
  client-side aggregation, event replay, sequence-critical logic) cannot rely on
  the `update` stream when it falls behind — do the aggregation server-side with
  a pipeline node instead. Chart rendering is unaffected: the newest sample is
  always delivered.
- **Drops are stale-first, at every queue depth.** A value frame is only ever
  dropped in favour of a *newer* one, so the last value a producer emits is
  always either queued or coalesced into a queued frame, and the queue delivers
  it once production stops. That is what "the newest value always arrives" has
  to mean for a chart, which would otherwise sit on a stale number indefinitely.
  Until ENC-1072 this held only *below* the hard bound: at the bound the
  arriving frame was the one dropped, so a stream whose key had nothing pending
  went silent for as long as the client stayed behind.
  What the bound still costs is the final value of a key that fell silent while
  other traffic kept arriving; a key that keeps producing always wins a slot
  back.
- **Order is preserved.** Coalescing replaces the *newest* pending frame for a
  key, so surviving values still arrive in production order, and the final value
  of a burst is always delivered. Displacement at the hard bound only ever
  targets a key with nothing mutable pending, so it cannot put a value ahead of
  an older value of its own key either. Ordering *between* different keys has
  never been promised and is not preserved by either path.
- **The connection is not dropped.** The pre-ENC-996 policy closed the session
  at 4096 queued frames (`ws.outbox_overflow`), so a replay faster than the
  browser could render disconnected the browser. `MAX_OUTBOX_SIZE` is now a
  memory bound that sheds stale updates; only a queue of *entirely* lossless
  protocol frames can still force a close.

Operator signals: `ws.outbox_backpressure` (one Info line the first time a
session degrades, tagged with the policy that fired) and the counters:

| counter | meaning |
|---|---|
| `ws.outbox_coalesced` | a newer value superseded the pending value for its own key — the designed degradation |
| `ws.outbox_shed` | a **stale** queued value frame was dropped to admit something newer |
| `ws.outbox_shed_newest` | the **arriving** value was dropped. Only possible when all 4096 queued frames are lossless, so there is no value frame to trade against and protocol state must not be dropped for a sample. Non-zero means a stream really did lose its head value |
| `ws.outbox_overflow` | the queue was entirely lossless at the cap and a further lossless frame arrived — the session is closed |

### TCP feed and vendor feeds

The TCP feed server (`cfg.feedPort`, the `ob` / `control` message schema) and the vendor feed adapters are market-connector-owned and live in krn-common; see its [feed-adapters.md](https://github.com/Dynacharting/krn-common/blob/main/docs/feed-adapters.md).

## 9. Shutdown sequencing

`rt::ShutdownCoordinator` (local in `runHost`, not a global). Steps are registered with `(name, order, fn)`; `stop()` runs them in ascending order exactly once. SIGINT/SIGTERM trigger `stop()` through an asio `signal_set`.

Order in `src/host/Host.cpp`: `ws-stop-accept=5, metrics-stop=10, connectors-stop=30, ingress-stop=35, ws-close-sessions=40, asio-stop=60, pool-drain=80, pool-destroy=85`. Connectors never register their own steps.

## 10. Tests

One gtest executable, `krn_tests` (links no market code), plus script and gate cases, all run by `ctest`. Organization under `tests/`:

- `engine/` — engine registry unit tests
- `connectors/` — reference and synthetic connector tests (proves routing invariant)
- `dispatch/`, `nodes/`, `treebuilder/`, `validation/`, `registry/`, `core/`, `context/`, `mapping/`, `feed/`, `forum/` — engine pipeline
- `integration/`, `ws/`, `connection/`, `server/` — end-to-end
- `consumer/` — the `FetchContent` and `find_package` consumer smoke projects
- `scripts/` — `krn_engine_server_reference` and `krn_port_conflict_ws`
- `test_bootstrap.cpp` — `krn_tests`' gtest Environment: registers builtins, no connector

The market connector's tests (its `book/`, `ob/`, `feed/` and `market/`) are krn-common's `krn_common_tests`; the data layer's are krn-data's `krn_data_tests`.

`DispatcherRoutesByEventType` in `tests/connectors/SyntheticConnectorTest.cpp` is the regression check for "two computers on different event types must not interfere": with the synthetic computer and the reference plugin's `ReferenceComputer` both added to one Dispatcher, a `synthetic.tick` event writes no `reference.*` key and a `reference.tick` event writes no `synthetic.*` key. If that fails, the type-routing contract is broken.

## 11. Known residuals

Minor architectural debts that are deliberately unresolved — none block adding new connectors:

- **Source field mapping is connector-owned (ENC-35).** The old engine-level
  `include/krn/SourceProfile.hpp` and the `util::Config::sourceProfile` member
  are **gone**. The replacement is krn-common's `krn::market::MarketFieldMap`,
  populated from the `market.source.*` config namespace via
  `ConfigNamespaceRegistry`. The engine no longer knows about market-flavored
  fields.
- **JSON wire type name `"SymbolSplit"`** was preserved for backward compatibility when the class was renamed to `GroupSplit`. The string literal is correct and intentional; don't "fix" it.

## 12. Key files

If you need to find something quickly:

| Goal | File |
|---|---|
| Add a new node type | `include/krn/engine/NodeTypeRegistry.hpp`, register in your connector's `registerWith` |
| Add a math function | `FunctionMap::instance().registerFunction(...)` in your connector or in `src/core/BuiltinFunctions.cpp` for engine-wide defaults |
| Write a computer | `plugins/reference/src/ReferenceComputer.cpp` |
| Trace an incoming WS request | `src/server/ClientSession.cpp::handleSubscribe` → `src/core/TreeBuilder.cpp::buildForRequest` |
| Trace an incoming tick | `src/core/Dispatcher.cpp::onTick` |
| Change shutdown order | `src/host/Host.cpp` |
| Look at the boot flow | `src/host/Host.cpp::runHost` top-to-bottom; `apps/engine_server/main.cpp` for a binary |
| Check the layering | `tools/check_engine_isolation.py` |
