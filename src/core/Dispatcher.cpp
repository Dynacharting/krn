#include "krn/Dispatcher.hpp"
#include "krn/engine/JsonFieldSource.hpp"
#include "krn/util/Logger.hpp"

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

using namespace krn;

void Dispatcher::addComputer(std::unique_ptr<engine::IEventComputer> c) {
  if (!c) return;
  // Guard the append: onTick() may be iterating _computers from an ingress
  // thread. Without the lock this is a data race plus a potential
  // iterator-invalidating realloc (ENC-791/M8).
  std::unique_lock<std::shared_mutex> lock(_computersMutex);
  _computers.push_back(std::move(c));
}

Dispatcher::Dispatcher(rt::ThreadPool* threadPool,
                                   AtomicStore* store,
                                   const util::Config& cfg)
  : _threadPool(threadPool)
  , _store(store)
  , _cfg(cfg)
  , _maxHistory(static_cast<std::size_t>(std::max(1, cfg.taHistoryMax)))
  , _maxSymbols(static_cast<std::size_t>(std::max(1, cfg.maxSymbols)))
  , _maxFieldsPerSymbol(static_cast<std::size_t>(std::max(1, cfg.maxFieldsPerSymbol)))
{}

// ENC-1634 — COPY-ON-WRITE. See `_listeners` in the header. A writer never
// mutates a published map: it copies the symbol's current one, edits the copy
// and swaps the pointer, all under the unique lock, so a reader that took the
// old pointer keeps iterating a map nobody will touch again. The copy here is
// O(this symbol's subscriptions) and happens once per (un)registration — the
// cost the per-call copy in `computeAndStoreAtomics` used to pay per field per
// tick.
void Dispatcher::registerListener(const std::string& symbol,
                                        const std::string& field,
                                        std::shared_ptr<INode> listener)
{
  std::unique_lock<std::shared_mutex> lock(_listenerMutex);
  auto& slot = _listeners[symbol];
  auto next = slot ? std::make_shared<FieldListeners>(*slot)
                   : std::make_shared<FieldListeners>();
  (*next)[field].emplace_back(std::move(listener));
  slot = std::move(next);
}

std::size_t Dispatcher::subscriptionCount() const {
  std::shared_lock<std::shared_mutex> lock(_listenerMutex);
  std::size_t n = 0;
  for (const auto& [symbol, byField] : _listeners) {
    (void)symbol;
    for (const auto& [field, subs] : *byField) { (void)field; n += subs.size(); }
  }
  return n;
}

void Dispatcher::unregisterListener(const std::string& symbol,
                                          const std::string& field,
                                          std::shared_ptr<INode> listener)
{
  std::unique_lock<std::shared_mutex> lock(_listenerMutex);
  auto symIt = _listeners.find(symbol);
  if (symIt == _listeners.end()) return;
  const FieldListeners& current = *symIt->second;
  auto fldIt = current.find(field);
  if (fldIt == current.end()) return;
  const auto& vec = fldIt->second;
  // Not registered on this field: nothing changes, so publish nothing.
  if (std::find(vec.begin(), vec.end(), listener) == vec.end()) return;

  auto next = std::make_shared<FieldListeners>(current);
  auto& nvec = (*next)[field];
  nvec.erase(std::remove(nvec.begin(), nvec.end(), listener), nvec.end());
  if (nvec.empty()) next->erase(field);
  if (next->empty()) {
    _listeners.erase(symIt);
  } else {
    symIt->second = std::move(next);
  }
}

std::shared_ptr<const Dispatcher::FieldListeners>
Dispatcher::listenerSnapshot(const std::string& symbol) const {
  std::shared_lock<std::shared_mutex> lock(_listenerMutex);
  auto sit = _listeners.find(symbol);
  if (sit == _listeners.end()) return nullptr;
  return sit->second;
}

// ENC-1338. See the header: a diagnostic snapshot, never a synchronisation
// point. Returns a copy of the field's vector out of the symbol's snapshot,
// exactly what it returned when it copied it under the lock.
std::vector<std::shared_ptr<INode>>
Dispatcher::listenersFor(const std::string& symbol,
                         const std::string& field) const {
  const auto snap = listenerSnapshot(symbol);
  if (!snap) return {};
  auto fit = snap->find(field);
  if (fit == snap->end()) return {};
  return fit->second;
}

// ENC-1486. An ADAPTER, nothing more: wrap the DOM and hand it to `ingest`.
// Everything this method used to do below `if (tick.symbol.empty())` now lives
// in `ingest`, verbatim, with each JSON read replaced by the equivalent
// `FieldSource` call. See `Dispatcher::onRecord` (src/core/DispatcherRecord.cpp)
// for the other adapter and `include/krn/Dispatcher.hpp` for why there is only
// one body.
void Dispatcher::onTick(const Event& tick) {
  if (tick.symbol.empty() || !tick.payload) return;
  const engine::JsonFieldSource fields(*tick.payload);
  ingest(tick.symbol, tick.type, fields);
}

// ENC-1486 — THE ingress body, shared by `onTick` and `onRecord`.
//
// This is the pre-ENC-1486 tail of `onTick` with three substitutions and no
// other change:
//
//   payload->IsObject()                       -> fields.isObject()
//   MemberBegin()..MemberEnd() + IsNumber     -> fields.forEachNumber(sink)
//   HasMember(f) / (*payload)[f].GetDouble()  -> fields.has(f) / tryGetNumber
//
// The `try`/`catch` that used to wrap the value read is gone. rapidjson's
// `operator[]` does not throw — it asserts — so the catch was unreachable and
// its warning log had never fired; the two reachable outcomes (absent field,
// present non-numeric field) were both a silent `continue` and still are.
void Dispatcher::ingest(const std::string& symbol,
                        const std::string& eventType,
                        const engine::FieldSource& fields) {
  // ENC-1007: publish the RAW injected fields into the AtomicStore before
  // anything derived runs.
  //
  // Previously the store only ever received DERIVED atomics (FunctionMap
  // builtins, written by computeAndStoreAtomics below) and connector-computed
  // values. The injected value itself went to Listeners and was then dropped,
  // so an externally computed series — a client bringing its own RSI rather
  // than having krn recompute it — could not be read by AtomicAccessor and so
  // could not be driven from an Interval clock (§8.1).
  //
  // Deliberately NOT gated on having a registered Listener: an
  // Interval-driven AtomicAccessor is a PULL consumer that registers with
  // nothing, so requiring a Listener would leave §8.1 unachievable in exactly
  // the configuration it describes.
  //
  // Ordering matters. This runs FIRST, so both IEventComputer writes and the
  // builtin atomics below overwrite a raw field of the same name rather than
  // the other way round. Builtin atomics share a flat per-symbol namespace
  // keyed by bare function name (ENC-792/M9), so a raw field literally called
  // `mean` collides with the builtin `mean`; keeping the derived value as the
  // last writer means no existing Listener or AtomicAccessor binding changes
  // meaning. ENC-1008 namespaces the two apart when
  // `atomicKeyNamespaceByField` is on: the derived value moves to
  // `<field>.<fn>`, so a raw injected `mean` is no longer contested by the
  // builtin `mean`. It does NOT make collisions impossible — the namespaced
  // key is a plain concatenation, so a raw field literally named `alpha.mean`
  // is still clobbered by field `alpha`'s derived `mean`. With the flag off
  // (the default) the ordering above still decides, unchanged.
  //
  // Cardinality is bounded by AtomicStore's existing caps (maxSymbols /
  // maxFieldsPerSymbol, applied in main.cpp), which drop only NEW keys past
  // the cap and always update existing ones — so an unbounded stream of novel
  // field names cannot grow the store without limit.
  //
  // ENC-1486, found by an adversarial pass and recorded because it is on the
  // high-cardinality path now: A FIELDLESS TICK STILL BURNS A `_rawAdmitted`
  // SYMBOL SLOT. The ledger entry is created before anything is known about
  // how many fields will be admitted, so an all-`kNoPrice`/zero-size
  // record — which emits nothing — consumes one of `maxSymbols` and can
  // block raw admission for a later symbol. This is NOT a parity break: a
  // JSON `{}` payload, which is that record's equivalent tick, does exactly
  // the same, and `IdenticalForAFieldlessTick` pins that. It is a resource
  // quirk of the shared body, pre-existing on the JSON path and inherited
  // rather than introduced. Fixing it means deferring the ledger insert until
  // the first admitted field, which changes the JSON path's cap accounting and
  // therefore needs its own ticket.
  // ENC-1627 — STORE-WRITE ELISION (B2 step 5; `util::Config::
  // storeWriteElision`). Off: `elide` is false and every write below is the
  // one it always was. On: this tick first seals the store's pull-read set
  // (a no-op after the first tick), then the raw-field write and the builtin
  // write each skip any key not in it. The set is immutable once sealed, so
  // `readKeys` is looked up once here, without a lock, and stays valid.
  //
  // What elision deliberately does NOT touch, so that every declared key ends
  // each tick holding exactly what it holds with the setting off: the
  // raw-admission ledger below (an elided field still consumes its
  // `maxFieldsPerSymbol` slot, so a declared field is admitted or refused
  // exactly as before), the histories, which builtins are computed, the
  // Listener deliveries, and `IEventComputer` writes.
  const bool elide = _cfg.storeWriteElision && _store != nullptr;
  const std::unordered_set<std::string>* readKeys = nullptr;
  if (elide) {
    _store->sealReads();
    readKeys = _store->sealedReadFieldsFor(symbol);
  }

  if (_store && fields.isObject()) {
    std::vector<std::pair<std::string, ArgType>> raw;
    {
      // Admission is bounded by the SAME maxSymbols / maxFieldsPerSymbol
      // budget that bounds history, so raw injection cannot become a second,
      // unbounded way into the store. Enforced here rather than left to
      // AtomicStore's own caps because an embedder may construct a capped
      // Dispatcher over an uncapped store (main.cpp caps both; tests do not).
      std::unique_lock<std::shared_mutex> lock(_rawMutex);
      auto sit = _rawAdmitted.find(symbol);
      if (sit == _rawAdmitted.end()) {
        if (_rawAdmitted.size() >= _maxSymbols) {
          sit = _rawAdmitted.end();  // symbol cap reached — reject wholesale
        } else {
          sit = _rawAdmitted.emplace(symbol,
                                     std::unordered_set<std::string>{}).first;
        }
      }
      if (sit != _rawAdmitted.end()) {
        // The admission filter, unchanged, expressed as a sink so one copy of
        // it serves both ingress shapes. An abstract sink and not a
        // `std::function`: this captures three pointers, past libstdc++'s
        // small-object buffer, so a `std::function` would heap-allocate once
        // per tick on the path built to remove an allocation per tick.
        struct Admit final : engine::NumericFieldSink {
          std::unordered_set<std::string>*                     admitted;
          std::vector<std::pair<std::string, ArgType>>*        out;
          std::size_t                                          cap;
          bool                                                 elide;
          const std::unordered_set<std::string>*               keep;
          // Explicit ctor: a class that declares a virtual override is not an
          // aggregate, so braced init of the members alone does not compile.
          Admit(std::unordered_set<std::string>* a,
                std::vector<std::pair<std::string, ArgType>>* o,
                std::size_t c, bool e,
                const std::unordered_set<std::string>* k)
            : admitted(a), out(o), cap(c), elide(e), keep(k) {}
          void field(const std::string& name, double value) override {
            if (admitted->find(name) == admitted->end()) {
              if (admitted->size() >= cap) return;
              admitted->insert(name);
            }
            // ENC-1627: elided AFTER admission, so the ledger is unchanged.
            if (elide && !(keep && keep->contains(name))) return;
            out->emplace_back(name, ArgType{ value });
          }
        } admit( &sit->second, &raw, _maxFieldsPerSymbol, elide, readKeys );
        raw.reserve(fields.fieldCountHint());
        fields.forEachNumber(admit);
      }
    }
    if (!raw.empty()) {
      _store->setBatch(symbol, raw);
    }
  }

  engine::ComputeContext ctx{ _store, this, _threadPool };

  // Per-type cache fed from EventComputerRegistry. First event of a given
  // type instantiates the registered factories; subsequent events reuse the
  // cached instances. Late-registered factories are picked up the first time
  // an event of their type arrives.
  std::vector<engine::IEventComputer*> typedComputers;
  {
    std::lock_guard<std::mutex> lk(_computerCacheMx);
    auto it = _computersByType.find(eventType);
    if (it == _computersByType.end()) {
      auto fresh = engine::EventComputerRegistry::createAll(eventType, _cfg);
      it = _computersByType.emplace(eventType, std::move(fresh)).first;
    }
    typedComputers.reserve(it->second.size());
    for (auto& c : it->second) typedComputers.push_back(c.get());
  }
  // NOTE (ENC-791/M11): compute() runs outside _computerCacheMx, so the same
  // cached instance can be entered concurrently by parallel ingress threads.
  // IEventComputer implementations must be concurrency-safe — see the contract
  // on Dispatcher::onTick() in the header.
  for (auto* c : typedComputers) {
    if (c) c->compute(symbol, fields, ctx);
  }

  // Computers added directly via addComputer() — kept for tests and code
  // paths that want to inject without the global registry. Snapshot the
  // matching raw pointers under a shared lock (addComputer() only ever
  // appends, so the pointers stay valid), then compute outside the lock so a
  // computer's compute() can re-enter the dispatcher safely (ENC-791/M8).
  std::vector<engine::IEventComputer*> directComputers;
  {
    std::shared_lock<std::shared_mutex> lock(_computersMutex);
    directComputers.reserve(_computers.size());
    for (auto& c : _computers) {
      if (c && c->eventType() == eventType) directComputers.push_back(c.get());
    }
  }
  for (auto* c : directComputers) {
    c->compute(symbol, fields, ctx);
  }

  // Walk, from one listener snapshot, each subscribed field this tick carries
  // together with ALL of its Listeners.
  //
  // ENC-1624: GROUPED BY FIELD, and the grouping is the fix. This used to be a
  // flat list of (field, listener) PAIRS, and the loop below appended the raw
  // value to the history and ran `computeAndStoreAtomics` once per PAIR. With
  // N Listeners on `price`, each price entered `_histories[symbol]["price"]`
  // N times and every builtin was recomputed N times over that corrupted
  // window, so a derived value depended on how many unrelated subscriptions
  // shared its source field. A value now enters its history once per field,
  // the builtins run once per field, and only the raw delivery fans out.
  //
  // Delivery order is unchanged: fields in `_listeners`' (std::map) order, and
  // within a field its Listeners in registration order — the same sequence the
  // flat pair list produced. What disappears is the repeated derived delivery
  // that used to sit between a field's second and later Listeners.
  //
  // ENC-1634: the per-tick collection is now the symbol's copy-on-write
  // snapshot itself — one `shared_ptr` taken under the shared lock, walked
  // with no lock held. It is the same set the copy held: taken once, before
  // any delivery, so a Listener that (un)registers from inside `onValue` is
  // seen from the next tick on (and by the next `computeAndStoreAtomics`
  // call, which takes its own snapshot), never by the rest of this fan-out.
  const auto symListeners = listenerSnapshot(symbol);
  if (!symListeners) return;
  std::shared_lock<std::shared_mutex> mutationM1(_listenerMutex);

  for (const auto& [field, nodes] : *symListeners) {
    if (nodes.empty() || !fields.has(field)) continue;
    double raw = 0.0;
    if (!fields.tryGetNumber(field, raw)) continue;

    // The caps reject the FIELD, and with it every Listener on it, exactly as
    // before: the per-pair version decided the same way for each pair, since
    // a rejected field is never inserted.
    std::vector<double> histVec;
    {
      std::unique_lock<std::shared_mutex> lock(_histMutex);
      if (_histories.find(symbol) == _histories.end() &&
          _histories.size() >= _maxSymbols) {
        continue;
      }
      auto& symFields = _histories[symbol];
      if (symFields.find(field) == symFields.end() &&
          symFields.size() >= _maxFieldsPerSymbol) {
        continue;
      }
      auto& hist = symFields[field];
      hist.push_back(raw);
      if (hist.size() > _maxHistory) hist.pop_front();
      histVec.assign(hist.begin(), hist.end());
    }

    computeAndStoreAtomics(symbol, field, histVec, elide, readKeys);

    const StreamValue out{ symbol, raw };
    for (auto& node : nodes) {
      deliver(node, out);
    }
  }
}

// ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3, D4.
//
// WHY THIS IS NOT ALWAYS A POOL POST. The per-DAG strand orders everything
// DOWNSTREAM of a Listener. It cannot order what happens UPSTREAM of one — and
// this hop is upstream. `onTick` walks its `_listeners` entry for the symbol,
// whose inner container is a `std::map<field, ...>`, so it produces `ask`
// before `bid` deterministically; posting each notification as an independent
// pool task then hands that order straight back to the scheduler. At four
// workers the two tasks run concurrently, each calls `Listener::onValue`, and
// whichever wins reaches the strand first. The strand would faithfully preserve
// an order that was already scrambled one hop earlier.
//
// So a node that says `deliversOnOwnExecutor()` is called INLINE, on the
// ingress thread, in the order this loop produced. It does almost no work
// there — take a lock, lock a weak_ptr, enqueue — and hands the value to its
// strand, which is what gets the real compute off the ingress thread. The pool
// hop is not removed, it is moved one node downstream, to the place that can
// keep the order.
//
// EVERYTHING ELSE IS UNCHANGED. `INode::deliversOnOwnExecutor()` defaults to
// false and only `nodes::Listener` overrides it, only when it holds a strand.
// A Listener on the legacy unordered path, and every raw `INode` that tests
// register directly, still gets exactly the pool post it got before.
void Dispatcher::deliver(const std::shared_ptr<INode>& node,
                         const StreamValue& out) {
  if (!node) return;
  if (node->deliversOnOwnExecutor()) {
    node->onValue(out);
    return;
  }
  if (_threadPool) {
    auto n = node;
    _threadPool->post([n, out]() {
      if (n) n->onValue(out);
    });
  } else {
    node->onValue(out);
  }
}

void Dispatcher::notifyListeners(const std::string& symbol,
                                       const std::string& field,
                                       double value) {
  // ENC-1634: deliver straight out of the snapshot; it is immutable, and
  // holding it keeps every target alive exactly as the copied vector did.
  const auto snap = listenerSnapshot(symbol);
  if (!snap) return;
  auto fit = snap->find(field);
  if (fit == snap->end()) return;
  const auto& targets = fit->second;
  if (targets.empty()) return;

  StreamValue out{ symbol, value };
  for (auto& node : targets) {
    deliver(node, out);
  }
}

void Dispatcher::computeAndStoreAtomics(const std::string& symbol,
                                              const std::string& field,
                                              const std::vector<double>& history,
                                              bool elide,
                                              const std::unordered_set<std::string>* readKeys)
{
  // ENC-1008. `field` is folded into the stored key only when
  // `atomicKeyNamespaceByField` is on; see the ATOMIC-KEY CONTRACT on the
  // declaration in Dispatcher.hpp for what the two states mean and what
  // flipping the flag breaks. OFF is the pre-ENC-1008 flat namespace
  // (ENC-792/M9) and must stay bit-identical to it.
  const bool nsByField = _cfg.atomicKeyNamespaceByField;
  // Built once rather than per-builtin: forEach below runs over all 56
  // registered reducers, and this is the per-tick hot path.
  const std::string nsPrefix = nsByField ? field + "." : std::string{};
  auto& fmap = FunctionMap::instance();

  // Snapshot this symbol's subscribers once. If nothing is subscribed there is
  // no listener to notify and no atomic worth materialising, so bail before
  // touching FunctionMap at all (ENC-792/M10). We must not hold
  // _listenerMutex while invoking onValue()/post() below (re-entrant
  // (un)registerListener would deadlock on the unique_lock). Until ENC-1634
  // that was bought by deep-copying the symbol's whole map here, on every
  // call; it is now the copy-on-write snapshot — one shared_ptr under the
  // shared lock — and the walk below is lock-free over an immutable map.
  const auto snap = listenerSnapshot(symbol);
  if (!snap) return;
  const FieldListeners& symListeners = *snap;
  if (symListeners.empty()) return;

  // Compute ONLY the builtins that have a subscriber for this symbol. The old
  // path recomputed all ~50 builtins (some O(N log N)) over the full history on
  // every field tick and set() each into the store regardless of subscription;
  // the subscription check now precedes the (potentially expensive) compute
  // (ENC-792/M10). Behaviour for subscribed functions is unchanged.
  fmap.forEach([&](const std::string& fnName, const Func& fn) {
    if (!fn) return;

    // Subscription gate (ENC-792/M10): skip the compute entirely when nothing
    // is listening. With namespacing on, EITHER key counts as a subscription —
    // the bare name keeps working so push behaviour does not change, and
    // `<field>.<fn>` becomes additionally subscribable.
    auto bareIt = symListeners.find(fnName);
    auto nsIt   = symListeners.end();
    std::string nsKey;
    if (nsByField) {
      nsKey = nsPrefix + fnName;
      nsIt  = symListeners.find(nsKey);
    }
    if (bareIt == symListeners.end() && nsIt == symListeners.end()) {
      return;  // not subscribed -> skip compute
    }

    double result = 0.0;
    try {
      result = fn(history);
    } catch (const std::exception& ex) {
      krn::util::logger().log(krn::util::LogLevel::Warn,
                              "Dispatcher: atomic function error",
                              { {"symbol", symbol}, {"fn", fnName},
                                {"err", ex.what()} });
      return;
    }

    // ENC-1627: with elision on, only a declared pull read is stored. The
    // value is still computed above and still delivered below.
    const std::string& storedKey = nsByField ? nsKey : fnName;
    if (_store && (!elide || (readKeys && readKeys->contains(storedKey)))) {
      // The ONE line this ticket is about. Flat `<fn>` is a single slot shared
      // by every field of the symbol, so a second field driving the same
      // builtin destroys the first field's value. `<field>.<fn>` gives each
      // source field its own slot.
      _store->set(symbol, storedKey, result);
    }

    // Same routing rule as the raw-field path — see Dispatcher::deliver.
    const StreamValue derived{ symbol, result };
    const auto notify = [&](const std::vector<std::shared_ptr<INode>>& targets) {
      for (auto& listener : targets) {
        deliver(listener, derived);
      }
    };

    if (bareIt != symListeners.end()) notify(bareIt->second);
    if (nsIt   != symListeners.end()) notify(nsIt->second);
  });
}
