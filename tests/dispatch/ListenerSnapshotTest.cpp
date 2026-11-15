// ENC-1634 — copy-on-write listener snapshots.
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D3 (bit-identical), §6 T9.
//
// `Dispatcher` must never hold `_listenerMutex` while it calls out — into a
// node's `onValue` or into `ThreadPool::post` — because a node may register or
// unregister from inside `onValue`, and that takes the unique lock. Until this
// ticket that was bought by COPYING: `computeAndStoreAtomics` copied the
// symbol's whole `std::map<field, vector<shared_ptr<INode>>>` on every call,
// and `ingest`, `notifyListeners` and `listenersFor` copied vectors. ENC-1634
// keeps the property and drops the copies: each symbol's map is an immutable
// `shared_ptr<const ...>` replaced wholesale under the unique lock, and a
// reader takes one `shared_ptr` under the shared lock and walks it lock-free.
//
// What is pinned here:
//
//   1. RE-ENTRANCY. A node that registers and unregisters — others and itself,
//      on raw and on derived fields — from inside `onValue` does not deadlock,
//      on the inline path (no pool), on the strand-style inline path under a
//      pool, and on the pooled path. Each run is under a watchdog that fails
//      the process rather than hanging ctest.
//   2. SNAPSHOT SEMANTICS, EXACTLY AS BEFORE. A change made during a tick is
//      not seen by the rest of THAT tick's raw fan-out (ingest's snapshot is
//      taken once per tick) and is seen from the next `computeAndStoreAtomics`
//      call and the next tick on. Raw delivery order is registration order
//      within a field and `std::map` order across fields.
//   3. CONCURRENT CHURN. Registrations, unregistrations and the diagnostic
//      readers race one ingress thread; the end state is the start state.
//      This is the case for a TSan build (`-DKRN_SANITIZE=thread`).
//   4. A PARITY DUMP. Every combination of the three engine settings
//      (`atomicKeyNamespaceByField`, `inlineDagDrive`, `storeWriteElision`)
//      on both ingress shapes, over a TreeBuilder graph plus direct inline
//      recorders. With `KRN_ENC1634_DUMP_DIR` set it writes each outcome's
//      exact bytes there, so a build of master and a build of this branch can
//      be compared with `cmp`; unset, it only asserts it is non-vacuous.
//
// NO PROCESS-GLOBAL REGISTRY IS WRITTEN BY THIS FILE (ENC-1102).

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/engine/FieldSource.hpp"
#include "krn/engine/SymbolTable.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/util/Config.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

using namespace krn;

namespace enc1634 {

constexpr char kSym[] = "SNAP";

// ---- watchdog -------------------------------------------------------------
// A deadlock must FAIL, not hang: run `body` on its own thread and, if it has
// not finished in time, report and end the process (a blocked thread cannot be
// joined or cancelled).
void underWatchdog(const char* what, std::function<void()> body,
                   std::chrono::seconds limit = std::chrono::seconds(30)) {
  std::packaged_task<void()> task(std::move(body));
  auto done = task.get_future();
  std::thread t(std::move(task));
  if (done.wait_for(limit) != std::future_status::ready) {
    std::fprintf(stderr, "ENC-1634 watchdog: '%s' did not finish in %llds — "
                 "deadlock in re-entrant (un)registerListener\n",
                 what, static_cast<long long>(limit.count()));
    std::fflush(stderr);
    std::_Exit(3);
  }
  t.join();
  done.get();  // rethrows a body exception
}

// ---- nodes ------------------------------------------------------------------
struct Log {
  std::mutex               mx;
  std::vector<std::string> who;  // every delivery, in arrival order
  void add(const std::string& w) { std::lock_guard<std::mutex> lk(mx); who.push_back(w); }
  std::vector<std::string> copy() { std::lock_guard<std::mutex> lk(mx); return who; }
  std::size_t count(const std::string& w) {
    std::lock_guard<std::mutex> lk(mx);
    return static_cast<std::size_t>(std::count(who.begin(), who.end(), w));
  }
};

// Records its name. `inlineDelivery` makes it answer `deliversOnOwnExecutor`
// true, so the Dispatcher calls it on the ingress thread even with a pool —
// the path a strand-bearing Listener takes.
class Recorder : public INode {
public:
  Recorder(std::string who, Log* log, bool inlineDelivery = false)
    : who_(std::move(who)), log_(log), inline_(inlineDelivery) {}
  void onValue(const StreamValue&) override { log_->add(who_); }
  void shutdown() noexcept override {}
  bool deliversOnOwnExecutor() const noexcept override { return inline_; }
private:
  std::string who_;
  Log*        log_;
  bool        inline_;
};

// Runs `action(callIndex)` from inside `onValue`, then records itself.
class Churner final : public INode {
public:
  Churner(std::string who, Log* log, bool inlineDelivery,
          std::function<void(int)> action)
    : who_(std::move(who)), log_(log), inline_(inlineDelivery),
      action_(std::move(action)) {}
  void onValue(const StreamValue&) override {
    const int n = calls_.fetch_add(1);
    if (action_) action_(n);
    log_->add(who_);
  }
  void shutdown() noexcept override {}
  bool deliversOnOwnExecutor() const noexcept override { return inline_; }
  int calls() const { return calls_.load(); }
private:
  std::string              who_;
  Log*                     log_;
  bool                     inline_;
  std::function<void(int)> action_;
  std::atomic<int>         calls_{0};
};

Event alphaTick(double v) {
  auto d = std::make_shared<rapidjson::Document>();
  d->SetObject();
  d->AddMember("alpha", v, d->GetAllocator());
  return Event{kSym, std::move(d)};
}

std::vector<std::string> only(const std::vector<std::string>& log,
                              std::initializer_list<const char*> names) {
  std::vector<std::string> out;
  for (const auto& w : log)
    for (const char* n : names)
      if (w == n) { out.push_back(w); break; }
  return out;
}

// ---- 1 + 2: re-entrancy, with the exact snapshot semantics ------------------
//
// Registered, in order: `meanRec` on `mean`; then on `alpha` `churner`,
// `victim`, `tail`. On its FIRST call `churner` registers `newcomer` on
// `alpha` and `newMean` on `mean`, and unregisters `victim`; on its SECOND it
// unregisters itself. `meanRec`, on its first call, registers `stddevRec` on
// `stddev` — from inside `computeAndStoreAtomics`' own fan-out.
//
// Per tick, `ingest` snapshots the raw listeners once, then for field `alpha`
// runs `computeAndStoreAtomics` (its own snapshot, so the derived fan-out
// happens BEFORE the raw one) and then the raw fan-out. Expected raw sequence:
//   tick 1: churner victim tail            (victim still in tick 1's snapshot)
//   tick 2: churner tail newcomer          (victim gone, newcomer appended)
//   tick 3: tail newcomer                  (churner unregistered itself)
//   tick 4: tail newcomer
// Derived: meanRec 4; newMean 3 (registered during tick 1's RAW fan-out, which
// follows tick 1's atomics); stddevRec 3 (registered during tick 1's atomics,
// whose snapshot was already taken).
enum class Mode { InlineNoPool, InlineUnderPool, Pooled };

void runChurn(Mode mode) {
  Log log;
  AtomicStore store;
  std::shared_ptr<rt::ThreadPool> pool;
  if (mode != Mode::InlineNoPool) pool = std::make_shared<rt::ThreadPool>(4);
  Dispatcher disp(pool.get(), &store, util::Config{});
  const bool inl = mode == Mode::InlineUnderPool;

  auto victim    = std::make_shared<Recorder>("victim", &log, inl);
  auto tail      = std::make_shared<Recorder>("tail", &log, inl);
  auto newcomer  = std::make_shared<Recorder>("newcomer", &log, inl);
  auto newMean   = std::make_shared<Recorder>("newMean", &log, inl);
  auto stddevRec = std::make_shared<Recorder>("stddevRec", &log, inl);

  std::shared_ptr<Churner> churner;
  std::weak_ptr<Churner>   selfRef;
  churner = std::make_shared<Churner>("churner", &log, inl, [&](int n) {
    if (n == 0) {
      disp.registerListener(kSym, "alpha", newcomer);
      disp.registerListener(kSym, "mean", newMean);
      disp.unregisterListener(kSym, "alpha", victim);
    } else if (n == 1) {
      disp.unregisterListener(kSym, "alpha", selfRef.lock());
    }
  });
  selfRef = churner;
  auto meanRec = std::make_shared<Churner>("meanRec", &log, inl, [&](int n) {
    if (n == 0) disp.registerListener(kSym, "stddev", stddevRec);
  });

  disp.registerListener(kSym, "mean", meanRec);
  disp.registerListener(kSym, "alpha", churner);
  disp.registerListener(kSym, "alpha", victim);
  disp.registerListener(kSym, "alpha", tail);
  ASSERT_EQ(disp.subscriptionCount(), 4u);

  for (int t = 1; t <= 4; ++t) {
    disp.onTick(alphaTick(static_cast<double>(t)));
    // Pooled: the churn runs on a worker, so let tick t's re-entrant changes
    // land before tick t+1 is snapshotted — the expectation is per tick.
    if (pool) pool->drain();
  }
  if (pool) { pool->drain(); pool->shutdown(); }

  const auto all = log.copy();
  if (mode != Mode::Pooled) {
    // Inline: arrival order is the Dispatcher's order.
    EXPECT_EQ(only(all, {"churner", "victim", "tail", "newcomer"}),
              (std::vector<std::string>{"churner", "victim", "tail",
                                        "churner", "tail", "newcomer",
                                        "tail", "newcomer",
                                        "tail", "newcomer"}));
  }
  EXPECT_EQ(log.count("churner"), 2u);
  EXPECT_EQ(log.count("victim"), 1u);
  EXPECT_EQ(log.count("tail"), 4u);
  EXPECT_EQ(log.count("newcomer"), 3u);
  EXPECT_EQ(log.count("meanRec"), 4u);
  EXPECT_EQ(log.count("newMean"), 3u);
  EXPECT_EQ(log.count("stddevRec"), 3u);
  // meanRec, newMean, stddevRec, tail, newcomer.
  EXPECT_EQ(disp.subscriptionCount(), 5u);
  EXPECT_TRUE(disp.listenersFor(kSym, "alpha") ==
              (std::vector<std::shared_ptr<INode>>{tail, newcomer}));
}

TEST(ListenerSnapshot, ReentrantRegisterUnregisterInlineNoPool) {
  underWatchdog("InlineNoPool", [] { runChurn(Mode::InlineNoPool); });
}
TEST(ListenerSnapshot, ReentrantRegisterUnregisterInlineUnderPool) {
  underWatchdog("InlineUnderPool", [] { runChurn(Mode::InlineUnderPool); });
}
TEST(ListenerSnapshot, ReentrantRegisterUnregisterPooled) {
  underWatchdog("Pooled", [] { runChurn(Mode::Pooled); });
}

// notifyListeners (the IEventComputer hook) has the same contract: a node it
// calls may unregister every listener on the field, itself included, and the
// rest of THIS call's fan-out still happens.
TEST(ListenerSnapshot, NotifyListenersFanOutSurvivesItsTargetsUnregistering) {
  underWatchdog("notifyListeners", [] {
    Log log;
    Dispatcher disp(nullptr, nullptr, util::Config{});
    std::vector<std::shared_ptr<INode>> targets;
    auto wiper = std::make_shared<Churner>("wiper", &log, false, [&](int) {
      for (auto& t : targets) disp.unregisterListener(kSym, "x", t);
    });
    targets.push_back(wiper);
    for (const char* n : {"b", "c"}) targets.push_back(std::make_shared<Recorder>(n, &log));
    for (auto& t : targets) disp.registerListener(kSym, "x", t);

    disp.notifyListeners(kSym, "x", 1.0);
    EXPECT_EQ(log.copy(), (std::vector<std::string>{"wiper", "b", "c"}));
    EXPECT_EQ(disp.subscriptionCount(), 0u);
    EXPECT_TRUE(disp.listenersFor(kSym, "x").empty());
    disp.notifyListeners(kSym, "x", 2.0);
    EXPECT_EQ(log.copy().size(), 3u);
  });
}

// `listenersFor` is a snapshot: what it returned does not move when the
// registration does, and duplicates and unregister-all-copies behave as before.
TEST(ListenerSnapshot, ListenersForIsASnapshotAndUnregisterRemovesEveryCopy) {
  Log log;
  Dispatcher disp(nullptr, nullptr, util::Config{});
  auto a = std::make_shared<Recorder>("a", &log);
  auto b = std::make_shared<Recorder>("b", &log);
  disp.registerListener(kSym, "f", a);
  disp.registerListener(kSym, "f", b);
  disp.registerListener(kSym, "f", a);  // a duplicate is two subscriptions
  const auto before = disp.listenersFor(kSym, "f");
  ASSERT_EQ(before, (std::vector<std::shared_ptr<INode>>{a, b, a}));
  EXPECT_EQ(disp.subscriptionCount(), 3u);

  disp.unregisterListener(kSym, "f", a);  // removes BOTH copies
  EXPECT_EQ(before, (std::vector<std::shared_ptr<INode>>{a, b, a}));
  EXPECT_EQ(disp.listenersFor(kSym, "f"), (std::vector<std::shared_ptr<INode>>{b}));
  EXPECT_EQ(disp.subscriptionCount(), 1u);

  disp.unregisterListener(kSym, "f", a);       // not registered: no-op
  disp.unregisterListener(kSym, "nope", b);    // unknown field: no-op
  disp.unregisterListener("NOSYM", "f", b);    // unknown symbol: no-op
  EXPECT_EQ(disp.subscriptionCount(), 1u);
  disp.unregisterListener(kSym, "f", b);
  EXPECT_EQ(disp.subscriptionCount(), 0u);
  EXPECT_TRUE(disp.listenersFor(kSym, "f").empty());
  disp.onTick(alphaTick(1.0));  // no symbol entry left: nothing to deliver
  EXPECT_TRUE(log.copy().empty());
}

// ---- 3: concurrent churn ----------------------------------------------------
TEST(ListenerSnapshot, ConcurrentChurnAgainstOneIngressThread) {
  underWatchdog("ConcurrentChurn", [] {
    Log log;
    AtomicStore store;
    auto pool = std::make_shared<rt::ThreadPool>(4);
    Dispatcher disp(pool.get(), &store, util::Config{});
    auto stay = std::make_shared<Recorder>("stay", &log, true);
    disp.registerListener(kSym, "alpha", stay);
    disp.registerListener(kSym, "mean", stay);

    std::atomic<bool> stop{false};
    std::vector<std::thread> churn;
    for (int c = 0; c < 2; ++c) {
      churn.emplace_back([&, c] {
        const char* fields[] = {"alpha", "mean", "stddev", "beta"};
        int i = 0;
        while (!stop.load()) {
          auto n = std::make_shared<Recorder>("churn", &log, (i & 1) != 0);
          const char* f = fields[(i + c) % 4];
          disp.registerListener(kSym, f, n);
          disp.registerListener("OTHER", f, n);
          std::this_thread::yield();
          disp.unregisterListener(kSym, f, n);
          disp.unregisterListener("OTHER", f, n);
          ++i;
        }
      });
    }
    std::thread reader([&] {
      while (!stop.load()) {
        (void)disp.subscriptionCount();
        (void)disp.listenersFor(kSym, "alpha");
        disp.notifyListeners(kSym, "beta", 1.0);
      }
    });
    for (int t = 0; t < 2000; ++t) disp.onTick(alphaTick(static_cast<double>(t)));
    stop.store(true);
    for (auto& t : churn) t.join();
    reader.join();
    pool->drain();
    pool->shutdown();

    EXPECT_EQ(disp.subscriptionCount(), 2u);
    EXPECT_EQ(log.count("stay"), 4000u);  // 2000 raw + 2000 mean, every tick
  });
}

// ---- 4: the parity dump ------------------------------------------------------
template <typename T> void putPod(std::string& o, const T& v) {
  o.append(reinterpret_cast<const char*>(&v), sizeof v);
}
void putStr(std::string& o, const std::string& s) { putPod<std::uint64_t>(o, s.size()); o += s; }

void encode(std::string& o, const ArgType& v) {
  o.push_back(static_cast<char>(v.index()));
  std::visit([&o](auto&& x) {
    using T = std::decay_t<decltype(x)>;
    if constexpr (std::is_same_v<T, bool>) o.push_back(x ? 1 : 0);
    else if constexpr (std::is_same_v<T, int> || std::is_same_v<T, double>) putPod(o, x);
    else if constexpr (std::is_same_v<T, std::string>) putStr(o, x);
    else if constexpr (std::is_same_v<T, std::vector<int>> || std::is_same_v<T, std::vector<double>>) {
      putPod<std::uint64_t>(o, x.size());
      for (const auto& e : x) putPod(o, e);
    } else if constexpr (std::is_same_v<T, std::vector<ArgValue>>) {
      putPod<std::uint64_t>(o, x.size());
      for (const auto& e : x) encode(o, e.value);
    } else {
      putPod<std::uint64_t>(o, x.fields.size());
      for (const auto& f : x.fields) { putStr(o, f.name); encode(o, f.value.value); }
    }
  }, v);
}

class Sink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx);
    putStr(bytes, sv.symbol);
    encode(bytes, sv.value);
    putPod(bytes, sv.bucketStartMs);
    ++count;
  }
  void shutdown() noexcept override {}
  std::mutex  mx;
  std::string bytes;
  std::size_t count = 0;
};

// Direct, inline-delivered: one shared byte log whose ORDER is the
// Dispatcher's raw + derived fan-out order across every field.
class OrderTap final : public INode {
public:
  OrderTap(std::string who, std::string* log) : who_(std::move(who)), log_(log) {}
  void onValue(const StreamValue& sv) override {
    putStr(*log_, who_);
    putStr(*log_, sv.symbol);
    encode(*log_, sv.value);
  }
  void shutdown() noexcept override {}
  bool deliversOnOwnExecutor() const noexcept override { return true; }
private:
  std::string  who_;
  std::string* log_;
};

const std::vector<std::string>& requests() {
  static const std::vector<std::string> r = {
      R"({"streamKey":"SNAP","field":"ask"})",
      R"({"streamKey":"SNAP","field":"size","node":{"type":"Aggregate","arity":2,"inputs":[
          {"type":"Listener","streamKey":"SNAP","field":"ask"},
          {"type":"Listener","streamKey":"SNAP","field":"bid"}]}})",
      R"({"streamKey":"SNAP","field":"mean"})",
      R"({"streamKey":"SNAP","field":"ask.stddev"})",
      R"({"streamKey":"SNAP","field":"ask","node":{"type":"Aggregate","arity":2,"inputs":[
          {"type":"AtomicAccessor","streamKey":"SNAP","field":"mean"},
          {"type":"AtomicAccessor","streamKey":"SNAP","field":"bid.max"}]}})",
      R"({"streamKey":"SNAP","field":"size","pipeline":[{"type":"Worker","fn":"sum"}]})",
      R"({"streamKey":"SNAP","field":"bid"})",
  };
  return r;
}

struct Tick { double ask, bid, size; };

class TickFields final : public engine::FieldSource {
public:
  explicit TickFields(const Tick& t) : t_(t) {}
  bool isObject() const noexcept override { return true; }
  std::size_t fieldCountHint() const noexcept override { return 3; }
  bool has(const std::string& n) const override { return n == "ask" || n == "bid" || n == "size"; }
  bool tryGetNumber(const std::string& n, double& out) const override {
    if (n == "ask")  { out = t_.ask;  return true; }
    if (n == "bid")  { out = t_.bid;  return true; }
    if (n == "size") { out = t_.size; return true; }
    return false;
  }
  bool tryGetUint64(const std::string&, std::uint64_t&) const override { return false; }
  void forEachNumber(engine::NumericFieldSink& s) const override {
    s.field("ask", t_.ask);
    s.field("bid", t_.bid);
    s.field("size", t_.size);
  }
private:
  Tick t_;
};

Event jsonEvent(const Tick& t) {
  auto d = std::make_shared<rapidjson::Document>();
  d->SetObject();
  auto& a = d->GetAllocator();
  d->AddMember("ask", t.ask, a);
  d->AddMember("bid", t.bid, a);
  d->AddMember("size", t.size, a);
  return Event{kSym, std::move(d)};
}

struct Combo { bool ns, inl, elide, record; };

std::string comboName(const Combo& c) {
  return std::string("ns") + (c.ns ? "1" : "0") + "_inline" + (c.inl ? "1" : "0") +
         "_elide" + (c.elide ? "1" : "0") + (c.record ? "_onRecord" : "_onTick");
}

// Exact bytes of one run: per-request terminal streams, the inline order tap,
// and the sorted AtomicStore.
std::string drive(const Combo& c, std::size_t* minCount) {
  util::Config cfg;
  cfg.atomicKeyNamespaceByField = c.ns;
  cfg.inlineDagDrive = c.inl;
  cfg.storeWriteElision = c.elide;
  cfg.taHistoryMax = 16;
  auto pool = std::make_shared<rt::ThreadPool>(4);
  AtomicStore store;
  Dispatcher disp(pool.get(), &store, cfg);
  data::SymbolTable symbols;
  const auto sid = symbols.intern(kSym);
  disp.setSymbolTable(&symbols);

  std::vector<tree::BuiltChain>      chains;
  std::vector<std::shared_ptr<Sink>> sinks;
  for (const auto& json : requests()) {
    tree::Deps deps;
    deps.store = &store;
    deps.pool = pool.get();
    deps.dispatcher = &disp;
    rapidjson::Document d;
    d.Parse(json.c_str());
    EXPECT_FALSE(d.HasParseError()) << json;
    auto sink = std::make_shared<Sink>();
    chains.push_back(tree::buildForRequest(d, deps, sink));
    sinks.push_back(sink);
  }
  std::string order;
  std::vector<std::shared_ptr<INode>> taps;
  for (const char* f : {"ask", "bid", "size", "mean", "max", "ask.mean",
                        "bid.stddev", "size.sum"}) {
    for (int k = 0; k < 2; ++k) {
      taps.push_back(std::make_shared<OrderTap>(std::string(f) + "#" + std::to_string(k), &order));
      disp.registerListener(kSym, f, taps.back());
    }
  }

  for (std::size_t n = 0; n < 200; ++n) {
    const double ask = 100.0 + 0.01 * static_cast<double>((n * 37) % 101);
    const Tick t{ask, ask - 0.02 * static_cast<double>(n % 3 + 1),
                 static_cast<double>(n % 7 + 1)};
    if (c.record) {
      const TickFields fields(t);
      disp.onRecord(sid, fields);
    } else {
      disp.onTick(jsonEvent(t));
    }
    // Pull samples race the pool unless the DAG is inline; drain per tick so
    // the dump is a deterministic function of the code, on both settings.
    pool->drain();
  }
  pool->drain();

  std::string out;
  *minCount = static_cast<std::size_t>(-1);
  for (auto& s : sinks) {
    std::lock_guard<std::mutex> lk(s->mx);
    putStr(out, s->bytes);
    *minCount = std::min(*minCount, s->count);
  }
  putStr(out, order);
  auto snap = store.snapshot();
  std::sort(snap.begin(), snap.end(), [](const auto& a, const auto& b) {
    return std::tie(std::get<0>(a), std::get<1>(a)) < std::tie(std::get<0>(b), std::get<1>(b));
  });
  std::string st;
  for (const auto& [k, f, v] : snap) { putStr(st, k); putStr(st, f); encode(st, v); }
  putStr(out, st);

  for (auto& ch : chains) {
    for (auto& n : ch.keepAlive) if (n) n->shutdown();
    if (ch.head) ch.head->shutdown();
  }
  pool->drain();
  pool->shutdown();
  if (order.empty() || st.empty()) *minCount = 0;
  return out;
}

class ListenerSnapshotParity : public ::testing::TestWithParam<int> {};

TEST_P(ListenerSnapshotParity, DumpIsDeterministicAndNonVacuous) {
  const int b = GetParam();
  const Combo c{(b & 1) != 0, (b & 2) != 0, (b & 4) != 0, (b & 8) != 0};
  std::size_t minCount = 0;
  const std::string first = drive(c, &minCount);
  ASSERT_GT(minCount, 0u) << comboName(c) << ": some terminal received nothing";
  std::size_t again = 0;
  EXPECT_EQ(drive(c, &again), first) << comboName(c) << ": not deterministic";

  std::uint64_t h = 1469598103934665603ull;  // FNV-1a 64
  for (unsigned char ch : first) { h ^= ch; h *= 1099511628211ull; }
  std::printf("ENC-1634 dump %s bytes=%zu fnv=%016llx\n", comboName(c).c_str(),
              first.size(), static_cast<unsigned long long>(h));
  if (const char* dir = std::getenv("KRN_ENC1634_DUMP_DIR")) {
    std::ofstream f(std::string(dir) + "/" + comboName(c) + ".bin", std::ios::binary);
    f.write(first.data(), static_cast<std::streamsize>(first.size()));
    ASSERT_TRUE(f.good());
  }
}

INSTANTIATE_TEST_SUITE_P(AllSettings, ListenerSnapshotParity, ::testing::Range(0, 16));

}  // namespace enc1634
