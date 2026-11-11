// ENC-1625 — B2 step 3: the inline DAG executor.
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D3 (bit-identical by
// construction); workspace specs/2026-09-20-krn-join-correctness D3 (delivery
// is serialized per request DAG, under one ingress thread).
//
// `util::Config::inlineDagDrive` makes `tree::buildForRequest` give each
// request DAG an inline `rt::Strand` instead of a pooled one, so the whole DAG
// runs on the ingress thread with no `ThreadPool::post` between nodes. These
// tests assert that, under one ingress thread, that changes WHERE the graph
// runs and nothing else:
//
//   1. the setting is off by default and parses like the other engine flags;
//   2. it is really in force when on — every registered Listener holds an
//      inline strand, every delivery runs on the ingress thread, and with it
//      off none does (so the comparison below is between two different
//      executors, not one executor twice);
//   3. a multi-node graph (a raw push, a two-Listener join, a pull fan-in
//      over Dispatcher builtins, a Worker pipeline) delivers byte-identical
//      streams in identical order, and leaves a byte-identical AtomicStore,
//      through both `onTick` and `onRecord`;
//   4. the comparison can fail: a one-ULP input change and a swap of two
//      ticks, each asserted to have applied, each turn it red.
//
// NO PROCESS-GLOBAL REGISTRY IS WRITTEN BY THIS FILE (ENC-1102). It uses the
// builtins test_bootstrap.cpp registers and its own Dispatcher per run.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/engine/FieldSource.hpp"
#include "krn/engine/SymbolTable.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/nodes/Listener.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/util/Config.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

using namespace krn;

namespace enc1625 {

constexpr char        kSym[]  = "INL";
constexpr std::size_t kTicks  = 300;
constexpr unsigned    kPoolThreads = 4;

// The graph: five shapes, eight request DAGs, every node kind the B2 harness
// drives (Listener, Aggregate over Listeners, Aggregate over AtomicAccessors
// clocked by a Listener, Worker) — so a DAG is several nodes deep.
const std::vector<std::string>& requests() {
  static const std::vector<std::string> r = {
      R"({"streamKey":"INL","field":"ask"})",
      R"({"streamKey":"INL","field":"size","node":{"type":"Aggregate","arity":2,"inputs":[
          {"type":"Listener","streamKey":"INL","field":"ask"},
          {"type":"Listener","streamKey":"INL","field":"bid"}]}})",
      R"({"streamKey":"INL","field":"mean"})",
      R"({"streamKey":"INL","field":"stddev"})",
      R"({"streamKey":"INL","field":"ask","node":{"type":"Aggregate","arity":2,"inputs":[
          {"type":"AtomicAccessor","streamKey":"INL","field":"mean"},
          {"type":"AtomicAccessor","streamKey":"INL","field":"stddev"}]}})",
      R"({"streamKey":"INL","field":"size","pipeline":[{"type":"Worker","fn":"sum"}]})",
      R"({"streamKey":"INL","field":"bid","pipeline":[{"type":"Worker","fn":"mean"}]})",
      R"({"streamKey":"INL","field":"bid"})",
  };
  return r;
}

struct Tick { double ask, bid, size; };

std::vector<Tick> ticks() {
  std::vector<Tick> t;
  for (std::size_t n = 0; n < kTicks; ++n) {
    const double ask = 100.0 + 0.01 * static_cast<double>((n * 37) % 101);
    t.push_back({ask, ask - 0.02 * static_cast<double>(n % 3 + 1),
                 static_cast<double>(n % 7 + 1)});
  }
  return t;
}

// ---- exact bytes --------------------------------------------------------
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

// Terminal: every delivery's bytes in arrival order, and the thread it ran on.
class Sink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx_);
    putStr(bytes, sv.symbol);
    encode(bytes, sv.value);
    putPod(bytes, sv.bucketStartMs);
    ++count;
    threads.push_back(std::this_thread::get_id());
  }
  void shutdown() noexcept override {}
  std::mutex                   mx_;
  std::string                  bytes;
  std::size_t                  count = 0;
  std::vector<std::thread::id> threads;
};

// One FieldSource per tick for the record path.
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

enum class Ingress { OnTick, OnRecord };

struct Run {
  std::vector<std::string>                   bytes;    // per request
  std::vector<std::size_t>                   counts;
  std::string                                store;
  std::size_t                                deliveries = 0;
  std::size_t                                onCaller = 0;  // deliveries on the ingress thread
  std::size_t                                listeners = 0;
  std::size_t                                inlineStrands = 0;
};

Run drive(bool inlineDrive, Ingress ingress, const std::vector<Tick>& in) {
  util::Config cfg;
  cfg.inlineDagDrive = inlineDrive;
  auto pool = std::make_shared<rt::ThreadPool>(kPoolThreads);
  AtomicStore store;
  Dispatcher disp(pool.get(), &store, cfg);
  data::SymbolTable symbols;
  const auto sid = symbols.intern(kSym);
  disp.setSymbolTable(&symbols);

  std::vector<tree::BuiltChain>      chains;
  std::vector<std::shared_ptr<Sink>> sinks;
  for (const auto& json : requests()) {
    tree::Deps deps;                 // one per request (TreeBuilder.hpp)
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

  Run r;
  for (const char* f : {"ask", "bid", "size", "mean", "stddev"}) {
    for (const auto& n : disp.listenersFor(kSym, f)) {
      auto l = std::dynamic_pointer_cast<nodes::Listener>(n);
      if (!l) continue;
      ++r.listeners;
      if (l->strand() && l->strand()->isInline()) ++r.inlineStrands;
    }
  }

  const auto caller = std::this_thread::get_id();
  for (const auto& t : in) {
    if (ingress == Ingress::OnTick) {
      disp.onTick(jsonEvent(t));
    } else {
      const TickFields fields(t);
      disp.onRecord(sid, fields);
    }
  }
  pool->drain();

  for (auto& s : sinks) {
    std::lock_guard<std::mutex> lk(s->mx_);
    r.bytes.push_back(s->bytes);
    r.counts.push_back(s->count);
    r.deliveries += s->count;
    r.onCaller += static_cast<std::size_t>(
        std::count(s->threads.begin(), s->threads.end(), caller));
  }
  auto snap = store.snapshot();
  std::sort(snap.begin(), snap.end(), [](const auto& a, const auto& b) {
    return std::tie(std::get<0>(a), std::get<1>(a)) < std::tie(std::get<0>(b), std::get<1>(b));
  });
  for (const auto& [k, f, v] : snap) { putStr(r.store, k); putStr(r.store, f); encode(r.store, v); }

  for (auto& c : chains) {
    for (auto& n : c.keepAlive) if (n) n->shutdown();
    if (c.head) c.head->shutdown();
  }
  pool->drain();
  pool->shutdown();
  return r;
}

// Empty when identical; otherwise names the first differing terminal.
std::string diff(const Run& a, const Run& b) {
  for (std::size_t q = 0; q < a.bytes.size(); ++q) {
    if (a.bytes[q] != b.bytes[q])
      return "request " + std::to_string(q) + ": " + std::to_string(a.counts[q]) + " vs " +
             std::to_string(b.counts[q]) + " deliveries, bytes differ";
  }
  if (a.store != b.store) return "AtomicStore contents differ";
  return {};
}

void expectNonVacuous(const Run& r) {
  for (std::size_t q = 0; q < r.counts.size(); ++q)
    ASSERT_GT(r.counts[q], 0u) << "request " << q << " delivered nothing; comparison vacuous";
  ASSERT_FALSE(r.store.empty());
}

// ---- 1 --------------------------------------------------------------------
TEST(InlineDagDrive, OffByDefaultAndParsedLikeTheOtherEngineFlags) {
  EXPECT_FALSE(util::Config{}.inlineDagDrive);
  char path[] = "/tmp/enc1625-config-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  for (const char* v : {"true", "1", "yes"}) {
    { std::ofstream f(path); f << "inlineDagDrive=" << v << "\n"; }
    util::Config cfg;
    ASSERT_TRUE(cfg.loadFromFile(path));
    EXPECT_TRUE(cfg.inlineDagDrive) << v;
  }
  for (const char* v : {"false", "0", "no", ""}) {
    { std::ofstream f(path); f << "inlineDagDrive=" << v << "\n"; }
    util::Config cfg;
    ASSERT_TRUE(cfg.loadFromFile(path));
    EXPECT_FALSE(cfg.inlineDagDrive) << "'" << v << "'";
  }
  std::remove(path);
}

// ---- 2 --------------------------------------------------------------------
TEST(InlineDagDrive, TheSettingSelectsTheExecutorAndNothingElseReadsIt) {
  const auto in = ticks();
  const Run pooled = drive(false, Ingress::OnRecord, in);
  const Run inl    = drive(true,  Ingress::OnRecord, in);
  expectNonVacuous(pooled);
  expectNonVacuous(inl);

  // 8 requests: 8 heads, plus the 2 join inputs.
  EXPECT_EQ(pooled.listeners, 10u);
  EXPECT_EQ(inl.listeners, 10u);
  EXPECT_EQ(pooled.inlineStrands, 0u) << "default must keep the pooled strand";
  EXPECT_EQ(inl.inlineStrands, 10u)   << "every Listener of every DAG must hold the inline strand";

  // Where the graph ran. Off: every delivery on a pool worker. On: every one
  // on the ingress thread — i.e. no pool hop anywhere on the way down.
  EXPECT_EQ(pooled.onCaller, 0u);
  EXPECT_EQ(inl.onCaller, inl.deliveries);
}

// ---- 3 --------------------------------------------------------------------
class InlineDagDriveParity : public ::testing::TestWithParam<Ingress> {};

TEST_P(InlineDagDriveParity, IdenticalDeliveriesInIdenticalOrderToThePoolPath) {
  const auto in = ticks();
  const Run pooled = drive(false, GetParam(), in);
  expectNonVacuous(pooled);
  // The pool path is the reference; repeat the inline run so a nondeterminism
  // in either would have more than one chance to show.
  for (int rep = 0; rep < 3; ++rep) {
    const Run inl = drive(true, GetParam(), in);
    ASSERT_EQ(inl.onCaller, inl.deliveries) << "inline mode not in force";
    EXPECT_EQ(diff(pooled, inl), "") << "rep " << rep;
  }
  // And a second pool run, so a red above cannot be blamed on the pool path
  // disagreeing with itself.
  EXPECT_EQ(diff(pooled, drive(false, GetParam(), in)), "");
}

INSTANTIATE_TEST_SUITE_P(BothIngressPaths, InlineDagDriveParity,
                         ::testing::Values(Ingress::OnTick, Ingress::OnRecord),
                         [](const auto& info) {
                           return info.param == Ingress::OnTick ? std::string("onTick")
                                                                : std::string("onRecord");
                         });

// ---- 4 --------------------------------------------------------------------
TEST(InlineDagDrive, TheComparisonCanFail) {
  const auto in = ticks();
  for (Ingress ing : {Ingress::OnTick, Ingress::OnRecord}) {
    const Run pooled = drive(false, ing, in);
    expectNonVacuous(pooled);

    // One ULP on one tick's ask.
    auto ulp = in;
    const double was = ulp[kTicks / 2].ask;
    ulp[kTicks / 2].ask = std::nextafter(was, INFINITY);
    ASSERT_NE(std::memcmp(&was, &ulp[kTicks / 2].ask, sizeof was), 0) << "ULP mutation did not apply";
    EXPECT_NE(diff(pooled, drive(true, ing, ulp)), "") << "a one-ULP input change went unseen";

    // Two adjacent, distinct ticks swapped: same multiset, different order.
    auto swp = in;
    std::size_t i = kTicks / 2;
    while (i + 1 < swp.size() && swp[i].ask == swp[i + 1].ask) ++i;
    ASSERT_LT(i + 1, swp.size());
    std::swap(swp[i], swp[i + 1]);
    ASSERT_NE(swp[i].ask, in[i].ask) << "swap mutation did not apply";
    EXPECT_NE(diff(pooled, drive(true, ing, swp)), "") << "an order change went unseen";
  }
}

} // namespace enc1625
