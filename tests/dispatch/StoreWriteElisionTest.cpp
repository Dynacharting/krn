// ENC-1627 — B2 step 5: store-write elision.
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D3 (bit-identical by
// construction). The setting is `util::Config::storeWriteElision`; the read
// set is `AtomicStore::declareRead` / `sealReads`.
//
// With elision on, the Dispatcher writes raw fields and builtin atomics only to
// keys a pull node declared. These tests assert:
//
//   1. the setting is off by default and parses like the other engine flags;
//   2. off, the read set is recorded but nothing consults it: the store holds
//      every key, the set is never sealed, and a late pull read is accepted;
//   3. on, through both `onTick` and `onRecord`: every delivery is identical
//      in order, every declared key holds identical bytes after EVERY tick, and
//      the final store is exactly the off store restricted to declared keys.
//      That is strictly fewer entries, in a graph whose builtins write many
//      unread keys;
//   4. the comparison can fail: a one-ULP change and a swap, each asserted to
//      have applied;
//   5. late registration, the chosen semantics: a pull read on a key nobody
//      declared before the first tick is REFUSED (the build throws and leaves
//      no subscription behind). A pull read on an already-declared key,
//      including one whose request was torn down, is accepted and delivers
//      exactly what the off engine delivers;
//   6. under `AtomicStore::setCaps`, elision never loses a declared key the
//      off run had;
//   7. a non-accessor reader (an `IEventComputer` reading `ctx.store`) sees an
//      undeclared raw key elided, and sees it intact once it declares it.
//
// NO PROCESS-GLOBAL REGISTRY IS WRITTEN BY THIS FILE (ENC-1102). It uses the
// builtins test_bootstrap.cpp registers and its own Dispatcher per run.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/engine/FieldSource.hpp"
#include "krn/engine/IEventComputer.hpp"
#include "krn/engine/SymbolTable.hpp"
#include "krn/nodes/INode.hpp"
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
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace krn;

namespace enc1627 {

constexpr char        kSym[]       = "ELD";
constexpr std::size_t kTicks       = 300;
constexpr unsigned    kPoolThreads = 4;

// The one pull fan-in: clocked by `ask`, it samples two builtins and one raw
// field out of the store.
constexpr char kPull[] =
    R"({"streamKey":"ELD","field":"ask","node":{"type":"Aggregate","arity":3,"inputs":[
        {"type":"AtomicAccessor","streamKey":"ELD","field":"mean"},
        {"type":"AtomicAccessor","streamKey":"ELD","field":"stddev"},
        {"type":"AtomicAccessor","streamKey":"ELD","field":"bid"}]}})";

// A pull read on a key the graph never declares (raw `ask`).
constexpr char kUndeclaredPull[] =
    R"({"streamKey":"ELD","field":"ask","node":{"type":"AtomicAccessor","streamKey":"ELD","field":"ask"}})";

// A pull read on an already-declared key (`mean`), built late.
constexpr char kDeclaredPull[] =
    R"({"streamKey":"ELD","field":"ask","node":{"type":"AtomicAccessor","streamKey":"ELD","field":"mean"}})";

// Push requests on nine builtins (each computed and, with elision off, stored
// under a key nothing reads back), a raw push, a Worker pipeline, and the pull
// fan-in. Store off: ask, bid, size and the nine builtins = 12 keys.
// Declared: mean, stddev, bid = 3.
const std::vector<std::string>& requests() {
  static const std::vector<std::string> r = {
      R"({"streamKey":"ELD","field":"ask"})",
      R"({"streamKey":"ELD","field":"mean"})",
      R"({"streamKey":"ELD","field":"stddev"})",
      R"({"streamKey":"ELD","field":"sum"})",
      R"({"streamKey":"ELD","field":"min"})",
      R"({"streamKey":"ELD","field":"max"})",
      R"({"streamKey":"ELD","field":"median"})",
      R"({"streamKey":"ELD","field":"variance"})",
      R"({"streamKey":"ELD","field":"range"})",
      R"({"streamKey":"ELD","field":"count"})",
      R"({"streamKey":"ELD","field":"size","pipeline":[{"type":"Worker","fn":"sum"}]})",
      kPull,
  };
  return r;
}
constexpr std::size_t kOffKeys      = 12;
constexpr std::size_t kDeclaredKeys = 3;

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

// An absent key encodes as one 0xFF byte, distinct from every ArgType index.
void encodeOpt(std::string& o, const std::optional<ArgType>& v) {
  if (!v) { o.push_back(static_cast<char>(0xFF)); return; }
  encode(o, *v);
}

class Sink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx_);
    putStr(bytes, sv.symbol);
    encode(bytes, sv.value);
    putPod(bytes, sv.bucketStartMs);
    ++count;
  }
  void shutdown() noexcept override {}
  std::mutex  mx_;
  std::string bytes;
  std::size_t count = 0;
};

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

using Keys = std::vector<std::pair<std::string, std::string>>;

// One engine: pool, store, Dispatcher, and the graph.
struct Rig {
  explicit Rig(bool elide, std::size_t capFields = 0, int dispatcherFieldCap = 0)
    : pool(std::make_shared<rt::ThreadPool>(kPoolThreads)),
      disp(pool.get(), &store, config(elide, dispatcherFieldCap)) {
    if (capFields) store.setCaps(0, capFields);
    sid = symbols.intern(kSym);
    disp.setSymbolTable(&symbols);
  }
  static util::Config config(bool elide, int dispatcherFieldCap) {
    util::Config c;
    c.storeWriteElision = elide;
    if (dispatcherFieldCap > 0) c.maxFieldsPerSymbol = dispatcherFieldCap;
    return c;
  }
  ~Rig() {
    pool->drain();
    for (auto& c : chains) shut(c);
    pool->drain();
    pool->shutdown();
  }
  static void shut(tree::BuiltChain& c) {
    for (auto& n : c.keepAlive) if (n) n->shutdown();
    if (c.head) c.head->shutdown();
  }
  // Builds one request; returns its index, or throws what the builder threw.
  std::size_t build(const std::string& json) {
    tree::Deps deps;
    deps.store = &store;
    deps.pool = pool.get();
    deps.dispatcher = &disp;
    rapidjson::Document d;
    d.Parse(json.c_str());
    if (d.HasParseError()) throw std::logic_error("request does not parse: " + json);
    auto sink = std::make_shared<Sink>();
    chains.push_back(tree::buildForRequest(d, deps, sink));
    sinks.push_back(sink);
    return sinks.size() - 1;
  }
  void buildGraph() { for (const auto& j : requests()) build(j); }
  void ingest(Ingress how, const Tick& t) {
    if (how == Ingress::OnTick) disp.onTick(jsonEvent(t));
    else { const TickFields f(t); disp.onRecord(sid, f); }
  }
  // The declared keys' bytes, read straight out of the store. The store is
  // written synchronously inside ingress, so this is the state after the tick.
  std::string readKeyBytes(const Keys& keys) const {
    std::string o;
    for (const auto& [k, f] : keys) { putStr(o, k); putStr(o, f); encodeOpt(o, store.get(k, f)); }
    return o;
  }
  std::string storeBytes(const Keys* only = nullptr) const {
    std::string o;
    for (const auto& [k, f, v] : store.snapshot()) {
      if (only && std::find(only->begin(), only->end(), std::make_pair(k, f)) == only->end()) continue;
      putStr(o, k); putStr(o, f); encode(o, v);
    }
    return o;
  }

  std::shared_ptr<rt::ThreadPool>    pool;
  AtomicStore                        store;
  Dispatcher                         disp;
  data::SymbolTable                  symbols;
  data::SymbolId                     sid = 0;
  std::vector<tree::BuiltChain>      chains;
  std::vector<std::shared_ptr<Sink>> sinks;
};

const Keys& declared() {
  static const Keys k = {{kSym, "bid"}, {kSym, "mean"}, {kSym, "stddev"}};
  return k;
}

struct Outcome {
  std::vector<std::string> deliveries;   // per request
  std::vector<std::size_t> counts;
  std::vector<std::string> perTick;      // declared keys after every tick
  std::string              store;        // full sorted snapshot
  std::string              storeDeclared;// snapshot restricted to declared()
  std::size_t              storeEntries = 0;
  Keys                     readKeys;
  bool                     sealed = false;
  std::size_t              declaredPresent = 0;  // declared keys holding a value at the end
};

Outcome drive(bool elide, Ingress how, const std::vector<Tick>& in,
              int dispatcherFieldCap = 0) {
  Rig rig(elide, 0, dispatcherFieldCap);
  rig.buildGraph();
  Outcome r;
  r.readKeys = rig.store.readKeys();
  for (const auto& t : in) {
    rig.ingest(how, t);
    r.perTick.push_back(rig.readKeyBytes(declared()));
  }
  rig.pool->drain();
  for (auto& s : rig.sinks) {
    std::lock_guard<std::mutex> lk(s->mx_);
    r.deliveries.push_back(s->bytes);
    r.counts.push_back(s->count);
  }
  r.store         = rig.storeBytes();
  r.storeDeclared = rig.storeBytes(&declared());
  r.storeEntries  = rig.store.snapshot().size();
  r.sealed        = rig.store.readsSealed();
  for (const auto& [k, f] : declared()) r.declaredPresent += rig.store.get(k, f).has_value();
  return r;
}

// Empty when every delivery and every declared key matches; otherwise the
// first difference.
std::string diff(const Outcome& a, const Outcome& b) {
  for (std::size_t q = 0; q < a.deliveries.size(); ++q)
    if (a.deliveries[q] != b.deliveries[q])
      return "request " + std::to_string(q) + ": " + std::to_string(a.counts[q]) + " vs " +
             std::to_string(b.counts[q]) + " deliveries, bytes differ";
  if (a.perTick.size() != b.perTick.size()) return "tick counts differ";
  for (std::size_t i = 0; i < a.perTick.size(); ++i)
    if (a.perTick[i] != b.perTick[i]) return "declared keys differ after tick " + std::to_string(i);
  if (a.storeDeclared != b.storeDeclared) return "final declared keys differ";
  return {};
}

void expectNonVacuous(const Outcome& r) {
  for (std::size_t q = 0; q < r.counts.size(); ++q)
    ASSERT_GT(r.counts[q], 0u) << "request " << q << " delivered nothing; comparison vacuous";
  // Every declared key must actually hold a value at the end, or comparing it
  // would compare two absences.
  ASSERT_EQ(r.readKeys, declared());
  ASSERT_EQ(r.declaredPresent, kDeclaredKeys) << "a declared key is absent after the last tick";
}

// ---- 1 --------------------------------------------------------------------
TEST(StoreWriteElision, OffByDefaultAndParsedLikeTheOtherEngineFlags) {
  EXPECT_FALSE(util::Config{}.storeWriteElision);
  char path[] = "/tmp/enc1627-config-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  for (const char* v : {"true", "1", "yes"}) {
    { std::ofstream f(path); f << "storeWriteElision=" << v << "\n"; }
    util::Config cfg;
    ASSERT_TRUE(cfg.loadFromFile(path));
    EXPECT_TRUE(cfg.storeWriteElision) << v;
  }
  for (const char* v : {"false", "0", "no", ""}) {
    { std::ofstream f(path); f << "storeWriteElision=" << v << "\n"; }
    util::Config cfg;
    ASSERT_TRUE(cfg.loadFromFile(path));
    EXPECT_FALSE(cfg.storeWriteElision) << "'" << v << "'";
  }
  std::remove(path);
}

// ---- 2 --------------------------------------------------------------------
TEST(StoreWriteElision, OffRecordsTheReadSetAndElidesNothing) {
  const Outcome off = drive(false, Ingress::OnRecord, ticks());
  expectNonVacuous(off);
  EXPECT_EQ(off.readKeys, declared()) << "the set is exposed with the setting off too";
  EXPECT_FALSE(off.sealed) << "nothing may seal the set with the setting off";
  EXPECT_EQ(off.storeEntries, kOffKeys) << "off writes every raw field and every subscribed builtin";

  // And a pull read on a brand-new key, after ingest, is still accepted off.
  Rig rig(false);
  rig.buildGraph();
  for (const auto& t : ticks()) rig.ingest(Ingress::OnTick, t);
  EXPECT_NO_THROW(rig.build(kUndeclaredPull));
}

// ---- 3 --------------------------------------------------------------------
class StoreWriteElisionParity : public ::testing::TestWithParam<Ingress> {};

TEST_P(StoreWriteElisionParity, EveryDeclaredKeyIsBitIdenticalAndTheStoreHoldsStrictlyFewer) {
  const auto in = ticks();
  const Outcome off = drive(false, GetParam(), in);
  expectNonVacuous(off);
  for (int rep = 0; rep < 3; ++rep) {
    const Outcome on = drive(true, GetParam(), in);
    ASSERT_TRUE(on.sealed) << "elision not in force";
    EXPECT_EQ(diff(off, on), "") << "rep " << rep;
    // The on store is EXACTLY the off store restricted to declared keys:
    // nothing extra, nothing missing, every byte the same.
    EXPECT_EQ(on.store, off.storeDeclared) << "rep " << rep;
    EXPECT_EQ(on.storeEntries, kDeclaredKeys);
    EXPECT_LT(on.storeEntries, off.storeEntries);
  }
  // A second off run, so a red above cannot be blamed on off disagreeing
  // with itself.
  EXPECT_EQ(diff(off, drive(false, GetParam(), in)), "");
}

INSTANTIATE_TEST_SUITE_P(BothIngressPaths, StoreWriteElisionParity,
                         ::testing::Values(Ingress::OnTick, Ingress::OnRecord),
                         [](const auto& info) {
                           return info.param == Ingress::OnTick ? std::string("onTick")
                                                                : std::string("onRecord");
                         });

// ---- 4 --------------------------------------------------------------------
TEST(StoreWriteElision, TheComparisonCanFail) {
  const auto in = ticks();
  for (Ingress how : {Ingress::OnTick, Ingress::OnRecord}) {
    const Outcome off = drive(false, how, in);
    expectNonVacuous(off);

    // One ULP on one tick's bid, a raw field a pull node reads.
    auto ulp = in;
    const double was = ulp[kTicks / 2].bid;
    ulp[kTicks / 2].bid = std::nextafter(was, INFINITY);
    ASSERT_NE(std::memcmp(&was, &ulp[kTicks / 2].bid, sizeof was), 0) << "ULP mutation did not apply";
    const Outcome onUlp = drive(true, how, ulp);
    EXPECT_NE(diff(off, onUlp), "") << "a one-ULP change to a declared key went unseen";
    EXPECT_NE(onUlp.perTick[kTicks / 2], off.perTick[kTicks / 2])
        << "the per-tick declared-key comparison must see it on the tick it happened";

    // Two adjacent, distinct ticks swapped.
    auto swp = in;
    std::size_t i = kTicks / 2;
    while (i + 1 < swp.size() && swp[i].bid == swp[i + 1].bid) ++i;
    ASSERT_LT(i + 1, swp.size());
    std::swap(swp[i], swp[i + 1]);
    ASSERT_NE(swp[i].bid, in[i].bid) << "swap mutation did not apply";
    EXPECT_NE(diff(off, drive(true, how, swp)), "") << "an order change went unseen";
  }
}

// ---- 5 --------------------------------------------------------------------
// Late registration. Chosen semantics: REFUSE a new key once the eliding
// Dispatcher has ingested; ACCEPT an already-declared key at any time.
TEST(StoreWriteElision, ALateNewPullReadIsRefusedAndLeavesNothingBehind) {
  const auto in = ticks();
  Rig rig(true);
  rig.buildGraph();
  for (std::size_t n = 0; n < kTicks / 2; ++n) rig.ingest(Ingress::OnTick, in[n]);
  ASSERT_TRUE(rig.store.readsSealed());

  const std::size_t subsBefore = rig.disp.subscriptionCount();
  const Keys readsBefore = rig.store.readKeys();
  try {
    rig.build(kUndeclaredPull);
    FAIL() << "a pull read on an undeclared key was accepted after the seal";
  } catch (const std::runtime_error& ex) {
    EXPECT_NE(std::string(ex.what()).find("storeWriteElision"), std::string::npos) << ex.what();
  }
  EXPECT_EQ(rig.disp.subscriptionCount(), subsBefore) << "the refused build left a subscription";
  EXPECT_EQ(rig.store.readKeys(), readsBefore) << "the refusal changed the read set";
  EXPECT_FALSE(rig.store.declareRead(kSym, "ask"));
  EXPECT_TRUE(rig.store.declareRead(kSym, "mean"));

  // Before the first tick, the same request is accepted and its key written.
  Rig early(true);
  early.buildGraph();
  EXPECT_NO_THROW(early.build(kUndeclaredPull));
  early.ingest(Ingress::OnTick, in[0]);
  EXPECT_TRUE(early.store.get(kSym, "ask").has_value());
}

// Drives `in`, building `late` after tick `at`, and optionally tearing the
// pull fan-in down at `teardownAt` and rebuilding it at `rebuildAt`.
struct LateOutcome { std::string late, rebuilt; };

LateOutcome driveLate(bool elide, const std::vector<Tick>& in, std::size_t at,
                      std::size_t teardownAt, std::size_t rebuildAt) {
  Rig rig(elide);
  rig.buildGraph();
  const std::size_t pull = rig.sinks.size() - 1;
  std::size_t lateIdx = 0, rebuiltIdx = 0;
  for (std::size_t n = 0; n < in.size(); ++n) {
    if (n == at) lateIdx = rig.build(kDeclaredPull);
    if (n == teardownAt) { rig.pool->drain(); Rig::shut(rig.chains[pull]); }
    if (n == rebuildAt) rebuiltIdx = rig.build(kPull);
    rig.ingest(Ingress::OnRecord, in[n]);
    rig.pool->drain();  // so a teardown point is a clean cut in the delivery stream
  }
  rig.pool->drain();
  LateOutcome r;
  { std::lock_guard<std::mutex> lk(rig.sinks[lateIdx]->mx_);    r.late = rig.sinks[lateIdx]->bytes; }
  { std::lock_guard<std::mutex> lk(rig.sinks[rebuiltIdx]->mx_); r.rebuilt = rig.sinks[rebuiltIdx]->bytes; }
  return r;
}

TEST(StoreWriteElision, ALateOrRebuiltPullReadOnADeclaredKeyMatchesOff) {
  const auto in = ticks();
  const std::size_t at = kTicks / 3, down = kTicks / 2, up = (2 * kTicks) / 3;
  const LateOutcome off = driveLate(false, in, at, down, up);
  const LateOutcome on  = driveLate(true,  in, at, down, up);
  ASSERT_FALSE(off.late.empty());
  ASSERT_FALSE(off.rebuilt.empty());
  EXPECT_EQ(on.late, off.late)
      << "a pull read built late on a declared key read something off would not";
  EXPECT_EQ(on.rebuilt, off.rebuilt)
      << "tearing a request down let its keys go stale for the rebuild";
}

// ---- 6 --------------------------------------------------------------------
// Under a store cap, the off run's unread keys can push a declared key out.
// Elision may then GAIN that key; it must never LOSE one the off run had.
TEST(StoreWriteElision, UnderAStoreCapElisionNeverLosesADeclaredKey) {
  const auto in = ticks();
  constexpr std::size_t kCap = 4;
  Rig off(false, kCap), on(true, kCap);
  off.buildGraph();
  on.buildGraph();
  std::size_t offHad = 0;
  for (const auto& t : in) {
    off.ingest(Ingress::OnRecord, t);
    on.ingest(Ingress::OnRecord, t);
    for (const auto& [k, f] : declared()) {
      const auto a = off.store.get(k, f);
      if (!a) continue;
      ++offHad;
      const auto b = on.store.get(k, f);
      ASSERT_TRUE(b.has_value()) << f << ": elision lost a declared key the off run had";
      std::string ea, eb;
      encode(ea, *a);
      encode(eb, *b);
      ASSERT_EQ(ea, eb) << f;
    }
  }
  ASSERT_GT(offHad, 0u) << "vacuous: the off run held no declared key at all";
  // The gain is real in this graph: off fills its 4 slots with ask, bid, size
  // and one builtin, so at least one declared builtin is missing off.
  std::size_t offDeclared = 0, onDeclared = 0;
  for (const auto& [k, f] : declared()) {
    offDeclared += off.store.get(k, f).has_value();
    onDeclared  += on.store.get(k, f).has_value();
  }
  EXPECT_LT(offDeclared, onDeclared);
  EXPECT_EQ(onDeclared, kDeclaredKeys);
}

// The Dispatcher's OWN raw-admission budget (`maxFieldsPerSymbol`) must
// decide exactly as it does off: an elided field still takes its slot, so a
// declared field behind it is refused in both modes, not admitted only on.
// With a budget of 1 the ledger admits `ask` (unread) and refuses `bid`.
TEST(StoreWriteElision, TheDispatcherAdmissionBudgetDecidesAsItDoesOff) {
  const auto in = ticks();
  for (Ingress how : {Ingress::OnTick, Ingress::OnRecord}) {
    const Outcome off = drive(false, how, in, 1);
    const Outcome on  = drive(true,  how, in, 1);
    ASSERT_TRUE(on.sealed);
    EXPECT_EQ(diff(off, on), "");
    EXPECT_EQ(on.store, off.storeDeclared);
  }
}

// ---- 7 --------------------------------------------------------------------
// An IEventComputer that reads the store. Computers run after the raw write.
class ReadsAsk final : public engine::IEventComputer {
public:
  explicit ReadsAsk(std::vector<std::optional<ArgType>>* seen) : seen_(seen) {}
  std::string_view eventType() const override { return Dispatcher::recordEventType(); }
  void compute(const std::string& symbol, const engine::FieldSource&,
               engine::ComputeContext& ctx) override {
    seen_->push_back(ctx.store->get(symbol, "ask"));
  }
private:
  std::vector<std::optional<ArgType>>* seen_;
};

std::vector<std::optional<ArgType>> computerSees(bool elide, bool declare) {
  std::vector<std::optional<ArgType>> seen;
  Rig rig(elide);
  rig.disp.addComputer(std::make_unique<ReadsAsk>(&seen));
  rig.buildGraph();
  if (declare) EXPECT_TRUE(rig.store.declareRead(kSym, "ask"));
  for (std::size_t n = 0; n < 20; ++n) rig.ingest(Ingress::OnRecord, ticks()[n]);
  return seen;
}

TEST(StoreWriteElision, AComputerThatReadsTheStoreMustDeclareItsKeys) {
  const auto off = computerSees(false, false);
  ASSERT_EQ(off.size(), 20u);
  ASSERT_TRUE(off.back().has_value());

  const auto undeclared = computerSees(true, false);
  ASSERT_EQ(undeclared.size(), 20u);
  EXPECT_FALSE(undeclared.back().has_value())
      << "the documented hazard: an undeclared raw key is elided for every reader";

  const auto declaredRead = computerSees(true, true);
  ASSERT_EQ(declaredRead.size(), 20u);
  for (std::size_t i = 0; i < off.size(); ++i) {
    std::string a, b;
    encodeOpt(a, off[i]);
    encodeOpt(b, declaredRead[i]);
    EXPECT_EQ(a, b) << "tick " << i;
  }
}

} // namespace enc1627
