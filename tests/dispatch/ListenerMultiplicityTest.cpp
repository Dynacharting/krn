// ENC-1624 — a value enters its (symbol, field) history ONCE, however many
// Listeners share that field.
//
// `Dispatcher::ingest` used to walk (field, listener) PAIRS and, per pair,
// append the raw value to `_histories[symbol][field]` and call
// `computeAndStoreAtomics`. With N Listeners on `alpha`, every `alpha` value
// entered the history N times and every builtin was recomputed N times over
// that corrupted window, so an atomic's value depended on how many UNRELATED
// subscriptions happened to share its source field. ENC-1487's B2 harness saw
// it as `mean` being delivered three times per record.
//
// What is pinned here, for N in {2, 3}, on BOTH ingress shapes (`onTick` and
// `onRecord`, which share `ingest`):
//
//   * history length — read through the `count` builtin, which returns the
//     window's size — equals the number of values ingested;
//   * every windowed builtin (`mean`, `stddev`, `first`, `sum`) is bit-for-bit
//     the single-Listener result, under a window SHORTER than the run, which
//     is where duplication changes the answer (an unwindowed `mean` of
//     [1,1,2,2] equals that of [1,2], so it alone would not see the defect);
//   * a derived atomic is delivered once per ingested value, not N times;
//   * each raw Listener receives every value once, in ingest order, and the
//     raw Listeners on one field are served in registration order.
//
// `alpha` rather than `price`, so no market computer could pick the field up;
// the engine suite registers none anyway. No thread pool: delivery is inline,
// so the shared log's order is the order `ingest` produced.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/engine/FieldSource.hpp"
#include "krn/engine/SymbolTable.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/util/Config.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <bit>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace krn;

namespace {

constexpr char kSym[]   = "MULTI";
constexpr char kField[] = "alpha";

// The run, and its window. Six values into a window of four: the correct
// window at the end is [4, 8, 16, 32]; with two Listeners it was
// [16, 16, 32, 32]. Powers of two keep every sum exact, so a mismatch is the
// window and never rounding.
const std::vector<double> kValues = {1.0, 2.0, 4.0, 8.0, 16.0, 32.0};
constexpr int kWindow = 4;

// The builtins observed. `count` is the history length.
const std::vector<std::string> kAtomics = {"count", "first", "mean", "stddev",
                                           "sum"};

enum class Path { Tick, Record };

struct Entry {
  std::string who;
  double      value;
};

class Recorder final : public INode {
public:
  Recorder(std::string who, std::vector<Entry>* log)
    : who_(std::move(who)), log_(log) {}
  void onValue(const StreamValue& sv) override {
    double v = 0.0;
    if (std::holds_alternative<double>(sv.value)) v = std::get<double>(sv.value);
    else if (std::holds_alternative<int>(sv.value)) v = std::get<int>(sv.value);
    else ADD_FAILURE() << who_ << " received a non-numeric value";
    log_->push_back({who_, v});
  }
  void shutdown() noexcept override {}
private:
  std::string         who_;
  std::vector<Entry>* log_;
};

class AlphaFields final : public engine::FieldSource {
public:
  explicit AlphaFields(double v) : v_(v) {}
  bool isObject() const noexcept override { return true; }
  std::size_t fieldCountHint() const noexcept override { return 1; }
  bool has(const std::string& name) const override { return name == kField; }
  bool tryGetNumber(const std::string& name, double& out) const override {
    if (name != kField) return false;
    out = v_;
    return true;
  }
  bool tryGetUint64(const std::string&, std::uint64_t&) const override {
    return false;
  }
  void forEachNumber(engine::NumericFieldSink& sink) const override {
    sink.field(kField, v_);
  }
private:
  double v_;
};

Event makeTick(double v) {
  auto doc = std::make_shared<rapidjson::Document>();
  doc->SetObject();
  doc->AddMember(rapidjson::Value(kField, doc->GetAllocator()),
                 rapidjson::Value(v), doc->GetAllocator());
  return Event{kSym, std::move(doc)};
}

struct Outcome {
  std::map<std::string, double> stored;     // builtin -> AtomicStore value
  std::vector<Entry>            log;        // every delivery, in order
};

// Ingest `kValues` with `rawListeners` Listeners on `alpha`, one Listener on
// each observed builtin, and the given window.
Outcome run(int rawListeners, Path path, int window) {
  util::Config cfg;
  cfg.taHistoryMax = window;
  AtomicStore store;
  Outcome out;
  Dispatcher disp(nullptr, &store, cfg);

  std::vector<std::shared_ptr<INode>> keep;
  for (const auto& fn : kAtomics) {
    keep.push_back(std::make_shared<Recorder>(fn, &out.log));
    disp.registerListener(kSym, fn, keep.back());
  }
  for (int i = 0; i < rawListeners; ++i) {
    keep.push_back(std::make_shared<Recorder>("raw" + std::to_string(i),
                                              &out.log));
    disp.registerListener(kSym, kField, keep.back());
  }

  data::SymbolTable symbols;
  const data::SymbolId id = symbols.intern(kSym);
  disp.setSymbolTable(&symbols);

  for (double v : kValues) {
    if (path == Path::Tick) {
      disp.onTick(makeTick(v));
    } else {
      const AlphaFields fields(v);
      disp.onRecord(id, fields);
    }
  }

  for (const auto& fn : kAtomics) {
    auto got = store.get(kSym, fn);
    if (!got) {
      ADD_FAILURE() << "builtin " << fn << " was never stored";
      continue;
    }
    out.stored[fn] = std::holds_alternative<double>(*got)
                         ? std::get<double>(*got)
                         : static_cast<double>(std::get<int>(*got));
  }
  return out;
}

std::uint64_t bits(double d) { return std::bit_cast<std::uint64_t>(d); }

std::size_t deliveriesTo(const Outcome& o, const std::string& who) {
  std::size_t n = 0;
  for (const auto& e : o.log) n += (e.who == who);
  return n;
}

class ListenerMultiplicityTest
    : public ::testing::TestWithParam<std::tuple<int, Path>> {};

// History length equals the ingested count — not N times it.
TEST_P(ListenerMultiplicityTest, HistoryHoldsEachValueOnce) {
  const auto [n, path] = GetParam();
  const Outcome o = run(n, path, /*window=*/1000);
  EXPECT_EQ(o.stored.at("count"), static_cast<double>(kValues.size()))
      << n << " Listeners on one field put each value into its history "
      << o.stored.at("count") / static_cast<double>(kValues.size())
      << " times";
}

// Under a window shorter than the run, every builtin is bit-for-bit the
// single-Listener result, and it is delivered once per value.
TEST_P(ListenerMultiplicityTest, WindowedAtomicsMatchTheSingleListenerCase) {
  const auto [n, path] = GetParam();
  const Outcome one  = run(1, path, kWindow);
  const Outcome many = run(n, path, kWindow);

  // The baseline itself is the textbook answer over [4, 8, 16, 32].
  ASSERT_EQ(one.stored.at("count"), 4.0);
  ASSERT_EQ(one.stored.at("first"), 4.0);
  ASSERT_EQ(one.stored.at("mean"), 15.0);
  ASSERT_EQ(one.stored.at("sum"), 60.0);

  for (const auto& fn : kAtomics) {
    EXPECT_EQ(bits(many.stored.at(fn)), bits(one.stored.at(fn)))
        << fn << " with " << n << " Listeners = " << many.stored.at(fn)
        << ", with 1 Listener = " << one.stored.at(fn);
    EXPECT_EQ(deliveriesTo(many, fn), kValues.size())
        << fn << " was delivered " << deliveriesTo(many, fn) << " times for "
        << kValues.size() << " ingested values";
  }

  // The derived stream each subscriber sees is the single-Listener stream,
  // value for value.
  for (const auto& fn : kAtomics) {
    std::vector<std::uint64_t> a, b;
    for (const auto& e : one.log)  if (e.who == fn) a.push_back(bits(e.value));
    for (const auto& e : many.log) if (e.who == fn) b.push_back(bits(e.value));
    EXPECT_EQ(b, a) << fn << " stream differs from the single-Listener stream";
  }
}

// Each raw Listener gets each value once, in ingest order; within one value
// the raw Listeners are served in registration order.
TEST_P(ListenerMultiplicityTest, RawDeliveryIsOncePerListenerInOrder) {
  const auto [n, path] = GetParam();
  const Outcome o = run(n, path, kWindow);

  std::vector<std::string> rawOrder;
  for (int i = 0; i < n; ++i) {
    const std::string who = "raw" + std::to_string(i);
    std::vector<double> seen;
    for (const auto& e : o.log) if (e.who == who) seen.push_back(e.value);
    EXPECT_EQ(seen, kValues) << who;
  }
  for (const auto& e : o.log) {
    if (e.who.rfind("raw", 0) == 0) rawOrder.push_back(e.who);
  }
  std::vector<std::string> expected;
  for (std::size_t v = 0; v < kValues.size(); ++v) {
    for (int i = 0; i < n; ++i) expected.push_back("raw" + std::to_string(i));
  }
  EXPECT_EQ(rawOrder, expected);
}

INSTANTIATE_TEST_SUITE_P(
    TwoAndThreeListeners, ListenerMultiplicityTest,
    ::testing::Combine(::testing::Values(2, 3),
                       ::testing::Values(Path::Tick, Path::Record)),
    [](const ::testing::TestParamInfo<std::tuple<int, Path>>& info) {
      return std::to_string(std::get<0>(info.param)) + "Listeners_" +
             (std::get<1>(info.param) == Path::Tick ? "onTick" : "onRecord");
    });

}  // namespace
