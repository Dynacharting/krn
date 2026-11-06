// ENC-1293 — a Record-valued terminal is a BUILD-TIME error.
// SPEC specs/2026-09-20-krn-join-correctness D7, §3.
//
// ─────────────────────────────────────────────────────────────────────────────
// THE DEFECT THIS CLOSES IS SILENT, AND IT SPANS TWO REPOS.
//
// `Pack` emits a `krn::Record`. `util::writeArgTypeJson`
// (include/krn/util/JsonUtil.hpp) serializes a Record as a JSON **object** onto
// the live update frame written by `src/server/ClientSession.cpp`'s `sendFn`
// — which is the production outbound path (`src/ws/WSResponder.cpp` is dead
// code: `WSResponder` is constructed only by `src/ws/WsBridge.cpp`, and
// `WsBridge` is constructed only in `tests/ws/WsBridgeTest.cpp`).
//
// embassy then drops it on the floor without a sound. `asFloat32`
// (embassy/internal/gma/types.go) decodes `number | string | boolean` and
// returns false for a JSON object; its caller in `internal/gma/client.go` is
// literally `v, ok := asFloat32(p.Value); if !ok { return }` — no log, no
// metric, no error — and the whole downstream contract is
// `ValueHandler func(value float32)`. The user sees an empty chart and there is
// no diagnostic anywhere, in either repo.
//
// So this file's job is to prove the failure is now LOUD and SINGLE-REPO.
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPORARY. ENC-1295 (embassy) teaches the data path to consume a Record, and
// lifts this restriction. When it lands, delete this file together with the
// `shapeInto` / `recordTerminalMessage` block in `src/core/TreeBuilder.cpp` and
// its call site in `buildForRequest`.
//
// ─────────────────────────────────────────────────────────────────────────────
// BOTH DIRECTIONS ARE ASSERTED, because a check that can only fail gates
// nothing (the lesson ENC-1289 spent a ticket on — `chain.head != nullptr`
// inside a try/catch was a build smoke test wearing a correctness test's name):
//
//   REJECT   — a Record-valued terminal throws, with the specific message that
//              names the reduction to apply. The full WS round-trip proving the
//              resulting `{"type":"error","where":"build",...}` frame reaches
//              the client lives in `tests/ws/ClientSessionTest.cpp`
//              (`SubscribeRejectsRecordValuedTerminal`), next to the harness.
//   ACCEPT   — `Pack -> Field -> Responder` still builds AND still emits, with
//              the emitted values pinned exactly. Build success alone is not
//              accepted as proof here.
//   NEUTRAL  — all 272 checked-in corpus requests still build. If this ever
//              starts refusing one, that entry's chart is silently empty today
//              and the number belongs in the SPEC.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/rt/ThreadPool.hpp"

#include "../support/CorpusPath.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>

#include <fstream>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

using namespace krn;

// ─── Recording terminal ──────────────────────────────────────────────────────
// Records the *shape* as well as the value, so "it emitted something" can never
// be mistaken for "it emitted a number".
class Sink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx_);
    if (const double* d = std::get_if<double>(&sv.value)) doubles_.push_back(*d);
    else if (std::get_if<Record>(&sv.value))              ++records_;
    else                                                  ++other_;
  }
  void shutdown() noexcept override {}

  std::vector<double> doubles() const {
    std::lock_guard<std::mutex> lk(mx_); return doubles_;
  }
  std::size_t records() const { std::lock_guard<std::mutex> lk(mx_); return records_; }
  std::size_t other()   const { std::lock_guard<std::mutex> lk(mx_); return other_;   }

private:
  mutable std::mutex  mx_;
  std::vector<double> doubles_;
  std::size_t         records_{0};
  std::size_t         other_{0};
};

// ─── Fixture ────────────────────────────────────────────────────────────────
// One worker thread: nothing here is a race test, and a single FIFO worker
// makes `Dispatcher::onTick`'s notification order deterministic (the inner
// listener container is a std::map, so "ask" precedes "bid").
class RecordTerminal : public ::testing::Test {
protected:
  void SetUp() override {
    prevPool_   = gThreadPool;
    pool_       = std::make_shared<rt::ThreadPool>(1);
    gThreadPool = pool_;
    dispatcher_ = std::make_unique<Dispatcher>(pool_.get(), &store_);
    deps_.store      = &store_;
    deps_.pool       = pool_.get();
    deps_.dispatcher = dispatcher_.get();
  }

  void TearDown() override {
    dispatcher_.reset();
    if (pool_) pool_->shutdown();
    pool_.reset();
    gThreadPool = prevPool_;
  }

  // Build `json` as a request. Returns the thrown message, or "" on success.
  // Always tears the chain down so no Listener/Interval thread outlives it.
  std::string buildAndReport(const char* json) {
    rapidjson::Document d;
    d.Parse(json);
    EXPECT_FALSE(d.HasParseError()) << "test JSON is malformed: " << json;
    auto sink = std::make_shared<Sink>();
    try {
      auto chain = tree::buildForRequest(d, deps_, sink);
      for (auto& n : chain.keepAlive) if (n) n->shutdown();
      if (chain.head) chain.head->shutdown();
      return "";
    } catch (const std::exception& ex) {
      return ex.what();
    }
  }

  void tick(const char* symbol,
            std::initializer_list<std::pair<const char*, double>> fields) {
    auto payload = std::make_shared<rapidjson::Document>();
    payload->SetObject();
    auto& al = payload->GetAllocator();
    for (const auto& [name, value] : fields)
      payload->AddMember(rapidjson::StringRef(name), value, al);
    Event ev;
    ev.symbol  = symbol;         // ev.type defaults to "tick" — the production type
    ev.payload = payload;
    dispatcher_->onTick(ev);
  }

  AtomicStore                   store_;
  std::shared_ptr<rt::ThreadPool> pool_;
  std::shared_ptr<rt::ThreadPool> prevPool_;
  std::unique_ptr<Dispatcher>   dispatcher_;
  tree::Deps                    deps_;
};

// Every rejection must carry the reduction to apply and say it is temporary —
// an error the reader cannot act on is barely better than the silent drop.
void expectCanonicalRejection(const std::string& msg, const char* what) {
  ASSERT_FALSE(msg.empty()) << what << " built successfully; it must not";
  EXPECT_NE(msg.find("Record"), std::string::npos)      << what << ": " << msg;
  EXPECT_NE(msg.find("Pack -> Field"), std::string::npos)
      << what << " must name the Field reduction: " << msg;
  EXPECT_NE(msg.find("Pack -> Expr"), std::string::npos)
      << what << " must name the Expr reduction: " << msg;
  EXPECT_NE(msg.find("TEMPORARY"), std::string::npos)
      << what << " must say the restriction is temporary: " << msg;
  EXPECT_NE(msg.find("ENC-1295"), std::string::npos)
      << what << " must name the ticket that lifts it: " << msg;
}

// ═══ DIRECTION 1 — REJECT ════════════════════════════════════════════════════

// The flagship case: the shape the join work makes attractive.
TEST_F(RecordTerminal, PackAtTheTerminalIsRejected) {
  const std::string msg = buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]
  })");
  expectCanonicalRejection(msg, "Pack -> Responder");
}

// The same node reached through `node` rather than `pipeline`. With NO
// pipeline the `node` subtree's output is what reaches the terminal, so it is
// still analysed after ENC-1290 narrowed the check to the composed tail.
// `ComposedChainTest.NodePackPipelineFieldIsAcceptedAndEmits` is the other
// half: once a pipeline IS present, the node feeds the pipeline and not the
// terminal, and `node:Pack` + `pipeline:[Field]` must be ACCEPTED.
TEST_F(RecordTerminal, PackUnderNodeKeyIsRejected) {
  const std::string msg = buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}
  })");
  expectCanonicalRejection(msg, "node:Pack -> Responder");
}

// `Filter` forwards its input unchanged, so it does not launder a Record.
TEST_F(RecordTerminal, PackBehindAPassThroughFilterIsRejected) {
  const std::string msg = buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[
      {"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}},
      {"type":"Filter","when":{"op":"gt","args":[{"ref":"bid"},0]}}]
  })");
  expectCanonicalRejection(msg, "Pack -> Filter -> Responder");
}

// A `Tee` builds EVERY branch into the shared downstream, so one Record-valued
// branch poisons the terminal even when its sibling is a clean scalar.
TEST_F(RecordTerminal, TeeWithOneRecordBranchIsRejected) {
  const std::string msg = buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Tee","outputs":[
      {"type":"Worker","fn":"last"},
      {"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]}]
  })");
  expectCanonicalRejection(msg, "Tee{scalar, Pack} -> Responder");
}

// The reject must happen BEFORE anything is constructed: the Listener,
// Interval and BucketTime builders call start() and spawn threads, so a reject
// after the fact would leak live work for a request that is never served.
// A Pack fed by an Interval-driven subtree is the cheap way to notice: if the
// analysis ran after the build, this would leave a timer thread ticking.
TEST_F(RecordTerminal, RejectionHappensBeforeAnyNodeIsConstructed) {
  const std::string msg = buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Pack","fields":{
        "a":{"type":"Interval","ms":10,
             "child":{"type":"AtomicAccessor","streamKey":"AAPL","field":"bid"}},
        "b":{"type":"Listener","streamKey":"AAPL","field":"ask"}}}]
  })");
  expectCanonicalRejection(msg, "Pack{Interval,...} -> Responder");
}

// ═══ DIRECTION 2 — ACCEPT, AND EMIT ══════════════════════════════════════════

// The acceptance criterion, in full. Build success alone is NOT the assertion:
// real ticks go in through `Dispatcher::onTick` and the emitted value is
// pinned exactly.
TEST_F(RecordTerminal, PackFieldResponderBuildsAndEmitsTheExactValue) {
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[
      {"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}},
      {"type":"Field","name":"bid"}]
  })";

  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink))
      << "Pack -> Field -> Responder must still build; the restriction is on "
         "Record-valued TERMINALS, not on Pack";
  ASSERT_NE(chain.head, nullptr);

  // One tick carrying both sides. `Pack` completes on the field that arrives
  // last, so exactly one Record is assembled and exactly one `bid` is
  // projected out of it. Driving `bid`/`ask` only leaves the outer
  // Listener(AAPL,lastPrice) silent, so the terminal stream is unambiguously
  // the pipeline's output (the same sidestep ENC-1289 uses for SPEC defect 1).
  tick("AAPL", {{"bid", 1000.00}, {"ask", 1000.02}});
  pool_->drain();

  auto vals = sink->doubles();
  EXPECT_EQ(sink->records(), 0u)
      << "a Record reached the terminal — the rejection is not doing its job";
  EXPECT_EQ(sink->other(), 0u);
  ASSERT_EQ(vals.size(), 1u) << "expected exactly one projected value";
  EXPECT_DOUBLE_EQ(vals[0], 1000.00)
      << "Field{name:'bid'} must project the bid, not the ask";

  // A second tick re-completes the record twice (ask arrives against the
  // retained bid, then bid arrives) — last-value-wins, which is Pack's
  // documented behaviour. Both projections are the bid at that moment.
  tick("AAPL", {{"bid", 1001.00}, {"ask", 1001.02}});
  pool_->drain();

  vals = sink->doubles();
  ASSERT_EQ(vals.size(), 3u) << "Pack emits once per arrival once complete";
  EXPECT_DOUBLE_EQ(vals[1], 1000.00) << "ask arrived against the retained bid";
  EXPECT_DOUBLE_EQ(vals[2], 1001.00) << "then the new bid completed the record";
  EXPECT_EQ(sink->records(), 0u);

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  chain.head->shutdown();
}

// The other canonical reduction named in the error message. Kept separate so a
// regression in `Expr` cannot be read as a regression in the rejection.
TEST_F(RecordTerminal, PackExprResponderBuildsAndEmitsTheExactValue) {
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[
      {"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}},
      {"type":"Expr","expr":{"op":"sub","args":[{"ref":"ask"},{"ref":"bid"}]}}]
  })";

  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink));
  ASSERT_NE(chain.head, nullptr);

  tick("AAPL", {{"bid", 1000.00}, {"ask", 1000.02}});
  pool_->drain();

  auto vals = sink->doubles();
  EXPECT_EQ(sink->records(), 0u);
  ASSERT_EQ(vals.size(), 1u);
  EXPECT_NEAR(vals[0], 0.02, 1e-9)
      << "Pack -> Expr must reduce the record to the spread";

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  chain.head->shutdown();
}

// A plain scalar pipeline is untouched — the narrowest possible control that
// the rejection is not simply refusing everything.
TEST_F(RecordTerminal, OrdinaryScalarRequestIsUnaffected) {
  EXPECT_EQ(buildAndReport(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Worker","fn":"last"}]
  })"), "");
}

// ═══ DIRECTION 3 — THE REAL CORPUS ═══════════════════════════════════════════
//
// `AllCorpusRequestsBuild` (tests/treebuilder/CorpusTest.cpp) already fails if
// any of the 272 entries stops building. This test says the quantity out loud
// so the number is in the record rather than inferred from a green suite, and
// NAMES any entry it refuses.
//
// Measured when this landed: 0 of 272. The corpus authors only Worker,
// AtomicAccessor, Listener, Aggregate, Interval and SymbolSplit — it constructs
// no Record anywhere. A non-zero count here is a significant finding: it means
// checked-in corpus entries are rendering silently empty charts today, and it
// belongs on ENC-1293 and in the SPEC.
TEST_F(RecordTerminal, NoCheckedInCorpusRequestIsRefused) {
  // ENC-1340: one resolver, one diagnostic — tests/support/CorpusPath.hpp.
  std::ifstream ifs = krn::testsupport::openCorpusRequests();
  ASSERT_TRUE(ifs.is_open())
      << "a missing corpus must not silently green this gate (ENC-807 L17).\n"
      << krn::testsupport::corpusNotFoundDiagnostic();

  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);
  ifs.close();
  ASSERT_FALSE(doc.HasParseError());
  ASSERT_TRUE(doc.IsArray());
  ASSERT_EQ(doc.Size(), 272u)
      << "the corpus changed size; re-measure the refusal count and update "
         "ENC-1293 / the SPEC";

  int refused = 0;
  for (const auto& entry : doc.GetArray()) {
    if (!entry.IsObject() || !entry.HasMember("request")) continue;
    const int id = (entry.HasMember("corpus_id") && entry["corpus_id"].IsInt())
                     ? entry["corpus_id"].GetInt() : -1;
    auto sink = std::make_shared<Sink>();
    try {
      auto chain = tree::buildForRequest(entry["request"], deps_, sink);
      for (auto& n : chain.keepAlive) if (n) n->shutdown();
      if (chain.head) chain.head->shutdown();
    } catch (const std::exception& ex) {
      const std::string what = ex.what();
      // Only OUR rejection counts here; any other build failure is
      // AllCorpusRequestsBuild's business and is reported there.
      if (what.find("would receive a Record") == std::string::npos) continue;
      ++refused;
      ADD_FAILURE() << "corpus_id " << id << " is refused by the ENC-1293 "
                       "Record-terminal rejection: " << what;
    }
  }
  EXPECT_EQ(refused, 0)
      << refused << " checked-in corpus request(s) terminate in a Record and "
                    "render a silently empty chart today — record this on "
                    "ENC-1293 and in the SPEC before changing this expectation";
}

} // namespace
