// ENC-1290 — `node` and `pipeline` compose into ONE chain, and the outer
// `Listener(streamKey, field)` is that chain's CLOCK.
//
// SPEC specs/2026-09-20-krn-join-correctness D5 and §5 Q1 (ruled 2026-09-20 by
// ENC-1317; the rule's first wording was broken by that ticket's own
// adversarial review and replaced — Corrections C4.0).
//
// ─────────────────────────────────────────────────────────────────────────────
// THE SHAPE THIS FILE GATES
//
//     Listener(streamKey, field) -> node subtree -> pipeline stages -> terminal
//
// and nothing reaches the terminal except through the pipeline. Before this,
// `buildForRequest` wired the `node` subtree straight to the terminal and then
// restarted from the terminal for the pipeline, leaving BOTH chains live on one
// Responder: the client got two unrelated streams interleaved on one request
// key and the pipeline never saw the values it exists to reduce (SPEC §1.1
// defect 1). 52 of the 272 checked-in corpus requests carry both keys.
//
// ─────────────────────────────────────────────────────────────────────────────
// THE PART THAT IS EASY TO GET BACKWARDS, AND WHICH ONE TEST HERE EXISTS FOR
//
// The head Listener is ALWAYS built. For a FAN-IN node it is not a member of
// the join — the members are the node's own declared `inputs` — so its value
// must never enter `Aggregate::buf_`. `CompositeRoot` forwards it to an
// explicit `clockTargets` set and to nothing else, and that set is filled from
// DECLARED INPUT HEADS ONLY, and only those with no `Listener` in their
// subtree.
//
// The rule's first published wording — "forward to the roots that are not
// themselves Dispatcher-subscribed" — reads plausibly and is WRONG, because
// `roots_` is a lifecycle list that contains the `Aggregate` itself. Under it
// the clock's `lastPrice` lands in `buf_[sv.symbol]` as a join member, which is
// SPEC §1.1 defect 2 manufactured inside the builder.
// `ClockIsNeverAJoinMember` below fails under that implementation and passes
// under this one. It is the only reason that distinction is checkable.
//
// ENC-1291 NOTE: what makes it fail changed. `Aggregate::onValue` is now a
// warn-and-drop (SPEC D2), so the clock reaching the fan-in no longer corrupts
// the output and the value comparison alone could not fail any more. The gate
// is now `Aggregate::pipelineEdgeValues()`, asserted not to move. See the block
// above the test.
//
// ─────────────────────────────────────────────────────────────────────────────
// SCOPE — FAN-IN NODES ONLY
//
// For a single-input transform node (Worker, AtomicAccessor, Interval,
// GroupSplit) the outer Listener IS the data source, before and after. All 162
// `node`-only corpus requests are in that category and NONE of them builds a
// `CompositeRoot`. `NodeOnlyCorpusRequestsAreUnaffected` proves the scope
// clause holds across all 162 rather than asserting it in prose.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT THIS FILE DOES **NOT** CLAIM
//
// That corpus 86 now reports the 0.02 spread. It does not, and D5 was never
// going to make it — see `Corpus86ComposedChainEmitsTheExactMeasuredValues`,
// whose pinned values are the DEFECT, recorded so the next ticket inherits a
// measurement instead of a guess (SPEC §5 Q5, open; ENC-1291/ENC-1005 own it).

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/nodes/Aggregate.hpp"
#include "krn/nodes/BucketTime.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/nodes/Interval.hpp"
#include "krn/rt/ThreadPool.hpp"

#include "../support/CorpusPath.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

using namespace krn;

// ─── Recording terminal ─────────────────────────────────────────────────────
class Sink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx_);
    if (const double* d = std::get_if<double>(&sv.value)) vals_.push_back(*d);
    else                                                  ++nonNumeric_;
  }
  void shutdown() noexcept override {}

  std::vector<double> values() const {
    std::lock_guard<std::mutex> lk(mx_); return vals_;
  }
  std::size_t nonNumeric() const {
    std::lock_guard<std::mutex> lk(mx_); return nonNumeric_;
  }

private:
  mutable std::mutex  mx_;
  std::vector<double> vals_;
  std::size_t         nonNumeric_{0};
};

std::string render(const std::vector<double>& v) {
  std::string s = "[";
  for (std::size_t i = 0; i < v.size(); ++i) {
    if (i) s += ", ";
    s += std::to_string(v[i]);
  }
  return s + "]";
}

std::string toJson(const rapidjson::Value& v) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);
  v.Accept(w);
  return sb.GetString();
}

// ─── Corpus access ──────────────────────────────────────────────────────────
// Same search paths as CorpusTest / RecordTerminalTest: CMake copies the
// corpus next to the binary. A missing corpus must be LOUD, never a skip
// (ENC-807 L17), so every caller ASSERTs on this.
rapidjson::Document& corpusDoc() {
  static rapidjson::Document doc = [] {
    // ENC-1340: one resolver, and it no longer depends on the cwd —
    // tests/support/CorpusPath.hpp. A null Document still means "not found",
    // and callers print corpusNotFoundDiagnostic() rather than guessing.
    rapidjson::Document d;
    std::ifstream ifs = krn::testsupport::openCorpusRequests();
    if (!ifs.is_open()) { d.SetNull(); return d; }
    rapidjson::IStreamWrapper isw(ifs);
    d.ParseStream(isw);
    return d;
  }();
  return doc;
}

const rapidjson::Value* corpusRequest(int corpusId, int key = 1) {
  rapidjson::Document& d = corpusDoc();
  if (d.IsNull() || d.HasParseError() || !d.IsArray()) return nullptr;
  for (auto& e : d.GetArray()) {
    if (!e.IsObject() || !e.HasMember("corpus_id") || !e["corpus_id"].IsInt()) continue;
    if (e["corpus_id"].GetInt() != corpusId) continue;
    if (!e.HasMember("request")) continue;
    const auto& rq = e["request"];
    const int k = (rq.HasMember("key") && rq["key"].IsInt()) ? rq["key"].GetInt() : 1;
    if (k != key) continue;
    return &rq;
  }
  return nullptr;
}

bool hasNode(const rapidjson::Value& rq) {
  return rq.HasMember("node") && rq["node"].IsObject();
}
bool hasPipeline(const rapidjson::Value& rq) {
  for (const char* k : {"pipeline", "stages"})
    if (rq.HasMember(k) && rq[k].IsArray()) return true;
  return false;
}

// ─── Fixture ────────────────────────────────────────────────────────────────
// One worker thread throughout. Nothing in this file is a race test, and a
// single FIFO worker makes `Dispatcher::onTick`'s notification order
// deterministic (the per-symbol listener container is a std::map, so field
// names are notified in lexicographic order: "ask" before "bid" before
// "lastPrice").
class ComposedChain : public ::testing::Test {
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

  void tick(const char* symbol,
            std::initializer_list<std::pair<const char*, double>> fields) {
    auto payload = std::make_shared<rapidjson::Document>();
    payload->SetObject();
    auto& al = payload->GetAllocator();
    for (const auto& [name, value] : fields)
      payload->AddMember(rapidjson::StringRef(name), value, al);
    Event ev;
    ev.symbol  = symbol;          // ev.type defaults to "tick" — production type
    ev.payload = payload;
    dispatcher_->onTick(ev);
  }

  void tick(const std::string& symbol,
            const std::vector<std::pair<std::string, double>>& fields) {
    auto payload = std::make_shared<rapidjson::Document>();
    payload->SetObject();
    auto& al = payload->GetAllocator();
    for (const auto& [name, value] : fields)
      payload->AddMember(rapidjson::Value(name.c_str(), al).Move(),
                         rapidjson::Value(value).Move(), al);
    Event ev;
    ev.symbol  = symbol;
    ev.payload = payload;
    dispatcher_->onTick(ev);
  }

  AtomicStore                     store_;
  std::shared_ptr<rt::ThreadPool> pool_;
  std::shared_ptr<rt::ThreadPool> prevPool_;
  std::unique_ptr<Dispatcher>     dispatcher_;
  tree::Deps                      deps_;
};

// ═══════════════════════════════════════════════════════════════════════════
// 1. THE COMPOSITION ITSELF — corpus 86, end to end, values pinned
// ═══════════════════════════════════════════════════════════════════════════
//
// corpus_id 86 "Bid-ask spread for AAPL", verbatim:
//   {streamKey:AAPL, field:lastPrice,
//    node: Aggregate{arity:2, inputs:[Listener(AAPL,ask), Listener(AAPL,bid)]},
//    pipeline: [Worker{fn:"diff"}]}
//
// BEFORE: the `Aggregate` fed the terminal directly, so ticking bid/ask put the
// RAW PRICES (1000.02, 1000.00, …) on the wire and `Worker{fn:"diff"}` — on its
// own dead chain behind Listener(AAPL,lastPrice) — never ran.
//
// AFTER: every value reaches the terminal THROUGH the Worker. No raw price can
// appear at all, which is the structural half of the assertion; the exact
// sequence is the other half.
//
// THE PINNED VALUES ARE THE DEFECT, NOT THE ANSWER. The truth on every tick is
// a +0.02 spread; the chain emits 0.00, -0.02, 1.00, 0.98, 2.00, 1.98. Three
// lines cause it and none of them is D5's to fix:
//   * `Aggregate::onValue` forwards a completed batch MEMBER BY MEMBER
//     (src/nodes/Aggregate.cpp), so the Worker sees two separate values, never
//     a pair;
//   * `Worker::onValue` runs its fn over a per-symbol accumulator cleared only
//     in shutdown() (src/nodes/Worker.cpp), so it never forgets tick 0;
//   * `diff` is `v.back() - v.front()` (src/core/BuiltinFunctions.cpp).
// That is SPEC §5 Q5 — filed by ENC-1317, still OPEN. Pinning the measurement
// here is deliberate: the next ticket inherits a number it can diff against
// rather than a prediction. WHEN Q5 IS RULED AND FIXED, this expectation
// changes and this comment comes with it.
TEST_F(ComposedChain, Corpus86ComposedChainEmitsTheExactMeasuredValues) {
  const rapidjson::Value* req = corpusRequest(86);
  ASSERT_NE(req, nullptr) << "corpus_id 86 not found in corpus_requests.json";
  ASSERT_TRUE(hasNode(*req) && hasPipeline(*req))
      << "corpus_id 86 must carry BOTH keys for this test to mean anything: "
      << toJson(*req);

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(*req, deps_, sink));
  ASSERT_NE(chain.head, nullptr);

  constexpr double kBase = 1000.00, kSpread = 0.02;
  for (int n = 0; n < 3; ++n)
    tick("AAPL", {{"bid", kBase + n}, {"ask", kBase + n + kSpread}});
  pool_->drain();

  const auto vals = sink->values();
  EXPECT_EQ(sink->nonNumeric(), 0u);

  // Structural: nothing may reach the terminal except through the pipeline.
  // A raw price is two orders of magnitude above any difference of two of
  // them, so "did a raw price arrive?" is decidable by value alone.
  for (double v : vals)
    EXPECT_LT(v, 900.0)
        << "a RAW input value (" << v << ") reached the terminal. The `node` "
           "subtree is still wired around the pipeline instead of through it — "
           "SPEC §1.1 defect 1. Got " << render(vals);

  const std::vector<double> kExpected = {0.00, -0.02, 1.00, 0.98, 2.00, 1.98};
  ASSERT_EQ(vals.size(), kExpected.size())
      << "expected six arrivals (three ticks x a two-member batch forwarded "
         "one at a time), got " << render(vals);
  for (std::size_t i = 0; i < kExpected.size(); ++i)
    EXPECT_NEAR(vals[i], kExpected[i], 1e-9)
        << "arrival " << i << " of " << render(vals)
        << "\n    These values are WRONG — the spread is 0.02 on every tick — "
           "and they are pinned\n    because SPEC §5 Q5 is open, not because "
           "they are correct. See this test's comment.";

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  chain.head->shutdown();
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. THE CLOCK RULE — the head Listener must never become a join member
// ═══════════════════════════════════════════════════════════════════════════
//
// Same request, same ticks, but each tick ALSO carries `lastPrice` — the outer
// Listener's own field — with a value no bid or ask ever takes. Because both of
// the Aggregate's declared inputs carry Listeners, the clock's forwarding set
// is EMPTY and the head Listener stays inert: the emitted sequence must be
// identical to the test above, to the last bit.
//
// THIS IS THE REFUTATION TEST. Implement the rule's retracted wording —
// "CompositeRoot forwards to the roots that are not themselves
// Dispatcher-subscribed" — and `roots_` hands you the `Aggregate` itself, so
// 7777 is pushed into `buf_["AAPL"]` as a join member, every batch shifts, and
// this test fails. Delete the `clockTargets` filter entirely and it fails the
// same way. There is no other test in the suite that separates the two rules.
//
// ENC-1291 RE-ARMED THIS TEST AFTER DISARMING IT. Read this before touching it.
//
// As ENC-1290 wrote it, the gate was the value comparison below: forward the
// clock to `roots_` (which holds the `Aggregate` itself) and `lastPrice` lands
// in `Aggregate::buf_` as a join member, corrupting the six arrivals. That
// worked because a stray value reaching the fan-in CHANGED THE OUTPUT.
//
// SPEC D2 (ENC-1291) made `Aggregate::onValue` a warn-and-drop, so a stray
// value reaching the fan-in now changes nothing observable — and the exact
// retracted-wording mutation (`clockTargets.emplace_back(agg);` after
// `roots.push_back(agg);` in the Aggregate builder) produced output
// BIT-IDENTICAL to baseline. The gate was silently unable to fail. Measured by
// an adversarial pass, not noticed in review.
//
// So the assertion is now in two parts, and the FIRST is the one that gates
// the clock rule:
//
//   1. `Aggregate::pipelineEdgeValues()` must not move. The clock must not
//      reach the fan-in AT ALL — not "must not corrupt it". This is directional
//      against the retracted wording and stays directional however defensively
//      `Aggregate::onValue` behaves.
//   2. The six values, unchanged from ENC-1290. Kept because it pins the
//      arithmetic as well as the wiring.
//
TEST_F(ComposedChain, ClockIsNeverAJoinMember) {
  const rapidjson::Value* req = corpusRequest(86);
  ASSERT_NE(req, nullptr);

  const std::size_t edgeBefore = Aggregate::pipelineEdgeValues();

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(*req, deps_, sink));

  constexpr double kBase = 1000.00, kSpread = 0.02, kClock = 7777.0;
  for (int n = 0; n < 3; ++n)
    tick("AAPL", {{"bid", kBase + n},
                  {"ask", kBase + n + kSpread},
                  {"lastPrice", kClock}});   // the CLOCK's own field
  pool_->drain();

  const std::vector<double> kExpected = {0.00, -0.02, 1.00, 0.98, 2.00, 1.98};
  const auto vals = sink->values();
  EXPECT_EQ(sink->nonNumeric(), 0u);

  // ── PART 1: the clock never reaches the fan-in at all (ENC-1291) ──────────
  EXPECT_EQ(Aggregate::pipelineEdgeValues(), edgeBefore)
      << (Aggregate::pipelineEdgeValues() - edgeBefore)
      << " value(s) reached an `Aggregate` on its PIPELINE edge while driving "
         "corpus 86.\n"
         "    The head Listener(AAPL,lastPrice) is the chain's CLOCK. Both of "
         "corpus 86's declared inputs\n"
         "    carry Listeners of their own, so the clock's forwarding set is "
         "EMPTY and `lastPrice` must\n"
         "    reach nothing. A non-zero count means `CompositeRoot` is "
         "forwarding the clock to something\n"
         "    that is not a declared input head — almost certainly `roots_`, "
         "which holds the Aggregate\n"
         "    ITSELF. That is the RETRACTED first wording of SPEC section 5 "
         "Q1's rule (ENC-1317 C4.0),\n"
         "    and it is SPEC section 1.1 defect 2 manufactured inside the "
         "builder.\n"
         "    Since ENC-1291 the value is DROPPED rather than buffered, so it "
         "no longer corrupts the\n"
         "    numbers below — which is exactly why this counter, and not those "
         "numbers, is the gate.";

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  if (chain.head) chain.head->shutdown();

  ASSERT_EQ(vals.size(), kExpected.size())
      << "driving the outer Listener's own field changed the ARRIVAL COUNT: "
      << render(vals) << " vs the clock-silent run "
      << render(kExpected)
      << "\n    The head Listener(AAPL,lastPrice) is the chain's CLOCK. Both of "
         "corpus 86's declared\n    inputs carry Listeners of their own, so the "
         "clock's forwarding set is EMPTY and\n    `lastPrice` must contribute "
         "NOTHING.\n"
         "    If this fails, the clock is being forwarded to something that is "
         "not a declared\n    input head — almost certainly `roots_`, which "
         "holds the Aggregate ITSELF. That is\n    SPEC §1.1 defect 2 "
         "manufactured inside the builder (ENC-1317 Corrections C4.0).";
  for (std::size_t i = 0; i < kExpected.size(); ++i)
    EXPECT_NEAR(vals[i], kExpected[i], 1e-9)
        << "arrival " << i << ": the clock value " << kClock
        << " has entered the join. Got " << render(vals);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. THE SEVEN PULL-ONLY JOINS — dead today, clocked from now on
// ═══════════════════════════════════════════════════════════════════════════
//
// corpus_id 111 "Bollinger bandwidth for AAPL — upper minus lower band":
//   node: Aggregate{arity:2, inputs:[AtomicAccessor(AAPL,bollinger_upper),
//                                    AtomicAccessor(AAPL,bollinger_lower)]}
//   pipeline: [Worker{fn:"diff"}]
//
// An `AtomicAccessor` is a PULL node: it has no Dispatcher subscription and
// only samples the store when something calls its `onValue`. Nothing ever did.
// So this entry — and ids 112, 113, 114, 115, 192, 200 with it — was not "two
// live chains interleaved" (SPEC §1.1 defect 1's stated symptom, corrected by
// ENC-1317's C2.1); it was ONE live chain and a join that never fired once.
// The worse failure mode: a plausible number instead of a visibly wrong one.
//
// With the head Listener as its clock the join fires for the first time.
TEST_F(ComposedChain, Corpus111PullOnlyJoinFiresForTheFirstTime) {
  const rapidjson::Value* req = corpusRequest(111);
  ASSERT_NE(req, nullptr) << "corpus_id 111 not found in corpus_requests.json";

  store_.set("AAPL", "bollinger_upper", 110.0);
  store_.set("AAPL", "bollinger_lower", 90.0);

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(*req, deps_, sink));

  tick("AAPL", {{"lastPrice", 123.45}});     // one clock tick, nothing else
  pool_->drain();

  const auto vals = sink->values();
  ASSERT_FALSE(vals.empty())
      << "corpus_id 111's join emitted NOTHING. Both of its declared inputs are "
         "AtomicAccessors —\n    pull nodes with no Dispatcher subscription — so "
         "without the head Listener clocking them\n    this request is inert, "
         "which is what it was before ENC-1290 (SPEC §5 Q1, probe S2).";

  // One clock tick samples both accessors under symbol "AAPL", completing one
  // arity-2 batch. `Aggregate` forwards it member by member into
  // `Worker{fn:"diff"}` (= back - front over a never-cleared accumulator), so:
  //   upper 110 arrives -> acc [110]      -> 110 - 110 =   0
  //   lower  90 arrives -> acc [110, 90]  ->  90 - 110 = -20
  const std::vector<double> kExpected = {0.0, -20.0};
  ASSERT_EQ(vals.size(), kExpected.size()) << render(vals);
  for (std::size_t i = 0; i < kExpected.size(); ++i)
    EXPECT_NEAR(vals[i], kExpected[i], 1e-9) << "arrival " << i << " of " << render(vals);

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  if (chain.head) chain.head->shutdown();
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. THE STRUCTURAL SWEEP — all 52, and the ONE path to the terminal
// ═══════════════════════════════════════════════════════════════════════════
//
// The generic statement of D5 is "nothing reaches the terminal except through
// the pipeline", and it is checkable without knowing a single entry's
// semantics: append an always-false `Filter` to the pipeline and the terminal
// must go completely silent. Under the old two-chain wiring the `node` subtree
// bypassed the pipeline entirely, so it kept emitting straight past the filter.
//
// Two runs per entry:
//   A. verbatim              -> did this entry emit anything at all? Without
//                               this, B passes vacuously for a chain that was
//                               simply dead.
//   B. + always-false Filter -> exactly zero arrivals, FOR ALL 52. This is the
//                               D5 property, and it is true of any correct
//                               engine, now or later.
//
// A's floor is deliberately **29, not 52**, and that is not slack. 29 of the 52
// are same-`streamKey` joins; the other 23 are cross-`streamKey` and emit today
// ONLY because `Aggregate::buf_` is keyed on `sv.symbol` and counts values
// rather than ports — SPEC §1.1 defects 2 and 3, i.e. the very thing ENC-1291
// removes. The moment per-port arity is enforced those 23 emit nothing until
// ENC-1292's `by:"none"` exists, so `EXPECT_EQ(silentA, 0)` here would be a
// landmine planted in the path of the next two tickets in this project. 29 can
// only go up as the join is fixed. The actual count is printed either way, so
// a real regression is still visible in the message.
//
// ENC-1292 HAS LANDED AND THE COUNT IS STILL EXACTLY 29. That is the correct
// outcome, not a shortfall, and it is worth saying because "the cross-symbol
// join now works, so this should be 52" is the natural misreading. This test
// drives every entry VERBATIM, and no corpus entry declares a `by` — the
// member did not exist when the corpus was authored and forum does not emit
// one. Under D1's locked default those 23 still (correctly) emit nothing, and
// D6's no-migration guarantee for stored forum graphs is exactly that
// property. What they do once the key IS declared is measured separately, on a
// copy of each entry, by `EveryCrossStreamKeyCorpusRequestJoinsUnderByNone` in
// tests/treebuilder/FanInJoinKeyTest.cpp — 23 of 23, against 0 of 23 under the
// default, both halves in the one test.
//
// So this floor moves only if the CORPUS gains a `by`, which is a forum-side
// change (SPEC §3), not an engine one.
TEST_F(ComposedChain, EveryNodePlusPipelineEntryReachesTheTerminalOnlyViaThePipeline) {
  rapidjson::Document& doc = corpusDoc();
  ASSERT_FALSE(doc.IsNull()) << krn::testsupport::corpusNotFoundDiagnostic();
  ASSERT_FALSE(doc.HasParseError());
  ASSERT_TRUE(doc.IsArray());

  int seen = 0, silentA = 0, leakedB = 0;
  std::string detail;

  for (const auto& entry : doc.GetArray()) {
    if (!entry.IsObject() || !entry.HasMember("request")) continue;
    const rapidjson::Value& rq = entry["request"];
    if (!hasNode(rq) || !hasPipeline(rq)) continue;
    ++seen;
    const int id = (entry.HasMember("corpus_id") && entry["corpus_id"].IsInt())
                     ? entry["corpus_id"].GetInt() : -1;

    // Everything the request mentions, so the driver can actually drive it:
    // every AtomicAccessor's field is seeded in the store, and every
    // (streamKey, field) a Listener names — plus the outer pair — is ticked.
    std::map<std::string, std::set<std::string>> tickFields;
    double seedVal = 10.0;
    std::function<void(const rapidjson::Value&)> walk =
      [&](const rapidjson::Value& v) {
        if (v.IsArray()) { for (const auto& x : v.GetArray()) walk(x); return; }
        if (!v.IsObject()) return;
        if (v.HasMember("type") && v["type"].IsString()) {
          const std::string t = v["type"].GetString();
          const std::string sk =
            (v.HasMember("streamKey") && v["streamKey"].IsString())
              ? v["streamKey"].GetString()
              : (rq["streamKey"].IsString() ? rq["streamKey"].GetString() : "");
          const std::string f = (v.HasMember("field") && v["field"].IsString())
                                  ? v["field"].GetString() : "";
          if (t == "AtomicAccessor" && !f.empty())
            store_.set(sk, f, seedVal += 1.0);
          if (t == "Listener" && !f.empty()) tickFields[sk].insert(f);
        }
        for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) walk(m->value);
      };
    walk(rq["node"]);
    tickFields[rq["streamKey"].GetString()].insert(rq["field"].GetString());

    auto drive = [&](const rapidjson::Value& request) -> std::size_t {
      auto sink = std::make_shared<Sink>();
      tree::BuiltChain chain;
      try {
        chain = tree::buildForRequest(request, deps_, sink);
      } catch (const std::exception& ex) {
        ADD_FAILURE() << "corpus_id " << id << " failed to build: " << ex.what();
        return 0;
      }
      double v = 100.0;
      for (int rep = 0; rep < 4; ++rep)
        for (const auto& [sk, fields] : tickFields) {
          std::vector<std::pair<std::string, double>> kv;
          for (const auto& f : fields) kv.emplace_back(f, v += 1.0);
          tick(sk, kv);
        }
      pool_->drain();
      const std::size_t n = sink->values().size();
      for (auto& x : chain.keepAlive) if (x) x->shutdown();
      if (chain.head) chain.head->shutdown();
      return n;
    };

    const std::size_t a = drive(rq);

    // B: the same request with `{"type":"Filter","when":{"op":"lt","args":[1,0]}}`
    // appended as the LAST pipeline stage. `Filter` passes a value on only when
    // its predicate is > 0.5 (src/nodes/Filter.cpp) and `1 < 0` compiles to a
    // constant 0.0, so the composed tail is a wall.
    rapidjson::Document blocked;
    blocked.CopyFrom(rq, blocked.GetAllocator());
    auto& al = blocked.GetAllocator();
    const char* key = blocked.HasMember("pipeline") ? "pipeline" : "stages";
    rapidjson::Value wall(rapidjson::kObjectType);
    rapidjson::Value when(rapidjson::kObjectType);
    rapidjson::Value args(rapidjson::kArrayType);
    args.PushBack(1, al).PushBack(0, al);
    when.AddMember("op", "lt", al).AddMember("args", args, al);
    wall.AddMember("type", "Filter", al).AddMember("when", when, al);
    blocked[key].PushBack(wall, al);

    const std::size_t b = drive(blocked);

    if (a == 0) {
      ++silentA;
      detail += "\n      corpus_id " + std::to_string(id) +
                ": verbatim run emitted nothing — B proves nothing for it";
    }
    if (b != 0) {
      ++leakedB;
      detail += "\n      corpus_id " + std::to_string(id) + ": " +
                std::to_string(b) +
                " value(s) reached the terminal past an always-false Filter at "
                "the END of the pipeline";
    }
  }

  EXPECT_EQ(seen, 52)
      << "expected 52 corpus entries carrying BOTH `node` and `pipeline` (SPEC "
         "§5 Q1's classification); found " << seen
      << ". If the corpus changed size, re-run the classification before "
         "trusting anything below.";
  EXPECT_GE(seen - silentA, 29)
      << "only " << (seen - silentA) << " of " << seen
      << " entries emitted anything at all, so run B is vacuous for the rest.\n"
         "    At least the 29 same-streamKey joins must emit under any engine "
         "that has a working join;\n"
         "    the 23 cross-streamKey ones legitimately go silent once ENC-1291 "
         "enforces per-port arity.\n"
         "    Currently silent:" << detail;
  EXPECT_EQ(leakedB, 0)
      << leakedB << " of " << seen << " entries reach the terminal WITHOUT "
         "passing through the pipeline.\n"
         "    That is SPEC §1.1 defect 1: the `node` subtree wired straight to "
         "the Responder while\n"
         "    the pipeline runs on a second, separate chain." << detail;
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. THE SCOPE CLAUSE — the 162 `node`-only requests are untouched
// ═══════════════════════════════════════════════════════════════════════════
//
// The clock rule applies to FAN-IN nodes only. Every one of the 162 `node`-only
// corpus requests has a single-input transform at its head (Worker 47,
// AtomicAccessor 81, Interval 24, SymbolSplit 10 — independently recounted
// here), none of them builds a `CompositeRoot`, and for them the head Listener
// is wired straight into the node and is the data source. Without the scope
// clause the rule would have broken all 162.
//
// ONE CORRECTION TO THE RULING, MEASURED HERE RATHER THAN ARGUED. §5 Q1 says
// "for all 162 the head Listener IS the sole data source". That holds for 138
// of them. It does not hold for the 24 whose head is an `Interval`:
// `Interval::onValue` is an explicit no-op ("source node: no upstream input",
// src/nodes/Interval.cpp) and a timer thread drives the child instead. Those 24
// ignore the head Listener before this change and after it — which is still
// "untouched", just not for the stated reason. The assertion below splits the
// two groups rather than asserting the ruling's wording.
TEST_F(ComposedChain, NodeOnlyCorpusRequestsAreUnaffected) {
  rapidjson::Document& doc = corpusDoc();
  ASSERT_FALSE(doc.IsNull()) << krn::testsupport::corpusNotFoundDiagnostic();
  ASSERT_TRUE(doc.IsArray());

  int seen = 0, driven = 0, timerDriven = 0, silent = 0;
  std::string detail;

  for (const auto& entry : doc.GetArray()) {
    if (!entry.IsObject() || !entry.HasMember("request")) continue;
    const rapidjson::Value& rq = entry["request"];
    if (!hasNode(rq) || hasPipeline(rq)) continue;
    ++seen;
    const int id = (entry.HasMember("corpus_id") && entry["corpus_id"].IsInt())
                     ? entry["corpus_id"].GetInt() : -1;
    const std::string headType = rq["node"]["type"].GetString();

    // Seed every AtomicAccessor the subtree names, so a pull head has
    // something to return.
    double seedVal = 42.0;
    std::function<void(const rapidjson::Value&)> walk =
      [&](const rapidjson::Value& v) {
        if (v.IsArray()) { for (const auto& x : v.GetArray()) walk(x); return; }
        if (!v.IsObject()) return;
        if (v.HasMember("type") && v["type"].IsString() &&
            std::string(v["type"].GetString()) == "AtomicAccessor" &&
            v.HasMember("field") && v["field"].IsString()) {
          const std::string sk =
            (v.HasMember("streamKey") && v["streamKey"].IsString())
              ? v["streamKey"].GetString() : rq["streamKey"].GetString();
          store_.set(sk, v["field"].GetString(), seedVal += 1.0);
        }
        for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) walk(m->value);
      };
    walk(rq["node"]);

    auto sink = std::make_shared<Sink>();
    tree::BuiltChain chain;
    try {
      chain = tree::buildForRequest(rq, deps_, sink);
    } catch (const std::exception& ex) {
      ADD_FAILURE() << "corpus_id " << id << " failed to build: " << ex.what();
      continue;
    }

    // Drive the OUTER (streamKey, field) and nothing else. For a single-input
    // transform head that is the data source, so a value must come out.
    for (int rep = 0; rep < 3; ++rep)
      tick(rq["streamKey"].GetString(),
           {{rq["field"].GetString(), 100.0 + rep}});
    pool_->drain();

    const std::size_t n = sink->values().size();
    for (auto& x : chain.keepAlive) if (x) x->shutdown();
    if (chain.head) chain.head->shutdown();

    if (headType == "Interval") { ++timerDriven; continue; }
    if (n > 0) { ++driven; continue; }
    ++silent;
    detail += "\n      corpus_id " + std::to_string(id) + " (head " + headType +
              "): ticking the outer (" + rq["streamKey"].GetString() + ", " +
              rq["field"].GetString() + ") produced NOTHING";
  }

  EXPECT_EQ(seen, 162)
      << "expected 162 `node`-only corpus entries (SPEC §5 Q1); found " << seen;
  EXPECT_EQ(timerDriven, 24)
      << "expected 24 Interval-headed entries, which are timer-driven and "
         "correctly ignore the head Listener; found " << timerDriven;
  EXPECT_EQ(silent, 0)
      << silent << " of the " << (seen - timerDriven)
      << " non-Interval `node`-only entries stopped being fed by their head "
         "Listener.\n"
         "    The ENC-1290 clock rule is scoped to FAN-IN nodes; for a "
         "single-input transform the\n"
         "    outer Listener IS the data source and nothing about it may "
         "change." << detail;
  EXPECT_EQ(driven, 138) << "138 = 47 Worker + 81 AtomicAccessor + 10 SymbolSplit";
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. ENC-1293's CHECK, NARROWED — `node:Pack` + `pipeline:[Field]` is LEGAL
// ═══════════════════════════════════════════════════════════════════════════
//
// D7 rejects a Record-valued TERMINAL and blesses `Pack -> Field -> scalar ->
// Responder` one line above its own restriction. ENC-1293's check analysed
// `node` and `pipeline` as two independent feeders, which was right while they
// were two chains — and which rejects exactly that canonical shape the moment
// they become one.
//
// Nothing in the corpus goes red on the day that becomes wrong: all 272 entries
// construct no `Record` at all (Worker, AtomicAccessor, Listener, Aggregate,
// Interval, SymbolSplit only), so `AllCorpusRequestsBuild` and
// `NoCheckedInCorpusRequestIsRefused` would both stay green while the check
// refused something it should accept. This test is the gate instead.
TEST_F(ComposedChain, NodePackPipelineFieldIsAcceptedAndEmits) {
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}},
    "pipeline":[{"type":"Field","name":"bid"}]
  })";

  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink))
      << "node:Pack + pipeline:[Field] is the canonical D7 shape and must build. "
         "If this throws the Record-terminal check is still analysing `node` as "
         "a direct feeder of the terminal (ENC-1293's call site), which "
         "ENC-1290 composed away.";
  ASSERT_NE(chain.head, nullptr);

  tick("AAPL", {{"bid", 1000.00}, {"ask", 1000.02}});
  pool_->drain();

  const auto vals = sink->values();
  EXPECT_EQ(sink->nonNumeric(), 0u)
      << "a Record reached the terminal — the rejection is not doing its job";
  ASSERT_EQ(vals.size(), 1u) << "expected exactly one projected value: " << render(vals);
  EXPECT_DOUBLE_EQ(vals[0], 1000.00) << "Field{name:'bid'} must project the bid";

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  chain.head->shutdown();
}

// The other direction, so §6 cannot be read as "the check was switched off":
// with NO pipeline, the `node` subtree IS what feeds the terminal and a Pack
// there is still refused.
TEST_F(ComposedChain, NodePackWithNoPipelineIsStillRejected) {
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}
  })";
  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());
  auto sink = std::make_shared<Sink>();
  try {
    auto chain = tree::buildForRequest(d, deps_, sink);
    for (auto& n : chain.keepAlive) if (n) n->shutdown();
    if (chain.head) chain.head->shutdown();
    FAIL() << "node:Pack with no pipeline must still be refused — it is the "
              "thing feeding the terminal";
  } catch (const std::exception& ex) {
    EXPECT_NE(std::string(ex.what()).find("would receive a Record"),
              std::string::npos) << ex.what();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. A REJECTED BUILD MUST LEAVE NOTHING SUBSCRIBED
// ═══════════════════════════════════════════════════════════════════════════
//
// `Dispatcher::_listeners` holds a `shared_ptr<INode>` per subscription and the
// only thing that unregisters is `Listener::shutdown()`. A builder that throws
// part-way therefore used to strand every Listener it had already registered,
// permanently and with no handle left to stop it — and `ClientSession` turns
// the throw into an error frame, so a client can drive it in a loop.
//
// ENC-1290 reversed the build order (pipeline first, `node` second), which
// moved the trigger onto the deeper and much more failure-prone subtree. The
// leak is older than that, so the unwind guard in `buildForRequest` covers
// BOTH orders and both are asserted here.
TEST_F(ComposedChain, ARejectedBuildLeavesNothingSubscribed) {
  // The pipeline is built FIRST and its last stage's downstream is the caller's
  // terminal — so a `Listener` in last-stage position registers with the
  // Dispatcher AND holds our `sink` as its downstream. Then the `node` fails to
  // build. If the partial build is not unwound, that Listener stays in
  // `Dispatcher::_listeners` forever with a live downstream, and the next tick
  // on (AAPL, ask) walks straight into `sink`.
  //
  // Deterministic on purpose: an earlier version of this test asserted a TIME
  // BUDGET, which five concurrently-compiling agents can blow through without
  // any leak, and which stayed green under the mutation that removes the guard.
  // A stranded subscription is now observed directly, not inferred.
  const char* kBadNode = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Aggregate","arity":2,"inputs":[]},
    "pipeline":[{"type":"Listener","streamKey":"AAPL","field":"ask"}]
  })";

  rapidjson::Document d;
  d.Parse(kBadNode);
  ASSERT_FALSE(d.HasParseError());

  auto stranded = std::make_shared<Sink>();      // ONE sink, kept alive
  constexpr int kRejects = 25;
  int rejected = 0;
  for (int i = 0; i < kRejects; ++i) {
    try {
      auto chain = tree::buildForRequest(d, deps_, stranded);
      for (auto& n : chain.keepAlive) if (n) n->shutdown();
      if (chain.head) chain.head->shutdown();
    } catch (const std::exception&) { ++rejected; }
  }
  ASSERT_EQ(rejected, kRejects)
      << "this request must be REFUSED for the test to mean anything";

  for (int n = 0; n < 4; ++n) tick("AAPL", {{"ask", 1.0 + n}});
  pool_->drain();

  EXPECT_EQ(stranded->values().size(), 0u)
      << stranded->values().size() << " value(s) reached a terminal belonging "
         "to a build that was REFUSED " << kRejects << " times.\n"
         "    `Dispatcher::_listeners` holds a shared_ptr per subscription and "
         "the only thing that\n    unregisters is `Listener::shutdown()`, so "
         "every Listener a failed build already registered\n    stays "
         "subscribed for the life of the process with nothing left holding a "
         "handle to stop it.\n"
         "    Reachable from untrusted client JSON: ClientSession turns the "
         "throw into an error frame,\n    so a client can drive this in a loop."
      << " Got: " << render(stranded->values());
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. THE CLOCK PREDICATE, at the two edges no corpus entry reaches
// ═══════════════════════════════════════════════════════════════════════════
//
// `Let` + `Ref`. An earlier draft of `declaredInputIsSelfClocked` answered
// `true` for `Ref`, which poisoned every ENCLOSING node: a `Let` with pull-only
// bindings whose body merely mentions a `Ref` — that is, every non-degenerate
// `Let` — was declared self-clocked and its join stayed dead. Nothing in the
// suite covered it; deleting the clause left the whole suite green. This is the
// gate. (`Ref` itself is harmless to clock — its head is a `RefStub` no-op.)
TEST_F(ComposedChain, LetBoundPullOnlyJoinIsClocked) {
  // A `Let` whose binding is pull-only, whose body is a fan-in, and whose body
  // REFERENCES the binding — the ordinary shape, and the one the retracted
  // clause broke. Nothing else in the suite builds a `Ref`, which is why
  // deleting or restoring that clause used to leave the whole suite green.
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Let",
      "bindings":{"u":{"type":"AtomicAccessor","streamKey":"AAPL","field":"bollinger_upper"}},
      "body":{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Ref","name":"u"},
        {"type":"AtomicAccessor","streamKey":"AAPL","field":"bollinger_lower"}]}},
    "pipeline":[{"type":"Worker","fn":"last"}]
  })";
  store_.set("AAPL", "bollinger_upper", 110.0);
  store_.set("AAPL", "bollinger_lower", 90.0);

  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink));

  tick("AAPL", {{"lastPrice", 1.0}});
  pool_->drain();

  const auto vals = sink->values();
  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  if (chain.head) chain.head->shutdown();

  ASSERT_FALSE(vals.empty())
      << "a `Let` with a pull-only binding and a fan-in body emitted NOTHING.\n"
         "    Neither the binding's producer nor the body has a clock of its "
         "own, so BOTH belong in\n    `clockTargets`. If "
         "`declaredInputIsSelfClocked` answers `true` for anything merely "
         "CONTAINING a\n    `Ref`, the body is excluded, only one of the two "
         "join members ever fires, and the join stays\n    dead — the exact "
         "failure the clock rule exists to end for the 7 pull-only corpus "
         "entries.";

  // One clock tick samples both accessors under "AAPL", completing one arity-2
  // batch that `Worker{fn:"last"}` forwards member by member.
  const std::vector<double> kExpected = {110.0, 90.0};
  ASSERT_EQ(vals.size(), kExpected.size()) << render(vals);
  for (std::size_t i = 0; i < kExpected.size(); ++i)
    EXPECT_NEAR(vals[i], kExpected[i], 1e-9) << "arrival " << i << " of " << render(vals);
}

// A timer-headed input IS self-clocked: `Interval`'s own thread drives its
// child directly, so the request's head Listener must not drive it as well.
//
// READ THIS BEFORE DELETING THE `Interval`/`BucketTime` CLAUSE FROM
// `declaredInputIsSelfClocked`. **No behavioural test can gate that clause
// today, and I checked** — removing it leaves this whole file green, because
// `Interval::onValue` and `BucketTime::onValue` are explicit no-ops ("source
// node: no upstream input"), so forwarding the clock to them is inert either
// way. The clause is therefore DOCUMENTATION OF AN INVARIANT, not a behaviour
// change: it says out loud what is currently true only by accident, and it is
// what stops the next edit to `Interval::onValue` from silently double-firing
// every timer-headed join input.
//
// `TimerOnValueIsANoOpWhichIsWhyTheClauseIsInert` below is the other half of
// the pair: it pins the accident. Break the no-op and it goes red, and whoever
// broke it lands here.
//
// This test pins the observable part: the accessor branch is clocked, the timer
// branch is not driven by the clock, and with a period long enough never to
// fire the join completes from neither.
TEST_F(ComposedChain, TimerHeadedInputIsNotDrivenByTheClock) {
  const char* kRequest = R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Aggregate","arity":2,"inputs":[
      {"type":"Interval","ms":3600000,
       "child":{"type":"AtomicAccessor","streamKey":"AAPL","field":"aa"}},
      {"type":"Listener","streamKey":"AAPL","field":"ask"}]},
    "pipeline":[{"type":"Worker","fn":"last"}]
  })";
  store_.set("AAPL", "aa", 5.0);

  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink));

  for (int n = 0; n < 4; ++n) tick("AAPL", {{"ask", 10.0 + n}, {"lastPrice", 99.0}});
  pool_->drain();

  // Four `ask` values, arity 2, one symbol -> two batches of two `ask`s
  // (SPEC §1.1 defect 2, unchanged by this ticket). The point is what is NOT
  // there: no 5.0 from the timer's accessor, because the clock did not drive
  // it, and no 99.0 from the clock itself.
  for (double v : sink->values()) {
    EXPECT_NE(v, 5.0) << "the clock drove a timer-headed input, which the timer "
                         "already drives — a double-fire";
    EXPECT_NE(v, 99.0) << "the clock's own value entered the join";
  }

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  if (chain.head) chain.head->shutdown();
}

// The pinned accident. If either of these stops being a no-op, the
// `Interval`/`BucketTime` clause in `declaredInputIsSelfClocked` stops being
// inert documentation and becomes load-bearing — and a timer-headed join input
// would otherwise be driven twice per period.
TEST_F(ComposedChain, TimerOnValueIsANoOpWhichIsWhyTheClauseIsInert) {
  auto child = std::make_shared<Sink>();

  Interval interval(std::chrono::milliseconds(3600000), child, nullptr);
  interval.onValue(StreamValue{"AAPL", 42.0});
  EXPECT_EQ(child->values().size(), 0u)
      << "Interval::onValue forwarded to its child. It is documented as a "
         "source node with no upstream\n    input, and "
         "`declaredInputIsSelfClocked` names `Interval` on that basis — see the "
         "comment above\n    TimerHeadedInputIsNotDrivenByTheClock. A "
         "timer-headed join input would now fire twice.";
  interval.shutdown();

  BucketTime bucket(std::chrono::milliseconds(3600000), child, nullptr);
  bucket.onValue(StreamValue{"AAPL", 42.0});
  EXPECT_EQ(child->values().size(), 0u)
      << "BucketTime::onValue forwarded to its child — same consequence.";
  bucket.shutdown();
}


// ═══════════════════════════════════════════════════════════════════════════
// 9. A FAN-IN IS NOT A PIPELINE STAGE — REFUSED AT BUILD TIME
// ═══════════════════════════════════════════════════════════════════════════
//
// SPEC §5 Q7 (ruled 2026-09-20 by ENC-1334), D5 as amended. Implemented by
// ENC-1336 in `src/core/TreeBuilder.cpp` (`isFanInType` /
// `fanInPipelineStageMessage` and the block at the head of `buildForRequest`).
//
// THIS SECTION REPLACES `FanInInPipelinePositionClocksRatherThanPassesThrough`,
// which ENC-1290 wrote to PIN the behaviour this ruling removes. That test drove
// the request below and asserted that the `node`'s output never reached the
// terminal. It was correct as a measurement and is exactly why the shape is now
// refused: a value an author asked to be computed vanished, and the only place
// that fact was written down was a comment in this file.
//
// THE RULE, BOTH HALVES.
//
//   REFUSED — a fan-in (`Aggregate`, `Pack`, `Let`: the three builders that
//   construct a `CompositeRoot`) as an element of `pipeline`/`stages` when the
//   request ALSO carries a `node`, OR when it is not the FIRST element.
//   Something is then built upstream of it, and a `CompositeRoot` forwards its
//   upstream to `clockTargets_` and to nothing else — so with all-`Listener`
//   stage inputs the forwarding set is empty and the upstream's output is
//   discarded in silence.
//
//   ACCEPTED — a fan-in as `pipeline[0]` in a request with NO `node` key. Its
//   upstream IS the head `Listener`, because `midHead` starts at `terminal`
//   either way, so that is bit-for-bit the wiring the same fan-in gets under
//   `node` with no pipeline — the case §5 Q1 already ruled.
//   `FanInAsFirstPipelineStageWithNoNodeIsWiredExactlyLikeTheNodeForm` proves
//   that equivalence by VALUE rather than asserting it, and it is the test that
//   matters here: a refusal written one condition too broad is silent in the
//   other direction, and no corpus count would catch it (0 of 272 entries put a
//   fan-in in a pipeline stage at all).
//
// NOT RECURSIVE, DELIBERATELY. A fan-in nested inside a stage (a `Chain`, a
// `Tee`) is NOT refused. That shape is currently an empty forwarding set rather
// than a designed behaviour; ENC-1336 was scoped non-recursive and inventing a
// rule for it here would be inventing behaviour. Said out loud so the next
// reader inherits a decision rather than a gap.

// Build `json` as a request and return the thrown message, or "" on success.
// Always tears the chain down, so no Listener/Interval thread outlives the call.
std::string buildAndReport(const rapidjson::Value& d,
                           const tree::Deps&       deps) {
  auto sink = std::make_shared<Sink>();
  try {
    auto chain = tree::buildForRequest(d, deps, sink);
    if (chain.head) chain.head->shutdown();
    for (auto& n : chain.keepAlive) if (n) n->shutdown();
    return "";
  } catch (const std::exception& ex) {
    return ex.what();
  }
}

// Every refusal must be actionable: name the culprit stage, say what happens
// otherwise, name the fix, and name the rule. An error the author cannot act on
// is barely better than the silent drop it replaces.
void expectFanInRefusal(const std::string& msg,
                        const char*        what,
                        const char*        culpritType,
                        const char*        culpritWhere) {
  ASSERT_FALSE(msg.empty()) << what << " built successfully; it must not";
  EXPECT_NE(msg.find(culpritType), std::string::npos)
      << what << " must name the offending node type: " << msg;
  EXPECT_NE(msg.find(culpritWhere), std::string::npos)
      << what << " must name WHICH stage is offending (" << culpritWhere
      << "): " << msg;
  EXPECT_NE(msg.find("FAN-IN"), std::string::npos)
      << what << " must say the problem is that the stage is a fan-in: " << msg;
  EXPECT_NE(msg.find("delivered as a CLOCK"), std::string::npos)
      << what << " must say what happens to the upstream's value: " << msg;
  EXPECT_NE(msg.find("no log, no metric and no error"), std::string::npos)
      << what << " must say why the failure it prevents is invisible: " << msg;
  EXPECT_NE(msg.find("move the fan-in to 'node' position"), std::string::npos)
      << what << " must name the fix: " << msg;
  EXPECT_NE(msg.find("ENC-1336"), std::string::npos)
      << what << " must name the rule's ticket: " << msg;
  EXPECT_NE(msg.find("section 5 Q7"), std::string::npos)
      << what << " must cite the SPEC section that ruled it: " << msg;
}

std::string buildAndReportJson(const char* json, const tree::Deps& deps) {
  rapidjson::Document d;
  d.Parse(json);
  EXPECT_FALSE(d.HasParseError()) << "test JSON is malformed: " << json;
  return buildAndReport(d, deps);
}

// ─── DIRECTION 1: REFUSE ────────────────────────────────────────────────────

// ENC-1290's request, verbatim. It used to build and silently swallow the
// `Worker{fn:"last"}`; it must now refuse.
TEST_F(ComposedChain, FanInAsAPipelineStageUnderANodeIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]
  })", deps_);
  expectFanInRefusal(msg, "node:Worker + pipeline:[Aggregate]",
                     "Aggregate", "pipeline[0]");
  EXPECT_NE(msg.find("'node'"), std::string::npos)
      << "with a `node` present the message must say THAT is the upstream being "
         "thrown away: " << msg;
}

// All three fan-in types, so the rule is the shape and not one node's name.
// This cannot gate the absence of a FOURTH fan-in — nothing in this suite or
// the corpus can — which is why `isFanInType` carries a written warning.
TEST_F(ComposedChain, FanInAsAPipelineStageUnderANodeIsRefused_AllThreeTypes) {
  const std::string agg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]
  })", deps_);
  expectFanInRefusal(agg, "node + pipeline:[Aggregate]", "Aggregate", "pipeline[0]");

  // NOTE — this case deliberately has NO trailing `Field`, so the terminal
  // WOULD receive a Record and D7's check would fire on it too. That is what
  // makes the `Record` assertion below load-bearing: it fails if the two checks
  // are reordered. With a `Field` appended (as this test first had it) D7 never
  // fires at all, the assertion is inert, and moving the ENC-1336 block after
  // D7's killed no test — measured, mutation M8.
  const std::string pack = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]
  })", deps_);
  expectFanInRefusal(pack, "node + pipeline:[Pack]", "Pack", "pipeline[0]");
  EXPECT_EQ(pack.find("Record"), std::string::npos)
      << "a MISPLACED Pack trips BOTH this rule and D7's Record-terminal rule. "
         "It must be diagnosed as a placement problem: the fan-in check runs "
         "first on purpose, because moving the stage is the fix and D7 will "
         "still speak up afterwards if the terminal really would get a Record: "
      << pack;

  const std::string let = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Let",
      "bindings":{"b":{"type":"Listener","streamKey":"AAPL","field":"bid"}},
      "body":{"type":"Ref","name":"b"}}]
  })", deps_);
  expectFanInRefusal(let, "node + pipeline:[Let]", "Let", "pipeline[0]");
}

// The other refused half: no `node` at all, but a stage before the fan-in.
TEST_F(ComposedChain, FanInAsANonFirstPipelineStageIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Worker","fn":"last"},
                {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]
  })", deps_);
  expectFanInRefusal(msg, "pipeline:[Worker, Aggregate]", "Aggregate", "pipeline[1]");
  EXPECT_NE(msg.find("pipeline[0] precedes it"), std::string::npos)
      << "the message must name the stage that WOULD be discarded, not only the "
         "fan-in — and must say `precedes`, not `is built`, because a stage "
         "this check deliberately did not inspect may not be buildable at all: "
      << msg;
}

// `stages` is the legacy spelling of `pipeline` and the build loop treats them
// identically (first key present wins). The check must too, or the rule is
// bypassable by renaming one key.
TEST_F(ComposedChain, FanInAsANonFirstStagesElementIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "stages":[{"type":"Worker","fn":"last"},
              {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]
  })", deps_);
  expectFanInRefusal(msg, "stages:[Worker, Aggregate]", "Aggregate", "stages[1]");
}

// The refusal must precede EVERY builder, not merely the fan-in's own: the
// Listener / Interval / BucketTime builders call start() and spawn threads, so
// a late reject leaks live work for a request that is never served (SPEC §1.5).
//
// This is checkable rather than asserted. The `node` below names a function
// that does not exist, so `buildOne` throws "Worker: unknown fn" — and the
// `node` is built LAST, after the whole pipeline. If the ENC-1336 check ran
// anywhere other than before all construction, the message would be the
// Worker's, and the pipeline's `Interval` and `Listener` would already be live.
TEST_F(ComposedChain, FanInRefusalPrecedesEveryBuilder) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"__no_such_fn__"},
    "pipeline":[{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Interval","ms":10,
         "child":{"type":"AtomicAccessor","streamKey":"AAPL","field":"bid"}},
        {"type":"Listener","streamKey":"AAPL","field":"ask"}]}]
  })", deps_);
  expectFanInRefusal(msg, "node:Worker{bad fn} + pipeline:[Aggregate{Interval,...}]",
                     "Aggregate", "pipeline[0]");
  EXPECT_EQ(msg.find("unknown fn"), std::string::npos)
      << "the fan-in check must run BEFORE buildOne touches anything; getting "
         "the Worker's error means the pipeline had already been constructed: "
      << msg;
  EXPECT_TRUE(dispatcher_->listenersFor("AAPL", "ask").empty())
      << "a refused build must leave nothing subscribed";
}

// ─── DIRECTION 2: ACCEPT, AND EMIT ──────────────────────────────────────────

// THE HALF THAT MATTERS. A fan-in as `pipeline[0]` with no `node` is not an
// exception to the rule — it is §5 Q1's already-ruled case wearing a different
// key — and this pins the equivalence by VALUE instead of by argument:
//
//   A:  pipeline:[Aggregate{ask,bid}, Worker{diff}]        (no `node`)
//   B:  node:Aggregate{ask,bid}, pipeline:[Worker{diff}]   (= corpus 86)
//
// `midHead` starts at `terminal` in A, so its head Listener feeds the
// `CompositeRoot` exactly as B's does. Same ticks, same emitted sequence, and
// the sequence must be NON-EMPTY — build success alone would be satisfied by a
// chain that emits nothing at all.
TEST_F(ComposedChain, FanInAsFirstPipelineStageWithNoNodeIsWiredExactlyLikeTheNodeForm) {
  auto run = [this](const char* json) {
    rapidjson::Document d;
    d.Parse(json);
    EXPECT_FALSE(d.HasParseError()) << "test JSON is malformed: " << json;
    auto sink = std::make_shared<Sink>();
    tree::BuiltChain chain;
    EXPECT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink));
    for (int n = 0; n < 3; ++n)
      tick("AAPL", {{"ask", 1000.02 + n}, {"bid", 1000.00 + n},
                    {"lastPrice", 7777.0}});
    pool_->drain();
    auto vals = sink->values();
    if (chain.head) chain.head->shutdown();
    for (auto& x : chain.keepAlive) if (x) x->shutdown();
    return vals;
  };

  const auto asPipelineHead = run(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]},
      {"type":"Worker","fn":"diff"}]
  })");
  const auto asNode = run(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]},
    "pipeline":[{"type":"Worker","fn":"diff"}]
  })");

  ASSERT_FALSE(asPipelineHead.empty())
      << "the accepted shape must BUILD AND EMIT; build success alone is not "
         "the criterion (ENC-1289)";
  EXPECT_EQ(asPipelineHead, asNode)
      << "pipeline[0] with no `node` must be wired identically to the same "
         "fan-in under `node` — that equivalence is the whole reason §5 Q7 "
         "accepts it.\n  as pipeline[0]: " << render(asPipelineHead)
      << "\n  as node:        " << render(asNode);
  for (double v : asPipelineHead)
    EXPECT_NE(v, 7777.0)
        << "the head Listener is the chain's CLOCK, never a join member (§5 "
           "Q1); got " << render(asPipelineHead);
}

// A `Pack` at `pipeline[0]` with no `node` is the form `RecordTerminalTest` and
// `ClientSessionTest` are both written in. If the placement rule were one
// condition too broad it would refuse them, and the refusal would arrive as a
// DIFFERENT message than D7's — so this pins which rule owns that shape.
TEST_F(ComposedChain, PackAtPipelineZeroWithNoNodeIsNotAPlacementError) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Pack","fields":{
        "ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},
        "bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]
  })", deps_);
  ASSERT_FALSE(msg.empty()) << "D7 still rejects a Record-valued terminal";
  EXPECT_EQ(msg.find("FAN-IN"), std::string::npos)
      << "Pack at pipeline[0] with no `node` is the ACCEPTED placement; the "
         "only thing wrong with it is D7's Record terminal: " << msg;
  EXPECT_NE(msg.find("Record"), std::string::npos)
      << "expected D7's message, got: " << msg;
}

// ─── THE CORPUS GATE ────────────────────────────────────────────────────────

// 0 of 272. The measurement, as a test rather than a note: if a future corpus
// edit ever authors the shape, this NAMES the entries instead of leaving them
// to be discovered a month later. It is also the instrument that proves the
// green is a measurement and not a vacuum. Measured: widen `isFanInType` to
// include "Worker" and this fails naming **69 refused requests across 63
// distinct corpus_ids** (86-115, 146-170, 178, 181, 185, 192, 195, 197, 198,
// 200 — some ids carry more than one request key), because `Worker` is the
// corpus's ONLY pipeline stage type at all, 104 uses of it.
TEST_F(ComposedChain, NoCheckedInCorpusRequestIsRefusedForFanInPlacement) {
  rapidjson::Document& doc = corpusDoc();
  ASSERT_FALSE(doc.IsNull()) << krn::testsupport::corpusNotFoundDiagnostic();
  ASSERT_FALSE(doc.HasParseError()) << "corpus_requests.json failed to parse";
  ASSERT_TRUE(doc.IsArray());
  EXPECT_EQ(doc.Size(), 272u)
      << "the corpus size moved; re-measure the 0-of-272 claim in "
         "src/core/TreeBuilder.cpp and SPEC §5 Q7 rather than editing this "
         "number";

  std::vector<std::string> refused;
  for (auto& e : doc.GetArray()) {
    if (!e.IsObject() || !e.HasMember("request")) continue;
    const int id = (e.HasMember("corpus_id") && e["corpus_id"].IsInt())
                     ? e["corpus_id"].GetInt() : -1;
    const std::string msg = buildAndReport(e["request"], deps_);
    if (msg.find("is a FAN-IN") == std::string::npos) continue;
    refused.push_back("corpus_id " + std::to_string(id) + ": " + msg);
  }

  std::string detail;
  for (const auto& r : refused) detail += "\n  " + r;
  EXPECT_TRUE(refused.empty())
      << "the ENC-1336 placement rule refused " << refused.size()
      << " checked-in corpus request(s); it must refuse none:" << detail;
}

// forum's flagship graph, and the reason the accept half must not be widened.
// `pipelinetranslate.Translate()` turns the ENC-672 "RSI overbought" node graph
// (Listener -> Pack -> Filter -> Field -> Responder) into EXACTLY the request
// below — a `Pack` at pipeline[0] with no `node`. Measured by running forum's
// own translator, not copied from a doc. If ENC-1336's placement rule were one
// condition broader, this would stop building, and nothing in GMA's corpus
// would have said so.
//
// The other two graphs the same translator can emit —
// `pipeline:[Filter, Pack, Field]` and `node:Aggregate + pipeline:[Pack, ...]`
// — ARE refused, deliberately. At `b273278` both emitted a sequence identical
// to the same request with the upstream stage deleted, which is the silent
// discard this rule exists to convert into an error.
TEST_F(ComposedChain, ForumsRsiOverboughtDemoShapeStillBuildsAndEmits) {
  const char* kRequest = R"({
    "key":42,"streamKey":"NEXO","field":"lastPrice",
    "pipeline":[
      {"type":"Pack","fields":{
        "rsi":{"type":"Listener","streamKey":"NEXO","field":"rsi_14"},
        "price":{"type":"Listener","streamKey":"NEXO","field":"lastPrice"}}},
      {"type":"Filter","when":{"op":"gt","args":[{"ref":"rsi"},70]}},
      {"type":"Field","name":"price"}]
  })";
  rapidjson::Document d;
  d.Parse(kRequest);
  ASSERT_FALSE(d.HasParseError());

  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink))
      << "forum's ENC-672 demo request must still build: a fan-in at "
         "pipeline[0] with no `node` is the ACCEPTED placement";

  for (int n = 0; n < 3; ++n)
    tick("NEXO", {{"lastPrice", 100.0 + n}, {"rsi_14", 71.0 + n}});
  pool_->drain();

  const auto vals = sink->values();
  EXPECT_FALSE(vals.empty())
      << "it must EMIT, not merely build — build success alone is the weakness "
         "ENC-1289 spent a ticket removing";
  for (double v : vals)
    EXPECT_GE(v, 100.0) << "expected projected prices; got " << render(vals);

  if (chain.head) chain.head->shutdown();
  for (auto& n : chain.keepAlive) if (n) n->shutdown();
}

// ─── THE NESTED FORM: WHAT THE RECURSION REFUSES (ENC-1344) ─────────────────

// This block replaces `FanInWrappedInAChainIsNotRefused_KnownGap`, which
// ENC-1336's refuter added to PIN a defect it was scoped not to fix. The gap is
// closed, so the test flips from asserting the discard to asserting the
// refusal. Its two shapes are carried over verbatim so the flip is legible in
// the diff.
//
// Read these against the enumeration in `src/core/TreeBuilder.cpp`: the rule is
// no longer "this stage's `type` is a fan-in name", it is "a fan-in has a
// value-producing node built directly upstream of it, however many wrappers
// deep".

// The two shapes the known-gap test pinned, now refused, with the nested path
// NAMED. `pipeline[0]` alone would send the reader looking for an `Aggregate`
// that is not there.
TEST_F(ComposedChain, FanInWrappedInAChainIsRefused) {
  const std::string viaChain = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Chain","stages":[
      {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]}]
  })", deps_);
  expectFanInRefusal(viaChain, "node + pipeline:[Chain{Aggregate}]",
                     "Aggregate", "pipeline[0].stages[0]");
  EXPECT_NE(viaChain.find("NOT the stage itself"), std::string::npos)
      << "the message must say the fan-in is nested, or the reader looks for "
         "an Aggregate at pipeline[0] and finds a Chain: " << viaChain;

  const std::string viaTee = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Tee","outputs":[
      {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]}]
  })", deps_);
  expectFanInRefusal(viaTee, "node + pipeline:[Tee{Aggregate}]",
                     "Aggregate", "pipeline[0].outputs[0]");
}

// THE ENUMERATION, AS A TEST. Every builder the comment on `isFanInType`
// classifies as forwarding the upstream value into a sub-node gets a row, so
// the classification is checked rather than asserted in prose. `Tee`, `Switch`
// and `GroupSplit` matter most here: none of them builds a `CompositeRoot`, so
// a rule keyed on "the stage's head IS a CompositeRoot" would catch `Chain` and
// miss all three.
TEST_F(ComposedChain, EveryForwardingWrapperIsSeenThrough) {
  struct Row { const char* what; const char* stage; const char* where; };
  const char* kAgg =
    R"({"type":"Aggregate","arity":2,"inputs":[
         {"type":"Listener","streamKey":"AAPL","field":"ask"},
         {"type":"Listener","streamKey":"AAPL","field":"bid"}]})";

  const std::vector<Row> rows = {
    {"Chain{Aggregate}",
     R"({"type":"Chain","stages":[%A%]})",             "pipeline[0].stages[0]"},
    {"Tee{Worker, Aggregate}",
     R"({"type":"Tee","outputs":[{"type":"Worker","fn":"last"},%A%]})",
                                                       "pipeline[0].outputs[1]"},
    {"Switch{cases:[Worker, Aggregate]}",
     R"({"type":"Switch","select":{"ref":"value"},
         "cases":[{"type":"Worker","fn":"last"},%A%]})", "pipeline[0].cases[1]"},
    {"Switch{default:Aggregate}",
     R"({"type":"Switch","select":{"ref":"value"},
         "cases":[{"type":"Worker","fn":"last"}],"default":%A%})",
                                                       "pipeline[0].default"},
    {"GroupSplit{Aggregate}",
     R"({"type":"GroupSplit","child":%A%})",           "pipeline[0].child"},
    {"SymbolSplit{Aggregate}",
     R"({"type":"SymbolSplit","child":%A%})",          "pipeline[0].child"},
    {"Tee{Chain{Aggregate}} (two wrappers deep)",
     R"({"type":"Tee","outputs":[{"type":"Chain","stages":[%A%]}]})",
                                            "pipeline[0].outputs[0].stages[0]"},
    {"Chain{GroupSplit{Tee{Aggregate}}} (three deep)",
     R"({"type":"Chain","stages":[{"type":"GroupSplit","child":
         {"type":"Tee","outputs":[%A%]}}]})",
                               "pipeline[0].stages[0].child.outputs[0]"},
  };

  for (const auto& r : rows) {
    std::string stage = r.stage;
    for (std::size_t at = stage.find("%A%"); at != std::string::npos;
         at = stage.find("%A%"))
      stage.replace(at, 3, kAgg);
    const std::string json =
      R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
          "node":{"type":"Worker","fn":"last"},"pipeline":[)" + stage + "]}";
    const std::string msg = buildAndReportJson(json.c_str(), deps_);
    SCOPED_TRACE(r.what);
    expectFanInRefusal(msg, r.what, "Aggregate", r.where);
  }
}

// `Chain`'s SECOND defect, which the pass-through property alone does not
// describe. `Chain{[Worker, Aggregate]}` at `pipeline[0]` of a request with NO
// `node` is in the one ACCEPTED outer position — and is still wrong, because
// the thing upstream of the `Aggregate` is the `Chain`'s own first stage. The
// `Worker`'s output is discarded inside the Chain wherever the Chain sits.
TEST_F(ComposedChain, AFanInAfterAStageInsideAChainIsRefusedEvenAtPipelineZero) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Chain","stages":[
      {"type":"Worker","fn":"last"},
      {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]}]
  })", deps_);
  expectFanInRefusal(msg, "pipeline:[Chain{Worker, Aggregate}] with no node",
                     "Aggregate", "pipeline[0].stages[1]");
  EXPECT_NE(msg.find("stages[0] precedes it"), std::string::npos)
      << "the message must name the stage INSIDE the Chain that would be "
         "discarded, not the Chain: " << msg;
}

// The same predicate applied to the `node` key, which ENC-1336 never scanned.
// `node:Aggregate` is the BLESSED shape and stays accepted (the head Listener
// is its clock); a fan-in nested behind a Chain stage under `node` is the same
// silent discard and is refused. Leaving `node` unscanned would have made the
// rule inconsistent in exactly the way that files the next gap ticket.
TEST_F(ComposedChain, NestedFanInUnderTheNodeKeyIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Chain","stages":[
      {"type":"Worker","fn":"last"},
      {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]},
    "pipeline":[{"type":"Worker","fn":"diff"}]
  })", deps_);
  expectFanInRefusal(msg, "node:Chain{Worker, Aggregate}",
                     "Aggregate", "node.stages[1]");
}

// A fan-in nested inside ANOTHER fan-in's declared input. The outer `Aggregate`
// is in the accepted position, so the recursion enters its inputs in the
// clock-only disposition — and finds the Chain-internal discard there too.
TEST_F(ComposedChain, NestedFanInInsideAFanInsDeclaredInputIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Aggregate","arity":2,"inputs":[
      {"type":"Listener","streamKey":"AAPL","field":"ask"},
      {"type":"Chain","stages":[
        {"type":"Worker","fn":"last"},
        {"type":"Pack","fields":{
          "a":{"type":"Listener","streamKey":"AAPL","field":"bid"},
          "b":{"type":"Listener","streamKey":"AAPL","field":"ask"}}}]}]}
  })", deps_);
  expectFanInRefusal(msg, "node:Aggregate{Listener, Chain{Worker, Pack}}",
                     "Pack", "node.inputs[1].stages[1]");
}

// ─── DIRECTION 2 FOR THE NESTED FORM: WHAT IT STILL ACCEPTS ─────────────────
//
// A widened refusal that only proves the NEW refusals is half a test. These
// three are the accept half of the widening specifically; the corpus gate
// (`NoCheckedInCorpusRequestIsRefusedForFanInPlacement`, 272 entries),
// `ForumsRsiOverboughtDemoShapeStillBuildsAndEmits` and the whole of
// `RecordTerminalTest` are the rest of it.

// THE ONE ACCEPTED PLACEMENT SURVIVES BEING WRAPPED. If the recursion had been
// written as "a fan-in anywhere inside a stage is refused" — the obvious wrong
// widening — this would stop building, and nothing in the corpus would say so.
TEST_F(ComposedChain, TheAcceptedPlacementIsStillAcceptedThroughAWrapper) {
  auto run = [this](const char* json) {
    rapidjson::Document d;
    d.Parse(json);
    EXPECT_FALSE(d.HasParseError()) << "test JSON is malformed: " << json;
    auto sink = std::make_shared<Sink>();
    tree::BuiltChain chain;
    EXPECT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink))
        << "wrapping the ACCEPTED placement must not refuse it";
    for (int n = 0; n < 3; ++n)
      tick("AAPL", {{"ask", 1000.02 + n}, {"bid", 1000.00 + n},
                    {"lastPrice", 7777.0}});
    pool_->drain();
    auto vals = sink->values();
    if (chain.head) chain.head->shutdown();
    for (auto& x : chain.keepAlive) if (x) x->shutdown();
    return vals;
  };

  // `Chain{[Aggregate, Worker]}` at pipeline[0] with no node: the Aggregate's
  // only upstream is the head Listener (a clock), and the Worker AFTER it is
  // fed by it, not discarded by it.
  const auto wrapped = run(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Chain","stages":[
      {"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]},
      {"type":"Worker","fn":"diff"}]}]
  })");
  const auto unwrapped = run(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]},
      {"type":"Worker","fn":"diff"}]
  })");

  ASSERT_FALSE(wrapped.empty())
      << "it must BUILD AND EMIT; build success alone is not the criterion";
  EXPECT_EQ(wrapped, unwrapped)
      << "a `Chain` around the accepted placement is the same wiring:\n"
         "  wrapped:   " << render(wrapped)
      << "\n  unwrapped: " << render(unwrapped);
  for (double v : wrapped)
    EXPECT_NE(v, 7777.0) << "the head Listener is still only the CLOCK; got "
                         << render(wrapped);
}

// A TIMER IS A CLOCK, NOT A DATA SOURCE, so the fan-in under it is in the
// accepted disposition. `Interval::onValue` is a documented no-op — the child
// is driven by the timer thread — which is why the recursion enters a timer's
// `child` with `hasUpstream = false`. Getting this wrong would refuse
// `Interval{child:Aggregate}`, the canonical "join on a clock" shape.
TEST_F(ComposedChain, AFanInUnderATimerChildIsAccepted) {
  rapidjson::Document d;
  d.Parse(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Interval","ms":3600000,
      "child":{"type":"Aggregate","arity":2,"inputs":[
        {"type":"Listener","streamKey":"AAPL","field":"ask"},
        {"type":"Listener","streamKey":"AAPL","field":"bid"}]}}]
  })");
  ASSERT_FALSE(d.HasParseError());
  auto sink = std::make_shared<Sink>();
  tree::BuiltChain chain;
  ASSERT_NO_THROW(chain = tree::buildForRequest(d, deps_, sink))
      << "a timer is a clock; the fan-in under it is the blessed shape";
  if (chain.head) chain.head->shutdown();
  for (auto& x : chain.keepAlive) if (x) x->shutdown();

  // ...and the Chain-internal defect is still caught THROUGH the timer, so the
  // `hasUpstream = false` above is a disposition and not a `return`.
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Interval","ms":3600000,
      "child":{"type":"Chain","stages":[
        {"type":"Worker","fn":"last"},
        {"type":"Aggregate","arity":2,"inputs":[
          {"type":"Listener","streamKey":"AAPL","field":"ask"},
          {"type":"Listener","streamKey":"AAPL","field":"bid"}]}]}}]
  })", deps_);
  expectFanInRefusal(msg, "pipeline:[Interval{child:Chain{Worker, Aggregate}}]",
                     "Aggregate", "pipeline[0].child.stages[1]");
}

// The wrappers that do NOT forward the upstream value are not walked, and the
// types with no sub-node cannot hide anything. This is the negative half of the
// enumeration: a `Ref` drops its upstream too (its head is a no-op `RefStub`),
// but that is a different defect — it discards with no fan-in involved — and
// deliberately is NOT widened into here. If a future ticket makes a `Ref` as a
// pipeline stage an error, it will be its own rule and this test says so.
// AMENDED BY ENC-1398, AND THE AMENDMENT IS THE POINT. This row was written to
// say "a Ref at pipeline[0] under a `node` fails as an UNKNOWN BINDING, not as a
// fan-in placement". It still passes, but no longer for that reason: that shape
// has the `node` subtree built directly upstream of the Ref, so ENC-1398's rule
// refuses it FIRST, before any binding lookup happens. Left asserting only the
// absence of "FAN-IN", the row would have kept passing while the behaviour it was
// written to pin had been displaced — a green that means nothing. So it now
// asserts the reason it actually passes for, and the unknown-binding path it used
// to cover moved to its own row below, on a shape ENC-1398 does not touch.
TEST_F(ComposedChain, ARefStageWithUpstreamIsRefusedAsAPlacementNotAsAFanIn) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Worker","fn":"last"},
    "pipeline":[{"type":"Ref","name":"nope"}]
  })", deps_);
  ASSERT_FALSE(msg.empty()) << "a Ref outside a Let is a build error already";
  EXPECT_EQ(msg.find("FAN-IN"), std::string::npos)
      << "a `Ref` joins nothing and must never be described as a fan-in: " << msg;
  // What it IS now: ENC-1398's placement refusal, because the `node` subtree is
  // built directly upstream of pipeline[0].
  EXPECT_NE(msg.find("'Ref'"), std::string::npos) << msg;
  EXPECT_NE(msg.find("DISCARDED"), std::string::npos)
      << "this is the ENC-1398 placement refusal, which says the upstream value "
         "is discarded: " << msg;
  EXPECT_NE(msg.find("ENC-1398"), std::string::npos) << msg;
}

// The coverage ENC-1398 displaced above, on a shape its rule does not reach: a
// `Ref` naming a binding that does not exist, with NO upstream (pipeline[0] of a
// request with no `node`, so its only upstream is the head Listener — a clock).
// The builder's own unknown-binding error must still be what fires here.
TEST_F(ComposedChain, ARefNamingAnUnknownBindingStillFailsAsAnUnknownBinding) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{"type":"Ref","name":"nope"}]
  })", deps_);
  ASSERT_FALSE(msg.empty()) << "a Ref outside a Let is a build error";
  EXPECT_EQ(msg.find("FAN-IN"), std::string::npos)
      << "still not a fan-in: " << msg;
  // And NOT ENC-1398's refusal — there is nothing upstream of this Ref to
  // discard, so the rule must stay out of the way and let the real error through.
  EXPECT_EQ(msg.find("DISCARDED"), std::string::npos)
      << "ENC-1398's placement rule must NOT fire here — this Ref has no "
         "upstream, and swallowing the unknown-binding error would hide the "
         "author's actual mistake: " << msg;
}

} // namespace

// ===========================================================================
// ENC-1398 — a `Ref` reached FROM upstream drops that value in silence.
//
// Same condition as ENC-1344's fan-in rule, different mechanism: a fan-in hands
// the upstream value to a `CompositeRoot`; a `Ref`'s head is a `RefStub` whose
// `onValue` is an empty no-op. ENC-1344 found this as a by-product of its
// 19-type enumeration and deliberately did not widen into it, so it is a
// PARALLEL rule here and `isFanInType` is untouched — folding `Ref` into that
// predicate would make the message say "fan-in" about a node that joins nothing.
//
// REACHABILITY, MEASURED (the ticket's "establish first"):
//   * Corpus: INVISIBLE. 0 of 272 entries contain `Chain`, `Tee`, `Switch`,
//     `GroupSplit`, `Ref` or `Let` at all.
//   * forum: REACHABLE — contrary to the ticket's premise that it is
//     forum-invisible. `translateStage` in
//     forum/internal/pipelinetranslate/config_mapping.go carries `case "let"`,
//     `case "ref"` AND `case "switch"`, and `Let.body` / `Switch.cases` go
//     through `requireRaw` UNVALIDATED, so the whole node vocabulary reaches
//     GMA through them — `Chain` included, despite there being no `case
//     "chain"`. The absence of a case label is not the absence of a path.
//   * The symptom is a plausible WRONG NUMBER, not a missing value. Before this
//     rule, `Let{bindings:{p:Listener}, body:Chain{stages:[Expr{value*10},
//     Ref{p}]}}` built with no refusal and one price=4 event delivered **4** to
//     the terminal rather than 40: the Expr's output went into
//     `RefStub::onValue` and the binding's own `Tee` supplied the raw value in
//     its place. That is why it is a build-time refusal and not a warning.
// ===========================================================================

// The defect shape. Goes red if the refusal is removed.
TEST_F(ComposedChain, RefWithUpstreamIsRefused) {
  const std::string msg = buildAndReportJson(R"({
    "key":1,"streamKey":"AAPL","field":"lastPrice",
    "pipeline":[{
      "type":"Let",
      "bindings":{ "p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"} },
      "body":{"type":"Chain","stages":[
        {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}},
        {"type":"Ref","name":"p"}
      ]}
    }]
  })", deps_);

  ASSERT_FALSE(msg.empty())
      << "a 'Ref' with a value-producing node built directly upstream of it must "
         "be refused at build time (ENC-1398): unrefused, the Expr's output is "
         "discarded by RefStub::onValue and the binding's own Tee delivers the "
         "raw value instead, so the terminal receives 4 and not 40";
  EXPECT_NE(msg.find("'Ref'"), std::string::npos)
      << "the message must name the node type: " << msg;
  // It must NOT claim to be a fan-in — a Ref joins nothing, and saying so sends
  // the reader looking for declared inputs that do not exist.
  EXPECT_EQ(msg.find("is a FAN-IN"), std::string::npos)
      << "a 'Ref' is not a fan-in and must not be described as one: " << msg;
  EXPECT_NE(msg.find("DISCARDED"), std::string::npos)
      << "the message must say the upstream value is discarded: " << msg;
  EXPECT_NE(msg.find("PLAUSIBLE WRONG NUMBER"), std::string::npos)
      << "the message must say the symptom is a wrong value, not a missing one, "
         "because that is what makes it worse than the fan-in case: " << msg;
  // ENC-1344's shape: name the Ref's own path inside the wrapper, not the
  // wrapper's, or the reader looks for a Ref at pipeline[0] and finds a Let.
  EXPECT_NE(msg.find("stages[1]"), std::string::npos)
      << "the message must name the Ref's own path, not the wrapper's: " << msg;
  EXPECT_NE(msg.find("section 5 Q7"), std::string::npos)
      << "the message must cite the SPEC section that ruled it: " << msg;
  EXPECT_NE(msg.find("ENC-1398"), std::string::npos)
      << "the message must name its own ticket: " << msg;
}

// Reached through the other forwarding wrappers too, so the rule is structural
// rather than a special case for `Chain`. `Tee`/`Switch`/`GroupSplit` fan the
// same incoming value into every branch, so a `Ref` behind one of them still has
// that value upstream of it.
TEST_F(ComposedChain, RefWithUpstreamIsRefusedThroughEveryForwardingWrapper) {
  struct Row { const char* what; const char* body; const char* where; };
  const std::vector<Row> rows = {
    {"Chain{Expr, Ref}",
     R"({"type":"Chain","stages":[
          {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}},
          {"type":"Ref","name":"p"}]})",
     "stages[1]"},
    {"Chain{Expr, Tee{Ref}}",
     R"({"type":"Chain","stages":[
          {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}},
          {"type":"Tee","outputs":[{"type":"Ref","name":"p"}]}]})",
     "stages[1].outputs[0]"},
    {"Chain{Expr, Switch{cases:[Ref]}}",
     R"({"type":"Chain","stages":[
          {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}},
          {"type":"Switch","select":{"ref":"value"},
           "cases":[{"type":"Ref","name":"p"}]}]})",
     "stages[1].cases[0]"},
    {"Chain{Expr, GroupSplit{child:Ref}}",
     R"({"type":"Chain","stages":[
          {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}},
          {"type":"GroupSplit","child":{"type":"Ref","name":"p"}}]})",
     "stages[1].child"},
  };

  for (const auto& r : rows) {
    const std::string json =
      std::string(R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
        "pipeline":[{"type":"Let",
          "bindings":{"p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"}},
          "body":)") + r.body + "}]}";
    const std::string msg = buildAndReportJson(json.c_str(), deps_);
    EXPECT_FALSE(msg.empty())
        << r.what << " must be refused — the Expr's output reaches a Ref: " << json;
    if (msg.empty()) continue;
    EXPECT_NE(msg.find("'Ref'"), std::string::npos)
        << r.what << " must name the node type: " << msg;
    EXPECT_NE(msg.find(r.where), std::string::npos)
        << r.what << " must name WHERE the Ref is (" << r.where << "): " << msg;
    EXPECT_EQ(msg.find("is a FAN-IN"), std::string::npos)
        << r.what << " must not call a Ref a fan-in: " << msg;
  }
}

// The accept direction, and it is the half that matters: every placement where a
// `Ref` legitimately has no upstream must still build. Without these rows the
// tests above would be equally satisfied by refusing every request containing a
// `Ref` at all, which would break named reuse outright.
TEST_F(ComposedChain, EveryAcceptedRefPlacementStillBuilds) {
  struct Row { const char* what; const char* json; };
  const std::vector<Row> rows = {
    // (a) `Ref` as stages[0] of a Chain — a chain HEAD has no upstream. This is
    //     the shape ExpressionLanguageCapstoneTest.LetBindingReusedByTwoExpressions
    //     writes down, here as a fan-in's declared inputs.
    {"Ref as stages[0] inside a fan-in's declared inputs",
     R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
        "node":{"type":"Let",
          "bindings":{"p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"}},
          "body":{"type":"Aggregate","arity":2,"inputs":[
            {"type":"Chain","stages":[{"type":"Ref","name":"p"},
              {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}}]},
            {"type":"Chain","stages":[{"type":"Ref","name":"p"},
              {"type":"Expr","expr":{"op":"add","args":[{"ref":"value"},1]}}]}]}}})"},
    // (b) `Ref` as the whole `Let` body — its only upstream is the head
    //     Listener, which SPEC §5 Q1 rules is a CLOCK and not a data source.
    {"Ref as the Let body under 'node'",
     R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
        "node":{"type":"Let",
          "bindings":{"p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"}},
          "body":{"type":"Ref","name":"p"}}})"},
    // (c) `Ref` as a branch head under `Tee` — each branch inherits the Tee's
    //     own disposition, which here is clock-only.
    {"Ref as a Tee branch head",
     R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
        "node":{"type":"Let",
          "bindings":{"p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"}},
          "body":{"type":"Tee","outputs":[{"type":"Ref","name":"p"},
            {"type":"Expr","expr":{"op":"add","args":[{"ref":"value"},1]}}]}}})"},
    // (d) the transform written the RIGHT way round — Ref first, then the Expr.
    //     This is exactly what the refusal's message tells the author to do, so
    //     it has to actually build.
    {"Chain{[Ref, Expr]} — the fix the refusal message names",
     R"({"key":1,"streamKey":"AAPL","field":"lastPrice",
        "pipeline":[{"type":"Let",
          "bindings":{"p":{"type":"Listener","streamKey":"AAPL","field":"lastPrice"}},
          "body":{"type":"Chain","stages":[{"type":"Ref","name":"p"},
            {"type":"Expr","expr":{"op":"mul","args":[{"ref":"value"},10]}}]}}]})"},
  };

  for (const auto& r : rows) {
    const std::string msg = buildAndReportJson(r.json, deps_);
    EXPECT_TRUE(msg.empty())
        << r.what << " has no upstream at the Ref and must still build; it was "
           "refused with: " << msg;
  }
}
