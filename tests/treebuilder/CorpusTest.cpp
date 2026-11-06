#include "krn/TreeBuilder.hpp"
#include "krn/ExecutionContext.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/AtomicStore.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/nodes/Listener.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "../support/CorpusPath.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <cmath>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace krn;

// Stub terminal node
class CorpusTerminal : public INode {
public:
    void onValue(const StreamValue&) override {}
    void shutdown() noexcept override {}
};

class CorpusTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        gThreadPool = std::make_shared<rt::ThreadPool>(2);
        dispatcher = std::make_unique<Dispatcher>(gThreadPool.get(), &store);
        deps.store = &store;
        deps.pool = gThreadPool.get();
        deps.dispatcher = dispatcher.get();
    }

    void TearDown() override {
        if (dispatcher) dispatcher.reset();
        if (gThreadPool) {
            gThreadPool->shutdown();
            gThreadPool.reset();
        }
    }

    AtomicStore store;
    std::unique_ptr<Dispatcher> dispatcher;
    tree::Deps deps;
};

TEST_F(CorpusTestFixture, AllCorpusRequestsBuild) {
    // ENC-807 (L17): a missing corpus must NOT silently green the suite.
    // ENC-1340: one resolver, one diagnostic — see tests/support/CorpusPath.hpp.
    std::ifstream ifs = krn::testsupport::openCorpusRequests();
    if (!ifs.is_open()) {
        FAIL() << krn::testsupport::corpusNotFoundDiagnostic();
        return;
    }

    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    ifs.close();

    ASSERT_FALSE(doc.HasParseError()) << "Failed to parse corpus JSON";
    ASSERT_TRUE(doc.IsArray()) << "Corpus must be a JSON array";

    int passed = 0;
    int failed = 0;
    int total = doc.Size();

    for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
        const auto& entry = doc[i];
        int corpusId = entry.HasMember("corpus_id") ? entry["corpus_id"].GetInt() : -1;
        std::string nl = entry.HasMember("nl") ? entry["nl"].GetString() : "";

        ASSERT_TRUE(entry.HasMember("request")) << "Entry " << i << " missing 'request'";
        const auto& req = entry["request"];

        auto terminal = std::make_shared<CorpusTerminal>();

        try {
            auto chain = tree::buildForRequest(req, deps, terminal);
            EXPECT_NE(chain.head, nullptr)
                << "Corpus #" << corpusId << " built null head: " << nl;

            // Shut down to clean up Listener/Interval threads
            if (chain.head) chain.head->shutdown();
            for (auto& node : chain.keepAlive) {
                if (node) node->shutdown();
            }

            ++passed;
        } catch (const std::exception& e) {
            ADD_FAILURE() << "Corpus #" << corpusId << " FAILED: " << e.what()
                          << "\n  NL: " << nl;
            ++failed;
        }
    }

    std::cout << "\n[CorpusTest] " << passed << "/" << total << " requests built successfully\n";
    if (failed > 0) {
        std::cout << "[CorpusTest] " << failed << " FAILURES\n";
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ENC-1289 — real value assertions (SPEC specs/2026-09-20-krn-join-correctness
// D8, §1.2).
//
// Everything above this line asserts `chain.head != nullptr`. That is a build
// smoke test: it cannot distinguish a join that computes the right answer from
// one that computes `+1.00` where the truth is `0.02`. What follows drives a
// deterministic tick sequence into the *real* DAG of a named corpus entry and
// asserts what comes out.
//
// WHAT IS REAL HERE. The DAG is built by `tree::buildForRequest` from the
// verbatim `request` object of the corpus file — the same entry, read from the
// same file, that `AllCorpusRequestsBuild` iterates. Values enter through
// `Dispatcher::onTick`. No node is stubbed, subclassed or reimplemented; the
// single substitution is the terminal, which `buildForRequest` takes as a
// parameter and which `AllCorpusRequestsBuild` already substitutes.
//
// ───────────────────────────────────────────────────────────────────────────
// THE CONCURRENCY PROBLEM, AND HOW THESE TESTS HANDLE IT
//
// SPEC §1.1 names four defects. Two of them are *state-machine* defects and
// two are *ordering* defects, and they need opposite test strategies:
//
//   defect 2 — `Aggregate` counts VALUES, not distinct inputs
//              (`Aggregate::onValue` pre-ENC-1291: `buf_[sv.symbol].vals` grew and
//              fires at `>= arity_`), so a two-input node completes a "tuple"
//              from two values of ONE input.
//   defect 3 — the correlation key is `sv.symbol`, so no cross-streamKey join
//              exists at all; two symbols occupy two independent buffers.
//   defect 4 — `Listener::onValue` posts every value to a shared `ThreadPool`
//              queue, so two ticks from one Listener execute concurrently and
//              the join pairs across ticks.
//
// Defects 2 and 3 are properties of `Aggregate`'s buffer, held under its own
// mutex. They do NOT depend on scheduling: the counting is the same at one
// thread as at sixteen. So the tests for them run at **threads=1** and are
// exactly reproducible — no seed, no repetition budget, no flake.
//
// Defect 4 is a race, and it is the trap in this ticket. At **threads=1** the
// join is correct BY CONSTRUCTION: `ThreadPool`'s queue is FIFO, one worker
// drains it in order, and `Dispatcher::onTick`'s inner listener container is a
// `std::map`, so `ask` is notified before `bid` deterministically (`"ask" <
// "bid"`). The committed probe at
// specs/2026-09-20-krn-join-correctness/probe/ measured 0 wrong tuples in
// 1,200,000 consecutive single-threaded tuples. **A value assertion written at
// one thread is therefore green by construction against the very defect it
// names.**
//
// So the value assertion is written twice, deliberately:
//
//   `Corpus86_SpreadIsExactlyTwoCents_SingleThreadControl` runs the identical
//   driver and the identical checker at threads=1. It PASSES today. Its job is
//   to prove the checker is capable of returning "clean" — a checker that
//   always reports corruption gates nothing — and to pin, in the suite itself,
//   the fact that one thread hides this defect completely.
//
//   `Corpus86_SpreadIsExactlyTwoCents` runs the same checker at threads=4 —
//   the worst width the probe measured (pooled 6.62-7.27% wrong tuples over
//   1.2M tuples; per-1000-tick-run 1.70%-13.80%, minimum over 1,200 runs
//   1.70%, never zero). It FAILS today.
//
// WHAT THE THREAD-COUNT CHOICE COVERS, AND WHAT IT DOES NOT. Four threads is
// one point on a curve that is NOT monotonic (probe: 2 threads 2.05%, 4
// threads 6.66%, 8 threads 3.06%, 16 threads 2.62%). These tests do not sweep
// widths — that is the probe's job, and the probe is committed for exactly
// that reason. They pick the width with the largest measured effect so the
// gate's detection probability is highest, and they say so here rather than
// implying the choice was principled beyond that.
//
// THE REPETITION BUDGET. `kRaceReps` x `kRaceTicks` ticks yields
// `kRaceReps * kRaceTicks` tuples. Taking the probe's *minimum* observed
// per-run rate (1.70%) as a pessimistic p, the probability this gate sees zero
// wrong tuples and passes by luck is (1 - 0.017)^(reps*ticks): at 4 x 1000
// that is e^-68, i.e. ~2e-30. Even at a p of 0.001 — sixteen times below
// anything measured — it is e^-4 ~ 1.8%. The budget is stated, not assumed.
//
// WHAT IS NOT COVERED HERE. SPEC §1.1 defect 1 (a request carrying BOTH `node`
// and `pipeline` builds two live chains into one terminal) has no assertion
// below. Its correct shape is `Q1`, still open in the SPEC, and it blocks
// ENC-1290; pinning an expectation before Q1 is answered would pin a guess.
// The tests below sidestep it instead: corpus 86 is driven with `bid`/`ask`
// only (never `lastPrice`), which leaves the outer `Listener(AAPL,lastPrice)`
// silent and makes the terminal stream unambiguously the join's output.
//
// ───────────────────────────────────────────────────────────────────────────
// MEASURED, NOT ASSUMED (ENC-1289, GMA_V3@7928d78, g++ 16.2.1, AMD Ryzen 7
// 5800XT 8C/16T, Fedora 44, Release; 1-minute load average 12.4-17.6
// throughout — other agents were compiling)
//
//   Corpus86_JoinMustNotCompleteFromOneInputAlone   red 40/40 runs, and every
//       run reported the SAME numbers (8 arrivals, 4 tuples, 4 same-side).
//   Corpus87_CrossSymbolJoinNeverPairsOneSideWithItself
//                                                   red 40/40 runs, same
//       numbers every run (6 tuples, 6 same-side).
//   Corpus86_SpreadIsExactlyTwoCents                red 40/40 runs; wrong-tuple
//       rate per invocation 3.62% - 8.22% of 4000 tuples (mean ~6.4%),
//       consistent with the probe's pooled 6.62-7.27% at this width. At the
//       lowest rate observed here the chance of a clean pass is (1-0.0362)^4000
//       ~ 1e-64; at the probe's all-time minimum single-run rate (1.70%) it is
//       ~1e-30.
//       ENC-1005 (2026-09-20): GREEN 40/40 after the per-request strand —
//       160,000 further tuples at threads=4, 0 wrong. The rates above are what
//       the same binary measured the day before, and are what makes the green
//       mean something.
//   Corpus86_SpreadIsExactlyTwoCents_SingleThreadControl
//                                                   green 40/40 runs
//       = 40,000 further single-threaded tuples, 0 wrong.
//
// FALSIFIED IN BOTH DIRECTIONS. A check never seen fail is not a check; a check
// that can only fail is not one either. Each assertion was run against a
// throwaway prototype of the fixes it names (NOT committed, reverted after
// measurement):
//
//   port-indexed fan-in alone (a prototype of ENC-1291/D2: one slot per
//       declared input, emit only when all are filled)
//         -> JoinMustNotCompleteFromOneInputAlone            RED  -> GREEN
//         -> Corpus87_CrossSymbolJoinNeverPairsOneSideWithItself
//                                                            RED  -> GREEN
//         -> SpreadIsExactlyTwoCents      still RED, and its failures changed
//            character completely: 0 same-side, 0 sign-flip, 1303/3979 (32.75%)
//            CROSS_TICK. Exactly what SPEC D3 predicts is left once the
//            counting defect is gone, and evidence the classifier discriminates
//            between the two defects rather than reporting one number.
//   + ordered delivery (a prototype of ENC-1005/D3: both ThreadPool hops made
//       synchronous)
//         -> all four GREEN at threads=4.
//
// ENC-1005 SHIPPED ordered delivery and it flipped EXACTLY ONE of the three, as
// the line above predicts for the ordering defect alone: SpreadIsExactlyTwoCents
// RED -> GREEN, and the two state-machine gates (defects 2 and 3, both driven at
// threads=1) unmoved. The shipped form is not "both hops synchronous": hop one
// (Dispatcher -> Listener) is inline, hop two (Listener -> the DAG) is a
// per-request `rt::Strand`, so the compute is still off the ingress thread and
// the thread count is unchanged.
//
// ───────────────────────────────────────────────────────────────────────────
// SOME OF THESE FAIL TODAY, ON PURPOSE, AND `ctest` IS STILL GREEN
//
// SPEC D8: "an instrument that cannot fail is not a gate. This is sequenced
// first deliberately." So three of the four assertions below were red against
// the engine at ENC-1289 and were MEANT to be. One still is; the bookkeeping
// of which is at the foot of this block.
//
// They are not disabled, skipped or commented out. Each is registered in
// CMakeLists.txt as its own ctest case with `WILL_FAIL TRUE`: the assertion
// executes on every run, ctest reports the case as a pass *because* the
// assertion fails, and the suite stays green for the other sessions building
// this repo. A red `ctest` on master would have turned "is the build green?"
// into a question everyone has to re-investigate, and the answer decays to
// "ignore those three" within a day.
//
// The property that makes this better than a DISABLED_ test: it unwinds
// itself. When the fix lands the assertion starts passing, `WILL_FAIL` flips
// its ctest case RED, and whoever landed the fix has to come here and remove
// the marker. A disabled test rots silently; this one demands attention
// exactly once, at the moment it becomes wrong.
//
//   Corpus87_CrossSymbolJoinNeverPairsOneSideWithItself
//                                                  -> ENC-1292 (declared `by`,
//                                                     D1)
//
// TWO, not three, as of ENC-1005 (2026-09-20). `Corpus86_SpreadIsExactlyTwoCents`
// was the third; SPEC D3's per-request strand landed, it went green, its
// `WILL_FAIL` block was deleted and the count pinned by
// `gma_enc1289_expected_failures_really_failed` went from 3 to 2. It now runs
// inside the main `krn_tests` case. This paragraph unwinding itself one line at
// a time is the mechanism working as designed.
//
// ONE, not two, as of ENC-1291 (2026-09-20). SPEC D2's port-indexed fan-in
// landed and `Corpus86_JoinMustNotCompleteFromOneInputAlone` went green; its
// block is gone and the pinned count is 1.
//
// **AND THE SURVIVOR DID NOT STAY RED BY ITSELF — READ THIS BEFORE TRUSTING
// IT.** ENC-1289's own prototype measurement four screens above predicted
// `Corpus87_...` would go RED -> GREEN under port-indexing alone, and it did.
// It went green VACUOUSLY: port-indexing puts AAPL and MSFT in two different
// `buf_[sv.symbol]` entries which each stay half-filled forever, so the join
// emits NOTHING, `joined` is empty, the same-side loop never runs and
// `sameSide == 0` holds over zero tuples. That is D1's locked `by:"streamKey"`
// default behaving correctly and the cross-symbol join still not existing —
// the branch ENC-1289 flagged as "honest but weak". ENC-1291 therefore added
// an explicit anti-vacuity ASSERT to that test (see the block above it) so it
// is red for the real reason instead of green for a hollow one. The marker
// stays; ENC-1292 clears it by making the join actually emit six mixed tuples
// under `by:"none"`.
//
// `Corpus86_SpreadIsExactlyTwoCents_SingleThreadControl` is deliberately NOT
// registered that way. It is an ordinary always-green test — inverting it
// would assert that the checker must never return clean, which is the opposite
// of what a control is for.
//
// To see the real verdicts rather than the inverted ones, run the binary
// directly: `./krn_tests --gtest_filter='CorpusValueAssertions.*'`. That is
// also the form the ENC-1065 `--gtest_repeat` check needs (full, unfiltered).
// ═══════════════════════════════════════════════════════════════════════════

namespace corpus_values {

// ─── Recording terminal ────────────────────────────────────────────────────
// `Aggregate::onValue` takes the completed batch under its mutex and then
// forwards its members one at a time, OUTSIDE the lock, from a single thread
// in a tight loop (`Aggregate::onPortValue`'s forwarding loop, outside the
// lock). So the terminal's
// per-thread arrival sequence is exactly a concatenation of arity-sized
// batches: two batches forwarded concurrently are on two threads and cannot
// interleave. Stamping each arrival with the forwarding thread is what makes
// tuples recoverable at all. Every recovery below asserts each per-thread run
// has even length — if that ever fails the pairing is unsound and the verdict
// must be discarded, not believed.
class RecordingTerminal final : public krn::INode {
public:
  void onValue(const krn::StreamValue& sv) override {
    const double* d = std::get_if<double>(&sv.value);
    std::lock_guard<std::mutex> lk(mx_);
    // ENC-1292: the SYMBOL is recorded too. `ClientSession` serialises
    // `sv.symbol` as the frame's `streamKey`, so a join's output identity is
    // whatever the last node put there — and under `by:"none"` that is a
    // design decision, not an accident (SPEC section 5 Q6). Nothing could ask
    // this terminal what identity it saw before.
    ++symbols_[sv.symbol];
    if (d) byThread_[std::this_thread::get_id()].push_back(*d);
    else   ++nonNumeric_;
  }
  void shutdown() noexcept override {}

  std::map<std::string, std::size_t> symbols() const {
    std::lock_guard<std::mutex> lk(mx_);
    return symbols_;
  }

  std::map<std::thread::id, std::vector<double>> take() {
    std::lock_guard<std::mutex> lk(mx_);
    auto out = std::move(byThread_);
    byThread_.clear();
    return out;
  }
  std::size_t nonNumeric() const {
    std::lock_guard<std::mutex> lk(mx_);
    return nonNumeric_;
  }

private:
  mutable std::mutex mx_;
  std::map<std::thread::id, std::vector<double>> byThread_;
  std::map<std::string, std::size_t> symbols_;
  std::size_t nonNumeric_{0};
};

// ─── Corpus access ─────────────────────────────────────────────────────────
// Same search paths as AllCorpusRequestsBuild above; CMake copies the corpus
// next to the test binary and runs the suite from that directory.
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

// Returns nullptr when the corpus is missing/unparseable or the id is absent —
// callers ASSERT on it, so a packaging failure is loud rather than a skip.
const rapidjson::Value* corpusRequest(int corpusId) {
  rapidjson::Document& d = corpusDoc();
  if (d.IsNull() || d.HasParseError() || !d.IsArray()) return nullptr;
  for (auto& e : d.GetArray()) {
    if (!e.IsObject() || !e.HasMember("corpus_id") || !e["corpus_id"].IsInt()) continue;
    if (e["corpus_id"].GetInt() != corpusId) continue;
    return e.HasMember("request") ? &e["request"] : nullptr;
  }
  return nullptr;
}

// ─── The join's observation point (ENC-1290) ───────────────────────────────
//
// Every assertion in this block is about the JOIN: which values `Aggregate`
// pairs into a tuple. Until ENC-1290 the join's raw members reached the
// terminal directly, because a request carrying both `node` and `pipeline`
// built TWO live chains into one Responder (SPEC §1.1 defect 1) and the
// `node` chain was one of them. Driving only the join's own input fields left
// the other chain silent, so the terminal stream WAS the join's output.
//
// D5 composed the two into one chain, so for corpus 86 and 87 the terminal now
// sees `Worker{fn:"diff"}` output instead. That is the fix working, and it
// moves the observation point rather than changing what the join does. Left
// alone, these tests would have gone quietly wrong in three different ways —
// all three measured before this change landed:
//
//   * Corpus86_SpreadIsExactlyTwoCents      — HOLLOWED OUT. `decodeBidAsk`
//     decodes 0 of 6 arrivals, every tuple scores UNDECODABLE, and it stays
//     red for a reason ENC-1005 can never clear.
//   * ..._SingleThreadControl               — the same checker, and it is NOT
//     in GMA_ENC1289_EXPECTED_FAILURES, so it runs inside the MAIN `krn_tests`
//     ctest case and took the primary target red.
//   * Corpus87_CrossSymbolJoin...           — BROKEN. Its `>= 900.0` filter
//     (there to drop the second chain's small diffs) kept 0 values, the test
//     passed vacuously, and its WILL_FAIL ctest case flipped to FAILED —
//     taking `gma_enc1289_expected_failures_really_failed` with it.
//
// So each of them drives the corpus entry's `node` WITHOUT its pipeline. The
// request is still built by the real `tree::buildForRequest` and driven
// through the real `Dispatcher`, the `node` is the corpus entry's own
// verbatim, and `tests/treebuilder/corpus_requests.json` is NOT touched. The
// assertions, their thread counts, their tick encodings and their expected
// red/green states are all byte-for-byte what ENC-1289 committed.
//
// What D5 itself introduced — the composed chain end to end, including corpus
// 86's exact emitted values through `Worker{fn:"diff"}` — is asserted in
// tests/treebuilder/ComposedChainTest.cpp, not here.
rapidjson::Document nodeWithoutPipeline(const rapidjson::Value& request) {
  rapidjson::Document d;
  d.CopyFrom(request, d.GetAllocator());
  d.RemoveMember("pipeline");
  d.RemoveMember("stages");
  return d;
}

// ENC-1292 / SPEC D1 — the same `node`, with the join key it needs DECLARED.
//
// `tests/treebuilder/corpus_requests.json` is still NOT touched: `by` is added
// to the COPY. That is deliberate and is the D6 boundary. The corpus is the
// authored record of what clients ask for, and no authored request carries a
// `by` — the member did not exist when the corpus was written, and forum does
// not emit one. Editing the file would delete the very baseline the default
// has to be measured against, which is why `Corpus87_ObservationPointIsSound_
// Control` below drives the request BOTH ways from the one entry.
rapidjson::Document nodeWithJoinByNone(const rapidjson::Value& request) {
  rapidjson::Document d = nodeWithoutPipeline(request);
  auto& al = d.GetAllocator();
  EXPECT_TRUE(d.HasMember("node") && d["node"].IsObject())
      << "this helper declares a join key on the request's `node`, and there "
         "is none";
  auto& node = d["node"];
  node.RemoveMember("by");
  node.AddMember("by", rapidjson::Value("none", al).Move(), al);
  return d;
}

// ─── Driver ────────────────────────────────────────────────────────────────
struct Drive {
  std::map<std::thread::id, std::vector<double>> byThread;
  std::map<std::string, std::size_t> symbols;     // ENC-1292, SPEC Q6
  std::size_t arrivals{0};
  std::size_t nonNumeric{0};

  std::string renderSymbols() const {
    std::ostringstream os;
    for (const auto& [sym, n] : symbols) os << " " << sym << "=" << n;
    return os.str();
  }
};

Drive driveCorpus(const rapidjson::Value& request,
                  unsigned threads,
                  const std::function<void(krn::Dispatcher&)>& inject) {
  using namespace krn;

  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(threads);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);
  tree::Deps deps;
  deps.store      = &store;
  deps.pool       = pool.get();
  deps.dispatcher = &dispatcher;

  auto terminal = std::make_shared<RecordingTerminal>();
  auto chain    = tree::buildForRequest(request, deps, terminal);

  inject(dispatcher);
  pool->drain();

  Drive out;
  out.byThread   = terminal->take();
  out.symbols    = terminal->symbols();
  out.nonNumeric = terminal->nonNumeric();
  for (auto& [tid, vals] : out.byThread) { (void)tid; out.arrivals += vals.size(); }

  for (auto& n : chain.keepAlive) if (n) n->shutdown();
  if (chain.head) chain.head->shutdown();
  chain.keepAlive.clear();
  chain.head.reset();
  pool->shutdown();
  gThreadPool = prevPool;
  return out;
}

void tick(krn::Dispatcher& d, const char* symbol,
          std::initializer_list<std::pair<const char*, double>> fields) {
  auto payload = std::make_shared<rapidjson::Document>();
  payload->SetObject();
  auto& al = payload->GetAllocator();
  for (const auto& [name, value] : fields)
    payload->AddMember(rapidjson::StringRef(name), value, al);
  krn::Event ev;
  ev.symbol  = symbol;
  ev.payload = payload;          // ev.type defaults to "tick" — the production type
  d.onTick(ev);
}

// ─── Encoding ──────────────────────────────────────────────────────────────
// Corpus 86 (one symbol, two fields). Tick n carries
//
//     bid = 1000.00 + n        ask = 1000.00 + n + 0.02
//
// so the truth for every tick is a +0.02 spread, and every value decodes
// EXACTLY (not within a tolerance — n is integral and 0.02 is the only
// fraction) back to its (tick, side). Base 1000 keeps every raw price an order
// of magnitude above any difference of two of them, so a raw price can never
// be confused with a `Worker{fn:"diff"}` output.
constexpr double kBase   = 1000.00;
constexpr double kSpread = 0.02;

enum class Side { Bid, Ask, Unknown };

struct Decoded {
  long long tick{-1};
  Side      side{Side::Unknown};
};

Decoded decodeBidAsk(double v) {
  const long long m = std::llround((v - kBase) * 100.0);
  if (m < 0) return {};
  Decoded d;
  d.tick = m / 100;
  switch (m % 100) {
    case 0: d.side = Side::Bid; break;
    case 2: d.side = Side::Ask; break;
    default: return {};
  }
  return d;
}

const char* sideName(Side s) {
  switch (s) {
    case Side::Bid: return "bid";
    case Side::Ask: return "ask";
    default:        return "??";
  }
}

// How a recovered tuple is wrong. A tuple is CORRECT only if it is
// {ask(n), bid(n)} in that order — the order a pairwise `diff` needs to yield
// +0.02.
enum class Kind { Correct, SignFlip, SameSide, CrossTick, Undecodable };

Kind classifyTuple(double first, double second) {
  const Decoded a = decodeBidAsk(first), b = decodeBidAsk(second);
  if (a.side == Side::Unknown || b.side == Side::Unknown) return Kind::Undecodable;
  if (a.side == b.side)  return Kind::SameSide;    // ask-with-ask -> a +-k.00 spread
  if (a.tick != b.tick)  return Kind::CrossTick;
  return (a.side == Side::Ask) ? Kind::Correct : Kind::SignFlip;
}

const char* kindName(Kind k) {
  switch (k) {
    case Kind::Correct:     return "CORRECT";
    case Kind::SignFlip:    return "SIGN_FLIP";
    case Kind::SameSide:    return "SAME_SIDE";
    case Kind::CrossTick:   return "CROSS_TICK";
    default:                return "UNDECODABLE";
  }
}

struct TupleReport {
  std::size_t tuples{0};
  std::size_t wrong{0};
  std::size_t signFlip{0};
  std::size_t sameSide{0};
  std::size_t crossTick{0};
  std::size_t undecodable{0};
  std::size_t oddRuns{0};        // per-thread runs of odd length: pairing unsound
  std::string examples;          // first few offending tuples, rendered
};

void accumulate(const Drive& d, TupleReport& r, std::size_t maxExamples = 6) {
  for (const auto& [tid, vals] : d.byThread) {
    (void)tid;
    if (vals.size() % 2 != 0) ++r.oddRuns;
    for (std::size_t i = 0; i + 1 < vals.size(); i += 2) {
      ++r.tuples;
      const Kind k = classifyTuple(vals[i], vals[i + 1]);
      if (k == Kind::Correct) continue;
      ++r.wrong;
      switch (k) {
        case Kind::SignFlip:  ++r.signFlip;  break;
        case Kind::SameSide:  ++r.sameSide;  break;
        case Kind::CrossTick: ++r.crossTick; break;
        default:              ++r.undecodable; break;
      }
      if (r.wrong <= maxExamples) {
        const Decoded a = decodeBidAsk(vals[i]), b = decodeBidAsk(vals[i + 1]);
        std::ostringstream os;
        os << "\n      " << kindName(k) << ": {" << std::fixed
           << std::setprecision(2) << vals[i] << ", " << vals[i + 1]
           << "}  = {" << sideName(a.side) << "(tick " << a.tick << "), "
           << sideName(b.side) << "(tick " << b.tick << ")}"
           << "  -> pairwise diff " << std::showpos << (vals[i] - vals[i + 1])
           << std::noshowpos << " (truth: +" << kSpread << ")";
        r.examples += os.str();
      }
    }
  }
}

// ─── Budget ────────────────────────────────────────────────────────────────
constexpr unsigned    kRaceThreads = 4;     // the worst width the probe measured
constexpr std::size_t kRaceTicks   = 1000;
constexpr std::size_t kRaceReps    = 4;     // 4000 tuples; see the budget note above

} // namespace corpus_values

// ═══ 1. Defect 2 — the join counts values, not inputs ══════════════════════
//
// DETERMINISTIC, at threads=1, and red today for a reason that has nothing to
// do with scheduling.
//
// Corpus 86 declares a two-input join: Listener(AAPL,ask) and
// Listener(AAPL,bid). Drive ticks that carry ONLY `ask`. The `bid` input never
// fires once. A two-input join cannot complete a single tuple, so the terminal
// must receive nothing.
//
// Today `Aggregate` buffers per `sv.symbol` with no input index and fires at
// `vals.size() >= arity_`, so eight `ask` values become four "complete tuples"
// of {ask(n), ask(n+1)} — a pairwise spread of exactly -1.00 where the truth
// is 0.02. That is SPEC §0's headline number, reproduced here with no race,
// no repetition and no thread-count dependence.
//
// GREEN as of ENC-1291 (port-indexed fan-in + arity enforcement, SPEC D2),
// 2026-09-20. Each declared input now terminates in its own `InputPort` and the
// join completes only when every port has contributed, so eight `ask` values
// with no `bid` complete nothing. The `WILL_FAIL` registration and the marker
// in `GMA_ENC1289_EXPECTED_FAILURES` were removed in that same commit, and the
// count pinned by `gma_enc1289_expected_failures_really_failed` went 2 -> 1.
// The assertion itself is untouched.
//
// IT IS NOT VACUOUS, and the thing that proves it is a SIBLING test rather than
// anything in here: `Corpus86_SpreadIsExactlyTwoCents` and its
// `_SingleThreadControl` twin drive the SAME corpus entry through the SAME
// driver with BOTH sides ticking, and assert that the join emits tuples and
// that every one of them is {ask(n), bid(n)}. So "0 arrivals" below cannot be
// the driver, the terminal or the corpus entry being inert — it is the missing
// `bid` and nothing else. Break the driver and those two go red, loudly.
TEST(CorpusValueAssertions, Corpus86_JoinMustNotCompleteFromOneInputAlone) {
  using namespace corpus_values;

  const rapidjson::Value* req = corpusRequest(86);
  ASSERT_NE(req, nullptr) << "corpus_id 86 not found in corpus_requests.json";
  rapidjson::Document join = nodeWithoutPipeline(*req);   // ENC-1290, see above

  constexpr std::size_t kTicks = 8;

  Drive d = driveCorpus(join, /*threads=*/1, [](krn::Dispatcher& disp) {
    for (std::size_t n = 0; n < kTicks; ++n)
      tick(disp, "AAPL", {{"ask", kBase + double(n) + kSpread}});  // no "bid"
  });

  TupleReport r;
  accumulate(d, r);

  EXPECT_EQ(d.arrivals, 0u)
      << "corpus_id 86 \"Bid-ask spread for AAPL\" declares a TWO-input join "
         "(Listener(AAPL,ask) + Listener(AAPL,bid)).\n"
         "    " << kTicks << " ticks carrying ONLY `ask` were injected; `bid` never "
         "fired.\n"
         "    A two-input join has no complete tuple to emit, so the terminal must "
         "see 0 values.\n"
         "    It saw " << d.arrivals << ", forming " << r.tuples << " \"tuple(s)\", "
      << r.wrong << " of them wrong (" << r.sameSide << " same-side):"
      << r.examples
      << "\n\n    This was RED until ENC-1291: `Aggregate` buffered a flat vector per "
         "`sv.symbol` with no\n"
         "    input index and fired on `vals.size() >= arity_` — counting VALUES, "
         "not distinct INPUTS.\n"
         "    A failure here now is a REGRESSION of SPEC D2: check that "
         "`TreeBuilder`'s Aggregate builder\n"
         "    still gives each declared input its own `InputPort`, and that "
         "`Aggregate::onPortValue`\n"
         "    still requires every slot filled before it emits.";

  EXPECT_EQ(r.oddRuns, 0u)
      << "per-thread arrival run had odd length — tuple recovery is unsound here";
  EXPECT_EQ(d.nonNumeric, 0u) << "terminal received a non-numeric value";
}

// ═══ 2a. Defect 4 — CONTROL. The same checker, at one thread. ══════════════
//
// PASSES today, and must keep passing.
//
// This test exists to falsify the checker, not the engine. At threads=1 the
// join is correct by construction (FIFO queue, one worker, `std::map` field
// order puts `ask` before `bid`), so a green here proves `classifyTuple` can
// return CORRECT at all — without which the red in 2b below would prove
// nothing. It also pins, in the suite itself, the reason a single-threaded
// value assertion is not a gate for defect 4.
//
// NOT registered WILL_FAIL, deliberately: this one is an ordinary always-green
// ctest case. Inverting it would assert that the checker must never return
// clean, which is the opposite of what a control is for.
TEST(CorpusValueAssertions, Corpus86_SpreadIsExactlyTwoCents_SingleThreadControl) {
  using namespace corpus_values;

  const rapidjson::Value* req = corpusRequest(86);
  ASSERT_NE(req, nullptr) << "corpus_id 86 not found in corpus_requests.json";
  rapidjson::Document join = nodeWithoutPipeline(*req);   // ENC-1290, see above

  Drive d = driveCorpus(join, /*threads=*/1, [](krn::Dispatcher& disp) {
    for (std::size_t n = 0; n < kRaceTicks; ++n)
      tick(disp, "AAPL", {{"bid", kBase + double(n)},
                          {"ask", kBase + double(n) + kSpread}});
  });

  TupleReport r;
  accumulate(d, r);

  ASSERT_EQ(r.oddRuns, 0u) << "tuple recovery unsound (odd-length per-thread run)";
  ASSERT_GT(r.tuples, 0u) << "the join emitted nothing — the control proves nothing";
  EXPECT_EQ(r.wrong, 0u)
      << "CONTROL FAILED. At threads=1 this join is correct BY CONSTRUCTION and the\n"
         "    committed probe (specs/2026-09-20-krn-join-correctness/probe/) measured 0\n"
         "    wrong tuples in 1,200,000 consecutive single-threaded tuples. A failure\n"
         "    here means either the engine's single-threaded ordering changed or the\n"
         "    checker below is broken — investigate before trusting any verdict from\n"
         "    Corpus86_SpreadIsExactlyTwoCents."
      << r.examples;
}

// ═══ 2b. Defect 4 — the value assertion. THE spread must be 0.02. ═════════
//
// GREEN as of ENC-1005 (per-request strand, SPEC D3/D4), 2026-09-20. This is
// the "emitted value asserted exactly" ENC-1289 was asked for: every tuple the
// join completes must be {ask(n), bid(n)} of the SAME tick, in that order, so a
// pairwise diff is exactly +0.02 — never +-1.00 (ask paired with ask), never
// -0.02 (sign flip), never +-0.98 (across ticks).
//
// Runs at threads=4 for `kRaceReps` x `kRaceTicks` ticks. See the budget note
// at the top of this block for the probability this would pass by luck if the
// defect were still present (~2e-30 at the probe's most favourable observed
// rate) — which is what makes it a gate rather than a sample.
//
// +-- WAS `WILL_FAIL`, AND IS NOT ANY MORE -------------------------------+
// | ENC-1289 registered this as ctest case                                |
// |   gma_enc1289_xfail_spread_is_not_two_cents_under_concurrency         |
// | with WILL_FAIL TRUE. ENC-1005 deleted that block and dropped the name |
// | from GMA_ENC1289_EXPECTED_FAILURES, so this test now runs inside the  |
// | main `krn_tests` ctest case like any ordinary test, and a regression  |
// | in ordered delivery takes the PRIMARY target red.                     |
// | The assertion is byte-for-byte ENC-1289's. Nothing about what it      |
// | checks, how it drives, or how many threads it uses was touched.       |
// +------------------------------------------------------------------------+
//
// MEASURED AFTER THE FIX (ENC-1005, Release, Ryzen 7 5800XT, 1-min load
// average 4.3-6.9): 40 consecutive invocations green = 160,000 tuples at
// threads=4, 0 wrong. Before the fix the same binary's own baseline was red
// 40/40. See the ticket for the full before/after.
TEST(CorpusValueAssertions, Corpus86_SpreadIsExactlyTwoCents) {
  using namespace corpus_values;

  const rapidjson::Value* req = corpusRequest(86);
  ASSERT_NE(req, nullptr) << "corpus_id 86 not found in corpus_requests.json";

  rapidjson::Document join = nodeWithoutPipeline(*req);   // ENC-1290, see above

  TupleReport r;
  for (std::size_t rep = 0; rep < kRaceReps; ++rep) {
    Drive d = driveCorpus(join, kRaceThreads, [](krn::Dispatcher& disp) {
      for (std::size_t n = 0; n < kRaceTicks; ++n)
        tick(disp, "AAPL", {{"bid", kBase + double(n)},
                            {"ask", kBase + double(n) + kSpread}});
    });
    accumulate(d, r);
  }

  ASSERT_EQ(r.oddRuns, 0u)
      << "tuple recovery unsound (odd-length per-thread run) — discard this verdict";
  ASSERT_GT(r.tuples, 0u) << "the join emitted nothing";

  const double pct = 100.0 * double(r.wrong) / double(r.tuples);
  EXPECT_EQ(r.wrong, 0u)
      << "corpus_id 86 \"Bid-ask spread for AAPL\": the spread is 0.02 on every tick.\n"
         "    Driven at threads=" << kRaceThreads << ", " << kRaceReps << " x "
      << kRaceTicks << " ticks.\n"
         "    " << r.wrong << " of " << r.tuples << " tuples (" << std::fixed
      << std::setprecision(2) << pct << "%) pair the wrong two values:\n"
         "      same-side (ask with ask / bid with bid, spread +-k.00): " << r.sameSide << "\n"
         "      sign flip (bid before ask, spread -0.02):               " << r.signFlip << "\n"
         "      cross-tick:                                             " << r.crossTick << "\n"
      << r.examples
      << "\n\n    THIS IS A REGRESSION IN ORDERED DELIVERY (ENC-1005, SPEC D3/D4).\n"
         "    It passed at 4 threads, 40 consecutive invocations, when ENC-1005 landed.\n"
         "    Ordered delivery is TWO cooperating pieces and breaking either one\n"
         "    reddens this test — check both before looking anywhere else:\n"
         "      1. every Listener of ONE request DAG shares ONE rt::Strand\n"
         "         (tree::Deps::strand, minted per subscription in\n"
         "         ClientSession::handleSubscribe and per DAG in buildForRequest);\n"
         "      2. Dispatcher delivers INLINE to a strand-bearing Listener\n"
         "         (Dispatcher::deliver / INode::deliversOnOwnExecutor) instead of\n"
         "         posting each notification as an independent pool task, which\n"
         "         scrambles ask-vs-bid one hop UPSTREAM of the strand.\n"
         "    Without (2), (1) alone leaves this test red — measured, see ENC-1005.\n"
         "    At threads=1 this test's control twin passes; only the pool width differs.";
}

// ═══ 2c. ENC-1290 — the CONTROL on corpus 87's observation point ═══════════
//
// PASSES today, and must keep passing.
//
// WHY IT EXISTS, IN THE PAST TENSE (ENC-1292). Gate 3 below carries a
// `>= 900.0` filter and an assertion that the filter dropped nothing. While
// gate 3 was registered `WILL_FAIL TRUE` that assertion could not gate
// anything from where it sat: the case was *required* to fail, so ctest read a
// second failure inside it as the expected one, and injecting the exact
// regression it guards against (stop stripping the pipeline, so the terminal
// sees `Worker{fn:"diff"}` output and the filter keeps 0 of 12 arrivals) left
// `gma_enc1289_xfail_no_cross_symbol_join` reporting **Passed**.
//
// That registration is GONE — ENC-1292 cleared the gate and deleted the whole
// expected-failure block from CMakeLists.txt — so gate 3's own assertions now
// gate normally. This control is kept anyway, and it is not redundant: it
// drives the entry BOTH ways from one corpus row, so it is the only thing that
// measures the DEFAULT's zero against a driver it simultaneously proves is
// live.
//
// Corpus 86 already has such a control (2a, and it is what caught D5 hollowing
// out gate 2). Corpus 87 had none. This is it: the same drive as gate 3, with
// only the soundness of the observation point asserted and no claim about the
// join's correctness — so it is green now, stays green through ENC-1291 and
// ENC-1292, and goes red the moment gate 3 starts measuring the wrong stream.
//
// ENC-1291 (2026-09-20) FOUND IT GREEN FOR A REASON IT WAS NOT WRITTEN FOR:
// since SPEC D2's port-indexed fan-in a cross-streamKey join completed
// nothing, so `d.arrivals` was 0 and `raw == d.arrivals` held as `0 == 0` —
// the filter could not drop anything because there was nothing to drop. It
// PINNED that zero so the control would fail the moment corpus 87 started
// emitting, and told whoever landed ENC-1292 to re-arm it.
//
// ENC-1292 (2026-09-20) RE-ARMS IT, AND THE PIN IS GONE. The control now
// drives corpus 87 BOTH WAYS from the one corpus entry, which is the only
// shape that can be non-vacuous in both directions:
//
//   * as authored (no `by`) -> 0 arrivals, which is D6's no-migration
//     guarantee stated as a measurement rather than a promise: a stored forum
//     graph carries no `by` and must keep meaning exactly what it meant;
//   * with `by:"none"` declared -> 12 arrivals, all of them raw AAPL/MSFT
//     prices, so `raw == arrivals` is a LIVE check over a non-empty set and
//     gate 3 below is demonstrably measuring the join's own members.
//
// Neither branch can hold over an empty set: the first asserts an exact zero
// against a driver the second proves is live, and the second asserts an exact
// twelve.
TEST(CorpusValueAssertions, Corpus87_ObservationPointIsSound_Control) {
  using namespace corpus_values;

  const rapidjson::Value* req = corpusRequest(87);
  ASSERT_NE(req, nullptr) << "corpus_id 87 not found in corpus_requests.json";

  constexpr double kAaplBase = 1000.0, kMsftBase = 5000.0;
  constexpr std::size_t kTicksPerSide = 6;

  auto drive = [](krn::Dispatcher& disp) {
    for (std::size_t n = 0; n < kTicksPerSide; ++n) {
      tick(disp, "AAPL", {{"lastPrice", kAaplBase + double(n)}});
      tick(disp, "MSFT", {{"lastPrice", kMsftBase + double(n)}});
    }
  };

  // ── A. AS AUTHORED. SPEC D6 — a stored graph carries no `by` and must keep
  // meaning exactly what it meant. For a cross-streamKey request that means
  // emitting NOTHING: the two sides never share a buffer key.
  {
    rapidjson::Document asAuthored = nodeWithoutPipeline(*req);
    Drive d = driveCorpus(asAuthored, /*threads=*/1, drive);
    EXPECT_EQ(d.arrivals, 0u)
        << "corpus 87 emitted " << d.arrivals << " value(s) WITHOUT a declared "
           "`by`." << d.renderSymbols() << "\n"
           "    The default is `by:\"streamKey\"` (SPEC D1) and under it two "
           "streamKeys never complete a\n"
           "    tuple. If this moved, every stored forum graph changed meaning "
           "with no migration available\n"
           "    (SPEC D6) — which is a far larger event than whatever made this "
           "test red.";
    EXPECT_EQ(d.nonNumeric, 0u);
  }

  // ── B. WITH THE JOIN KEY DECLARED. This is the branch that makes the
  // observation-point claim mean anything: the filter must drop nothing out of
  // TWELVE, not out of zero.
  rapidjson::Document join = nodeWithJoinByNone(*req);
  Drive d = driveCorpus(join, /*threads=*/1, drive);

  ASSERT_EQ(d.arrivals, 2 * kTicksPerSide)
      << "corpus 87 under by:\"none\" emitted " << d.arrivals << " value(s), "
         "expected " << (2 * kTicksPerSide) << " (" << kTicksPerSide
      << " two-member tuples)." << d.renderSymbols() << "\n"
         "    Every check below — and gate 3's — is vacuous over an empty "
         "tuple list, so this comes first.";

  std::size_t raw = 0;
  for (const auto& [tid, vals] : d.byThread) {
    (void)tid;
    for (double v : vals) if (v >= 900.0) ++raw;
  }

  EXPECT_EQ(d.nonNumeric, 0u) << "terminal received a non-numeric value";

  EXPECT_EQ(raw, d.arrivals)
      << "CONTROL FAILED. " << (d.arrivals - raw) << " of " << d.arrivals
      << " arrivals are not raw AAPL/MSFT prices.\n"
         "    Corpus 87's `node` is driven WITHOUT its pipeline (see the "
         "ENC-1290 note above the driver),\n"
         "    so every value reaching the terminal is a join member and the "
         "`>= 900.0` filter in gate 3\n"
         "    must drop nothing. If computed values are arriving, gate 3 is "
         "measuring the wrong stream\n"
         "    and any verdict it reports is worthless.";
}

// ═══ 3. Defect 3 — the correlation key is `symbol`, so no cross-symbol join ═
//
// DETERMINISTIC, at threads=1. GREEN as of ENC-1292; red for everything before
// it, and for two different reasons in succession.
//
// Corpus 87 "Price difference between AAPL and MSFT" declares a two-input join
// across two streamKeys. Drive six ticks on each side. Every completed tuple
// must contain one value from each side — that is what "join" means.
//
// THE THREE STATES THIS TEST HAS BEEN IN, because the middle one is the
// failure class this project exists to remove and it was reached by a FIX:
//
//   1. Before ENC-1291: `buf_[sv.symbol]` gave AAPL and MSFT one independent
//      buffer each and each fired every second value, so the terminal received
//      six tuples of {AAPL, AAPL} and {MSFT, MSFT} — "a price difference
//      between AAPL and MSFT" computed without ever looking at both. RED,
//      honestly.
//   2. After ENC-1291 (port-indexed fan-in, SPEC D2): each declared input got
//      its own port, AAPL filled port 0 of one buffer and MSFT port 1 of
//      another, NEITHER completed, and the terminal saw ZERO arrivals. Every
//      assertion here then held over an empty tuple list and the test would
//      have gone GREEN HAVING CHECKED NOTHING. ENC-1289 predicted this in
//      writing ("can pass vacuously under D1's locked default — honest but
//      weak"); ENC-1291 measured it, added the anti-vacuity ASSERT below, and
//      deliberately left the xfail marker in place rather than bank the
//      vacuous green.
//   3. ENC-1292 (declared `by`, SPEC D1): the request is driven with
//      `by:"none"` declared and the join emits SIX MIXED TUPLES. The
//      anti-vacuity ASSERT, the `sameSide == 0` EXPECT and everything else
//      below are now checking a non-empty set, and the marker is gone.
//
// WHAT "STRENGTHENED" MEANS HERE, concretely. `sameSide == 0` over six tuples
// is necessary and nowhere near sufficient: a join emitting six tuples of
// {AAPL(n), MSFT(n+3)} would satisfy it. So this test now pins the EXACT
// tuples — count, membership, declared port order, and the exact arithmetic
// truth of the natural-language request ("AAPL minus MSFT" is -4000.00 on
// every tick, by construction of the encoding) — plus the output identity the
// join emits under, which is SPEC section 5 Q6 and had no gate at all.
//
// THE `by` IS INJECTED INTO A COPY; `tests/treebuilder/corpus_requests.json`
// IS NOT TOUCHED. See `nodeWithJoinByNone` above the driver for why that
// boundary matters: the unmodified entry is the baseline the DEFAULT is
// measured against, one test up, and deleting it would make D6's no-migration
// guarantee unmeasurable.
//
// The ASSERT is deliberately on `d.arrivals` rather than on `joined.size()`:
// `joined` is what the `>= 900.0` filter left, so asserting on it would confuse
// "the join emitted nothing" with "the filter dropped everything", which are
// different defects with different owners.
//
// Corpus 87 also carries `pipeline:[Worker{fn:"diff"}]`. SPEC section 1.1
// defect 1 wired that as a SECOND live chain into the same terminal, and the
// `>= 900.0` filter below existed to drop its values. ENC-1290/D5 composed the
// chains, so this test drives corpus 87's `node` alone (see the ENC-1290 note
// above the driver) and there is no second chain left to filter — which the
// assertion next to the filter checks rather than assumes.
TEST(CorpusValueAssertions, Corpus87_CrossSymbolJoinNeverPairsOneSideWithItself) {
  using namespace corpus_values;

  const rapidjson::Value* req = corpusRequest(87);
  ASSERT_NE(req, nullptr) << "corpus_id 87 not found in corpus_requests.json";
  // ENC-1290 strips the pipeline; ENC-1292 declares the join key. Both act on
  // a COPY of the corpus entry.
  rapidjson::Document join = nodeWithJoinByNone(*req);

  constexpr double kAaplBase = 1000.0;   // AAPL lastPrice(n) = 1000 + n
  constexpr double kMsftBase = 5000.0;   // MSFT lastPrice(n) = 5000 + n
  constexpr std::size_t kTicksPerSide = 6;
  // "AAPL minus MSFT" on tick n = (1000+n) - (5000+n) = -4000.00, for every n.
  // The tick index cancels, so a CROSS-TICK pairing is indistinguishable from
  // a correct one by this number alone — which is why the members are pinned
  // individually below and this is only the closing statement.
  constexpr double kTruth = kAaplBase - kMsftBase;

  Drive d = driveCorpus(join, /*threads=*/1, [](krn::Dispatcher& disp) {
    for (std::size_t n = 0; n < kTicksPerSide; ++n) {
      tick(disp, "AAPL", {{"lastPrice", kAaplBase + double(n)}});
      tick(disp, "MSFT", {{"lastPrice", kMsftBase + double(n)}});
    }
  });

  // ── ANTI-VACUITY (ENC-1291, kept and tightened by ENC-1292) ───────────────
  // Everything below this line is a statement about the tuples the join
  // emitted. Between ENC-1291 and ENC-1292 it emitted NONE, and every one of
  // those statements was then true over an empty set. A gate that cannot
  // distinguish "the join is correct" from "the join is silent" is not a gate.
  // ENC-1291 asserted `> 0`; the count is now EXACT, so a join that emits some
  // but not all of its tuples can no longer satisfy it either.
  ASSERT_EQ(d.arrivals, 2 * kTicksPerSide)
      << "corpus_id 87 \"Price difference between AAPL and MSFT\" emitted "
      << d.arrivals << " value(s) from " << (2 * kTicksPerSide)
      << " ticks; expected " << (2 * kTicksPerSide) << " — "
      << kTicksPerSide << " complete two-member tuples." << d.renderSymbols()
      << "\n"
         "    ZERO means the cross-streamKey join is not happening: the "
         "correlation key is back to\n"
         "    `sv.symbol` (SPEC section 1.1 defect 3), or the `by:\"none\"` "
         "this test declares is no longer\n"
         "    reaching the node. Every assertion below would hold vacuously "
         "over an empty tuple list, so\n"
         "    this test would report GREEN having checked nothing — which is "
         "exactly the state ENC-1291\n"
         "    measured and refused to bank.\n"
         "    A count BETWEEN the two means the barrier is completing "
         "partially, which is a different\n"
         "    defect and is not this test's to diagnose.";

  // Keep only raw prices. This filter was written to drop the SECOND chain's
  // small diffs; since ENC-1290 composed the chains and this test drives the
  // `node` alone, there is no second chain and nothing to drop. The filter
  // stays — and the assertion below it — because a filter that silently starts
  // dropping everything is exactly how this test passes VACUOUSLY.
  std::vector<double> joined;
  for (const auto& [tid, vals] : d.byThread) {
    (void)tid;
    for (double v : vals) if (v >= 900.0) joined.push_back(v);
  }
  ASSERT_EQ(joined.size(), d.arrivals)
      << "the >= 900.0 filter dropped " << (d.arrivals - joined.size())
      << " of " << d.arrivals << " arrivals. Driving corpus 87's `node` alone, "
         "every arrival is a raw AAPL/MSFT price and NOTHING should be "
         "filtered. If the terminal is seeing computed values again, this test "
         "is measuring the wrong thing and any verdict below is worthless — "
         "see the ENC-1290 note at the top of this block.";

  auto sym = [&](double v) { return v >= kMsftBase ? 'M' : 'A'; };

  ASSERT_EQ(joined.size() % 2, 0u)
      << "odd number of join arrivals (" << joined.size() << ") — recovery unsound";

  // ── THE SIX TUPLES, PINNED ────────────────────────────────────────────────
  // ENC-1289 finding #2 asked ENC-1292 to "assert 6 mixed tuples under
  // by:\"none\", not 0". `sameSide == 0` is the weakest reading of that and is
  // kept below as the headline; these are the statements that make it mean
  // something.
  std::size_t sameSide = 0;
  std::ostringstream examples;
  for (std::size_t i = 0; i + 1 < joined.size(); i += 2) {
    const std::size_t t = i / 2;
    if (sym(joined[i]) == sym(joined[i + 1])) {
      ++sameSide;
      if (sameSide <= 6)
        examples << "\n      {" << std::fixed << std::setprecision(2) << joined[i]
                 << ", " << joined[i + 1] << "}  = both " << sym(joined[i])
                 << "  -> \"AAPL minus MSFT\" = "
                 << std::showpos << (joined[i] - joined[i + 1]) << std::noshowpos;
      continue;
    }
    // Declared PORT ORDER, not arrival order: input 0 is the AAPL Listener.
    EXPECT_DOUBLE_EQ(joined[i], kAaplBase + double(t))
        << "tuple " << t << " member 0 must be AAPL's own tick-" << t
        << " price. Port order is declared, not observed (SPEC D2).";
    EXPECT_DOUBLE_EQ(joined[i + 1], kMsftBase + double(t))
        << "tuple " << t << " member 1 must be MSFT's own tick-" << t
        << " price — the value the engine could not reach at all before "
           "ENC-1292.";
    EXPECT_DOUBLE_EQ(joined[i] - joined[i + 1], kTruth)
        << "tuple " << t << " does not answer the request: \"price difference "
           "between AAPL and MSFT\" is " << kTruth << " on every tick.";
  }

  EXPECT_EQ(sameSide, 0u)
      << "corpus_id 87 \"Price difference between AAPL and MSFT\" declares a join "
         "across TWO\n"
         "    streamKeys. " << kTicksPerSide << " ticks were driven into each side. "
      << (joined.size() / 2) << " tuple(s) were emitted and\n"
         "    " << sameSide << " of them pair one symbol with ITSELF:" << examples.str()
      << "\n\n    Cause: the fan-in is keying its buffer on `sv.symbol` again, so "
         "AAPL and MSFT occupy\n"
         "    two independent buffers and each completes alone (SPEC section "
         "1.1 defect 3). Under the\n"
         "    declared `by:\"none\"` they share one buffer and complete each "
         "other.";

  // ── OUTPUT IDENTITY — SPEC section 5 Q6, which had no gate before ─────────
  // `ClientSession` serialises `sv.symbol` as the frame's `streamKey`. A
  // `by:"none"` join has no per-symbol identity, so without an explicit
  // substitution it would report AAPL or MSFT depending on which side happened
  // to arrive second — under a race. It must be the REQUEST's own top-level
  // `streamKey`.
  ASSERT_EQ(d.symbols.size(), 1u)
      << "the joined stream carries " << d.symbols.size() << " identities:"
      << d.renderSymbols() << "\n"
         "    A cross-symbol join is ONE logical stream. Two identities on one "
         "request key is precisely\n"
         "    the two-streams-interleaved shape SPEC section 1.1 defect 1 "
         "describes, arriving by a new route.";
  EXPECT_EQ(d.symbols.begin()->first, (*req)["streamKey"].GetString())
      << "got" << d.renderSymbols() << ", expected the request's own top-level "
         "'streamKey'.\n"
         "    `sv.symbol` at the point of emission is MSFT (the side that "
         "released the tuple) and would\n"
         "    be AAPL had the ticks interleaved the other way — an identity "
         "that depends on arrival order\n"
         "    is a race, not an identity (SPEC section 5 Q6).";

  EXPECT_EQ(d.nonNumeric, 0u) << "terminal received a non-numeric value";
}
