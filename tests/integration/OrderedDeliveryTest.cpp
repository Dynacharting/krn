// tests/integration/OrderedDeliveryTest.cpp
//
// ENC-1005 — SPEC specs/2026-09-20-krn-join-correctness D3, D4.
// "Delivery is serialized per request DAG."
//
// `tests/treebuilder/CorpusTest.cpp`'s `Corpus86_SpreadIsExactlyTwoCents` is
// the PRODUCT gate for this: it says the bid-ask spread of a real corpus entry
// comes out at 0.02 under concurrency. It is a value assertion, so it can only
// tell you that *something* about the join went wrong. These tests pin the
// MECHANISM underneath it, one property per test, so a future regression names
// itself:
//
//   1. `rt::Strand` really is a strand — FIFO, and never two tasks at once.
//   2. One DAG's delivery is in production order, end to end, at 8 threads.
//   3. The two sides of one tick reach the join in the order the Dispatcher
//      produced them — the half that the strand alone does NOT buy, because
//      that ordering is decided one hop UPSTREAM of it.
//   4. The strand is per REQUEST, not per process: four DAGs still occupy four
//      cores. This is the structural claim SPEC §5 Q2's ruling rests on, and
//      it is the difference between "serialize a DAG" and "serialize the
//      engine".
//
// Every one of these was mutation-tested: the defect each names was introduced
// into the engine and each test was confirmed to go red, and the others to stay
// green. The mutations and their results are in the ENC-1005 ticket.
//
// NOTHING IS STUBBED. Each test builds through the real `tree::buildForRequest`
// from a real request object and drives values through the real
// `Dispatcher::onTick`. The only substitution is the terminal, the parameter
// `buildForRequest` already takes.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <rapidjson/document.h>

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"

using namespace krn;

namespace {

// ─── A terminal that records GLOBAL arrival order ──────────────────────────
//
// Deliberately NOT the per-thread bucketing `CorpusTest`'s RecordingTerminal
// does. That exists because without ordered delivery the arrival sequence is
// only recoverable per thread. The whole claim under test here is that ONE
// global sequence exists, so recording one is the assertion's precondition:
// if delivery were still concurrent this vector would interleave and every
// order assertion below would fail — which is exactly what it should do.
class SequenceTerminal final : public INode {
public:
  // `spinNs` makes delivery cost something. It matters for exactly one test:
  // a terminal that costs nothing always finishes before the ingress thread
  // produces the next tick, so two DAGs on two different strands never
  // actually overlap and a test that tries to detect the difference cannot.
  // Default 0 — the order tests want ingress to be the slow side.
  explicit SequenceTerminal(long long spinNs = 0) : spinNs_(spinNs) {}

  void onValue(const StreamValue& sv) override {
    if (spinNs_ > 0) {
      const auto until =
          std::chrono::steady_clock::now() + std::chrono::nanoseconds(spinNs_);
      volatile double sink = 0.0;
      while (std::chrono::steady_clock::now() < until)
        for (int i = 0; i < 64; ++i) sink = sink * 1.000000001 + 1.0;
      (void)sink;
    }
    // Track the maximum number of onValue calls in flight at once. For a
    // strand-serialised DAG this must never exceed 1.
    const int now = ++inFlight_;
    int prev = maxInFlight_.load(std::memory_order_relaxed);
    while (now > prev &&
           !maxInFlight_.compare_exchange_weak(prev, now,
                                               std::memory_order_relaxed)) {}
    if (const double* d = std::get_if<double>(&sv.value)) {
      std::lock_guard<std::mutex> lk(mx_);
      seq_.push_back(*d);
    } else {
      ++nonNumeric_;
    }
    --inFlight_;
  }
  void shutdown() noexcept override {}

  std::vector<double> sequence() const {
    std::lock_guard<std::mutex> lk(mx_);
    return seq_;
  }
  int  maxInFlight() const { return maxInFlight_.load(); }
  std::size_t nonNumeric() const { return nonNumeric_.load(); }

private:
  long long           spinNs_{0};
  mutable std::mutex  mx_;
  std::vector<double> seq_;
  std::atomic<int>    inFlight_{0};
  std::atomic<int>    maxInFlight_{0};
  std::atomic<std::size_t> nonNumeric_{0};
};

rapidjson::Document parse(const char* json) {
  rapidjson::Document d;
  d.Parse(json);
  EXPECT_FALSE(d.HasParseError()) << "bad test JSON: " << json;
  return d;
}

void tick(Dispatcher& d, const char* symbol,
          std::initializer_list<std::pair<const char*, double>> fields) {
  auto payload = std::make_shared<rapidjson::Document>();
  payload->SetObject();
  auto& al = payload->GetAllocator();
  for (const auto& [name, value] : fields)
    payload->AddMember(rapidjson::StringRef(name), value, al);
  Event ev;
  ev.symbol  = symbol;
  ev.payload = payload;            // ev.type defaults to "tick", the production type
  d.onTick(ev);
}

} // namespace

// ═══ 1. `rt::Strand` is a strand ═══════════════════════════════════════════
//
// The primitive on its own, with no engine around it: tasks posted to one
// strand run one at a time and in post order, while running on a shared pool's
// worker threads. Posted from four threads at once so the "one at a time" claim
// is exercised against real contention rather than against a single producer.
//
// Post order is only *defined* per producer thread, so each producer stamps its
// own monotonically increasing counter and the test asserts each producer's
// subsequence came out ascending. The mutual-exclusion claim is global and is
// asserted globally.
TEST(OrderedDelivery, StrandRunsOneTaskAtATimeInPostOrder) {
  constexpr int kProducers = 4;
  constexpr int kPerProducer = 2000;

  rt::ThreadPool pool(8);
  auto strand = std::make_shared<rt::Strand>(&pool);

  std::atomic<int> inFlight{0};
  std::atomic<int> maxInFlight{0};
  std::mutex mx;
  std::vector<std::pair<int, int>> ran;   // (producer, its counter)
  ran.reserve(kProducers * kPerProducer);

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kPerProducer; ++i) {
        strand->post([&, p, i] {
          const int now = ++inFlight;
          int prev = maxInFlight.load(std::memory_order_relaxed);
          while (now > prev &&
                 !maxInFlight.compare_exchange_weak(prev, now,
                                                    std::memory_order_relaxed)) {}
          {
            std::lock_guard<std::mutex> lk(mx);
            ran.emplace_back(p, i);
          }
          --inFlight;
        });
      }
    });
  }
  for (auto& t : producers) t.join();
  pool.drain();

  std::vector<std::pair<int, int>> got;
  { std::lock_guard<std::mutex> lk(mx); got = ran; }

  ASSERT_EQ(got.size(), std::size_t(kProducers * kPerProducer))
      << "the strand dropped or duplicated work";
  EXPECT_EQ(maxInFlight.load(), 1)
      << "rt::Strand ran " << maxInFlight.load() << " tasks CONCURRENTLY. A "
         "strand that does that is a queue, not a strand, and every ordering "
         "guarantee above it is void.";

  // Each producer's own posts must come out in the order that producer made
  // them.
  std::vector<int> nextExpected(kProducers, 0);
  for (const auto& [p, i] : got) {
    ASSERT_EQ(i, nextExpected[p])
        << "producer " << p << "'s task " << i << " ran out of post order";
    ++nextExpected[p];
  }
}

// ═══ 2. One DAG delivers in production order, at 8 threads ═════════════════
//
// The simplest possible request — a bare `Listener` straight to the terminal,
// no node, no pipeline — driven with 5,000 strictly increasing values from one
// ingress thread. Every value must arrive, exactly once, in that order.
//
// This is D3 reduced to its smallest true statement, and it needs no join to
// state it. Before ENC-1005 `Listener::onValue` posted each value to the shared
// pool as an independent task, so at 8 workers this sequence came out shuffled.
//
// NOTE the `Deps` here sets NO strand. That is deliberate: it is the assertion
// that `buildForRequest` mints one for a caller who did not, so ordered
// delivery is a property of the DAG and not of the caller remembering.
TEST(OrderedDelivery, OneDagDeliversEveryValueOnceInProductionOrder) {
  constexpr std::size_t kTicks = 5000;

  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(8);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);
  tree::Deps deps;
  deps.store      = &store;
  deps.pool       = pool.get();
  deps.dispatcher = &dispatcher;
  // deps.strand deliberately left null — see the note above.

  auto terminal = std::make_shared<SequenceTerminal>();
  auto req = parse(R"({"streamKey":"AAPL","field":"px"})");
  auto chain = tree::buildForRequest(req, deps, terminal);

  for (std::size_t n = 0; n < kTicks; ++n)
    tick(dispatcher, "AAPL", {{"px", double(n)}});
  pool->drain();

  const auto seq = terminal->sequence();

  EXPECT_EQ(terminal->maxInFlight(), 1)
      << "two values of ONE request DAG were in the terminal concurrently";
  ASSERT_EQ(seq.size(), kTicks)
      << "expected every tick to arrive exactly once";
  for (std::size_t n = 0; n < kTicks; ++n) {
    ASSERT_DOUBLE_EQ(seq[n], double(n))
        << "value " << n << " of a single request DAG arrived out of order "
           "(got " << seq[n] << "). Delivery is not serialized — see "
           "tree::Deps::strand and Dispatcher::deliver.";
  }

  for (auto& nptr : chain.keepAlive) if (nptr) nptr->shutdown();
  if (chain.head) chain.head->shutdown();
  pool->shutdown();
  gThreadPool = prevPool;
}

// ═══ 3. The two sides of ONE tick keep the Dispatcher's order ══════════════
//
// THIS IS THE HALF THE STRAND CANNOT BUY ON ITS OWN, and it is why ENC-1005
// touches `Dispatcher` at all.
//
// `Dispatcher::onTick` walks its listener map for the symbol. The inner
// container is a `std::map<field, …>`, so for a tick carrying both `ask` and
// `bid` it produces `ask` first, deterministically. Posting each notification
// as an independent pool task hands that order straight back to the scheduler:
// at four workers both tasks run at once, each calls `Listener::onValue`, and
// whichever wins reaches the strand first. The strand then faithfully preserves
// an order that was already lost one hop upstream.
//
// So a strand-bearing Listener is called INLINE (`Dispatcher::deliver` /
// `INode::deliversOnOwnExecutor`), and the assertion below is the gate on that.
//
// The shape is corpus 86's, written out here rather than read from the corpus
// so this test states its own premise: `Aggregate(2)` over `ask` and `bid` of
// one symbol forwards a completed batch member by member
// (`Aggregate::onPortValue` forwards a completed tuple member by member), so
// the terminal must see exactly
//     ask(0), bid(0), ask(1), bid(1), …
// with no pair swapped and no pair straddling two ticks.
TEST(OrderedDelivery, BothSidesOfOneTickReachTheJoinInDispatcherOrder) {
  constexpr std::size_t kTicks = 2000;
  constexpr double kBase = 1000.0;
  constexpr double kSpread = 0.02;

  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(4);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);
  tree::Deps deps;
  deps.store      = &store;
  deps.pool       = pool.get();
  deps.dispatcher = &dispatcher;

  auto terminal = std::make_shared<SequenceTerminal>();
  auto req = parse(R"({
    "streamKey":"AAPL","field":"lastPrice",
    "node":{"type":"Aggregate","arity":2,"inputs":[
      {"type":"Listener","streamKey":"AAPL","field":"ask"},
      {"type":"Listener","streamKey":"AAPL","field":"bid"}]}
  })");
  auto chain = tree::buildForRequest(req, deps, terminal);

  for (std::size_t n = 0; n < kTicks; ++n)
    tick(dispatcher, "AAPL", {{"bid", kBase + double(n)},
                              {"ask", kBase + double(n) + kSpread}});
  pool->drain();

  const auto seq = terminal->sequence();

  EXPECT_EQ(terminal->maxInFlight(), 1);
  ASSERT_EQ(seq.size(), kTicks * 2)
      << "the join emitted " << seq.size() << " values for " << kTicks
      << " two-input ticks; expected " << (kTicks * 2);
  for (std::size_t n = 0; n < kTicks; ++n) {
    ASSERT_DOUBLE_EQ(seq[2 * n], kBase + double(n) + kSpread)
        << "position " << (2 * n) << " should be ask(" << n << "). The `ask`/"
           "`bid` of one tick reached the join in the wrong order, or a pair "
           "straddles two ticks.";
    ASSERT_DOUBLE_EQ(seq[2 * n + 1], kBase + double(n))
        << "position " << (2 * n + 1) << " should be bid(" << n << ").";
  }

  for (auto& nptr : chain.keepAlive) if (nptr) nptr->shutdown();
  if (chain.head) chain.head->shutdown();
  pool->shutdown();
  gThreadPool = prevPool;
}

// ═══ 4. A strand is per REQUEST — four DAGs still occupy four cores ════════
//
// SPEC §5 Q2 rules the single-subscription regression acceptable, and the
// reason it gives is structural: *"A strand is not a thread and does not reduce
// the pool; N subscriptions still occupy up to N cores."* That is an assertion
// about this code, so it is asserted here.
//
// Four independent requests are built — four `buildForRequest` calls, hence
// four strands — and each one's terminal parks until all four have arrived.
// If the four DAGs share one ordering (one process-wide strand, or a strand
// minted per session rather than per subscription) the first terminal parks
// forever and the others never arrive, so this test hangs to its own timeout
// and fails. If they are genuinely independent it completes immediately.
//
// A DEADLINE, NOT A SLEEP. There is no "wait 200ms and hope": each terminal
// spins on the shared counter until it reaches four or the deadline passes, so
// the pass is proof of real overlap and the failure is bounded.
//
// EVERY terminal must see all four, not just one of them. The first draft of
// this test asserted a single shared flag, and it SURVIVED ITS OWN MUTATION:
// with all four DAGs forced onto one strand the first terminal parked to the
// deadline and the other three then ran one after another, and the FOURTH
// still observed a count of four — trivially, because it was itself the
// fourth — and set the flag. It reported "they overlapped" about a run that was
// perfectly serial. Counting the terminals that succeeded fixes it: under one
// shared strand the first one times out, the count is 3 of 4, and the test goes
// red. Verified against that exact mutation.
TEST(OrderedDelivery, DistinctRequestsAreNotSerializedAgainstEachOther) {
  constexpr int kDags = 4;

  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(kDags * 2);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);

  std::atomic<int> arrived{0};
  std::atomic<int> sawAllFour{0};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

  class ParkingTerminal final : public INode {
  public:
    ParkingTerminal(std::atomic<int>& arrived, std::atomic<int>& sawAll,
                    std::chrono::steady_clock::time_point deadline, int target)
      : arrived_(arrived), sawAll_(sawAll), deadline_(deadline), target_(target) {}
    void onValue(const StreamValue&) override {
      if (done_.exchange(true)) return;       // one park per DAG is enough
      ++arrived_;
      while (arrived_.load() < target_ &&
             std::chrono::steady_clock::now() < deadline_) {
        std::this_thread::yield();
      }
      // Counted per terminal, and only before the deadline: a terminal that
      // ran AFTER the others had given up would also see the full count, which
      // is exactly how the first draft of this test lied.
      if (arrived_.load() >= target_ &&
          std::chrono::steady_clock::now() < deadline_) {
        ++sawAll_;
      }
    }
    void shutdown() noexcept override {}
  private:
    std::atomic<int>&  arrived_;
    std::atomic<int>&  sawAll_;
    std::chrono::steady_clock::time_point deadline_;
    int                target_;
    std::atomic<bool>  done_{false};
  };

  std::vector<tree::BuiltChain> chains;
  for (int i = 0; i < kDags; ++i) {
    tree::Deps deps;                       // a fresh Deps per request: one
    deps.store      = &store;              // strand each, which is exactly what
    deps.pool       = pool.get();          // handleSubscribe does per
    deps.dispatcher = &dispatcher;         // subscription.
    auto terminal = std::make_shared<ParkingTerminal>(arrived, sawAllFour,
                                                      deadline, kDags);
    const std::string json =
        std::string(R"({"streamKey":"SYM)") + char('A' + i) +
        R"(","field":"px"})";
    auto req = parse(json.c_str());
    chains.push_back(tree::buildForRequest(req, deps, terminal));
  }

  for (int i = 0; i < kDags; ++i) {
    const std::string sym = std::string("SYM") + char('A' + i);
    tick(dispatcher, sym.c_str(), {{"px", 1.0}});
  }
  pool->drain();

  EXPECT_EQ(sawAllFour.load(), kDags)
      << "only " << sawAllFour.load() << " of " << kDags << " request DAGs "
         "observed all " << kDags << " running at once (" << arrived.load()
      << " arrived in total). The strand is supposed to serialize ONE DAG, not "
         "the engine: SPEC D3/D4 mint one per subscription, and SPEC §5 Q2's "
         "ruling on the single-subscription regression rests on N "
         "subscriptions still occupying N cores.";

  for (auto& c : chains) {
    for (auto& nptr : c.keepAlive) if (nptr) nptr->shutdown();
    if (c.head) c.head->shutdown();
  }
  pool->shutdown();
  gThreadPool = prevPool;
}

// ═══ 5. A CALLER-SUPPLIED strand is used, not replaced ═════════════════════
//
// This is the property `ClientSession::handleSubscribe` depends on, and the
// reason it is worth pinning is that the line there is otherwise invisible:
// deleting `deps.strand = std::make_shared<rt::Strand>(...)` from
// `handleSubscribe` reddens NOTHING, because `buildForRequest`'s backstop mints
// an equivalent strand one call deeper. (Measured — mutation M7 on the ticket.)
// What is NOT equivalent, and what this test gates, is `buildForRequest`
// *honouring* what it was handed: if it ever overwrote `deps.strand`, the
// production site's granularity choice would be silently discarded and nothing
// would say so.
//
// Two DAGs are built against ONE caller-supplied strand and share one terminal.
// If the strand is honoured they are one ordering: the terminal sees the exact
// interleaving the ingress thread produced, and never two values at once. If it
// is replaced they are two orderings and both assertions fail.
//
// THE TERMINAL SPINS, AND THAT IS LOAD-BEARING. The first draft of this test
// used the default free terminal and SURVIVED ITS OWN MUTATION: `onTick` costs
// far more than a `push_back`, so each delivery finished before the ingress
// thread produced the next tick and two separate strands never once overlapped
// — the test could not tell one strand from two. 20 us per value makes
// delivery the slow side, which is the only regime in which the question has
// an observable answer. Verified red against that mutation.
//
// Note this is the INVERSE of test 4 and they are both true at once — that is
// the whole point of the granularity being a parameter rather than a policy.
TEST(OrderedDelivery, ACallerSuppliedStrandIsUsedNotReplaced) {
  constexpr std::size_t kTicks = 1000;

  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(8);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);
  auto shared = std::make_shared<rt::Strand>(pool.get());

  auto terminal = std::make_shared<SequenceTerminal>(/*spinNs=*/20000);
  std::vector<tree::BuiltChain> chains;
  for (const char* sym : {"AAA", "BBB"}) {
    tree::Deps deps;
    deps.store      = &store;
    deps.pool       = pool.get();
    deps.dispatcher = &dispatcher;
    deps.strand     = shared;              // ONE strand for BOTH requests
    const std::string json =
        std::string(R"({"streamKey":")") + sym + R"(","field":"px"})";
    auto req = parse(json.c_str());
    chains.push_back(tree::buildForRequest(req, deps, terminal));
  }

  for (std::size_t n = 0; n < kTicks; ++n) {
    tick(dispatcher, "AAA", {{"px", double(2 * n)}});
    tick(dispatcher, "BBB", {{"px", double(2 * n + 1)}});
  }
  pool->drain();

  const auto seq = terminal->sequence();
  EXPECT_EQ(terminal->maxInFlight(), 1)
      << "two DAGs sharing ONE strand ran concurrently — buildForRequest did "
         "not use the strand it was handed";
  ASSERT_EQ(seq.size(), kTicks * 2);
  for (std::size_t n = 0; n < kTicks * 2; ++n) {
    ASSERT_DOUBLE_EQ(seq[n], double(n))
        << "position " << n << ": two DAGs sharing one strand must deliver in "
           "one combined production order";
  }

  for (auto& c : chains) {
    for (auto& nptr : c.keepAlive) if (nptr) nptr->shutdown();
    if (c.head) c.head->shutdown();
  }
  pool->shutdown();
  gThreadPool = prevPool;
}

// ═══ 6. A stopping pool must not wedge a strand ════════════════════════════
//
// Found by the ENC-1005 adversarial review, and it is the failure mode a strand
// is worst at having: silent and permanent.
//
// `ThreadPool::post` drops a task when the pool is stopping. `Strand::post`
// sets its single-drainer token BEFORE it kicks the pool, and only `drain()`
// clears it — so a dropped kick left the token set forever. Every later post
// then took the "a drainer is already live" early return and appended to a
// queue no drainer would ever visit. Measured before the fix: six tasks queued,
// one ran, zero progress, and the queued closures pinned the whole DAG alive.
//
// `ThreadPool::post` now reports the drop and `Strand::post` drains inline when
// the pool refuses. The assertion is that the work RUNS — in order — rather
// than that it is merely not lost.
TEST(OrderedDelivery, StrandDeliversEvenWhenThePoolIsAlreadyStopping) {
  rt::ThreadPool pool(2);
  pool.shutdown();                       // pool is now permanently stopping

  auto strand = std::make_shared<rt::Strand>(&pool);

  std::vector<int> ran;
  for (int i = 0; i < 6; ++i)
    strand->post([&ran, i] { ran.push_back(i); });

  EXPECT_EQ(strand->queueDepth(), 0u)
      << "the strand is wedged: " << strand->queueDepth() << " task(s) queued "
         "with no drainer that will ever come. `running_` was left set by a "
         "kick the stopping pool dropped.";
  ASSERT_EQ(ran.size(), 6u) << "tasks were silently lost, not just delayed";
  for (int i = 0; i < 6; ++i)
    EXPECT_EQ(ran[std::size_t(i)], i) << "inline draining must still be in order";
}
