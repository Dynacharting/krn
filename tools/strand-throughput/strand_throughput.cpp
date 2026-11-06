// tools/strand-throughput/strand_throughput.cpp
//
// ENC-1005 — the instrument SPEC specs/2026-09-20-krn-join-correctness §5 Q2
// obliges this ticket to commit.
//
// WHY IT EXISTS. Q2 is ruled: the single-subscription heavy-compute regression
// from serializing one request DAG is acceptable, and the ruling is structural,
// so it does not depend on the magnitude. But the two magnitudes the SPEC and
// ENC-1005's description used to carry — "422,946 -> 117,269 values/sec" and
// "faster in 3 of 4 profiles (up to 3.3x at 8 threads)" — came from an
// instrument that was never committed and exist in no file in this workspace.
// They are struck (SPEC Corrections C2.4). Q2's ruling therefore put ENC-1005
// under an obligation: commit an instrument, and report before/after at 1, 2,
// 4, 8 and 16 threads for BOTH a single subscription and a multi-subscription
// fan-out, with the load average per rep.
//
// This is that instrument. Nothing here is a claim; it is a program that
// prints numbers you can reproduce.
//
// ───────────────────────────────────────────────────────────────────────────
// HOW "BEFORE" AND "AFTER" ARE BOTH MEASURED FROM ONE BINARY
//
// They are two live code paths at this commit, not two checkouts:
//
//   AFTER  (ordered, ENC-1005)   `deps.strand` set. `Listener::onValue` posts
//                                to the DAG's `rt::Strand`, and `Dispatcher`
//                                delivers to that Listener inline because it
//                                answers `deliversOnOwnExecutor()`.
//   BEFORE (unordered, legacy)   `deps.strand` left null. `Listener::onValue`
//                                posts straight to `rt::ThreadPool`, and
//                                `Dispatcher` posts its notification as an
//                                independent pool task — bit for bit the
//                                pre-ENC-1005 path, which is retained
//                                precisely so this comparison is honest.
//
// So both arms run the same binary, the same DAG, the same driver and the same
// machine minutes apart. The BEFORE arm is a real measurement of the old
// behaviour, not a reconstruction of it — but note what it is NOT: it is the
// old path *as it survives in this build*, so if a future change removes that
// path this instrument's BEFORE arm stops being meaningful and must go with it.
//
// ───────────────────────────────────────────────────────────────────────────
// WHAT IS BEING TIMED, STATED SO THE NUMBER CAN BE ARGUED WITH
//
// One ingress thread drives `ticksPerSub` ticks into each of `subs` request
// DAGs, round-robin. Each DAG is `Listener(SYMi, px) -> terminal`. The terminal
// spins for `spinNs` nanoseconds per value — that is the "per-value compute",
// and it is deliberately a spin rather than a sleep so it occupies a core the
// way real reduction work does. Timing starts before the first tick and stops
// after `ThreadPool::drain()` returns, so it covers ingress AND every queued
// value reaching its terminal. The rate is `arrivals / elapsed`.
//
// LIMITS, up front:
//   * ONE ingress thread. With `spinNs = 0` the ingress loop itself can be the
//     bottleneck, which flattens both arms; that is why the light profile is
//     reported but not leaned on.
//   * This measures DELIVERY, not the whole server. There is no socket, no
//     JSON, no `ClientSession` outbox.
//   * A spin is not a cache-realistic workload.
//
// ───────────────────────────────────────────────────────────────────────────
// USAGE
//   strand_throughput [--threads 1,2,4,8,16] [--subs 1] [--ticks 20000]
//                     [--spin-ns 2000] [--reps 3] [--label NAME]
//
// Prints one CSV row per (profile, threads, arm, rep) to stdout, with the
// 1-minute load average sampled at the START of every rep — other agents
// compile in this workspace continuously and a throughput number without a
// load figure beside it is not re-checkable.

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <rapidjson/document.h>

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/nodes/Listener.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"

using namespace krn;
using Clock = std::chrono::steady_clock;

namespace {

double loadAvg1() {
  std::ifstream f("/proc/loadavg");
  double v = -1.0;
  if (f) f >> v;
  return v;
}

// Occupy a core for approximately `ns` nanoseconds. A spin, not a sleep: a
// sleeping terminal would hand the core back and make a serialized DAG look
// free.
inline void spinFor(long long ns) {
  if (ns <= 0) return;
  const auto until = Clock::now() + std::chrono::nanoseconds(ns);
  volatile double sink = 0.0;
  while (Clock::now() < until) {
    for (int i = 0; i < 64; ++i) sink = sink * 1.000000001 + 1.0;
  }
  (void)sink;
}

class SpinTerminal final : public INode {
public:
  explicit SpinTerminal(long long spinNs) : spinNs_(spinNs) {}
  void onValue(const StreamValue&) override {
    spinFor(spinNs_);
    arrivals_.fetch_add(1, std::memory_order_relaxed);
  }
  void shutdown() noexcept override {}
  std::size_t arrivals() const { return arrivals_.load(); }
private:
  long long spinNs_;
  std::atomic<std::size_t> arrivals_{0};
};

struct RunResult {
  double   seconds{0};
  std::size_t arrivals{0};
};

RunResult runOne(unsigned threads, int subs, std::size_t ticksPerSub,
              long long spinNs, bool ordered) {
  AtomicStore store;
  auto pool = std::make_shared<rt::ThreadPool>(threads);
  auto prevPool = gThreadPool;
  gThreadPool = pool;

  Dispatcher dispatcher(pool.get(), &store);

  std::vector<tree::BuiltChain> chains;
  std::vector<std::shared_ptr<SpinTerminal>> terminals;
  std::vector<std::string> symbols;
  std::vector<rapidjson::Document> reqs;

  for (int i = 0; i < subs; ++i) {
    symbols.push_back("SYM" + std::to_string(i));
    rapidjson::Document d;
    d.SetObject();
    auto& a = d.GetAllocator();
    d.AddMember("streamKey", rapidjson::Value(symbols.back().c_str(), a), a);
    d.AddMember("field", rapidjson::Value("px", a), a);
    reqs.push_back(std::move(d));
  }

  for (int i = 0; i < subs; ++i) {
    tree::Deps deps;
    deps.store      = &store;
    deps.pool       = pool.get();
    deps.dispatcher = &dispatcher;
    if (ordered) {
      // AFTER: one strand per subscription, exactly what
      // ClientSession::handleSubscribe mints.
      deps.strand = std::make_shared<rt::Strand>(pool.get());
    }
    auto term = std::make_shared<SpinTerminal>(spinNs);
    terminals.push_back(term);
    if (ordered) {
      chains.push_back(tree::buildForRequest(reqs[i], deps, term));
    } else {
      // BEFORE: buildForRequest mints a strand when the caller supplies none,
      // which is the whole point of that backstop — so the legacy arm cannot
      // go through it. It builds the same two nodes by hand, through the same
      // real builder entry points, with no strand anywhere.
      auto head = krn::nodes::Listener::Create(symbols[i], "px", term,
                                   deps.pool, deps.dispatcher, /*strand=*/nullptr);
      if (!head) { std::fprintf(stderr, "listener build failed\n"); std::exit(2); }
      tree::BuiltChain bc;
      bc.head = head.value();
      bc.keepAlive.push_back(term);
      chains.push_back(std::move(bc));
    }
  }

  // Pre-build the tick payloads so JSON construction is not inside the timed
  // region.
  std::vector<std::shared_ptr<rapidjson::Document>> payloads;
  for (int i = 0; i < subs; ++i) {
    auto p = std::make_shared<rapidjson::Document>();
    p->SetObject();
    auto& a = p->GetAllocator();
    p->AddMember("px", 1.0 + i, a);
    payloads.push_back(std::move(p));
  }

  const auto t0 = Clock::now();
  for (std::size_t n = 0; n < ticksPerSub; ++n) {
    for (int i = 0; i < subs; ++i) {
      Event ev;
      ev.symbol  = symbols[i];
      ev.payload = payloads[i];
      dispatcher.onTick(ev);
    }
  }
  pool->drain();
  const auto t1 = Clock::now();

  RunResult r;
  r.seconds = std::chrono::duration<double>(t1 - t0).count();
  for (auto& t : terminals) r.arrivals += t->arrivals();

  for (auto& c : chains) {
    for (auto& n : c.keepAlive) if (n) n->shutdown();
    if (c.head) c.head->shutdown();
  }
  pool->shutdown();
  gThreadPool = prevPool;
  return r;
}

std::vector<unsigned> parseWidths(const std::string& s) {
  std::vector<unsigned> out;
  std::size_t i = 0;
  while (i < s.size()) {
    std::size_t j = s.find(',', i);
    if (j == std::string::npos) j = s.size();
    out.push_back(static_cast<unsigned>(std::stoul(s.substr(i, j - i))));
    i = j + 1;
  }
  return out;
}

} // namespace

int main(int argc, char** argv) {
  std::vector<unsigned> widths{1, 2, 4, 8, 16};
  int         subs   = 1;
  std::size_t ticks  = 20000;
  long long   spinNs = 2000;
  int         reps   = 3;
  std::string label  = "profile";

  for (int i = 1; i < argc; ++i) {
    auto next = [&](const char* what) -> const char* {
      if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", what); std::exit(2); }
      return argv[++i];
    };
    if      (!std::strcmp(argv[i], "--threads")) widths = parseWidths(next("--threads"));
    else if (!std::strcmp(argv[i], "--subs"))    subs   = std::atoi(next("--subs"));
    else if (!std::strcmp(argv[i], "--ticks"))   ticks  = std::strtoull(next("--ticks"), nullptr, 10);
    else if (!std::strcmp(argv[i], "--spin-ns")) spinNs = std::atoll(next("--spin-ns"));
    else if (!std::strcmp(argv[i], "--reps"))    reps   = std::atoi(next("--reps"));
    else if (!std::strcmp(argv[i], "--label"))   label  = next("--label");
    else { std::fprintf(stderr, "unknown arg %s\n", argv[i]); return 2; }
  }

  std::printf("profile,subs,ticks_per_sub,spin_ns,threads,arm,rep,"
              "arrivals,seconds,values_per_sec,load1\n");
  for (unsigned w : widths) {
    for (int rep = 0; rep < reps; ++rep) {
      for (int arm = 0; arm < 2; ++arm) {
        const bool ordered = (arm == 1);
        const double load = loadAvg1();
        const RunResult r = runOne(w, subs, ticks, spinNs, ordered);
        std::printf("%s,%d,%zu,%lld,%u,%s,%d,%zu,%.4f,%.0f,%.2f\n",
                    label.c_str(), subs, ticks, spinNs, w,
                    ordered ? "after_strand" : "before_pool",
                    rep, r.arrivals, r.seconds,
                    r.seconds > 0 ? double(r.arrivals) / r.seconds : 0.0,
                    load);
        std::fflush(stdout);
      }
    }
  }
  return 0;
}
