// Tests for krn::BucketTime — wall-clock-aligned periodic tick source.
//
// The alignment guarantee is the load-bearing property: every consumer
// of a BucketTime(60000) ticks at the same wall-clock minute boundary,
// so a "1m bar" means the same window across the whole data plane.
// These tests use shorter periods (10–100 ms) to keep runtime tight
// while still exercising the alignment math.

#include "krn/nodes/BucketTime.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/nodes/INode.hpp"
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace krn;
using namespace std::chrono;
using namespace std::chrono_literals;

namespace {

// Captures (count, timestamps) per onValue. Timestamps let us verify
// alignment to wall-clock period boundaries.
class TimestampingStub : public INode {
public:
    std::atomic<int> count{0};
    std::vector<system_clock::time_point> ticks;
    std::mutex mx;
    void onValue(const StreamValue&) override {
        std::lock_guard<std::mutex> lk(mx);
        ++count;
        ticks.push_back(system_clock::now());
    }
    void shutdown() noexcept override {}
};

} // namespace

TEST(BucketTimeTest, EmitsAtLeastOnceWithinTwoPeriods) {
    rt::ThreadPool pool(1);
    auto stub = std::make_shared<TimestampingStub>();
    auto bt = std::make_shared<BucketTime>(20ms, stub, &pool);
    bt->start();
    std::this_thread::sleep_for(50ms);
    int cnt = stub->count.load();
    EXPECT_GE(cnt, 1) << "Expected at least 1 tick within 2 periods, got " << cnt;
    bt->shutdown();
    pool.shutdown();
}

// ENC-1340: this test used to flake red under CPU contention, and the reason
// was a race in the TEST, not in BucketTime.
//
// The timer thread does not call the child directly — it POSTS to the
// ThreadPool. `shutdown()` joins the timer thread, so no further post is
// possible once it returns, but a tick the timer had ALREADY posted is at that
// moment sitting in the pool queue, not yet executed. Sampling `before`
// straight after `shutdown()` therefore misses it; the pool's single worker
// runs it during the 45 ms observation window; `after` is one greater and the
// test goes red with the shutdown having worked perfectly.
//
// It is load-sensitive because the only thing deciding the outcome is whether
// the pool worker gets scheduled before the main thread resumes. Measured on a
// 16-core box with ONE core oversubscribed 9x (loadavg 3.7-5.4 machine-wide):
// 8 reds in 200 runs (4.0%), every one of them `after=N+1, before=N`. Two
// independent observers reported the same shape at loadavg 35-47 (ENC-1338 saw
// 1 in 5 shuffled iterations; ENC-1291's refuter lost a CONTROL run to it,
// which is what makes this worth fixing rather than retrying — a mutation
// sweep is only as trustworthy as its control).
//
// The fix is `pool.drain()`, not a longer sleep: it waits for the queue to
// empty AND for in-flight tasks to finish, so `before` is taken once every
// already-posted tick has landed. That is deterministic — there is no timing
// assumption left to lose. The leading sleep is likewise replaced by a bounded
// poll for the first tick, so a slow box delays the test instead of making it
// assert `0 == 0` and prove nothing.
TEST(BucketTimeTest, ShutdownStopsTicks) {
    rt::ThreadPool pool(1);
    auto stub = std::make_shared<TimestampingStub>();
    auto bt = std::make_shared<BucketTime>(15ms, stub, &pool);
    bt->start();

    // Wait (bounded) until the timer is demonstrably running, so "the count
    // did not increase" is a statement about a timer that was ticking. Same
    // anti-vacuity shape as DestructionWithoutShutdownStopsTheTimerThread.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (stub->count.load() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_GE(stub->count.load(), 2)
        << "timer never started; a post-shutdown comparison would prove nothing";

    bt->shutdown();   // joins the timer thread => no further post() is possible
    pool.drain();     // ...but let the ticks ALREADY posted land before sampling

    int before = stub->count.load();
    std::this_thread::sleep_for(45ms);
    int after = stub->count.load();
    EXPECT_EQ(after, before) << "Count should not increase after shutdown";
    pool.shutdown();
}

// The alignment property: ticks land at wall-clock multiples of the
// period (within a small tolerance for kernel scheduling jitter). With
// period=100ms we expect every tick's epoch_ms % 100 to be near zero.
TEST(BucketTimeTest, TicksAlignToWallClockPeriod) {
    rt::ThreadPool pool(1);
    auto stub = std::make_shared<TimestampingStub>();
    constexpr auto period = 100ms;
    auto bt = std::make_shared<BucketTime>(period, stub, &pool);
    bt->start();
    std::this_thread::sleep_for(550ms); // ~5 ticks expected
    bt->shutdown();

    std::vector<system_clock::time_point> snapshot;
    {
        std::lock_guard<std::mutex> lk(stub->mx);
        snapshot = stub->ticks;
    }
    ASSERT_GE(snapshot.size(), 3u) << "Need at least 3 ticks to verify alignment";

    constexpr long tolerance_ms = 25; // generous for slow CI / loaded box
    for (size_t i = 0; i < snapshot.size(); ++i) {
        const auto epoch_ms = duration_cast<milliseconds>(
            snapshot[i].time_since_epoch()).count();
        const auto offset = epoch_ms % period.count();
        // Distance from the boundary (it's never negative; offset is in
        // [0, period_ms)).
        const auto delta = std::min(offset, period.count() - offset);
        EXPECT_LE(delta, tolerance_ms)
            << "tick[" << i << "] offset=" << offset << "ms from boundary";
    }
    pool.shutdown();
}

// Two BucketTime nodes constructed at slightly different moments still
// fire on the SAME wall-clock boundaries (the property Interval cannot
// provide). Confirms alignment is a function of the wall clock, not
// the node's start time.
//
// ENC-1639: this used to compare wall-clock ARRIVAL times — for every bt2
// tick, the nearest bt1 tick had to be within 25 ms — and it flaked under load
// (8 reds in 200 runs at loadavg ~28, every one 49-52 ms: exactly one period).
// That is not misalignment. Under load a timer thread can wake after the NEXT
// boundary has already passed, and nextAlignedAfter(now) then arms the one
// after it, so that node simply emits no tick for the bucket in between; and
// bt1 is shut down before bt2, so bt2 can fire a final boundary bt1 never
// reaches. Either way the nearest bt1 arrival is a whole period away while
// both nodes are on the same grid.
//
// So the shared boundary is asserted from what each tick SAYS it is — the
// `bucketStartMs` stamped from the very `target` the timer waited on — not from
// when the tick happened to be delivered:
//   1. every stamp from either node lies on the wall-clock period grid
//      (an Interval-style, start-time-relative node lands 13 ms off it);
//   2. the two nodes emitted at least one IDENTICAL bucket, so "shared" is
//      observed rather than inferred from both being on the grid;
//   3. no tick arrived before the boundary its stamp names — ties the stamp
//      to the real firing time. This bound is one-sided (late is permitted),
//      which is what makes it immune to scheduler delay.
// The upper bound on lateness, i.e. "ticks land promptly at the boundary", is
// TicksAlignToWallClockPeriod's job, not this test's.
TEST(BucketTimeTest, TwoBucketTimesShareBoundary) {
    struct StampingStub : INode {
        std::mutex mx;
        std::vector<std::int64_t> stamps;              // StreamValue::bucketStartMs
        std::vector<system_clock::time_point> arrivals;
        std::atomic<int> count{0};
        void onValue(const StreamValue& v) override {
            const auto now = system_clock::now();
            std::lock_guard<std::mutex> lk(mx);
            stamps.push_back(v.bucketStartMs);
            arrivals.push_back(now);
            ++count;
        }
        void shutdown() noexcept override {}
    };

    rt::ThreadPool pool(2);
    auto stub1 = std::make_shared<StampingStub>();
    auto stub2 = std::make_shared<StampingStub>();
    constexpr auto period = 50ms;
    constexpr std::int64_t period_ms = period.count();
    auto bt1 = std::make_shared<BucketTime>(period, stub1, &pool);
    bt1->start();
    std::this_thread::sleep_for(13ms); // off-period
    auto bt2 = std::make_shared<BucketTime>(period, stub2, &pool);
    bt2->start();

    // Bounded poll instead of a fixed sleep: a slow box delays the test rather
    // than leaving it with too few ticks to say anything.
    const auto deadline = steady_clock::now() + 5s;
    while ((stub1->count.load() < 4 || stub2->count.load() < 4) &&
           steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    bt1->shutdown();
    bt2->shutdown();
    pool.drain(); // every already-posted tick has landed before we read

    std::vector<std::int64_t> s1, s2;
    std::vector<system_clock::time_point> a1, a2;
    {
        std::lock_guard<std::mutex> lk(stub1->mx);
        s1 = stub1->stamps; a1 = stub1->arrivals;
    }
    {
        std::lock_guard<std::mutex> lk(stub2->mx);
        s2 = stub2->stamps; a2 = stub2->arrivals;
    }
    ASSERT_GE(s1.size(), 4u) << "bt1 barely ticked; nothing to compare";
    ASSERT_GE(s2.size(), 4u) << "bt2 barely ticked; nothing to compare";

    auto checkNode = [&](const char* name,
                         const std::vector<std::int64_t>& s,
                         const std::vector<system_clock::time_point>& a) {
        for (size_t i = 0; i < s.size(); ++i) {
            EXPECT_NE(s[i], 0) << name << " tick[" << i << "] carries no bucket identity";
            EXPECT_EQ(s[i] % period_ms, 0)
                << name << " tick[" << i << "] bucketStartMs=" << s[i]
                << " is " << (s[i] % period_ms) << "ms off the " << period_ms
                << "ms wall-clock grid — alignment broken";
            const auto boundary_ms = s[i] + period_ms;
            const auto arrival_ms =
                duration_cast<milliseconds>(a[i].time_since_epoch()).count();
            EXPECT_GE(arrival_ms, boundary_ms)
                << name << " tick[" << i << "] arrived " << (boundary_ms - arrival_ms)
                << "ms BEFORE the boundary its stamp names";
        }
    };
    checkNode("bt1", s1, a1);
    checkNode("bt2", s2, a2);

    { // DIAG ENC-1639
      int gaps1=0,gaps2=0,only2=0;
      for(size_t i=1;i<s1.size();++i) if(s1[i]-s1[i-1]!=period_ms) ++gaps1;
      for(size_t i=1;i<s2.size();++i) if(s2[i]-s2[i-1]!=period_ms) ++gaps2;
      for(auto b:s2) if(std::find(s1.begin(),s1.end(),b)==s1.end()) ++only2;
      if(gaps1||gaps2||only2) fprintf(stderr,"DIAG gaps1=%d gaps2=%d only2=%d last1=%lld last2=%lld\n",gaps1,gaps2,only2,(long long)s1.back(),(long long)s2.back());
    }
    size_t shared = 0;
    for (const auto b : s2)
        if (std::find(s1.begin(), s1.end(), b) != s1.end()) ++shared;
    EXPECT_GE(shared, 1u)
        << "bt1 and bt2 never emitted the same bucket (bt1 " << s1.size()
        << " ticks from " << s1.front() << ", bt2 " << s2.size()
        << " ticks from " << s2.front() << ") — they are not sharing a boundary";
    pool.shutdown();
}

TEST(BucketTimeTest, NextAlignedAfterReturnsStrictlyGreaterMultiple) {
    using TP = system_clock::time_point;
    using MS = milliseconds;
    constexpr auto period = MS{100};

    // Mid-bucket: now=12345 should yield 12400.
    {
        const auto now = TP{MS{12345}};
        const auto next = BucketTime::nextAlignedAfter(now, period);
        EXPECT_EQ(duration_cast<MS>(next.time_since_epoch()).count(), 12400);
    }
    // On-boundary: now=12300 should yield the *next* boundary, 12400.
    {
        const auto now = TP{MS{12300}};
        const auto next = BucketTime::nextAlignedAfter(now, period);
        EXPECT_EQ(duration_cast<MS>(next.time_since_epoch()).count(), 12400);
    }
    // Just past boundary: now=12301 should yield 12400.
    {
        const auto now = TP{MS{12301}};
        const auto next = BucketTime::nextAlignedAfter(now, period);
        EXPECT_EQ(duration_cast<MS>(next.time_since_epoch()).count(), 12400);
    }
}

// ----- ENC-1080 (sibling of ENC-1065, which fixed the same bug on Interval) -----
//
// Dropping the last owning reference must actually destroy the BucketTime and
// stop its timer thread. The timer thread used to hold its own
// shared_from_this(), so the refcount never reached zero, ~BucketTime never
// ran, and the thread kept firing into a child, a ThreadPool and a store that
// the test had already torn down.
//
// This is the failure that does not fail where it is caused: in the ENC-1007
// work the Interval instance of it surfaced as a SIGFPE in an unrelated suite
// ~40 suites later. A test that passes in isolation proves nothing about it,
// which is why the primary assertion here is on the lifetime itself, not on a
// downstream symptom.
TEST(BucketTimeTest, DestructionWithoutShutdownStopsTheTimerThread) {
    rt::ThreadPool pool(1);
    auto stub = std::make_shared<TimestampingStub>();
    std::weak_ptr<BucketTime> weak;
    {
        auto bt = std::make_shared<BucketTime>(10ms, stub, &pool);
        weak = bt;
        bt->start();
        // Wait until the timer thread is demonstrably running, so the test is
        // about lifetime and not about a thread that never got going.
        auto deadline = std::chrono::steady_clock::now() + 2s;
        while (stub->count.load() < 2 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(1ms);
        ASSERT_GE(stub->count.load(), 2) << "timer never started; test proves nothing";
        // Drop the last owning reference WITHOUT calling shutdown().
    }

    EXPECT_TRUE(weak.expired())
        << "BucketTime outlived its last owner — the timer thread still holds a "
           "strong self-reference, so ~BucketTime can never run";

    // Let any tick already posted to the pool land before sampling.
    std::this_thread::sleep_for(20ms);
    const int before = stub->count.load();
    std::this_thread::sleep_for(60ms);
    EXPECT_EQ(stub->count.load(), before)
        << "timer kept firing after the BucketTime's last owner was dropped";

    pool.shutdown();
}
