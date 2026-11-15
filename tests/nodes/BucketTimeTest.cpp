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
#include <atomic>
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
TEST(BucketTimeTest, TwoBucketTimesShareBoundary) {
    rt::ThreadPool pool(2);
    auto stub1 = std::make_shared<TimestampingStub>();
    auto stub2 = std::make_shared<TimestampingStub>();
    constexpr auto period = 50ms;
    auto bt1 = std::make_shared<BucketTime>(period, stub1, &pool);
    bt1->start();
    std::this_thread::sleep_for(13ms); // off-period
    auto bt2 = std::make_shared<BucketTime>(period, stub2, &pool);
    bt2->start();
    std::this_thread::sleep_for(220ms);
    bt1->shutdown();
    bt2->shutdown();

    std::vector<system_clock::time_point> ticks1, ticks2;
    {
        std::lock_guard<std::mutex> lk(stub1->mx);
        ticks1 = stub1->ticks;
    }
    {
        std::lock_guard<std::mutex> lk(stub2->mx);
        ticks2 = stub2->ticks;
    }
    ASSERT_FALSE(ticks1.empty());
    ASSERT_FALSE(ticks2.empty());
    { // DIAG ENC-1639: did ticks land AFTER the snapshot?
      pool.drain();
      size_t n1,n2;{std::lock_guard<std::mutex> lk(stub1->mx);n1=stub1->ticks.size();}
      {std::lock_guard<std::mutex> lk(stub2->mx);n2=stub2->ticks.size();}
      long maxLate=0; // arrival lateness vs nearest boundary
      for(auto*v:{&ticks1,&ticks2}) for(auto&t:*v){long o=duration_cast<milliseconds>(t.time_since_epoch()).count()%50; if(o>maxLate) maxLate=o;}
      {std::string o="DIAGB b1:";for(auto&t:ticks1)o+=" "+std::to_string(duration_cast<milliseconds>(t.time_since_epoch()).count()/50%1000);o+=" b2:";for(auto&t:ticks2)o+=" "+std::to_string(duration_cast<milliseconds>(t.time_since_epoch()).count()/50%1000);fprintf(stderr,"%s\n",o.c_str());}
      if(n1!=ticks1.size()||n2!=ticks2.size()||maxLate>25) fprintf(stderr,"DIAG late1=%zu late2=%zu maxLateMs=%ld\n",n1-ticks1.size(),n2-ticks2.size(),maxLate);
    }

    // For every tick from bt2, find the closest tick from bt1; they
    // should be within scheduling jitter, NOT 13ms apart (which is what
    // Interval would produce).
    constexpr long jitter_ms = 25;
    for (const auto& t2 : ticks2) {
        long minDiff = std::numeric_limits<long>::max();
        for (const auto& t1 : ticks1) {
            const long d = std::abs(duration_cast<milliseconds>(t1 - t2).count());
            if (d < minDiff) minDiff = d;
        }
        EXPECT_LE(minDiff, jitter_ms)
            << "bt2 tick is " << minDiff << "ms from nearest bt1 tick — alignment broken";
    }
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
