#include "krn/nodes/BucketTime.hpp"
#include "krn/util/Logger.hpp"

#include <cstdint>

namespace krn {

BucketTime::BucketTime(std::chrono::milliseconds period,
                       std::shared_ptr<INode> child,
                       krn::rt::ThreadPool* pool)
  : state_(std::make_shared<State>(period, std::move(child), pool))
{
}

BucketTime::~BucketTime() {
  // The timer thread holds no reference back to this object (ENC-1080), so
  // reaching the destructor at all is the normal path even when nobody called
  // shutdown(). Stop and join here so no thread outlives the BucketTime.
  BucketTime::shutdown();
}

void BucketTime::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true))
    return;

  // Capture the State, never `this` and never a shared_from_this().
  timerThread_ = std::thread([st = state_] { timerLoop(st); });
}

std::chrono::system_clock::time_point
BucketTime::nextAlignedAfter(
    std::chrono::system_clock::time_point from,
    std::chrono::milliseconds period) {
  using namespace std::chrono;
  // Compute (epoch ms / period) * period + period — the next strictly-
  // greater wall-clock multiple of `period`.
  const auto epoch_ms = duration_cast<milliseconds>(from.time_since_epoch()).count();
  const auto period_ms = period.count();
  if (period_ms <= 0) {
    // Defensive — caller shouldn't pass non-positive periods. Return
    // `from` so the caller sleeps zero and re-evaluates.
    return from;
  }
  const auto next_ms = ((epoch_ms / period_ms) + 1) * period_ms;
  return system_clock::time_point{milliseconds{next_ms}};
}

std::int64_t
BucketTime::bucketStartMsFor(
    std::chrono::system_clock::time_point boundary,
    std::chrono::milliseconds period) {
  using namespace std::chrono;
  const auto period_ms = period.count();
  if (period_ms <= 0) {
    // No period => no bucket grid => no bucket identity (StreamValue's 0
    // sentinel), matching nextAlignedAfter's defensive branch.
    return 0;
  }
  const auto boundary_ms =
      duration_cast<milliseconds>(boundary.time_since_epoch()).count();
  return static_cast<std::int64_t>(boundary_ms - period_ms);
}

void BucketTime::timerLoop(const std::shared_ptr<State>& st) {
  // MUTATION ENC-1639: start-relative phase (Interval-like) — misaligns nodes started at different moments
  const auto phase = std::chrono::milliseconds{std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() % st->period.count()};
  while (true) {
    if (st->stopping.load(std::memory_order_acquire))
      break;

    const auto now = std::chrono::system_clock::now();
    const auto target = nextAlignedAfter(now - phase, st->period) + phase;
    // ENC-1280: the boundary used to die here as a loop-body local. Keep the
    // identity of the bucket that closes at `target` so the pulse carries the
    // bar it belongs to all the way to the responder (SPEC D9).
    const std::int64_t bucketStartMs = bucketStartMsFor(target, st->period);
    {
      std::unique_lock<std::mutex> lk(st->mx);
      // wait_until lets shutdown wake us early without re-arming.
      if (st->cv.wait_until(lk, target, [&st] {
            return st->stopping.load(std::memory_order_acquire);
          })) {
        break;
      }
    }
    if (st->stopping.load(std::memory_order_acquire))
      break;
    if (!st->child) break;

    try {
      if (st->pool) {
        auto c = st->child;
        st->pool->post([c, bucketStartMs] {
          c->onValue(StreamValue{"", 0.0, bucketStartMs});
        });
      } else {
        st->child->onValue(StreamValue{"", 0.0, bucketStartMs});
      }
    } catch (const std::exception& ex) {
      krn::util::logger().log(krn::util::LogLevel::Error,
        "BucketTime::timerLoop: onValue exception",
        {{"err", ex.what()}});
    }
  }
}

void BucketTime::onValue(const StreamValue&) {
  // source node: no upstream input
}

void BucketTime::shutdown() noexcept {
  state_->stopping.store(true, std::memory_order_release);
  state_->cv.notify_all();
  if (timerThread_.joinable()) {
    // If shutdown() is called from the timer thread itself (e.g. via a
    // downstream callback), join() would deadlock. Detach instead: the thread
    // owns the State it is still reading, so it can finish safely even if the
    // BucketTime is destroyed first.
    if (timerThread_.get_id() == std::this_thread::get_id()) {
      timerThread_.detach();
    } else {
      timerThread_.join();
    }
  }
}

} // namespace krn
