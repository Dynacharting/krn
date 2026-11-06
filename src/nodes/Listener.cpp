#include "krn/nodes/Listener.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/atomic/AtomicProviderRegistry.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/util/Logger.hpp"

#include <utility>

using namespace krn::nodes;

krn::Result<std::shared_ptr<Listener>> Listener::Create(
    std::string symbol,
    std::string field,
    std::shared_ptr<INode> downstream,
    krn::rt::ThreadPool* pool,
    krn::Dispatcher* dispatcher,
    std::shared_ptr<krn::rt::Strand> strand) {
  // ENC-101 / ENC-1575: a key in a namespace a plugin declared pipeline-only
  // is never pushed, so a Listener on it would silently never fire.
  if (krn::AtomicProviderRegistry::isPipelineOnlyKey(field)) {
    return krn::Error{
      "listener: field '" + field +
        "' is pipeline-only — see docs/atomic-keys.md; bind via "
        "AtomicAccessor downstream of a bare-key Listener clock",
      "Listener::Create"
    };
  }
  auto self = std::make_shared<Listener>(
      std::move(symbol),
      std::move(field),
      std::move(downstream),
      pool,
      dispatcher,
      std::move(strand));
  self->start();
  return self;
}

Listener::Listener(std::string symbol,
                   std::string field,
                   std::shared_ptr<INode> downstream,
                   krn::rt::ThreadPool* pool,
                   krn::Dispatcher* dispatcher,
                   std::shared_ptr<krn::rt::Strand> strand)
  : symbol_(std::move(symbol))
  , field_(std::move(field))
  , downstream_(std::move(downstream))
  , pool_(pool)
  , dispatcher_(dispatcher)
  , strand_(std::move(strand))
{
}

void Listener::start() {
  // Must be called after construction when owned by a shared_ptr.
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true))
    return; // already started

  if (dispatcher_) {
    dispatcher_->registerListener(symbol_, field_, shared_from_this());
  }
}

void Listener::onValue(const krn::StreamValue& sv) {
  if (stopping_.load(std::memory_order_acquire)) return;

  std::shared_ptr<INode> down;
  {
    std::lock_guard<std::mutex> lk(downMx_);
    down = downstream_.lock();
  }
  if (!down) return;

  // ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3, D4.
  //
  // The STRAND is the whole fix. Every Listener in one request DAG shares one,
  // so the values this DAG is built to join arrive downstream in the order the
  // Dispatcher produced them, tick after tick. Posting to the bare pool (the
  // `else if` below) is what put tick N and tick N+1 — and `ask` and `bid` of
  // one tick — on two workers at once, which is how `Aggregate`'s buffer came
  // to pair `ask(n)` with `ask(n+1)` and report a spread of +-1.00 where the
  // truth is 0.02 (SPEC §1.1 defect 4).
  //
  // A strand is not a thread and adds none: the work still runs on the shared
  // pool, one task at a time per DAG. The cost is that one DAG can no longer
  // occupy more than one core, which SPEC §5 Q2 rules acceptable.
  //
  // The strand path forwards the whole `StreamValue`, so `bucketStartMs`
  // (ENC-1280) survives; the pool path below rebuilds a two-field value and
  // drops it. That is not a behaviour change today — `Dispatcher` constructs
  // every pushed value with `bucketStartMs == 0` (StreamValue.hpp:134-138
  // calls the raw Listener path "no bucket identity"), so there is nothing to
  // lose on either path. It is written the correct way here so the difference
  // does not become one later.
  // ENC-1335 — THE PULL SAMPLE IS TAKEN HERE, NOT ON THE EXECUTOR.
  //
  // This function runs on the ingress thread (ENC-1005 made `Dispatcher`
  // call a strand-bearing Listener inline), and `Dispatcher::onTick` has just
  // written this tick's atomics into the `AtomicStore`. That makes HERE the
  // only point at which the store is coherent with the tick being delivered:
  // by the time the executor runs, the ingress thread has posted tick n+1 and
  // overwritten it. A downstream that reads the store rather than the value —
  // `AtomicAccessor`, and the `CompositeRoot` that clocks a pull-only fan-in's
  // inputs — therefore gets its sample taken now and its DELIVERY posted.
  //
  // An empty binding means the pull found nothing for this tick, and the
  // correct action is to post nothing at all. Falling through to `onValue`
  // would read the store again, later, and emit a value this tick did not
  // have — the phantom arrival in `INode::samplesAtClock`'s note.
  //
  // Both async paths bind. The strand path is production; the bare-pool path
  // is the legacy unordered one, where binding does not buy ordering (it never
  // had any) but does stop every sample being read at a scheduler's whim. The
  // inline `else` needs nothing: there, delivery already happens at the clock.
  if (strand_) {
    if (down->samplesAtClock()) {
      if (auto deliver = down->bindAtClock(sv)) strand_->post(std::move(deliver));
      return;
    }
    strand_->post([d = down, sv]() mutable {
      d->onValue(sv);
    });
  } else if (pool_) {
    if (down->samplesAtClock()) {
      if (auto deliver = down->bindAtClock(sv)) pool_->post(std::move(deliver));
      return;
    }
    pool_->post([d = std::move(down), sym = sv.symbol, val = sv.value]() mutable {
      d->onValue(krn::StreamValue{std::move(sym), std::move(val)});
    });
  } else {
    down->onValue(sv);
  }
}

void Listener::shutdown() noexcept {
  bool expected = false;
  if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    return; // already shutting down

  try {
    if (dispatcher_) {
      dispatcher_->unregisterListener(symbol_, field_, shared_from_this());
    }
  } catch (const std::exception& ex) {
    krn::util::logger().log(krn::util::LogLevel::Error,
                            "Listener shutdown error",
                            { {"symbol", symbol_}, {"err", ex.what()} });
  } catch (...) {
    krn::util::logger().log(krn::util::LogLevel::Error,
                            "Listener shutdown unknown error",
                            { {"symbol", symbol_} });
  }
  {
    std::lock_guard<std::mutex> lk(downMx_);
    downstream_.reset();
  }
}
