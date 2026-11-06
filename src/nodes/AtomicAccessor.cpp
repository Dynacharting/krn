#include "krn/nodes/AtomicAccessor.hpp"
#include "krn/atomic/AtomicProviderRegistry.hpp"

#include <utility>

namespace krn {

AtomicAccessor::AtomicAccessor(std::string symbol,
                               std::string field,
                               AtomicStore* store,
                               std::shared_ptr<INode> downstream)
  : symbol_(std::move(symbol))
  , field_(std::move(field))
  , store_(store)
  , downstream_(std::move(downstream))
{}

// ENC-1335 / SPEC specs/2026-09-20-krn-join-correctness D3 (the pull half).
//
// THE SAMPLE IS TAKEN HERE, AND THE DELIVERY IS WHAT GETS DEFERRED. The caller
// runs this on the clock's own thread — for a `Dispatcher`-driven chain that is
// the ingress thread, one statement after the tick's atomics were written into
// the store — and posts the returned closure to its executor. Before this, the
// whole of `onValue` ran on the executor, so the `get()` below observed the
// store at the moment a pool worker happened to reach it: ticks n+1, n+2 … had
// already overwritten it. See `INode::samplesAtClock` for the measurement.
//
// `onValue` is now the degenerate case of the same operation — bind and run in
// one step — so there is exactly one copy of the sampling logic and the
// synchronous path cannot drift from the deferred one.
std::function<void()> AtomicAccessor::bindAtClock(const StreamValue& clock) {
  // Early-out on stopping_ is an optimization; correctness is guaranteed by
  // the mutex: if shutdown() races, the lock ensures we see downstream_==nullptr.
  if (stopping_.load(std::memory_order_acquire)) return {};
  if (!store_) return {};

  // First check AtomicStore for the field
  auto opt = store_->get(symbol_, field_);

  // If not found in the store, try connector-registered namespace providers
  if (!opt.has_value()) {
    auto resolved = AtomicProviderRegistry::tryResolve(symbol_, field_);
    if (resolved.has_value()) {
      opt = resolved.value();
    }
  }

  // Nothing to deliver. An EMPTY closure says "sampled, nothing to emit" — it
  // must never be read as "did not sample", or the caller re-reads the store
  // later and emits a value this tick had no business producing.
  if (!opt.has_value()) return {};

  std::shared_ptr<INode> ds;
  {
    std::lock_guard<std::mutex> lk(mx_);
    ds = downstream_;
  }
  if (!ds) return {};

  // ENC-1280: an AtomicAccessor is clocked by its upstream, which for the
  // ENC-101 canonical pattern may be a BucketTime pulse. Sampling happens
  // in the bucket that pulse closed, so inherit its identity (SPEC D9).
  //
  // The downstream is captured STRONGLY, exactly as `Listener::onValue`'s own
  // strand post captures its downstream: a closure already queued when the DAG
  // is torn down still delivers, and every node's `onValue` is a no-op once it
  // has been shut down.
  return [ds = std::move(ds),
          out = StreamValue{ symbol_, opt.value(), clock.bucketStartMs }]() {
    ds->onValue(out);
  };
}

void AtomicAccessor::onValue(const StreamValue& sv) {
  if (auto deliver = bindAtClock(sv)) deliver();
}

void AtomicAccessor::shutdown() noexcept {
  stopping_.store(true, std::memory_order_release);
  std::lock_guard<std::mutex> lk(mx_);
  downstream_.reset();
}

} // namespace krn
