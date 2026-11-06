#include "krn/nodes/Aggregate.hpp"
#include "krn/util/Logger.hpp"
#include <stdexcept>
#include <utility>

namespace krn {

Aggregate::Aggregate(std::size_t arity,
                     std::shared_ptr<INode> parent,
                     JoinBy by,
                     std::string outStreamKey)
  : arity_(arity), by_(by), outKey_(std::move(outStreamKey)),
    parent_(std::move(parent))
{
  if (arity_ == 0)
    throw std::invalid_argument("Aggregate: arity must be > 0");

  // ENC-1292 / SPEC section 5 Q6. A `by:"none"` join has no per-symbol
  // identity to emit under — that is the point of it — so it MUST be given
  // one. The builder passes the request's top-level `streamKey`, which
  // `buildForRequest` already refuses to leave empty; this is the node's own
  // guard for every other construction path. Emitting under the empty symbol
  // would be a silent wrong answer on the wire.
  if (by_ == JoinBy::None && outKey_.empty())
    throw std::invalid_argument(
      "Aggregate: by:\"none\" requires a non-empty output streamKey — a join "
      "that ignores the symbol has no identity of its own and must be given "
      "the request's top-level 'streamKey' (SPEC "
      "specs/2026-09-20-krn-join-correctness section 5 Q6)");
}

void Aggregate::addPort(std::shared_ptr<INode> port) {
  ports_.push_back(std::move(port));
}

std::atomic<std::size_t> Aggregate::pipelineEdgeValues_{0};

std::size_t Aggregate::pipelineEdgeValues() noexcept {
  return pipelineEdgeValues_.load(std::memory_order_relaxed);
}

// A value on the plain INode edge is NOT a join member — see the header. It is
// dropped, counted, and said out loud once per node so the drop is never
// silent. The count is what `ClockIsNeverAJoinMember` asserts against; without
// it that gate cannot fail, because a dropped value leaves no other trace.
void Aggregate::onValue(const StreamValue& sv) {
  pipelineEdgeValues_.fetch_add(1, std::memory_order_relaxed);
  if (!warnedOnPipelineValue_.exchange(true, std::memory_order_acq_rel)) {
    krn::util::logger().log(krn::util::LogLevel::Warn,
      "Aggregate: value arrived on the pipeline edge, not on a declared input "
      "port, and was dropped. A fan-in node joins its own 'inputs'; it cannot "
      "consume an upstream value (SPEC specs/2026-09-20-krn-join-correctness "
      "D2). Logged once per node.",
      {{"symbol", sv.symbol}});
  }
}

void Aggregate::onPortValue(std::size_t portIndex, const StreamValue& sv) {
  // Early-out on stopping_ is an optimization; correctness is guaranteed by
  // the mutex: if shutdown() races, the lock ensures we see parent_==nullptr.
  if (stopping_.load(std::memory_order_acquire)) return;

  // Defensive: the builder never hands out an index >= arity_ (it constructs
  // exactly one port per declared input and refuses a request whose `arity`
  // disagrees with `inputs.size()`), so this is unreachable from client JSON.
  if (portIndex >= arity_) return;

  // ENC-1292 / SPEC D1 — THE DECLARED CORRELATION KEY, and it is the SAME
  // string the completed tuple is emitted under (SPEC section 5 Q6):
  //
  //   by:"streamKey"  -> sv.symbol. Every byte of this path is what shipped
  //                      before ENC-1292, which is what D6's no-migration
  //                      guarantee for stored forum graphs rests on.
  //   by:"none"       -> the request's own top-level streamKey. One shared
  //                      pending tuple for every symbol, so AAPL on port 0 and
  //                      MSFT on port 1 complete each other, and the result is
  //                      ONE logical stream rather than a coin-flip between
  //                      the two input symbols.
  //
  // Buffering and emitting under one key is deliberate: it makes it impossible
  // for the two to drift apart, which is the bug a separate `outSymbol` local
  // would eventually grow.
  const std::string& joinKey =
      (by_ == JoinBy::None) ? outKey_ : sv.symbol;

  std::vector<ArgType> batch;
  std::shared_ptr<INode> p;
  {
    std::lock_guard<std::mutex> lk(mx_);
    // Cap distinct symbol count to prevent unbounded map growth. Under
    // `by:"none"` there is exactly one entry and the cap can never trip.
    //
    // ONE lookup, on ONE key. This was a `find` followed by an `emplace`, and
    // the pair was an EQUIVALENT-MUTATION surface: `emplace` returns the
    // existing element when the key is present, so keying the `find` on
    // `sv.symbol` while the `emplace` still used `joinKey` produced
    // bit-identical behaviour and no test could ever have caught it (measured,
    // ENC-1292 mutation M18). `try_emplace` says the key once.
    auto [it, inserted] = buf_.try_emplace(joinKey);
    if (inserted) {
      if (buf_.size() > MAX_SYMBOLS) {
        buf_.erase(it);
        krn::util::logger().log(krn::util::LogLevel::Warn,
          "Aggregate: max symbols reached, dropping",
          {{"symbol", sv.symbol}});
        return;
      }
      it->second.slots.resize(arity_);
    }

    auto& sb = it->second;
    if (!sb.slots[portIndex].has_value()) ++sb.filled;
    sb.slots[portIndex] = sv.value;          // last-value-wins within the tuple

    if (sb.filled < arity_) return;          // tuple not complete yet

    // Complete. Emit in DECLARED PORT ORDER, then reset the barrier.
    batch.reserve(arity_);
    for (std::size_t i = 0; i < arity_; ++i)
      batch.push_back(std::move(*sb.slots[i]));
    for (auto& s : sb.slots) s.reset();
    sb.filled = 0;

    p = parent_;
  }

  // Forward outside the lock to avoid holding it during downstream calls.
  if (p) {
    for (auto& v : batch) {
      // ENC-1280: the tuple is released by `sv`, so every value in it is
      // forwarded under `sv`'s bucket identity.
      // ENC-1291 / SPEC Q5: still one onValue per member, deliberately — the
      // tuple is not delivered as a unit. See the header.
      // ENC-1292 / SPEC Q6: the SYMBOL, however, is the join key, not `sv`'s —
      // identical to `sv.symbol` under the default, and the request's own
      // streamKey under `by:"none"`, where `sv.symbol` is whichever side
      // happened to arrive second and is therefore a race.
      p->onValue(StreamValue{ joinKey, std::move(v), sv.bucketStartMs });
    }
  }
}

void Aggregate::shutdown() noexcept {
  stopping_.store(true, std::memory_order_release);

  std::vector<std::shared_ptr<INode>> ports;
  {
    std::lock_guard<std::mutex> lk(mx_);
    buf_.clear();
    parent_.reset();
    ports.swap(ports_);
  }
  for (auto& p : ports)
    if (p) p->shutdown();
}

} // namespace krn
