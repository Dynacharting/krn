#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "krn/nodes/INode.hpp"
#include "krn/Span.hpp"

namespace krn {

// Applies fn to accumulated per-symbol values and forwards the result.
// Computes on every incoming value.
//
// READ THIS BEFORE REASONING ABOUT THE ACCUMULATOR. The two sentences that used
// to stand here were both false, and SPEC
// specs/2026-09-20-krn-join-correctness/SPEC.md section 1.3 lists them as one
// of the three places the engine's own documentation described semantics it
// does not implement:
//
//   * "the accumulator contains all values seen so far for that symbol SINCE
//     LAST CLEAR" — there is no clear. `acc_` is emptied only in `shutdown()`
//     (src/nodes/Worker.cpp). It grows to MAX_ACC and is then trimmed, never
//     reset on any semantic boundary. So `fn_` runs over every value the symbol
//     has ever produced, and e.g. `diff` computes newest-minus-oldest-retained
//     rather than this batch's difference.
//   * "For deterministic N-ary batching, wire Aggregate(N) upstream" —
//     `Aggregate` forwards a completed tuple MEMBER BY MEMBER, one `onValue`
//     per value (src/nodes/Aggregate.cpp). It delivers no batch, so wiring one
//     upstream does not give this node an N-ary batch to reduce. ENC-1291
//     (SPEC D2) fixed WHICH values form a tuple; it deliberately did not change
//     the member-by-member delivery, because doing so changes what every
//     `Worker` in the corpus sees.
//
// The gap between the two bullets is SPEC section 5 **Q5**, open, and it is why
// `corpus_id 86` still does not report a 0.02 spread on a fully composed chain.
// Whoever closes Q5 owns this comment.
class Worker final : public INode {
public:
  using Fn = std::function<ArgType(Span<const ArgType>)>;

  Worker(Fn fn, std::shared_ptr<INode> downstream);

  void onValue(const StreamValue& sv) override;
  void shutdown() noexcept override;

private:
  Fn fn_;
  std::shared_ptr<INode> downstream_;

  static constexpr std::size_t MAX_ACC     = 1000;
  static constexpr std::size_t MAX_SYMBOLS = 10000;

  std::atomic<bool> stopping_{false};
  mutable std::mutex mx_;
  std::unordered_map<std::string, std::vector<ArgType>> acc_;
};

} // namespace krn
