#pragma once
#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include "krn/nodes/INode.hpp"
#include "krn/nodes/InputPort.hpp"
#include "krn/nodes/JoinBy.hpp"
#include "krn/Span.hpp"

namespace krn {

// Fan-in node: joins N DECLARED INPUTS into one tuple and forwards it
// downstream.
//
// ENC-1291 / SPEC specs/2026-09-20-krn-join-correctness D2, D6, section 1.1
// defect 2.
//
// WHAT CHANGED, AND WHY IT IS A FIX RATHER THAN A BREAK (D6). Until ENC-1291
// this node buffered a flat `std::vector<ArgType>` per `sv.symbol` and fired on
// `vals.size() >= arity_` — it counted VALUES, not INPUTS. An `Aggregate(2)`
// therefore completed a "tuple" from TWO TICKS OF ONE INPUT while the other
// input had never fired once: measured, corpus_id 86 "Bid-ask spread for AAPL"
// reported a spread of +-1.00 where the truth is 0.02. `Aggregate` keeps its
// wire name (D6: forum emits it as the only join shape it can produce, and its
// stored graphs are unmigratable from KRN), so the semantics are corrected
// in place. Turning a wrong answer into a right one under a stored graph is a
// fix, not a compatibility break.
//
// HOW AN INPUT IS IDENTIFIED. Structurally, by build-time port index — never by
// anything on the value. `TreeBuilder` builds each element of the request's
// `inputs` array terminating in its own `InputPort`, and the port calls
// `onPortValue(index, sv)`. `StreamValue` is unchanged and
// `INode::onValue(const StreamValue&)` is still the single edge contract (D2).
//
// JOIN SEMANTICS, stated because ENC-1289's open finding #3 left them open and
// this node is where they get decided:
//
//   * A tuple is a BARRIER over the declared inputs. It completes when every
//     one of the N ports has contributed at least one value; on completion all
//     N slots are emptied and the next tuple starts clean. This is the
//     "emits when all N inputs have reported for a tick cycle" of
//     docs/ARCHITECTURE.md and the positional join of
//     forum/internal/pipelinetranslate/translator.go.
//   * Within an open tuple a port is LAST-VALUE-WINS: if input A fires twice
//     before input B fires at all, A's second value replaces its first and the
//     completed tuple is {A2, B1}. The alternative — queueing per port and
//     zipping — was rejected because it is unbounded under a rate mismatch
//     between inputs (one side outpacing the other grows a queue with no
//     natural bound), whereas last-value-wins is bounded by construction at
//     O(arity) per symbol and pairs the FRESHEST value from each side, which
//     is what a bid/ask join wants.
//   * Emission order is DECLARED PORT ORDER, not arrival order. `inputs[0]`'s
//     value is forwarded first, always. Before ENC-1291 the order was whatever
//     order the values happened to arrive in.
//
// WHAT THIS DELIBERATELY DOES NOT CHANGE (SPEC section 5 Q5, still open). A
// completed tuple is still forwarded MEMBER BY MEMBER — one `onValue` per
// value, under the symbol and bucket of the value that released the tuple —
// not as a single unit. Making it a unit would change what every downstream
// `Worker` in the corpus sees (47 stand alone in `node` position; the 52
// `node`+`pipeline` requests carry 68 `Worker` pipeline stages between them),
// which is a separate decision and not something D2 should smuggle in.
// `Worker`'s never-resetting accumulator (`src/nodes/Worker.cpp:26-32`, cleared
// only in `shutdown()`) is the other half of Q5 and is untouched here.
//
// CORRELATION KEY — DECLARED, as of ENC-1292 (SPEC D1, section 1.1 defect 3).
// `by:"streamKey"` (the default) keys the pending tuple on `sv.symbol`, which
// is what this node always did: two ports fed from two different streamKeys
// land in two different buffers and NEITHER completes, so a cross-streamKey
// join emits nothing rather than pairing one side with itself. `by:"none"`
// keys the pending tuple on NOTHING — every port shares one tuple — so AAPL on
// port 0 and MSFT on port 1 complete each other. Under `by:"none"` the emitted
// value carries the REQUEST'S OWN top-level `streamKey` rather than the symbol
// of whichever side released the tuple, because that symbol is a race (SPEC
// section 5 Q6). include/krn/nodes/JoinBy.hpp carries the full reasoning,
// including why the substitution must NOT apply to `by:"streamKey"`.
//
// Thread-safe: ports may call `onPortValue` concurrently.
class Aggregate final : public INode, public IFanIn {
public:
  // `by` defaults to D1's locked default, so an omitted argument reproduces
  // pre-ENC-1292 behaviour exactly. `outStreamKey` is the request's top-level
  // `streamKey` and is REQUIRED (and enforced) when `by == JoinBy::None`: it is
  // the output identity of the joined stream. It is ignored under
  // `by == JoinBy::StreamKey`, where each symbol keeps its own identity.
  Aggregate(std::size_t arity,
            std::shared_ptr<INode> parent,
            JoinBy by = JoinBy::StreamKey,
            std::string outStreamKey = {});

  // NOT a join member. A fan-in's data comes from its own declared `inputs`,
  // through the ports; a value arriving on the plain edge is an upstream
  // pipeline value, and feeding it into the buffer is precisely how the
  // builder would manufacture defect 2 (see the `CompositeRoot` comment in
  // src/core/TreeBuilder.cpp). It is dropped — loudly, once per node, rather
  // than silently, AND counted (see `pipelineEdgeValues()` below).
  void onValue(const StreamValue& sv) override;

  // The real ingress. `portIndex` is the value's position in the declared
  // `inputs` array, assigned at build time.
  void onPortValue(std::size_t portIndex, const StreamValue& sv) override;

  void shutdown() noexcept override;

  // Builder helper: take ownership of a constructed port so it outlives the
  // upstream Listener's weak_ptr to it. See InputPort.hpp on why the fan-in
  // owns the port and the port only weakly observes the fan-in.
  void addPort(std::shared_ptr<INode> port);

  std::size_t arity() const noexcept { return arity_; }
  JoinBy      by()    const noexcept { return by_; }

  // ─────────────────────────────────────────────────────────────────────────
  // HOW MANY VALUES HAVE EVER REACHED A FAN-IN ON THE PIPELINE EDGE, PROCESS
  // WIDE. Zero in a correct build. ENC-1291.
  //
  // WHY THIS EXISTS, AND WHY IT IS STATIC. `ClockIsNeverAJoinMember`
  // (tests/treebuilder/ComposedChainTest.cpp) is the ONLY gate on SPEC D5 /
  // section 5 Q1's clock rule: the outer `Listener` is the chain's clock and
  // must never become a join member. ENC-1290 built it to redden under the
  // rule's RETRACTED first wording — "forward the clock to the roots that are
  // not Dispatcher-subscribed", where `roots_` hands you the `Aggregate`
  // itself — by watching the clock's value corrupt the join's output.
  //
  // ENC-1291 DISARMED THAT GATE WITHOUT MEANING TO. Making `onValue` a
  // warn-and-drop means the clock can no longer reach `buf_` by any route, so
  // that mutation now produces bit-identical output and the test cannot fail.
  // A green suite that no longer enforces D5 is worse than the defect it
  // replaced, so the invariant is re-armed here rather than left to be
  // rediscovered: the test asserts this counter does not move.
  //
  // It is static because the node under test is constructed by
  // `tree::buildForRequest` behind a `CompositeRoot` the test cannot reach —
  // there is no handle to ask. gtest runs cases sequentially in one process,
  // so a before/after delta is well defined. Nothing in src/ reads it.
  static std::size_t pipelineEdgeValues() noexcept;

private:
  struct SymBuf {
    // Exactly `arity_` slots, one per declared input. Never grows.
    std::vector<std::optional<ArgType>> slots;
    std::size_t filled = 0;
  };

  static constexpr std::size_t MAX_SYMBOLS = 10000;

  const std::size_t arity_;
  const JoinBy      by_;
  // Output identity for a `by:"none"` join, and ALSO its single buffer key —
  // deliberately the same string, so the tuple is buffered under the name it
  // will be emitted under. Empty and unused under `by:"streamKey"`.
  const std::string outKey_;
  std::shared_ptr<INode> parent_;
  std::vector<std::shared_ptr<INode>> ports_;

  std::atomic<bool> stopping_{false};
  std::atomic<bool> warnedOnPipelineValue_{false};
  static std::atomic<std::size_t> pipelineEdgeValues_;
  mutable std::mutex mx_;
  std::unordered_map<std::string, SymBuf> buf_;
};

} // namespace krn
