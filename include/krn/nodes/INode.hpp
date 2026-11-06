// include/krn/nodes/INode.hpp
#pragma once
#include <functional>

#include "krn/StreamValue.hpp"
#include "krn/AtomicStore.hpp"

namespace krn {

class INode {
public:
  INode() = default;
  virtual ~INode() = default;

  INode(const INode&) = delete;
  INode& operator=(const INode&) = delete;

  virtual void onValue(const StreamValue& sv) = 0;
  virtual void shutdown() noexcept = 0;

  // ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3.
  //
  // "I re-post every value onto my own SERIALIZING executor, so do not
  // interpose a queue of your own in front of me."
  //
  // `Dispatcher` normally hands each notification to `ThreadPool::post`, which
  // is where per-DAG ordering is lost: the two sides of one tick (`ask`, then
  // `bid`) become two independent pool tasks and can reach the join in either
  // order. A node that answers true here is called INLINE on the ingress
  // thread instead, and is responsible for getting the work off that thread
  // itself — which a `rt::Strand` does, in post order.
  //
  // Only `nodes::Listener` overrides this, and only when it actually holds a
  // strand. Every other node — and every `Listener` on the legacy unordered
  // path — keeps the pool hop it has today, so this is inert for everything
  // that has not opted in.
  virtual bool deliversOnOwnExecutor() const noexcept { return false; }

  // ENC-1335 / SPEC specs/2026-09-20-krn-join-correctness D3 — THE PULL HALF.
  //
  // "My output is read from somewhere else, so WHEN I read it is part of my
  // semantics. Take my sample at the clock, not at my turn in the queue."
  //
  // A PUSH node computes from the value it is handed, so running it later
  // changes nothing: the value came with it. A PULL node — `AtomicAccessor`
  // above all, which discards its argument's `value` and `symbol` entirely —
  // computes from the `AtomicStore`, and the store is written by the ingress
  // thread one statement before the clock is posted. `Dispatcher::onTick` does,
  // per tick and in this order:
  //
  //     write the tick's atomics into the store   (IEventComputer / builtins)
  //     deliver the clock value to the Listener   (inline, ENC-1005)
  //
  // The Listener hands the clock to its `rt::Strand`, and the strand runs it on
  // a pool worker — by which time the ingress thread has moved on and written
  // tick n+1, n+2, … So the value a pull node emits is the store's content at
  // the moment the POOL got to it, not at the moment of the tick that caused
  // it. The emitted sequence duplicates some samples and skips others, and
  // which ones differs from run to run of the same binary. Measured for corpus
  // 51 ("5-period SMA of AAPL"): 20 of 20 runs differed from the per-tick truth
  // at threads=1, 2 and 4 alike.
  //
  // THIS IS NOT WHAT ENC-1005 FIXED, and the thread count proves it. D3's
  // strand orders the deliveries of one DAG against each other, and it does
  // that correctly here — one arrival per tick, in tick order. What it cannot
  // do is move the SAMPLE back to the tick it belongs to, because the strand
  // runs after the ingress thread has gone. A `Listener`-driven pull races the
  // store at ONE pool thread, which an ordering fix cannot reach.
  //
  // THE RULE. A node that answers true here is asked to BIND at clock time:
  // `bindAtClock` runs on the ingress thread, where the store is still
  // coherent with the tick, and returns a closure that DELIVERS the sample it
  // took. The caller posts that closure to its executor instead of posting
  // `onValue`. Sampling moves to the clock; the delivery stays off the ingress
  // thread, so D3's cost model is unchanged.
  //
  // THE RULE IS ADJACENCY, AND THAT IS NOT A SHORTCUT. Only a node sitting
  // directly at a clock hop is bound — the two hops are `nodes::Listener`
  // (the Dispatcher's clock) and `TreeBuilder`'s `CompositeRoot` (a fan-in's
  // clock fan-out, which filters and computes nothing and so can forward the
  // bind unchanged). A pull node further down a chain is NOT bound, and must
  // not be: binding it would require evaluating everything between the clock
  // and it on the ingress thread — a `Filter`'s predicate, a `Worker`'s
  // accumulator — which is the inline-the-whole-DAG design D3's cheap path was
  // struck for. Deeper placements keep execution-time sampling, which is their
  // own semantics, not a coherent reading of the clock.
  //
  // Measured reach in `tests/treebuilder/corpus_requests.json`: all 95
  // `AtomicAccessor` occurrences across the 272 entries sit at a bind point —
  // 81 as the head of `node` (directly downstream of the head `Listener`) and
  // 14 as the declared inputs of the 7 pull-only fan-ins (ids 111-115, 192,
  // 200), which `CompositeRoot` clocks. Zero sit deeper. See
  // `tests/dispatch/ClockCoherentPullTest.cpp`.
  //
  // A PARALLEL RULE, NOT A WIDENING of `deliversOnOwnExecutor()`. That
  // predicate answers *who posts*; this one answers *when the value is read*.
  // They are independent — `Listener` overrides the first and not the second,
  // `AtomicAccessor` the second and not the first — and folding them together
  // would make one bool mean two things.
  virtual bool samplesAtClock() const noexcept { return false; }

  // Take the sample NOW; return a closure that delivers it. Called only when
  // `samplesAtClock()` is true, and called on the clock's own thread.
  //
  // AN EMPTY RETURN MEANS "sampled, and there is nothing to deliver" — never
  // "I did not sample". That distinction is the whole reason this returns a
  // closure rather than a value: an `AtomicAccessor` whose field is not in the
  // store yet emits nothing, and a caller that read an empty result as "fall
  // back to onValue()" would re-read the store later and manufacture exactly
  // the phantom arrival this fix removes (corpus 51, tick 0: 12 arrivals from
  // 12 ticks where the truth is 11).
  virtual std::function<void()> bindAtClock(const StreamValue& clock) {
    (void)clock;
    return {};
  }
};

} // namespace krn
