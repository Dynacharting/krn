// include/krn/nodes/InputPort.hpp
#pragma once
#include <cstddef>
#include <memory>
#include <utility>
#include "krn/nodes/INode.hpp"

namespace krn {

// ---------------------------------------------------------------------------
// STRUCTURAL INPUT IDENTITY FOR FAN-IN NODES
// ENC-1291 / SPEC specs/2026-09-20-krn-join-correctness D2 (and D6).
// ---------------------------------------------------------------------------
//
// A fan-in node has to know WHICH of its declared inputs a value came from.
// `INode::onValue(const StreamValue&)` carries no such information and does not
// gain any — D2 rejects both a port-addressed `onValue` overload (it would
// burden all 18 INode implementors for the benefit of two) and a provenance
// token on `StreamValue` (withdrawn by its own advocate; see D2).
//
// So the identity is carried STRUCTURALLY, by the edge rather than by the
// value: `TreeBuilder` builds each declared input terminating in its own
// `InputPort`, constructed with that input's BUILD-TIME index, and the port
// calls `IFanIn::onPortValue(index, sv)`. An input's index is fixed when the
// DAG is built and can never be confused at run time, no matter how the values
// interleave. This is the generalization of `PackPort`, which did exactly this
// for `Pack` and was the one fan-in in the engine that already joined
// correctly by input.
//
// OWNERSHIP IS FORCED, AND THE DIRECTION MATTERS:
//
//     fan-in  --shared_ptr-->  port      (the fan-in OWNS its ports)
//     port    --weak_ptr---->  fan-in    (the port only observes)
//
// The upstream `Listener` holds its downstream WEAKLY (`src/nodes/Listener.cpp`),
// so if nobody owned the port it would be destroyed the moment the builder's
// local `shared_ptr` went out of scope and the input edge would go dead in
// silence. The fan-in is therefore the owner (`addPort`). The back-reference
// must then be weak: a `shared_ptr` back to the fan-in would close an
// `owner -> port -> owner` reference cycle that no `shutdown()` can break, and
// every rejected-then-retried subscription would leak one (see SPEC section 1.5
// for what an unbounded per-subscription leak costs on this hot path).
//
// Both halves are gated: `PortsDoNotKeepTheFanInAlive` and
// `FanInOwnsItsPortsSoTheEdgeSurvivesTheBuilder` in tests/nodes/AggregateTest.cpp.

// The fan-in half of the port contract. Implemented by `Aggregate` and `Pack`.
class IFanIn {
public:
  virtual ~IFanIn() = default;

  // `portIndex` is the position of this input in the node's declared `inputs`
  // (or `fields`) array, assigned at build time. Implementors must tolerate an
  // out-of-range index defensively rather than trusting the caller.
  virtual void onPortValue(std::size_t portIndex, const StreamValue& sv) = 0;
};

// Per-input adapter: tags each incoming value with its build-time input index
// and forwards it to the owning fan-in. Stateless apart from the index, so it
// adds no synchronization of its own — the fan-in remains the single point at
// which concurrent inputs are serialized.
class InputPort final : public INode {
public:
  InputPort(std::weak_ptr<IFanIn> owner, std::size_t idx)
    : owner_(std::move(owner)), idx_(idx) {}

  void onValue(const StreamValue& sv) override {
    if (auto p = owner_.lock()) p->onPortValue(idx_, sv);
  }

  // Nothing to tear down: the port owns no state and no thread, and it must
  // NOT shut its owner down — the owner shuts the ports down, not vice versa.
  void shutdown() noexcept override {}

  std::size_t index() const noexcept { return idx_; }

private:
  const std::weak_ptr<IFanIn> owner_;
  const std::size_t           idx_;
};

} // namespace krn
