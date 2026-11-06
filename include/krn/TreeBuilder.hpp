#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "krn/Span.hpp"
#include "krn/StreamValue.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"
#include <rapidjson/document.h>

namespace krn {

// Forward declarations of runtime dependencies
class AtomicStore;
class Dispatcher;

namespace tree {

  // Build-time scope for let-bindings (ENC-647). A `Let` node installs a scope
  // while building its body; each `Ref` resolves the binding name against the
  // scope chain and registers its downstream as a consumer. After the body is
  // built, the Let builds each referenced binding's producer once and fans it
  // out (via Tee) to its consumers — turning the tree into a reuse DAG.
  //
  // Build-time only: producers and consumers are wired during buildOne and the
  // scope is dead afterwards, so Ref is unsupported inside lazily-built
  // subtrees (e.g. GroupSplit children).
  struct BindingScope {
    const rapidjson::Value* bindings { nullptr };   // the "bindings" object
    BindingScope*           parent   { nullptr };   // enclosing scope, if any
    // Referenced binding name -> the consumer nodes that should receive it.
    std::map<std::string, std::vector<std::shared_ptr<INode>>> consumers;
  };

  // Dependencies needed to build a tree
  struct Deps {
    AtomicStore*      store      { nullptr };   // for AtomicAccessor
    rt::ThreadPool*       pool       { nullptr };   // for Listener queues
    Dispatcher* dispatcher { nullptr };   // for Listener wiring
    BindingScope*     bindingScope { nullptr }; // active let-binding scope (ENC-647)

    // ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3, D4.
    //
    // THE REQUEST DAG's serializing executor. ONE per request — shared by every
    // Listener the request builds, which is precisely what orders the two sides
    // of a join against each other. `ClientSession::handleSubscribe` mints one
    // per subscription; `buildForRequest` mints one when the caller did not, so
    // the ordering guarantee is a property of the DAG rather than of the
    // caller's discipline.
    //
    // PER-REQUEST, NOT PER-SYMBOL (D4). A per-symbol shard puts the two sides
    // of a cross-symbol join on two shards with no ordering relation at all,
    // which would make the 23 cross-streamKey `Aggregate` corpus requests
    // permanently unfixable rather than merely broken. `Aggregate` and `Pack`
    // hold pending state for the REQUEST, so the request is the unit.
    //
    // Sub-builds (`Let` bodies, fan-in inputs) copy `Deps` and MUST keep this
    // pointer: two strands inside one DAG is two orderings, i.e. none.
    //
    // CONSTRUCT A `Deps` PER REQUEST. `buildForRequest` mints only when this is
    // null, so a caller that hoists one `Deps` out of a loop hands every
    // subscription the SAME strand and serialises them all against each other —
    // silently, and it is exactly the per-session granularity the comment at
    // `ClientSession::handleSubscribe` explains why not to have. Both current
    // callers build `Deps` inside the per-request scope; the hazard is that
    // hoisting it reads as a pure cleanup. Honouring a supplied strand is
    // deliberate and gated (`ACallerSuppliedStrandIsUsedNotReplaced`), so this
    // cannot be fixed by ignoring the field — only written down.
    //
    // WHAT THIS DOES NOT COVER, STATED SO IT IS NOT MISTAKEN FOR COVERED.
    // The strand orders every value that enters the DAG through a `Listener`,
    // which is every value the Dispatcher pushes. It does NOT order the three
    // nodes that carry their own `std::thread` and post straight to the pool:
    // `Interval` (`src/nodes/Interval.cpp:48`), `BucketTime` (`:92`) and
    // `TumblingWindow` (`:128`). A DAG containing one of those has a second,
    // unordered producer inside it, and a value from that branch can still
    // interleave with a Listener-driven one. Measured on a single
    // `Tee{Listener, Interval}` DAG: 206 arrivals, **max 4 concurrent** — so
    // "delivery is serialized per request DAG" is literally false for that
    // shape, and nothing gates it.
    //
    // That is the SPEC's own descope, not an oversight:
    // `specs/2026-09-20-krn-join-correctness/SPEC.md` §4 keeps the timer
    // conversion out of this project because it collides file-for-line with
    // ENC-1280 in the active *Timestamps on the wire* project. Note the case it
    // does NOT leave open: a PULL-only fan-in input (`AtomicAccessor` and
    // friends) is clocked by the head Listener under ENC-1290's clock rule, so
    // it travels the strand like everything else. The gap is specifically a
    // DAG that declares a timer node of its own.
    std::shared_ptr<rt::Strand> strand;
  };

  // Result of buildForRequest – head plus the downstream chain.
  // `keepAlive` retains every pipeline node to prevent weak_ptr expiry.
  struct BuiltChain {
    std::shared_ptr<INode> head;
    std::vector<std::shared_ptr<INode>> keepAlive;
  };

  // Convenience alias for Worker functions
  using Fn = std::function<ArgType(Span<const ArgType>)>;

  // ---- Low level builders (used internally & for tests) ----

  // Build a single node from JSON spec.
  //  - defaultStreamKey: used when spec omits explicit "streamKey"
  //  - downstream   : parent node (may be nullptr at the tail)
  std::shared_ptr<INode> buildOne(
    const rapidjson::Value& spec,
    const std::string&      defaultStreamKey,
    const Deps&             deps,
    std::shared_ptr<INode>  downstream = nullptr
  );

  // Build a whole tree (root spec is usually the "tree" object from a request)
  std::shared_ptr<INode> buildTree(
    const rapidjson::Value& rootSpec,
    const Deps&             deps
  );

  // ---- Public helpers used by other core components ----

  // Build a pipeline from spec → terminal
  std::shared_ptr<INode> buildNode(
    const rapidjson::Value& spec,
    const std::string&      defaultStreamKey,
    const Deps&             deps,
    std::shared_ptr<INode>  terminal
  );

  // Build a simple AtomicAccessor, optionally wrapped in an Interval poller
  std::shared_ptr<INode> buildSimple(
    const std::string&      streamKey,
    const std::string&      field,
    int                     pollMs,
    const Deps&             deps,
    std::shared_ptr<INode>  terminal
  );

  // High-level entry: given a **validated request JSON**, build a
  // Listener head wired into the rest of the tree that terminates
  // at `terminal` (usually a Responder).
  BuiltChain buildForRequest(
    const rapidjson::Value& requestJson,
    const Deps&             deps,
    std::shared_ptr<INode>  terminal
  );

} // namespace tree
} // namespace krn
