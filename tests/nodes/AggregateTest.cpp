// ENC-1291 — the join counts INPUTS, not values.
//
// SPEC specs/2026-09-20-krn-join-correctness D2, D6, section 1.1 defect 2.
//
// ─────────────────────────────────────────────────────────────────────────────
// THIS FILE USED TO PIN THE DEFECT.
//
// Until ENC-1291 every test here drove `Aggregate` through the plain
// `INode::onValue` edge with no input identity at all, and asserted that N
// values — from ANY source, including N ticks of ONE input — completed a
// "tuple". SPEC section 1.2 names it: *"tests/nodes/AggregateTest.cpp:30-44
// actively PINS the defect, asserting that N values arrive individually."*
// It is rewritten rather than extended, because every one of its assertions
// was a statement of the bug.
//
// The join is now addressed STRUCTURALLY: each declared input terminates in its
// own `InputPort`, constructed with that input's build-time index, and the port
// calls `Aggregate::onPortValue(index, sv)`. `StreamValue` is unchanged and
// `INode::onValue` keeps its single-argument signature (D2).
//
// What each block gates:
//   1. the join itself — a tuple needs one value from EVERY declared input
//   2. the semantics D2 had to choose — declared-order emission, barrier reset,
//      last-value-wins inside an open tuple
//   3. ownership — the fan-in owns its ports, the ports observe it weakly, and
//      getting either direction wrong leaks or dangles
//   4. the plain pipeline edge — a value arriving there is NOT a join member

#include "krn/nodes/Aggregate.hpp"
#include "krn/nodes/InputPort.hpp"
#include "krn/StreamValue.hpp"
#include "krn/nodes/INode.hpp"
#include <gtest/gtest.h>
#include <stdexcept>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace krn;

namespace {

// Parent stub collects everything the join forwards.
class TestParent : public INode {
public:
    std::mutex mx;
    std::vector<StreamValue> received;
    std::atomic<int> count{0};
    void onValue(const StreamValue& sv) override {
        std::lock_guard<std::mutex> lk(mx);
        received.push_back(sv);
        ++count;
    }
    void shutdown() noexcept override {}
};

double extractDouble(const ArgType& a) { return std::get<double>(a); }

// Build an Aggregate wired exactly the way TreeBuilder wires one: `arity`
// ports, owned by the Aggregate, each holding a weak_ptr back. Returns the
// ports so a test can drive each declared input independently — which is the
// whole point: there is no other way to reach the join.
struct Join {
    std::shared_ptr<Aggregate>          agg;
    std::vector<std::shared_ptr<INode>> ports;
    std::shared_ptr<TestParent>         parent;
};

Join makeJoin(std::size_t arity) {
    Join j;
    j.parent = std::make_shared<TestParent>();
    j.agg    = std::make_shared<Aggregate>(arity, j.parent);
    for (std::size_t i = 0; i < arity; ++i) {
        auto p = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(j.agg), i);
        j.agg->addPort(p);
        j.ports.push_back(p);
    }
    return j;
}

void feed(Join& j, std::size_t port, const char* sym, double v) {
    j.ports[port]->onValue(StreamValue{sym, v});
}

} // namespace

// ═══ 1. The join counts INPUTS ═══════════════════════════════════════════════

// THE regression gate for SPEC section 1.1 defect 2, at unit level. Before
// ENC-1291 this sequence emitted TWO "complete tuples" of {100,101} and
// {102,103} from input 0 alone, while input 1 had never fired.
TEST(AggregateTest, OneInputAloneNeverCompletesATuple) {
    Join j = makeJoin(2);

    for (int n = 0; n < 8; ++n) feed(j, 0, "SYM", 100.0 + n);

    EXPECT_EQ(j.parent->count.load(), 0)
        << "8 values arrived on input 0 and NONE on input 1. A two-input join "
           "has no complete tuple to emit.\n"
           "    Before ENC-1291 `Aggregate` buffered a flat vector per "
           "`sv.symbol` and fired on `vals.size() >= arity_`,\n"
           "    counting VALUES rather than distinct INPUTS — which is how "
           "corpus_id 86 reported a +-1.00 spread\n"
           "    where the truth is 0.02 (SPEC section 1.1 defect 2).";
}

TEST(AggregateTest, CompletesOnlyWhenEveryInputHasReported) {
    Join j = makeJoin(3);

    feed(j, 0, "SYM", 1.0);
    feed(j, 2, "SYM", 3.0);
    EXPECT_EQ(j.parent->count.load(), 0) << "inputs 0 and 2 reported; input 1 did not";

    feed(j, 1, "SYM", 2.0);
    ASSERT_EQ(j.parent->count.load(), 3);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 1.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 2.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[2].value), 3.0);
}

// ═══ 2. The semantics D2 had to choose ═══════════════════════════════════════

// Emission order is DECLARED PORT ORDER, not arrival order. Before ENC-1291 it
// was arrival order, which is what made corpus 86's tuple order depend on
// `Dispatcher`'s std::map field ordering and on the thread pool.
TEST(AggregateTest, EmitsInDeclaredPortOrderNotArrivalOrder) {
    Join j = makeJoin(2);

    feed(j, 1, "SYM", 99.0);      // input 1 arrives FIRST
    feed(j, 0, "SYM", 11.0);

    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 11.0)
        << "inputs[0]'s value must be forwarded first regardless of arrival order";
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 99.0);
}

// A completed tuple empties every slot: the next tuple starts clean and needs
// one fresh value from each input again.
TEST(AggregateTest, BarrierResetsAfterEachTuple) {
    Join j = makeJoin(2);

    feed(j, 0, "A", 10.0);
    feed(j, 1, "A", 20.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    j.parent->received.clear();
    j.parent->count = 0;

    // One input alone must NOT complete a second tuple off the first one's
    // leftovers — the failure mode a combineLatest-style fan-in would have.
    feed(j, 0, "A", 30.0);
    EXPECT_EQ(j.parent->count.load(), 0);

    feed(j, 1, "A", 40.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 30.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 40.0);
}

// Within an OPEN tuple a port is last-value-wins. Chosen over per-port queueing
// because a queue is unbounded under a rate mismatch between inputs, while this
// is O(arity) per symbol and pairs the freshest value from each side.
TEST(AggregateTest, LastValueWinsWithinAnOpenTuple) {
    Join j = makeJoin(2);

    feed(j, 0, "A", 10.0);
    feed(j, 0, "A", 11.0);
    feed(j, 0, "A", 12.0);
    EXPECT_EQ(j.parent->count.load(), 0) << "still no value from input 1";

    feed(j, 1, "A", 20.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 12.0)
        << "the freshest value from input 0, not its oldest";
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 20.0);

    // And the superseded values are GONE, not queued: no second tuple is owed.
    j.parent->received.clear();
    j.parent->count = 0;
    feed(j, 1, "A", 21.0);
    EXPECT_EQ(j.parent->count.load(), 0);
}

// The correlation key is still `sv.symbol` (D1's locked default
// `by:"streamKey"`), so two symbols keep independent tuples — and a join whose
// two ports are fed from two DIFFERENT symbols completes neither. That second
// half is SPEC section 1.1 defect 3 and is ENC-1292's (`by:"none"`), recorded
// here so the boundary between the two tickets is checkable.
TEST(AggregateTest, SeparateSymbolsAreIndependentBuffers) {
    Join j = makeJoin(2);

    // INTERLEAVED ON PURPOSE. Driving X to completion and then Y to completion
    // does not test this at all: a single shared buffer produces the same four
    // values in the same order, and the first draft of this test was green
    // under exactly that mutation (ENC-1291 mutation M13, `buf_.find("")`).
    // Interleaving makes the two readings diverge — with one buffer, Y's value
    // on port 0 overwrites X's before X's port 1 arrives.
    feed(j, 0, "X", 1.0);
    feed(j, 0, "Y", 3.0);      // a DIFFERENT symbol, same port, X still open
    feed(j, 1, "X", 2.0);      // completes X, and only X

    ASSERT_EQ(j.parent->count.load(), 2)
        << "expected exactly X's tuple; Y is still waiting on port 1";
    EXPECT_EQ(j.parent->received[0].symbol, "X");
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 1.0)
        << "X's port-0 value was overwritten by Y's — the two symbols are "
           "sharing one buffer";
    EXPECT_EQ(j.parent->received[1].symbol, "X");
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 2.0);

    feed(j, 1, "Y", 4.0);      // now Y completes, from ITS OWN slots
    ASSERT_EQ(j.parent->count.load(), 4);
    EXPECT_EQ(j.parent->received[2].symbol, "Y");
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[2].value), 3.0);
    EXPECT_EQ(j.parent->received[3].symbol, "Y");
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[3].value), 4.0);
}

TEST(AggregateTest, CrossSymbolPortsCompleteNothingUntilENC1292) {
    Join j = makeJoin(2);

    for (int n = 0; n < 6; ++n) {
        feed(j, 0, "AAPL", 1000.0 + n);
        feed(j, 1, "MSFT", 5000.0 + n);
    }

    EXPECT_EQ(j.parent->count.load(), 0)
        << "AAPL and MSFT land in two different `buf_[sv.symbol]` entries, so "
           "neither tuple completes.\n"
           "    That is inert rather than WRONG — before ENC-1291 this emitted "
           "six tuples each pairing one\n"
           "    symbol with ITSELF. The cross-streamKey join is SPEC D1 "
           "`by:\"none\"` and belongs to ENC-1292.";
}

TEST(AggregateTest, ArityOneEmitsEveryValue) {
    Join j = makeJoin(1);
    for (int n = 0; n < 5; ++n) feed(j, 0, "S", double(n));
    EXPECT_EQ(j.parent->count.load(), 5);
}

TEST(AggregateTest, ArityZeroIsRejected) {
    auto parent = std::make_shared<TestParent>();
    EXPECT_THROW((Aggregate{0, parent}), std::invalid_argument);
}

TEST(AggregateTest, ShutdownPreventsFurtherCallbacks) {
    Join j = makeJoin(1);

    feed(j, 0, "Z", 5.0);
    EXPECT_EQ(j.parent->count.load(), 1);

    j.agg->shutdown();
    j.parent->received.clear();
    j.parent->count = 0;
    feed(j, 0, "Z", 6.0);
    EXPECT_EQ(j.parent->count.load(), 0);
}

// READ THIS BEFORE TRUSTING THIS TEST: IT IS NOT THE GATE ON THE MUTEX.
//
// Deleting `Aggregate::onPortValue`'s `std::lock_guard` outright — leaving an
// `unordered_map` insertion and a `vector<optional>` mutation racing between
// two threads — leaves the FULL SUITE green (measured, ENC-1291 adversarial
// pass: 641/640/1, bit-identical to baseline; the test alone failed 5 of 30
// runs in isolation, and when it did fail it was the anti-vacuity floor that
// fired, never the tuple-integrity assertion). A data race is undefined
// behaviour: it is not reliably observable by assertion, and a test that
// catches it 17% of the time is a coin flip, not a gate.
//
// **THE GATE IS THE SANITIZER RUN.** TSan reports the missing lock
// deterministically, and ENC-1291 verified that specifically rather than only
// checking TSan was clean on the fixed code: the same deletion, built
// `-DKRN_SANITIZE=thread`, produced **11 `WARNING: ThreadSanitizer: data race`
// reports naming `krn::Aggregate::onPortValue` in the first frame**, and the
// binary exited 66. So the invariant IS gated — by `mage`-equivalent
// `cmake -DKRN_SANITIZE=thread` + this suite, not by the assertions below. This test is a smoke test plus a structural
// check that what escapes is always a WHOLE tuple, in port order — it is kept
// because the structural half is worth having, and it says so out loud rather
// than implying a guarantee it cannot make.
TEST(AggregateTest, ConcurrentPortValuesAreWholeTuplesInPortOrder) {
    Join j = makeJoin(2);

    // Disjoint value ranges per port, so every emitted value announces which
    // declared input it came from. Under the barrier the parent must see a
    // strict alternation: an input-0 value, then an input-1 value, forever.
    constexpr double kPort0Base = 0.0;
    constexpr double kPort1Base = 1000.0;
    const int perThread = 500;

    std::vector<std::thread> threads;
    for (int port = 0; port < 2; ++port) {
        threads.emplace_back([&j, port, perThread] {
            const double base = port == 0 ? kPort0Base : kPort1Base;
            for (int i = 0; i < perThread; ++i)
                feed(j, std::size_t(port), "SYM", base + double(i));
        });
    }
    for (auto& th : threads) th.join();

    const int n = j.parent->count.load();
    EXPECT_EQ(n % 2, 0) << "a partial tuple escaped: " << n;
    EXPECT_GT(n, 0);
    EXPECT_LE(n, 2 * perThread);

    for (std::size_t i = 0; i + 1 < j.parent->received.size(); i += 2) {
        const double a = extractDouble(j.parent->received[i].value);
        const double b = extractDouble(j.parent->received[i + 1].value);
        ASSERT_LT(a, kPort1Base)
            << "tuple " << (i / 2) << " starts with an input-1 value (" << a
            << "): emission is not in declared port order";
        ASSERT_GE(b, kPort1Base)
            << "tuple " << (i / 2) << " ends with an input-0 value (" << b
            << "): a tuple was completed from one input twice";
    }
}

// ═══ 3. Ownership: fan-in owns ports, ports observe weakly ═══════════════════

// The upstream Listener holds its downstream WEAKLY, so if nobody owned the
// port the input edge would go dead the moment the builder returned. The fan-in
// is that owner.
TEST(AggregateTest, FanInOwnsItsPortsSoTheEdgeSurvivesTheBuilder) {
    auto parent = std::make_shared<TestParent>();
    auto agg    = std::make_shared<Aggregate>(2, parent);

    std::weak_ptr<INode> weakPort;
    {   // the builder's local shared_ptr, exactly as in TreeBuilder
        auto p0 = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(agg), 0);
        agg->addPort(p0);
        weakPort = p0;
    }   // local handle gone

    auto alive = weakPort.lock();
    ASSERT_NE(alive, nullptr)
        << "the port died with the builder's local handle — every value on "
           "input 0 would be dropped in silence";
    alive->onValue(StreamValue{"S", 1.0});

    auto p1 = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(agg), 1);
    agg->addPort(p1);
    p1->onValue(StreamValue{"S", 2.0});
    EXPECT_EQ(parent->count.load(), 2);
}

// The other direction. A shared_ptr from port back to fan-in would close an
// owner->port->owner cycle that no shutdown() can break, and every rejected
// then retried subscription would leak one (SPEC section 1.5).
TEST(AggregateTest, PortsDoNotKeepTheFanInAlive) {
    auto parent = std::make_shared<TestParent>();
    std::weak_ptr<Aggregate> weakAgg;
    std::shared_ptr<INode>   port;

    {
        auto agg = std::make_shared<Aggregate>(1, parent);
        weakAgg  = agg;
        port     = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(agg), 0);
        agg->addPort(port);
    }   // the ONLY owning handle to agg goes away here

    EXPECT_TRUE(weakAgg.expired())
        << "the Aggregate outlived its last owner — the port is holding it "
           "alive, which is an uncollectable reference cycle";

    // And a port whose owner is gone is inert, not a crash.
    EXPECT_NO_THROW(port->onValue(StreamValue{"S", 1.0}));
    EXPECT_EQ(parent->count.load(), 0);
}

TEST(AggregateTest, PortCarriesItsBuildTimeIndex) {
    auto parent = std::make_shared<TestParent>();
    auto agg    = std::make_shared<Aggregate>(3, parent);
    for (std::size_t i = 0; i < 3; ++i) {
        auto p = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(agg), i);
        EXPECT_EQ(p->index(), i);
    }
}

// ═══ 4. The plain pipeline edge is NOT an input ══════════════════════════════

// A fan-in's data comes from its own declared inputs. Feeding an upstream
// pipeline value into the buffer is exactly how the builder would manufacture
// defect 2 (see the `CompositeRoot` comment in src/core/TreeBuilder.cpp, and
// `ClockIsNeverAJoinMember` in tests/treebuilder/ComposedChainTest.cpp).
//
// THIS TEST'S FIRST DRAFT WAS FAKE, AND THE SHAPE OF THE MISTAKE IS WORTH
// KEEPING. It drove only the pipeline edge into an arity-2 join and asserted
// nothing was emitted. Mutating `Aggregate::onValue` to call
// `onPortValue(0, sv)` — i.e. making the pipeline edge a join member, the exact
// defect it claims to gate — SURVIVED it (ENC-1291 mutation M8): port 1 was
// still empty, so the mutated node emitted nothing either and the test stayed
// green. A test whose assertion holds for the same reason under both the fix
// and the defect is not a test.
//
// Both halves below are directional. Half one uses arity 1, where ANY value
// that reaches the buffer completes a tuple on the spot. Half two pre-fills the
// OTHER input, so a pipeline-edge value routed to ANY port completes the tuple
// immediately.
TEST(AggregateTest, AValueOnThePipelineEdgeIsNotAJoinMember) {
    {
        Join j1 = makeJoin(1);
        for (int n = 0; n < 8; ++n)
            j1.agg->onValue(StreamValue{"SYM", 100.0 + n});
        EXPECT_EQ(j1.parent->count.load(), 0)
            << "a one-input join emitted from values that arrived on the "
               "PIPELINE edge, not on its declared input";
        feed(j1, 0, "SYM", 1.0);
        EXPECT_EQ(j1.parent->count.load(), 1)
            << "control: the declared input must still work";
    }

    // Arity 3 with ALL BUT ONE port already filled: a pipeline value routed to
    // ANY index — 0, 1 or 2 — completes the tuple on the spot. The arity-1 case
    // above already covers `onPortValue(0)` and `onPortValue(arity_-1)`, which
    // coincide there; this covers every index in between as well, so no choice
    // of victim port survives.
    {
        Join j3 = makeJoin(3);
        feed(j3, 0, "SYM", 1.0);
        feed(j3, 1, "SYM", 2.0);
        for (int n = 0; n < 8; ++n)
            j3.agg->onValue(StreamValue{"SYM", 500.0 + n});
        EXPECT_EQ(j3.parent->count.load(), 0)
            << "a pipeline-edge value completed a three-input tuple whose "
               "input 2 never reported";
        feed(j3, 2, "SYM", 3.0);
        ASSERT_EQ(j3.parent->count.load(), 3);
        EXPECT_DOUBLE_EQ(extractDouble(j3.parent->received[0].value), 1.0);
        EXPECT_DOUBLE_EQ(extractDouble(j3.parent->received[1].value), 2.0);
        EXPECT_DOUBLE_EQ(extractDouble(j3.parent->received[2].value), 3.0);
    }

    Join j = makeJoin(2);
    feed(j, 1, "SYM", 99.0);        // input 1 has reported; only input 0 is open

    for (int n = 0; n < 8; ++n) j.agg->onValue(StreamValue{"SYM", 100.0 + n});
    EXPECT_EQ(j.parent->count.load(), 0)
        << "8 values on the pipeline edge completed a tuple against input 1's "
           "value. A fan-in joins its own declared `inputs` (SPEC D2); an "
           "upstream value is not one of them.";

    // And they leave no residue: the real tuple is still {input0, input1}.
    feed(j, 0, "SYM", 1.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 1.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 99.0);
}

TEST(AggregateTest, OutOfRangePortIndexIsIgnored) {
    Join j = makeJoin(2);
    auto rogue = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(j.agg), 7);
    rogue->onValue(StreamValue{"SYM", 1.0});
    feed(j, 0, "SYM", 2.0);
    feed(j, 1, "SYM", 3.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 2.0);
}

// ═══ 5. ENC-1292 — the DECLARED correlation key ══════════════════════════════
//
// SPEC specs/2026-09-20-krn-join-correctness D1, D6, section 1.1 defect 3, and
// section 5 Q3 (ruled) / Q6.
//
// Until ENC-1292 this node keyed its pending tuple on `sv.symbol` and nothing
// else, so no join across two streamKeys was possible anywhere in the engine.
// Measured on the real node: `Aggregate(2)` over AAPL+MSFT with one tick each
// emitted 0; after AAPL ticked a second time it emitted 2, both labelled AAPL
// and both carrying AAPL's own values. MSFT was never joined. 23 of the 52
// `Aggregate` requests in the checked-in corpus ask for exactly that join.
//
// The default is unchanged and is gated first, because D6's no-migration
// guarantee for stored forum graphs rests on it.

namespace {
// The same wiring as makeJoin(), with a declared join key. Kept separate so
// makeJoin()'s call sites above keep exercising the DEFAULT constructor —
// which is the thing D6 promises did not move.
Join makeJoinBy(std::size_t arity, JoinBy by, const char* outKey) {
    Join j;
    j.parent = std::make_shared<TestParent>();
    j.agg    = std::make_shared<Aggregate>(arity, j.parent, by, outKey);
    for (std::size_t i = 0; i < arity; ++i) {
        auto p = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(j.agg), i);
        j.agg->addPort(p);
        j.ports.push_back(p);
    }
    return j;
}
} // namespace

// The defect, pinned as a BASELINE rather than as correct behaviour. This is
// what `by:"streamKey"` means and it is right for a bid/ask join on one symbol;
// it is also exactly why a cross-symbol request needs `by:"none"`.
TEST(AggregateTest, ByStreamKeyIsTheDefaultAndNeverJoinsAcrossSymbols) {
    Join implicit = makeJoin(2);                                   // no `by`
    Join explicitDefault = makeJoinBy(2, JoinBy::StreamKey, "AAPL");

    for (Join* j : {&implicit, &explicitDefault}) {
        feed(*j, 0, "AAPL", 1000.0);
        feed(*j, 1, "MSFT", 5000.0);
        EXPECT_EQ(j->agg->by(), JoinBy::StreamKey);
        EXPECT_EQ(j->parent->count.load(), 0)
            << "under by:\"streamKey\" the two sides occupy two independent "
               "buffers and NEITHER completes. Emitting here would mean the "
               "default had started joining across symbols, which SPEC D6 "
               "forbids: every stored forum graph would change meaning with no "
               "migration.";
    }

    // And the half-open AAPL buffer completes with AAPL's own next value —
    // the pre-ENC-1292 behaviour, preserved byte for byte under the default.
    feed(implicit, 1, "AAPL", 1000.02);
    ASSERT_EQ(implicit.parent->count.load(), 2);
    EXPECT_EQ(implicit.parent->received[0].symbol, "AAPL");
    EXPECT_EQ(implicit.parent->received[1].symbol, "AAPL");
}

// THE gate for SPEC section 1.1 defect 3 at unit level.
TEST(AggregateTest, ByNoneJoinsAcrossTwoStreamKeys) {
    Join j = makeJoinBy(2, JoinBy::None, "AAPL");
    ASSERT_EQ(j.agg->by(), JoinBy::None);

    feed(j, 0, "AAPL", 1000.0);
    EXPECT_EQ(j.parent->count.load(), 0) << "one side is not a tuple";

    feed(j, 1, "MSFT", 5000.0);
    ASSERT_EQ(j.parent->count.load(), 2)
        << "AAPL on port 0 and MSFT on port 1 are a COMPLETE tuple under "
           "by:\"none\" — this is the join 23 of the 52 corpus Aggregate "
           "requests ask for and the engine could not express at all.";

    // Declared port order, not arrival order, and the values are the two
    // sides' OWN values — not one side paired with itself.
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 1000.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 5000.0);

    // SPEC section 5 Q6 — OUTPUT IDENTITY. `ClientSession` serialises
    // `sv.symbol` onto the wire, so without the substitution this tuple would
    // be labelled by whichever side happened to arrive second: MSFT here, AAPL
    // had the ticks interleaved the other way. That is a race, not an
    // identity. It must be the request's own top-level streamKey.
    EXPECT_EQ(j.parent->received[0].symbol, "AAPL");
    EXPECT_EQ(j.parent->received[1].symbol, "AAPL")
        << "the joined stream must carry ONE stable identity. `sv.symbol` "
           "here is MSFT (the side that released the tuple); reporting it "
           "would make the output symbol depend on arrival order.";

    // The barrier resets: the next tuple needs BOTH sides again.
    feed(j, 0, "AAPL", 1001.0);
    EXPECT_EQ(j.parent->count.load(), 2) << "half a tuple is not a tuple";
    feed(j, 1, "MSFT", 5001.0);
    ASSERT_EQ(j.parent->count.load(), 4);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[2].value), 1001.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[3].value), 5001.0);
}

// Order-independence: the identity of the tuple must not depend on which side
// arrives second. Drive the SAME two symbols with the arrival order reversed
// and demand the same output symbol — the direct negative control on Q6.
TEST(AggregateTest, ByNoneOutputIdentityDoesNotDependOnArrivalOrder) {
    Join a = makeJoinBy(2, JoinBy::None, "AAPL");
    feed(a, 0, "AAPL", 1000.0);
    feed(a, 1, "MSFT", 5000.0);          // MSFT releases the tuple

    Join b = makeJoinBy(2, JoinBy::None, "AAPL");
    feed(b, 1, "MSFT", 5000.0);
    feed(b, 0, "AAPL", 1000.0);          // AAPL releases the tuple

    ASSERT_EQ(a.parent->count.load(), 2);
    ASSERT_EQ(b.parent->count.load(), 2);
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(a.parent->received[i].symbol, "AAPL");
        EXPECT_EQ(b.parent->received[i].symbol, "AAPL");
        EXPECT_DOUBLE_EQ(extractDouble(a.parent->received[i].value),
                         extractDouble(b.parent->received[i].value))
            << "declared port order must survive a reversed arrival order";
    }
}

// Under `by:"none"` a tuple is shared across symbols, so last-value-wins is
// across symbols too: a second AAPL tick before MSFT reports replaces the
// first. Stated because it is a consequence of the key, not of the port.
TEST(AggregateTest, ByNoneIsLastValueWinsPerPortAcrossSymbols) {
    Join j = makeJoinBy(2, JoinBy::None, "AAPL");
    feed(j, 0, "AAPL", 1000.0);
    feed(j, 0, "AAPL", 1001.0);          // same port, replaces
    feed(j, 1, "MSFT", 5000.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 1001.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 5000.0);
}

// SPEC section 5 Q6, the other half: a join that ignores the symbol has no
// identity unless it is given one, and emitting under the empty symbol would
// be a silent wrong answer on the wire. The builder guards this too; this is
// the node's own guard, for every other construction path.
TEST(AggregateTest, ByNoneWithNoOutputStreamKeyIsRefused) {
    auto parent = std::make_shared<TestParent>();
    EXPECT_THROW(Aggregate(2, parent, JoinBy::None, ""), std::invalid_argument);
    EXPECT_THROW(Aggregate(2, parent, JoinBy::None), std::invalid_argument);

    // ...and the default mode does NOT require one, because each symbol keeps
    // its own identity there.
    EXPECT_NO_THROW(Aggregate(2, parent, JoinBy::StreamKey, ""));
    EXPECT_NO_THROW(Aggregate(2, parent));
}

// The pipeline edge stays closed under `by:"none"` too. ENC-1291 made a value
// arriving there a warn-and-drop; a new correlation key must not reopen it,
// because with one shared tuple a stray value would complete tuples for every
// symbol at once rather than for one.
TEST(AggregateTest, ByNonePipelineEdgeValueIsStillNotAJoinMember) {
    Join j = makeJoinBy(2, JoinBy::None, "AAPL");
    feed(j, 0, "AAPL", 1000.0);
    for (int n = 0; n < 8; ++n)
        j.agg->onValue(StreamValue{"MSFT", 5000.0 + n});
    EXPECT_EQ(j.parent->count.load(), 0)
        << "8 pipeline-edge values completed a tuple against port 0 — a "
           "fan-in joins its own declared inputs (SPEC D2), and by:\"none\" "
           "does not change that.";
    feed(j, 1, "MSFT", 5000.0);
    ASSERT_EQ(j.parent->count.load(), 2);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 5000.0);
}

// The per-symbol buffer cap. Nothing covered it before ENC-1292, and ENC-1292
// rewrote the lookup that enforces it (find+emplace -> try_emplace, to remove
// an equivalent-mutation surface), so it is gated here rather than left to be
// trusted. It can only trip under `by:"streamKey"` — `by:"none"` has exactly
// one buffer entry by construction.
TEST(AggregateTest, MaxSymbolsCapDropsNewSymbolsAndSparesExistingOnes) {
    Join j = makeJoin(2);

    constexpr int kCap = 10000;      // Aggregate::MAX_SYMBOLS
    // Fill the map to the cap with half-open tuples (port 0 only, so nothing
    // has emitted yet).
    for (int n = 0; n < kCap; ++n)
        feed(j, 0, ("S" + std::to_string(n)).c_str(), double(n));
    ASSERT_EQ(j.parent->count.load(), 0) << "no tuple is complete yet";

    // One more DISTINCT symbol is over the cap: both its ports are fed and it
    // must still emit nothing, because it never got a buffer.
    feed(j, 0, "OVERFLOW", 1.0);
    feed(j, 1, "OVERFLOW", 2.0);
    EXPECT_EQ(j.parent->count.load(), 0)
        << "a symbol past the " << kCap << "-entry cap was admitted; the cap "
           "is what bounds this node's memory under an unbounded symbol space";

    // ...and an ALREADY-BUFFERED symbol still completes. A cap that also broke
    // the symbols it already accepted would be a far worse bug than the leak
    // it prevents, and `size() >= cap` vs `size() > cap` is exactly the
    // off-by-one that causes it.
    feed(j, 1, "S0", 99.0);
    ASSERT_EQ(j.parent->count.load(), 2)
        << "an existing buffered symbol must still complete its tuple";
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[0].value), 0.0);
    EXPECT_DOUBLE_EQ(extractDouble(j.parent->received[1].value), 99.0);
}
