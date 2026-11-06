#include "krn/nodes/Pack.hpp"
#include "krn/StreamValue.hpp"
#include "krn/nodes/INode.hpp"
#include <gtest/gtest.h>
#include <stdexcept>
#include <memory>
#include <string>
#include <vector>

using namespace krn;

namespace {

class Sink : public INode {
public:
  std::vector<StreamValue> received;
  void onValue(const StreamValue& sv) override { received.push_back(sv); }
  void shutdown() noexcept override {}
};

// Build a Pack with `names` fields wired to fresh ports; returns the ports so
// the test can drive each field independently.
std::shared_ptr<Pack> makePack(const std::vector<std::string>& names,
                               std::shared_ptr<INode> sink,
                               std::vector<std::shared_ptr<INode>>& ports) {
  auto pack = std::make_shared<Pack>(names, std::move(sink));
  for (std::size_t i = 0; i < names.size(); ++i) {
    auto p = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(pack), i);
    pack->addPort(p);
    ports.push_back(p);
  }
  return pack;
}

double field(const StreamValue& sv, const char* key) {
  const Record& r = std::get<Record>(sv.value);
  const ArgType* f = recordFind(r, key);
  return std::get<double>(*f);
}

} // namespace

TEST(PackTest, EmitsOnlyWhenAllFieldsSeen) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePack({"a", "b"}, sink, ports);

  ports[0]->onValue(StreamValue{"S", 1.0});
  EXPECT_TRUE(sink->received.empty());        // incomplete

  ports[1]->onValue(StreamValue{"S", 2.0});
  ASSERT_EQ(sink->received.size(), 1u);        // now complete
  EXPECT_EQ(sink->received[0].symbol, "S");
  EXPECT_DOUBLE_EQ(field(sink->received[0], "a"), 1.0);
  EXPECT_DOUBLE_EQ(field(sink->received[0], "b"), 2.0);
}

TEST(PackTest, ReEmitsLatestOnSubsequentUpdate) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePack({"a", "b"}, sink, ports);

  ports[0]->onValue(StreamValue{"S", 1.0});
  ports[1]->onValue(StreamValue{"S", 2.0});   // emit {a:1,b:2}
  ports[0]->onValue(StreamValue{"S", 5.0});   // emit {a:5,b:2}

  ASSERT_EQ(sink->received.size(), 2u);
  EXPECT_DOUBLE_EQ(field(sink->received[1], "a"), 5.0);
  EXPECT_DOUBLE_EQ(field(sink->received[1], "b"), 2.0);
}

TEST(PackTest, FieldOrderFollowsDeclaration) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePack({"o", "h", "l", "c"}, sink, ports);

  ports[3]->onValue(StreamValue{"S", 4.0});
  ports[1]->onValue(StreamValue{"S", 2.0});
  ports[0]->onValue(StreamValue{"S", 1.0});
  ports[2]->onValue(StreamValue{"S", 3.0});   // completes

  ASSERT_EQ(sink->received.size(), 1u);
  const Record& r = std::get<Record>(sink->received[0].value);
  ASSERT_EQ(r.fields.size(), 4u);
  EXPECT_EQ(r.fields[0].name, "o");
  EXPECT_EQ(r.fields[1].name, "h");
  EXPECT_EQ(r.fields[2].name, "l");
  EXPECT_EQ(r.fields[3].name, "c");
}

TEST(PackTest, SymbolsAreIndependent) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePack({"a", "b"}, sink, ports);

  ports[0]->onValue(StreamValue{"X", 1.0});
  ports[0]->onValue(StreamValue{"Y", 9.0});
  EXPECT_TRUE(sink->received.empty());          // neither symbol complete

  ports[1]->onValue(StreamValue{"Y", 8.0});     // only Y completes
  ASSERT_EQ(sink->received.size(), 1u);
  EXPECT_EQ(sink->received[0].symbol, "Y");
  EXPECT_DOUBLE_EQ(field(sink->received[0], "a"), 9.0);
  EXPECT_DOUBLE_EQ(field(sink->received[0], "b"), 8.0);
}

TEST(PackTest, DropsAfterShutdown) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePack({"a", "b"}, sink, ports);

  ports[0]->onValue(StreamValue{"S", 1.0});
  pack->shutdown();
  ports[1]->onValue(StreamValue{"S", 2.0});     // dropped — no emit
  EXPECT_TRUE(sink->received.empty());
}

TEST(PackTest, PortDoesNotKeepPackAlive) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  std::weak_ptr<Pack> weak;
  {
    auto pack = makePack({"a"}, sink, ports);
    weak = pack;
    EXPECT_FALSE(weak.expired());
  }
  // Pack dropped; the port's weak_ptr must not have kept it alive.
  EXPECT_TRUE(weak.expired());
  // Calling a now-orphaned port is a safe no-op.
  ports[0]->onValue(StreamValue{"S", 1.0});
  EXPECT_TRUE(sink->received.empty());
}

// ═══ ENC-1292 — the DECLARED correlation key ═════════════════════════════════
//
// SPEC specs/2026-09-20-krn-join-correctness D1, section 1.1 defect 3, Q6.
// `Pack{a:AAPL, b:MSFT}` with 6 ticks on each side emitted 0 records, forever,
// silently — measured on the real node. It keys on `sv.symbol` exactly as
// `Aggregate` did, so it gets exactly the same declared key.

namespace {
std::shared_ptr<Pack> makePackBy(const std::vector<std::string>& names,
                                 std::shared_ptr<INode> sink,
                                 std::vector<std::shared_ptr<INode>>& ports,
                                 JoinBy by, const char* outKey) {
  auto pack = std::make_shared<Pack>(names, std::move(sink), by, outKey);
  for (std::size_t i = 0; i < names.size(); ++i) {
    auto p = std::make_shared<InputPort>(std::weak_ptr<IFanIn>(pack), i);
    pack->addPort(p);
    ports.push_back(p);
  }
  return pack;
}
} // namespace

TEST(PackTest, ByStreamKeyIsTheDefaultAndNeverAssemblesAcrossSymbols) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePackBy({"a", "b"}, sink, ports, JoinBy::StreamKey, "AAPL");
  EXPECT_EQ(pack->by(), JoinBy::StreamKey);

  ports[0]->onValue(StreamValue{"AAPL", 1000.0});
  ports[1]->onValue(StreamValue{"MSFT", 5000.0});
  EXPECT_TRUE(sink->received.empty())
      << "under the default the two symbols hold two independent states and "
         "neither completes — the pre-ENC-1292 behaviour D6 preserves";
}

TEST(PackTest, ByNoneAssemblesOneRecordAcrossTwoStreamKeys) {
  auto sink = std::make_shared<Sink>();
  std::vector<std::shared_ptr<INode>> ports;
  auto pack = makePackBy({"a", "b"}, sink, ports, JoinBy::None, "AAPL");
  ASSERT_EQ(pack->by(), JoinBy::None);

  ports[0]->onValue(StreamValue{"AAPL", 1000.0});
  EXPECT_TRUE(sink->received.empty());

  ports[1]->onValue(StreamValue{"MSFT", 5000.0});
  ASSERT_EQ(sink->received.size(), 1u)
      << "Pack{a:AAPL, b:MSFT} emitted nothing — the defect this clears";
  EXPECT_DOUBLE_EQ(field(sink->received[0], "a"), 1000.0);
  EXPECT_DOUBLE_EQ(field(sink->received[0], "b"), 5000.0);
  // SPEC Q6: one stable identity, not the symbol that happened to complete it.
  EXPECT_EQ(sink->received[0].symbol, "AAPL");
}

TEST(PackTest, ByNoneWithNoOutputStreamKeyIsRefused) {
  auto sink = std::make_shared<Sink>();
  EXPECT_THROW(Pack({"a"}, sink, JoinBy::None, ""), std::invalid_argument);
  EXPECT_NO_THROW(Pack({"a"}, sink, JoinBy::StreamKey, ""));
}
