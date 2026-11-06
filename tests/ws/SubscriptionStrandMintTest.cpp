// tests/ws/SubscriptionStrandMintTest.cpp
//
// ENC-1338 — SPEC specs/2026-09-20-krn-join-correctness D3, D4.
// "One `asio::strand` minted per subscription in `ClientSession::handleSubscribe`."
//
// THE GAP THIS CLOSES. ENC-1005 implemented D3 and mutation-tested its own work
// ten ways. It published the two mutations that reddened NOTHING:
//
//     M7   delete `ClientSession::handleSubscribe`'s strand mint  -> no test red
//     M10  drop that mint's null-pool guard                       -> no test red
//
// Both cover the PRODUCTION mint site — D3's named site, the one every real
// subscription goes through. `tree::buildForRequest`'s backstop mint IS covered
// (it is why `CorpusTest` went green without a line of its drive being touched),
// and that coverage is precisely what concealed the gap: delete the production
// line and the backstop silently mints a replacement, so every assertion in the
// suite stays true while the production path has lost its own guarantee. The
// race gate ENC-1005 had just turned green would have stayed green.
//
// WHY THESE TESTS AND NOT A BEHAVIOURAL ONE. There is no behavioural difference
// to find. The backstop exists so a caller supplying no strand still gets one,
// and it is guarded on the same `pool` the session site is, so the two paths
// produce a strand over the same pool under exactly the same condition. From
// outside the engine they are indistinguishable BY CONSTRUCTION — which is why
// ENC-1005 concluded the line was "documentation rather than load-bearing code".
//
// So the observable had to be built. `rt::Strand` now carries the mint site that
// created it (`origin()`), stamped through `rt::Strand::Attribution` — a token
// whose constructor is private and whose only friend is
// `krn::server::SubscriptionStrandMint`. An attributed strand therefore cannot
// be constructed anywhere else in the engine, so reading that tag off the
// `Listener` of a subscription driven through a REAL WebSocket session answers
// the exact question M7 asks: is the strand this DAG was given the one the
// session minted, or the one the backstop substituted?
//
// THE FIRST DRAFT OF THAT WAS A PLAIN STRING AND IT DID NOT HOLD. The tag was a
// `const char*` with the literal hidden in an anonymous namespace. An
// adversarial review broke it two ways: the literal is public through
// `SubscriptionStrandMint::origin()`, and an anonymous-namespace member is
// visible through its enclosing namespace name for the rest of the translation
// unit — so `handleSubscribe` itself could reach it. An unguarded mint inlined
// at the call site that simply kept the tag passed all five tests. The
// capability token is what makes that a compile error instead.
//
// WHAT THESE TESTS DO AND DO NOT CLAIM. Tests 1 and 2 assert the strand the DAG
// was GIVEN and that the DAG's values were delivered THROUGH it (`tasksRun()`).
// They do not assert that ordering holds — that is ENC-1005's
// `tests/integration/OrderedDeliveryTest.cpp` and `Corpus86_SpreadIsExactlyTwoCents`,
// and those are what redden if `Listener::onValue` stops using the strand it
// holds. The two suites are complementary; neither subsumes the other.
//
// NOTHING IS STUBBED on the path under test. `SessionHarness` boots a real
// `WebSocketServer` on 127.0.0.1:0, a real client connects over Beast, and the
// subscription is built by the real `ClientSession::handleSubscribe` through the
// real `tree::buildForRequest`. The strand is read back off the real `Listener`
// the real `Dispatcher` holds.
//
// MUTATION RESULTS (ENC-1338; one mutation at a time, control re-run between):
//
//   M7   delete the mint at the call site          -> tests 1, 2 RED
//   M7b  make the mint return nullptr              -> tests 1, 2, 4 RED
//   M10  drop the guard inside the mint            -> test 3 RED
//   M10b unguarded mint inlined at the call site   -> tests 1, 2 RED (untagged)
//                                                  -> DOES NOT COMPILE (tagged)
//
// RELATED, DELIBERATELY NOT TESTED HERE. Ordered delivery additionally requires
// single-threaded ingress per symbol, which `Dispatcher::onTick`'s contract
// expressly does not promise (ENC-1005's HIGH finding; ruled under ENC-1337).
// Nothing below depends on ingress concurrency: every tick here is driven from
// one thread, so these tests neither assert nor contradict that ruling.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <rapidjson/document.h>

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/ExecutionContext.hpp"
#include "krn/nodes/Listener.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/server/ClientSession.hpp"
#include "krn/server/WebSocketServer.hpp"

namespace beast = boost::beast;
namespace asio  = boost::asio;
namespace ws    = beast::websocket;
using tcp       = asio::ip::tcp;

namespace {

// ─── A real server, a real socket, a real ClientSession ────────────────────
//
// `poolForExec` is the pool the ExecutionContext hands to `handleSubscribe` as
// `Deps::pool`, i.e. the one the mint's guard tests. It is passed separately
// from the Dispatcher's pool so the null-pool case can be driven with an
// otherwise completely live engine.
struct SessionHarness {
  asio::io_context ioc;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> work;
  std::unique_ptr<krn::rt::ThreadPool>   pool;
  std::unique_ptr<krn::AtomicStore>      store;
  std::unique_ptr<krn::Dispatcher>       dispatcher;
  std::unique_ptr<krn::ExecutionContext> exec;
  std::unique_ptr<krn::WebSocketServer>  server;
  std::thread                            ioThread;

  explicit SessionHarness(bool execHasPool = true) {
    pool       = std::make_unique<krn::rt::ThreadPool>(4);
    store      = std::make_unique<krn::AtomicStore>();
    dispatcher = std::make_unique<krn::Dispatcher>(pool.get(), store.get());
    exec       = std::make_unique<krn::ExecutionContext>(
                   store.get(), execHasPool ? pool.get() : nullptr);
    server     = std::make_unique<krn::WebSocketServer>(ioc, exec.get(),
                                                        dispatcher.get(), 0);
    server->run();
    work = std::make_unique<
             asio::executor_work_guard<asio::io_context::executor_type>>(
               ioc.get_executor());
    ioThread = std::thread([this] { ioc.run(); });
  }

  ~SessionHarness() {
    try { server->stopAccept(); } catch (...) {}
    try { server->closeAll();   } catch (...) {}
    if (work) work.reset();
    ioc.stop();
    if (ioThread.joinable()) ioThread.join();
  }

  unsigned short port() const { return server->port(); }
};

ws::stream<tcp::socket> connectClient(asio::io_context& clientIoc,
                                      unsigned short port) {
  tcp::resolver resolver(clientIoc);
  auto endpoints = resolver.resolve("127.0.0.1", std::to_string(port));
  ws::stream<tcp::socket> stream(clientIoc);
  asio::connect(stream.next_layer(), endpoints);
  stream.handshake("127.0.0.1", "/");
  return stream;
}

// Read frames under a single watchdog, returning the first whose "type" matches,
// or "" on timeout. Single-threaded bounded read loop — no second thread ever
// touches the socket, so there is no read/close race (the pattern
// ClientSessionTest.cpp uses, and for the same reason).
std::string readUntilType(ws::stream<tcp::socket>& stream,
                          const char* wantType,
                          std::chrono::milliseconds budget) {
  auto& ioc = static_cast<asio::io_context&>(stream.get_executor().context());
  const auto deadline = std::chrono::steady_clock::now() + budget;
  std::string out;
  for (;;) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) break;
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    beast::flat_buffer buf;
    std::string frame;
    bool completed = false, ok = false;
    stream.async_read(buf, [&](beast::error_code ec, std::size_t) {
      completed = true;
      if (!ec) { frame = beast::buffers_to_string(buf.data()); ok = true; }
    });
    ioc.restart();
    ioc.run_for(remaining);
    if (!completed) {
      beast::error_code ec;
      stream.next_layer().cancel(ec);
      ioc.restart();
      ioc.run_for(std::chrono::milliseconds(200));
      break;
    }
    if (!ok) break;
    rapidjson::Document d;
    d.Parse(frame.c_str());
    if (!d.HasParseError() && d.IsObject() && d.HasMember("type") &&
        d["type"].IsString() &&
        std::string(d["type"].GetString()) == wantType) {
      out = std::move(frame);
      break;
    }
  }
  return out;
}

// The FIRST Listener the live session registered on (symbol, field), or null.
// For the single-field request these tests drive there is exactly one, so it is
// the head; a request whose join inputs listened on the same (symbol, field)
// would make "first" and "head" different things.
std::shared_ptr<krn::nodes::Listener> listenerOn(krn::Dispatcher& d,
                                                   const std::string& symbol,
                                                   const std::string& field) {
  auto nodes = d.listenersFor(symbol, field);
  for (const auto& n : nodes) {
    if (auto l = std::dynamic_pointer_cast<krn::nodes::Listener>(n)) return l;
  }
  return nullptr;
}

constexpr const char* kSubscribeAapl =
    R"({"type":"subscribe","requests":[)"
    R"({"key":1,"streamKey":"AAPL","field":"lastPrice"}]})";

} // namespace

// ───────────────────────────────────────────────────────────────────────────
// 1. THE CALL. A subscription made through a live WebSocket session runs on a
//    strand minted by `ClientSession::handleSubscribe` — not on one the
//    `buildForRequest` backstop substituted.
//
//    This is the test ENC-1005's M7 had nothing to redden. Deleting the mint
//    leaves the DAG on a backstop strand, whose origin is `unattributed`.
//
//    The delivery half is not decoration: without it this would assert the
//    provenance of a strand belonging to a DAG that might never deliver
//    anything, which is the shape of a test that cannot fail.
// ───────────────────────────────────────────────────────────────────────────
TEST(SubscriptionStrandMint, ProductionSubscribeMintsItsOwnStrandAtTheSessionSite) {
  SessionHarness srv;
  asio::io_context clientIoc;
  auto stream = connectClient(clientIoc, srv.port());

  stream.write(asio::buffer(std::string(kSubscribeAapl)));
  const auto ack = readUntilType(stream, "subscribed", std::chrono::seconds(3));
  ASSERT_FALSE(ack.empty()) << "no 'subscribed' ack — the DAG was never built";

  auto listener = listenerOn(*srv.dispatcher, "AAPL", "lastPrice");
  ASSERT_TRUE(listener) << "the live session registered no head Listener for "
                           "(AAPL, lastPrice)";

  const auto strand = listener->strand();
  ASSERT_TRUE(strand) << "the DAG a live subscription built has NO strand at "
                         "all — D3 is not in force on the production path";

  // The assertion M7 exists to break. The tag is spelled in exactly one place
  // in the engine (file-local to ClientSession.cpp), so nothing else can
  // produce it: the backstop's strand reads `unattributed`, and so does a mint
  // inlined at the call site.
  EXPECT_STREQ(strand->origin(), krn::server::SubscriptionStrandMint::origin())
      << "the strand this subscription is running on was NOT minted by "
         "ClientSession::handleSubscribe. `"
      << krn::rt::Strand::kUnattributedOrigin
      << "` means tree::buildForRequest's backstop silently supplied one — "
         "which is ENC-1005's M7, and the production mint is gone.";

  // ... and the DAG built on that strand is live, and its values went THROUGH
  // that strand. `tasksRun()` is the difference between "the DAG was handed
  // this strand" and "the DAG is running on it": a Listener can hold a strand,
  // answer `strand()` with it, and still deliver down the unordered pool path.
  const auto ranBefore = strand->tasksRun();
  srv.dispatcher->notifyListeners("AAPL", "lastPrice", 101.25);
  const auto update = readUntilType(stream, "update", std::chrono::seconds(3));
  ASSERT_FALSE(update.empty())
      << "no 'update' frame — the origin assertion above would have been made "
         "about a DAG that delivers nothing";
  EXPECT_GT(strand->tasksRun(), ranBefore)
      << "the value was delivered, but NOT through the strand the session "
         "minted — the Listener holds it and bypasses it";

  rapidjson::Document d;
  d.Parse(update.c_str());
  ASSERT_FALSE(d.HasParseError());
  ASSERT_TRUE(d.HasMember("value") && d["value"].IsNumber());
  EXPECT_DOUBLE_EQ(d["value"].GetDouble(), 101.25);

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ───────────────────────────────────────────────────────────────────────────
// 2. Same claim, held across a re-subscribe on the same request key. ENC-1005
//    reasoned that a replaced subscription gets a FRESH strand (so a straggler
//    from the shut-down chain shares nothing with the new one) and recorded
//    under "what I could NOT verify" that this was never driven through a live
//    WS session. It is now: both the freshness and the mint site.
// ───────────────────────────────────────────────────────────────────────────
TEST(SubscriptionStrandMint, AReSubscribeGetsAFreshSessionMintedStrand) {
  SessionHarness srv;
  asio::io_context clientIoc;
  auto stream = connectClient(clientIoc, srv.port());

  stream.write(asio::buffer(std::string(kSubscribeAapl)));
  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(3)).empty());
  auto first = listenerOn(*srv.dispatcher, "AAPL", "lastPrice");
  ASSERT_TRUE(first);
  const auto firstStrand = first->strand();
  ASSERT_TRUE(firstStrand);
  EXPECT_STREQ(firstStrand->origin(), krn::server::SubscriptionStrandMint::origin());

  stream.write(asio::buffer(std::string(kSubscribeAapl)));
  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(3)).empty());

  // The replacement DAG's Listener is a different object on a different strand,
  // and that strand is the SESSION's, not a backstop's.
  bool sawFresh = false;
  for (const auto& n : srv.dispatcher->listenersFor("AAPL", "lastPrice")) {
    auto l = std::dynamic_pointer_cast<krn::nodes::Listener>(n);
    if (!l || l == first) continue;
    ASSERT_TRUE(l->strand());
    EXPECT_NE(l->strand(), firstStrand)
        << "the re-subscribed DAG shares the shut-down DAG's strand";
    EXPECT_STREQ(l->strand()->origin(), krn::server::SubscriptionStrandMint::origin())
        << "the re-subscribed DAG's strand did not come from the session site";
    sawFresh = true;
  }
  EXPECT_TRUE(sawFresh) << "the re-subscribe registered no new Listener";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ───────────────────────────────────────────────────────────────────────────
// 3. THE GUARD (ENC-1005's M10). With no executor there is nothing to
//    serialise, and a pool-less Strand would make the `Listener` answer
//    `deliversOnOwnExecutor()` — so `Dispatcher` would go inline too and the
//    whole DAG compute would run on the WebSocket read thread.
//
//    This is asserted on the helper rather than end to end BECAUSE IT CANNOT BE
//    ASSERTED END TO END: `buildForRequest` throws "missing dispatcher/pool"
//    before building anything, so a pool-less subscription is rejected with or
//    without the guard and the two are indistinguishable from the wire. Test 4
//    pins that rejection so this limitation is recorded in the suite and not
//    only in a comment.
// ───────────────────────────────────────────────────────────────────────────
TEST(SubscriptionStrandMint, TheMintReturnsNullForAPoollessExecutor) {
  EXPECT_EQ(krn::server::SubscriptionStrandMint::mint(nullptr), nullptr)
      << "the production mint built a Strand over a null pool — ENC-1005's M10. "
         "A pool-less Strand runs inline on its caller AND makes the Listener "
         "claim its own executor, putting the entire DAG compute on the "
         "WebSocket read thread.";
}

// ───────────────────────────────────────────────────────────────────────────
// 4. The mint produces a WORKING strand, not merely a non-null pointer. Without
//    this, a helper mutated to `return nullptr` (M7 applied inside the helper)
//    would still leave test 3 green, and "the guard is present" would be the
//    only thing under test.
// ───────────────────────────────────────────────────────────────────────────
TEST(SubscriptionStrandMint, TheMintReturnsAWorkingStrandTaggedWithItsSite) {
  krn::rt::ThreadPool pool(4);
  auto strand = krn::server::SubscriptionStrandMint::mint(&pool);
  ASSERT_TRUE(strand) << "the production mint produced NO strand for a real "
                         "pool — ENC-1005's M7 applied inside the helper";
  EXPECT_STREQ(strand->origin(), krn::server::SubscriptionStrandMint::origin());

  // It is a strand: FIFO, and never two tasks at once.
  constexpr int kTasks = 200;
  std::mutex mx;
  std::vector<int> order;
  std::atomic<int> inFlight{0};
  std::atomic<int> maxInFlight{0};
  std::atomic<int> done{0};

  for (int i = 0; i < kTasks; ++i) {
    strand->post([&, i] {
      const int now = ++inFlight;
      int prev = maxInFlight.load(std::memory_order_relaxed);
      while (now > prev && !maxInFlight.compare_exchange_weak(
                               prev, now, std::memory_order_relaxed)) {}
      {
        std::lock_guard<std::mutex> lk(mx);
        order.push_back(i);
      }
      --inFlight;
      ++done;
    });
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (done.load() < kTasks && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_EQ(done.load(), kTasks) << "the minted strand did not run its queue";

  EXPECT_EQ(maxInFlight.load(), 1)
      << "two tasks of one subscription's strand ran at once";
  std::lock_guard<std::mutex> lk(mx);
  ASSERT_EQ(order.size(), static_cast<std::size_t>(kTasks));
  for (int i = 0; i < kTasks; ++i)
    ASSERT_EQ(order[static_cast<std::size_t>(i)], i)
        << "the minted strand ran its queue out of post order at " << i;
}

// ───────────────────────────────────────────────────────────────────────────
// 5. The production consequence of test 3's guard, driven end to end, and the
//    record of why test 3 has to live at the helper. A session whose
//    ExecutionContext has no pool rejects the subscription at build time and
//    registers no Listener — so there is no DAG for a pool-less strand to
//    attach itself to, which is why unguarding the mint is invisible from the
//    wire and had to be gated one level in.
// ───────────────────────────────────────────────────────────────────────────
TEST(SubscriptionStrandMint, APoollessExecutorRejectsTheSubscriptionAndBuildsNoDag) {
  SessionHarness srv(/*execHasPool=*/false);
  asio::io_context clientIoc;
  auto stream = connectClient(clientIoc, srv.port());

  stream.write(asio::buffer(std::string(kSubscribeAapl)));
  const auto frame = readUntilType(stream, "error", std::chrono::seconds(3));
  ASSERT_FALSE(frame.empty()) << "no error frame for a pool-less subscription";

  rapidjson::Document d;
  d.Parse(frame.c_str());
  ASSERT_FALSE(d.HasParseError());
  ASSERT_TRUE(d.HasMember("where") && d["where"].IsString());
  EXPECT_STREQ(d["where"].GetString(), "build");
  // The MESSAGE matters, not just that something rejected it: this test's job
  // is to pin WHY test 3 has to live at the mint, and that reason is
  // `buildForRequest`'s pool check (TreeBuilder.cpp) firing before anything is
  // constructed. Any other build rejection would leave the claim unproven.
  ASSERT_TRUE(d.HasMember("message") && d["message"].IsString());
  EXPECT_NE(std::string(d["message"].GetString()).find("missing dispatcher/pool"),
            std::string::npos)
      << "rejected for a different reason: " << d["message"].GetString();

  EXPECT_TRUE(srv.dispatcher->listenersFor("AAPL", "lastPrice").empty())
      << "a rejected subscription left a Listener registered";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}
