// ENC-48: live-socket coverage for ClientSession failure paths. Boots a
// real WebSocketServer on 127.0.0.1:0 and sends malformed / oversize /
// oversubscribed payloads through Beast, asserting the server emits the
// expected error frames.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/ExecutionContext.hpp"
#include "krn/FunctionRegistry.hpp"
#include "krn/NodeRegistry.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/server/WebSocketServer.hpp"
#include "../support/ScopedPipelineOnly.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstdint>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace beast = boost::beast;
namespace asio  = boost::asio;
namespace ws    = beast::websocket;
using tcp       = asio::ip::tcp;

namespace {

struct ServerHarness {
  asio::io_context                    ioc;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> work;
  std::unique_ptr<krn::rt::ThreadPool> pool;
  std::unique_ptr<krn::AtomicStore>    store;
  std::unique_ptr<krn::Dispatcher>     dispatcher;
  std::unique_ptr<krn::ExecutionContext> exec;
  std::unique_ptr<krn::WebSocketServer>  server;
  std::thread                            ioThread;

  ServerHarness() {
    pool       = std::make_unique<krn::rt::ThreadPool>(1);
    store      = std::make_unique<krn::AtomicStore>();
    dispatcher = std::make_unique<krn::Dispatcher>(pool.get(), store.get());
    exec       = std::make_unique<krn::ExecutionContext>(store.get(), pool.get());
    server     = std::make_unique<krn::WebSocketServer>(ioc, exec.get(), dispatcher.get(), 0);
    server->run();
    work     = std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(
                 ioc.get_executor());
    ioThread = std::thread([this]{ ioc.run(); });
  }

  ~ServerHarness() {
    try { server->stopAccept(); } catch (...) {}
    try { server->closeAll();   } catch (...) {}
    if (work) work.reset();
    ioc.stop();
    if (ioThread.joinable()) ioThread.join();
  }

  unsigned short port() const { return server->port(); }
};

// Connect to the harness' WS endpoint. Returns a connected stream.
ws::stream<tcp::socket> connect(asio::io_context& clientIoc, unsigned short port) {
  tcp::resolver resolver(clientIoc);
  auto endpoints = resolver.resolve("127.0.0.1", std::to_string(port));
  ws::stream<tcp::socket> stream(clientIoc);
  asio::connect(stream.next_layer(), endpoints);
  stream.handshake("127.0.0.1", "/");
  return stream;
}

// Read one frame with a bounded wall-clock timeout. Returns empty on timeout.
// Driven entirely on THIS thread via run_for — no second thread touches the
// socket, so there is no read/close data race (TSan-clean). The client
// io_context is otherwise idle (only the server harness runs its own).
std::string readFrameBounded(ws::stream<tcp::socket>& stream,
                             std::chrono::milliseconds timeout) {
  auto& ioc = static_cast<asio::io_context&>(stream.get_executor().context());
  beast::flat_buffer buf;
  std::string out;
  bool completed = false;
  stream.async_read(buf, [&](beast::error_code ec, std::size_t) {
    completed = true;
    if (!ec) out = beast::buffers_to_string(buf.data());
  });
  ioc.restart();
  ioc.run_for(timeout);
  if (!completed) {
    // Timed out with a read still pending: cancel it and drain so the handler
    // (holding references to these locals) runs before we return.
    beast::error_code ec;
    stream.next_layer().cancel(ec);
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(200));
  }
  return out;
}

// Read frames under a SINGLE watchdog (so an intermediate frame never trips a
// per-read close), returning the first frame whose "type" == wantType, or "" on
// timeout. Used as a deterministic readiness/observation barrier in place of
// fixed sleeps (L18): the "subscribed" ack means the Listener is registered;
// the "canceled" frame means the subscription is torn down.
std::string readUntilType(ws::stream<tcp::socket>& stream,
                          const char* wantType,
                          std::chrono::milliseconds budget) {
  // Single-threaded bounded read loop (see readFrameBounded): each frame is read
  // via async_read + run_for against the shared deadline, so no second thread
  // ever closes the socket out from under an in-flight read (TSan-clean).
  auto& ioc = static_cast<asio::io_context&>(stream.get_executor().context());
  auto deadline = std::chrono::steady_clock::now() + budget;
  std::string out;
  for (;;) {
    auto now = std::chrono::steady_clock::now();
    if (now >= deadline) break;
    auto remaining =
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
    if (!completed) {                 // budget elapsed with read pending
      beast::error_code ec;
      stream.next_layer().cancel(ec);
      ioc.restart();
      ioc.run_for(std::chrono::milliseconds(200));
      break;
    }
    if (!ok) break;                   // connection closed/errored
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

// Parse a frame as a JSON object; ASSERT it has type=="error". Return the
// "where" and "message" fields.
struct ErrorFrame {
  std::string where;
  std::string message;
  // ENC-1396: the request the rejection belongs to, as the frame renders it —
  // `key` for an int subscription, `requestId` for a string one. Both stay
  // absent on a connection-level error, which has no request to name.
  bool                hasKey{false};
  int                 key{0};
  bool                hasRequestId{false};
  std::string         requestId;
};

ErrorFrame expectErrorFrame(const std::string& payload) {
  ErrorFrame ef;
  rapidjson::Document doc;
  doc.Parse(payload.c_str());
  EXPECT_FALSE(doc.HasParseError()) << "frame is not JSON: " << payload;
  EXPECT_TRUE(doc.IsObject());
  EXPECT_TRUE(doc.HasMember("type"));
  if (doc.HasMember("type") && doc["type"].IsString()) {
    EXPECT_STREQ(doc["type"].GetString(), "error") << "frame: " << payload;
  }
  if (doc.HasMember("where") && doc["where"].IsString())
    ef.where = doc["where"].GetString();
  if (doc.HasMember("message") && doc["message"].IsString())
    ef.message = doc["message"].GetString();
  if (doc.HasMember("key") && doc["key"].IsInt()) {
    ef.hasKey = true;
    ef.key    = doc["key"].GetInt();
  }
  if (doc.HasMember("requestId") && doc["requestId"].IsString()) {
    ef.hasRequestId = true;
    ef.requestId    = doc["requestId"].GetString();
  }
  return ef;
}

} // namespace

TEST(ClientSessionTest, UnknownTypeReturnsError) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  stream.write(asio::buffer(R"({"type":"nope"})"));
  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty()) << "no frame received";

  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "type");
  EXPECT_NE(err.message.find("unknown type"), std::string::npos)
      << "message: " << err.message;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, ParseErrorOnInvalidJson) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  stream.write(asio::buffer("{not json"));
  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty());

  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "parse");

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, SubscribeRejectsOversizedStreamKey) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // MAX_STREAM_KEY_LEN is 64 in ClientSession.cpp.
  std::string oversized(80, 'X');
  std::string req =
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":")" + oversized
    + R"(","field":"px"}]})";
  stream.write(asio::buffer(req));

  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty());
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "subscribe");
  EXPECT_NE(err.message.find("streamKey"), std::string::npos)
      << "message: " << err.message;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, SubscribeRejectsOversizedField) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // MAX_FIELD_LEN is 128 in ClientSession.cpp.
  std::string oversized(150, 'F');
  std::string req =
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":"X","field":")" + oversized
    + R"("}]})";
  stream.write(asio::buffer(req));

  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty());
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "subscribe");
  EXPECT_NE(err.message.find("field"), std::string::npos)
      << "message: " << err.message;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ENC-101 push-vs-pull rule: a subscribe with field=ob.* must round-trip
// as an `error` frame whose `where` is "build" (TreeBuilder catch block at
// src/server/ClientSession.cpp ~line 510) and whose `message` contains
// the literal "pipeline-only" + the offending field. This is the only
// test that exercises the full WS round-trip; ListenerTest +
// TreeBuilderTest already cover the reject at lower layers.
TEST(ClientSessionTest, SubscribeRejectsObListenerField) {
  const krn::testsupport::ScopedPipelineOnly ob("ob");
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  std::string req =
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":"NEXO","field":"ob.best.bid.price"}]})";
  stream.write(asio::buffer(req));

  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty()) << "expected an error frame; got nothing";
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "build")
      << "ENC-101 reject is thrown from TreeBuilder, caught by the 'build' "
         "catch in ClientSession; got where=" << err.where;
  EXPECT_NE(err.message.find("pipeline-only"), std::string::npos)
      << "message must echo the canonical ENC-101 wording; got: " << err.message;
  EXPECT_NE(err.message.find("ob.best.bid.price"), std::string::npos)
      << "message must echo the offending field; got: " << err.message;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ENC-1293 / SPEC specs/2026-09-20-krn-join-correctness D7: a request whose
// terminal would receive a `Record` must round-trip as an `error` frame whose
// `where` is "build", naming the reduction to apply.
//
// This is the acceptance criterion's frame half, proved over a real socket
// rather than inferred from a thrown exception. The frame shape PRE-EXISTS —
// `ClientSession::sendError` has written {"type","where","message"} since
// ENC-48, and the "build" catch around `tree::buildForRequest` is the same one
// the ENC-101 `ob.*` reject above rides. ENC-1293 adds no new frame type and no
// new error path; it adds a throw that reaches the existing one.
//
// TEMPORARY: ENC-1295 (embassy) teaches the data path to consume a Record and
// lifts the restriction. Delete this test with it.
TEST(ClientSessionTest, SubscribeRejectsRecordValuedTerminal) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // Pack -> Responder: the terminal receives a krn::Record, which GMA
  // serializes as a JSON object and embassy's scalar-only asFloat32 drops with
  // no log, no metric and no error.
  std::string req =
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":"AAPL","field":"lastPrice",)"
    R"("pipeline":[{"type":"Pack","fields":{)"
    R"("ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},)"
    R"("bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]}]})";
  stream.write(asio::buffer(req));

  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty()) << "expected an error frame; got nothing";
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "build")
      << "the ENC-1293 reject is thrown from buildForRequest and caught by the "
         "'build' catch in ClientSession; got where=" << err.where;
  EXPECT_NE(err.message.find("Record"), std::string::npos)
      << "message must say what was wrong with the value; got: " << err.message;
  EXPECT_NE(err.message.find("Pack -> Field"), std::string::npos)
      << "message must name the reduction to apply; got: " << err.message;
  EXPECT_NE(err.message.find("ENC-1295"), std::string::npos)
      << "message must name the ticket that lifts the restriction; got: "
      << err.message;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// The accept direction over the same socket: `Pack -> Field -> Responder` must
// still subscribe successfully. Without this, the test above would be equally
// satisfied by refusing every request containing a Pack.
TEST(ClientSessionTest, SubscribeAcceptsPackFieldResponder) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  std::string req =
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":"AAPL","field":"lastPrice",)"
    R"("pipeline":[{"type":"Pack","fields":{)"
    R"("ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},)"
    R"("bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}},)"
    R"({"type":"Field","name":"bid"}]}]})";
  stream.write(asio::buffer(req));

  auto frame = readUntilType(stream, "subscribed", std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty())
      << "Pack -> Field -> Responder must still subscribe; the restriction is "
         "on Record-valued TERMINALS, not on Pack";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, CancelMissingKeysArrayError) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  stream.write(asio::buffer(R"({"type":"cancel"})"));
  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty());
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "cancel");
  EXPECT_NE(err.message.find("keys"), std::string::npos);

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, CancelNonIntegerKeysError) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  stream.write(asio::buffer(R"({"type":"cancel","keys":["abc"]})"));
  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty());
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "cancel");

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

TEST(ClientSessionTest, RateLimitEnforcedOnBurstSubscribes) {
  // RATE_LIMIT_BURST is 20 subscribe-batch invocations; the 21st should
  // produce a "rate limit exceeded" error frame.
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // Send 22 subscribe messages back-to-back (each is one rate-limit token).
  // The first 20 are accepted. The 21st should be rate-limited.
  for (int i = 0; i < 22; ++i) {
    std::string req =
      std::string(R"({"type":"subscribe","requests":[{"key":)") + std::to_string(i) +
      R"(,"streamKey":"R","field":"px"}]})";
    stream.write(asio::buffer(req));
  }

  bool sawRateLimit = false;
  for (int i = 0; i < 30 && !sawRateLimit; ++i) {
    auto frame = readFrameBounded(stream, std::chrono::milliseconds(100));
    if (frame.empty()) break;
    rapidjson::Document doc;
    doc.Parse(frame.c_str());
    if (doc.HasParseError() || !doc.IsObject()) continue;
    if (doc.HasMember("type") && doc["type"].IsString() &&
        std::string(doc["type"].GetString()) == "error" &&
        doc.HasMember("message") && doc["message"].IsString()) {
      const std::string msg = doc["message"].GetString();
      if (msg.find("rate limit") != std::string::npos) {
        sawRateLimit = true;
      }
    }
  }

  EXPECT_TRUE(sawRateLimit)
      << "expected at least one 'rate limit exceeded' error after 22 bursts";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// krn-string-id-subscriptions phase 2: subscribe via the embassy-shaped
// {id:"<string>"} input. The ack must carry "requestId" (not "key"); the
// value frame must too. No "key" field in either path.
TEST(ClientSessionTest, SubscribeWithStringIdEchoesRequestId) {
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  const std::string req =
    R"({"type":"subscribe","requests":[{"id":"r-NEXO-test","streamKey":"STRID","field":"px"}]})";
  stream.write(asio::buffer(req));

  // Deterministic readiness (L18): the ack guarantees the listener is wired,
  // so the injected tick must produce an update — no fixed sleep needed.
  std::string ackPayload = readUntilType(stream, "subscribed", std::chrono::seconds(2));
  ASSERT_FALSE(ackPayload.empty()) << "no 'subscribed' ack received";

  auto ev = std::make_shared<rapidjson::Document>();
  ev->SetObject();
  ev->AddMember("px", 7.5, ev->GetAllocator());
  krn::Event tick;
  tick.type = "tick";
  tick.symbol = "STRID";
  tick.payload = std::move(ev);
  srv.dispatcher->onTick(tick);

  std::string updatePayload = readUntilType(stream, "update", std::chrono::seconds(2));
  ASSERT_FALSE(updatePayload.empty()) << "no 'update' frame received";

  rapidjson::Document ack; ack.Parse(ackPayload.c_str());
  EXPECT_TRUE(ack.HasMember("requestId")) << "ack: " << ackPayload;
  EXPECT_FALSE(ack.HasMember("key")) << "ack must not have 'key' field for string-id sub: " << ackPayload;
  if (ack.HasMember("requestId") && ack["requestId"].IsString())
    EXPECT_STREQ(ack["requestId"].GetString(), "r-NEXO-test");

  rapidjson::Document upd; upd.Parse(updatePayload.c_str());
  EXPECT_TRUE(upd.HasMember("requestId")) << "update: " << updatePayload;
  EXPECT_FALSE(upd.HasMember("key")) << "update must not have 'key' field: " << updatePayload;
  if (upd.HasMember("requestId") && upd["requestId"].IsString())
    EXPECT_STREQ(upd["requestId"].GetString(), "r-NEXO-test");

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// Mixed int + string subscriptions on the same session. Each value frame
// must carry the field-shape that matches its subscription's input form.
TEST(ClientSessionTest, MixedIntAndStringSubsCoexist) {
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // Two subscriptions on the same streamKey but different fields, so values
  // route to one or the other deterministically.
  stream.write(asio::buffer(
    R"({"type":"subscribe","requests":[{"key":1,"streamKey":"MIX","field":"px"}]})"));
  stream.write(asio::buffer(
    R"({"type":"subscribe","requests":[{"id":"alpha","streamKey":"MIX","field":"qty"}]})"));

  // Deterministic readiness (L18): both subscriptions must be acked before the
  // tick so each routes to a live listener. Two subscribes -> two acks.
  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(2)).empty())
      << "first subscribe never acked";
  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(2)).empty())
      << "second subscribe never acked";

  auto payload = std::make_shared<rapidjson::Document>();
  payload->SetObject();
  payload->AddMember("px", 11.0, payload->GetAllocator());
  payload->AddMember("qty", 22.0, payload->GetAllocator());
  krn::Event tick;
  tick.type = "tick";
  tick.symbol = "MIX";
  tick.payload = std::move(payload);
  srv.dispatcher->onTick(tick);

  bool sawIntFrame = false;
  bool sawStringFrame = false;
  for (int i = 0; i < 12 && (!sawIntFrame || !sawStringFrame); ++i) {
    auto frame = readFrameBounded(stream, std::chrono::milliseconds(500));
    if (frame.empty()) break;
    rapidjson::Document doc;
    doc.Parse(frame.c_str());
    if (doc.HasParseError() || !doc.IsObject()) continue;
    if (!doc.HasMember("type") || std::string(doc["type"].GetString()) != "update") continue;

    if (doc.HasMember("key") && doc["key"].IsInt() && doc["key"].GetInt() == 1) {
      sawIntFrame = true;
      EXPECT_FALSE(doc.HasMember("requestId")) << "int-keyed frame must not have requestId: " << frame;
    } else if (doc.HasMember("requestId") && doc["requestId"].IsString() &&
               std::string(doc["requestId"].GetString()) == "alpha") {
      sawStringFrame = true;
      EXPECT_FALSE(doc.HasMember("key")) << "string-keyed frame must not have key: " << frame;
    }
  }

  EXPECT_TRUE(sawIntFrame) << "no int-keyed update frame for key=1";
  EXPECT_TRUE(sawStringFrame) << "no string-keyed update frame for id=alpha";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// Cancel-by-ids removes the string-keyed subscription. After cancel, no
// further update frames should fire for that id.
TEST(ClientSessionTest, CancelByIdsRemovesStringKeyedSub) {
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  stream.write(asio::buffer(
    R"({"type":"subscribe","requests":[{"id":"foo","streamKey":"CANCEL","field":"px"}]})"));

  // Deterministic readiness (L18): wait for the ack so the listener is live.
  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(2)).empty())
      << "subscribe never acked";

  // Confirm the sub is live by firing a tick and observing an update.
  auto ev1 = std::make_shared<rapidjson::Document>();
  ev1->SetObject();
  ev1->AddMember("px", 1.0, ev1->GetAllocator());
  krn::Event t1;
  t1.type = "tick"; t1.symbol = "CANCEL"; t1.payload = std::move(ev1);
  srv.dispatcher->onTick(t1);

  bool sawPreCancelUpdate = false;
  for (int i = 0; i < 6 && !sawPreCancelUpdate; ++i) {
    auto frame = readFrameBounded(stream, std::chrono::milliseconds(300));
    if (frame.empty()) break;
    rapidjson::Document doc;
    doc.Parse(frame.c_str());
    if (!doc.HasParseError() && doc.IsObject() && doc.HasMember("type") &&
        std::string(doc["type"].GetString()) == "update") {
      sawPreCancelUpdate = true;
    }
  }
  ASSERT_TRUE(sawPreCancelUpdate) << "string-id sub never fired even before cancel";

  // Cancel by ids array. The server emits a "canceled" frame AFTER it removes
  // and shuts down the subscription (ClientSession::handleCancel), so waiting
  // for it is a deterministic barrier (L18) — no fixed sleep/drain needed.
  stream.write(asio::buffer(R"({"type":"cancel","ids":["foo"]})"));
  ASSERT_FALSE(readUntilType(stream, "canceled", std::chrono::seconds(2)).empty())
      << "cancel was never acknowledged with a 'canceled' frame";

  // Fire another tick — no update frame should follow.
  auto ev2 = std::make_shared<rapidjson::Document>();
  ev2->SetObject();
  ev2->AddMember("px", 2.0, ev2->GetAllocator());
  krn::Event t2;
  t2.type = "tick"; t2.symbol = "CANCEL"; t2.payload = std::move(ev2);
  srv.dispatcher->onTick(t2);

  bool sawPostCancelUpdate = false;
  for (int i = 0; i < 4; ++i) {
    auto frame = readFrameBounded(stream, std::chrono::milliseconds(200));
    if (frame.empty()) break;
    rapidjson::Document doc;
    doc.Parse(frame.c_str());
    if (!doc.HasParseError() && doc.IsObject() && doc.HasMember("type") &&
        std::string(doc["type"].GetString()) == "update" &&
        doc.HasMember("requestId") && doc["requestId"].IsString() &&
        std::string(doc["requestId"].GetString()) == "foo") {
      sawPostCancelUpdate = true;
    }
  }

  EXPECT_FALSE(sawPostCancelUpdate)
    << "received update frame for id='foo' after cancel by ids";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ---------------------------------------------------------------------------
// ENC-1280 — the bucket basis on the LIVE serialiser.
//
// SPEC specs/2026-09-20-timestamps-on-the-wire/SPEC.md D9, and its §5
// correction: `src/ws/WSResponder.cpp` is dead code (WsBridge is constructed
// nowhere outside tests/ws/WsBridgeTest.cpp), so a stamp plumbed through it
// would have had no runtime effect — the ENC-813 failure class. The live
// producer is the `sendFn` lambda inside ClientSession::handleSubscribe,
// reached from main.cpp -> WebSocketServer -> ClientSession.
//
// This test refuses to assume that. It boots a REAL WebSocketServer (the same
// class main.cpp constructs), connects a REAL Beast WebSocket client, and
// reads the bytes off the socket. Nothing below touches a node directly.
// ---------------------------------------------------------------------------

namespace {

std::int64_t nowEpochMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

} // namespace

// A bucketed pipeline (TumblingWindow -> VectorReducer -> Responder) must put
// an exact, grid-aligned integer `bucketStartMs` on the wire. The
// VectorReducer hop is deliberate: it constructs a new StreamValue, so this
// also proves the stamp survives a value-transforming node on the real path.
TEST(ClientSessionTest, BucketedPipelineStampsBucketStartMsOnTheWire) {
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  constexpr std::int64_t kPeriodMs = 100;

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  const std::string req =
    R"({"type":"subscribe","requests":[{"id":"r-bucket","streamKey":"TWLIVE","field":"px",)"
    R"("pipeline":[{"type":"TumblingWindow","periodMs":100},)"
    R"({"type":"VectorReducer","fn":"max"}]}]})";
  stream.write(asio::buffer(req));

  std::string ackPayload = readUntilType(stream, "subscribed", std::chrono::seconds(2));
  ASSERT_FALSE(ackPayload.empty()) << "no 'subscribed' ack received";

  const std::int64_t t0 = nowEpochMs();

  auto ev = std::make_shared<rapidjson::Document>();
  ev->SetObject();
  ev->AddMember("px", 7.5, ev->GetAllocator());
  krn::Event tick;
  tick.type   = "tick";
  tick.symbol = "TWLIVE";
  tick.payload = std::move(ev);
  srv.dispatcher->onTick(tick);

  std::string updatePayload = readUntilType(stream, "update", std::chrono::seconds(3));
  const std::int64_t t1 = nowEpochMs();
  ASSERT_FALSE(updatePayload.empty()) << "no 'update' frame received";

  rapidjson::Document upd;
  upd.Parse(updatePayload.c_str());
  ASSERT_FALSE(upd.HasParseError()) << updatePayload;

  ASSERT_TRUE(upd.HasMember("bucketStartMs"))
      << "the live update frame carries no bucket basis: " << updatePayload;
  // Must be a JSON integer, not a double — a current ms epoch needs 41 bits
  // and the float32 lane D4 rejects would quantise it to ~2-minute steps.
  ASSERT_TRUE(upd["bucketStartMs"].IsInt64())
      << "bucketStartMs must be an exact integer: " << updatePayload;

  const std::int64_t stamp = upd["bucketStartMs"].GetInt64();
  EXPECT_NE(stamp, 0) << "boundary discarded before the wire: " << updatePayload;
  EXPECT_EQ(stamp % kPeriodMs, 0)
      << "stamp is off the wall-clock grid TumblingWindow aligns to: " << updatePayload;
  // The bar must be one that closed while this test was running.
  EXPECT_GE(stamp, (t0 / kPeriodMs) * kPeriodMs - kPeriodMs) << updatePayload;
  EXPECT_LE(stamp, (t1 / kPeriodMs) * kPeriodMs) << updatePayload;

  // The rest of the frame is unchanged.
  EXPECT_STREQ(upd["type"].GetString(), "update");
  EXPECT_TRUE(upd.HasMember("streamKey"));
  EXPECT_TRUE(upd.HasMember("value"));
  EXPECT_TRUE(upd.HasMember("requestId"));

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// The complement, and the compatibility guarantee: a subscription with no
// bucketing declares no basis, so the key is ABSENT — not `0`, which would
// read as a 1970 epoch. Un-bucketed update frames keep the exact four-key
// shape they had before ENC-1280.
TEST(ClientSessionTest, UnbucketedSubscriptionEmitsNoBucketStartMs) {
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  const std::string req =
    R"({"type":"subscribe","requests":[{"id":"r-raw","streamKey":"RAWLIVE","field":"px"}]})";
  stream.write(asio::buffer(req));

  ASSERT_FALSE(readUntilType(stream, "subscribed", std::chrono::seconds(2)).empty());

  auto ev = std::make_shared<rapidjson::Document>();
  ev->SetObject();
  ev->AddMember("px", 3.25, ev->GetAllocator());
  krn::Event tick;
  tick.type   = "tick";
  tick.symbol = "RAWLIVE";
  tick.payload = std::move(ev);
  srv.dispatcher->onTick(tick);

  std::string updatePayload = readUntilType(stream, "update", std::chrono::seconds(2));
  ASSERT_FALSE(updatePayload.empty()) << "no 'update' frame received";

  rapidjson::Document upd;
  upd.Parse(updatePayload.c_str());
  ASSERT_FALSE(upd.HasParseError()) << updatePayload;
  EXPECT_FALSE(upd.HasMember("bucketStartMs"))
      << "an un-bucketed stream must declare no basis at all: " << updatePayload;

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// ---------------------------------------------------------------------------
// ENC-1396: a rejection must name the request it belongs to.
//
// Before this, `sendError` wrote exactly three keys — type/where/message — while
// every other frame the session writes (`update`, `subscribed`, `canceled`)
// carried the request identity through `writeRequestKeyJSON`. A `subscribe` of N
// requests where one fails to build produced one frame naming neither the failed
// request nor the survivors, so a client had to re-derive which by noticing whose
// `subscribed` ack never arrived.
//
// The batch below is the case that matters: two requests, one good and one that
// throws out of `buildForRequest` (a `Pack`-valued terminal, the ENC-1293 reject
// reused from SubscribeRejectsRecordValuedTerminal above). The error frame must
// name key 7, and the surviving request must still be acked — a fix that refused
// the whole batch would pass a weaker assertion.
// ---------------------------------------------------------------------------
TEST(ClientSessionTest, BuildErrorInABatchNamesTheFailingRequest) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // key 6 is well-formed; key 7's Pack-valued terminal throws in buildForRequest.
  std::string req =
    R"({"type":"subscribe","requests":[)"
    R"({"key":6,"streamKey":"AAPL","field":"lastPrice"},)"
    R"({"key":7,"streamKey":"AAPL","field":"lastPrice",)"
    R"("pipeline":[{"type":"Pack","fields":{)"
    R"("ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},)"
    R"("bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]}]})";
  stream.write(asio::buffer(req));

  // Collect both the error and the ack, in whichever order they arrive.
  ErrorFrame err;
  bool sawError = false, sawAckFor6 = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while ((!sawError || !sawAckFor6) && std::chrono::steady_clock::now() < deadline) {
    auto frame = readFrameBounded(stream, std::chrono::milliseconds(600));
    if (frame.empty()) continue;
    rapidjson::Document d;
    d.Parse(frame.c_str());
    ASSERT_FALSE(d.HasParseError()) << "frame is not JSON: " << frame;
    ASSERT_TRUE(d.HasMember("type") && d["type"].IsString()) << frame;
    const std::string type = d["type"].GetString();
    if (type == "error") {
      err = expectErrorFrame(frame);
      sawError = true;
    } else if (type == "subscribed" && d.HasMember("key") && d["key"].IsInt()
               && d["key"].GetInt() == 6) {
      sawAckFor6 = true;
    }
  }

  ASSERT_TRUE(sawError) << "expected an error frame for the Pack-terminal request";
  EXPECT_EQ(err.where, "build")
      << "the reject is thrown from buildForRequest and caught by the 'build' "
         "catch; got where=" << err.where;

  // The point of the ticket.
  EXPECT_TRUE(err.hasKey)
      << "the error frame carries no request key, so the rejection cannot be "
         "attributed to a subscription (ENC-1396); frame said where=" << err.where
      << " message=" << err.message;
  EXPECT_EQ(err.key, 7)
      << "the error must name the request that FAILED (7), not the one that "
         "succeeded (6); got key=" << err.key;
  EXPECT_FALSE(err.hasRequestId)
      << "an int-keyed subscription renders `key`, never `requestId`";

  // And the batch is not refused wholesale: request 6 still subscribes.
  EXPECT_TRUE(sawAckFor6)
      << "the well-formed request in the same batch must still be acked — "
         "otherwise this test would also pass if the whole batch were refused";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// The string-keyed half: the same rejection on an `id`-carrying request must
// render `requestId`, not `key`. Without this the fix could hardcode `key` and
// still pass the test above, leaving embassy — which reads `requestId` — exactly
// as blind as before (ENC-1387 wired its onError to this frame shape).
TEST(ClientSessionTest, BuildErrorOnStringIdRequestNamesRequestId) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  std::string req =
    R"({"type":"subscribe","requests":[)"
    R"({"id":"r-AAPL-packed","streamKey":"AAPL","field":"lastPrice",)"
    R"("pipeline":[{"type":"Pack","fields":{)"
    R"("ask":{"type":"Listener","streamKey":"AAPL","field":"ask"},)"
    R"("bid":{"type":"Listener","streamKey":"AAPL","field":"bid"}}}]}]})";
  stream.write(asio::buffer(req));

  auto frame = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(frame.empty()) << "expected an error frame; got nothing";
  auto err = expectErrorFrame(frame);
  EXPECT_EQ(err.where, "build") << "got where=" << err.where;
  EXPECT_TRUE(err.hasRequestId)
      << "a string-keyed subscription must render `requestId` on the error frame "
         "(ENC-1396); message=" << err.message;
  EXPECT_EQ(err.requestId, "r-AAPL-packed");
  EXPECT_FALSE(err.hasKey)
      << "a string-keyed subscription renders `requestId`, never `key`";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}

// The negative half of the contract: a CONNECTION-level failure has no request
// to name and must keep the original three-key shape. Without this row the fix
// could attach a key unconditionally (defaulting to 0 / ""), which would be
// worse than the defect — a client would attribute a parse failure to request 0.
TEST(ClientSessionTest, ConnectionLevelErrorsCarryNoRequestKey) {
  ServerHarness srv;
  asio::io_context clientIoc;
  auto stream = connect(clientIoc, srv.port());

  // (a) not JSON at all -> where="parse"
  stream.write(asio::buffer(std::string("{not json")));
  auto f1 = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(f1.empty());
  auto e1 = expectErrorFrame(f1);
  EXPECT_EQ(e1.where, "parse");
  EXPECT_FALSE(e1.hasKey)      << "a parse failure has no request to attribute to";
  EXPECT_FALSE(e1.hasRequestId) << "a parse failure has no request to attribute to";

  // (b) a per-request reject that fires BEFORE a key is parsed. The ticket's
  // enumeration put these in the "key available" group; they are not — the key
  // is precisely what is missing, so they stay keyless.
  stream.write(asio::buffer(std::string(
    R"({"type":"subscribe","requests":[{"streamKey":"AAPL","field":"lastPrice"}]})")));
  auto f2 = readFrameBounded(stream, std::chrono::seconds(2));
  ASSERT_FALSE(f2.empty());
  auto e2 = expectErrorFrame(f2);
  EXPECT_EQ(e2.where, "subscribe");
  EXPECT_NE(e2.message.find("missing valid"), std::string::npos)
      << "expected the no-valid-key reject; got: " << e2.message;
  EXPECT_FALSE(e2.hasKey)
      << "this reject fires because there IS no valid key — it cannot name one";
  EXPECT_FALSE(e2.hasRequestId)
      << "this reject fires because there IS no valid key — it cannot name one";

  beast::error_code ec;
  stream.close(ws::close_code::normal, ec);
}
