// File: src/server/ClientSession.cpp

#include "krn/server/ClientSession.hpp"
#include "krn/server/WebSocketServer.hpp"
#include "krn/ExecutionContext.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/rt/Strand.hpp"
#include "krn/JsonValidator.hpp"
#include "krn/nodes/Responder.hpp"
#include "krn/util/Logger.hpp"
#include "krn/util/Metrics.hpp"
#include "krn/util/JsonUtil.hpp"

#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <boost/asio/buffer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/bind_executor.hpp>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <deque>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <type_traits>
#include <variant>
#include <vector>
#include <cstdint>

namespace krn {

namespace websocket = boost::beast::websocket;
namespace beast     = boost::beast;
namespace http      = boost::beast::http;

namespace server {
namespace {
// ENC-1338. The origin tag. Its containment is NOT what makes the gate work —
// `rt::Strand::Attribution`'s private constructor is, and only
// `SubscriptionStrandMint` is its friend. A string literal alone was the
// earlier draft and an adversarial review broke it twice over; see the comment
// on `rt::Strand::Attribution`. Static storage duration, as `Attribution`
// requires.
constexpr const char* kSubscriptionStrandOrigin =
    "krn::ClientSession::handleSubscribe";
} // namespace

const char* SubscriptionStrandMint::origin() noexcept {
  return kSubscriptionStrandOrigin;
}

// ENC-1338 / ENC-1005 / SPEC D3, D4. See the long comment in the header: this
// is D3's named mint site, and the null-pool guard below is what ENC-1005's
// M10 removes. Both are gated by `tests/ws/SubscriptionStrandMintTest.cpp`.
std::shared_ptr<krn::rt::Strand>
SubscriptionStrandMint::mint(krn::rt::ThreadPool* pool) {
  if (!pool) return nullptr;          // <-- the guard M10 deletes
  return std::make_shared<krn::rt::Strand>(
      pool, krn::rt::Strand::Attribution(kSubscriptionStrandOrigin));
}

} // namespace server

// ------------------------------
// Construction / lifecycle
// ------------------------------
ClientSession::ClientSession(tcp::socket socket,
                             WebSocketServer* server,
                             ExecutionContext* exec,
                             Dispatcher* dispatcher)
  : server_(server)
  , exec_(exec)
  , dispatcher_(dispatcher)
, ws_(std::move(socket))
, strand_(ws_.get_executor())
{}

void ClientSession::run() {
  auto self = shared_from_this();

  // Cap incoming WebSocket frame size to prevent memory exhaustion from
  // a malicious client sending a multi-gigabyte message.
  ws_.read_message_max(64 * 1024);  // 64 KB

  // Heartbeat: send pings after 30s idle, close if no pong within timeout.
  {
    websocket::stream_base::timeout opt{};
    opt.handshake_timeout = std::chrono::seconds(30);
    opt.idle_timeout      = std::chrono::seconds(30);
    opt.keep_alive_pings  = true;
    ws_.set_option(opt);
  }
  ws_.set_option(websocket::stream_base::decorator(
      [](websocket::response_type& res) {
        res.set(http::field::server,
                std::string(BOOST_BEAST_VERSION_STRING) + " krn-websocket");
      }));

  // Accept + begin read loop on strand.
  ws_.async_accept(
    boost::asio::bind_executor(
      strand_,
      [self](beast::error_code ec) {
        if (ec) {
          krn::util::logger().log(krn::util::LogLevel::Error,
                                  "ws.accept_failed",
                                  { {"err", ec.message()} });
          return;
        }

        self->open_.store(true);
        KRN_METRIC_HIT("ws.accept");

        if (self->server_) {
          self->sessionId_ = self->server_->registerSession(self);
        }

        krn::util::logger().log(krn::util::LogLevel::Info,
                                "ws.accepted",
                                { {"sessionId", std::to_string(self->sessionId_)} });

        self->doRead();
      }
    )
  );
}

void ClientSession::doRead() {
  auto self = shared_from_this();

  ws_.async_read(
    buffer_,
    boost::asio::bind_executor(
      strand_,
      [self](beast::error_code ec, std::size_t bytes) {
        self->onRead(ec, bytes);
      }
    )
  );
}

void ClientSession::onRead(beast::error_code ec, std::size_t) {
  // This handler is on-strand.
  if (ec == websocket::error::closed) {
    close();
    return;
  }
  if (ec) {
    krn::util::logger().log(krn::util::LogLevel::Error,
                            "ws.read_failed",
                            { {"err", ec.message()} });
    close();
    return;
  }

  const std::string text = beast::buffers_to_string(buffer_.data());
  buffer_.consume(buffer_.size());

  KRN_METRIC_HIT("ws.msg_in");

  handleMessage(text);

  // Continue reading.
  doRead();
}

// ------------------------------
// Close / cleanup
// ------------------------------
void ClientSession::close() {
  auto self = shared_from_this();

  boost::asio::dispatch(strand_, [self]() {
    // idempotent
    if (!self->open_.exchange(false)) return;

    // Best-effort shutdown of active requests/trees. Shut down EVERY node in
    // each pipeline (head shutdown() does not propagate down the chain). Head
    // shutdown() is idempotent, so re-running it here when the head also
    // appears in its keepAlive chain is harmless.
    //
    // This sweep is now belt-and-braces rather than required. It used to be
    // load-bearing: the timer nodes (Interval, then TumblingWindow and
    // BucketTime) launched their timer thread holding a shared_from_this(), so
    // the thread owned the node, the destructor could never run, and a
    // pipeline dropped without an explicit shutdown() leaked a live thread
    // firing into an already-destroyed child, ThreadPool and store. ENC-1065
    // fixed Interval and ENC-1080 fixed TumblingWindow and BucketTime: none of
    // them holds a reference back to itself any more, so dropping the last
    // owner destroys the node and its destructor stops and joins the thread.
    // The sweep is kept because it still stops timers *early* (at close, not
    // at refcount zero) and covers any future node that needs the nudge.
    {
      std::lock_guard<std::mutex> lk(self->reqMu_);
      for (auto& kv : self->active_) {
        if (kv.second) {
          try { kv.second->shutdown(); } catch (...) {}
        }
      }
      for (auto& kv : self->chains_) {
        for (auto& node : kv.second) {
          if (node) {
            try { node->shutdown(); } catch (...) {}
          }
        }
      }
      self->active_.clear();
      self->chains_.clear();
    }

    websocket::close_reason cr;
    cr.code   = websocket::close_code::normal;
    cr.reason = "closing";

    self->ws_.async_close(
      cr,
      boost::asio::bind_executor(
        self->strand_,
        [self](beast::error_code ec) {
          if (ec) {
            krn::util::logger().log(krn::util::LogLevel::Warn,
                                    "ws.close_failed",
                                    { {"err", ec.message()} });
          }

          KRN_METRIC_HIT("ws.close");

          if (self->server_ && self->sessionId_ != 0) {
            self->server_->unregisterSession(self->sessionId_);
            self->sessionId_ = 0;  // prevent double-unregister
          }
        }
      )
    );
  });
}

// ------------------------------
// Outbound sending (thread-safe)
// ------------------------------
void ClientSession::sendText(std::string s) {
  if (!open_.load()) return;

  auto self = shared_from_this();
  boost::asio::dispatch(strand_, [self, p = std::move(s)]() mutable {
    self->enqueue(std::move(p), nullptr);
  });
}

void ClientSession::sendUpdate(std::string payload,
                               std::uint64_t subId,
                               const std::string& streamKey) {
  if (!open_.load()) return;

  auto self = shared_from_this();
  boost::asio::dispatch(strand_,
    [self, p = std::move(payload), subId, sk = streamKey]() mutable {
      CoalesceKeyView ck{subId, sk};
      self->enqueue(std::move(p), &ck);
    });
}

// Must be called on-strand.
void ClientSession::enqueue(std::string payload, const CoalesceKeyView* ckey) {
  if (!open_.load()) return;

  // ---- 1. Coalesce-latest --------------------------------------------------
  // A pending frame for the same (subscription, streamKey) is superseded by
  // this one, so overwrite it in place instead of appending — but only once the
  // queue is a genuine backlog. Below the watermark every frame is delivered,
  // so a consumer that is keeping up loses nothing.
  if (ckey && outbox_.size() >= COALESCE_WATERMARK) {
    // coalesceIndex_ always points at the NEWEST pending frame for a key (it is
    // re-pointed on every append), so replacing it keeps the queue in value
    // order: older frames for the same key sit ahead of it and still go out
    // first.
    auto it = coalesceIndex_.find(*ckey);
    if (it != coalesceIndex_.end()) {
      const std::uint64_t seq          = it->second;
      const std::uint64_t firstMutable = outboxHeadSeq_ + (writing_ ? 1u : 0u);
      if (seq >= firstMutable && seq < outboxHeadSeq_ + outbox_.size()) {
        outbox_[static_cast<std::size_t>(seq - outboxHeadSeq_)].payload = std::move(payload);
        KRN_METRIC_HIT("ws.outbox_coalesced");
        noteBackpressure("coalesce-latest");
        return;
      }
      // Either stale (already drained) or the frame currently in flight —
      // Beast holds a buffer pointing straight into that payload string, so it
      // must never be mutated. Fall through and append; the index is re-pointed
      // at the new frame below.
    }
  }

  // ---- 2. Hard memory bound ----------------------------------------------
  // The outbox is at its memory cap, so this frame can only be admitted by
  // dropping one that is already queued — or by dropping itself.
  //
  // ENC-1072: a coalescable frame used to drop ITSELF here, which contradicted
  // ENC-996's guarantee at precisely the point the guarantee matters. Step 1
  // above did not fire, so this key has NO mutable pending frame — the value
  // being discarded was the client's only chance at a fresh number for that
  // stream, and nothing newer was coming to repair it. Displace the OLDEST
  // pending value instead: that is the same trade coalescing makes below the
  // bound (newer supersedes older), applied across keys rather than within
  // one, and it makes drops stale-first everywhere.
  if (outbox_.size() >= MAX_OUTBOX_SIZE) {
    if (ckey) {
      const std::size_t victim = oldestCoalescableIndex();
      if (victim < outbox_.size()) {
        replaceFrame(victim, std::move(payload), *ckey);
        KRN_METRIC_HIT("ws.outbox_shed");
        noteBackpressure("displace-oldest");
        return;
      }
      // Nothing to displace: every queued frame is lossless protocol state,
      // which must not be dropped to make room for a sample. This is the one
      // case where the newest value really is lost, so it gets its own counter
      // — `ws.outbox_shed` means "a stale value was dropped", which is the
      // designed degradation, while this means "the head of a stream was
      // dropped", which is not.
      KRN_METRIC_HIT("ws.outbox_shed_newest");
      noteBackpressure("shed-newest");
      return;
    }
    // Lossless frame: make room by shedding the oldest update.
    if (!evictOldestCoalescable()) {
      // Nothing shedable — the queue is pure protocol traffic, which really is
      // unbounded memory growth. Keep the original protection.
      krn::util::logger().log(krn::util::LogLevel::Warn,
                              "ws.outbox_overflow",
                              {{"sessionId", std::to_string(sessionId_)},
                               {"queueSize", std::to_string(outbox_.size())}});
      KRN_METRIC_HIT("ws.outbox_overflow");
      close();
      return;
    }
  }

  // ---- 3. Append ---------------------------------------------------------
  const std::uint64_t seq = outboxHeadSeq_ + outbox_.size();
  OutFrame f;
  f.payload = std::move(payload);
  if (ckey) {
    f.key.sub = ckey->sub;
    f.key.streamKey.assign(ckey->streamKey);
  }
  outbox_.push_back(std::move(f));
  if (ckey) coalesceIndex_[outbox_.back().key] = seq;

  if (!writing_) {
    writing_ = true;
    startWrite();
  }
}

// Must be called on-strand. Index of the oldest coalescable frame that may be
// dropped or overwritten, or outbox_.size() if there is none.
//
// outbox_.front() is skipped while a write is in flight: Beast holds a buffer
// pointing straight into that frame's payload string.
//
// The scan costs the length of the run of lossless frames at the head, not the
// length of the queue. Under the shape that actually reaches the bound — a
// value flood — the head is a value frame and this returns immediately; the
// walk only gets long for a session whose queue is mostly protocol traffic,
// which is also the session that is about to be closed by the overflow branch.
std::size_t ClientSession::oldestCoalescableIndex() const {
  const std::size_t start = writing_ ? 1u : 0u;
  for (std::size_t i = start; i < outbox_.size(); ++i) {
    if (outbox_[i].coalescable()) return i;
  }
  return outbox_.size();
}

// Must be called on-strand. Overwrite slot `i` with a newer coalescable frame.
//
// In place rather than erase+push_back on purpose: the deque keeps its length,
// so every absolute sequence number stays valid and only the two affected index
// entries need touching. An erase would shift every later frame down one slot
// and force a full reindexOutbox(), turning the hot path quadratic.
//
// Ordering is safe because this is only reached for a key with no MUTABLE
// pending frame: either the key has nothing queued, or its only queued frame is
// the one in flight at the head — which sits ahead of slot `i` (the scan starts
// at 1 while writing). So the replacement can never land in front of an older
// value for its own key. Values for OTHER keys are overtaken, which coalescing
// already does and which the wire has never ordered across keys.
void ClientSession::replaceFrame(std::size_t i,
                                 std::string payload,
                                 const CoalesceKeyView& ckey) {
  OutFrame& f = outbox_[i];
  const std::uint64_t seq = outboxHeadSeq_ + i;
  if (f.coalescable()) {
    // Only drop the victim's index entry if it still points HERE; a newer frame
    // for the victim's key may sit further back and must keep its mapping.
    auto it = coalesceIndex_.find(CoalesceKeyView{f.key.sub, f.key.streamKey});
    if (it != coalesceIndex_.end() && it->second == seq) {
      coalesceIndex_.erase(it);
    }
  }
  f.payload = std::move(payload);
  f.key.sub = ckey.sub;
  f.key.streamKey.assign(ckey.streamKey);
  coalesceIndex_[f.key] = seq;
}

// Must be called on-strand.
void ClientSession::noteBackpressure(const char* policy) {
  if (backpressureLogged_) return;  // one Info line per session, not per frame
  backpressureLogged_ = true;
  krn::util::logger().log(krn::util::LogLevel::Info,
                          "ws.outbox_backpressure",
                          {{"sessionId", std::to_string(sessionId_)},
                           {"queueSize", std::to_string(outbox_.size())},
                           {"policy", policy}});
}

// Must be called on-strand.
bool ClientSession::evictOldestCoalescable() {
  const std::size_t i = oldestCoalescableIndex();
  if (i >= outbox_.size()) return false;
  outbox_.erase(outbox_.begin() + static_cast<std::ptrdiff_t>(i));
  // Erasing from the middle shifts every later frame down one slot, so the
  // recorded sequence numbers are stale. This path only fires to make room for
  // a LOSSLESS frame — rare, and never the value hot path, which displaces in
  // place via replaceFrame() — so a full rebuild is cheaper to reason about
  // than patching the index.
  reindexOutbox();
  KRN_METRIC_HIT("ws.outbox_shed");
  return true;
}

// Must be called on-strand.
void ClientSession::reindexOutbox() {
  coalesceIndex_.clear();
  for (std::size_t i = 0; i < outbox_.size(); ++i) {
    // Ascending order: if two frames share a key (in-flight + queued), the
    // newer one wins, which is exactly the coalescing target we want.
    if (outbox_[i].coalescable()) {
      coalesceIndex_[outbox_[i].key] = outboxHeadSeq_ + i;
    }
  }
}

void ClientSession::startWrite() {
  // This function must be called on-strand.
  if (!open_.load()) {
    writing_ = false;
    outboxHeadSeq_ += outbox_.size();
    outbox_.clear();
    coalesceIndex_.clear();
    return;
  }

  if (outbox_.empty()) {
    writing_ = false;
    return;
  }

  auto self = shared_from_this();

  ws_.text(true);
  ws_.async_write(
    boost::asio::buffer(outbox_.front().payload),
    boost::asio::bind_executor(
      strand_,
      [self](beast::error_code ec, std::size_t bytes) {
        self->onWrite(ec, bytes);
      }
    )
  );
}

void ClientSession::onWrite(beast::error_code ec, std::size_t) {
  // This handler is on-strand.
  if (ec) {
    krn::util::logger().log(krn::util::LogLevel::Error,
                            "ws.write_failed",
                            { {"err", ec.message()} });
    // If writes fail, tear down session to prevent stuck state.
    close();
    return;
  }

  if (!outbox_.empty()) {
    const OutFrame& front = outbox_.front();
    if (front.coalescable()) {
      // Only drop the index entry if it still points at THIS frame: a newer
      // frame for the same key may have been appended while this one was in
      // flight, and that mapping must survive.
      auto it = coalesceIndex_.find(CoalesceKeyView{front.key.sub, front.key.streamKey});
      if (it != coalesceIndex_.end() && it->second == outboxHeadSeq_) {
        coalesceIndex_.erase(it);
      }
    }
    outbox_.pop_front();
    ++outboxHeadSeq_;
  }

  // Continue draining queue.
  startWrite();
}

// ------------------------------
// Protocol helpers
// ------------------------------
void ClientSession::sendError(const std::string& where, const std::string& message,
                              const std::optional<krn::server::RequestKey>& reqKey) {
  ::rapidjson::StringBuffer sb;
  ::rapidjson::Writer<::rapidjson::StringBuffer> w(sb);

  w.StartObject();
  w.Key("type");    w.String("error");
  w.Key("where");   w.String(where.c_str());
  w.Key("message"); w.String(message.c_str());
  // ENC-1396: attribute the rejection to its request when one is in scope, using
  // the same renderer `update`/`subscribed`/`canceled` use — `key` for an int
  // subscription, `requestId` for a string one. Absent for connection-level
  // failures, which have no request to name.
  if (reqKey) krn::server::writeRequestKeyJSON(w, *reqKey);
  w.EndObject();

  KRN_METRIC_HIT("ws.msg_out");
  sendText(sb.GetString());
}

void ClientSession::handleMessage(const std::string& text) {
  ::rapidjson::Document doc;
  doc.Parse(text.c_str());

  if (doc.HasParseError() || !doc.IsObject()) {
    sendError("parse", "invalid JSON");
    return;
  }

  if (!doc.HasMember("type") || !doc["type"].IsString()) {
    sendError("type", "missing 'type'");
    return;
  }

  const std::string type = doc["type"].GetString();

  if (type == "subscribe") {
    handleSubscribe(doc);
    return;
  }
  if (type == "cancel") {
    handleCancel(doc);
    return;
  }

  sendError("type", "unknown type: " + type);
}

bool ClientSession::rateLimitCheck() {
  auto now = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(now - rateLastRefill_).count();
  rateLastRefill_ = now;
  rateTokens_ = std::min(static_cast<double>(RATE_LIMIT_BURST),
                          rateTokens_ + elapsed * RATE_LIMIT_PER_SEC);
  if (rateTokens_ < 1.0) return false;
  rateTokens_ -= 1.0;
  return true;
}

void ClientSession::handleSubscribe(const ::rapidjson::Document& doc) {
  if (!exec_ || !dispatcher_) {
    sendError("subscribe", "server missing exec/dispatcher");
    return;
  }

  if (!rateLimitCheck()) {
    sendError("subscribe", "rate limit exceeded");
    return;
  }

  if (!doc.HasMember("requests") || !doc["requests"].IsArray()) {
    sendError("subscribe", "missing 'requests' array");
    return;
  }

  const auto& arr = doc["requests"].GetArray();

  for (auto& r : arr) {
    if (!r.IsObject()) {
      sendError("subscribe", "request must be object");
      continue;
    }

    // Request key — int (smoke.js / legacy clients) or string
    // (embassy / saved-scene). See krn/server/RequestKey.hpp.
    if (krn::server::requestObjHasBothKeyAndId(r)) {
      sendError("subscribe", "request must have key (int) OR id (int|string), not both");
      continue;
    }
    auto keyOpt = krn::server::parseRequestKeyFromObj(r);
    if (!keyOpt) {
      sendError("subscribe", "request missing valid 'key' (int) or 'id' (int|string)");
      continue;
    }
    krn::server::RequestKey key = std::move(*keyOpt);

    if (!r.HasMember("streamKey") || !r["streamKey"].IsString()) {
      sendError("subscribe", "request missing 'streamKey' string", key);
      continue;
    }
    if (!r.HasMember("field") || !r["field"].IsString()) {
      sendError("subscribe", "request missing 'field' string", key);
      continue;
    }

    const std::string streamKey = r["streamKey"].GetString();
    const std::string field     = r["field"].GetString();

    // Validate streamKey/field length to prevent absurdly large map keys.
    static constexpr std::size_t MAX_STREAM_KEY_LEN = 64;
    static constexpr std::size_t MAX_FIELD_LEN      = 128;
    if (streamKey.empty() || streamKey.size() > MAX_STREAM_KEY_LEN) {
      sendError("subscribe", "invalid 'streamKey' (empty or too long, max "
                + std::to_string(MAX_STREAM_KEY_LEN) + ")", key);
      continue;
    }
    if (field.empty() || field.size() > MAX_FIELD_LEN) {
      sendError("subscribe", "invalid 'field' (empty or too long, max "
                + std::to_string(MAX_FIELD_LEN) + ")", key);
      continue;
    }

    // Callback from Responder -> send update message over this WS session.
    // Capture weak_ptr to avoid reference cycle:
    //   ClientSession → chains_ → Responder → sendFn → ClientSession
    //
    // subId is this subscription *instance*'s coalescing identity (ENC-996).
    // A re-subscribe on the same request key gets a fresh id, so a straggling
    // frame from the shut-down Responder can never overwrite a new one.
    // handleSubscribe runs on-strand (called from onRead), so the bump is
    // unsynchronised by design.
    const std::uint64_t subId = nextSubId_++;
    auto weak = weak_from_this();
    auto sendFn = [weak, subId](const krn::server::RequestKey& reqKey,
                                const krn::StreamValue& sv) {
      try {
        auto self = weak.lock();
        if (!self) return;

        ::rapidjson::StringBuffer sb;
        ::rapidjson::Writer<::rapidjson::StringBuffer> w(sb);

        w.StartObject();
        w.Key("type");   w.String("update");
        krn::server::writeRequestKeyJSON(w, reqKey);
        w.Key("streamKey"); w.String(sv.symbol.c_str());
        w.Key("value");  krn::util::writeArgTypeJson(w, sv.value);
        // ENC-1280 / SPEC specs/2026-09-20-timestamps-on-the-wire D9: the
        // epoch-ms start of the wall-clock-aligned bucket this value belongs
        // to, stamped by TumblingWindow / BucketTime from the same boundary
        // they already align on. embassy derives baseMs (first bucket seen on
        // the stream) and the D7 bar ordinal from it.
        //
        // Emitted as a JSON integer, NOT through writeArgTypeJson's double
        // path: a current ms epoch needs 41 bits and would be mangled by the
        // float32 lane D4 rejects. rapidjson's Int64 writer is exact.
        //
        // Omitted entirely when 0 — 0 is the "not bucketed" sentinel, not a
        // 1970 epoch, and a stream with no bucket grid declares no basis at
        // all (D7's corollary). Un-bucketed update frames therefore keep the
        // exact four-key shape they have today.
        if (sv.bucketStartMs != 0) {
          w.Key("bucketStartMs"); w.Int64(sv.bucketStartMs);
        }
        w.EndObject();

        KRN_METRIC_HIT("ws.msg_out");
        // Value updates are last-value-wins: queue them on the coalescable
        // path, keyed by (subscription instance, streamKey) so a fan-out
        // subscription's streams never supersede each other.
        self->sendUpdate(sb.GetString(), subId, sv.symbol);
      } catch (const std::exception& ex) {
        krn::util::logger().log(krn::util::LogLevel::Error,
          "ws.sendFn exception",
          {{"err", ex.what()}, {"reqKey", krn::server::keyDebugString(reqKey)}});
      }
    };

    std::shared_ptr<krn::INode> terminal =
      std::make_shared<krn::nodes::Responder>(sendFn, key);

    // Build a validated request JSON object to pass to TreeBuilder.
    ::rapidjson::Document rq;
    rq.SetObject();
    auto& a = rq.GetAllocator();

    rq.AddMember("streamKey", ::rapidjson::Value(streamKey.c_str(), a), a);
    rq.AddMember("field",  ::rapidjson::Value(field.c_str(),  a), a);

    // Optional pass-through: pipeline/stages/node
    if (r.HasMember("pipeline") && r["pipeline"].IsArray()) {
      ::rapidjson::Value pipe(::rapidjson::kArrayType);
      pipe.CopyFrom(r["pipeline"], a);
      rq.AddMember("pipeline", pipe, a);
    }
    if (r.HasMember("stages") && r["stages"].IsArray()) {
      ::rapidjson::Value stages(::rapidjson::kArrayType);
      stages.CopyFrom(r["stages"], a);
      rq.AddMember("stages", stages, a);
    }
    if (r.HasMember("node") && r["node"].IsObject()) {
      ::rapidjson::Value node(::rapidjson::kObjectType);
      node.CopyFrom(r["node"], a);
      rq.AddMember("node", node, a);
    }

    // Validate pipeline/stages/node sub-trees before building
    try {
      if (r.HasMember("pipeline") && r["pipeline"].IsArray()) {
        for (const auto& elem : r["pipeline"].GetArray()) {
          if (elem.IsObject()) krn::JsonValidator::validateTree(elem);
        }
      }
      if (r.HasMember("stages") && r["stages"].IsArray()) {
        for (const auto& elem : r["stages"].GetArray()) {
          if (elem.IsObject()) krn::JsonValidator::validateTree(elem);
        }
      }
      if (r.HasMember("node") && r["node"].IsObject()) {
        krn::JsonValidator::validateTree(r["node"]);
      }
    } catch (const std::exception& ex) {
      sendError("validate", ex.what(), key);
      continue;
    }

    krn::tree::Deps deps;
    deps.store      = exec_->store();
    deps.pool       = exec_->pool();
    deps.dispatcher = dispatcher_;
    // ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3, D4 — ONE
    // STRAND PER SUBSCRIPTION. This is the site D3 names, and the granularity
    // is the point: per SUBSCRIPTION, not per session and not per symbol.
    //
    //   * Per session would serialise 256 independent charts behind each
    //     other (`MAX_SUBSCRIPTIONS`), turning one slow DAG into every DAG's
    //     latency.
    //   * Per symbol is what this ticket was re-scoped AWAY from (D4): it puts
    //     the two sides of a cross-symbol join on two strands with no ordering
    //     relation at all.
    //
    // A re-subscribe on the same request key builds a fresh DAG and so gets a
    // fresh strand, exactly like `subId` above: a straggling value from the
    // shut-down chain can never be ordered against the new one, because they
    // share nothing.
    //
    // `buildForRequest` would mint one if this line were absent. It is here
    // anyway so the production path states its own ordering guarantee rather
    // than inheriting it from a default several files away.
    //
    // Guarded on `deps.pool` for the same reason `buildForRequest`'s backstop
    // is: with no executor there is nothing to serialise, delivery is already
    // inline and already in order, and minting a pool-less Strand here would
    // additionally make the Listener answer `deliversOnOwnExecutor()` — so
    // `Dispatcher` would go inline too and the entire DAG compute would run
    // synchronously on the WebSocket read thread. The two mint sites are now
    // symmetric; the asymmetry was found by the ENC-1005 adversarial review.
    //
    // ENC-1338: the mint itself lives in `SubscriptionStrandMint::mint` and
    // stamps the strand with this site's attribution, so BOTH halves of this
    // line are gated. Deleting the call (ENC-1005's M7) makes the DAG's strand
    // read `unattributed`, because the backstop minted it. Dropping the guard
    // (M10) inside the mint reddens the mint's own test — and dropping it by
    // INLINING a mint here does not compile, because `rt::Strand::Attribution`
    // has a private constructor and `SubscriptionStrandMint` is its only
    // friend. Before ENC-1338 neither mutation reddened a single test.
    deps.strand = krn::server::SubscriptionStrandMint::mint(deps.pool);

    try {
      // Check subscription limit BEFORE building the pipeline to avoid
      // wasting resources and leaking registered listeners.
      {
        std::lock_guard<std::mutex> lk(reqMu_);
        auto it = active_.find(key);
        if (it == active_.end() && active_.size() >= MAX_SUBSCRIPTIONS) {
          sendError("subscribe", "max subscriptions reached", key);
          continue;
        }
      }

      // Build pipeline OUTSIDE the lock — buildForRequest may be expensive
      // and should not block other subscribe/cancel operations.
      auto built = krn::tree::buildForRequest(rq, deps, terminal);

      {
        std::lock_guard<std::mutex> lk(reqMu_);
        // Replace any existing request with the same key. Shut down the old
        // head AND every node in its keepAlive chain before discarding it, so
        // a replaced mid-pipeline timer node stops at replace time rather than
        // whenever its last reference happens to go (see close()).
        auto it = active_.find(key);
        if (it != active_.end() && it->second) {
          it->second->shutdown();
        }
        auto cit = chains_.find(key);
        if (cit != chains_.end()) {
          for (auto& node : cit->second) {
            if (node) {
              try { node->shutdown(); } catch (...) {}
            }
          }
        }
        active_[key] = built.head;
        chains_[key] = std::move(built.keepAlive);
      }

      // Ack
      ::rapidjson::StringBuffer sb;
      ::rapidjson::Writer<::rapidjson::StringBuffer> w(sb);
      w.StartObject();
      w.Key("type"); w.String("subscribed");
      krn::server::writeRequestKeyJSON(w, key);
      w.EndObject();

      KRN_METRIC_HIT("ws.subscribe");
      KRN_METRIC_HIT("ws.msg_out");
      sendText(sb.GetString());

      // Log structured field uses "key" or "requestId" mirroring the
      // wire so operators grepping forum's instruction_id can hop
      // straight into gma's view.
      krn::util::logger().log(krn::util::LogLevel::Info,
                              "ws.subscribe",
                              { {krn::server::isInt(key) ? "key" : "requestId",
                                 krn::server::isInt(key)
                                     ? std::to_string(std::get<int>(key))
                                     : std::get<std::string>(key)},
                                {"streamKey", streamKey},
                                {"field", field} });
    } catch (const std::exception& ex) {
      sendError("build", ex.what(), key);
    }
  }
}

void ClientSession::handleCancel(const ::rapidjson::Document& doc) {
  // Accept legacy `keys: [int,...]` AND new `ids: ["...",...]` arrays.
  // Both present in the same payload is a protocol error — keep the
  // parser tight and the failure modes obvious.
  const bool hasKeys = doc.HasMember("keys") && doc["keys"].IsArray();
  const bool hasIds  = doc.HasMember("ids")  && doc["ids"].IsArray();

  if (hasKeys && hasIds) {
    sendError("cancel", "specify keys (int) or ids (string), not both");
    return;
  }
  if (!hasKeys && !hasIds) {
    sendError("cancel", "missing 'keys' (int array) or 'ids' (string array)");
    return;
  }

  // Normalize to a vector<RequestKey>.
  std::vector<krn::server::RequestKey> toCancel;
  if (hasKeys) {
    for (auto& v : doc["keys"].GetArray()) {
      if (!v.IsInt()) {
        sendError("cancel", "keys must be integers");
        continue;
      }
      toCancel.push_back(krn::server::requestKeyInt(v.GetInt()));
    }
  } else {  // hasIds
    for (auto& v : doc["ids"].GetArray()) {
      if (!v.IsString()) {
        sendError("cancel", "ids must be strings");
        continue;
      }
      toCancel.push_back(krn::server::requestKeyStr(v.GetString()));
    }
  }

  for (auto& key : toCancel) {
    std::shared_ptr<krn::INode> root;
    std::vector<std::shared_ptr<krn::INode>> chainVec;
    {
      std::lock_guard<std::mutex> lk(reqMu_);
      auto it = active_.find(key);
      if (it != active_.end()) {
        root = std::move(it->second);
        active_.erase(it);
      }
      auto cit = chains_.find(key);
      if (cit != chains_.end()) {
        chainVec = std::move(cit->second);
        chains_.erase(cit);
      }
    }

    // Shut down the head AND every node in the keepAlive chain (outside the
    // lock) — head shutdown() does not propagate down a linear pipeline, so
    // without this a mid-pipeline timer node keeps ticking until its last
    // reference is dropped (see close()).
    if (root) root->shutdown();
    for (auto& node : chainVec) {
      if (node) {
        try { node->shutdown(); } catch (...) {}
      }
    }

    ::rapidjson::StringBuffer sb;
    ::rapidjson::Writer<::rapidjson::StringBuffer> w(sb);
    w.StartObject();
    w.Key("type"); w.String("canceled");
    krn::server::writeRequestKeyJSON(w, key);
    w.EndObject();

    KRN_METRIC_HIT("ws.cancel");
    KRN_METRIC_HIT("ws.msg_out");
    sendText(sb.GetString());

    krn::util::logger().log(krn::util::LogLevel::Info,
                            "ws.cancel",
                            { {krn::server::isInt(key) ? "key" : "requestId",
                               krn::server::isInt(key)
                                   ? std::to_string(std::get<int>(key))
                                   : std::get<std::string>(key)} });
  }
}

} // namespace krn
