#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/io_context.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include <rapidjson/document.h> // easiest: include real type here (prevents forward-decl mistakes)

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "krn/rt/Strand.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/server/RequestKey.hpp"

namespace krn {
namespace server {

// ENC-1338 / ENC-1005 / SPEC specs/2026-09-20-krn-join-correctness D3, D4.
//
// THE PRODUCTION SUBSCRIPTION-STRAND MINT. `ClientSession::handleSubscribe`
// calls `mint()` once per subscription and puts the result in
// `tree::Deps::strand`. It is D3's named mint site, the one every real
// subscription goes through.
//
// It is a named type rather than two inline lines because those two lines could
// be DELETED WITH A GREEN SUITE. ENC-1005 mutation-tested its own work ten ways
// and published the two that reddened nothing — M7 (delete this mint) and M10
// (drop its null-pool guard) — both because `tree::buildForRequest`'s backstop
// quietly mints a replacement that no assertion could tell apart.
//
// THIS CLASS IS THE SOLE HOLDER OF `rt::Strand::Attribution`. That is what
// makes the gate a gate rather than a convention: an attributed `Strand` can
// only be constructed inside `mint()`, so a mint inlined at the call site —
// guarded or not — cannot carry the tag and is caught by the origin assertion
// in `tests/ws/SubscriptionStrandMintTest.cpp`. An earlier draft used a
// file-local string literal for this and an adversarial review broke it in two
// independent ways; see the comment on `rt::Strand::Attribution`.
class SubscriptionStrandMint {
public:
  // Returns null when `pool` is null, and that is the guard M10 removes: with
  // no executor there is nothing to serialise, delivery is already inline and
  // already in order, and a pool-less Strand would additionally make the
  // `Listener` answer `deliversOnOwnExecutor()` — so `Dispatcher` would go
  // inline too and the entire DAG compute would run on the WebSocket read
  // thread. The backstop has always had this guard; before ENC-1005 the
  // production site did not.
  static std::shared_ptr<krn::rt::Strand> mint(krn::rt::ThreadPool* pool);

  // The `rt::Strand::origin()` tag `mint()` stamps. Compare against this rather
  // than re-spelling the literal.
  static const char* origin() noexcept;
};

} // namespace server

class WebSocketServer;
class ExecutionContext;
class Dispatcher;
class INode;

class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
  using tcp = boost::asio::ip::tcp;
  using Ws  = boost::beast::websocket::stream<tcp::socket>;

  ClientSession(tcp::socket socket,
                WebSocketServer* server,
                ExecutionContext* exec,
                Dispatcher* dispatcher);

  // Start the WebSocket handshake and begin reading messages.
  void run();

  // Send a text frame to the client on the LOSSLESS path (no-op if the session
  // is closed). Protocol frames — acks, errors, cancels — go here: they are
  // never coalesced and never shed.
  void sendText(std::string s);

  // Gracefully close the WebSocket (idempotent).
  void close();

private:
  // --------------------------------------------------------------------
  // Outbound flow control (ENC-996)
  //
  // Outbound frames fall into two classes:
  //   * lossless    — subscribe/cancel acks and errors. These carry protocol
  //                   state; dropping one desynchronises the client.
  //   * coalescable — value updates. A newer value for the same
  //                   (subscription, streamKey) fully supersedes a pending
  //                   one, so the queued frame is REPLACED rather than
  //                   appended. Value streams are last-value-wins by nature;
  //                   a chart that is behind wants the newest sample, not a
  //                   backlog of stale ones.
  //
  // Consequence: once past COALESCE_WATERMARK the queue length is bounded by
  // the number of live (subscription, streamKey) pairs instead of by producer
  // rate, so a producer that outruns the socket degrades to "latest value
  // wins" instead of being disconnected (the pre-ENC-996 policy).
  //
  // DROPS ARE STALE-FIRST (ENC-1072). Whenever a value frame has to be thrown
  // away, the one thrown away is the OLDEST candidate, never the arriving one:
  //   * below MAX_OUTBOX_SIZE a newer value supersedes the pending value for
  //     its own key (coalescing);
  //   * at MAX_OUTBOX_SIZE a newer value displaces the oldest pending value in
  //     the whole outbox, because there is no free slot to append into.
  // So the last value a producer emits is always either queued or coalesced
  // into a queued frame, and once production stops the queue drains it — which
  // is what "the newest value always arrives" has to mean for a chart that
  // would otherwise sit on a stale number forever. Before ENC-1072 the bound
  // dropped the ARRIVING frame, which broke that guarantee at exactly the
  // point it mattered most.
  //
  // What the bound still costs: the final value of a key that fell silent
  // while other traffic kept arriving can be displaced by a newer value for a
  // different key. A key that keeps producing always wins a slot back.
  //
  // One residual case sheds the newest value, counted separately as
  // `ws.outbox_shed_newest`: an outbox holding MAX_OUTBOX_SIZE frames that are
  // ALL lossless. There is no value frame to trade against, and protocol state
  // must not be dropped to make room for a sample.
  // --------------------------------------------------------------------
  struct CoalesceKey {
    std::uint64_t sub{0};      // per-session subscription instance id; 0 = lossless
    std::string   streamKey;   // one subscription can fan out over many streams
    bool operator==(const CoalesceKey& o) const noexcept {
      return sub == o.sub && streamKey == o.streamKey;
    }
  };

  // Borrowed view of a CoalesceKey, so the hot path can probe the index
  // without materialising a std::string.
  struct CoalesceKeyView {
    std::uint64_t    sub{0};
    std::string_view streamKey;
  };

  struct CoalesceHash {
    using is_transparent = void;
    static std::size_t mix(std::uint64_t sub, std::string_view sk) noexcept {
      std::size_t h = std::hash<std::string_view>{}(sk);
      h ^= static_cast<std::size_t>(sub) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
      return h;
    }
    std::size_t operator()(const CoalesceKey& k) const noexcept {
      return mix(k.sub, k.streamKey);
    }
    std::size_t operator()(const CoalesceKeyView& k) const noexcept {
      return mix(k.sub, k.streamKey);
    }
  };

  struct CoalesceEq {
    using is_transparent = void;
    bool operator()(const CoalesceKey& a, const CoalesceKey& b) const noexcept {
      return a.sub == b.sub && a.streamKey == b.streamKey;
    }
    bool operator()(const CoalesceKey& a, const CoalesceKeyView& b) const noexcept {
      return a.sub == b.sub && a.streamKey == b.streamKey;
    }
    bool operator()(const CoalesceKeyView& a, const CoalesceKey& b) const noexcept {
      return a.sub == b.sub && a.streamKey == b.streamKey;
    }
  };

  struct OutFrame {
    std::string payload;
    CoalesceKey key;  // key.sub == 0 → lossless
    bool coalescable() const noexcept { return key.sub != 0; }
  };

  void doRead();
  void onRead(boost::beast::error_code ec, std::size_t bytes);

  // Queue one outbound frame. MUST be called on-strand. `ckey` non-null marks
  // the frame coalescable.
  void enqueue(std::string payload, const CoalesceKeyView* ckey);
  // Drop the oldest coalescable frame (never the one currently in flight) to
  // make room for a lossless frame. Returns false if there is nothing to shed.
  bool evictOldestCoalescable();
  // Index of the oldest coalescable frame that is safe to touch, or
  // outbox_.size() if there is none.
  std::size_t oldestCoalescableIndex() const;
  // Overwrite the frame at `i` in place with a newer coalescable frame,
  // keeping every absolute sequence number valid.
  void replaceFrame(std::size_t i, std::string payload, const CoalesceKeyView& ckey);
  void reindexOutbox();
  // Emit the one-per-session "this client is behind" Info line.
  void noteBackpressure(const char* policy);

  // Send a value update on the COALESCABLE path. `subId` is the per-session
  // subscription instance id; `streamKey` disambiguates fan-out subscriptions
  // (e.g. GroupSplit) so one stream's update never supersedes another's.
  void sendUpdate(std::string payload, std::uint64_t subId, const std::string& streamKey);

  void startWrite();
  void onWrite(boost::beast::error_code ec, std::size_t bytes);

  void handleMessage(const std::string& text);
  void handleSubscribe(const ::rapidjson::Document& doc);
  void handleCancel(const ::rapidjson::Document& doc);
  // sendError — emit an `{"type":"error","where":…,"message":…}` frame.
  //
  // ENC-1396: `reqKey`, when present, adds the request's identity to the frame
  // via `writeRequestKeyJSON` — `"key": <int>` for an int subscription,
  // `"requestId": "<string>"` for a string one, the same rendering `update`,
  // `subscribed` and `canceled` already use. Without it a `subscribe` carrying
  // N requests of which one is rejected produced a frame naming neither the
  // failed request nor the survivors, and the client had to re-derive which by
  // noticing whose `subscribed` ack never arrived.
  //
  // It is std::nullopt for CONNECTION-level failures, which have no request to
  // name: a payload that is not JSON, a missing/unknown `type`, a malformed
  // `subscribe`/`cancel` envelope, and the three per-request rejects that fire
  // BEFORE a key has been parsed (not an object / both `key` and `id` / no
  // valid key). Those keep the original three-key shape.
  //
  // Additive on the wire, so a client ignoring unknown keys is unaffected.
  void sendError(const std::string& where, const std::string& message,
                 const std::optional<krn::server::RequestKey>& reqKey = std::nullopt);

private:
  WebSocketServer*  server_{nullptr};
  ExecutionContext* exec_{nullptr};
  Dispatcher* dispatcher_{nullptr};

  Ws ws_;
  // Serialize all async stream ops on the stream's OWN executor (the
  // per-connection strand the socket was accepted on). Beast's keep-alive
  // ping / idle-timeout timers also run on this executor, so binding our
  // reads/writes to it keeps control frames and user writes on one strand.
  Ws::executor_type strand_;
  boost::beast::flat_buffer buffer_;

  std::atomic<bool> open_{false};
  std::uint64_t sessionId_{0};

  // Outbound write serialization (Responder can call sendUpdate from worker
  // threads). All outbox_ / coalesceIndex_ / writing_ access is on-strand.
  //
  // MAX_OUTBOX_SIZE is now a memory bound, not a kill switch: coalescing keeps
  // a well-behaved session far below it, and a session that still reaches it
  // sheds the OLDEST queued update to make room for whatever arrives next,
  // whether that is a lossless frame or a newer value (ENC-1072). Only a queue
  // made up ENTIRELY of lossless protocol frames can still force a close —
  // that really is unbounded memory growth.
  static constexpr std::size_t MAX_OUTBOX_SIZE = 4096;
  // Low-water mark: below this depth the outbox is a plain lossless FIFO, so a
  // consumer that is keeping up (or only briefly behind) still receives EVERY
  // value — coalescing from depth 1 would silently drop intermediates even for
  // a healthy client, since on a fast link there is essentially always one
  // write in flight. Above it the queue is treated as a backlog and
  // coalesce-latest engages. 256 frames of a single subscription is well under
  // 32 KB, and bounds staleness to 256 samples before degradation starts.
  static constexpr std::size_t COALESCE_WATERMARK = 256;
  std::deque<OutFrame> outbox_;
  // Absolute sequence number of outbox_.front(). coalesceIndex_ maps a frame's
  // coalesce key to its absolute seq, which stays stable as the deque drains
  // (unlike a raw index).
  std::uint64_t outboxHeadSeq_{0};
  std::unordered_map<CoalesceKey, std::uint64_t, CoalesceHash, CoalesceEq> coalesceIndex_;
  bool writing_{false};
  bool backpressureLogged_{false};  // one Info line per session, not per frame

  // Per-session subscription instance counter. Assigned on-strand in
  // handleSubscribe and captured by the Responder's send callback, so two
  // subscriptions (or a re-subscribe on the same request key) never coalesce
  // into each other.
  std::uint64_t nextSubId_{1};

  // Active requests for this session. Variant key supports both
  // smoke.js's int-keyed wire and embassy's string-id wire.
  std::mutex reqMu_;
  std::unordered_map<krn::server::RequestKey, std::shared_ptr<INode>> active_;
  std::unordered_map<krn::server::RequestKey, std::vector<std::shared_ptr<INode>>> chains_; // keeps pipeline alive

  // Rate limiting: token-bucket for subscribe requests
  static constexpr int    RATE_LIMIT_BURST    = 20;   // max burst of subscribes
  static constexpr double RATE_LIMIT_PER_SEC  = 5.0;  // sustained rate
  static constexpr std::size_t MAX_SUBSCRIPTIONS = 256; // max concurrent subscriptions
  double rateTokens_{static_cast<double>(RATE_LIMIT_BURST)};
  std::chrono::steady_clock::time_point rateLastRefill_{std::chrono::steady_clock::now()};
  bool rateLimitCheck();
};

} // namespace krn
