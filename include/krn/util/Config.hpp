#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cstdint>

namespace krn {
namespace util {

class Config {
public:
  // Construct with sensible defaults.
  Config() = default;

  // Load from a simple "key=value" file. Engine-known keys are parsed
  // directly; everything else is parked in a pending list and forwarded
  // to ConfigNamespaceRegistry on dispatchPendingKeys() (called after
  // connectors register their readers in registerWith).
  // Returns true if the file was read successfully (even if some keys
  // are unknown to both the engine and any registered namespace).
  bool loadFromFile(const std::string& path);

  // Replay every key parked during loadFromFile through
  // ConfigNamespaceRegistry::dispatch(). Called by the composition root
  // after each IConnector::registerWith() has had a chance to register
  // its namespaces. Returns the number of keys that found a reader. A
  // second call dispatches nothing again and returns 0.
  std::size_t dispatchPendingKeys();

  // ENC-1595: marks a parked key as read by a plugin's own parse of the
  // file, so reportUnclaimedKeys() does not warn about it. A plugin that
  // re-reads the file (rather than registering a ConfigNamespaceRegistry
  // reader) calls this from a HostHooks callback or from registerWith()
  // for each key it used. Marking a key the file does not hold is a no-op.
  void markConsumed(std::string_view key);

  struct UnclaimedKey {
    std::string key;
    std::string file;
  };
  // Parked keys that no engine field, namespace reader or markConsumed()
  // call claimed, one entry per distinct key, in file order.
  std::vector<UnclaimedKey> unclaimedKeys() const;

  // Logs one WARN "config.unclaimed_key" per unclaimedKeys() entry and
  // returns how many. Never fails startup: an old file may carry a key that
  // moved to a plugin. krn_host calls it after dispatchPendingKeys(), so
  // every hook and connector has had its chance to claim a key.
  std::size_t reportUnclaimedKeys() const;

  // Server port
  int wsPort = 8080;

  // Thread pool size (0 = use hardware_concurrency)
  int threadPoolSize = 0;

  // History cap for Dispatcher (per symbol+field pair)
  int taHistoryMax = 1000;

  // Maximum distinct symbols tracked before rejecting new ones.
  int maxSymbols = 10000;

  // Maximum distinct fields per symbol in per-field histories.
  int maxFieldsPerSymbol = 200;

  // ENC-1008 — namespace derived builtin atomics by their source field.
  //
  // OFF (default, and today's behaviour): FunctionMap builtins are stored in a
  // FLAT per-symbol namespace keyed by bare function name, so two fields of one
  // symbol that both drive `mean` overwrite each other in the AtomicStore
  // (last field wins, ordered by the listener map). See the ATOMIC-KEY CONTRACT
  // on Dispatcher::computeAndStoreAtomics and docs/atomic-keys.md.
  //
  // ON: builtins are stored as `<field>.<fn>` (e.g. `price.mean`), so each
  // source field keeps its own derived values. This CHANGES a client-visible
  // key: `field` in a WS subscribe is exactly this string (TreeBuilder reads it
  // for both Listener and AtomicAccessor), so flipping it breaks any
  // AtomicAccessor bound to a bare builtin name. Listener push is unaffected —
  // bare-name subscribers still fire — so the migration is store-readers only.
  // Default OFF because there is no wire-level version negotiation for it.
  bool atomicKeyNamespaceByField = false;

  // ENC-1625 — run each request DAG on an INLINE executor (B2 step 3, SPEC
  // specs/2026-10-14-krn-data-layer D3).
  //
  // OFF (default, and today's behaviour): each request DAG's `rt::Strand` runs
  // on the shared `ThreadPool`, so every value a Listener receives costs one
  // `ThreadPool::post` hop before the nodes below it run.
  //
  // ON: `tree::buildForRequest` gives the DAG an inline strand
  // (`rt::Strand::makeInline()`), which drains on the caller — the ingress
  // thread — in post order. The whole DAG then runs on the thread that called
  // `Dispatcher::onTick` / `onRecord`, with no pool hop anywhere between nodes.
  // Same nodes, same order, same arithmetic: bit-identical to OFF under ONE
  // ingress thread (join-correctness D3's own precondition, its §5 Q8). The
  // cost is that ingress blocks for the DAG's compute, so this is for replay
  // and benchmarks, not a live feed. Timer nodes (Interval, BucketTime,
  // TumblingWindow) still post from their own threads, as they do when OFF.
  bool inlineDagDrive = false;

  // ENC-1627 — STORE-WRITE ELISION (B2 step 5, SPEC
  // specs/2026-10-14-krn-data-layer D3).
  //
  // OFF (default, and today's behaviour): the Dispatcher writes every admitted
  // raw field of a tick (ENC-1007) and every subscribed builtin it computes
  // into the `AtomicStore`, whether or not anything ever reads them back.
  //
  // ON: those two kinds of write go only to `(streamKey, field)` keys some pull
  // node reads — the keys `AtomicAccessor`s declared to the store when they
  // were built (`AtomicStore::declareRead`). Nothing else changes: the same
  // builtins are computed, the same values are pushed to the same Listeners in
  // the same order, the raw-admission and history caps decide exactly as
  // before, and `IEventComputer` writes are untouched. Every key a pull node
  // reads therefore holds, after every tick, exactly the bytes it holds with
  // the setting OFF; the store simply holds fewer keys.
  //
  // THE PRICE IS A CLOSED READ SET. The first tick a Dispatcher with this on
  // ingests SEALS the store's read set (`AtomicStore::sealReads`), and from
  // then on a pull node on a key nobody declared before that tick is REFUSED
  // at build time (the build throws; `ClientSession` replies with a build
  // error). A key declared before the seal stays declared for the life of the
  // store — tearing a request down never undeclares it — so rebuilding a pull
  // read on it later is always accepted and always reads a current value.
  // Backfill is not offered because it cannot be exact: the elided raw value
  // was never kept anywhere. See `AtomicStore::declareRead` for the rest.
  //
  // So this is for replay and benchmarks with a graph built up front, like
  // `inlineDagDrive`, not for a live server taking new subscriptions. Anything
  // else that reads the store — a plugin's `IEventComputer` reading
  // `ctx.store`, an embedder calling `get` — must declare its keys before the
  // first tick too, or it will see a key elided. Two documented differences
  // remain, both by design: `AtomicStore::snapshot()` holds fewer entries, and
  // under `AtomicStore::setCaps` a read key that the OFF run's unread keys
  // pushed past a cap can be admitted ON (a value gained, never one lost).
  bool storeWriteElision = false;

  // Metrics reporter
  bool metricsEnabled = false;
  int  metricsIntervalSec = 15;

  // Logging
  std::string logLevel = "info";

  // ENC-31: engine-driven ingress entries (`ingress.N.kind = ..., ingress.N.X = ...`).
  // The engine reads this list after connectors register their factories,
  // looks each kind up in IngressRegistry, and drives start/stop centrally.
  struct Ingress {
      std::string kind;
      std::unordered_map<std::string, std::string> params;
  };
  std::vector<Ingress> ingress;

  // forum-driven ingress (feed-sim-connector phase 2).
  // When forumUrl is non-empty the composition root replaces the static
  // `ingress[]` with the result of `krn::forum::ConnectorsClient::
  // fetchIngresses(forumUrl, forumTenantId, forumAuthToken)` after the
  // INI is loaded but before connectors start. Empty values disable
  // the pull (legacy INI ingress remains authoritative).
  std::string forumUrl;
  std::string forumTenantId;
  std::string forumAuthToken;

private:
  static bool parseLineKV(const std::string& line, std::string& k, std::string& v);
  static std::string trim(const std::string& s);

  // Parked unknown-to-engine keys awaiting dispatchPendingKeys(), kept
  // afterwards so reportUnclaimedKeys() can name the ones nobody claimed.
  struct PendingKey {
    std::string key;
    std::string value;
    std::string file;
    bool dispatched = false;
    bool claimed = false;
  };
  std::vector<PendingKey> _pendingKeys;
  std::unordered_set<std::string> _consumed;
};

} // namespace util
} // namespace krn
