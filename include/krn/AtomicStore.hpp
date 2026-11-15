// include/krn/AtomicStore.hpp
#pragma once
#include "StreamValue.hpp"

#include <atomic>
#include <string>
#include <unordered_map>
#include <variant>
#include <shared_mutex>
#include <algorithm>
#include <optional>
#include <mutex>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

namespace krn {

class AtomicStore {
public:
  AtomicStore() = default;

  /// Configure cap enforcement. 0 means unlimited; defaults are unlimited.
  /// Apply once at boot from the engine Config; thread-safe but expected to
  /// be a single up-front call before any writes.
  void setCaps(std::size_t maxStreamKeys, std::size_t maxFieldsPerStreamKey);

  /// Write a single field. Silently dropped if a cap would be exceeded —
  /// only-on-new-key/new-field; existing entries are always updatable.
  void set(const std::string& streamKey, const std::string& field, ArgType value);

  /// Write multiple fields for a streamKey under a single lock acquisition.
  /// Same cap semantics as set(): drops only the entries that would push past
  /// a cap; existing entries always update.
  void setBatch(const std::string& streamKey,
                const std::vector<std::pair<std::string, ArgType>>& fields);

  std::optional<ArgType> get(const std::string& streamKey, const std::string& field) const;

  /// ENC-1486 — DIAGNOSTIC FULL ENUMERATION of everything the store holds, as
  /// `(streamKey, field, value)` triples sorted by key then field.
  ///
  /// Read it as an observability accessor, exactly like
  /// `Dispatcher::listenersFor` / `subscriptionCount()`: nothing in `src/`
  /// calls it, it is NOT a synchronisation point, and the returned vector is a
  /// snapshot that may be stale the instant it returns. Do not build routing
  /// or caching on it.
  ///
  /// WHY IT EXISTS. ENC-1486's parity test has to assert that
  /// `Dispatcher::onRecord` and `Dispatcher::onTick` leave IDENTICAL store
  /// contents. With no enumeration accessor the only available comparison was
  /// over a candidate key list the test derived from `FunctionMap`,
  /// `computeAllAtomicValues` and the record→field mapping — and an
  /// adversarial probe showed exactly what that misses: a one-line
  /// `_store->set(symbol, "zzGhost", 1.0)` on the record path only was
  /// INVISIBLE to the comparison, because `zzGhost` was not a name the test
  /// knew to ask for. A parity assertion that can only see keys it already
  /// guessed is not a parity assertion. This makes the comparison total.
  ///
  /// Deliberately sorted and deliberately by value: `_data` is an
  /// `unordered_map`, so iteration order is an implementation detail and two
  /// stores holding the same contents must compare equal regardless of
  /// insertion history.
  std::vector<std::tuple<std::string, std::string, ArgType>> snapshot() const;

  // ── ENC-1627: THE PULL-READ SET (B2 step 5, store-write elision) ──────────
  //
  // Design: specs/2026-10-14-krn-data-layer/SPEC.md D3. The setting that uses
  // it is `util::Config::storeWriteElision`; read that first.
  //
  // Which `(streamKey, field)` keys something reads back out of this store.
  // `AtomicAccessor` declares its key in its constructor, so every pull node
  // is in the set from the moment it is built, whichever builder built it.
  // A Dispatcher with `storeWriteElision` on writes raw fields and builtin
  // atomics only to declared keys. With the setting off nothing consults the
  // set: it is recorded and exposed, and changes no write.
  //
  // WHY IT LIVES ON THE STORE AND NOT ON THE DISPATCHER. An `AtomicAccessor`
  // holds a store pointer, not a Dispatcher, and can be built with no
  // Dispatcher in its `tree::Deps` at all. Keeping the set beside the data it
  // describes means no pull node can read this store without being in it.
  //
  // THE SET ONLY GROWS. Tearing a request down does not undeclare its key.
  // Undeclaring would stop the key being written while its last value stayed
  // in the store, so a later pull read on it would sample a stale value — the
  // one failure elision must never cause. Keeping it costs that key's write
  // per tick, which is exactly what the setting-off engine pays for it.
  //
  // THE SEAL. `sealReads()` closes the set; the eliding Dispatcher calls it at
  // the start of its first tick, before it writes anything. After the seal a
  // key already in the set is still accepted (it has been written on every
  // tick since the seal, so it is current), and a NEW key is refused:
  // `declareRead` returns false and `AtomicAccessor`'s constructor throws, so
  // the build fails rather than producing a pull node that would read a
  // missing value it would have had with elision off. Refusal was chosen over
  // backfill because backfill cannot be exact: an elided raw field's value was
  // never kept anywhere, and a flat-namespace builtin key's value depends on
  // which field last wrote it. A sealed set is also immutable, which is what
  // lets the per-tick lookups below run without a lock.
  //
  // ANYTHING ELSE THAT READS THIS STORE — an `IEventComputer` reading
  // `ctx.store`, a provider, an embedder calling `get` — and wants a raw field
  // or builtin under elision must call `declareRead` before the first tick.
  // Keys an `IEventComputer` writes itself are never elided.

  /// Declare that something reads `(streamKey, field)`. Returns true when the
  /// key is (now) in the set; false only when the set is sealed and the key
  /// is new, i.e. refused.
  bool declareRead(const std::string& streamKey, const std::string& field);

  /// Close the read set. Idempotent and thread-safe.
  void sealReads();

  bool readsSealed() const noexcept { return _readsSealed.load(std::memory_order_acquire); }

  /// The declared fields of `streamKey`, or null when it has none. ONLY valid
  /// once `readsSealed()` is true: the set is immutable from then on, so the
  /// pointer is stable for the life of the store and is read without a lock.
  /// Returns null when the set is not sealed.
  const std::unordered_set<std::string>* sealedReadFieldsFor(const std::string& streamKey) const;

  /// Diagnostic: the declared keys, sorted. A snapshot, like `snapshot()`.
  std::vector<std::pair<std::string, std::string>> readKeys() const;

private:
  using FieldMap = std::unordered_map<std::string, ArgType>;

  mutable std::shared_mutex _mutex;
  // ENC-1340: writer-starvation guard. `std::shared_mutex` on libstdc++ is a
  // `pthread_rwlock_t` with DEFAULT attributes — PTHREAD_RWLOCK_PREFER_READER_NP
  // — so an arriving reader may join a live shared acquisition even while a
  // writer is already blocked. A reader population that never lets the shared
  // count reach zero starves that writer for as long as it keeps reading, with
  // no bound. Writers publish here before they block and an arriving reader
  // stands down (bounded) while it is non-zero, so the count can drain. Read
  // the full argument, and why this is a HINT rather than a fair lock, in
  // src/core/AtomicStore.cpp.
  std::atomic<unsigned> _writersQueued{0};
  std::unordered_map<std::string, FieldMap> _data;
  std::size_t _maxStreamKeys{0};         // 0 = unlimited
  std::size_t _maxFieldsPerStreamKey{0}; // 0 = unlimited

  // ENC-1627. Guarded by `_readsMutex` until `_readsSealed`; immutable after.
  mutable std::mutex _readsMutex;
  std::atomic<bool>  _readsSealed{false};
  std::unordered_map<std::string, std::unordered_set<std::string>> _reads;
};

} // namespace krn
