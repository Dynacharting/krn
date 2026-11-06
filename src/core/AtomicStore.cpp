// src/core/AtomicStore.cpp
#include "krn/AtomicStore.hpp"
#include <algorithm>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

namespace krn {
namespace {

// ── ENC-1340: writer starvation under a continuous reader population ────────
//
// THE HAZARD. `std::shared_mutex` is, on libstdc++/glibc, a `pthread_rwlock_t`
// built with DEFAULT attributes, i.e. PTHREAD_RWLOCK_PREFER_READER_NP. A
// reader that arrives while other readers hold the lock is admitted
// immediately EVEN IF a writer is already blocked. So a set of readers whose
// acquisitions overlap — which is what a read-heavy loop with little work
// between reads produces — can hold the shared count above zero indefinitely,
// and the writer waits for as long as they keep going. There is no bound on
// that wait; it is starvation, not slowness.
//
// THE MEASUREMENT (this is not a theoretical shape). It is the mechanism
// behind tests/integration/ConcurrencyContentionTest.cpp's
// MultiReaderMultiWriterNoTornReads — 4 readers calling get() with zero work
// in between, against 4 writers. 40 runs on an otherwise-quiet 16-core box,
// loadavg 5.1-12.1, each capped at 25 s:
//
//     30 runs   1075-1370 ms    (healthy mode, median 1209 ms)
//     10 runs   2814 ms - >25 s (starved mode: 25% of runs)
//      of which 3 ran past the 25 s cap
//
// The distribution is BIMODAL, not a long tail — a mean over it describes no
// run that ever happened. Uncapped, one such run was clocked at 71 s against a
// 1.2 s median (ENC-1338). The test always PASSES, which is why it went
// unnoticed for so long; what it corrupts is every wall-clock measurement made
// over a suite containing it, and this repo's verification discipline runs the
// binary dozens of times per mutation sweep.
//
// THE MITIGATION. A writer publishes that it is queued BEFORE it blocks, and
// an arriving reader yields kReaderYieldsWhenWriterQueued times while that is
// true, giving the shared count a chance to drain to zero.
//
// THE BOUND IS MEASURED, NOT PICKED. Sweeping it on the same box (15 runs per
// row, 25 s cap, loadavg ~4-5; `reads` is the reader throughput the test now
// records, against 80,000 writes):
//
//     yields   starved   slowest run   mean reads   min reads
//        0      3 / 15      >25 s        47.4 M      18.5 M     <- the bug
//        1      0 / 15       629 ms      492 K       455 K      <- chosen
//        2      0 / 15       649 ms      380 K       358 K
//        4      0 / 15       609 ms      454 K       322 K
//        8      0 / 15       187 ms       93 K        64 K
//       64      0 / 15        21 ms      2.3 K         -
//
// Read that table in both directions, because the second direction is the trap.
// Downward, any non-zero value kills the starvation. Upward, the read counts
// collapse: at 64 the readers manage 2,335 reads against 80,000 writes, i.e.
// the test "passes" in 14 ms having barely overlapped the writers at all — a
// concurrency test turned into a no-op by its own fix. That is not a
// hypothesis; it is what the first attempt at this change did, and
// MultiReaderMultiWriterNoTornReads's reader-throughput assertion (added by
// ENC-1340 for exactly this reason) is what caught it.
//
// 1 is therefore the right value and the largest one is the wrong instinct:
// it is sufficient, and it preserves the most overlap. The baseline's 47 M
// reads are not a target to recover — those reads are the pathology itself,
// readers spinning for 25 s because the writers cannot get in.
//
// It is deliberately a HINT and deliberately BOUNDED:
//   * bounded, so a continuous write stream cannot starve readers — the exact
//     mirror of the bug being fixed. A reader stands down for one yield and
//     then takes the lock regardless of what the writers are doing. Strict
//     writer preference (a hand-rolled fair rwlock, or
//     PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP) would trade this bug for
//     that one.
//   * costs the read path ONE acquire-ordered atomic load when no writer is
//     queued, which is the uncontended steady state, and nothing else. A fair
//     rwlock would put a std::mutex round trip on every get().
//   * needs no non-portable rwlock attribute and no platform #ifdef —
//     specs/2026-09-16-krn-code-review-remediation D9 wants a portable,
//     reproducible default build.
//
// What it does NOT claim: it is not a fairness guarantee. A reader that has
// spent its yield still overtakes a queued writer. It converts an unbounded
// starvation into a bounded delay, which is the property the data path
// actually needs.
constexpr int kReaderYieldsWhenWriterQueued = 1;

// Publishes "a writer is queued" for exactly as long as the writer is blocked
// on the unique lock. Released explicitly once the lock is held — readers that
// arrive after that point would block on the shared lock anyway, so keeping
// them spinning would be pure waste. Exception-safe: the destructor releases
// if lock acquisition threw.
class WriterQueueTicket {
public:
  explicit WriterQueueTicket(std::atomic<unsigned>& queued) : queued_(&queued) {
    queued_->fetch_add(1, std::memory_order_release);
  }
  WriterQueueTicket(const WriterQueueTicket&)            = delete;
  WriterQueueTicket& operator=(const WriterQueueTicket&) = delete;
  void release() noexcept {
    if (auto* q = queued_) {
      queued_ = nullptr;
      q->fetch_sub(1, std::memory_order_release);
    }
  }
  ~WriterQueueTicket() { release(); }

private:
  std::atomic<unsigned>* queued_;
};

} // namespace

void AtomicStore::setCaps(std::size_t maxStreamKeys, std::size_t maxFieldsPerStreamKey) {
  WriterQueueTicket ticket(_writersQueued);
  std::unique_lock lock(_mutex);
  ticket.release();
  _maxStreamKeys         = maxStreamKeys;
  _maxFieldsPerStreamKey = maxFieldsPerStreamKey;
}

void AtomicStore::set(const std::string& streamKey, const std::string& field, ArgType value) {
  WriterQueueTicket ticket(_writersQueued);
  std::unique_lock lock(_mutex);
  ticket.release();

  auto skIt = _data.find(streamKey);
  if (skIt == _data.end()) {
    if (_maxStreamKeys > 0 && _data.size() >= _maxStreamKeys) {
      // Cap reached — drop the write rather than evict.
      return;
    }
    skIt = _data.emplace(streamKey, FieldMap{}).first;
  }

  auto& fm = skIt->second;
  if (!fm.contains(field)) {
    if (_maxFieldsPerStreamKey > 0 && fm.size() >= _maxFieldsPerStreamKey) {
      return;
    }
  }
  fm[field] = std::move(value);
}

void AtomicStore::setBatch(const std::string& streamKey,
                           const std::vector<std::pair<std::string, ArgType>>& fields) {
  WriterQueueTicket ticket(_writersQueued);
  std::unique_lock lock(_mutex);
  ticket.release();

  auto skIt = _data.find(streamKey);
  if (skIt == _data.end()) {
    if (_maxStreamKeys > 0 && _data.size() >= _maxStreamKeys) {
      return;
    }
    skIt = _data.emplace(streamKey, FieldMap{}).first;
  }
  auto& fm = skIt->second;

  for (const auto& [key, val] : fields) {
    if (!fm.contains(key)) {
      if (_maxFieldsPerStreamKey > 0 && fm.size() >= _maxFieldsPerStreamKey) {
        continue;
      }
    }
    fm[key] = val;
  }
}

std::optional<ArgType> AtomicStore::get(const std::string& streamKey, const std::string& field) const {
  // Bounded stand-down so a queued writer can get in — see the ENC-1340 note
  // at the top of this file. One atomic load in the common (no writer queued)
  // case; the loop body runs only while a writer is actually blocked.
  for (int i = 0; i < kReaderYieldsWhenWriterQueued &&
                  _writersQueued.load(std::memory_order_acquire) != 0; ++i) {
    std::this_thread::yield();
  }

  std::shared_lock lock(_mutex);

  auto skIt = _data.find(streamKey);
  if (skIt == _data.end()) return std::nullopt;

  const auto& fieldMap = skIt->second;
  auto fieldIt = fieldMap.find(field);
  if (fieldIt == fieldMap.end()) return std::nullopt;

  return fieldIt->second;
}

// ENC-1486. See the header for why a parity test needs this and what a
// candidate-key comparison missed without it.
//
// Takes the SAME writer-queued yield the other readers take: this runs inside
// a test binary that also runs the ENC-1340 contention suite, and a reader
// that skips the stand-down is the reader that reintroduces the starvation
// that suite exists to bound.
std::vector<std::tuple<std::string, std::string, ArgType>>
AtomicStore::snapshot() const {
  for (int i = 0; i < kReaderYieldsWhenWriterQueued &&
                  _writersQueued.load(std::memory_order_acquire) != 0; ++i) {
    std::this_thread::yield();
  }

  std::vector<std::tuple<std::string, std::string, ArgType>> out;
  {
    std::shared_lock lock(_mutex);
    std::size_t n = 0;
    for (const auto& [streamKey, fieldMap] : _data) n += fieldMap.size();
    out.reserve(n);
    for (const auto& [streamKey, fieldMap] : _data) {
      for (const auto& [field, value] : fieldMap) {
        out.emplace_back(streamKey, field, value);
      }
    }
  }

  // Sorted by (streamKey, field) only — ArgType has no operator<, and sorting
  // by value would be meaningless anyway: a (key, field) pair is unique, so
  // the first two elements are already a total order.
  std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) {
    if (std::get<0>(a) != std::get<0>(b)) return std::get<0>(a) < std::get<0>(b);
    return std::get<1>(a) < std::get<1>(b);
  });
  return out;
}

} // namespace krn
