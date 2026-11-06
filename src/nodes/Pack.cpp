#include "krn/nodes/Pack.hpp"
#include "krn/util/Logger.hpp"
#include <stdexcept>

namespace krn {

Pack::Pack(std::vector<std::string> names,
           std::shared_ptr<INode> downstream,
           JoinBy by,
           std::string outStreamKey)
  : names_(std::move(names)), by_(by), outKey_(std::move(outStreamKey)),
    downstream_(std::move(downstream)) {
  if (names_.empty())
    throw std::invalid_argument("Pack: at least one field required");

  // ENC-1292 / SPEC section 5 Q6 — same invariant as Aggregate's: a join that
  // ignores the symbol has no identity of its own and must be given one.
  if (by_ == JoinBy::None && outKey_.empty())
    throw std::invalid_argument(
      "Pack: by:\"none\" requires a non-empty output streamKey — a join that "
      "ignores the symbol has no identity of its own and must be given the "
      "request's top-level 'streamKey' (SPEC "
      "specs/2026-09-20-krn-join-correctness section 5 Q6)");
}

void Pack::addPort(std::shared_ptr<INode> port) {
  ports_.push_back(std::move(port));
}

void Pack::onPortValue(std::size_t idx, const StreamValue& sv) {
  if (stopping_.load(std::memory_order_acquire)) return;
  if (idx >= names_.size()) return;

  // ENC-1292 / SPEC D1, Q6 — the declared correlation key, and the identity
  // the assembled Record is emitted under. See Aggregate::onPortValue.
  const std::string& joinKey =
      (by_ == JoinBy::None) ? outKey_ : sv.symbol;

  Record rec;
  std::shared_ptr<INode> ds;
  {
    std::lock_guard<std::mutex> lk(mx_);

    // ONE lookup, on ONE key — see the note in Aggregate::onPortValue on why
    // a find/emplace pair on two different keys is an equivalent-mutation
    // surface no test can gate (ENC-1292 mutation M18).
    auto [it, inserted] = state_.try_emplace(joinKey);
    if (inserted) {
      if (state_.size() > MAX_SYMBOLS) {
        state_.erase(it);
        krn::util::logger().log(krn::util::LogLevel::Warn,
          "Pack: max symbols reached, dropping", {{"symbol", sv.symbol}});
        return;
      }
      it->second.latest.resize(names_.size());
    }

    auto& st = it->second;
    if (!st.latest[idx].has_value()) ++st.filled;
    st.latest[idx] = ArgValue{sv.value};

    if (st.filled < names_.size()) return;   // not yet complete for this symbol

    // Assemble the record from the latest per-field values, in declared order.
    rec.fields.reserve(names_.size());
    for (std::size_t i = 0; i < names_.size(); ++i)
      rec.fields.push_back(RecordField{names_[i], *st.latest[i]});
    ds = downstream_;
  }

  // ENC-1280: a record is completed by the field that arrived last, so the
  // record belongs to that field's bucket. Slots filled in earlier buckets
  // are last-value-wins already — this does not make them older than they
  // are, it dates the record by its completion.
  if (ds) ds->onValue(StreamValue{joinKey, ArgType{std::move(rec)}, sv.bucketStartMs});
}

void Pack::shutdown() noexcept {
  stopping_.store(true, std::memory_order_release);

  std::vector<std::shared_ptr<INode>> ports;
  {
    std::lock_guard<std::mutex> lk(mx_);
    state_.clear();
    downstream_.reset();
    ports.swap(ports_);
  }
  for (auto& p : ports)
    if (p) p->shutdown();
}

} // namespace krn
