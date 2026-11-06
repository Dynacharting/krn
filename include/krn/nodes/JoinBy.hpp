// include/krn/nodes/JoinBy.hpp
#pragma once
#include <stdexcept>
#include <string>

namespace krn {

// ---------------------------------------------------------------------------
// THE DECLARED CORRELATION KEY FOR A FAN-IN JOIN
// ENC-1292 / SPEC specs/2026-09-20-krn-join-correctness D1, D6, section 1.1
// defect 3, and section 5 Q3 (RULED 2026-09-20) and Q6.
// ---------------------------------------------------------------------------
//
// THE DEFECT THIS EXISTS FOR. Until ENC-1292 both fan-in nodes keyed their
// pending state on `sv.symbol` — `Aggregate`'s `buf_[sv.symbol]` and `Pack`'s
// `state_.find(sv.symbol)` — so NO JOIN ACROSS TWO STREAMKEYS WAS POSSIBLE.
// That is not a bug in one node; it is the expressiveness ceiling of the whole
// system. Measured: `Aggregate(2)` over AAPL+MSFT with one tick on each side
// emits nothing, and 23 of the 52 `Aggregate` requests in the checked-in
// corpus ask for exactly that join.
//
// So the key is DECLARED IN THE JSON rather than hardcoded:
//
//   `by:"streamKey"`  DEFAULT. Today's semantics — correlate by `sv.symbol`.
//                     Correct for a bid/ask join on one symbol, which is what
//                     29 of the 52 corpus `Aggregate`s are. It is the default
//                     so that every stored forum graph keeps its meaning with
//                     NO MIGRATION (D6): forum emits `Aggregate` as the only
//                     join shape it can produce and its stored graphs are
//                     unmigratable from KRN.
//
//   `by:"none"`       Correlate by PORT ALONE, ignoring the symbol. This is
//                     the cross-symbol join. Every port's values land in one
//                     shared pending tuple, so AAPL on port 0 and MSFT on
//                     port 1 complete each other.
//
// OUTPUT IDENTITY UNDER `by:"none"` (SPEC section 5 Q6, which this header
// answers). D1 originally said a `by:"none"` join "emits under the request's
// own top-level `streamKey`, which `buildForRequest` already holds" — a true
// premise with a conclusion that does not follow, and it is STRUCK in the SPEC
// (Corrections C2.3). The outbound frame serialises `sv.symbol`
// (`ClientSession.cpp`'s `w.Key("streamKey"); w.String(sv.symbol...)`), never
// the request's `streamKey`, so holding the string in the builder puts it
// nowhere on the wire. Left alone, a cross-symbol join would report `AAPL` or
// `MSFT` depending on which side happened to arrive second — under a race.
//
// The design step D1 did not name, taken here:
//
//   * THE FAN-IN NODE ITSELF substitutes the symbol, because it is the only
//     thing that knows both the join key and the request's `streamKey`. The
//     builder passes the request's top-level `streamKey` (`defaultStreamKey`)
//     into the node's constructor; the node emits under it.
//   * THE SUBSTITUTION IS CONDITIONAL ON `by:"none"` AND MUST STAY SO. Under
//     `by:"streamKey"` the per-symbol output identity is correct and D6
//     forbids changing what a stored graph means, so that path emits under
//     `sv.symbol` exactly as before — bit for bit.
//   * A `by:"none"` JOIN WITH NO OUTPUT IDENTITY IS REFUSED, at build time and
//     again in the node's constructor. `buildForRequest` already rejects an
//     empty top-level `streamKey`, so this is reachable only through
//     `buildTree`/`buildNode` with no default — but a join emitting under the
//     empty symbol is a silent wrong answer, which is the shape D7 exists to
//     convert into a loud one.
//   * CONSEQUENCE, STATED BECAUSE IT IS A CONSEQUENCE AND NOT A DETAIL:
//     `Worker` and `Pack` downstream also key their state on `sv.symbol`, so
//     substituting at the fan-in merges their per-symbol state into ONE
//     logical stream. That is what a cross-symbol join wants — "the price
//     difference between AAPL and MSFT" is one series, not two — but it is a
//     real behavioural consequence of the substitution.
//
// `by` IS A CLOSED VOCABULARY (SPEC section 5 Q3, ruled). This is the part that
// is easy to skip and is most of the value. `JsonValidator::validateTree` is an
// explicit open-vocabulary walk — "every member is checked / recursed into
// based on its value's type, not its key name" (src/core/JsonValidator.cpp) —
// it bounds string length, array size and depth and NEVER looks at a key. The
// builders then read named members through `strOr`/`sizeOr` WITH DEFAULTS. So
// without an allowlist, `by:"streamkey"` (mis-cased), `by:"origin"` or
// `by:"typo"` would each pass validation, silently take the `"streamKey"`
// default, and return a PLAUSIBLE WRONG NUMBER with no diagnostic in either
// repo. Hence:
//
//   1. accept `"streamKey"` and `"none"`;
//   2. reject `"origin"` by name, as RESERVED AND NOT IMPLEMENTED — it is
//      exactly the same-upstream-event semantics D2 rejected (a provenance
//      token on the value) and section 4 names as future work, so holding the
//      name stops a future join being given an incompatible spelling;
//   3. reject every other value, mis-casing included, rather than defaulting.
//
// Reserving the name without (3) would be worth very little: the hole is the
// typo, not the future feature.
//
// WHERE THE CLOSED VOCABULARY IS AND IS NOT ENFORCED — say it out loud, because
// an unstated boundary is how the next `by` typo gets through. `joinByFor` in
// `src/core/TreeBuilder.cpp` is called from exactly the two fan-in builders,
// so `by` is meaningful only on `Aggregate` and `Pack`. `by` on ANY OTHER node
// type used to be accepted and silently ignored — `{"type":"Listener",…,
// "by":"typo"}` built and ran, which is Q3's own failure shape one node over
// (found by adversarial review, ENC-1292). It is now REFUSED in `buildOne`,
// for every node type, before the type's builder is reached. Two more holes
// the same review found and this file closes:
//
//   * a DUPLICATE `by` member made the vocabulary order-dependent. RapidJSON
//     keeps both and `v["by"]` returns the first, so `{"by":"none","by":"typo"}`
//     was ACCEPTED while `{"by":"typo","by":"none"}` was refused. A repeated
//     `by` is now refused outright rather than resolved by position.
//   * a WHITESPACE-ONLY output identity satisfied Q6's guard, because
//     `buildForRequest` only rejects an empty `streamKey`. A join emitting
//     under `" "` is the same silent wrong answer on the wire as one emitting
//     under `""`, so `by:"none"` now requires an identity with at least one
//     non-whitespace character. (The general weakness in `buildForRequest` is
//     older than this ticket and is NOT changed here: tightening it would
//     change what every stored graph means, which is exactly what D6 forbids.)
//
// A CONSEQUENCE OF THE SUBSTITUTION, recorded because it is surprising and is
// not what the "one logical stream" note below describes. Because a
// `by:"none"` join rewrites `sv.symbol`, a fan-in DOWNSTREAM of one — carrying
// the DEFAULT `by:"streamKey"` — sees both branches under the request's key
// and therefore completes a tuple across what were two different streamKeys.
// That is the substitution working as designed (the upstream joins genuinely
// are one stream now), but the outer node's behaviour is decided by its
// upstream's `by` rather than its own. It requires an explicit `by:"none"`, so
// no stored graph reaches it. Gated by
// `FanInJoinKey.InnerByNoneMakesAnOuterDefaultFanInSeeOneStream`.
enum class JoinBy {
  StreamKey,   // "streamKey" — the default. Correlate by sv.symbol.
  None,        // "none"      — correlate by port alone. The cross-symbol join.
};

namespace detail {
// The rejected value is echoed back so the author can see their own spelling,
// but it arrives from untrusted client JSON, so it is bounded here rather than
// trusted to be short. `JsonValidator` caps a string's length; this caps what
// reaches a log line.
inline std::string quoteForDiagnostic(const std::string& raw) {
  constexpr std::size_t kMax = 48;
  if (raw.size() <= kMax) return "\"" + raw + "\"";
  return "\"" + raw.substr(0, kMax) + "...\" (" + std::to_string(raw.size()) +
         " chars)";
}
} // namespace detail

// Parse a declared `by` value. Throws `std::runtime_error` — which every
// builder call site already converts into a `{"type":"error","where":"build"}`
// reply — on anything outside the closed vocabulary. `nodeType` names the node
// in the message ("Aggregate" / "Pack") because a request may carry several.
inline JoinBy parseJoinBy(const std::string& raw, const char* nodeType) {
  if (raw == "streamKey") return JoinBy::StreamKey;
  if (raw == "none")      return JoinBy::None;

  const std::string who = std::string(nodeType) + ": ";

  // (2) — reserved, and said out loud. Do NOT quietly fold this into the
  // generic branch below: the point of reserving the name is that a client who
  // tries it learns the semantics are unimplemented rather than being told it
  // is a typo, and that a future implementation cannot be given a different
  // spelling.
  if (raw == "origin")
    throw std::runtime_error(
      who + "'by':\"origin\" is RESERVED and NOT IMPLEMENTED. It names a "
            "same-upstream-event join (correlate the values that descend from "
            "one source event), which SPEC "
            "specs/2026-09-20-krn-join-correctness D2 explicitly rejected for "
            "this release and section 4 records as future work. The name is "
            "held so that join cannot later ship under an incompatible "
            "spelling. Use \"streamKey\" (join per symbol, the default) or "
            "\"none\" (join by port alone, across symbols).");

  // (3) — everything else, including a mis-cased "streamkey". Rejecting rather
  // than defaulting is the whole ruling: a silent default here is a wrong
  // answer with no diagnostic in either repo.
  throw std::runtime_error(
    who + "unknown 'by' value " + detail::quoteForDiagnostic(raw) +
    ". 'by' is a CLOSED vocabulary and is case-sensitive: \"streamKey\" "
    "(default — correlate per symbol) or \"none\" (correlate by port alone, "
    "the cross-symbol join). \"origin\" is reserved and unimplemented. A "
    "value outside this set is refused rather than defaulted, because "
    "defaulting it would return a plausible wrong number with no diagnostic "
    "(SPEC specs/2026-09-20-krn-join-correctness section 5 Q3).");
}

} // namespace krn
