#!/usr/bin/env bash
# ENC-1005 — SPEC specs/2026-09-20-krn-join-correctness §5 Q2's instrument.
#
# Builds krn_engine and the throughput instrument into a PRIVATE mktemp -d and
# runs the four profiles Q2 asks for. Nothing is written into the repo's own
# build/ directory, so this never collides with a session that is mid-build —
# and nothing is written to a fixed /tmp path, which is the ENC-1222 hazard
# (the scratchpad and /tmp are shared with every other live agent).
#
#   ./run.sh                    # the four profiles, 3 reps, widths 1,2,4,8,16
#   REPS=1 WIDTHS=1,4 ./run.sh  # a quick pass
#   OUT=/path/results.csv ./run.sh
#
# Output is CSV on stdout and, if OUT is set, tee'd to that file. Every row
# carries the 1-minute load average sampled at the start of that rep: this
# machine runs several agents compiling continuously and a throughput figure
# with no load beside it cannot be re-checked.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

REPS="${REPS:-3}"
WIDTHS="${WIDTHS:-1,2,4,8,16}"
TICKS_HEAVY="${TICKS_HEAVY:-20000}"
TICKS_LIGHT="${TICKS_LIGHT:-200000}"
SPIN_HEAVY="${SPIN_HEAVY:-2000}"
SPIN_HEAVIER="${SPIN_HEAVIER:-20000}"
TICKS_HEAVIER="${TICKS_HEAVIER:-4000}"
SUBS_FANOUT="${SUBS_FANOUT:-8}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
echo "# build dir: $work" >&2

cmake -S "$repo" -B "$work/build" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DKRN_BUILD_TESTS=OFF >/dev/null
cmake --build "$work/build" --target krn_engine -j"$(nproc)" >/dev/null

# Boost/RapidJSON come from the same places the CMake build found them; the
# engine is header-light enough that the include dirs below are all it needs.
g++ -std=c++20 -O3 -DNDEBUG \
    -I"$repo/include" \
    "$here/strand_throughput.cpp" \
    "$work/build/libkrn_engine.a" \
    -lpthread -lssl -lcrypto \
    -o "$work/strand_throughput"

run() {   # label subs ticks spin
  "$work/strand_throughput" --label "$1" --subs "$2" --ticks "$3" \
      --spin-ns "$4" --threads "$WIDTHS" --reps "$REPS"
}

emit() {
  # one header, from the first profile only
  run single_heavy  1              "$TICKS_HEAVY" "$SPIN_HEAVY"
  run single_light  1              "$TICKS_LIGHT" 0            | tail -n +2
  run fanout_heavy  "$SUBS_FANOUT" "$TICKS_HEAVY" "$SPIN_HEAVY" | tail -n +2
  run fanout_light  "$SUBS_FANOUT" "$TICKS_LIGHT" 0            | tail -n +2
  # The regression Q2 rules on is a FUNCTION OF THE PER-VALUE COMPUTE, not a
  # constant of the change: one DAG gets one core, so the deeper the compute the
  # closer `after/before` gets to 1/cores. This profile exists to show that
  # slope rather than to quote a single ratio — it is 10x the heavy spin.
  run single_heavier 1             "$TICKS_HEAVIER" "$SPIN_HEAVIER" | tail -n +2
}

if [[ -n "${OUT:-}" ]]; then
  emit | tee "$OUT"
else
  emit
fi
