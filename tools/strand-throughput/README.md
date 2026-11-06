# `strand-throughput` — the instrument SPEC §5 Q2 obliges ENC-1005 to commit

`specs/2026-09-20-krn-join-correctness/SPEC.md` §5 **Q2** rules that the
single-subscription heavy-compute regression from serializing one request DAG
(**D3**) is acceptable, **on structural grounds that need no number**. In the
same breath it strikes the two numbers that had been offered for it —
*"422,946 → 117,269 values/sec"* and *"faster in 3 of 4 profiles (up to 3.3× at
8 threads)"* — as output of an instrument that was never committed (Corrections
**C2.4**), and puts ENC-1005 under an obligation: commit one, and report
before/after at 1, 2, 4, 8 and 16 threads for **both** a single subscription and
a multi-subscription fan-out, with the load average per rep.

This is that instrument. Run it; do not quote this file's numbers without
re-running it, which is the entire point of it existing.

```bash
./run.sh                                   # 5 profiles x 5 widths x 3 reps x 2 arms
REPS=1 WIDTHS=1,4 ./run.sh                 # a quick pass
OUT=/tmp/$USER-strand.csv ./run.sh         # also tee to a file
```

It builds `krn_engine` and the instrument into a **private `mktemp -d`**, never
into the repo's `build/`, so it cannot collide with a session that is mid-build
and cannot be clobbered by one (the shared-`/tmp` hazard, ENC-1222).

## The two arms are two live code paths, not two checkouts

| arm | what it exercises |
|---|---|
| `before_pool` | `deps.strand` null: `Listener::onValue` posts straight to `rt::ThreadPool`, and `Dispatcher` posts its notification as an independent pool task. Bit for bit the pre-ENC-1005 path, which is retained — so this is a measurement of the old behaviour, not a reconstruction of it. |
| `after_strand` | `deps.strand` set: `Listener::onValue` posts to the DAG's `rt::Strand`, and `Dispatcher` delivers to that Listener inline because it answers `deliversOnOwnExecutor()`. |

Same binary, same DAG, same driver, minutes apart. If a future change deletes
the legacy path, the `before_pool` arm stops meaning anything and must go with
it.

## What is timed, and what is not

One ingress thread drives `--ticks` ticks into each of `--subs` request DAGs,
round-robin. Each DAG is `Listener(SYMi, px) → terminal`; the terminal **spins**
(not sleeps) for `--spin-ns` per value, so per-value compute occupies a core the
way real reduction work does. The clock starts before the first tick and stops
after `ThreadPool::drain()` returns, so it covers ingress plus every queued
value reaching its terminal.

Not covered: sockets, JSON, `ClientSession`'s outbox, multi-threaded ingress,
and any cache-realistic workload. This measures **delivery**.

## Measured 2026-09-20

`GMA_V3` at the ENC-1005 branch, g++ 16.2.1, AMD Ryzen 7 5800XT (8C/16T),
Fedora 44, Release `-O3`. 3 reps per cell, median reported. Raw rows, including
the 1-minute load average sampled at the start of every rep, are in
`results-2026-09-20.csv`. **Load average 3.67–6.05 throughout** — other agents
were compiling.

| profile | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| `single_heavy` (1 sub, 2 µs/value) | 1.25× | **0.66×** | **0.71×** | 1.06× | 1.09× |
| `single_heavier` (1 sub, 20 µs/value) | 1.04× | **0.52×** | **0.27×** | **0.14×** | **0.13×** |
| `single_light` (1 sub, 0 µs/value) | 1.00× | 1.51× | 1.64× | 1.85× | 2.00× |
| `fanout_heavy` (8 subs, 2 µs/value) | 1.24× | 1.22× | 0.88× | 1.13× | 1.22× |
| `fanout_light` (8 subs, 0 µs/value) | 0.96× | 0.91× | 1.08× | 1.06× | 1.31× |

*(ratio = `after_strand / before_pool`; below 1.00 is a regression.)*

Three things this says, and one it does not.

1. **The regression is real, it is the single-subscription heavy-compute case
   Q2 names, and it is not a constant.** `single_heavier`'s `after_strand`
   column is flat at **≈49,500 values/sec at every width** — which is exactly
   `1 / 20 µs`, i.e. one core. That is the structural claim ("a per-DAG ordering
   guarantee necessarily serialises that DAG") measured rather than argued. The
   *ratio* therefore tracks how many cores the unordered arm was using, which is
   why it deepens from 0.52× to 0.13× as the pool widens while the absolute
   throughput does not move at all. **Quoting a single ratio for this
   regression is meaningless without the width and the per-value compute** —
   which is the second reason the struck `0.28×` was not usable.
2. **In most profiles the strand is FASTER.** `rt::ThreadPool` is one
   mutex-guarded queue (`src/rt/ThreadPool.cpp:24-31`) and the legacy path posts
   twice per value into it; the strand path posts once per *drain burst* and
   then loops locally, so it takes the global lock far less. `single_light` at
   16 threads is 2.00×. This is the same *claim* the struck "3 of 4 profiles,
   up to 3.3×" made — it is restated here only because it has now been
   measured, and it is **not** offered as a vindication of that figure, whose
   own provenance remains unrecoverable.
3. **Fan-out is where production lives** (`ClientSession` permits 256
   concurrent subscriptions, `include/krn/server/ClientSession.hpp:233`), and
   both fan-out profiles are flat-to-better.

**What it does not say:** nothing here re-opens Q2. The ruling is structural and
explicitly independent of the magnitude — *"even if ENC-1005 measures worse than
4×, the answer stays yes"* — and at 20 µs/value on 16 threads it does measure
worse than 4×. The record now exists; the decision is unchanged.
