# KRN Data Layer — L1 ClickHouse, krn as the only compute

**Design reference:** [`docs/data-layer.md`](../../docs/data-layer.md) (revision 3).
**Origin ticket:** ENC-1478. **Date:** 2026-10-14.

---

## 0. TL;DR

Raw Databento **trades + mbo** (L3 per-order), **one symbol, seven days**, lands in a
single-node self-hosted ClickHouse **L1** store. No derived layers. ClickHouse is indexed
columnar **storage and selection only**; krn performs **100% of the computation** through
one implementation of every operator, shared by live, replay, chart fill and headless
research. A **shim** derives the minimal lossless `SELECT` from a krn manifest and streams
column blocks into krn with no row materialization.

The first deliverable is a **measurement**, not a feature: whether krn can consume rows as
fast as ClickHouse delivers them (`B2 ≥ B1`). Everything else is sized by that number.

---

## 1. Why

krn today has no history, no persistence and no replay. Three separate needs converge on
one layer:

1. **Replay / backtest** — run a graph over a past window.
2. **Chart fill** — a DynaCharting view attaches and needs historical bars; no such path
   exists today (`customer-layer/src/api/history.ts` is pipeline-build history, not market
   data).
3. **Headless research** — krn as a query engine transforming tables for research
   processes, outside DynaCharting entirely.

And the provider is changing: off DTN IQFeed onto Databento, with Databento Plus (live
TotalView) out of budget, so historical bulk pull into our own store is the only route.

The obvious design — compile a krn graph to SQL and let ClickHouse execute it — **is
refused**, see D1.

---

## 2. Locked decisions

**D1 — ClickHouse selects, krn computes. One implementation of every operator.**
A transpiler producing SQL for krn's math yields small numerical differences even when
correct: float accumulation order, bucket edge inclusivity, EMA seeding, NaN/empty-bucket
handling, timestamp tie-breaking. This system's business is reading differences of that
size as **signal**, so a discrepancy manufactured by the storage layer is
indistinguishable from an indication. A diff-based parity gate cannot rescue it — two
engines never agree bit-for-bit on floating-point aggregation, so the gate needs a
tolerance, and the tolerance *is* the noise floor. `docs/data-layer.md` §1.

**D2 — Only lossless selection may be pushed down, and a lint enforces it.**
Permitted: `WHERE` on `symbol`/`ts_event`/`provider_seq`, partition pruning, column
projection, `ORDER BY` matching the physical PK, `LIMIT` on a raw page only. Forbidden
without exception: aggregates, window functions, `GROUP BY`, time bucketing, any
expression in the projection list, computed `MATERIALIZED`/`ALIAS` columns. Enforced by
`krn_shim_sql_lint` at build time over the 272-entry request corpus — a structural
guarantee on query *shape*, not a numerical diff. §1.2–1.3, §5.4.

**D3 — Performance comes from transport, never from semantics.**
krn's per-value costs (a `std::string` symbol copy, a virtual call per node, a
`ThreadPool::post` hop per non-strand node, an `AtomicStore` write under a `shared_mutex`,
a rapidjson DOM at ingress) dominate at MBO rates. Removing them is **bit-identical by
construction** — same functions, same order, same arithmetic. Reimplementing any operator's
math in a form that changes accumulation order is a second implementation and is forbidden
by D1. §6.

**D4 — No derived layers. Projections are allowed and are not a layer.**
No L2, no rollup, no bar table, no feature store: research is ad hoc with no stable access
pattern, so a derived layer is a bet on queries nobody has written, and any *computed*
layer violates D1. ClickHouse projections (same data, second physical sort order) are an
index, not a model — deferred as pointless at one symbol. §8.1–8.2.

**D5 — Plain `MergeTree`; re-ingest by `DROP PARTITION` + reload.**
Not `ReplacingMergeTree`: its dedup is exact only on a unique sort key, and `book_events`
is not unique (one message yields a book mutation *and* a print), so a background merge
could silently delete a real row in a way indistinguishable from a gap. Dropping dedup
dissolves that hazard **and** avoids merge amplification on an MBO-sized table.
`gma.ingest_windows` is the authority on what is loaded. §8.6.

**D6 — The raw DBN files are retained; L1 is a rebuildable projection of them.**
Load-bearing, not hygiene: D5's reload must reload *from something*, and re-pulling from
Databento spends the plan allowance this architecture exists to work around. It also makes
a schema change a replay rather than a re-purchase, and protects a single un-replicated
local disk. If the files are ever not retained, `ReplacingMergeTree` with a provably-unique
`book_events` key becomes mandatory. §8.7.

**D7 — `symbol` is the ticker; `instrument_id` is retained; `gma.symbology` holds the
mapping.** DBN's `instrument_id` is dataset-scoped and reassignable while charts, manifests
and krn's `streamKey` all want `"AAPL"`. Keying L1 on `instrument_id` would make every
query a join and the store unreadable by hand. The mapping table goes in **now, at one
symbol, because it costs nothing now and cannot be retrofitted.** Ticker reuse after a
delisting is detectable via `valid_from`/`valid_to` and otherwise out of scope. §8.9.

**D8 — Prices are `Int64` at a fixed global 1e-9 scale.** Matches DBN exactly: no per-row
scale column, no per-symbol lookup on decode, an exact integer path with no double round
trip. `OrderBookManager`'s per-symbol tick stays a book-quantization concern. §C9.

**D9 — Operators key off event time only, and the three clock nodes are wall-clock today.**
`StreamValue` has no event-time field; `BucketTime`/`TumblingWindow` use
`system_clock::now()` + `wait_until` and `Interval` uses `cv.wait_for`. A 10-minute window
replayed through `TumblingWindow(60s)` yields **one** bar — a different *number* of outputs,
so no tolerance recovers it. `StreamValue` gains `eventTimeNs`; the window nodes gain
event-time drivers; the wall-clock thread survives in live mode only. `Interval` is defined
**live-only** and excluded from replay and research. §C1, §14.2.

**D10 — Warm-up depth is derived from the graph, and emission is one rule.**
The planner computes `effective = requested − warmup` by walking the graph for the deepest
history requirement, then **emit iff `ts_event >= requested.fromNs`**. A human setting
warm-up per request gets it wrong silently, in the direction of too little, producing output
that looks plausible and is warm-up garbage. §5.1, §C4.

**D11 — Ingest completeness is recorded, never inferred; an incomplete window fails.**
"Quiet market", "provider hole" and "not ingested yet" are indistinguishable in a plain
`SELECT`, and the third silently returns a flat market. `gma.ingest_windows` is written by
the ingest job and `IL1Store::isComplete` makes a query over an incomplete window **fail**
rather than answer short. Row counts cannot substitute: a quiet window and a missing window
have the same count. §8.8.

**D12 — trades + MBO puts every execution in the store twice; the planner refuses both.**
An execution is a `trades` row *and* MBO trade/fill records — two views, not duplicates, but
a graph reading both double-counts volume. `ObTradeEvent::bookNeutral` is the existing
live-path precedent. The planner already derives which `kinds` a graph touches and must
**refuse** a graph reading `trades` and `book_events` for the same symbol and window.
Reconciliation is a semantic choice masquerading as a convenience. §C13, Q15.

**D13 — Measurement precedes construction.** `B1` (rows/sec ClickHouse → krn, transport
only) and `B2` (rows/sec through the DAG) run before the shim, event time or any §6 change
is built. Target `B2 ≥ B1`. Neither needs event time, because a raw-scan benchmark uses a
non-windowed graph. Confound to design around: krn and ClickHouse share one machine and
compete for cores and page cache, so `B2 ≥ B1` can be satisfied for the wrong reason —
pinned cores or a sequential protocol, and a capped `max_server_memory_usage`. §14.

*D13 status, 2026-11-10:* B1 (ENC-1485, krn-data #5) and B2 steps 1–2 (ENC-1487, krn-data #4)
have run, **on the 2000-row real T5 sample only**. They give **no `B2 ≥ B1` verdict and no
transport choice**; both wait for a multi-granule sample, and B2 is re-measured after
ENC-1631. Numbers and scope: §5, *B1 and B2 on the T5 sample*.

**D14 — Two deployment profiles, so ClickHouse being required is not a contradiction.**
Research/ingest runs a single-node self-hosted ClickHouse by design; the client profile is
live-only or reads a central L1 over the wire with no local store. Parquet and DuckDB are
**dropped entirely** — DuckDB was the largest dependency in the design and existed only to
query Parquet. §C10, §15.

---

## 3. What changes in each repo

| repo | change | when |
|---|---|---|
| **workspace** | `tools/stack.sh up clickhouse` — rootless podman, single node, memory-capped | first slice (T1) |
| **krn** | `data/` → `krn_data` library: schema, Databento adapter, ingest, shim; `Dispatcher::onRecord`; benchmarks; later event time + `ReplaySource` + sinks | first slice + post-measurement |
| **embassy** | seed `baseMs` from the fill window; pre-size `createBuffer` | deferred (chart fill) |
| **treaty** | wire change if Q1 resolves toward a second time basis | deferred |
| **customer-layer** | chart-fill request surface | deferred |

Dependency direction is unchanged: `krn_data` depends on `krn_engine`; `krn_engine` depends
on neither it nor any connector. **Arrow and the ClickHouse client must never become engine
dependencies.**

---

## 4. Non-goals

- Any derived layer, rollup, bar table or feature store (D4).
- Transpiling krn graphs to SQL, in any form (D1).
- Vectorized or columnar re-implementation of any operator (D3).
- `SplicedSource`'s live half — nothing exercises it until Databento live exists.
- Live TotalView / Databento Plus.
- `quotes` (L1 top-of-book) population: no `mbp-1` in the plan; TOB comes from the
  reconstructed book.
- Sharding (§7) and projections (§8.2) — premature at one symbol.
- Chart fill (§10), including infinite scroll-back, which the current wire cannot express.
- Corporate actions, adjusted prices, symbol-change continuity.
- Retention, compaction, multi-provider merge.
- Parquet and DuckDB.

---

## 5. Open questions

Resolve before the ticket that depends on each. None block the first slice.

| id | question | blocks |
|---|---|---|
| **Q1** | `bucketStartMs` under event time: amend `specs/2026-09-20-timestamps-on-the-wire` D9/D10, or carry both bases on the wire? *Recommend amend.* | the wire half of event time |
| **Q5** | Is bit-identical output promised for *all* graphs, or only those avoiding C11's execution-time-sampling case? *Honest answer: the latter.* | replay parity fixture |
| **Q7** | Cross-symbol graphs (`by:"none"`, ENC-1292) cannot shard. Unsharded, or symbol-group shard, and who picks the group? | sharding |
| **Q8** | Long-interval fill: accept the raw scan, or one narrow exception to D4? | after B3 |
| **Q10** | Chart fill depth at scene-init: accept bounded attach-with-N-bars, or change the wire? *Recommend accept.* | embassy ticket |
| **Q11** | Persist derived series as a cache? If yes, invalidation key is graph identity. | after B3 |
| ~~**Q14**~~ | ~~Databento client library, or decode DBN directly?~~ **RESOLVED by T4 (ENC-1482): decode DBN ourselves.** See Q14 below. | — |
| **Q15** | Confirm the planner **refuses** a graph reading `trades` + `book_events` for one symbol/window (D12) rather than reconciling. *Recommend refuse — and T5 measured the evidence: 157 of 157 `mbo` executions are also `trades` rows at the same `(ts_event, sequence)`.* | the planner |

**Q14 — resolved by T4 (ENC-1482): we decode DBN ourselves; we do not link
`databento-cpp`.** The deciding number is dependency weight, and it is larger than it looks.
`databento-cpp` is a full client SDK requiring OpenSSL, zstd, nlohmann_json, cpp-httplib and
HowardHinnant/date — and since it delegated DBN decoding to the Rust `dbn` crate's C-FFI, a
**Rust toolchain** as well, inside a C++20/CMake project that vendors its dependencies under
`third_party/`. That is a much bigger step than Arrow was (§15.1: 17 MB, 45 s), and D14 has
just dropped Parquet and DuckDB *specifically* to keep the client profile small. What we
actually need is small and static: two packed PODs, 56 and 48 bytes, byte-identical across
DBN v1/v2/v3 (the vendor aliases `v1::MboMsg = v2::MboMsg = v3::MboMsg`), plus a 100-byte
fixed metadata block and one variable-length symbology section.

**Stated against it, honestly:** this is a wire format we do not own and now have to track —
a DBN v4 lands on us rather than on a vendor bump — and metadata/symbology is exactly where a
hand-rolled decoder goes wrong. Three mitigations, all in the build rather than in a comment:
the framing is **measured** against real vendor bytes committed at
`krn/data/tests/fixtures/dbn/` and cross-checked against `databento-cpp`'s own decoder-test
expectations; an unknown DBN version is **refused**, never guessed; and every declared
`Action` / `Side` / `RType` enumerator is asserted to be *accepted* as well as every
undeclared byte *refused*. **Two independent adversarial sweeps**, one by the implementer and
one by a separate session that did not write the code, put **42 mutations** through the
decoder. They found one real behavioural bug — the Fill aggressor-side flip leaking onto the
book-mutation row it also emits, which nothing asserted because no test checked the `side` of
an `Update` row — plus a bound asserted by its counter rather than its invariant, two decoded
fields no test could reach, one unreachable branch, and two comments describing mechanisms
that do not exist. All fixed; the bug's test was written red first. Every one of those
mutations is now caught. Also deliberately **not** in
`krn_data`: zstd. `.dbn.zst` is decompressed by ENC-1483's ingest job, and feeding a zstd
frame to the adapter is refused by name.

**Facts T4 settled by measurement, which §8.9 of the design doc had flagged as "from
memory":**

- **The exact DBN field names and record layout.** `RecordHeader{length, rtype,
  publisher_id, instrument_id, ts_event}` (16 bytes; `length` counts 32-bit words).
  `MboMsg{hd, order_id, price, size, flags, channel_id, action, side, ts_recv, ts_in_delta,
  sequence}` (56 bytes). `TradeMsg{hd, price, size, action, side, flags, depth, ts_recv,
  ts_in_delta, sequence}` (48 bytes) — note `action`/`side` sit *before* `flags` here and
  *after* it in `MboMsg`. `Action` is `A C M T F R N`; `Side` is `A B N`; prices are `int64`
  at 1e-9 with `kUndefPrice = INT64_MAX`. The names in
  `data/include/krn/data/DatabentoAdapter.hpp`'s `dbn` namespace are the vendor's own
  spellings.
- **Metadata framing.** `"DBN"` + a version byte + a `uint32` length, then a **100-byte**
  fixed block in every version (v1 spends 8 of it on a `record_count` and 47 on reserved; v2+
  spend 2 on `symbol_cstr_len` — 22 in v1, 71 after — and 53 on reserved), then
  `layout_length` (always 0) and the symbol / partial / not_found / mappings sections.
- **DBN v3 pads its metadata to an 8-byte boundary with zero bytes; v1 and v2 do not.** The
  committed v3 fixture carries exactly 7 such bytes and the v2 one carries none. Nothing in
  the design anticipated this and it is the kind of detail that makes a decoder read every
  record at a 7-byte offset.

**Still unverified, to be established by measurement rather than assertion:** whether
`instrument_id` is stable within a dataset-day (**T5 narrowed it, did not close it** — see
below); whether `ob::Provider` consults `isStale` before serving `ob.*` (post-measurement).
`book_events` sort-key uniqueness is **measured** — see T5's block. And one T4 added:

- ~~**Does a DBN `Fill` arrive with a separate `Cancel`/`Modify` carrying the resting
  order's reduction?**~~ **RESOLVED by T5 (ENC-1483) against real MBO: YES — 91 of 91, so
  `fillReducesRestingOrder` stays `false`.** See T5's block below for the measurement. The
  original statement of the question is kept because the hazard it names is permanent for any
  future provider: DBN's own documentation says `Fill` "Does not affect the book", which
  implies yes; `ItchAdapter` does the opposite because ITCH has no such separate message and
  `routeOrderExecuted` must apply the reduction itself. **Applying both double-reduces the
  order; applying neither leaves phantom size**, and the public sample fixtures contain only
  `Cancel` records so it cannot be measured from them. The adapter defaults to DBN's contract
  (print only) and carries the alternative behind
  `DatabentoAdapter::Config::fillReducesRestingOrder`, with both branches tested. **T5
  (ENC-1483) must settle this against real MBO before the first ingest** — it is a wrong
  *answer*, not a cosmetic bug.

**Facts T5 (ENC-1483) settled by measurement, against the real Databento sample Nick
pulled** — `XNAS.ITCH`, `MSFT`, `mbo` + `trades`, requested `2022-06-10 12:30:00–14:00:00
UTC`, `limit: 1000`, CSV with `pretty_px: false` / `pretty_ts: false`. The files live at
`~/market-data/databento/XNAS.ITCH/MSFT/20220610/`, **outside every git working tree**,
because §14.2 names committing Databento data a redistribution question not worth answering.
Driven by `krn_ingest`, which transcodes CSV → the vendor's own PODs → **real DBN** via the
byte-for-byte-verified `DbnEncoder`, retains the `.dbn` beside the CSVs (D6), and decodes it
back through ENC-1482's adapter. One mapping to canonical form exists and it is the adapter's
(D1).

- **`Config::fillReducesRestingOrder` is `false`, and the default is now MEASURED.** **91 of
  91** `F` records are accompanied by a `C` for the **same `order_id`** at the same
  `(ts_event, sequence)` — the shape is `T`, `F`, `C` in one message group. The venue emits
  the resting reduction itself, so applying the fill as a second reduction double-reduces on
  **100%** of fills. T4's open question, the one flagged as "a wrong *answer*, not a cosmetic
  bug", is closed in the direction DBN's own documentation implies.
- **`ts_recv == ts_event + ts_in_delta`, 1000/1000, both kinds.** `ts_recv` is derivable and
  `ts_in_delta` is exactly the gateway delta, which supports §8.3 confining `ts_recv` to
  diagnostics. (Beware measuring this with `awk`: these are 19-digit integers, past 2^53, and
  floating-point arithmetic reports ~20% false mismatches. Measured exactly in Python and in
  C++.)
- **`book_events`' sort key is NOT unique, and this is T6/ENC-1484's number: 676 distinct
  `(symbol, ts_event, provider_seq)` over 1000 rows — 324 rows share a key with another
  row.** `trades` is 1000 distinct over 1000. The cause is visible in the first three lines of
  the file: one DBN message group yields a trade print, a fill and a cancel, all three
  carrying one `ts_event` and one `sequence`. **D5's plain `MergeTree` is therefore
  load-bearing on real data, not defensive** — a `ReplacingMergeTree` on this key would
  collapse 324 real rows in a way indistinguishable from a gap (C12). T6 still owns the
  *second* half of its question: whether adding discriminating columns to the key earns its
  insert cost.
- **`instrument_id` is constant at 7370 across the window — and this does NOT close the
  dataset-day question. Scoped, not settled.** The delivered data is 68 seconds of `mbo` and
  22 minutes of `trades` (next bullet), nowhere near a dataset-day, and reassignment is a
  between-days event. The full-day question still needs a full day.
- **D12/C13 is real and total: 157 of 157 `mbo` `action='T'` records appear in `trades` at the
  same `(ts_event, sequence)`.** Every execution is in the store twice, so the planner's
  refusal of a graph reading `trades` and `book_events` for one symbol and window (Q15) is
  load-bearing. This is the evidence for recommending **refuse**.
- **Action mix, and what it does NOT exercise:** `mbo` is `C` 407, `A` 345, `T` 157, `F` 91 —
  **no `M` and no `R`**. `trades` is `T` 1000. **The replace and clear/epoch-bump paths are
  unexercised by real data**; only the synthetic branches in `DatabentoAdapterTest` cover
  them. Stated plainly rather than implied by an absence. Canonically the 1000 `mbo` records
  become `Add` 345, `Delete` 402, `Update` 5 (the five partial cancels), `Trade` 248
  (= 157 `T` + 91 `F`). `side` `N` appears on 65 `mbo` rows (all `Trade`, all carrying
  `kGmaFlagSideUnspecified`) and on 546 of 1000 `trades` rows (all `Aggressor::Unknown`).

**And three facts T5 found that nothing in the design anticipated:**

- **⚠ THE PULL IS TRUNCATED, AND `ingest_windows` MUST RECORD THE DELIVERED WINDOW, NOT THE
  REQUESTED ONE.** Both jobs carried `limit: 1000` and returned exactly 1000 records, so the
  delivered data is a **prefix**: `mbo` spans `[12:30:01.100846675, 12:31:09.372036574]` —
  **68.3 seconds** — and `trades` spans `[12:30:01.100846675, 12:52:21.448758549]` —
  **22.3 minutes**. Recording the requested `[12:30, 14:00)` as complete would be the §8.8
  failure *in the one table that exists to prevent it*: a research run over 13:00–13:30 would
  read complete and get a flat market. So the job records
  `[requested_start, last_ts_event)` when `records == limit`, and
  `[requested_start, requested_end)` otherwise. Two clauses are deliberate: **`from_ts` is the
  requested start, not the first record's `ts_event`** — the vendor delivers everything from
  `start`, so a quiet opening second is *data*; and **`to_ts` is the last `ts_event`
  EXCLUSIVE, not `+1ns`** — the cut may have landed inside a group of records sharing one
  instant (ten records carry `mbo`'s last `ts_event`) and the file cannot say whether it did,
  so the possibly-partial final instant is dropped from the *claim* while its rows are still
  inserted. **Truncation is detectable at all only because `limit` is written into the
  retained DBN's metadata block**, which is what makes the D6 reload self-sufficient.
- **`gma.gaps` cannot be populated from a symbol-filtered pull, and zero rows is the honest
  answer.** §8.8 detects a provider hole as "a seq discontinuity", but Databento's `sequence`
  is the **venue's**, not the symbol's: consecutive MSFT records step
  `11404021 → 11404028 → 11423666`. `provider_seq` is **monotonic** within
  `(session_id, symbol)` exactly as §8.3 says — sorted, 1000/1000, both kinds — but it is not
  **contiguous**, and gap detection needs contiguity. A gap row per jump would have
  manufactured hundreds of phantom holes per kind, each of which a research run joining `gaps`
  reads as "there was no data here" — the same wrong-answer class the table exists to prevent.
  A real detector for a filtered pull needs an unfiltered channel feed or the vendor's
  `condition.json` (which for 2022-06-10 reads `available`). **New ticket, not a line in
  T5.**
- **Two batch jobs extracted into one directory leave ONE `metadata.json`, and it is silently
  the wrong one for one of the two CSVs.** Each vendor zip carries its own; the `trades` job's
  was overwritten on extraction, so the surviving file describes the `mbo` job. Since
  `start`, `end` and `limit` are exactly what `ingest_windows` forms its claim from, the job
  **refuses** a `metadata.json` whose `query.schema` does not match the CSV being read, and
  prints the `unzip -p` command that extracts per-job sidecars. It refused on the first run.

**§8.10's codec question is NOT answerable from this sample, and the reason is structural.**
1000 rows per table is under ClickHouse's `min_bytes_for_wide_part` (10 MB), so both parts are
**Compact** — one file for all columns — and `system.parts_columns` reports **0 compressed
bytes per column**. Whole-part ratios are `book_events` 70,099 → 16,322 (4.29×) and `trades`
54,090 → 14,884 (3.63×), and at this size those are dominated by per-column codec headers and
say nothing about §8.10's claim that `order_id` will dominate the table. Same reason B1/B2 are
not reported here (D13, ENC-1485/ENC-1487): fixed costs dominate.

**B1 and B2 on the T5 sample — ENC-1485 (krn-data #5) and ENC-1487 (krn-data #4).
Sample-scoped: no `B2 ≥ B1` verdict, no transport choice.** Input is the 2000-row real T5
sample above, repeated as iterations: B1 reads both tables (1000 rows each) and B2 drives the
1000 `trades` rows. Full tables: krn-data `docs/data-layer.md` §14.3 and the two tickets'
comments.

- **B1: native and HTTP `ArrowStream` are within noise.** About 0.7M rows/s cold and
  0.86–0.91M warm, with each arm inside the other's p10–p90 band in every cell. HTTP carries
  ~23–27% more wire bytes, and clickhouse-cpp makes ~2.3× the allocations per block. At
  ~1.1–1.5 ms per query the figure is per-query fixed cost, not a streaming ceiling.
- **B1's block-size sweep collapsed.** ClickHouse 25.3 does not split a granule below
  `max_block_size`, so every size from 64 to 65536 returned one 1000-row block per query.
  "Where block size stops helping" cannot be answered from this sample.
- **B1's "cold" is `posix_fadvise(DONTNEED)` eviction, read back with `mincore` as 0 resident
  pages — not `drop_caches`** (root only), so dentries, inodes and the device cache stay warm.
- **B2: `onRecord` is ~1.02× JSON `onTick`.** About 54k rows/s with 16 pool threads and 67k
  with 1, byte-identical in 399 of 399 comparisons, 4 of 4 falsifications red.
- **⚠ The B2 numbers predate ENC-1624.** The krn-data engine pin (`ca544ec`) still appended
  each value to history and recomputed atomics once per Listener, which ENC-1624 removed. B2
  is re-measured after ENC-1631 bumps the pin.
- **Open until a multi-granule sample exists:** the transport choice (§5.2) and any
  `B2 ≥ B1` comparison (D13).

**Q-level: `session_id` reuse on re-ingest — RULED, and the first answer was wrong.**
Assigned to T5 from ENC-1480.

> **A re-ingest reuses its `session_id`, and `session_id` is the identity of the retained DBN
> file rather than a per-run counter: `FNV-1a-32` over the whole `.dbn`, forced non-zero.**

The same bytes give the same number on every machine, every rebuild and every reload; a
*corrected* pull is different bytes and therefore a different session, which is the only case
where the distinction is real. The reasoning is D6 and D5: D6 makes L1 "a derived, rebuildable
projection of the raw DBN files", and a projection whose output changes when rebuilt from
unchanged input has a hidden input — a per-run counter is exactly that. D5's re-ingest and
ENC-1487's bit-identical assertions both fail on the `session_id` column alone if a reload
renumbers it. And the adapter's own note that `provider_seq` is "stable across a re-download,
which is what makes re-ingest converge and the splice's dedup exact" is about the **pair**
`(session_id, provider_seq)` (§8.3); a fresh `session_id` each run makes the stable half
useless.

**The first version derived it from the vendor `job_id`, and that was a bug caught by running
it:** the `job_id` is in `metadata.json` and is **not in DBN at all**, so the pure D6 reload —
which reads only the retained file — produced a *different* `session_id` from the run that
created the file, i.e. the reload's rows differed from the originals in exactly the column
meant to prove they did not. The `job_id` stays on every `ingest_windows` and `symbology` row
as `source_ref`; it is provenance, not identity. **Verified after the fix:** `ingest` re-run,
`reload` (DBN only, no CSV) and a hand `DROP PARTITION` followed by `reload` all produce
**bit-identical** rows — `sum(cityHash64(*))` equal across all four states, both tables.

**A correction to §8.8 that no answer to the `session_id` question rescues.** §8.8 calls
`(symbol, kind, from_ts, session_id)` "unique by construction". **That is false under every
answer.** ENC-1480 made the table an append-only transition log, so one window and one session
write at least two rows (`state=0` then `state=1`) and a failed run writes three — observed on
this ingest as exactly **4 rows for 2 windows**. The claim dates from revision 2's
one-row-per-window `ReplacingMergeTree` and was voided by the rewrite that replaced it. What
makes the table readable is the **read rule**, never uniqueness. §8.8 is amended.

**A residual hazard T5 measured and could not close in the schema.** The DDL's invariant block
makes maintaining `ingest_windows` across a `DROP PARTITION` a **code obligation of the ingest
job** — it writes a `state=3`/`state=0` row first. A `DROP PARTITION` typed at the server by
hand bypasses that entirely, and when it was, `isComplete` for the in-window request returned
**COMPLETE over an empty table**, with every coverage probe still green because the rule was
behaving correctly over a table that had been lied to. There is no foreign key and no trigger
that can prevent it. `krn_ingest verify` now carries the one sound counter-check — **a count
cannot *establish* completeness (D11), but it can *refute* a claim**: a `state=1` window
claiming `rows_inserted > 0` whose range holds **zero** rows is not quiet, it is a lie. That
check was observed red on exactly that condition and green after `krn_ingest reload`.

**One gap T4 found in ENC-1481's schema, reported rather than patched around:**
`BookRecord::side` is two-valued (`Bid`/`Ask`) and `gma.book_events.side` is its `UInt8`, but
DBN's `Side` is three-valued and `Side::None` is normal on a `Trade` record from a venue that
does not report an aggressor. `TickRecord` is fine — `Aggressor::Unknown` exists — but
`BookRecord` has no aggressor field and no "no side". The adapter marks such a row with
`kGmaFlagSideUnspecified` in the free high byte of `flags`, which is lossless and asserted;
the real fix is for `BookRecord` to carry an `Aggressor` for `BookAction::Trade` rows, or for
`side` to mirror DBN's three values. Related and deliberate: `BookAction::Trade` rows are
**unconditionally book-neutral** — DBN's own documentation for both `Action::Trade` and
`Action::Fill` is "Does not affect the book" — which is why `gma.book_events` needs no
`book_neutral` column: for a DBN-fed table the value is a constant. State that as **this
adapter's decision**, though, not as a property inherited from the ITCH model: `bookNeutral`
defaults to `false` in `FeedEvent.hpp`, and `WsFeedClient.cpp:385-387` has a live
`!e.bookNeutral` branch that *does* consume the book, so a non-neutral trade is representable
and is what a feed whose print *is* the book mutation would use. Both of `ItchAdapter`'s
production emission sites set the flag; that is the narrower true claim. A future non-DBN
provider has to state its own answer rather than inherit this one.

A related defect this work introduced and then fixed, because it is the cost of the column
`BookRecord::side` is: that column means the **resting** order's side on
Add/Update/Delete/Clear and the **aggressor**'s on a `Trade` row. `ItchAdapter` cannot mix
them — its `ObUpdateEvent`/`ObDeleteEvent` carry no side at all, and only `ObTradeEvent` gets
the flipped aggressor — but this adapter added the column, so it owns the separation. The
first version flipped one variable before dispatch, so a `Fill` under
`fillReducesRestingOrder` stamped the aggressor onto the `Update`/`Delete` row it also emits.
`DbnActionMappingTest.EveryBookMutationRowCarriesTheRestingSide` is the gate; before it
existed, inverting the side of *every* mutation row left the whole suite green.

---

## 6. Ticket breakdown

Nine tickets to a measured answer. One repo each, ≤5h each, dependency-ordered.
The 21-ticket plan in revision 2 of the design doc is the **post-measurement** plan.

| # | id | title | repo | est | deps |
|---|---|---|---|---|---|
| T1 | ENC-1479 | `[workspace] tools/stack.sh up clickhouse` — rootless podman single-node L1 | workspace | 3 | — |
| T2 | ENC-1480 | `[krn] L1 DDL` — trades, book_events, book_snapshots, symbology, ingest_windows, gaps | krn | 3 | T1 |
| T3 | ENC-1481 | `[krn] krn_data skeleton` + canonical Schema.hpp + CMake target | krn | 4 | — |
| T4 | ENC-1482 | `[krn] Databento DBN adapter` — trades + mbo → canonical batches | krn | 5 | T3 |
| T5 | ENC-1483 | `[krn] ingest job` — 7d/1symbol → L1, retain DBN, completeness rows | krn | 4 | T2, T4 |
| T6 | ENC-1484 | `[krn] measure book_events sort-key uniqueness` against real MBO | krn | 2 | T5 |
| T7 | ENC-1485 | `[krn] B1 benchmark` — ClickHouse → krn transport, native vs HTTP — **Done**, krn-data #5 | krn-data | 4 | T5 |
| T8 | ENC-1486 | `[krn] Dispatcher::onRecord` — non-JSON ingress | krn | 4 | T3 |
| T9 | ENC-1487 | `[krn] B2 benchmark` — rows/sec through the DAG — **Done** (steps 1–2), krn-data #4 | krn-data | 5 | T7, T8 |

Follow-ups filed by T7/T9:

| id | title | repo | est | deps |
|---|---|---|---|---|
| ENC-1624 | ingest appends to history and recomputes atomics once per Listener — **Done**, krn #112 | krn | 2 | — |
| ENC-1625 | B2 step 3 — inline DAG drive | krn | 4 | ENC-1631 |
| ENC-1626 | B2 step 4 — interned symbols on the value path | krn | 4 | ENC-1631 |
| ENC-1627 | B2 step 5 — store-write elision | krn | 4 | ENC-1631 |
| ENC-1628 | a `FieldSource` for BookRecords, so B2 can drive the mbo half | krn-data | 3 | — |
| ENC-1629 | `check_no_compute.py` ignores `STACK_PORT_OFFSET` | krn-data | 1 | — |
| ENC-1630 | record the sample-scoped B1/B2 results in this SPEC | krn | 1 | T7, T9 |
| ENC-1631 | bump the engine pin past ENC-1624 and re-measure B2 steps 1–2 | krn-data | 2 | ENC-1624 |

Critical path: T1 → T2 → T5 → T7 → T9. T3/T8 and T1/T2 parallelise.
