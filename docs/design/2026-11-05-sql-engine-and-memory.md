# apitap.sql() and the small-memory techniques (ideas 1–8)

Started 2026-11-05. The user's decisions:

- 0.65.0 is held, and everything ships together.
- The SQL engine is our own: sqlparser-rs parses, and the planner and join
  operators are apitap's. DataFusion is out: its HashJoin cannot spill, and it
  adds 50–90 MB to the binary.
- Ideas 1–6 are implemented and measured first, then idea 7. Idea 8 gets a
  design document and a feasibility measurement before any code.

The evidence these ideas come from is the 5 GB + 5 GB join measured on the rig
(pg `lineitem_5g` × my `orders_5g`, 0.5 CPU / 256 MB):

| run | wall |
|---|---|
| plain `read()` streams into DuckDB | 76 s |
| filters pushed to the servers | 63.6 s |
| plus a hand-written key range | 32.2 s |
| best join order | 24.9 s |
| DuckDB ATTACH, same cage | 99 s; 46.9 s with the same key range |
| read + DuckDB join with 1 GB of memory | 14.7 s |

The research behind the ideas covered ClickHouse joins, DataFusion,
RisingWave and walshadow. Only ideas are taken from walshadow, which is
AGPL.

## The eight ideas

1. **Automatic key-range runtime filter.** After pushdown, ask each join side
   for the min/max of its key, intersect the ranges, and push the result into
   both scans. This is the ClickHouse runtime filter / DataFusion dynamic filter
   in SQL form, measured at 63 → 32 s.
2. **A join structure chosen from (min, max, n) of the build keys:**
   - a direct-address array when range × payload is small;
   - otherwise a narrow struct-of-arrays open-addressing hash;
   - low-cardinality payloads are dictionary-coded, never stored as Strings.
3. **Aggregation fused into the probe.** GROUP BY accumulators are updated as
   rows probe, so the join output is never materialized.
4. **CDC transactions spill to disk.** A transaction past
   `APITAP_TX_BUF_BYTES` is written to local disk and replayed at commit, where
   today it is refused. Covers Postgres and MySQL.
5. **Memory-adaptive planning:**
   - (a) the `read()` pipe planner prices pipes by the projection, not by a
     fixed 22 MB calibrated on a 15-column table;
   - (b) an explicit `parallel=` is clamped to what fits the limit, with a
     note, never left to OOM;
   - (c) a join whose build does not fit falls back to key-range passes
     (re-scan with `key BETWEEN`) or spills.
6. **Merge join** when both sides stream in key order, verified at runtime: a
   batch that breaks the order hands over to the hash path.
7. **Incremental views from CDC.** Join + aggregate state is kept, and each CDC
   window applies deltas instead of recomputing.
8. **Bootstrap from a backup** (no SQL scans of the source). Design document
   and feasibility measurement only.

## B1 design: apitap.sql() v1

```python
apitap.sql(
    "SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) "
    "FROM pg.lineitem_5g l JOIN my.orders_5g o ON l.l_orderkey = o.o_orderkey "
    "WHERE l.l_shipdate > DATE '1995-03-15' AND o.o_orderdate < DATE '1995-03-15' "
    "GROUP BY 1 ORDER BY 1",
    sources={"pg": PG, "my": MY},
).to_polars()   # an Arrow stream: polars, DuckDB, pyarrow
```

The work runs in `apitap-core::sql`, in Rust. Python only passes the strings
through.

**v1 subset.** Anything outside it is refused before any read, naming what is
unsupported:

- one table, or two tables joined `INNER JOIN … ON a.k = b.k`, one integer
  equi-key;
- `WHERE` as a conjunction in which every conjunct names one side;
- `GROUP BY` columns (names or ordinals) from one side;
- aggregates `SUM`, `COUNT(*)`, `COUNT(x)`, `MIN`, `MAX`, `AVG`, over `+ - * /`
  arithmetic on the other side's columns and literals;
- `ORDER BY` output names or ordinals, and `LIMIT`.

**Plan:**

1. Probe both schemas.
2. Push each side's conjuncts down as `where=`, with column qualifiers
   stripped and the conjunct re-rendered by sqlparser, together with that
   side's projection.
3. **Key range (idea 1).** Ask each source for the unfiltered `MIN`/`MAX` of
   its key. This is instant on a PK, and a parallel server scan otherwise.
   Intersect the two ranges and add `k BETWEEN lo AND hi` to both scans.
4. **Build side** = the side holding the GROUP BY columns.
5. **Build structure (idea 2).** Each build row's group-key tuple is
   dictionary-coded to a group id. The structure is a direct-address array
   over [lo, hi] at the narrowest width (u8/u16/u32) when span × width ≤
   64 MiB, else a `HashMap<i64, u32>` with foldhash. On the 5 GB case the span
   is 30.1M with 5 groups, so the array is u8 and about 30 MB.
6. **Probe (idea 3).** Look up each row's group id and fold its aggregate
   arguments straight into that group's accumulators. Join rows never
   exist.
7. **Arithmetic.** Decimals are exact i128 with scale tracking (`*` adds
   scales, `+`/`-` align them). Floats are f64. Integers are i64 with checked
   overflow. `/` and `AVG` produce f64.
8. **Memory.** The probe read's pipes are fitted to the memory left after the
   build structure, so the build and the reader share the limit.
9. **Result.** One Arrow batch through the existing capsule, so `to_polars()`
   and DuckDB read it. `dest=` comes in B6.

## Phases and status

| step | what | status |
|---|---|---|
| A1 | 5b: explicit `parallel` clamped to the limit (`e2e_read_parallel_clamp`, RED on 0.65.0). 5a DROPPED, see below | done |
| A2 | 4: CDC transaction spill (pg after-commit + streamed, my). Replica lanes spill, changelog keeps the refusal (A2b). Legs `e2e_tx_cap`, `e2e_my_cdc_hardening txcap` (RED on 0.65.0) | done |
| A3 | `where=` on `read()`, parsed as exactly one expression by sqlparser 0.55 in the source dialect (`e2e_read_where`, RED on 0.65.0) | done |

### A1 finding: 5a has no evidence

A sweep of the 4-column lineitem read in 64/128/256 MiB cages put the
automatic planner's picks (2/5/8 pipes) within one pipe of the best safe
count:

- At 64 MiB, 4 pipes reached 92% of the cage, and 6 or 8 were OOM-killed.
- At 128 MiB, 6 pipes were 70% and 8 were 98%.

The 2-pipe slowness seen in the join came from `APITAP_MEM_BUDGET=64M`, which
was set by hand. It was not a planner defect. The 15-column read is
server-bound under a filter (1 pipe is fastest), so it says nothing about
pricing.

### A2 design (transaction spill)

A transaction's ops are collapsed into the window after its commit
(`Collapser`, last write wins per key), so the window alone can be as large
as the table under a full-table `UPDATE`. Spilling only `tx_buf` would not
bound memory. The plan:

1. **Transactions the source ships after commit** (pgoutput v1, the MySQL
   binlog):
   - Past the limit, apply the buffered prefix as a *partial window* whose
     watermark stays at the last complete commit, then keep reading.
   - Apply is idempotent per key and the ops run in order, so a crash replays
     the whole transaction from the held watermark and converges.
   - Memory is bounded by the chunk, not by the transaction.
   - Trade-off: readers can briefly see part of the transaction. Document it.
     Changelog dedup relies on deterministic (lsn, seq), which holds because
     BEGIN carries the final LSN.
2. **Streamed in-progress transactions** (pgoutput v2 streams, which can still
   abort): spill the stream's ops to a local file until stream commit or
   abort, then replay through the path in (1). An abort deletes the file.
3. The refusal stays only for a single row larger than the limit.

Constraints found in the code (2026-11-05):

- **pgoutput streams large transactions in progress.** apitap starts
  replication with proto v2 and `streaming 'true'`, so Postgres streams a
  transaction before it commits once it passes the server's
  `logical_decoding_work_mem` (default 64 MB). Large PG transactions therefore
  almost always arrive as streams, which can still abort, so they must go
  through the disk path in (2). The MySQL binlog can carry `ROLLBACK` too, so
  it uses the same disk path.
- **ClickHouse replica rows are versioned by `_apitap_ver = WindowId::end()`.**
  A partial window that holds the watermark would share that version with the
  previous window's rows, and ReplacingMergeTree resolves equal versions by
  part order.
  - So `WindowId` gains a separate *version*: partial chunk k is versioned
    S + k (the watermark stays S), and the final chunk is versioned and
    watermarked C, the commit LSN.
  - S + k < C holds because the transaction's WAL span is far larger than its
    chunk count.
  - A replay after a crash re-applies the same images at the same versions.
  - The pg, my and bq applies are ordered upserts or MERGEs keyed by PK, so
    sequential chunk application is enough there.
- **changelog=True stamps rows with the window start, and the replay rule
  treats two windows at one stamp as re-drains of one window.** Spilled
  chunks would be trimmed or skipped. A2 v1 therefore covers replica mode.
  Changelog keeps today's refusal, with its message naming the limitation,
  until the replay plan learns about chunked windows (follow-up A2b).
| B1 | `apitap.sql()` v1 (`e2e_sql_join`, RED on 0.65.0) | done |
| B2 | 1: key-range runtime filter (unfiltered MIN/MAX intersected) | done, in B1 |
| B3 | 2 + 3: direct u8/u16/u32 or hash map, aggregation fused into the probe | done, in B1 |
| B4 | 5c: build-side fallback | todo |
| B5 | 6: merge join | todo |
| B6 | output: Arrow stream and `dest=` through the existing sinks | todo |
| C | 7: incremental views | todo |
| D | 8: design + feasibility | todo |

Each step follows the same discipline:

- a leg run RED on the current wheel, then green;
- a `gate.py --matrix` check after the leg is registered;
- the headline number measured in the 0.5 CPU / 256 MB cage against
  `lineitem_5g` × `orders_5g`;
- cargo work runs only on the VPS.

## B1 measured (2026-11-05, lineitem_5g × orders_5g, 0.5 CPU / 256 MB, no pyarrow)

`apitap.sql()` on the headline query took **12.7 s and 12.1 s** (n=2) with a
cgroup peak of 219/222 MB. It did not OOM and wrote no spill file, and the
answer equals uncapped DuckDB ATTACH digit for digit. Where the time goes, on
the second run:

| step | time |
|---|---|
| key ranges | 1.8 s |
| build orders: 3.66M keys, 5 groups, direct u8, 28 MiB | +3.2 s |
| probe lineitem: 16.2M rows, 752,872 matched | +6.5 s |

The same query, same cage:

| run | wall |
|---|---|
| read() streams into DuckDB | 76 s |
| best hand-tuned DuckDB join | 24.9 s |
| DuckDB ATTACH | 99 s; 46.9 s with the hand-written key range |
