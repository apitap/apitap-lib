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
| B5 | REPLACED (my call, 2026-11-05): after B4 a merge join buys little (ordered streams force one pipe). The v1 limit that bites is a non-unique GROUP BY-side key, so B5 maps a repeated key to every group it carries (CSR), each probe row counting once per match (`e2e_sql_join` case 7) | done |
| B4 | 5c: a build past its budget (40% of the limit, or `APITAP_SQL_BUILD_BYTES`) is halved by key range into passes that share the accumulators (`e2e_sql_join` case 5) | done |
| B6 | `dest=`: the answer lands as `apitap-sql://<token>` through the ordinary transfer into all 7 destinations, typed (`e2e_sql_join` case 6: pg, ch, my) | done |
| B7 | non-database sources in `apitap.sql()` (user ask 2026-11-05): an Arrow read lane for ClickHouse first (typed, takes filters), then github+api / github CSV, then gsheets. Text sources need key casting or text keys. DONE for github+api (`e2e_sql_github`), github:// and gsheets:// (wired, read whole because their encoders frame cells by position; no fixture leg yet). ClickHouse needs a RowBinary→Arrow decoder: todo | partly done |
| C | 7: incremental views (design below, C1–C4). C1 + the C2/C3 delta core done in `sql/view.rs`: MIN/MAX, sources without a change stream, WHERE conditions apitap cannot re-check and timestamp/binary columns refused at plan; `R`/`G`/`S` state, signed-image apply (bootstrap = inserts), state file with SHA-256 trailer, query fingerprint and positions. Unit test: 6 seeds × 600 random insert/delete/update/filter-crossing/key-move steps on both join sides equal a naive model after every step, and the test went RED on a planted bug (S[k] not moved on a GROUP BY-side delete). C2 done: `sql/live.rs`, `apitap.sql(..., dest=, mode="log_based")` over one Postgres table. The view's own slot and publication (`apitap_v_<fp16>`), bootstrap read at the slot's EXPORT_SNAPSHOT, drain on the changelog lane with `DrainSession::keep_old` (each update's old image in `Changes::olds`), order state → answer (replace) → slot confirm. REPLICA IDENTITY FULL required; TRUNCATE refused. Leg `e2e_sql_live` (RED on the 0.65 wheel: 0 cases pass), 5 random rounds each equal to Postgres, ~6K change images applied in 0.3 s. C3 + C4 done: the join of a Postgres and a MySQL side, each with its own position in the state file. The MySQL side bootstraps under FLUSH TABLES WITH READ LOCK + START TRANSACTION WITH CONSISTENT SNAPSHOT (text protocol: MySQL refuses FTWRL as a prepared statement) and drains with `MySession::keep_old`. Leg `e2e_sql_live_join`: six random rounds on both sides equal a recompute; RED on 0.62.1 and on a planted bug (old image not subtracted, rounds 1-6 fail). Open: `S`/`G` held on disk for 20 GB (memory today scales with the key count); a MySQL-only view has no concurrent-run guard (Postgres sides have the slot's `active`) | in progress |
| B9 | 10 GB + 10 GB (pg lineitem_10g 60.3M rows × my orders_10g 69.9M, 11.4 GB) at 0.5 CPU / 256 MB: 27.6 s → **median 9.8 s** (9.5/9.8/9.8/9.5/10.2, n=5, no APITAP_DEBUG; cpu 3.6-3.8 s, peak 181-218 MB, exact vs DuckDB). Steps: one-pass direct build with no pair staging; the probe's aggregate argument computed by Postgres as one scaled int8 (overflow reruns without it, case 9); the build's keys sent to Postgres as a get_bit bitmap (32.5M → 1.5M probe rows, case 10); the probe's unindexed MIN/MAX runs as a spawned task over 8 TID page ranges while the build reads in 16M-key chunks; each page range is probed with its own key range's bitmap as soon as the build has passed it; linear group lookup up to 16 groups. Measured and rejected: one pipe per build chunk (15.5-17.3 s), build chunks aligned to the parts' ends (10.9-12.8 s). APITAP_DEBUG adds ~1.3 s CPU (11.8/12.1 s): headline numbers are taken without it. Rivals at 10 GB, defaults, n=1: 256 MB DuckDB/polars/ADBC all OOM; 1 GB DuckDB 164.7 s exact, polars and ADBC OOM | done |
| B8 | inner-join groups nothing matched were emitted as COUNT 0 rows (found while reading for C). Fixed; `e2e_sql_join` case 8, RED before (200 groups vs 40) | done |
| D | 8: feasibility notes below; measurement before code | design written |

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

## C design: incremental views (idea 7)

```python
apitap.sql(QUERY, sources={...}, dest=CH, table="revenue_by_priority", mode="log_based")
```

The first run computes the answer exactly as `dest=` does and records each
source's CDC position. Every later run reads only the changes since those
positions and moves the answer by them. Nothing is recomputed.

**What can be maintained.** `SUM`, `COUNT`, `COUNT(x)` and `AVG` are
invertible: the deltas add and subtract exactly. `MIN` and `MAX` are not (a
delete of the minimum needs the rest of the group), so a query using them is
refused with `mode="log_based"`, naming them. The WHERE on each side is
evaluated on both the before-image and the after-image of a change. That
needs `REPLICA IDENTITY FULL` on a Postgres side; the MySQL binlog is FULL by
default, and its `binlog_row_image` is checked at the start.

**State.** This is the eager-aggregate layout from the RisingWave research:

- `G[k]`: the GROUP BY side's group per join key, which is the build map
  itself. It is persisted, because a later window must look a key up without
  re-reading the table.
- `S[k]`: the other side's aggregate contribution per join key (sum and count
  per aggregate). It is needed when a key changes group, for example when an
  order's priority is updated: `S[k]` moves from the old group to the new one.
- `R[g]`: the answer's accumulators per group.

For the 5 GB case, `G` is a 30 MB u8 array, `S` is about 4.16M keys × (16 B
sum + 8 B count) ≈ 100 MB, and `R` is 5 rows. `S` and `G` live in a local
state directory (`APITAP_STATE_DIR`) as flat files that a window rewrites
by mmap-free chunked IO, so the 256 MB cage holds only the window. The
destination keeps the answer table plus one `_apitap_state` row per source
with its position and the state files' content hash. A state directory that
does not match that hash is rebuilt by a full first run, never trusted.

**Window apply, per change:**

- *Other-side row inserted or deleted, or updated in a way that crosses its
  filter:* `S[k] += ±contribution`. If `G[k]` exists, `R[G[k]] += ±contribution`.
- *GROUP BY-side row with key `k`, whose group goes from `g0` (or none) to
  `g1` (or none):* `R[g0] -= S[k]`, `R[g1] += S[k]`, and `G[k] = g1`.
  Duplicate keys on the GROUP BY side (B5) need a count per (k, g); v1 of C
  refuses them.
- *A key move* (`UPDATE` of the join key) is a delete of the old key plus an
  insert of the new one.

**Atomicity.** A window commits the new `R` (replace of the small answer
table through the sink's swap), then both sources' positions in one state
write. After a crash between the two, the old positions replay a window whose
`R` has already landed. That is safe only if a replay recomputes `R` from
`G`/`S` checkpoints, not from `R`. So the state files are written before the
answer and carry the positions they reflect, and a replay starts from the
state files' positions, which are idempotent.

**Order of work:**

- C1: invertibility and filter checks in the planner, and the state files with
  their hash.
- C2: a single-source incremental aggregate (one table).
- C3: the join, with both CDC streams drained in one run (pgoutput +
  binlog), positions per source.
  C3 note (2026-11-06): the MySQL log_based bootstrap captures the binlog
  position BEFORE its load and relies on idempotent applies over the overlap.
  A view cannot: an overlap event the snapshot already holds would count
  twice. The view's MySQL side bootstraps the way mysqldump
  `--single-transaction --source-data` does: `FLUSH TABLES WITH READ LOCK`,
  `START TRANSACTION WITH CONSISTENT SNAPSHOT`, read the position, `UNLOCK
  TABLES`, then read the table inside that one transaction. The write stall
  is the lock's milliseconds (it needs RELOAD). The two streams need no
  common order: every state move is a signed sum, so each source's snapshot
  plus its own changes, applied in any interleaving, lands the same `R`.
- C4: legs. Each is RED as a recompute-vs-incremental mismatch, with random
  insert/update/delete/key-move traffic on both sides, and every window
  checked against a full recompute.

## D design: bootstrap from a backup (idea 8): feasibility notes only

walshadow loads a table from a physical base backup or a wal-g backup with no
SQL scan of the source. The parts that would transplant into apitap:

1. Read heap pages straight from a base backup stream
   (`BASE_BACKUP` over the replication protocol, or object-store files).
2. Decode tuples with the catalog of the backup's moment.
3. Handle TOAST from the backup's TOAST relation.
4. Resolve visibility from `pg_xact`.

Each part is a large subsystem. walshadow spends about 100K lines of Rust and
a shadow Postgres on it, it needs Postgres 16+, and the base backup carries
the whole cluster even for one table. apitap's measured COPY-based bootstrap
already moves 10M rows in under a minute at 0.5 core, so the case for
idea 8 is load on the source, not speed.

**Feasibility measurement before any code:** the source CPU and IO of the COPY
bootstrap against a `BASE_BACKUP` stream of the same table on the rig. If a
backup-based load does not cut source load by a large factor at the bench
table sizes, idea 8 is dropped with that receipt.
