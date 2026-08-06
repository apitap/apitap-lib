# Batch CDC (`mode="log_based"`) — correctness suite and the ape-dts race

## The speed campaign (2026-08-02, evening): 15.8s → 11.9s, race won

Four levers, each measured before it was believed, in the order the
evidence arrived:

1. **Spill-free decode** — `pg_stat_replication_slots` showed the race
   window's 500K-row transaction spilling **72 MB** to pg_replslot files
   at the default `logical_decoding_work_mem=64MB`. The walsender startup
   now rides `-c logical_decoding_work_mem=1GB` in the `options` field
   (PGC_USERSET, no server config; plain-retry fallback). ape-dts CANNOT
   do this — their tokio-postgres fork strips `options` from replication
   URLs. Alone: 14.5s → 11.9s on the giant-tx shape.
2. **Overlapped windows** — a spawned apply task lands window N while the
   drain decodes N+1 (ape-dts's daemon pipeline, adopted batch-shaped);
   the slot is confirmed only behind the applier's watch channel, and the
   mid-drain keepalive reply now reports the APPLIED lsn (under overlap
   the old start_lsn reply could confirm past unapplied WAL — a real trap
   the design review caught). Window budget halves and caps at 24 MiB so
   windows rotate even on big boxes.
3. **The frame pump** — `strace -c` showed the drain client at 84% CPU,
   58% of it kernel time: 11K recvfrom/s at ~400 B each plus 16K
   epoll_wait/s (the walsender flushes per message). The socket now splits
   at connect and START_REPLICATION hands the read half to a task that
   only reads frames into a bounded channel — decode+collapse consume on
   their own core and the sender never stalls on our processing.
4. **proto v2 streaming** — the server ships a big transaction WHILE
   decoding it (blocks flush every work_mem, kept LOW on this path), so
   the client consumes concurrently with the server's WAL scan; v1
   fallback for pre-14 servers. Streamed ops buffer per xid and become
   real only at Stream Commit — the transaction-atomicity contract is
   unchanged.

Two hypotheses tested and REJECTED by measurement, for the record:
docker-proxy bypass (direct container IP: no change — the proxy is not
the bottleneck at 5 MB/s) and zero-alloc decode (ape-dts allocates MORE
per event than we do and still raced well — pipeline shape beats alloc
counts at 50-80K events/s).

| 650K-event window, after | before | after |
|---|---|---|
| official race shape (giant tx) → pg | 15.8 s | **11.9 s** (ape-dts: 13-14 s) |
| chunked → pg | 12.0 s | **10.5 s** |
| giant tx → **clickhouse** | ~15 s | **10.2 s** |
| chunked → **clickhouse** | ~15 s | **10.2 s** |
| 44 MB tier, 2.1M events | 89 s / 32.6 MB peak | **72.9 s / 32.1 MB peak** |

Every step re-validated by the full e2e suites (pg / ch / multi — all
MATCH, TOAST included) before its number was recorded.

**At the 10M scale** (`bench-cdc-10m-validate.sh`, all row-verified):
full-load parity holds post-campaign (ch 13.5/13.2 s replace/bootstrap,
pg 21.7/33.5 s — the delta is still the one-time ADD PRIMARY KEY), and a
2.5M-event window (with a 1M-row SINGLE transaction riding the v2
streaming path) drains into **clickhouse in 36.7 s** (~68K events/s,
consistent with the 650K-window rate) and postgres in 70.6 s. The honest
read on pg: at multi-million-event windows it becomes APPLY-bound — the
delete-join against a big indexed table outweighs the drain, and overlap
can only hide apply up to the drain's own duration. The next lever there
is a parallel apply (ape-dts phases its deletes/inserts across 8 workers;
our set-based phases have the same barrier structure to exploit).

**The home arena at scale** (0.5 cpu / 256 MB, the tier this project is
FOR — same 2.5M-event window in normal-sized transactions, both
row-verified): **apitap 43.6 s** (peak 245 MB, avg 0.48 cores — exactly
its ration) vs **ape-dts 92 s**. The uncapped order inverts 2.1× the
moment resources are honest. And the 30-second question at this tier:
a 10M-row full load lands in **23.0 s into clickhouse** (peak 140 MB);
pg-to-pg takes 62.4 s (peak 81 MB) — the half-core client pumping ~700 MB
of COPY binary is the bulk-path bottleneck there, a dest-tuning item, not
a CDC one.

**Full load, both tools, same 10M-row 15-column table**
(`bench-fullload-apedts.sh` — their snapshot mode, dest structure
pre-created per their contract; both row-count-complete):

| pg→pg full load 10M | uncapped | 0.5 cpu / 256 MB |
|---|---|---|
| ape-dts (snapshot, 8 workers, batch-200 INSERTs) | 238 s | 878 s |
| apitap (binary COPY, staging + swap) | **21.7 s** (11×) | **62.4 s** (14×) |

With clickhouse as the destination apitap's capped 23.0 s makes it 38×.
No mystery: row-SQL INSERTs versus the wire format the databases
themselves would use.

**The scale race, and what it costs** (same 2.5M-event window, both
row-verified): ape-dts catches up into pg in **45 s** to apitap's 60 s —
an honest loss at this shape — but the resource ledger reframes it:
apitap's run averages **1.00 core** (measured via cgroup cpu.stat under a
4-cpu cap; wall identical to uncapped) while ape-dts spends 8 apply
workers. At EQUAL cpu (the 0.5-core race above) the order inverts hard
(12 s vs 22 s). Peak RAM for the window: **630 MB**, dominated by the
1M-row single transaction buffered whole; the same event count in
normal-sized transactions runs at 32 MB. Into clickhouse apitap does the
window in 36.7 s on that same single core.

Rig: OVH VPS (16 vCPU / 61 GB), `postgres:16-alpine` source with
`wal_level=logical` and a second Postgres as destination, both on loopback.
apitap built from main; ape-dts `apecloud/ape-dts:latest`.

## Under sustained load: 40M changes, ten tables, 256 MB

A separate stress ledger — [cdc-stress.md](cdc-stress.md) — pushes ten
Postgres tables into ClickHouse with the CDC side confined to 0.5 CPU /
256 MB and the writer unconstrained: bootstrap 10M rows in **10.7 s**
(937K rows/s, peak 62 MB), then three rounds of 10M changes each, drained
at **51-56K changes/s** with peak RSS **87-91 MB** — while the replication
slot held up to **2,322 MB of WAL**. Falling behind costs disk in Postgres,
not memory in the worker. All 40,000,000 rows matched per table.

That run also found the real ceiling, and it is not row count:

> **CDC memory is bounded by your largest TRANSACTION, not by how many rows
> you change.** Logical decoding hands a transaction over as a unit, so a
> single 1M-row `INSERT … SELECT` is a ~200 MB window that no budget can
> slice — it OOM-killed a 256 MB worker. The identical 10M rows committed
> in 10K-row batches moved at 87 MB peak. Chunk bulk backfills, or run them
> as a `replace`/`append` transfer instead of through the change stream.

## Correctness first (the e2e suite)

`e2e_logbased.py` runs the whole lifecycle against live Postgres and
validates **row-by-row** (not just counts) after every stage:

| stage | events | wall | result |
|---|---|---|---|
| bootstrap (slot + snapshot-pinned full load, 100K rows) | — | 0.4 s | MATCH |
| mixed window across 6 transactions | 21 | 0.2 s | MATCH |
| empty drain (idempotence) | 0 | 0.1 s | MATCH |
| TRUNCATE + repopulate | 3 | 0.1 s | MATCH |
| heavy window (500K inserts, 100K updates, 50K deletes) | 650 000 | 15.8 s | MATCH |

The mixed window deliberately contains every trap we found while reading
ape-dts and PipelineWise:

- an UPDATE that **changes the primary key** (lands as delete-old +
  insert-new);
- an INSERT and its DELETE **inside the same window** (nets to nothing —
  a naive "last event wins" would leave a phantom row);
- an **empty string** column (PipelineWise's converter turns `''` into
  NULL; ours keeps it a value — asserted);
- an UPDATE touching a row with a **200 KB TOASTed column** that the WAL
  omits: the destination's big value must survive (asserted equal to
  `repeat('x', 200000)` afterwards). This is the single biggest
  correctness trap in batch CDC and it is why unchanged-TOAST rows leave
  the set-based path for column-masked UPDATEs.

## Head-to-head: same 650K-event window, same source, same destination shape

Both tools got a slot created *before* an identical window
(500K inserts + 100K updates + 50K deletes), then were timed until their
destination matched the source exactly (count, `sum(id)`, `sum(n)`).

| tool | catch-up | verdict |
|---|---|---|
| ape-dts (`parallel_type=rdb_merge`, batch 200, 8 workers) | **13 s** | MATCH |
| apitap `mode="log_based"` (first cut) | 28.4 s | MATCH |
| apitap `mode="log_based"` (after the fix below) | **15.8 s** | MATCH |

**What the first cut got wrong**, found by the run's own
`APITAP_DEBUG` split (`drain=12.1s apply=16.2s`): the apply used
`INSERT … ON CONFLICT DO UPDATE` for 450K rows, paying an index probe and
a heap update each. ape-dts's `rdb_merge` deletes every touched key first
and then inserts plainly — we had documented that trick and not copied it.
Clearing the delete-set **∪ the upsert keys** first and switching to a
bulk `INSERT … SELECT` took apply from **16.2 s to 2.8 s (5.8×)**.

**Where the remaining time goes, honestly.** After the fix the split is
`drain=12.8s apply=2.8s`. A `perf` profile of the drain is dominated by
`finish_task_switch` / `_raw_spin_unlock_irqrestore` — i.e. **waiting**;
our pgoutput decode and collapse barely register (≈1.5 % each). 650 000
events in 12.8 s is ≈51 K events/s, which is the throughput Postgres's
own single-threaded logical decoding delivers. The drain is bounded by the
source's WAL decoding, not by apitap.

That leaves one real structural difference: ape-dts is a **daemon** and
overlaps decoding with applying, so its wall time is roughly
`max(decode, apply)`; a batch run is `decode + apply`. With apply now at
2.8 s the two are within ~20 % of each other, and the batch model keeps
what the daemon cannot offer — no long-running process, one atomic
destination transaction per run, and the LSN watermark committed with the
data. Overlapping the drain with staging COPYs (stream every row version
into staging as it decodes, collapse in SQL at the end) is the obvious
next step and is not implemented yet.

## Head-to-head 2: pipelinewise, same recipe (2026-08-02)

Same shape as the ape-dts race — both tools' slots created BEFORE an
identical 650K-event window (500K inserts + 100K updates + 50K deletes on a
100K-row table), then timed until the destination matched the source
exactly (`count`, `sum(id)`, `sum(n)`).

| tool | catch-up | verdict |
|---|---|---|
| pipelinewise `tap-postgres` LOG_BASED → `target-postgres` | 86 s | MATCH |
| apitap `mode="log_based"` | **15 s** (drain 12.8 + apply 2.4) | MATCH |

Setup, for fairness: pipelinewise decodes via **wal2json** (built in the
source container, `with_llvm=no`), `hard_delete=true`,
`batch_size_rows=100000`, tap+target on Python 3.10 (their pinned deps
don't build on 3.13), everything on the same box over localhost. Its 86 s
is tap JSON-serializing every row + target COPYing into a temp table and
merging per 100K batch — the singer pipe pays one JSON encode/decode per
row on top of two SQL round-trips per batch. apitap's drain is bounded by
Postgres's own logical decoding (same 12.8 s as the ape-dts race); the
apply is one clear-then-insert transaction.

## Head-to-head 3: everyone at 0.5 cpu / 256 MB (2026-08-02)

Same 650K-event window in ~20K-event transactions, every catch-up inside a
`--cpus=0.5 --memory=256m` container, all three MATCH:

| tool | catch-up |
|---|---|
| apitap `mode="log_based"` | **12.0 s** (3 windows, 132 MB peak) |
| ape-dts (`rdb_merge`, 8 workers) | 22 s |
| pipelinewise LOG_BASED | 227 s |

The uncapped order flips at this tier: ape-dts's eight row-SQL workers
contend for half a core, while apitap's apply is set-based — the
destination server does the work, the client mostly waits. One honest
asterisk: a window whose largest SINGLE transaction is 500K rows buffers
it whole (v1 protocol) and measures a 307 MB peak — that shape needs the
512 MB tier, where the full window still lands in 19.7 s at 0.5 cpu.

Full-load at 10M rows, replace vs log_based bootstrap (uncapped box):
pg 23.1→33.3 s (the +10 s is `ADD PRIMARY KEY` on 10M rows — paid once),
ch 12.0→12.5 s, mysql 141.2→132.6 s, iceberg 14.8→14.8 s. The bootstrap
is the replace path plus the identity, nothing hidden.

## Multi-table: one slot vs three (2026-08-02)

Identical 3-table workload (200K rows each, then 220K events each = 660K
total), both setups row-verified:

| setup | catch-up |
|---|---|
| three single-table slots, sequential | 18.3 s (6.0 s each) |
| ONE group slot (`tables=[…]`) | **14.3 s** (−22%) |

Honest reading: the win is NOT N× — pgoutput only *renders* the rows its
publication carries, so what repeats per slot is the WAL scan/reorder, not
the rendering (two extra scans ≈ 4 s here). The structural wins are what
you can't buy back with separate slots: every member lands at the SAME
LSN each window (separate slots leave each table at its own moment), one
slot to manage instead of N retention risks, and per-run fixed cost ×1
instead of ×N — with many small tables the ~0.3–0.5 s per-call overhead
is the dominant term. Reproduce: `bench-cdc-multitable.sh`.

## Head-to-head 4: ingestr CDC (2026-08-02, v1.1.15)

Same recipe (their `postgres+cdc://` batch mode — the direct equivalent
of `mode="log_based"`: catch up between LSNs, exit), same 650K window,
slot+publication before the window, row-verified:

| tool | uncapped | 0.5 cpu / 256 MB |
|---|---|---|
| apitap `mode="log_based"` | **12.0 s** | **12.0 s** |
| ape-dts | 13-14 s | 22 s |
| ingestr `postgres+cdc://` | 39 s | 78 s |
| pipelinewise LOG_BASED | 86 s | 227 s |

Fair credit: ingestr's CDC is correct (row-matched, and its 100K
bootstrap snapshot took 2-3 s — far cleaner than pipelinewise). But the
dlt engine underneath pays the same per-event tax as every row-pipeline:
~17K events/s against our ~54K, and it halves again the moment the CPU
is honest — while apitap's number doesn't move at all between the full
box and half a core. Reproduce: `bench-cdc-ingestr.sh`.

## The 44 MB tier (2026-08-02)

The windowed drain (budget = cgroup limit minus a 24 MiB runtime reserve,
/8, floor 2 MiB) replays a **2.1M-event backlog inside a 0.5 cpu / 44 MB
container**: 104 windows, 89 s, cgroup peak **32.6 MB**, destination
byte-matched. Two real bugs surfaced building this: the pgoutput Relation
registry must outlive one window (announced once per stream), and an
existing publication must be checked for *membership*, not existence — a
dropped-and-recreated source table silently empties it. Both fixed, both
now covered by `e2e_logbased_capped.sh`.

## Reproduce

```bash
# correctness (pg | ch | my | ice)
APITAP_DEBUG=1 python e2e_logbased.py
APITAP_DEBUG=1 python e2e_logbased_dests.py ch

# the races
bash bench-cdc-showdown.sh        # vs ape-dts
bash bench-cdc-pipelinewise.sh    # vs pipelinewise

# the smallest tier
bash e2e_logbased_capped.sh
```

Raw: [logbased-cdc-raw.log](logbased-cdc-raw.log).
