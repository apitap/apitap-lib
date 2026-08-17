# CDC under load: 80M changes through a 256 MB box (2026-08-05)

How hard can you push log-based CDC on half a core? Ten tables into ClickHouse,
once from Postgres, once from MySQL. Everything on the CDC side is confined to
**0.5 CPU / 256 MB**; the writer runs unconstrained on the host. That is the
honest shape — in production the application is never the thing you starve.
40M rows verified per source, 80M in total.


> **Update — v0.30.0 (2026-08-08).** The measurements below are the run they
> describe and stand as recorded. Three apply-speed campaigns since (v0.28.0,
> v0.29.0, v0.30.0) moved the same 0.5 cpu / 256 MB tier to **132K changes/s**
> on the 5-column rig (MySQL binlog source; was 82–86K) and **37K/s** on a
> heavier 15-wide-column rig (~600 B/row, Postgres WAL source). The wide-row
> number sits at **92% of the wire's physics limit**: Postgres's own
> `pg_recvlogical`, receiving the identical stream to `/dev/null` in the same
> 0.5-core cage, needs 37 s for what apitap decodes, dedupes, renders and
> applies — verified exact — in 40.4 s. v0.30.0 also ships patch-part deletes:
> on ClickHouse ≥ 25.7 the per-window DELETE becomes a patch-part write
> (server-side delete work −82%, zero part rewrites — the destination stops
> churning); older ClickHouse keeps the previous path unchanged. Receipts:
> `benchmarks/cdc-apply-0.28.0.md`, `benchmarks/ch-ingest-r3.md`.
| phase | work | result |
|---|---|---|
| seed | 10 tables × 1M rows | 10,000,000 rows in Postgres |
| bootstrap | full load + slot coordinate | **10.7 s, 937K rows/s, peak 62 MB** |
| round 1 | 10M changes (1,000 transactions) | drain **196.4 s — 51K changes/s**, peak 87 MB |
| round 2 | 10M changes | drain **182.2 s — 55K/s**, peak 91 MB |
| round 3 | 10M changes | drain **179.6 s — 56K/s**, peak 87 MB |
| verify | per table, both engines | **40,000,000 = 40,000,000, 0 tables mismatched** |

Verification compares `count`, `sum(id)`, `sum(cust_id)` and `sum(amount)` per
table — aggregates that mean the same thing on both engines.

## What the numbers say

Sustained CDC throughput is ~51-56K changes/s on half a core, and it does not
decay: round 3 was the fastest of the three even though it started from the
deepest backlog. A 10M-change minute takes about three minutes to apply here,
so **to keep up with 10M changes per minute you want roughly 1.5-2 cores**, not
0.5. That is the sizing number. Everything else is detail.

**Falling behind does not cost memory.** At the peak of round 2 the replication
slot was holding **2,322 MB of WAL**, a backlog nine times the container's
entire memory limit, and the worker's peak RSS was **91 MB**. The backlog lives
in Postgres's WAL on disk, which is exactly where it should live. A CDC reader
that grows with its lag is a CDC reader that dies on the day it matters.

**Bootstrap is a different animal from apply**: 937K rows/s for the full load vs
~55K changes/s for the stream. Bulk COPY moves rows in blocks; a change stream
carries per-row identity, key lookups and upsert semantics. Expect the ratio.

## The same rig, MySQL binlog instead of Postgres WAL

Identical shape — ten tables, 1M rows each, three rounds of 10M changes in
10K-row transactions, CDC side in the same 0.5 CPU / 256 MB box:

| phase | MySQL → ClickHouse | Postgres → ClickHouse |
|---|---|---|
| bootstrap 10M rows | **9.1 s** (1,095K rows/s), peak 51 MB | 10.7 s (937K rows/s), peak 62 MB |
| round 1 · 10M changes | **116.6 s — 86K/s**, peak 67 MB | 196.4 s — 51K/s, peak 87 MB |
| round 2 · 10M changes | **121.4 s — 82K/s**, peak 67 MB | 182.2 s — 55K/s, peak 91 MB |
| round 3 · 10M changes | **120.8 s — 83K/s**, peak 66 MB | 179.6 s — 56K/s, peak 87 MB |
| log the source held | ~720 MB of binlog per 10M changes | ~1.9–2.3 GB of WAL per 10M changes |
| verify | 40,000,000 = 40,000,000, 0 mismatched | 40,000,000 = 40,000,000, 0 mismatched |

**MySQL's binlog lane is ~1.5× faster on the same box and uses less memory.**
The source-side log is roughly a third of the size too: a ROW-format binlog
event carries the row image, while Postgres WAL also carries full-page images
and index maintenance. Sizing follows: 10M changes/minute needs roughly one
core on the MySQL lane against ~1.5–2 on the Postgres one.

### One number in this run was wrong, and the run is what caught it

The MySQL drain first reported *100,000,000* changes for a 10M-change round —
ten times the truth, exactly the number of tables in the group. A probe
settled it: writing 300 changes to one table and 200 to another reported
`500` for **all ten** tables and `5,000` overall. The applied data was
correct every time (all 40M rows matched); the accounting was not — one
group-wide counter was handed back to each member instead of a per-table
count. Fixed, and the same probe now reports 300 / 200 / 0 … and 500 overall.
The throughput figures above are the corrected ones.

## The wall you will actually hit: transaction size

The first run of this test **was OOM-killed**, and the volume had nothing to do
with it. The writer inserted 1M rows per table in **one transaction**:

```sql
INSERT INTO t (…) SELECT … FROM generate_series(1, 1000000);   -- one commit
```

Logical decoding hands a transaction over as a unit. An apply is atomic or it
is nothing; no memory budget can slice one commit into pieces. One 1M-row
transaction is a ~200 MB window in a 256 MB container. It died, and it should
have.

Re-running the identical 10M-row workload committed in **10,000-row batches**
(1,000 transactions instead of 10) moved the same volume at 87 MB peak. So the
operational rule is:

> **What bounds CDC memory is the size of your largest transaction, not the
> number of rows you change.** A 256 MB worker is comfortable with commits up to
> ~10K wide rows; bulk backfills that commit millions of rows at once should
> either be chunked or run as a `replace`/`append` transfer instead of through
> the change stream.

apitap already negotiates pgoutput **protocol v2 with `streaming = true`**, so
the server ships a large transaction *while* it decodes rather than after the
commit; what remains is applying those in-progress chunks before the commit
arrives. That work would lift this ceiling entirely. Until it lands, size your
commits.

## Reproducing

Scripts are in this directory: `cdc_stress_seed.py` (10 tables × 1M),
`cdc_burst.py` (10M changes in N-row transactions), `cdc_drain.py` (one drain,
capped), `cdc_verify.py` (per-table truth), driven by `cdc_stress.sh` — and
`my_cdc_*.py` / `my_cdc_stress.sh` for the MySQL edition. The drain is one
call — ten tables share one slot, one group, one watermark:

```python
apitap.transfer(PG, CH, tables=[f"cdc_t{i:02d}" for i in range(1, 11)],
                mode="log_based")
```
