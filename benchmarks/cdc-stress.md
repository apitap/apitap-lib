# CDC under load: 40M changes through a 256 MB box (2026-08-05)

How hard can you push log-based CDC on half a core? Ten Postgres tables into
ClickHouse, with everything on the CDC side confined to **0.5 CPU / 256 MB**
while the writer runs unconstrained on the host — the honest shape, because in
production the application is never the thing you starve.

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

**Sustained CDC throughput is ~51-56K changes/s on half a core**, and it does
not decay: round 3 was the fastest of the three even though it started from the
deepest backlog. So a 10M-change minute takes about three minutes to apply here
— **to keep up with 10M changes per minute you want roughly 1.5-2 cores**, not
0.5. That is the sizing number; everything else is detail.

**Falling behind does not cost memory.** At the peak of round 2 the replication
slot was holding **2,322 MB of WAL** — a backlog nine times the container's
entire memory limit — and the worker's peak RSS was **91 MB**. The backlog lives
in Postgres's WAL on disk, which is exactly where it should live. A CDC reader
that grows with its lag is a CDC reader that dies on the day it matters.

**Bootstrap is a different animal from apply**: 937K rows/s for the full load vs
~55K changes/s for the stream. Bulk COPY moves rows in blocks; a change stream
carries per-row identity, key lookups and upsert semantics. Expect the ratio.

## The wall you will actually hit: transaction size

The first run of this test **was OOM-killed**, and the volume had nothing to do
with it. The writer inserted 1M rows per table in **one transaction**:

```sql
INSERT INTO t (…) SELECT … FROM generate_series(1, 1000000);   -- one commit
```

Logical decoding hands a transaction over as a **unit** — an apply is atomic or
it is nothing, so no memory budget can slice one commit into pieces. One
1M-row transaction is a ~200 MB window in a 256 MB container. It died, and it
should have.

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
arrives. That work would lift this ceiling entirely — until it lands, size your
commits.

## Reproducing

Scripts are in this directory: `cdc_stress_seed.py` (10 tables × 1M),
`cdc_burst.py` (10M changes in N-row transactions), `cdc_drain.py` (one drain,
capped), `cdc_verify.py` (per-table truth), driven by `cdc_stress.sh`. The
drain is one call — ten tables share one slot, one group, one watermark:

```python
apitap.transfer(PG, CH, tables=[f"cdc_t{i:02d}" for i in range(1, 11)],
                mode="log_based")
```
