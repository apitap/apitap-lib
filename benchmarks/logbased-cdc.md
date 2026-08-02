# Batch CDC (`mode="log_based"`) — correctness suite and the ape-dts race

Rig: OVH VPS (16 vCPU / 61 GB), `postgres:16-alpine` source with
`wal_level=logical` and a second Postgres as destination, both on loopback.
apitap built from main; ape-dts `apecloud/ape-dts:latest`.

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

## Reproduce

```bash
# correctness
APITAP_DEBUG=1 python e2e_logbased.py

# the race (creates both slots, generates one window, times both tools)
bash bench-cdc-showdown.sh
```

Raw: [logbased-cdc-raw.log](logbased-cdc-raw.log).
