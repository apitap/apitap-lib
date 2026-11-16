# transfer(): the next speed round for bulk and CDC — where the walls are, what is left

Status 2026-11-12: architecture + work order, design only. Written by the
architect session. Sister document: `2026-11-12-sql-bounded-memory-and-scaling.md`
(apitap.sql()). Every number below is quoted from an earlier ledger and names it;
none was re-measured on 0.67.2. Phase A re-measures before anything is built.

Owner's target (same as for sql): as fast as physics allows at 0.5 vCPU / 256 MB,
memory flat, and proportionally faster as cores are added.

## 0. What a transfer costs, in one line

A transfer is a relay: every byte is received from the source, changed (or not),
and sent to the destination. At 0.5 core its wall is

    max( source's send rate, destination's ingest rate,
         bytes × (kernel receive + user work + kernel send) ÷ 0.5 core )

Three walls, three different lever families. A lever from the wrong family
measures neutral; the campaign records below are full of exactly that.

## 1. Where each lane stands (from the ledgers)

### Bulk

| route | measured | the wall, as last profiled | source |
|---|---|---|---|
| pg → ch, 100 GB, 0.5c / 256 MB | 8m57s, 432K rows/s, 5 pipes, 171 MB | client CPU (receive + transcode + send) | profiling.md, 100gb ladder |
| pg → ch, 100 GB, 3 machines | 30.3 s, ~3.3 GB/s, 6.3 cores, 32 pipes | source (64/96 pipes were slower) | gcp-benchmark.md round C |
| pg → pg, 16c | 20.2 s / 10M | destination: 86% of worker time waiting on COPY IN | profiling.md session 1 |
| my → pg | 22.5 s / 10M | destination: saturates at ~8 COPYs, ~450K rows/s | mysql-source-tuning |
| my → ch, 0.5c | 36.1 s for 10 × 1M | client CPU (decode + encode) | README 0.57.0 |
| pg → my, 16c | 64.3 s / 10M | destination (`LOAD DATA`); the slowest route of the mesh | README |
| pg → bq | 28.4 s / 10M | upload bandwidth + load jobs | README |

Bulk already scales with cores through pipes (the planner asks `k × cores` and
fits the memory). The destination-tuning session queued on 2026-08-02 was never
recorded as done.

### CDC (0.5 core / 256 MB unless noted; `mesin-3jt` §14)

| lane | measured | the wall | section |
|---|---|---|---|
| pg → ch, 1 slot | plateau 42–45K changes/s at 0.34–0.48 core | NOT client CPU: the rest of the wall is waiting on the walsender and on ClickHouse statements | §14.23 |
| pg → ch, slots=2 | ~49K/s caught up fully (2.96M/min), exact | CPU 84–86% at a 62K offer: the core | §14.9, §14.17 |
| pg → ch, slots 2/3/4 at 2/3/4 cores | 70.8K / 85.0K / ~100K per s, CPU under 0.7 of the cage | the bench source could not offer more than ~100K/s | §14.22 |
| my → ch, single and 10-table group | ~49K/s caught up; at a 61–63K offer falls behind with CPU at 57–63% | NOT CPU: the apply path is serial per group | §14.18 |
| receive floor | `pg_recvlogical` to /dev/null: 37 s for the same stream at 0.45 core, 77% of it kernel receive; apitap's whole pipeline = that + 8% | kernel receive | ch-ingest-r3 |

Measured neutral or worse at 0.5 core, with receipts (do NOT retry without new
evidence): mimalloc, unix sockets, HTTP body compression, fat LTO, a blocking
reader thread, always-coalesce, a second runtime worker, MyWire windowed drain,
one pipe per build chunk, SmallVec cells, x86-64-v3, pull-apply (rejected on
identity as well). Kept: the 5 ms read coalescer (−6% CPU per change), binary
pgoutput → RowBinary, insert-only apply, follow mode, `slots="auto"`.

**A discrepancy to settle first.** The README says "today's honest number is
1.9M/min catch-up" and keeps the 3M/min roadmap box unticked, while §14.9 and
the `slots=` docstring record 2.96M/min at 0.5 core with two slots. Either the
README is stale or the 2.96M configuration (insert-only, slots=2) is not the
default path. Phase A must say which, on the released wheel.

## 2. Reading of the evidence

1. **At 0.5 core the flagship lanes sit at or near a measured floor.** The CDC
   pipeline is within 8% of receiving alone; six client-side levers in a row
   measured neutral. More user-space work on those lanes is the wrong family.
2. **Two lanes have idle client CPU and a serial wait**: MySQL-source CDC
   (57–63% CPU while behind) and every destination-bound bulk route (pg → pg,
   my → pg, pg → my). Their lever is structure: overlap and parallelism at the
   destination, not cheaper decoding.
3. **Scaling with cores is uneven.** Bulk scales by pipes. Postgres CDC scales
   only by slots (one drain task per slot is flat from 1 to 2 cores). MySQL CDC
   has one binlog stream and one serial apply: it does not scale at all today.
4. **The same kernel-receive term caps sql(), bulk and CDC.** It is paid per
   byte and per wakeup. The only levers of its family are fewer bytes and fewer
   wakeups; "fewer bytes" from the source means the source compresses or
   projects, which is the source's CPU.

## 3. Levers, by family

### F1. Destination-bound lanes: make the destination the only wall, then widen it

- **D1 MySQL-source CDC: parallel apply across the group's tables.** The lane
  pool that Postgres-source drains use for ClickHouse (`APITAP_CDC_APPLY_LANES`)
  must serve a MySQL-source window the same way; a window's tables are
  independent units with one watermark committed after all of them. Expected:
  the idle 40% of the half core is used and the 61K offer is caught, and it is
  the first thing that lets MySQL CDC rise with cores.
- **D2 Row-store CDC destinations (Postgres, MySQL) apply one table at a time**
  (lanes = 1 by design: one transaction per table-window). Measure whether a
  group's tables can apply on N connections with the watermark written by the
  last one, keeping the fence (`FOR UPDATE` on the lease row) per connection.
  Only if the destination shows idle capacity.
- **D3 Bulk into Postgres: where do the 86% go?** Split the wait: WAL, index
  maintenance, constraint checks, `fsync`. Candidates inside the engine's
  remit: load the staging table bare and build indexes/constraints once after
  the load and before the swap (each on its own connection, in parallel when
  cores allow); never session settings that weaken durability by default.
- **D4 Bulk into MySQL (`LOAD DATA`), the slowest route.** Session-level only
  (`unique_checks`, `foreign_key_checks` on the staging table, which nothing
  references yet), rows arriving in primary-key order per pipe (spans already
  are key ranges: verify the staging table's PK order is the span order),
  secondary indexes added after the load. No server-global setting.
- **D5 ClickHouse CDC statement time** (57 µs per change in the 0.58 report,
  against 15 µs of client CPU): re-measure under insert-only, which was built to
  remove the DELETE pair; if the per-window state write is still one statement
  per member, batch it (one statement per window).

### F2. Client CPU per byte at 0.5 core (only where the profile puts it in user space)

- **C1 my → ch / my → pg bulk**: the route with real user CPU (decode of the
  MySQL binary protocol + encode). Profile first; the raw `MyWire` plane and
  the direct encoders are already in.
- **C2 One receive policy for every plane.** The 5 ms coalescer exists only on
  the replication reader. Measure the same knee on the bulk raw COPY plane and
  on `MyWire` (as a policy on refill, which is not the windowed drain that was
  rejected). Expect a few percent or nothing; drop it on a neutral result.
- **C3 Kernel memory as a planned number.** Shared with the sql ledger: explicit
  `SO_RCVBUF`/`SO_SNDBUF` per pipe so the cgroup's `sock` charge is known and
  the freed headroom buys a pipe where pipes are the limit (128 MB tier).

### F3. Scaling law (cores and memory in, throughput out, no knob)

- **L1 Postgres CDC**: `slots="auto"` is opt-in. Decide, with the costs the
  docstring lists (one busy source core and one WAL retention per slot), whether
  auto becomes the default above a core threshold, or stays opt-in and the
  progress line says "a second slot would use the idle cores".
- **L2 MySQL CDC**: D1 is the scaling path (decode stays one stream; apply
  fans out). Measure decode alone to find where one stream caps.
- **L3 Bulk**: confirm each profile's `k × cores` against 2/4/8-core cages on
  0.67.2; the numbers were swept on older engines.

### Not on the list, and why

Source-side compression or projection beyond the user's columns (the source's
CPU; same spirit as no-heavy-pushdown); `splice`/zero-copy relays (the copy is
not the cost, per-packet and per-wakeup work is, and TLS or any transcode
defeats it); io_uring (streaming favours epoll, §14.26).

## 4. Phases

| phase | work | ends with |
|---|---|---|
| A | on the released 0.67.2 wheel, every bulk route of the README table and the CDC matrix (pg/my source × ch/pg/my destination; single, 10- and 30-table groups; catch-up and paced), at 0.5c/256 MB and at 2c/2 GB and 8c/8 GB: rate, client CPU split (kernel receive / user / kernel send), client idle share, source and destination CPU and their busiest statement, peak, `sock`. Settle the README discrepancy | one table per lane naming its wall — gate T1 |
| B | the F1 levers whose lane shows idle client CPU, each RED-first with an exactness leg, A/B interleaved n ≥ 3 | per-lever verdict (kept / neutral, reverted) — gate T2 |
| C | F2 levers the profile supports; F3 scaling legs (`e2e_scaling`) | tiers table |

Rules as for the sql round: Rust in `apitap-core`, VPS only, checksum/digest
exact on every timed run, no default that moves work onto a server or weakens a
guarantee, nothing kept that measures neutral, n ≥ 3 with the 0.5-core noise
band of §14.24–14.25, destination tables dropped after each run, slots and WAL
cleaned after each CDC run (the disk filled twice from stale slots), no GCP
without the owner's word.

**Sequencing.** One bench VPS: this round starts after the sql W1 round's
measurements end, or the two contaminate each other's timings.

## 5. What to expect, honestly

At 0.5 core the flagship numbers (pg → ch bulk, pg CDC) will move by percent, not
by multiples: they are at the receive floor. The multiples that remain are on
the lanes with a serial wait (MySQL CDC above 49K/s, bulk into Postgres and
MySQL) and in scaling (MySQL CDC with cores; default slot use). "Any size in
under 10 s" does not apply to a transfer: its wall is bytes ÷ rate, and at the
measured 0.5-core rate (432K rows/s of a 15-column table) 10 s is about 4M rows.
What a transfer can promise is a rate that holds at any size, in flat memory,
and rises with cores.
