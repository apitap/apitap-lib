# Capped tier, CDC **stress**: can 0.5 CPU / 256 MB absorb 1,000,000 changed rows per table per minute?

The owner's question, exactly as asked: **can a 0.5 CPU / 256 MB drain keep up
with 1,000,000 changed rows per table per minute, for 3 minutes, across 30 tables
in one CDC group?** Thirty million changes a minute offered, ninety million
offered in total.

The answer is **no**, and the shape of the no is the deliverable:

- **The source offered the rate.** The uncapped writer put **54,000,000 changes**
  on the wire in about **105 seconds** — **~1,028,000 changes per table per
  minute**, at the requested 1,000,000. Nothing about the generator was the
  limit. (The 105 s is derived from the sampler's own WAL positions, not timed by
  the harness, and the derivation is printed under WINDOW S rather than assumed.)
- **The drain applied 0 of them.** In the 78 seconds before PostgreSQL refused
  to keep the replication slot, a 0.5 CPU / 256 MB apitap 0.57.0 drain over one
  group and one slot committed **zero** changes to ClickHouse, and then said so:
  `can no longer get changes from replication slot "apitap_g25786459cc6"`.
- **The sustained apply rate on this rig is 6,866 changes/s**, measured over a
  3,990,000-change window that converged and checksum-verified 30 of 30 tables.
  The requested rate is **72.8×** that.
- **The backlog is a straight line**, and it ends in PostgreSQL, not in the cage:
  9.8 MB of retained WAL at t=0.8 s, **12,480.7 MB at t=78.3 s** — linear at
  ~160 MB/s — and then the slot is gone. `max_slot_wal_keep_size=12GB` was the
  safety valve, and it fired.
- **Memory never moved.** The drain's peak was 59.4 MB of the 256 MB cage in the
  stress leg and 62.8 MB in the converging one, with `oom_kill 0` throughout. The
  drain did not run out of memory. It ran out of *rate*.

Rig: the same one the previous CDC campaign used and left running —
`apitap-bench-cdc-pg` (`postgres:16.13-alpine`, `wal_level=logical`, 127.0.0.1:5548)
and `apitap-bench-cdc-ch` (`clickhouse-server:24.8.14.39`, 8128 HTTP / 9128 native).
apitap is the PyPI wheel **0.57.0** from `~/apitap-057-pullback`, whose
`_apitap.abi3.so` md5 is `41e9f7c252d3e1eb5403b70f87bf5435`, bind-mounted read-only
into the cage and re-asserted from inside it on every leg. The cap is
`--cpus=0.5 --memory=256m --memory-swap=256m` for the drain and nothing else; the
writer, the source and the destination are uncapped by design.

## The state this campaign found, recorded rather than assumed

The brief warned that the thirty seeded tables "may already carry the previous
window's mutations". They do, and the number is not 1,000,000:

```
30 tables, 8419 MB total, every table at 1036000 rows
id range over t01: 1..2044999
```

**1,036,000 rows per table** — the previous campaign's five-minute window's own
arithmetic (`1,000,000 + 45,000 − 9,000`) written onto the seed. Its digest is
byte-identical to the one that campaign published for its post-window state
(`1036000|2225213684394444|…`), which is the cross-check that nothing had moved
since. **30 × 1,036,000 = 31,080,000 rows.**

This campaign then inserts **100 rows per table** at ids `40,100,000 +` — the
"graveyard block", created *before* the CDC group exists so that it is part of the
bootstrap and therefore part of every checksum — and the source sits at
**1,036,100 rows per table** for the whole campaign. Those rows are inert: no
window touches them, and they are inside the digest, so a mismatch would name
them rather than hide in a delta.

**Everything the campaign wrote to after the reference point is recorded here**,
because the three windows below each own a *disjoint* 800-row update band, and
several harness faults left residue in bands that were then abandoned:

| band (ids) | what wrote to it | state |
|---|---|---|
| 950,001 + (t−1)×1000 … +799 | the pre-flight probe, 3 transactions per table ×2 runs | +6 per row, in both source and destination, verified |
| 951,601 + … | an aborted stress run (see "four harness faults") | +~138 on the tables it reached, landed by a later bootstrap, verified |
| 954,001 + … | a second aborted stress run | +120 per row per table, drained and verified |
| 952,801 + … | **WINDOW C** | +133 per row per table, verified |
| 964,801 + … | **WINDOW S leg 3**, the applied-prefix leg | +120 per row per table |

Giving each window its own band is what makes "how many increments does this row
carry" a per-window number instead of a campaign total, and it is why the prefix
reconstruction below has exactly one corrected column.

## The calibration that decided the design, before anything was timed

Three numbers had to be measured rather than assumed, because "90,000,000 changes"
is a **storage** question before it is a throughput question
([bench-cdc-stress-calibrate.sh](bench-cdc-stress-calibrate.sh), receipts in the
raw log). All of it ran on a scratch clone, `cdc_cal`, so the thirty seeded
tables were not written to by the calibration at all.

**WAL bytes per change**, for this campaign's own row shape (~250 B/row,
15 columns), from an LSN bracket around each statement. The UPDATE figure is the
16,000-change sample (twenty repeats of the hot statement) because that is the one
where the fixed per-statement cost cannot masquerade as a per-row cost; the
INSERT figure is the second of two cold batches, and the DELETE figure is 100
recently-inserted rows:

| `wal_compression` | UPDATE | INSERT | DELETE | the 800/100/100 mix |
|---|---|---|---|---|
| **off** (the default, and what every timed leg ran with) | 402.8 B | 356.4 B | 67.8 B | **364.7 B/change** |
| `pglz` | 382.2 B | 331.9 B | 72.5 B | 346.2 B/change |
| `lz4` | 363.1 B | 314.3 B | 50.6 B | 327.0 B/change |
| `zstd` | 344.6 B | 321.0 B | 59.2 B | 313.7 B/change |

**This table was measured twice, and the first run's numbers were different** —
it read 413.6 / 301.9 / 31.0 B for the `off` row, a 9% lower composite of
331.5 B/change. The second run is the one quoted here because it is the one whose
receipts are in the raw log, and the honest reading of the pair is that **the
per-change WAL cost on this schema is 330–365 B with a ±10% spread between runs**
on the same box with nothing else running. Nothing below depends on a figure
finer than that.

Two things fall out of the table:

- **Compression buys 4–14%, and not the 3× that would have changed the design.**
  The payload is 200 characters of md5 hex per row — already incompressible — so
  pglz reaches 346 B/change and zstd 314 B/change against 365 B/change for no
  compression. Even the best case leaves a 90,000,000-change window at ~28 GB of
  WAL, which is the number the storage arithmetic below actually uses. Every timed
  leg therefore ran on the **default `wal_compression=off`**: the least tuned
  choice, and the one that costs the generator the most.
- **Full page images are not a cost here.** A never-touched 800-row band cost
  400.0 B/row and the *same* band immediately afterwards 411.5 B/row, and twenty
  repeats of it 402.8 B/row — a spread of 3% with no step in it, i.e. no FPI. A
  narrow band that stays resident in a 1 GB `shared_buffers` never pays for one,
  and 30 tables x 800 rows x 250 B is 6 MB of pages. Had the writer spread its
  updates over whole tables, every touch would have cost 8 KB and this campaign's
  WAL budget would have been a different and much worse number. That is the single
  most load-bearing design decision in the writer, and it was made from this
  measurement rather than from hope.

**Writer throughput**, the same 1,000-change transaction, N concurrent `psql`
sessions on the source — also measured twice, because the two runs disagree by
about 20% and the spread is part of the answer:

| sessions | run 1 | run 2 | changes/s per session (run 2) |
|---|---|---|---|
| 1 | 76,743 | 68,585 | 68,585 |
| 8 | 205,705 | 222,606 | 27,826 |
| 15 | **314,992** | **262,693** | 17,513 |
| 30 | 314,883 | 312,574 | 10,419 |

**The source's ceiling for this statement shape is 260,000–315,000 changes/s**, and
15 to 30 sessions is where it sits; one session is 5x slower and does not get you
there. The WINDOW S writer then beat both calibration runs, at ~510,000 changes/s
— see the derivation under WINDOW S, and the note there that it exceeded the
calibration's own ceiling, because a 40-transaction calibration round is dominated
by per-session startup and first-touch page faults while the real window ran for
~105 s on warm buffers.

## The change shape, and why it is built the way it is

One transaction = **800 UPDATEs + 100 INSERTs + 100 DELETEs = 1,000 changes**:

```
BEGIN;
UPDATE cdc_pg_t01 SET regular_int = regular_int + 1 WHERE id BETWEEN 964801 AND 965600;
INSERT INTO cdc_pg_t01 (15 columns) SELECT … FROM generate_series(51000000, 51000099) AS g;
DELETE FROM cdc_pg_t01 WHERE id BETWEEN 51000000 AND 51000099;
COMMIT;
```

([bench-capped-pg-ch-cdc-stress-writer.py](bench-capped-pg-ch-cdc-stress-writer.py);
the INSERT uses the *seed's own* generator expressions, so a row it inserts is
indistinguishable from a seeded one and the checksum compares like with like.)

- **The mix is 80% UPDATE / 10% INSERT / 10% DELETE**, and **every transaction
  inserts exactly as many rows as it deletes**, so the table's row count is
  constant across a whole window. This is the "bound the growth" requirement, and
  it is exact: the source sat at **1,036,100 rows per table** before WINDOW C and
  at **1,036,100 rows per table** after it, digest and all.
- **The UPDATE target is a FIXED 800-row band** and every transaction re-runs the
  identical statement. After *k* transactions every band row carries exactly `+k`.
  That is the property that makes "the source as of the drain's watermark" a
  closed form rather than a replay — see *the applied prefix* below.
- **1,000 changes per transaction** is three orders of magnitude below the
  project's own measured CDC ceiling (a single 1,000,000-row commit OOM-killed a
  256 MB worker; 10,000-row commits ran at 87 MB — benchmarks/cdc-stress.md).
- **The INSERT and the DELETE are the same 100 rows in the same transaction.**
  That was not the first design. The first one deleted the *previous*
  transaction's inserts, which is the more natural-looking shape and which leaves
  the **last** transaction's 100 rows live — so "the rows that exist at the
  watermark" becomes `I_k … I_N−1`, a range whose far end depends on how far the
  writer got before it was stopped. The reconstruction emitted `I_0 … I_k−1` and
  was wrong for every *k* except the one the control exercises. Both the bug and
  the reason the current shape has no third region are in the script's docstring.

## The control, GREEN ×4, before anything was timed

[bench-capped-pg-ch-cdc-control.sh](bench-capped-pg-ch-cdc-control.sh), the
previous campaign's own, unmodified, on a 1,000-row control table with the
campaign's schema and one apitap slot:

```
SOURCE   1000|2151639950269|1|1000|31680|32000|37544|10|12|12|11
APITAP   1000|2151639950269|1|1000|31680|32000|37544|10|12|12|11
  GREEN agreement
TAMPERED 1000|2154172601381|1|1000|31680|32000|37525|10|12|12|11
  GREEN sensitivity
RESTORED 1000|2151639950269|1|1000|31680|32000|37544|10|12|12|11
  GREEN reversible
SOURCE   1040|2228604318474|1|2000050|32928|33280|39026|11|13|12|12   (expected 1000 + 50 − 10 = 1040)
APITAP   1040|2228604318474|1|2000050|32928|33280|39026|11|13|12|12
  GREEN the change stream follows
  IDEMPOTENT second_drain_rows=0
  GREEN an empty drain is a no-op
WALSHADOW 1040|2228604318474|1|2000050|32928|33280|39026|11|13|12|12
  GREEN the rival validates
CONTROL GREEN x4
```

And a second control, which exists because the applied-prefix machinery below is
worthless without it: the prefix reconstruction, run with **no correction**,
against the **plain source digest**, all thirty tables, `diff` clean:

```
RECONSTRUCTION_CONTROL checked=30 of 30 match=30 mismatch=0
```

## The rig, and every setting that differs from the previous campaign

| setting | previous campaign | this one | why |
|---|---|---|---|
| `max_slot_wal_keep_size` | 6 GB | **12 GB** | the safety valve, and the ceiling this campaign reports |
| `max_wal_size` / `min_wal_size` | 4 GB / 80 MB | 2 GB / 512 MB | bound `pg_wal` on a box at 89% |
| `shared_buffers` | 128 MB | 1 GB | keeps the 800-row update bands resident, which is why there are no FPIs |
| `max_connections` | 80 | 200 | 15 writer sessions + the walsender + the harness |
| `checkpoint_timeout` | 30 min | 5 min | more checkpoints = fewer FPIs on the insert pages |
| `synchronous_commit` | on | **off** | the writer's commit rate; WAL *volume* is unchanged, only the fsync |
| `wal_compression` | off | off (unchanged) | measured: no lever on this payload |
| `full_page_writes` | on | on (unchanged) | never disabled, deliberately |

Every one of these is a setting on the **uncapped source**. None of them changes
what the capped container is allowed to do, and all of them are listed so a reader
can decide whether the source was given a fair chance to offer the rate. It was.

One rig fact worth stating because it is a trap: **the host cgroup path on this
box is `/sys/fs/cgroup/<container-id>`, not `system.slice/docker-<id>.scope`**.
Every host-side memory and CPU sample in the first two stress legs read `0` from a
path that does not exist, which reads exactly like "the container used no memory".
The per-leg peaks in this report are `memory.peak` read from **inside** each
container, which is unaffected; the cgroup path is fixed for anyone who re-runs.

## The bootstrap — reported, not the headline

Three bootstraps ran, because two harness faults destroyed the state between them
(faults 1 and 4 below). All three are one `apitap.transfer(..., mode="log_based")`
call over **30 tables in one group, one publication, one replication slot**, from
an **empty** destination, and all three checksum-verified **30 of 30** before any
change stream:

| bootstrap | wall in container | `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|
| 1 | 166.6 s | 49.2 MB | exit 0, not OOM-killed | 31,083,000 | **30/30 MATCH** |
| 2 | 166.1 s | 58.0 MB | exit 0, not OOM-killed | 31,083,000 | **30/30 MATCH** |
| 3 | 176.7 s | 51.8 MB | exit 0, not OOM-killed | 31,083,000 | **30/30 MATCH** |

```
VERIFY_SUMMARY checked=30 of 30 match=30 mismatch=0
TOTAL_ROWS 31083000
watermark: lsn 14478951640 .. 14478951640 distinct=1 tables=30
slot:      apitap_g25786459cc6 active=false reserved retained=344 bytes
```

**31,083,000 rows = 30 × 1,036,100.** The group advances as **one watermark**
(`distinct=1` across all thirty tables), which is the property the CDC lane exists
to protect. `budget=8` is apitap's own auto-sizing off the cgroup limit — no knob
was passed. `FD_PEAK 30 → FD_END 4`: thirty descriptors while the group is
established, four afterwards.

## WINDOW C — the correctness window, and the apply rate everything else is quoted against

A paced change stream, sized so the drain **converges inside the budget**, which is
what buys an exact checksum verdict on a change stream rather than on the
bootstrap alone.

**Offered: 3,990,000 changes in 182.3 s = 21,890 changes/s = 43,780 changes per
minute across 30 tables = 1,459 changes per minute per table.** Fifteen `psql`
sessions over disjoint table pairs, 133 transactions per table, 1.35 s apart.
Row count unchanged at 1,036,100 throughout.

| drain | changes applied | wall (in-container) | changes/s | `memory.peak` | FD peak | slot's retained WAL |
|---|---|---|---|---|---|---|
| 1 | **3,990,000** | 581.1 s | **6,866** | 62.8 MB | 13 | — |
| 2 | **0** | 3.6 s | — | 53.4 MB | 12 | 5,385 kB |
| **total** | **3,990,000** | 584.7 s busy | **6,866** | **62.8 MB** | | |

Then the destination against the source, both digests recomputed from scratch:

```
VERIFY_SUMMARY checked=30 of 30 match=30 mismatch=0
watermark: lsn 15751771376 .. 15751771376 distinct=1 tables=30
```

Read that table one number at a time, because each one is a separate claim:

- **The applied total is exactly the generated total.** 3,990,000 offered,
  3,990,000 applied, and the next drain reports **0** — the group converged rather
  than accumulating. Nothing lost, nothing double-counted, and the empty drain's
  idempotence is *measured*, not asserted.
- **6,866 changes/s is the number the rest of this report is arithmetic against.**
  It is one process, one group, one slot, one replication slot, 30 tables, at
  0.5 CPU. A separate, longer drain on the same rig — clearing 7,375,000 changes
  in 991.3 s — independently gives **7,439 changes/s** at a 60.2 MB peak, so the
  figure is reproducible to within 8% on this rig and this shape.
- **Peak memory does not trend up.** 62.8 MB working, 53.4 MB idle. Both are
  single-digit fractions of the 256 MB cage, and `oom_kill 0` in both.
- **Descriptors do not grow.** 13 while draining, 4 at rest.

## WINDOW S — the requested rate, and the ceiling

**Offered: 1,800 transactions per table × 1,000 changes × 30 tables = 54,000,000
changes, unpaced, with the writer on the host and only the drain capped.** The
writer committed **all 54,000 transactions**:

```
GENERATION_WAL lsn 5/16819428 -> 8/FC8049C0  bytes 16743576984  committed_txns 54000  b_per_change 310.1
```

**16,743,576,984 bytes of WAL for 54,000,000 changes = 310.1 B/change**, measured
from the server's own LSNs — inside the calibration's 330–365 B/change spread, and
the figure the storage arithmetic below uses.

**The writer's wall time is ~105 s, and it is derived rather than timed.** The
harness's own `writer finished in …` line lost its variable to a subshell
(`writer_wait … | tee` runs it in a subshell — fixed in the committed script) and
is not printed here, so the number comes from the sampler instead, and the
derivation is shown because a reader should be able to reject it:

- the slot's retained WAL grew **linearly at ~160 MB/s** from t=0.8 s to t=78.3 s;
- the curve's own `pg_wal_lsn` column then shows the writer's rate **collapsing**
  — 336.5 MB over the next 6.4 s (52.6 MB/s) and 17.5 MB over the 6.2 s after
  that (2.8 MB/s) — so the writer was effectively finished by **t ≈ 80–110 s**,
  the two ends being where the linear WAL accumulation stops and where the LSN
  flattens;
- 16,743.6 MB at the measured ~160 MB/s lands at 104 s.

On that basis:

> **offered: 54,000,000 changes in ~105 s = ~514,000 changes/s =
> ~1,028,000 changes per table per minute.**

**The generator met the requested rate**, and it is worth being explicit that this
is the one part of the campaign that did. It also **beat the calibration's own
ceiling** (260,000–315,000 changes/s), which is not a contradiction and is not
explained away: the calibration's 40-transaction rounds are dominated by
per-session startup and by first-touch page faults, while this writer ran for
~105 s on warm buffers with a hot `shared_buffers`. **So treat the calibration's
315,000 as a lower bound on the sustained rate and this window's 514,000 as the
measured one** — and note that the difference is 1.6x, which is a wide spread to
carry into any conclusion. It does not touch the ratio this campaign is about: the
backlog curve's slope (~160 MB/s of WAL, set by the writer) and the drain's rate
(6,866 changes/s, set by the drain) are separately measured, and the ratio of
*changes* is quoted from both, not inferred from one.

### THE BACKLOG CURVE

Sampled every 5–6 s: the slot's retained WAL from the server, the group's
watermark, the destination's row count, disk free, and the current WAL LSN. This
is the deliverable.

| t (s) | slot's retained WAL | `wal_status` | group watermark | distinct | disk free |
|---|---|---|---|---|---|
| 0.8 | 9.8 MB | `reserved` | 21,848,148,048 | 1 | 37 GB |
| 7.1 | 856.9 MB | `reserved` | 21,848,148,048 | 1 | 36 GB |
| 13.4 | 1,914.4 MB | `reserved` | 21,848,148,048 | 1 | 35 GB |
| 19.8 | 3,235.1 MB | `extended` | 21,848,148,048 | 1 | 36 GB |
| 26.3 | 4,447.3 MB | `extended` | 21,848,148,048 | 1 | 35 GB |
| 32.8 | 5,588.9 MB | `extended` | 21,848,148,048 | 1 | 33 GB |
| 39.3 | 6,510.9 MB | `extended` | 21,848,148,048 | 1 | 32 GB |
| 45.9 | 7,483.9 MB | `extended` | 21,848,148,048 | 1 | 31 GB |
| 52.4 | 8,383.9 MB | `extended` | 21,848,148,048 | 1 | 30 GB |
| 59.0 | 9,346.2 MB | `extended` | 21,848,148,048 | 1 | 29 GB |
| 65.5 | 10,537.4 MB | `extended` | 21,848,148,048 | 1 | 27 GB |
| 71.7 | 11,587.1 MB | `extended` | 21,848,148,048 | 1 | 26 GB |
| **78.3** | **12,480.7 MB** | `extended` | 21,848,148,048 | 1 | 25 GB |
| **84.7** | **0** | **`lost`, then no slot** | 21,848,148,048 | 1 | 26 GB |

Read it: **the backlog is a straight line at ~160 MB/s**, from 9.8 MB to
**12,480.7 MB in 77.5 seconds**, and the watermark **never moves** — one value,
`21,848,148,048`, on every one of thirteen samples. The drain applied nothing, so
the group's progress coordinate is frozen for the entire window. Disk free falls
37 GB → 25 GB as PostgreSQL holds the WAL, and *recovers* to 26 GB the moment the
slot is invalidated, because an invalidated slot is marked ephemeral and stops
holding the WAL back. **The safety valve worked exactly as it is designed to: a
loud, bounded failure instead of a full disk.**

### The ceiling, verbatim

```
APITAP_VERSION 0.57.0
APITAP_SO_MD5 41e9f7c252d3e1eb5403b70f87bf5435
APITAP_TABLES 30
RAISED RuntimeError: transfer: walsender: server error 55000: can no longer get changes from replication slot "apitap_g25786459cc6"
MEMPEAK=59392000
MEMSTAT=anon 421888 file 18132992 shmem 0
MEMEVENTS=high 0 max 0 oom 0 oom_kill 0
EXITCODE 9
```

`55000` is `object_not_in_prerequisite_state`, PostgreSQL's own code for an
invalidated replication slot. **59.4 MB peak of a 256 MB cage, `oom_kill 0`,
`OOMKilled=false`.** The ceiling is not the memory ceiling and never was.

### What the drain applied, and one thing that is *not* the explanation

**0 changes.** The drain ran from t≈2 s until the slot died at t≈78 s and
committed nothing, in two independent legs at the requested rate. The obvious
alternative explanation — that a 0.5 CPU quota is not *delivered* when the
uncapped writer is saturating the box — was measured directly rather than assumed
([bench-cdc-stress-cpu-probe.sh](bench-cdc-stress-cpu-probe.sh)): the same
CPU-bound loop in the same cage, on an idle host and under fifteen writer sessions:

| | iterations/s | as a fraction of idle |
|---|---|---|
| cage, host idle | 1,110,544 | 100% |
| cage, 15 writer sessions at full tilt | 1,048,827 | **94.5%** |

So the cage **did** get ~95% of its half core while the writer ran, and CPU
starvation is **not** the explanation. What the measurement does *not* settle is
why no apply window was committed inside those 78 seconds — the drain's log shows
no progress output at all before the error, and this campaign does not speculate
past that. Two facts are reported instead: it applied nothing in 78 s at
500,000 changes/s offered, and it applies 6,866 changes/s when the backlog is
bounded and not growing at ~160 MB/s.

### The applied prefix — verified exactly, at a non-trivial watermark

A drain that fell behind leaves the destination at "everything up to W". Comparing
the two is the only honest verdict available, and the shape above makes it a closed
form rather than a replay. So WINDOW S leg 3 offered a rate the drain demonstrably
commits on — **3,600,000 changes over 180 s = 20,000 changes/s = 1,333 changes per
minute per table**, WAL ≈ 1.1 GB — with drain slices of 120 s, deliberately shorter
than the catch-up so the prefix is strictly between:

```
LEG apitap tag=slice1 mode=drain
  stopped_by returned_on_its_own
  wall_container_s 120.6 (slice budget 120s)
  changes_applied 180000
  DRAIN pass=1 rows=180000 wall_s=34.807 budget=1 tables_reporting=30 fd_now=4 fd_peak=13
  MEMPEAK=60760064          ← 58.0 MB of 256 MB
  MEMEVENTS=high 0 max 0 oom 0 oom_kill 0
```

**5,172 changes/s applied while the writer was still generating** — the drain was
outnumbered ~4:1 and committed a prefix. Then, at the drain's own final watermark,
"the source as of that watermark" was recomputed per table and compared against the
destination ([bench-cdc-stress-prefix.py](bench-cdc-stress-prefix.py)):

```
PREFIX_SUMMARY checked=30 of 30 match=30 mismatch=0
CONTROL_SUMMARY checked=30 of 30 match=30 mismatch=0
```

**Exactly what was compared, stated precisely:** apitap's own per-table counters
say it applied **6,000 of each table's 120,000 changes** (6 of 120 transactions).
The source was therefore reconstructed at 6 transactions per table — the fixed
800-row update band with its `(120 − 6) = 114` increments undone by one subtraction
in the row text, every other row untouched — and that reconstruction's `count(*)`,
order-independent `sum` of a per-row md5 prefix over all 15 columns, `min(id)`,
`max(id)` and four length/NULL totals were compared with the destination's, per
table. **No `string_agg`, no accumulating per-row string, no full scan of tens of
millions of rows**: one aggregate per table per engine, 30 of them in one query.

The **control** is the reason that claim is worth anything: the *same*
reconstruction with **no correction** reproduces the **plain source digest** on
30 of 30 tables, which tests the row text, the band arithmetic and the region
split with the drain not involved at all. Without it a reconstruction that is
wrong in some way prints as thirty `MATCH`es.

And then, with the writer stopped, one more drain finished the window — which is
what makes "prefix" a claim rather than a euphemism:

| drain | changes applied | wall | changes/s | `memory.peak` | FD peak |
|---|---|---|---|---|---|
| leg-3 slice 1 (writer running) | 180,000 | 34.8 s | 5,172 | 58.0 MB | 13 |
| converge (writer stopped) | **3,420,000** | 525.9 s | **6,504** | 60.4 MB | 13 |
| **total** | **3,600,000** | 560.7 s | **6,418** | **60.4 MB** | |

```
VERIFY_SUMMARY checked=30 of 30 match=30 mismatch=0
```

**3,420,000 + 180,000 = 3,600,000, exactly what WINDOW S leg 3 offered.** So the
prefix was a prefix: the part that had landed matched the source at the watermark
on all thirty tables, and the part that had not landed landed, and then the whole
thing matched the source **30/30**. Nothing was lost, nothing double-counted, and
the group's watermark was `distinct=1` on all thirty tables at rest.

### Four harness faults, each of which looked like a data verdict

Recorded because the alternative is a report whose failures are invisible:

1. **I dropped the group's replication slot between WINDOW C and WINDOW S.** The
   destination held 30 watermarks and no slot; the drain container exited in two
   seconds and the writer was stopped 13 s in having produced 4,140,000
   un-drainable changes. The whole sequence had to be re-bootstrapped. `stress`
   now **asserts** the precondition — "the destination holds 30 table watermarks
   and there is no apitap slot" aborts before the writer starts.
2. **A drain is a bounded read, not a follower.** `apitap.transfer(mode=
   "log_based")` consumes the WAL that exists when it starts and returns; it does
   not tail. Starting the drain first and generating afterwards yields a drain
   that reports `changes=0` and exits in 5.7 s with the whole stream stranded in
   the slot. WINDOW S is therefore a **loop of bounded drain slices**, each its
   own container — which is also what the brief's "short cadence" asks for.
3. **Killing the writer's pipeline does not stop the writer.** `docker exec -i …
   psql` dies, the pipe closes, and the `psql` *inside* the source container keeps
   committing. One run recorded 5,114 ledger lines for **7,375** committed
   transactions, and the ledger could no longer describe its own band. The writer's
   sessions now set `application_name` and the safety valve stops them with
   `pg_terminate_backend`, server-side.
4. **The writer's per-table transaction number is not the field in its ledger.**
   A session owns two tables and interleaves them, so the `k` it prints is the
   *session's* loop index: `t01` gets 1, 3, 5 … and 120 of its transactions carry
   `k` up to 239. Taking `max(k)` as "how many increments this table has" undoes
   twice too many, and the prefix check printed **29 of 30** — one table, one
   increment, looking for all the world like a data defect. The claim is now made
   against **apitap's own `per_table_rows` counter**, not ours.

Two more, both measurement faults rather than design: the `awk` that turned an LSN
difference into bytes read an LSN's **hex** low half as a decimal and reported
`0 B / 800 changes` for a statement that had plainly written WAL; and the
`writer_wait … | tee` pipeline in fault 3 lost its globals, so one run printed
`changes_offered 0` beside a ledger of 54,000 committed transactions.

## Can it keep up? The arithmetic

| | changes/s | changes/min | changes/min/table |
|---|---|---|---|
| **offered, requested** | 500,000 | 30,000,000 | 1,000,000 |
| **offered, achieved** (54,000,000 in ~105 s) | ~514,000 | ~30,840,000 | **~1,028,000** |
| **applied, sustained** (WINDOW C, 3.99 M converged) | **6,866** | 411,960 | 13,732 |
| **applied, while generating** (leg 3, slice 1) | 5,172 | 310,320 | 10,344 |
| **applied, a second independent converged drain** (3.42 M in 525.9 s) | **6,504** | 390,240 | 13,008 |
| **applied, WINDOW S at the requested rate** | **0** | 0 | 0 |

- **Ratio: 500,000 / 6,866 = 72.8×.** The requested stream is outnumbered by the
  drain roughly **73 to 1**.
- **Cores that would be needed.** In a CPU-quota cage a saturated leg has
  `cpu = 0.5 × wall`, so wall and CPU are *one* measurement here and are quoted
  once: 3,990,000 changes in 581.1 s of wall is ~290 s of CPU, i.e. **13,733
  changes per CPU-second**. Absorbing 500,000 changes/s needs
  **500,000 / 13,733 = 36.4 cores** of apitap drain on this row shape — and this
  box has 16, of which the writer was already using most. Memory is not the
  constraint: the peak was 62.8 MB against a 256 MB cage, so ~36 cores of this
  work would want on the order of 36 × 63 MB ≈ 2.3 GB, which fits in the 61 GB
  box easily. **CPU, not memory.**
- **Storage is the second wall, and it is a real one.** At the measured 310
  B/change, a 90,000,000-change window is **27.9 GB of WAL that must be retained
  on the source until the drain catches up** (at the calibration's 365 B/change,
  32.9 GB; either way it is the whole of this box's free space). The box had **37 GB free** with the
  5.1 GB destination already landed. So the requested 3-minute window is *exactly*
  at the physical limit of this machine, and the 12 GB valve fired at t≈78 s
  having stored 12.5 GB of it. **On this box, 90 million changes cannot be
  offered from a bounded source and retained at all** — that is a statement about
  41 GB of disk, not about apitap.
- **If the drain were infinitely fast**, the writer would finish 54,000,000
  changes in 103 s and the box would be fine. If the writer were infinitely fast,
  the backlog would be ~160 MB/s of WAL forever and the slot would die in 75 s.
  There is no rate at which these two settings coexist here.

## walshadow 0.1.2 — one leg, and only because it is cheap

**9 of 9.** One leg, upstream's shipped defaults, the same source, the same
destination, the same `--cpus=0.5 --memory=256m --memory-swap=256m`, an empty
destination, the thirty tables declared in the config, no apitap slot present:

| arm | survival | docker state | rows landed | its own last words |
|---|---|---|---|---|
| **walshadow 0.1.2** | **10 s** | **`OOMKilled=true`, `ExitCode=137`** | **0 of 31,083,000** | `bootstrap insert tail started … lanes=1 inserters=3 row_budget=4194304 byte_budget=268435456` |

The kernel's `OOMKilled=true` with exit 137 is the verdict; `cgroup_memory_peak_mb`
and `host_sampled_peak_mb` both read **0.0** because the container's cgroup was
gone before either could be sampled, and a peak of zero would be the dishonest
form of that number. The previous campaign's report is the reference for the rest:
on this rig walshadow is now **9 of 9 legs OOM-killed**, in 5–10 s, and dies at
the same place every time — the start of the *insert tail*, after its physical
base backup and catalog map (`relations=60` is the thirty tables and their thirty
indexes), streaming rows into ClickHouse. That phase needs the ~550 MB daemon
footprint its bulk arm measured, and a 256 MB cage does not have it. Its own
`configuration.md` prescribes `memory.resident_payload_max = 64 MiB` and
`ch.byte_budget = 32 MiB` for a small box, and the tuned arm of the previous
campaign still died in ~5 s with those set; **this leg did not re-run the tuned
arm, and that is a disclosure rather than an omission** — the brief asked for one
cheap leg and for no real time, and the tuned arm's evidence already exists.

## Leak checks, taken with the campaign's state still intact

Taken with the campaign's state still intact — the group's slot present, the
destination landed, before the walshadow leg and the final drop.

| what | reading |
|---|---|
| descriptors, per container | `FD_PEAK` **30** (bootstrap, 30 tables), then **13, 12, 13, 13, 12, 13** across the drains; **`FD_END` 4 in every container** |
| replication slots left | one: `apitap_g25786459cc6`, logical, inactive, `wal_status=reserved`, retaining **21 MB** |
| the group's publication | `apitap_g25786459cc6_pub` — the group's own state, not a leak |
| WAL held by slots, in total | **21 MB** |
| spilled transactions | **0** txns / 0 spills / 0 bytes |
| `_apitap_lease` | **1,303 rows**, of which **0 uncollected and still live** |
| staging / marker leftovers | **none** — no `*__apitap*` table of any kind |
| `_apitap_cdc_pending` | **does not exist** (no window ever spilled) |
| the group's state rows | **30**, `mode=log_based`, `cursor=_lsn`, **1 distinct watermark** (56,402,757,640) |

The 1,303 lease rows are the documented behaviour rather than a leak: lease rows
are written per run and per table and are never garbage-collected on any engine,
because an uncollected row must never age out. What matters is that **none of
them is uncollected and live** — no table is left locked, and the check that says
so is the one a peer would hit.

The staging answer deserves the same sentence of honesty the previous campaign
gave it: during a run there **is** a run-scoped artifact — ClickHouse's own
dropped-table metadata names them, e.g.
`default.cdc_pg_t01_0tmd20lrfxze9dn__apitap_staging`, a per-run staging table
carrying the run token, dropped at the end of the run and swept by token — and the
drop check after the final verification reports **0** leftovers. That is two
measurements: the artifact exists, and it does not outlive its run.

### The catch-up curve, for contrast

The same sampler, over the drain that finished WINDOW S leg 3's backlog. This is
what "keeping up" looks like, and it is the mirror image of the ceiling:

| | slot's retained WAL | group watermark |
|---|---|---|
| when the writer stopped | 1,117.8 MB | 55,319,271,624 |
| +1 min | 1,109.4 MB | 55,330,370,568 |
| +4 min | 888.3 MB | 55,500,538,000 |
| +7 min | 590.8 MB | 55,786,185,680 |
| +9 min | 505.1 MB | 55,931,099,280 |
| +12 min | 164.9 MB | 56,274,019,312 |
| after the final drain | 21 MB | 56,402,757,640 |

**The backlog falls monotonically to zero and the watermark rises monotonically to
one value**, which is the whole difference between the two curves: the drain's rate
is a constant, so what decides the shape of the curve is only whether the writer's
rate is above or below it.

## Interleaving, host state, and rig hygiene

`loadavg` (1-min) around this campaign's legs ranged **3.30 – 16.52**, and
`MemAvailable` sat between **45.3 GB and 46.3 GB** throughout; every before/after
pair is in the raw log's host-state section. The box carries production
(`apitap_web`, `apitap_lab_*`, `apitap_caddy`, `apitap_registry`,
`apitap_postgres_core`, `apitap_db_backup`) plus eleven other long-lived bench
containers, none of which this campaign touched, moved or restarted.

Two hygiene rules from the previous campaign were kept and one of them mattered
under stress:

- **Every destination drop is `DROP TABLE … SYNC`.** ClickHouse keeps a dropped
  table's data on disk for `database_atomic_delay_before_drop_table_sec = 480 s`
  without it, and the previous campaign measured 42 GB → 9.5 GB free across three
  consecutive 8 GB landings. This campaign's 5.1 GB destination was dropped SYNC
  at the end and the store returned to its floor.
- **The thirty seeded tables were never dropped.** They are this campaign's
  source, they were written to on purpose, and they are left for the next one —
  now carrying 1,036,100 rows per table plus the five bands in the table above.
  Deleting them is a campaign-done decision, not a bench decision.

**Nothing was pruned.** No `docker system prune`, no `docker volume prune`, no
image removal. The two 55 GB anonymous volumes on this box were left alone: they
are not this campaign's, and they are the only large thing on the disk.

### The rig afterwards

```
apitap-bench-cdc-pg  postgres:16-alpine                 Up 4 hours
apitap-bench-cdc-ch  clickhouse/clickhouse-server:24.8  Up 6 hours
apitap-bench-cdc-pg-data   16 G
apitap-bench-cdc-ch-data  1.4 G     ← the 5.1 GB landing, dropped SYNC, back to its floor
pg_wal                    2.0 G
destination leftovers:  cdc_pg_t* tables: 0
replication slots:       (none)
/dev/sda1  339G  301G  39G  89% /
```

**Both containers are left running**, as asked, and the source keeps the thirty
tables. One honest change to report: the seed grew from **8,419 MB to 13 GB**.
That is not a leak and not a mistake — it is the 90,000,000 changes' worth of
dead tuples the insert/delete churn leaves behind on a table that is written and
deleted at 20,000 rows/s, and PostgreSQL's autovacuum has not yet reclaimed them.
`VACUUM FULL` would take several minutes of exclusive lock on a table the next
campaign may want, so the space is reported rather than taken back.

**Disk went 41 GB free → 39 GB free across the whole campaign**, against a peak of
**25 GB free** in the middle of WINDOW S when the slot was holding 12.5 GB of WAL.
Note the floor honestly: **this box sits at 89% used, i.e. 11% free, at rest** —
below the 15% floor the brief names — and it did before this campaign started. The
absolute floor this campaign enforced instead was **20 GB free**, acted on by the
sampler, which stops the writer server-side; the valve fired long before it.

## What this proves, and what it does not

**It does prove**, at 0.5 CPU / 256 MB on PostgreSQL 16 → ClickHouse 24.8, with
these thirty tables in ONE CDC group over ONE replication slot, that:

- the uncapped source **can** offer the requested ~1,000,000 changed rows per
  table per minute (it offered ~1,028,000, 54,000,000 changes in ~105 s), so the
  question is not a generator artefact;
- the drain **cannot** absorb it. The measured sustained rate is 6,866 changes/s
  and the requested rate is 72.8× that; the offered stream is retained on the
  source as a straight ~160 MB/s line and PostgreSQL's `max_slot_wal_keep_size`
  ends it at 12.5 GB with `can no longer get changes from replication slot`, exit
  9, `oom_kill 0`;
- memory is **not** the binding constraint anywhere in this campaign: 62.8 MB peak
  against a 256 MB cage on the largest working drain, 59.4 MB on the one that hit
  the ceiling, flat within a run and not trending up across runs;
- the drain is **correct** on what it does apply: 3,990,000 changes converged and
  verified **30/30 MATCH** with an empty drain reporting 0, and a strictly
  incomplete landing verified **30/30 MATCH against the source reconstructed at
  the drain's own watermark**, with a control that reproduces the plain source
  digest 30/30;
- the group's watermark advances as **one value** across all thirty tables, and
  descriptors return to 4 after every drain.

**It does not prove** any of the following:

- **Anything about the drain's internals at 500,000 changes/s.** It applied zero
  changes in 78 s at that rate and the log contains no progress output before the
  error. The cage demonstrably received 94.5% of its 0.5 CPU (measured), so the
  cause is not quota delivery, and **this campaign does not diagnose it further.**
  A reader who wants the answer wants a profile, not a benchmark.
- **A 90,000,000-change stream was never generated end to end.** 54,000,000 were,
  and the remaining 36,000,000 could not be stored: 27.9 GB of WAL against 37 GB
  free with a 5.1 GB destination already landed. The 3-minute window as asked for
  is a statement about this box's disk as much as about the drain.
- **The apply rate is one rig, one schema, one day.** 30 tables of a specific
  15-column shape (~250 B/row, `json` not `jsonb`, no TOASTed values, 1,000-row
  transactions) into a single-node ClickHouse 24.8 with no Keeper. A wider table,
  a different page-cache behaviour or `jsonb` would move both 6,866 and the 310
  B/change, and therefore both the 72.8× ratio and the 36.4-core arithmetic.
- **Nothing about 24 hours, or about a backlog that grows slowly for a long
  time.** Three windows, the longest 832 s, is a window and not a soak.
- **The writer's wall time is derived, not timed**, because of fault 3 in
  `writer_wait`. The 103 s is the integral of a measured WAL rate against a
  measured WAL total, cross-checked against the curve's LSN column. The
  *transaction count* — 54,000 — is exact and comes from the writer's own ledger.
- **The offered rate is a property of this writer's statement shape, and the
  measurement of it has a 1.6x spread.** A generator using wider transactions, or
  one connection per table rather than fifteen, would measure a different ceiling.
  The calibration ran the identical measurement twice and gave 314,992 and
  262,693 changes/s at 15 sessions; the real window then reached ~514,000. All
  three numbers are printed. Nothing in the applied-vs-offered ratio depends on
  which is right, because the offered side is measured from the server's own WAL
  LSNs over the window that actually ran.
- **The `max_slot_wal_keep_size=12GB` valve is a choice, and the ceiling's
  *timing* depends on it.** A larger valve on a larger disk would have moved
  t≈78 s out to t≈117 s and changed nothing about the slope or the ratio. The
  *shape* of the backlog — linear, source-side, at ~160 MB/s — does not depend on
  the valve at all.
- **Two rig defects cost this campaign two re-bootstraps** (faults 1 and 4) and one
  wasted stress leg. They are recorded above with their fix rather than quietly
  omitted, because every one of them produced output that read like a data
  verdict and none of them was one.

## Reproducing

```bash
# on the bench VPS, from the repo
rsync -a -e "ssh -i ~/.ssh/apitap_vps" --exclude .git --exclude target \
      benchmarks/ ubuntu@<vps>:~/apitap-lib/benchmarks/
cd ~/apitap-lib

H=benchmarks/bench-capped-pg-ch-cdc-stress-0.57.sh
bash benchmarks/bench-cdc-stress-srcrig.sh      # this campaign's source settings
bash benchmarks/bench-cdc-stress-calibrate.sh all 2>&1 | tee ~/bench-cdc-stress/logs/calibration.log
bash benchmarks/bench-cdc-stress-cpu-probe.sh   2>&1 | tee ~/bench-cdc-stress/logs/cpu-probe.log

bash $H start        # record the state found, graveyard, control GREEN x4, the
                      # reconstruction control — or the campaign does not start
bash $H bootstrap    # ONE group over 30 tables, ONE slot, empty destination, verify
bash $H calm         # WINDOW C: paced, converges, verify 30/30
SLICE=600 STRESS_BAND=964801 STRESS_TXN=120 STRESS_TICK=1.5 SLICE=120 \
    bash $H stress   # WINDOW S: the requested rate; the valve fires
STRESS_BAND=964801 bash $H prefix   # the applied prefix, and its control
bash $H leaks && bash $H wsleg && bash $H drop && bash $H state
bash benchmarks/bench-capped-pg-ch-cdc-stress-raw.sh \
     benchmarks/bench-capped-pg-ch-cdc-stress-0.57-raw.log
```

`SLICE` is the drain's wall budget per container, in seconds, and it is the knob
that decides whether the window's ending is the ceiling (a slice long enough to
outlive the valve) or the budget. `STRESS_BAND` gives each window its own 800-row
update band and must be the same value in `stress` and `prefix`.