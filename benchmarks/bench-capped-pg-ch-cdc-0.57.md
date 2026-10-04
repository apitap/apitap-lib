# Capped tier, PostgreSQL → ClickHouse **CDC**: apitap 0.57.0 vs walshadow 0.1.2

The CDC version of the capped comparison, on the same tier as the two bulk arms:
**0.5 CPU and 256 MB of RAM per job**, **thirty** tables of a million rows each, all
thirty in **one CDC group** — one replication slot, one publication, one transfer
call — each tool inside one container. apitap is the PyPI wheel **0.57.0**;
walshadow is **v0.1.2** of `ClickHouse/walshadow`, installed from its own
sha256-verified release artifacts. Every landed table was checksum-validated
against PostgreSQL before any number counted.

The short version: **apitap caught up all 30,000,000 rows in a median of 63.8 s at
a 105 MB peak, 60 of 60 tables checksum-MATCH across two rounds. walshadow was
OOM-killed in all eight of its legs — both passes, both of its configurations — in
5–10 s, with at most 70,656 of 30,000,000 rows landed.** Then, with the group
established, apitap drained a **five-minute** window of **2,970,000 changes** at a
peak of **65.6 MB that does not trend up**, at **6,143 changes/s** while busy, the
group's watermark advancing monotonically as one value across all thirty tables,
and the destination matching the source exactly afterwards — **30 of 30 tables, each
at exactly 1,036,000 rows**, which is the change stream's arithmetic
(1,000,000 + 45,000 − 9,000) arriving on both sides. No leaked descriptors, no
leaked lease, no staging or marker table left behind.

Rig: OVH bench VPS (16 vCPU / 61 GB, shared with production), source
`apitap-bench-cdc-pg` (`postgres:16.13-alpine` on 127.0.0.1:5548, `wal_level=logical`,
db `bench`), destination `apitap-bench-cdc-ch` (`clickhouse-server:24.8.14.39` on
127.0.0.1:8128 HTTP / 9128 native). Both containers, both volumes and both names
belong to this campaign; nothing that existed before it was touched. Raw receipts:
[bench-capped-pg-ch-cdc-0.57-raw.log](bench-capped-pg-ch-cdc-0.57-raw.log).

## The seed — 30 identical tables, kept between every run

`public.cdc_pg_t01` … `cdc_pg_t30`, generated from `id` alone by **the same schema
file the two bulk arms used** and then `CREATE TABLE … LIKE`-cloned, so all thirty
are byte-identical, and the two campaigns describe one dataset rather than two:

```
id integer · small_str varchar(20) · medium_str varchar(100) · large_str varchar(500)
tiny_int smallint · regular_int integer · big_int bigint · float_val double precision
decimal_val numeric(18,4) · bool_val boolean · date_val date
ts_val timestamp(6) · ts_tz_val timestamptz · json_val json · extra_text text
PRIMARY KEY (id)
```

At the start of the campaign: **30 tables × 15 columns × exactly 1,000,000 rows**,
258 MB per table, **7,748 MB total**, seeded in **171.0 s** and excluded from every
measurement. The check that the clones are clones is the source aggregate: all
thirty print the identical digest, and it is *the same digest the bulk arm's ten
tables printed* — `1000000|2147445026505068|1|1000000|31670112|32000000|37518996|10309|12658|12048|11235`
— because it is the same schema and the same generator. The seeds were never
dropped; every destination table was dropped after its verification.

Three properties of the generator are inherited from the bulk arms and are
disclosures, not conveniences ([schema.sql](bench-capped-pg-ch-schema.sql) says so
at the top): **no TOAST** (every text value stays under 200 bytes, so nothing is
stored out of line and walshadow's TOAST-chunk mirror tables are never exercised);
**exact floats** (multiples of 1/8 below 1250, so PostgreSQL's shortest-round-trip
text and ClickHouse's `toString` agree byte for byte); and **`json`, not `jsonb`**
(`json` preserves the input text verbatim; `jsonb` would re-render it, which would
make the checksum a test of JSON normalisation). Four columns carry NULLs on a prime
modulus of ids (~1% each), so the checksum also proves a NULL survives the trip.

**One thing changed in the source during the campaign, and it is stated here rather
than discovered later.** The five-minute window's own change stream is applied to
these same tables — that is what a change stream *is* — so after the window each of
the thirty holds **1,036,000 rows** (8,419 MB) rather than 1,000,000. The cold
catch-up was therefore run **twice**: once on the 30 × 1,000,000 source the brief
specifies, and once more on the post-window source at 30 × 1,036,000. Both passes
are reported below, and both are checksum-validated against the source state that
was current for that pass. (The cached per-table sums are keyed on the *validator
text*, not on the data, so after the window they describe the seed and not the
source; refreshing them is a 97 s scan and is what the second pass used.)

## What this campaign added to the walshadow rig

Two answers to questions this campaign was briefed to expect, both different from
the brief — and both were found by measurement, not by reading the docs:

- **The module has to match the source's major, and the release ships one per
  major.** The bulk arm's rig is PostgreSQL 18 because the pg18 module was on the
  box; this rig is PostgreSQL 16, so walshadow needed
  `walshadow-pgext-0.1.2-pg16`, and a *separate image* built from `postgres:16`.
  The rule is upstream's own: "match image to source PostgreSQL major — physical
  bootstrap cannot cross major versions", and it applies to the module as much as
  to the data, because the shadow PostgreSQL walshadow supervises is a physical
  copy of the source.
- **The base image is the Debian one, not alpine.** The published module links
  against glibc (`ldd` → `libc.so.6`), so it cannot be loaded into a musl image at
  all. The image is `postgres:16` with two verified files dropped in, and **nothing
  is compiled on this box**:

  ```
  walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz            f0645c15…7e01  (the daemon)
  walshadow-pgext-0.1.2-pg16-x86_64-unknown-linux-gnu.tar.gz a1d42b9a…cb22  (the PG module)
  ```

  both matching the release's published `SHA256SUMS`, and
  `walshadow-stream --version` inside the image printing
  `walshadow-stream 0.1.2 (73329e4)` — the release commit.

Everything else about walshadow's requirements was the same as the bulk arm's and is
not repeated here: no ClickHouse Keeper is needed (it creates a plain
`ReplacingMergeTree(_lsn, _is_deleted)`), and the source's `pg_hba.conf` needs a
`host replication` trust line the official image omits for non-loopback peers —
without it the daemon's answer is not an error, it is `source unreachable — waiting
for it` every two seconds, forever. That line is in this rig's `pg_hba.conf` and in
the raw log.

## The control — GREEN ×4, before anything was timed

A validator that has only ever agreed is untested, and so is a CDC lane that has
never run end to end on a rig. [bench-capped-pg-ch-cdc-control.sh](bench-capped-pg-ch-cdc-control.sh)
drives the campaign's own harness over one 1000-row control table with the
campaign's schema, in the capped cage, and it is expected to be GREEN on all four
legs or the campaign does not start:

1. **AGREEMENT** — apitap's CDC landing digest equals the source digest, exactly:
   `1000|2151639950269|1|1000|31680|32000|37544|10|12|12|11`.
2. **SENSITIVITY** — changing one value in one row (`extra_text` of `id=777`) moves
   the digest (`2151639950269` → `2154172601381`), and restoring it brings it back,
   so a MATCH means something.
3. **THE CDC LANE ITSELF** — 50 inserts, 40 updates and 10 deletes on the source,
   drained again: the landing follows (`1040|2228604318474|…`, source and
   destination identical), and the second drain **in the same process** reports
   **0 changes**, so an empty drain is a no-op. Without this leg a checksum would
   only ever have proved the bootstrap.
4. **THE RIVAL** — walshadow's landing passes the *same* validator, so a MATCH later
   is a statement about the transfer and not about a rule one tool satisfies and the
   other cannot. Uncapped on purpose: that leg exists to prove the rival's landing
   can be *read* by the validator, and a control that failed on a memory ceiling
   would report "the validator is untested" when what it means is "the rival does not
   fit".

Leg 4 **caught a bug in its own harness** on the first run and is the reason this
section exists: the rival leg polls `system.tables.total_rows` against
`EXPECT_ROWS`, and `EXPECT_ROWS` was still the control's *starting* 1000 rows, so the
poll declared "caught up" at 1,024 of 1,040 rows, SIGTERMed the daemon mid-COPY and
printed RED. The number a leg must reach is the source's current count, and the leg
now takes it from the source. Re-run: **GREEN ×4**.

### Three faults the control could not have caught, and one it did

The validator definitions are the bulk arm's, reused verbatim
([bench-capped-pg-ch-validator.sh](bench-capped-pg-ch-validator.sh), with the two
container names made overridable so a second campaign cannot carry a second copy
that drifts). Everything new around them was mine, and three of those things were
wrong in ways that *looked like data verdicts*:

- **ClickHouse string literals do not accept `\_`.** Every `LIKE 'cdc\_pg\_t%'` was a
  `SYNTAX_ERROR` that the drop step swallowed — so the destination was never actually
  dropped between legs, and the second tool inherited the first tool's table shape
  (a plain `MergeTree` with no `_lsn`) and died on `No such column _lsn`. All the
  destination patterns are now `startsWith`/`position`, which are escape-free.
- **Naming the outer column of a derived table needs `AS q(d)`, which PostgreSQL
  accepts and ClickHouse rejects outright** ("Expected one of: FINAL, SAMPLE,
  table…": it parses `q(d)` as a table function). The batch form is
  `SELECT 't', * FROM (…) AS q` plus a `join_digest` that puts the `|` back.
- **`psql -A` separates unaligned output with `|`, not with a tab** (clickhouse-client's
  TSV uses tabs). Splitting psql's output on tabs yields one field and an empty
  digest — which printed as thirty `MISMATCH`es rather than as an error.
- A guard meant to catch exactly that class of fault (`awk … END { exit n+0 }` inside
  an `||` list) itself misreported for a while, and now compares an explicit count
  instead of an exit status.

None of these changed a published number, and all of them are recorded here because
the alternative — a report whose checksums were quietly vacuous for two rounds — is
the failure mode this project's own memories keep warning about. Every number
below comes from a leg whose checksum was read *after* these were fixed.

## Results

Every arm = the same 30 tables, the same cap, one container, one job, from a
pre-seeded source and an **empty** destination.

### Pass A — the source the brief specifies: 30 × 1,000,000 rows

| arm | round | wall in container | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|
| **apitap 0.57.0** (1 process, 1 group, 1 slot) | 1 | **66.2 s** | 105.2 MB | exit 0, not OOM-killed | 30,000,000 | **30/30 MATCH** |
| | 2 | **61.4 s** | 103.4 MB | exit 0, not OOM-killed | 30,000,000 | **30/30 MATCH** |
| | **median** | **63.8 s** | **105.2 MB** | | **30,000,000** | **60/60 MATCH** |
| **walshadow 0.1.2** (upstream defaults) | 1 | 5.4 s → killed | ≥256.0 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/30 |
| | 2 | 10.3 s → killed | ≥256.0 MB † | **OOMKilled=true, exit 137** | 70,656 ‡ | 0/30 |
| **walshadow 0.1.2** (its own small-box settings) | 1 | 5.4 s → killed | ≥256.0 MB † | **OOMKilled=true, exit 137** | 33,190 ‡ | 0/30 |
| | 2 | 7.9 s → killed | ≥256.0 MB † | **OOMKilled=true, exit 137** | 66,378 ‡ | 0/30 |

† **The peak is a lower bound, and that is the honest form of it.** The kernel's
`memory.peak` lives in the container's cgroup, and a container the kernel OOM-killed
takes that cgroup with it — so for a leg that dies the peak is the maximum of
`memory.current` sampled from the host at 20 Hz, and it reads 256.0 MB against a
256 MB cap. What makes the verdict unambiguous is not the peak but
`State.OOMKilled=true` with `ExitCode=137` on **eight of eight** legs across both
passes, which is the kernel saying so outright.

‡ "rows landed" is `system.tables.total_rows` at the kill, and it is not spread
evenly: in every walshadow leg **29 of the 30 tables held 0 rows** and the one table
its bootstrap lane happened to be working on held the rest. The best leg of the
four landed **0.24%** of the job. The destination is a *partial table*, not a
shorter table, which is why every walshadow leg's checksum verdict is 0/30 rather
than a count mismatch.

There is no completion time to report for walshadow, so its wall column is
**time-to-kill**, not a duration. A median survival time is a weak statistic and is
printed only so nobody has to reconstruct it; the result is the outcome.

### Pass B — the same legs after the window, 30 × 1,036,000 rows

This is the pass whose per-arm daemon logs are all intact: pass A gave both
walshadow arms of a round the *same* log file, so the second truncated the first.
The OOM verdict survived that (it is read from each leg's own container before
removal) but the evidence did not, so the whole interleaved campaign was re-run
with the arm in the container's name.

| arm | round | wall in container | transfer only | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|---|
| **apitap 0.57.0** | 1 | **67.2 s** | 65.4 s | 134.3 MB | exit 0, not OOM-killed | 31,080,000 | **30/30 MATCH** |
| | 2 | **67.3 s** | 65.4 s | 125.2 MB | exit 0, not OOM-killed | 31,080,000 | **30/30 MATCH** |
| | **median** | **67.2 s** | 65.4 s | **134.3 MB** | | **31,080,000** | **60/60 MATCH** |
| **walshadow 0.1.2** (upstream defaults) | 1 | 5.4 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/30 |
| | 2 | 5.4 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/30 |
| **walshadow 0.1.2** (its own small-box settings) | 1 | 5.5 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 33,199 ‡ | 0/30 |
| | 2 | 5.3 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 33,199 ‡ | 0/30 |

3.6% more rows costs 5.4% more wall and 28% more peak, and both rounds still read
30/30. (The raw log's seed section prints 281 MB per table, not the 258 MB
quoted above: it was generated after the window, and these are the tables the
window wrote to.) Each walshadow leg died at the same place, and its own log says where:

```
INFO walshadow::bootstrap: catalog map seeded relations=60 catalog_filenodes=272 mode=Direct
INFO walshadow::bootstrap: bootstrap insert tail started lanes=1 inserters=3 row_budget=4194304 byte_budget=268435456
<killed here>
```

`relations=60` is the thirty tables and their thirty indexes. The last thing the
daemon logs before dying is the start of the *insert tail* — it has taken its
physical base backup and catalog map and is now streaming rows into ClickHouse,
which is precisely the phase its 550 MB daemon footprint (measured on the bulk arm's
rig, and not re-measured here) cannot reach inside 256 MB.

The tuned arm is not a strawman either way: it is what walshadow's own
`configuration.md` prescribes for a memory-capped box, and it is what a reader would
try first.

| walshadow setting | upstream default | tuned arm |
|---|---|---|
| `xact-buffer-max` (entrypoint-injected) | **1073741824** (1 GiB) | 33554432 (32 MiB) |
| `ch.byte_budget` | 268435456 (256 MiB — the whole cage) | 33554432 (32 MiB) |
| `ch.inserter_pool_size` | 8 | 2 |
| decoder pool | 3 | 2 |
| `memory.resident_payload_max` | **512 MiB floor** ‡‡ | 67108864 (64 MiB) |
| `memory.value_reserve` | 64 MiB | 8388608 (8 MiB) |

‡‡ Its documented default is "one half of cgroup memory limit, **with a 512 MiB
minimum**". A 256 MB cage cannot satisfy a 512 MiB floor, so out of the box
walshadow is told it may hold roughly twice the memory that exists. Both arms ran
because "walshadow failed" is only an honest sentence if it also failed with its own
best settings — and it did, four of four, in this pass and four of four in the
previous one.

## The five-minute window

One group, established by a bootstrap that is reported but is **not** the headline
(60.7 s wall / 59.0 s in-container, 101.3 MB peak, 30,000,000 rows, **30/30 MATCH**),
then a steady change stream across all thirty tables drained on a ~60 s cadence for
five minutes.

**The change stream, with the exact counts.** 450 ticks, one every 0.65 s, each tick
issuing three statements against every one of the thirty tables in a single psql
session on the source, every statement its own implicit transaction. Per tick, per
table: **100 INSERT** (fresh ids from 2,000,000 upward), **100 UPDATE** (one
contiguous band of the seed's own id range, which nothing else touches, setting
`regular_int = regular_int + 1`), **20 DELETE** (ids from 900,001 upward, disjoint
from the update band). The three bands never intersect, so the counts are exact *by
construction* — and the source's own row counts are what confirms it:

```
450 ticks × 30 tables × 220 changes = 2,970,000 changes
  per table:  45,000 INSERT   45,000 UPDATE   9,000 DELETE
  net rows:   1,000,000 + 45,000 − 9,000 = 1,036,000
WINDOW_DONE ticks 450 per_table_ins 45000 per_table_upd 45000 per_table_del 9000 expected_rows 1036000
```

The generator is [bench-capped-pg-ch-cdc-window.py](bench-capped-pg-ch-cdc-window.py)
and it is piped from the host straight into `psql` — the statement stream is never
written to disk, and its progress is an artifact (`\echo WINDOW_PROGRESS tick
k/450`), never a `pgrep`. The `UPDATE` changes a column that is inside the
checksum's row text on purpose: an UPDATE that rewrote a row to the values it already
held logs nothing at all and makes the whole checksum a test of nothing.

**The writer's own throughput: 2,970,000 changes in 811.1 s = 3,662 changes/s
sustained.** That is the honest number for *generation*, and it is below the 0.65 s
per tick the generator asks for — the writer was throttled by the source's own
commit rate while the drains were competing for the same box, not by anything in the
cage.

| drain | changes applied | wall (in-container) | changes/s | cgroup `memory.peak` | FD peak | watermark after | slot's retained WAL |
|---|---|---|---|---|---|---|---|
| 1 | 534,600 | 96.6 s | 5,536 | 65.6 MB | 13 | 10,832,350,944 | 416 MB |
| 2 | 1,207,800 | 192.8 s | 6,265 | 64.4 MB | 13 | 11,423,948,760 | 628 MB |
| 3 | 1,227,600 | 194.1 s | 6,323 | 65.4 MB | 13 | 12,025,112,328 | 34 MB |
| 4 | 0 | 3.7 s | — | 40.7 MB | 12 | 12,025,113,736 | 56 B |
| 5 | 0 | 3.2 s | — | 40.7 MB | 12 | 12,025,116,440 | 2,816 B |
| 5 (second pass, same process) | 0 | 3.5 s | — | 40.7 MB | 12 | — | — |
| final | 0 | 3.3 s | — | 40.7 MB | 12 | 12,025,119,224 | 23 MB |
| final (second pass, same process) | 0 | 3.4 s | — | 40.7 MB | 12 | — | — |
| **total** | **2,970,000** | **483.5 s busy** | **6,143** | **65.6 MB** | | | |

Reading that table, one number at a time:

- **The applied total is exactly the generated total.** 534,600 + 1,207,800 +
  1,227,600 = **2,970,000**, and the last three drains report **0**, i.e. the group
  converged rather than merely accumulating. Nothing was lost and nothing was
  double-counted, and the two second-pass drains reporting 0 are the idempotence
  property measured rather than asserted.
- **Peak memory does not trend up.** 65.6 → 64.4 → 65.4 MB across the three drains
  that did work; the 40.7 MB of the idle drains is the wheel's floor with the
  transfer done. The largest number anywhere in the window is the *bootstrap's*
  101.3 MB — 30 million rows in one group costs more than 3 million changes.
- **The watermark advances monotonically, and as ONE value for the whole group.**
  Every reading is `distinct=1` across all thirty tables, and the sequence only goes
  up. A group whose members drifted apart would show `distinct>1` here, which is
  the failure this lane exists to prevent.
- **Descriptors do not grow.** `FD_PEAK` is 13 while draining and `FD_END` is 4 in
  every container; and in the two legs that ran **two drains in one process**, the
  count was 4 before pass 1, 4 after pass 1, 4 before pass 2, 4 after pass 2.
- **The cadence slipped, and the reason is in the table.** Each drain's own wall
  (97–195 s) exceeded the 60 s sleep, so the drains landed at roughly t+98, +352 and
  +548 s rather than on a clean minute. The slot's retained WAL is the visible
  consequence of that: it grew to 628 MB while a drain was still working and fell
  back to kilobytes once the group caught up. That is the shape to expect — the
  backlog lives in PostgreSQL's WAL on disk, never in the worker's memory.

**The destination matches the source after the final drain** — recomputed from
scratch on both sides, because the cached sums describe the pre-window seed:

```
VERIFY_SUMMARY checked=30 of 30 match=30 mismatch=0
  every table: rows=1036000 digest=1036000|2225213684394444|1|2044999|32810240|33152000|38869668|10680|13114|12482|11640
```

1,036,000 rows and one identical digest on all thirty, on both engines. That single
line is the whole window: the exact arithmetic of 45,000 inserts, 45,000 updates and
9,000 deletes per table, propagated through one slot into ClickHouse and back out as
a checksum that agrees with PostgreSQL row for row.

## Leak checks, taken with the window's state still intact

| what | reading |
|---|---|
| descriptors, per drain | `FD_PEAK` 12–13 while draining, `FD_END` **4** in every container; 4 before/after each of two drains in one process |
| replication slots left | one: `apitap_g25786459cc6`, logical, inactive, `wal_status=reserved`, retaining **23 MB** |
| the CDC group's publication | `apitap_g25786459cc6_pub` — the group's own state, not a leak |
| spilled transactions | **0** txns / 0 spills / 0 bytes |
| `_apitap_lease` | **661 rows**, of which **0 uncollected and still live** |
| staging / marker leftovers | **none** — no `*__apitap*` table of any kind |
| `_apitap_cdc_pending` | **does not exist** |
| the group's state rows | **30** rows, `mode=log_based`, `cursor=_lsn`, **1 distinct watermark** (12,025,119,224) |

The 661 lease rows are the documented behaviour rather than a leak: lease rows are
written per run and per table, are never garbage-collected on any engine (age-based
reaping is forbidden because an uncollected row must never age out), and what
matters is that **none of them is uncollected and live** — no table is left locked.

The staging answer deserves one sentence of honesty rather than a bare "none".
During a run there *is* a run-scoped artifact: ClickHouse's own dropped-table
metadata names them, e.g. `default.cdc_pg_t01_0tmd20lrfxze9dn__apitap_staging` — a
per-run staging table carrying the run token. It is dropped at the end of the run
and swept by token, and the drop check after every leg reports **0** leftovers. That
is a measurement that the artifact exists and a measurement that it does not
outlive its run, which is stronger than either alone.

## The exact commands

The cap, identical for every arm:

```
--network=host --cpus=0.5 --memory=256m --memory-swap=256m
```

**apitap** — the group, one process, one transfer call, one slot, no knobs
(`parallel=`, `chunk_bytes`, `slots=` and every env lever left alone; the wheel is
mounted read-only and put on `PYTHONPATH`, so the PyPI wheel is what runs and nothing
is built):

```bash
docker run --name apitap-bench-cdc-r1 --network=host $CAP \
    -v /home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages:/py:ro \
    -e PYTHONPATH=/py -v ~/apitap-lib/benchmarks:/job:ro \
    -e "APITAP_TABLES=$(printf 'cdc_pg_t%02d ' {1..30})" \
    python:3.13-slim sh /job/bench-capped-pg-ch-cdc-leg-apitap.sh
```

```python
# inside the cage — the same call is the cold catch-up (empty destination) and
# every drain afterwards; there is no separate "catch-up" API to be fair about.
apitap.transfer('postgres://postgres:bench@127.0.0.1:5548/bench',
                'clickhouse://default:bench@127.0.0.1:8128/default',
                tables=['cdc_pg_t01', … 'cdc_pg_t30'], mode='log_based')
```

It auto-sized to a **shared pipe budget of 8** for the whole job in every round
(read off the cgroup limit, no flag); the drains report `budget=1`. The leg prints
the wheel's version and the `_apitap.abi3.so` md5 from inside the cage, and samples
`/proc/self/fd` at 20 Hz throughout, which is where the descriptor numbers come from.

**walshadow** — its own documented model: one daemon for the whole set of tables,
configured rather than flag-driven, with the thirty `[table.public.*]` blocks and
nothing else (`replicate_all = false`). Discrete keys only, because `url` is a CLI
and environment form rather than a documented `[source]` key, and an unknown key in a
non-`[table]` section is silently ignored — which would look like "it connected and
replicated nothing".

```bash
docker run --name apitap-bench-cdc-ws-r1-def --network=host $CAP \
    -e WALSHADOW_PG_URL='postgres://postgres:bench@127.0.0.1:5548/bench' \
    -e WALSHADOW_CH_URL='clickhouse://default:bench@127.0.0.1:9128/default' \
    -e WALSHADOW_SHADOW_PORT=5442 -e WALSHADOW_WALSENDER_BIND=127.0.0.1:5433 \
    -v ~/bench-cdc-ws:/var/lib/walshadow -v ~/bench-cdc-ws-conf:/etc/walshadow \
    apitap-bench-ws16:0.1.2
```

The container is walshadow's own deployment unit — **the daemon plus the shadow
PostgreSQL it owns**, which is how upstream ships it. One operator fact, unchanged
from the bulk arm and still true here: the shadow's memory settings are inherited
from the source's `postgresql.conf`, because `BASE_BACKUP` copies the data directory
including that file.

## Interleaving, host state, and rig hygiene

Both passes ran their arms interleaved **apitap · walshadow-default ·
walshadow-tuned**, n=2, because this box carries other containers and a block of one
tool would confound host drift with engine identity. `loadavg` (1-min) around the
pass-B legs ranged **5.18 – 19.91** and `MemAvailable` sat between **45.8 GiB and
46.2 GiB** throughout; every before/after pair is in the raw log. At a 0.5-CPU quota,
CPU is 0.5 × wall for any leg that saturates the quota, so wall and CPU are the same
measurement here and are reported once.

Dropped after every verification: all 30 destination tables. Two rig facts worth
recording because they cost real time:

- **ClickHouse keeps a dropped table's data on disk for eight minutes.** Its own
  description of `database_atomic_delay_before_drop_table_sec` says the delay exists
  so `UNDROP` can find the table, and that it is **ignored for a `DROP TABLE …
  SYNC`**. Measured: three consecutive 8 GB landings without `SYNC` took the box
  from 42 GB free to **9.5 GB free** — below the campaign's own 15% floor — and the
  parts stayed on disk long after the delay had expired. `cmd_drop` drops `SYNC` and
  the store went back to ~130 MB. That is drop hygiene on this campaign's own
  ClickHouse; it does not touch the measured path.
- **The source's `max_wal_size` was reduced from 8 GB to 4 GB** part-way through,
  because `pg_wal` was sitting on 7.8 GB of recycled segments on a box at 88%. It is
  a setting on the **uncapped source**, and it changes nothing about what the capped
  container may do; both reported passes ran with it at 4 GB.

## What this does and does not prove

**It does prove** that at 0.5 CPU / 256 MB, on PostgreSQL 16 → ClickHouse 24.8, with
these thirty tables, apitap 0.57.0 establishes and catches up a **single** 30-table
CDC group over **one** replication slot in a median of **63.8 s at a 105 MB peak**
with every table checksum-verified (60 of 60 across two rounds), and that it then
sustains **2,970,000 changes over a five-minute window at a 65.6 MB peak that does
not trend up**, keeps the group's watermark moving monotonically as one value, leaks
no descriptors, no lease and no staging table, and ends the window with all thirty
tables matching the source exactly. walshadow 0.1.2 is OOM-killed doing the same job
in both its shipped configuration and the one its own documentation prescribes for a
small box, landing at most 0.24% of it — **8 of 8 legs**.

**It does not prove** any of the following, and none of it should be read into the
tables above:

- **The cap is per tool, and the destination is uncapped.** Only the loader is in a
  cage. Both tools wrote into the same full-size ClickHouse, so these numbers are
  "what each tool can do while the database is free to help".
- **The two tools are not doing the same job.** apitap's group call is a bootstrap
  plus a drain, done when it returns. walshadow is a CDC daemon measured as
  **catch-up**: it takes a physical base backup of the source, replays WAL through a
  shadow PostgreSQL it owns, and then keeps running. The comparison is
  catch-up-time vs catch-up-time, both from an empty destination and a pre-seeded
  source. It is **not** a claim that one tool is N× "faster at loading": a
  31,080,000-row catch-up includes a 7.7 GB physical backup, which a loader never
  performs.
- **For walshadow the cage contains a database; for apitap it does not.** The cap
  covers the daemon *and* the shadow PostgreSQL, because that is upstream's own
  deployment unit. That is not neutral: it works against walshadow in principle. It
  did not decide the outcome — the bulk arm's attribution run measured the daemon
  alone at 550 MB before any PostgreSQL process existed — but a reader should not
  have to take that on trust, and this campaign did not re-measure it on PG16.
- **walshadow's own defaults cannot fit this tier, which is a configuration fact,
  not a tuning failure.** `resident_payload_max` has a documented 512 MiB floor and
  `byte_budget` defaults to 256 MiB, on a cage of 256 MB. The tuned arm sets both,
  plus a 32 MiB transaction buffer and 2+2 pools, and is still killed in ~5 s.
- **Neither catch-up number is comparable to the bulk arms' 18.4 s / 36.1 s.** Three
  things differ at once: 30 tables instead of 10, the CDC lane instead of a bulk
  replace (which must additionally establish a slot, a publication and a snapshot
  coordinate), and PostgreSQL 16 instead of 18. The two campaigns' apitap figures
  should be compared only within their own arm.
- **The headline is n=2, and that was the owner's explicit ask** ("the owner wants
  this quick"). Two rounds is thinner than the three the bulk arms used, and it is
  why the campaign was then run a *second* time on a larger source rather than a
  third round on the same one: the replication across both passes (63.8 s median at
  30.0 M rows, 67.2 s at 31.08 M, 30/30 both times) is the evidence offered in place
  of a third sample.
- **A five-minute window is not a soak.** It is 2,970,000 changes over 811 s on
  tables of ~250 B, on a group whose peak did not move across three working drains
  and two idle ones. What it supports is "keeps up with a light steady stream and
  converges, with no growth in memory, descriptors, leases or staging over the
  window". It does not support any statement about 24 hours, about a growing
  backlog, or about a transaction larger than the window's own.
- **The writer was the bottleneck, not the cage.** 3,662 changes/s generated is well
  under what this tier can apply, so the numbers measure *keeping up*, not *maximum
  throughput*. The historical figure for a 10-table, 15-wide Postgres rig at this
  same tier is ~37K changes/s (benchmarks/cdc-stress.md); a 30-table group over one
  slot applied 6,143/s. That is a large difference and it is **not** explained by
  anything measured here — the rigs differ (different seed rows, different apitap
  version), and this campaign did not isolate the cause. It is reported as a number
  to ask about, not as a finding.
- **The drain cadence slipped** (each drain's own wall was 97–195 s against a 60 s
  sleep), so "five minutes, drained every 60 s" describes the intent; the table gives
  what happened.
- **The killed-drain self-heal test was deliberately skipped, not forgotten.** It
  needs a lease TTL shorter than the window, and the default is **300 s** with the
  keeper renewing every 30 s. Lowering a product default to make a five-minute test
  fit would have measured the override, not the product. It belongs in a longer
  campaign, and the raw log records the choice.
- **One rig, one schema, one day.** 30 M rows of a specific 15-column shape (~258
  B/row), no TOASTed values, a single-node ClickHouse with no Keeper, and a source
  with `shared_buffers` at 160 MB. A wider table, a different page-cache behaviour,
  or a `jsonb`/`numeric`-heavy schema would move the seconds. They would not move
  the OOM verdict, which reproduced 8 of 8 here.
- **The seed was reused, the destination never was** — and the seed was *written to*
  by the window, which is why the catch-up appears twice at two source sizes rather
  than three times at one.

## Reproducing

```bash
# on the bench VPS, from the repo
rsync -a -e "ssh -i ~/.ssh/apitap_vps" --exclude .git --exclude target \
      benchmarks/ ubuntu@<vps>:~/apitap-lib/benchmarks/
cd ~/apitap-lib/benchmarks

# the walshadow image: the release's own two artifacts, sha256-verified, on
# postgres:16 (Debian — the module links glibc and cannot load into alpine)
cd ~/ws-dl16 && curl -sSLO <the two release tarballs> && sha256sum -c SHA256SUMS
cp ~/apitap-lib/benchmarks/bench-capped-pg-ch-cdc-ws-{entrypoint.sh,image.Dockerfile} .
docker build -t apitap-bench-ws16:0.1.2 -f Dockerfile .

H=bench-capped-pg-ch-cdc-0.57.sh
bash $H rig        # apitap-bench-cdc-pg + apitap-bench-cdc-ch, this campaign's own
bash $H seed       # 30 tables, ~171 s, kept between every leg
bash $H srcsum     # source aggregates (cached per table; delete them to refresh)
bash $H drop && bash $H slotdrop
bash bench-capped-pg-ch-cdc-control.sh   # GREEN x4 before anything is timed
ROUNDS=2 bash $H catchup apitap wsdef wstuned     # pass A
bash $H drop && bash $H slotdrop && bash $H srcsum  # the window moved the source
ROUNDS=2 RESULTS=$HOME/bench-cdc/results-catchup-final.txt \
      bash $H catchup apitap wsdef wstuned          # pass B
bash $H window    # bootstrap + verify + 5-minute window + final fresh verify
bash $H leaks     # with the window's state still intact
bash $H state     # containers? disk? seeds? slots?
bash bench-capped-pg-ch-cdc-raw.sh bench-capped-pg-ch-cdc-0.57-raw.log
```

`LEG_TIMEOUT` (default 5400 s) is the walshadow leg's deadline: hitting it is
recorded as a result (`deadline_…s`) with the kernel's counters and whatever landed
in the destination captured *before* the container goes away. No leg in this
campaign needed it — every walshadow arm killed itself first.