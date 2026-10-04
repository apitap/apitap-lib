# Capped tier, MySQL → ClickHouse **CDC**: apitap 0.57.0 vs ingestr 1.1.61

The MySQL-source version of the capped CDC comparison, on the same tier as the
PostgreSQL CDC arm and the two bulk arms: **0.5 CPU and 256 MB of RAM per job**,
**thirty** tables of a million rows each and fifteen columns, all thirty in **one CDC
group** — one binlog stream, one coordinate, one `transfer()` call — each tool inside
one container. apitap is the PyPI wheel **0.57.0**; ingestr is **1.1.61**, its own
native binary. Every landed table was checksum-validated against MySQL before any
number counted.

The short version: **apitap caught up all 30,000,000 rows in a median of 106.2 s at a
119.1 MB peak, 60 of 60 tables checksum-MATCH across two rounds**, and again on the
post-window source at a median of 115.1 s / 124.7 MB with 60 of 60. Then, with the
group established, apitap drained **2,970,000 changes** — the exact arithmetic of the
stream offered — at a peak of **55.5 MB that does not trend up**, **2,880 changes/s**
while busy, the group's watermark advancing as **one value across all thirty tables**,
and the destination matching the source afterwards, **30 of 30 tables each at exactly
1,036,000 rows**. No leaked descriptors, no leaked lease, no staging table, no reader
left attached to the source.

**ingestr did not run at all, and the reason is a product boundary rather than a
memory ceiling.** ingestr 1.1.61 refuses ClickHouse as a managed-CDC destination,
before it reads a row:

```
Error: destination scheme "clickhouse" cannot safely run managed CDC:
destination-managed state with fencing, pruning, and truncation is not supported
```

**8 of 8 legs** (two rounds × two configurations, in both passes) exited **1** in
about 5.4 s with **0 rows** landed. So this arm reports **no catch-up time and no
footprint**, and it is *not* a statement that ingestr is slow or too large at this
tier — see [the rival's arm](#the-rival-refused-the-destination-not-the-memory) for
the measurements that pin the refusal to the destination and nothing else.

Two findings are about apitap, and both are stated here rather than discovered later:
**its MySQL CDC lane refuses a native `json` column**, so the campaign's seed stores
the document in a `longtext` column (the product's own suggested workaround, and
MariaDB's JSON is `LONGTEXT` anyway); and **ingestr's CDC needs `gtid_mode=ON`**,
which is not in its documented requirement list, so the rig was configured to the
*rival's* requirement and apitap's lane was then re-verified under GTID.

Rig: OVH bench VPS (16 vCPU / 61 GB, shared with production), source
`apitap-bench-cdc-my` — this campaign's own `mysql:8.0` (8.0.46) on 127.0.0.1:3313 with
its own volume — and destination `apitap-bench-cdc-ch`, the previous campaign's
`clickhouse-server:24.8.14.39` on 127.0.0.1:8128 HTTP / 9128 native, which this
campaign is explicitly allowed to share. Only `cdc_my_*` objects were ever created or
dropped there. No container that existed before this campaign was touched, moved or
restarted. Raw receipts:
[bench-capped-my-ch-cdc-0.57-raw.log](bench-capped-my-ch-cdc-0.57-raw.log).

## The seed — 30 identical tables, kept between every run

`cdc_my_t01` … `cdc_my_t30`, fifteen columns, from the capped **MySQL → ClickHouse bulk
campaign's** own seed table `bench.bench_my_1m` — and the local-clone route was
*proven* rather than assumed: that table's aggregate inside this rig equals
`apitap-bench-my.bench_my_1m`'s exactly (`1000000|2147266635853629|2147375244801801|1|1000000`),
so the thirty tables can only ever be the bulk arm's dataset.

```
id int · small_str varchar(20) · medium_str varchar(100) · large_str varchar(500)
tiny_int smallint · regular_int int · big_int bigint · float_val double
decimal_val decimal(18,4) · bool_val tinyint(1) · date_val date
ts_val datetime(6) · ts_tz_val datetime(6) · json_val longtext · extra_text longtext
PRIMARY KEY (id)
```

At the start of the campaign: **30 tables × 15 columns × exactly 1,000,000 rows**,
**12.67 GiB on disk**, seeded in **500.7 s** and excluded from every measurement. Every
count is a real `COUNT(*)`, and that is not fastidiousness — right after the seed,
`information_schema.tables.table_rows` claimed the thirty tables held between
**907,529 and 999,363** rows, which the harness prints alongside the exact figure
precisely because the gap is the kind of thing a report should not hide. The check
that the clones are clones is the source aggregate: all thirty print the identical
digest, and it is *the same digest the bulk arm's ten tables printed*.

**One declared type differs from the bulk arm's, and the reason is a product fact.**

> apitap 0.57.0's MySQL CDC lane **refuses** a table whose JSON column is a native
> `json`. Its own error, verbatim from the control's log:
>
> ```
> RAISED ValueError: log_based: bench.ctrl_json has JSON column(s) json_val and
> apitap cannot yet render MySQL's binary JSON encoding from the binlog. A CDC update
> would write the raw envelope where the full load wrote the document, so the run
> refuses instead of corrupting the column. Use mode='replace' or 'append' for this
> table, or store the document in a text column. (MariaDB is unaffected — its JSON is
> LONGTEXT.)
> ```

The refusal names its own workaround — "store the document in a text column" — and
that is what [the schema](bench-capped-my-ch-cdc-schema.sql) does. It is not an exotic
shape: MariaDB's JSON *is* `LONGTEXT`. The **values** are unchanged, and that is
measured rather than argued: over the same 1,000 ids, the aggregate's JSON-CRC term is
**2,130,606,287,102** on the campaign's `longtext` column and on the bulk arm's native
`JSON` column alike. The bulk arm's `mode='replace'` lane *did* take the native type,
so this is a restriction of the CDC lane specifically — a point the report would have
had to state either way, and one that the refusal reproduces on demand as control
leg 5.

**Binlog coordinates before any run:** `mysql-bin.000005` at position **7,748,752**,
GTID set `f1540e09-bfa6-11f1-b3c5-3cb1c3be6eee:1-224`, **5 files / 13 MB** on disk, and
**no reader attached**. The seed is loaded with `sql_log_bin=0`, for two reasons that
are disclosures rather than conveniences: a row-image binlog of a 12.67 GiB seed would
double the disk cost of a box with 39 GB free, and a real pre-seeded table's binlog was
rotated away long ago — it is the state *before* a campaign, not part of it. Every leg
therefore mints its own coordinate from an empty binlog tail, which is the position a
production source is in.

## The rig — and the three things the rivals required

The container's arguments, verbatim, and every CDC precondition read back from the
running server:

```
apitap-bench-cdc-my   mysql:8.0   127.0.0.1:3313   db bench   root/bench
  --server-id=3302 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL
  --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-expire-logs-seconds=86400
  --innodb-buffer-pool-size=2G --max-connections=200

log_bin = ON                     binlog_format = ROW
binlog_row_image = FULL          binlog_row_value_options =          (no PARTIAL_JSON)
server_id = 3302                 gtid_mode = ON
enforce_gtid_consistency = ON    binlog_expire_logs_seconds = 86400
innodb_buffer_pool_size = 2147483648
innodb_flush_log_at_trx_commit = 1        sync_binlog = 1
version 8.0.46
```

Three of those exist because a rival asked for them, and each is worth a line because
"we configured it our way" is not the same claim:

- **`gtid-mode=ON` is ingestr's requirement, not apitap's.** ingestr's MySQL CDC
  refuses to start with *"MySQL CDC requires gtid_mode=ON for lineage-safe
  checkpoints; current mode is \"OFF\""*. Its documented requirement list does **not**
  mention GTID, so this was found by running it. The rig was moved to the rival's
  requirement — the fair direction — and apitap's CDC lane was then **re-verified
  under GTID=ON** by the control before anything was timed.
- **`innodb-buffer-pool-size=2G`** replaces the image's 128 MB default, which makes a
  12.67 GiB seed's `INSERT…SELECT` and the CDC snapshot's full scans crawl. It is a
  setting on the **uncapped source** and it helps both tools identically.
- **`innodb-flush-log-at-trx-commit` and `sync-binlog` stay at 1** — the durability
  defaults — so the window's writer pays for its fsyncs exactly as the PostgreSQL
  window did. That is also why it was slow; see the window section.

`binlog_expire_logs_seconds=86400` is the bound that keeps a falling-behind drain from
filling this shared disk: the binlog *is* the backlog, and at 24 h the server rotates it
out from under a stalled reader rather than growing without limit. It was never needed
here (see the window: the binlog stabilised at 1.7 GB), and it is stated because it is
what makes the stress case survivable rather than fatal.

ingestr's own documented requirements are asserted from the server, not assumed: a
primary key on every table, **zero** `ENUM`/`SET`/`BIT` columns, and a `root` grant list
carrying `SELECT`, `RELOAD`, `RELOAD`-adjacent `FLUSH_TABLES`, `REPLICATION SLAVE` and
`REPLICATION CLIENT`.

## The control — GREEN ×5, before anything was timed

A validator that has only ever agreed is untested, and so is a CDC lane that has never
run end to end on this rig. [bench-capped-my-ch-cdc-control.sh](bench-capped-my-ch-cdc-control.sh)
drives the campaign's own harness over a 1,000-row control table in the capped cage,
and it is expected to be GREEN on all five legs or the campaign does not start:

1. **AGREEMENT** — apitap's CDC landing digest equals the source digest, exactly:
   `1000|2168502783323|2130606287102|1|1000`.
2. **SENSITIVITY** — changing one value in one row (`extra_text` of `id=777`) moves the
   digest (`2168502783323` → `2170094852481`) and restoring it brings it back, so a
   MATCH means something.
3. **THE CDC LANE ITSELF** — 50 inserts, 40 updates and 10 deletes on the source, drained
   again: the landing follows (`1040|2246973261221|2223082727652|1|2000050`, source and
   destination identical), and the second drain **in the same process** reports **0
   changes**, so an empty drain is a no-op. Without this leg a checksum would only ever
   have proved the bootstrap.
4. **THE RIVAL** — ingestr's CDC landing passes the *same* validator, digest and all:
   `1040|2246973261221|2223082727652|1|2000050`. See below for why this leg runs where
   it runs.
5. **THE TYPE GATE** — the native-`JSON` refusal above reproduces on demand, against a
   table declared exactly as the bulk arm's, so the schema note is a receipt in the logs
   rather than a sentence in a report.

Leg 4's destination deserves its own paragraph, because it is where the rival's fate
was found. ingestr cannot write CDC into ClickHouse at all, so a control that demanded
a ClickHouse landing would be demanding something the tool does not do. Its landing
therefore goes into this campaign's own MySQL container, in its own schema, and it is
read by the campaign's **MySQL-side digest definition** — literally the same aggregate
expression that reads the source. A MATCH in a timed arm is therefore a statement about
the transfer and not about a rule one tool satisfies and the other cannot.

## Results

Every arm = the same 30 tables, the same cap, one container, one job, from a pre-seeded
source and an **empty** destination, arms interleaved within each round.

### Pass A — the source the brief specifies: 30 × 1,000,000 rows

| arm | round | wall in container | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|
| **apitap 0.57.0** (1 process, 1 group, 1 binlog stream) | 1 | **105.9 s** | 119.1 MB | exit 0, not OOM-killed | 30,000,000 | **30/30 MATCH** |
| | 2 | **106.4 s** | 114.5 MB | exit 0, not OOM-killed | 30,000,000 | **30/30 MATCH** |
| | **median** | **106.2 s** | **119.1 MB** | | **30,000,000** | **60/60 MATCH** |
| **ingestr 1.1.61** (upstream defaults) | 1 | 5.5 s → refused | 44.4 MB ‡ | exit 1, not OOM-killed | **0** | 0/30 |
| | 2 | 5.4 s → refused | 42.2 MB ‡ | exit 1, not OOM-killed | **0** | 0/30 |
| **ingestr 1.1.61** (its own small-box settings) | 1 | 5.4 s → refused | 44.3 MB ‡ | exit 1, not OOM-killed | **0** | 0/30 |
| | 2 | 5.7 s → refused | 42.5 MB ‡ | exit 1, not OOM-killed | **0** | 0/30 |

‡ **A host-sampled lower bound, and the honest form of it.** The kernel's
`memory.peak` lives in the container's cgroup, and this box runs the systemd cgroup
driver, so the `docker-<id>.scope` is torn down the moment the container exits — which
for these legs is about a second after it starts. `cgroup_memory_peak_mb` therefore
reads `0.0` and the figure quoted is the maximum of `memory.current` sampled from the
host at 20 Hz. It is a floor, not a measurement of ingestr's footprint under load, and
it is reported only to show the legs were not killed for memory: **`OOMKilled=false`,
`ExitCode=1`** on all eight.

Inside the cage, per round: `budget=8` auto-sized for the whole 30-table job (read off
the cgroup limit, no flag), `tables_reporting=30`, every table at exactly 1,000,000
rows, `FD_PEAK 29` → `FD_END 4`, `IMPORT_S 0.694`, `EXITCODE 0`. The transfer itself
was 104.4 s and 104.9 s; the container's extra ~1.5 s is image start and interpreter
import. At a 0.5-CPU quota, CPU is 0.5 × wall for any leg that saturates the quota, so
wall and CPU are the same measurement here and are reported once.

There is no completion time to report for ingestr, so its wall column is **time to
refusal**, not a duration. The result is the refusal, not the number.

### Pass B — the same legs after the window, 30 × 1,036,000 rows

The window's change stream is applied to these same tables — that is what a change
stream *is* — so each of the thirty ends the campaign holding **1,036,000 rows**
(1,000,000 + 45,000 − 9,000). The cold catch-up therefore runs at **two source sizes**,
both checksum-validated against the source state current for that pass; the cached
per-table sums are keyed on the *validator text*, not on the data, so after the window
they describe the seed, and refreshing them is a fresh scan.

| arm | round | wall in container | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|
| **apitap 0.57.0** | 1 | **110.4 s** | 118.4 MB | exit 0, not OOM-killed | 31,080,000 | **30/30 MATCH** |
| | 2 | **119.7 s** | 124.7 MB | exit 0, not OOM-killed | 31,080,000 | **30/30 MATCH** |
| | **median** | **115.1 s** | **124.7 MB** | | **31,080,000** | **60/60 MATCH** |
| **ingestr 1.1.61** (defaults) | 1, 2 | 5.4 s → refused | 43–44 MB ‡ | exit 1 | **0** | 0/30 |
| **ingestr 1.1.61** (small-box settings) | 1, 2 | 5.2–5.4 s → refused | 42–43 MB ‡ | exit 1 | **0** | 0/30 |

3.6% more rows costs 8.4% more wall and 4.7% more peak, and both rounds still read
30/30. Across the whole campaign that is **four apitap catch-up legs, 120 of 120 table
checksum verdicts, all MATCH**, plus the window's own bootstrap and final drain.

## The five-minute window

One group, established by a bootstrap that is reported but is **not** the headline
(**105.6 s** wall / 104.0 s in-container, **111.1 MB** peak, 30,000,000 rows,
**30/30 MATCH**), then a steady change stream across all thirty tables drained on a
~60 s cadence.

**The change stream, with the exact counts.** 450 ticks, each issuing three statements
against every one of the thirty tables in a single `mysql` session on the source, every
statement its own implicit transaction. Per tick, per table: **100 INSERT** (rows cloned
out of the seed's own first `100·tick` ids and re-inserted at `+2,000,000`, so the two
bands can never collide), **100 UPDATE** (one contiguous band of seed ids, setting
`regular_int = regular_int + 1`), **20 DELETE** (ids from 900,001 upward, disjoint from
the update band). The three bands never intersect, so the counts are exact *by
construction* — and the source's own `COUNT(*)` is what confirms it:

```
WINDOW_DONE ticks 450 per_table_ins 45000 per_table_upd 45000 per_table_del 9000
WRITER_TOTAL ticks=450 tables=30 statements=40500 changes=2970000
                writer_wall_s=1357.8 changes_per_s=2187
30 tables at exactly 1036000 rows   (1000000 + 100·450 − 20·450)
```

The generator is [bench-capped-my-ch-cdc-window.py](bench-capped-my-ch-cdc-window.py)
and it is piped from the host straight into `mysql` — the statement stream is never
written to disk. The `UPDATE` changes a column that is inside the checksum's row text on
purpose: an `UPDATE` that rewrote a row to the values it already held logs nothing at
all and makes the whole checksum a test of nothing.

**The writer's own throughput: 2,970,000 changes in 1,357.8 s = 2,187 changes/s
sustained.** That is the honest number for *generation*, and it is far below the 0.65 s
per tick the generator asks for: 40,500 autocommit statements at two fsyncs each is
**29.8 statements/s**, and the writer is fsync-bound on a box at 94% disk, competing
with the drains. **The writer, not the cage, set this window's pace.**

| drain | changes applied | wall (in-container) | changes/s | cgroup `memory.peak` | FD peak | watermark after | binlog on disk |
|---|---|---|---|---|---|---|---|
| 1 | 397,860 | 133.1 s | 2,989 | 55.2 MB | 13 | 21,716,959,126 | 642 MB |
| 2 | 1,087,140 | 384.5 s | 2,827 | 55.5 MB | 13 | 22,361,761,902 | 1.7 GB |
| 3 | 1,485,000 | 513.5 s | 2,892 | 47.8 MB | 13 | 26,466,429,011 | 1.7 GB |
| 4 | 0 | 5.0 s | — | 48.4 MB | 10 | 26,466,429,011 (unchanged) | 1.7 GB |
| 5 | 0 | 7.4 s | — | 40.6 MB | 10 | 26,466,429,011 (unchanged) | 1.7 GB |
| 5 (second pass, same process) | 0 | 3.2 s | — | | 10 | | |
| final | 0 | 7.2 s | — | 55.3 MB | 10 | 26,466,429,011 (unchanged) | 1.7 GB |
| final (second pass, same process) | 0 | 2.7 s | — | | 10 | | |
| **total** | **2,970,000** | **1,031.1 s busy** | **2,880** | **55.5 MB** | | | |

"busy" is the sum of the in-container walls of the three drains that applied changes,
which is the same convention the PostgreSQL arm used; including the three idle legs
would give 1,050.7 s and 2,827 changes/s.

Reading that table, one number at a time:

- **The applied total is exactly the generated total.** 397,860 + 1,087,140 + 1,485,000 =
  **2,970,000**, and the last three drains report **0** — the group *converged* rather
  than merely accumulating. Nothing was lost and nothing was double-counted, and the
  two second-pass drains reporting 0 are the idempotence property measured rather than
  asserted.
- **Peak memory does not trend up.** 55.2 → 55.5 → 47.8 MB across the three drains that
  did work; the largest number anywhere in the window is the *bootstrap's* 111.1 MB —
  30 million rows in one group costs twice what 3 million changes costs.
- **The watermark advances monotonically, and as ONE value for the whole group.** Every
  reading is `distinct=1` across all thirty tables, and the sequence only goes up
  (21,716,959,126 → 22,361,761,902 → 26,466,429,011), then correctly stops moving once
  there is nothing left to apply. A group whose members drifted apart would show
  `distinct>1` here, which is the failure this lane exists to prevent.
- **Descriptors do not grow.** `FD_PEAK` is 13 while draining and `FD_END` is 4 in every
  container; and in the two legs that ran **two drains in one process**, the count was 4
  before pass 1, 4 after pass 1, 4 before pass 2, 4 after pass 2.
- **The backlog lives in the binlog on disk, never in the worker's memory.** The binlog
  grew 13 MB → 642 MB → **1.7 GB** and then stopped, because the drains caught up. That
  is the shape to expect, and it is the same shape the PostgreSQL arm's retained WAL
  showed.

**The destination matches the source after the final drain** — recomputed from scratch
on both sides, because the cached sums describe the pre-window seed:

```
VERIFY_SUMMARY checked=30 of 30 match=30 mismatch=0
  every table: rows=1036000 digest=1036000|2224523367011423|2224564487451289|1|2045000
```

1,036,000 rows and one identical digest on all thirty, on both engines. That single
line is the whole window: the exact arithmetic of 45,000 inserts, 45,000 updates and
9,000 deletes per table, propagated through one binlog stream into ClickHouse and back
out as a checksum that agrees with MySQL row for row.

### This window was not five minutes, and the table says so

The brief asked for a five-minute window. What ran was **21.4 minutes of drains**
(10:11:44 → 10:33:07 UTC) around **22.6 minutes of writer traffic** (10:10 → ~10:33),
because two measured things overran their budgets at once: each drain's own wall
(133–514 s) exceeded the 60 s sleep, so the drains landed at t+133, +517, +1,030, +1,685
and +1,752 s rather than on a clean minute; and the writer needed 1,357.8 s for the
292.5 s of ticks it was asked for. The number that matters — 2,970,000 changes applied
at 2,880 changes/s with a flat peak and one watermark — is unaffected, but the *duration*
is roughly four times the brief and is reported as what happened rather than as what was
intended.

One cosmetic harness note, recorded because it changed how progress was read: the
harness's own `writer ticks done` counter printed **0** for the first two drains and
**4** thereafter, because the `mysql` client block-buffers its stdout into the log file
and the progress markers are emitted only every 100 ticks. Progress was therefore
tracked from the source's exact `COUNT(*)` — 1,012,640 rows at tick ~158 — which is the
better instrument anyway.

## Leak checks, taken with the window's state still intact

| what | reading |
|---|---|
| descriptors, per drain | `FD_PEAK` 13 while draining, `FD_END` **4** in every container; 4 before/after each of two drains in one process |
| replicas attached to the source | `performance_schema.replication_connection_status` = **0** rows; binlog-dump threads = **0** |
| the source's binlog | 6 files, **1.7 GB**, stable across the last three windows; `Binlog_cache_disk_use` = 27,015 bytes |
| `_apitap_lease` | **1,660 rows**, of which **0 uncollected and still live** |
| staging / marker leftovers | **none** — no `*__apitap*`, `*__ingestr*` or `*__bruin*` table of any kind |
| `_apitap_cdc_pending` | **does not exist** |
| `_bruin_staging` | **0 tables** |
| the group's state rows | **60** rows for 30 tables, `mode=log_based`, `cursor=_lsn`: 30 **group** rows at **1 distinct watermark** (26,466,429,011, `last_rows` 220–440 each) and 30 `server-identity:` rows carrying one sentinel and `last_rows=0` |
| another campaign's rows in the shared `_apitap_state` | `cdc_pg_*` rows: **0**, untouched throughout |

Two of those deserve a sentence rather than a bare figure.

The **1,660 lease rows** are documented behaviour, not a leak: lease rows are written per
run and per table, are never garbage-collected on any engine (age-based reaping is
forbidden because an uncollected row must never age out), and what matters is that
**none of them is uncollected and live** — no table is left locked.

The **60 state rows for 30 tables** look like drift until they are broken down, so the
leak check breaks them down. There are two kinds of row: `mysql:bench.cdc_my_tNN` is the
**group's** row — the live position, `last_rows` the rows that table contributed, and all
thirty at one watermark — and `server-identity:mysql:bench.cdc_my_tNN` is written once at
bootstrap to pin *which* server/schema/table this is, so a later run can tell a resumed
group from a different server that happens to have a table with the same name. Its
watermark field holds a sentinel, not a position, and it is deliberately never advanced.
Counting `uniqExact(watermark)` across both kinds reads **2** on a perfectly healthy
group, which is exactly how a real drift would look; the question that matters — "do the
thirty group rows agree?" — is `distinct=1`.

The staging answer deserves the same honesty the PostgreSQL arm gave it. During a run
there *is* a run-scoped artifact, and ClickHouse's own dropped-table metadata names it:
`default.cdc_ctrl_0tmdqdsrjxktyx2__apitap_staging` and
`default.cdc_ctrl_0tmdqdxljxky1jd__apitap_cdc_del` — a per-run staging table carrying
the run token. It is dropped at the end of the run and swept by token, and the
destination-leftover check after every leg reports **0**. That is a measurement that the
artifact exists *and* a measurement that it does not outlive its run, which is stronger
than either alone.

## The rival: refused the destination, not the memory

ingestr's MySQL CDC path **exists, is documented, and works on this rig.** What it does
not do is write into ClickHouse. From a capped leg's own log, all thirty tables in one
invocation:

```
INGESTR_VERSION ingestr version v1.1.61
INGESTR_SOURCE_URI mysql+cdc://***@127.0.0.1:3313/bench?server_id=18888&dest_schema=default
INGESTR_TABLES 30
INGESTR_DEST_NAMING --cdc-table-naming table
INGESTR_DEFAULTS extract_parallelism=5 page_size=25000 loader_file_size=25000 batch_size=512 sql_limit=0
Error: destination scheme "clickhouse" cannot safely run managed CDC: destination-managed
state with fencing, pruning, and truncation is not supported
INGESTR_WALL_S 1.1
EXITCODE 1
```

Because "ingestr cannot run this" is only an honest sentence if the failure is
attributed correctly, the same source URI was run against four destinations, uncapped,
one at a time, by [bench-capped-my-ch-cdc-probe-ingestr.sh](bench-capped-my-ch-cdc-probe-ingestr.sh):

| destination | outcome |
|---|---|
| **clickhouse** | **refused** — `cannot safely run managed CDC: destination-managed state with fencing, pruning, and truncation is not supported` (exit 1) |
| **sqlite** | **refused** — `cannot safely run MySQL CDC: atomic target-incarnation fencing for merge is not supported` (exit 1) |
| **duckdb** | reached DDL, then failed on ingestr's own generated SQL: `Parser Error: syntax error at or near "4294967295"` — it emits `VARCHAR(4294967295)` for a `longtext` column (exit 1) |
| **mysql** | **succeeded** — 1,040 rows, 0.7 s, exit 0, and the landing's digest equals the source's exactly |

Three things follow, and they are the whole rival section:

1. **The refusal is about the destination, and only the destination.** It reproduces
   identically against a **3-column, 2-row table** with no `longtext` and no JSON, so it
   cannot be an artefact of this campaign's schema. It is a capability boundary in
   ingestr 1.1.61's managed-CDC path: ClickHouse is not among the destinations it will
   run CDC into.
2. **The source path is healthy.** Against a MySQL destination the same URI landed
   1,040 rows and matched the checksum — which is why control leg 4 exists, and why a
   MATCH in any timed arm is a statement about the transfer rather than about a rule one
   tool satisfies and the other cannot.
3. **So there is no catch-up number, and none is invented.** This arm reports a refusal,
   `OOMKilled=false`, `ExitCode=1`, 0 rows, eight legs of eight — under ingestr's own
   defaults *and* under a second configuration with smaller settings of its own
   (`--sql-limit 200000`, `--extract-parallelism 1`, `--batch-size 64`,
   `--loader-file-size 5000`), so the refusal is not a tuning artefact either. It is not
   a statement about memory at all, and the 42–44 MB host-sampled figures are reported
   only to make that explicit.

Two configuration details are disclosed because they are choices, not defaults:

- **`dest_schema` is a source-URI parameter, not a CLI flag.** ingestr's own cdc guide
  once documented a `--dest-schema` flag that does not exist; only the URI parameter
  does, and it is what decides where a multi-table CDC run lands. A comma-separated
  `--source-table` *is* a multi-table run, so it applies.
- **`--cdc-table-naming table`** rather than the default `schema_table`, which would
  flatten the source schema into the destination name (`bench.cdc_my_t01` →
  `default.bench_cdc_my_t01`). Both spellings are documented; `table` makes the rival's
  landing carry the same thirty names apitap's does, so one validator computes both
  digests with no special-casing. Naming is cosmetic — it moves no row and costs no byte
  of the cage.

Every other documented knob was left at its default (`--extract-parallelism 5`,
`--page-size 25000`, `--loader-file-size 25000`, `--batch-size 512` MiB, `--sql-limit 0`,
no `--stream`), because ingestr's own words are that "by default each invocation catches
up and exits" — the same shape apitap's one-call catch-up is measured against.

## Harness faults this campaign hit, and what they were

Five of them, recorded because the alternative is a report whose numbers came from a
broken instrument. None changed a published figure; every number below comes from a leg
run after its fix.

- **A passing verification also printed `VERIFY_INCOMPLETE … THIS IS NOT A PASS`, and
  returned 1.** The guard asked `[[ "$t" != "${TABLES[-1]}" ]]`, and bash's `read`
  assigns empty strings to its variables at EOF with no input — so the test was true on
  every run. It fired directly under a clean `30/30` summary, and `window` gates its
  bootstrap on that return value, so it would have aborted the window on success. Fixed
  to count the lines the loop actually saw, and **controlled in both directions**: a
  landed table prints a clean summary and returns 0; a dropped one prints
  `VERIFY_INCOMPLETE 1 of 1` and `VERIFY_ABSENT`.
- **`RECREATE=1 rig` printed "keeping it" and changed nothing.** The flag was read only
  inside the branch that was never taken. It surfaced when a `gtid_mode=ON` rebuild
  appeared to succeed and the server still answered `OFF`.
- **A `countIf(...)` with no `FROM`** is a query the server rejects; with stderr
  suppressed it printed as an empty line under a heading about what the campaign must not
  touch — an absent check that reads as a passed one.
- **The validator's per-column `cols` mode had an unclosed `CONCAT_WS(`.** Every table
  returned `ERROR 1064 … near 'FROM …'`, and since the caller pipes stderr away the
  branch silently produced **nothing**. A debug aid that returns nothing is worse than
  one that errors loudly: a `grep` against empty output prints no line and reads as
  "compared, found no difference". Found by paren-counting the definition after two
  wrong guesses about the server's error message; fixed, and the field it exists to
  report (`json_crc`) now prints.
- **The seed's own verification reported `rows each 907529-999363`.** That is
  `information_schema.tables.table_rows`, InnoDB's *estimate*, printed as if it were the
  row count — and `cmd_state` went further and labelled the same estimate "exact rows".
  Every count in this campaign is now a real `COUNT(*)`, and the estimate is printed
  beside it as the thing it is.

Two more were the rivals' own doing and are described above: `ingestr`'s missing
`dest_schema` (which, appended with `?` onto a URI that already had a query, produced
`?server_id=18888?dest_schema=default` and surfaced as the unrelated *"server_id must be
a positive uint32"*), and the fact that the ingestr leg runs under `sh`, where a bash
`[[ … ]]` printed `[[: not found` and **silently skipped** the append that would have
given the run a destination namespace at all.

## The exact commands

The cap, identical for every arm:

```
--network=host --cpus=0.5 --memory=256m --memory-swap=256m
```

**apitap** — the group, one process, one transfer call, one binlog stream, no knobs
(`parallel=`, `chunk_bytes`, `slots=` and every env lever left alone; the wheel is
mounted read-only and put on `PYTHONPATH`, so the PyPI wheel is what runs and nothing is
built):

```bash
docker run --name apitap-bench-cdc-r1 --network=host $CAP \
    -v /home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages:/py:ro \
    -e PYTHONPATH=/py -v ~/apitap-lib/benchmarks:/job:ro \
    -e "APITAP_TABLES=$(printf 'cdc_my_t%02d ' {1..30})" \
    python:3.13-slim sh /job/bench-capped-my-ch-cdc-leg-apitap.sh
```

```python
# inside the cage — the same call is the cold catch-up (empty destination) and every
# drain afterwards; there is no separate "catch-up" API to be fair about.
apitap.transfer('mysql://root:bench@127.0.0.1:3313/bench',
                'clickhouse://default:bench@127.0.0.1:8128/default',
                tables=['cdc_my_t01', … 'cdc_my_t30'], mode='log_based')
```

The leg prints the wheel's version and the `_apitap.abi3.so` md5 from **inside** the
cage, and samples `/proc/self/fd` at 20 Hz throughout, which is where the descriptor
numbers come from.

**ingestr** — its own documented model, one process for all thirty tables because a
comma-separated `--source-table` *is* its multi-table CDC mode:

```bash
docker run --name apitap-bench-cdc-ing-r1 --network=host $CAP \
    -e ING_SOURCE_URI='mysql+cdc://root:bench@127.0.0.1:3313/bench?server_id=18888' \
    -e ING_DEST_URI='clickhouse://default:bench@127.0.0.1:9128?http_port=8128' \
    -e ING_SOURCE_TABLE=cdc_my_t01,cdc_my_t02,…,cdc_my_t30 \
    -e ING_DEST_SCHEMA=default \
    -v /home/ubuntu/.cache/ingestr/bin/v1.1.61/Linux_x86_64/ingestr:/usr/local/bin/ingestr:ro \
    -v ~/apitap-lib/benchmarks:/job:ro \
    python:3.13-slim sh /job/bench-capped-my-ch-cdc-leg-ingestr.sh
```

The container is the tool's own deployment unit: ingestr 1.1.x is no longer a Python
program, and the 254 MB native binary is bind-mounted read-only so nothing is
downloaded inside a measurement.

## Interleaving, host state, and rig hygiene

Both passes ran their arms interleaved **apitap · ingestr · ingestr-tuned**, n=2,
because this box carries thirty other containers and a block of one tool would confound
host drift with engine identity. `loadavg` (1-min) across every leg of the campaign
ranged **3.38 – 14.18** and `MemAvailable` sat between **40.8 GiB and 42.1 GiB**
throughout; every before/after pair is in the raw log.

Dropped after every verification: all 30 destination tables, `DROP TABLE … SYNC`
(ClickHouse keeps a dropped table's metadata for 480 s so `UNDROP` can find it, and its
own description of `database_atomic_delay_before_drop_table_sec` says the delay is
ignored for a `SYNC` drop). The final state check reports **0** `cdc_my_*` tables in the
destination. The seed was never dropped.

**The killed-drain self-heal leg was deliberately skipped, not forgotten.** It needs a
lease TTL shorter than the window, and the default is **300 s** with the keeper renewing
every 30 s. Lowering a product default to make a test fit would have measured the
override rather than the product. It belongs in a longer campaign.

## What this does and does not prove

**It does prove** that at 0.5 CPU / 256 MB, on MySQL 8.0 → ClickHouse 24.8, with these
thirty tables, apitap 0.57.0 establishes and catches up a **single** 30-table CDC group
over **one** binlog stream in a median of **106.2 s at a 119.1 MB peak** with every table
checksum-verified (60 of 60 across two rounds), and **115.1 s / 124.7 MB** on the
post-window source (60 of 60 again); that it then applies **2,970,000 changes at 2,880
changes/s with a 55.5 MB peak that does not trend up**, keeps the group's watermark
moving monotonically as one value, leaks no descriptors, no lease, no staging table and
no source connection, and ends the window with all thirty tables matching the source
exactly at 1,036,000 rows each. And it proves that **ingestr 1.1.61 will not run managed
CDC into ClickHouse at all** — 8 of 8 legs, under its own defaults and under smaller
settings of its own, refused before reading a row, while the same source URI landed
1,040 checksum-exact rows into a MySQL destination.

**It does not prove** any of the following, and none of it should be read into the tables
above:

- **The cap is per tool, and the destination is uncapped.** Only the loader is in a cage.
  The source and the ClickHouse server both run at full size, so these numbers are "what
  each tool can do while the databases are free to help".
- **This is not a speed comparison, because the rival never ran.** No ratio against
  ingestr is available, and none is estimated. What the rival's arm establishes is a
  capability boundary, and the four-destination probe is what keeps that boundary from
  being mistaken for a performance verdict.
- **The two tools are not doing the same job even in principle.** apitap's group call is
  a bootstrap plus a drain, done when it returns, from ONE coordinate over ONE stream.
  ingestr's multi-table CDC, by its own documentation, "snapshots each selected table
  independently and then stream each table from its own snapshot position. Each table is
  consistent on its own, but a multi-table run is not a single global point-in-time
  snapshot across all tables." On a static pre-seeded source the two agree; on a mutating
  one they need not. That is a difference in model, not in speed, and it is the rival's
  documented design rather than a configuration.
- **One group over one stream is not thirty streams.** Thirty tables in one group share
  one binlog coordinate and one decoder. Thirty independent CDC jobs would each hold
  their own position, their own buffer and their own lease, and this campaign says
  nothing about that shape.
- **Neither catch-up number is comparable to the bulk arms' 18.4 s / 36.1 s, or to the
  PostgreSQL CDC arm's 63.8 s.** Three things differ at once: 30 tables instead of 10,
  the CDC lane instead of a bulk replace (which must additionally establish a coordinate
  and read a consistent snapshot), and MySQL instead of PostgreSQL. The campaigns'
  apitap figures should be compared only within their own arm.
- **The schema is the bulk arm's with one declared type changed.** `json_val` is
  `longtext`, not a native `json`, because apitap's MySQL CDC lane refuses the latter.
  The values are proven byte-identical and the digest covers that column on both engines,
  but a reader comparing this arm to the bulk arm is comparing a `longtext` column to a
  `json` one, and the type gate is a real limitation of the product at v0.57.0.
- **The headline is n=2, and that was the owner's explicit ask** ("the owner wants this
  quick"). Two rounds is thinner than the three the bulk arms used, which is why the
  campaign was then run a *second* time on a larger source rather than a third round on
  the same one: the replication across both passes (106.2 s median at 30.0 M rows,
  115.1 s at 31.08 M, 30/30 both times, four legs and 120 verdicts in total) is the
  evidence offered in place of a third sample.
- **The window was not a soak, and it was not five minutes.** 21 minutes of drains
  around 23 minutes of writer traffic, at a light steady rate, with a peak that did not
  move across three working drains and three idle ones. It supports "keeps up with a
  light steady stream and converges, with no growth in memory, descriptors, leases or
  staging over the window". It supports nothing about 24 hours, about a growing backlog,
  or about a transaction larger than the window's own.
- **The writer was the bottleneck, not the cage.** 2,187 changes/s generated is well
  under what this tier applied (2,880/s), so these numbers measure *keeping up*, not
  maximum throughput. The PostgreSQL arm at this same tier measured 6,143 changes/s on a
  30-table group and a historical 10-table Postgres rig measured ~37K/s; that spread is
  **not** explained by anything measured here — the rigs differ in seed size, in
  binlog-versus-WAL mechanics and in apitap version, and this campaign did not isolate
  the cause. It is reported as a number to ask about, not as a finding.
- **The drain cadence slipped** (each drain's own wall was 133–514 s against a 60 s
  sleep), so "five minutes, drained every 60 s" describes the intent; the table gives
  what happened.
- **The killed-drain self-heal test was skipped, not forgotten** — see above.
- **One rig, one schema, one day.** 30 M rows of a specific 15-column shape (~450 B/row),
  a source with `sync_binlog=1` and a 2 GiB buffer pool, a single-node ClickHouse with no
  Keeper, on a shared box that was at 89% disk before the seed and 94% after it. A wider
  table, a different page-cache behaviour, or an fsync-free source would move the
  seconds.
- **The seed was reused, the destination never was** — and the seed was *written to* by
  the window, which is why the catch-up appears twice at two source sizes rather than
  three times at one.

## Reproducing

```bash
# on the bench VPS, from the repo
rsync -a -e "ssh -i ~/.ssh/apitap_vps" --exclude .git --exclude target \
      benchmarks/ ubuntu@<vps>:~/apitap-lib/benchmarks/
cd ~/apitap-lib/benchmarks

H=bench-capped-my-ch-cdc-0.57.sh
RECREATE=1 bash $H rig        # apitap-bench-cdc-my: gtid_mode=ON for the RIVAL's sake
bash $H seed                  # 30 tables, ~501 s, kept between every leg
bash $H srcsum
bash $H drop
bash bench-capped-my-ch-cdc-control.sh   # GREEN x5 before anything is timed
ROUNDS=2 bash $H catchup apitap ingestr ingestrtuned          # pass A
bash $H window                # bootstrap + verify + the window + a fresh final verify
bash $H srcsumx && bash $H drop                              # the window moved the source
ROUNDS=2 RESULTS=$HOME/bench-cdc-my/results-catchup-final.txt \
      bash $H catchup apitap ingestr ingestrtuned             # pass B
bash $H leaks                 # with the window's state still intact
bash bench-capped-my-ch-cdc-probe-ingestr.sh                 # where the rival can land
bash $H state                 # containers? disk? seeds? binlog? leftovers?
bash bench-capped-my-ch-cdc-raw.sh bench-capped-my-ch-cdc-0.57-raw.log
```

`LEG_TIMEOUT` (default 3600 s) is the rival leg's deadline; hitting it is recorded as a
result. No leg in this campaign needed it — every ingestr arm refused itself first.