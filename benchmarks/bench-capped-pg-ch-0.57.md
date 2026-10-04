# Capped tier, PostgreSQL → ClickHouse: apitap 0.57.0 vs walshadow 0.1.2

The second half of the capped comparison, on the same tier as the MySQL arm:
**0.5 CPU and 256 MB of RAM per job**, ten tables of a million rows each, all ten
synced in one job, each tool inside one container. apitap is the PyPI wheel
**0.57.0**; walshadow is **v0.1.2** of `ClickHouse/walshadow`, installed from its
own sha256-verified release artifacts. Every landed table was checksum-validated
against PostgreSQL before any number counted.

The short version: **apitap moved all 10,000,000 rows in a median of 18.4 s at a
119 MB peak, 30 of 30 tables checksum-MATCH. walshadow was OOM-killed in all six
of its legs, in 5–10 s, with at most 93,184 of 10,000,000 rows landed.** Its own
process needs ~550 MB before it has finished taking the base backup, so the
verdict is not a tuning accident: at 512 MB it is still OOM-killed, and at 1 GB it
lands all ten tables correctly in ~125 s.

Rig: OVH bench VPS (16 vCPU / 61 GB, shared with production), source
`apitap-bench-ws-pg` (`postgres:18.6` on 127.0.0.1:5547, `wal_level=logical`, db
`bench`), destination `apitap-bench-ch` (`clickhouse-server:24.8.14` on
127.0.0.1:8124 HTTP / 9124 native). Raw receipts:
[bench-capped-pg-ch-0.57-raw.log](bench-capped-pg-ch-0.57-raw.log).

## The seed — 10 identical tables, kept between every run

`bench.public.cmp_pg_t01` … `cmp_pg_t10`, generated from `id` alone and then
`CREATE TABLE … LIKE`-cloned, so all ten are byte-identical and any difference can
only come from the transfer:

```
id integer · small_str varchar(20) · medium_str varchar(100) · large_str varchar(500)
tiny_int smallint · regular_int integer · big_int bigint · float_val double precision
decimal_val numeric(18,4) · bool_val boolean · date_val date
ts_val timestamp(6) · ts_tz_val timestamptz · json_val json · extra_text text
PRIMARY KEY (id)
```

The same 15 columns, in the same order, as the MySQL arm's seed — so the two
campaigns describe one dataset rather than two. Verified after seeding: **10
tables × 15 columns × exactly 1,000,000 rows**, 258 MB per table, 2.5 GB total.
Seeding took **63.7 s for all ten** and is excluded from every measurement. The
check that the clones really are clones is the source aggregate: all ten print the
identical digest, `1000000|2147445026505068|1|1000000|…`. The seeds were
re-checked intact at the end of the campaign and were never dropped; every
destination table was dropped after each verification.

Three properties of the generator are deliberate and are disclosures, not
conveniences ([schema.sql](bench-capped-pg-ch-schema.sql) says so at the top):

- **No TOAST.** Every text value stays under 200 bytes, so nothing is stored out
  of line and walshadow's TOAST-chunk mirror tables — its default `clickhouse`
  value mode, which keeps chunk history in ClickHouse — are never exercised.
- **Floats are exact.** Values are multiples of 1/8 below 1250, so PostgreSQL's
  shortest-round-trip text and ClickHouse's `toString` agree byte for byte and the
  checksum measures the transfer rather than float formatting.
- **`json`, not `jsonb`.** `json` preserves the input text verbatim; `jsonb`
  re-renders it (key order, whitespace), which would have made the checksum a test
  of JSON normalisation.

Four columns also carry NULLs on a prime modulus of ids (~1% each), so the
checksum proves a NULL survives the trip and renders the same on both sides.

## What walshadow is, and what it takes to run it

Researched from the repository before anything was installed, and verified on the
box ([docs](https://github.com/ClickHouse/walshadow/tree/main/docs)).

**It is not a loader.** walshadow is a continuous CDC daemon that reads
**physical** WAL — not logical decoding. `pgoutput`, `test_decoding` and
`wal2json` are all irrelevant to it. Its model is: take a physical base backup of
the source into a **shadow PostgreSQL it owns and supervises**, stream the
filtered WAL into that shadow so the shadow's catalogs follow DDL, decode the
original records against versioned descriptors, and emit committed rows into
ClickHouse as CDC. A first landing is therefore a *catch-up*: seed the source,
start the daemon against an empty destination, and measure how long until the rows
are readable. That is how it is measured here, and it is stated plainly rather
than dressed up as a bulk load.

Its real prerequisites, all of which the rig had to satisfy:

| requirement | source | on this rig |
|---|---|---|
| PostgreSQL **16+**, source and shadow the same major | docs/getting-started.md, limitations.md | `postgres:18.6`, pgext built for pg18 |
| `wal_level = logical` | docs/getting-started.md | set on the source |
| ≥ 2 WAL senders (streaming + base backup) | docs/getting-started.md | `max_wal_senders = 10` |
| a role with `REPLICATION` + read access | docs/getting-started.md | `postgres` (superuser) |
| a `host replication` pg_hba entry for the connecting peer | PG's own error | **had to be added** — see below |
| usable replica identity per table (PK, unique index, or `REPLICA IDENTITY FULL`) | docs/table-selection.md | `PRIMARY KEY (id)` |
| a **physical** replication slot, or archive coverage | docs/operations.md | `pg_create_physical_replication_slot('walshadow')` |
| ClickHouse **Native** endpoint (not HTTP) | docs/limitations.md | `clickhouse://…:9124/default` |
| `EXCHANGE TABLES` support (backup-mode loads only) | docs/limitations.md | not used (`initial_load = "copy"`) |

Two answers to questions the campaign was briefed to expect, both different from
the brief:

- **No ClickHouse Keeper, and no ReplicatedMergeTree.** walshadow creates plain
  `ReplacingMergeTree(_lsn, _is_deleted) ORDER BY <source row key>` and converges
  duplicate versions by `_lsn` during merges or `FINAL`. Verified on the box:
  `ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id`. Its dedup needs no
  ZooKeeper, which is why this campaign needed no Keeper container.
- **The pg_hba trap.** The official `postgres:18` image trusts loopback for
  replication but not the bridge gateway, and with `--network=host` the connection
  arrives from `172.17.0.1`. The daemon's answer is not an error, it is an
  infinite retry loop: `source unreachable — waiting for it`, once every two
  seconds, forever. Every millisecond of that is spent waiting on auth, not on
  data.

What state it keeps: the shadow data directory, the filtered WAL (`--out-dir`),
the transaction spill, and a `manifest.toml` resume cursor. Later starts resume
from it, which is why **every leg here starts from an empty state** — reusing it
would have measured a resume instead of a first landing.

## Results

Every arm = the same 10 tables, the same cap, one container, one job, from a
pre-seeded source and an empty destination.

| arm | round | wall in container | transfer only | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|---|
| **apitap 0.57.0** (1 process, `tables=[…]`) | 1 | **19.8 s** | 18.2 s | 119.2 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | 2 | **18.4 s** | 16.7 s | 115.9 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | 3 | **17.0 s** | 15.3 s | 114.3 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | **median** | **18.4 s** | 16.7 s | **119.2 MB** | | **10,000,000** | **30/30 MATCH** |
| **walshadow 0.1.2** (upstream defaults) | 1 | 10.1 s → killed | — | ≥255.2 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |
| | 2 | 7.8 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |
| | 3 | 7.6 s → killed | — | ≥255.9 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |
| **walshadow 0.1.2** (its own small-box settings) | 1 | 5.2 s → killed | — | ≥255.4 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |
| | 2 | 5.3 s → killed | — | ≥256.0 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |
| | 3 | 5.2 s → killed | — | ≥255.8 MB † | **OOMKilled=true, exit 137** | 0 ‡ | 0/10 |

† **The peak is a lower bound, and that is the honest form of it.** The kernel's
`memory.peak` lives in the container's cgroup, and a container the kernel
OOM-killed takes that cgroup with it — verified on this host, where
`memory.peak` does not exist once a container has stopped. So for a leg that dies,
the peak is the maximum of `memory.current` sampled from the host at 20 Hz, and it
reads 255–256 MB against a 256 MB cap. What makes the verdict unambiguous is not
the peak but `State.OOMKilled=true` with `ExitCode=137` on **six of six** legs,
which is the kernel saying so outright.

‡ "0" is the honest summary of "no table completed": in each of the six legs nine
of the ten tables held 0 rows, and the tenth (`cmp_pg_t06`, the table its bootstrap
lane happened to be working on) held between 33,190 and 93,184 rows at the kill.
The best of the six legs landed **0.93%** of the job.

There is no completion time to report for walshadow, so its wall column is
**time-to-kill**, not a duration. A median of survival time is a weak statistic and
is printed only so nobody has to reconstruct it; the result is the outcome.

The tuned arm is not a strawman either way: it is what walshadow's own
`configuration.md` prescribes for a memory-capped box, and it is what a reader
would try first.

| walshadow setting | upstream default | this arm |
|---|---|---|
| `xact-buffer-max` (entrypoint-injected) | **1073741824** (1 GiB) | 33554432 (32 MiB) |
| `ch.byte_budget` | 268435456 (256 MiB) | 33554432 (32 MiB) |
| `ch.inserter_pool_size` | 8 | 2 |
| decoder pool | 3 | 2 |
| `memory.resident_payload_max` | **512 MiB floor** ‡‡ | 67108864 (64 MiB) |
| `memory.value_reserve` | 64 MiB | 8388608 (8 MiB) |

‡‡ Its documented default is "one half of cgroup memory limit, **with a 512 MiB
minimum**". A 256 MB cage cannot satisfy a 512 MiB floor, so out of the box
walshadow is told it may hold roughly twice the memory that exists. Both arms were
run because "walshadow failed" is only an honest sentence if it also failed with
its own best settings — and it did, 3 of 3, in two thirds the time.

## The exact commands

The cap, identical for every arm:

```
--network=host --cpus=0.5 --memory=256m --memory-swap=256m
```

**apitap** — the multi-table path, one process, one shared pipe budget, no knobs
(`parallel=`, `chunk_bytes` and every env lever left alone; the wheel is mounted
read-only and put on `PYTHONPATH` exactly like the repo's `e2e_parquet_capped.py`
does, so the PyPI wheel is what runs and nothing is built):

```bash
docker run --name cmp-apitap-r1 --network=host $CAP \
    -v /home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages:/py:ro \
    -e PYTHONPATH=/py -v ~/apitap-lib/benchmarks:/job:ro \
    python:3.13-slim sh /job/bench-capped-pg-ch-leg-apitap.sh
```

```python
apitap.transfer('postgres://postgres:bench@127.0.0.1:5547/bench',
                'clickhouse://default:bench@127.0.0.1:8124/default',
                tables=['cmp_pg_t01', … 'cmp_pg_t10'])
```

It auto-sized to a **shared pipe budget of 8** for the whole job in every round
(read off the cgroup limit, no flag), and the leg prints the wheel's version and
the `_apitap.abi3.so` md5 from inside the cage.

**walshadow** — its own documented model, which is one daemon for the whole set of
tables, configured rather than flag-driven:

```toml
[source]                     # discrete keys only: `url` is a CLI/env form, and an
host = "127.0.0.1"           # unknown key in a non-[table] section is IGNORED,
port = 5547                  # which would look like "connected, replicated nothing"
user = "postgres"
password = "bench"
dbname = "bench"
sslmode = "disable"
slot  = "walshadow"          # physical slot, created before the leg

[ch]
host = "127.0.0.1"
port = 9124                   # NATIVE, required
database = "default"
user = "default"
password = "bench"

[stream]
replicate_all = false        # only the tables named below, not every future table

[table.public.cmp_pg_t01]    # … and cmp_pg_t02 … cmp_pg_t10
replicate = true
initial_load = "copy"
```

```bash
docker run --name cmp-wsdefault-r1 --network=host $CAP \
    -e WALSHADOW_PG_URL='postgres://postgres:bench@127.0.0.1:5547/bench' \
    -e WALSHADOW_CH_URL='clickhouse://default:bench@127.0.0.1:9124/default' \
    -e WALSHADOW_SHADOW_PORT=5442 -e WALSHADOW_WALSENDER_BIND=127.0.0.1:5433 \
    -v ~/bench-capped-pg-ws:/var/lib/walshadow -v ~/bench-capped-pg-ws-conf:/etc/walshadow \
    apitap-bench-ws:0.1.2
```

The container is walshadow's own deployment unit — **the daemon plus the shadow
PostgreSQL it owns**, which is how upstream ships it (their compose file runs one
service and says so). The image is `postgres:18` with two files dropped in, and
nothing is compiled on this box:

```
walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz          # 82 MB, the daemon
walshadow-pgext-0.1.2-pg18-x86_64-unknown-linux-gnu.tar.gz  # the PG module
```

both sha256-verified against the release's published `SHA256SUMS`, and
`walshadow-stream --version` inside the image prints `walshadow-stream 0.1.2
(73329e4)` — the release commit.

**Provenance, verified before the first leg and again in the raw log:**
`apitap.__version__ == "0.57.0"` and `apitap/_apitap.abi3.so` md5
`41e9f7c252d3e1eb5403b70f87bf5435`.

One thing an operator has to know that is not in walshadow's docs: **the shadow
PostgreSQL's memory settings are inherited from the source's `postgresql.conf`**,
because `BASE_BACKUP` copies the data directory including that file. So
`shared_buffers` on the source is also `shared_buffers` in the shadow. This rig's
source runs `shared_buffers = 32MB`, `max_connections = 60` — chosen small
deliberately, since the shadow runs inside the cage.

## What each tool costs before it moves a row

Same container, same cap, no work at all — the floor each tool pays to load:

| probe | wall | cgroup `memory.peak` |
|---|---|---|
| `import apitap` | 0.70 s | **15.7 MB** |
| `walshadow-stream --version` | 0.01 s | **9.4 MB** |

walshadow's floor is *smaller* than apitap's here, and the number should be read
for exactly what it is: starting the binary and printing a version. It is not the
floor of walshadow's deployment, which also owns a PostgreSQL. The section below
measures the whole container instead.

## How far walshadow gets as the cage grows

A capped result that is only "it died" would be the easy thing to publish. So the
same leg was re-run with **upstream's default configuration** at a bigger cage —
the honest best config at a bigger budget is the one walshadow ships, its 256 MiB
`byte_budget` and 8 inserters included — each from a pre-seeded source and an empty
destination, and each verified:

| memory cap | outcome | wall | cgroup `memory.peak` | checksum |
|---|---|---|---|---|
| 256 MB (both campaign arms) | **OOMKilled**, exit 137 | 7.6–10.1 s | ≥256 MB † | 0/10 |
| 512 MB | **OOMKilled**, exit 137 | 10.2 s | ≥512 MB † | 0/10 |
| **1 GB** | **caught up** | **125.3 s** | **770.7 MB** | **10/10 MATCH** |
| 2 GB | caught up | 131.6 s | 767.3 MB | 10/10 MATCH |
| uncapped (0.5 CPU only) | caught up | 128.1 s | 799.7 MB | 10/10 MATCH |

So the smallest cage walshadow completes this job in is **between 512 MB and 1 GB**,
its own peak is **662–800 MB** across five runs, and at 1 GB it needs 771 MB
to do a job apitap does in 119 MB. Four further runs — the attribution run, plus
the first pass of the ladder and of the attribution script, before both were made
append-only — landed at 131.5 s / 789.1 MB, 130.1 s / 772.7 MB, 140.7 s /
683.1 MB and 131.6 s / 661.7 MB, so the verdict reproduces at every rung while the
seconds and the peak both move by 10–20%.

One detail worth stating because it looks like a failure and is not: at catch-up,
`system.tables.total_rows` for the ten tables read **11.3–11.6 M** for 10 M logical
rows. That is ReplacingMergeTree holding superseded row versions, not extra data —
the `FINAL`-based checksum, which is what every verdict above uses, reads exactly
1,000,000 rows per table.

## Where walshadow's memory actually goes

This is the question the capped legs cannot answer, so it was measured directly
([bench-capped-pg-ch-attrib.sh](bench-capped-pg-ch-attrib.sh)): the same leg with
the memory limit removed, sampling `ps` inside the container next to the cgroup's
own accounting.

```
=== ~12s after container start ===
    PID   RSS    VSZ COMMAND
      1 562956 1577732 walshadow-strea      <- 550 MB, and it is alone
      30   3956    9348 ps
cgroup memory.current: 688484352             <- 657 MB total
anon 561020928   file 671744
cgroup memory.peak:   741126144             <- 707 MB

=== ~36s after container start ===
      1 587556 1694488 walshadow-strea      <- 574 MB
cgroup memory.current: 769118208             <- 734 MB
cgroup memory.peak:   827379712             <- 789 MB
```

**There is no PostgreSQL process in the container at any of these samples.** At 12 s
and 36 s walshadow is still receiving the base backup, and its *single process* is
already at 550–574 MB resident — 2.2× the entire cage. The shadow database has not
started yet.

That is the whole failure in one measurement, and it is why the OOM is not
attributed to a tuning knob: no setting of a database that has not launched yet can
bring 550 MB down to 256 MB. The defaults point the same way —
`resident_payload_max`'s documented floor is 512 MiB and `byte_budget` defaults to
256 MiB — which is a configuration that assumes a bigger box than this one.

## Validation

A number without a verified checksum is not a result, so the validator is
[bench-capped-pg-ch-validator.sh](bench-capped-pg-ch-validator.sh) — one definition
per engine, used for the source, apitap's landing and walshadow's landing. Per
table it is order-independent and cheap: row count, an order-independent sum of
the first four bytes of each row's md5 over all 15 columns, min/max of the primary
key, and the length/NULL totals that localise a mismatch. No `string_agg`, and no
accumulating per-row string: both are what breaks past ~100M rows. Source sums are
cached per table and keyed on a hash of the validator text, so the 10M-row scan was
paid once for the campaign.

`FINAL` is applied per engine, not per assumption: walshadow's ReplacingMergeTree
needs it (and *rejects* it on a plain MergeTree — "Storage MergeTree doesn't
support FINAL"), so the validator reads `system.tables.engine` and decides.

A validator that has only ever agreed is untested, so
[bench-capped-pg-ch-control.sh](bench-capped-pg-ch-control.sh) proves it both ways
on a 1000-row control table before any timed leg, and was re-run at the end of the
campaign. It was **GREEN ×3**:

1. **agreement** — apitap's landing digest equals the source digest, exactly:
   `1000|2151639950269|1|1000|31680|32000|37544|10|12|12|11`;
2. **sensitivity** — changing one value in one row (`extra_text` of `id=777`)
   moves the digest (`2151639950269` → `2154172601381`), and restoring it brings
   the digest back, so a MATCH means something;
3. **the rival validates under the same rules** — walshadow's own landing passes
   the same validator.

Step 3 runs uncapped, on purpose: that leg exists to prove walshadow's landing can
be *read* by the validator, and a control that failed on a memory ceiling would
report "the validator is untested" when what it means is "the rival does not fit".

Getting there took four traps that would each have produced confident nonsense,
and all four were found by diffing per column or bisecting rather than by staring
at a mismatch:

- **ClickHouse's `reinterpretAsUInt32` is little-endian.** Taking the first 8 hex
  characters of an md5 and reinterpreting them gives a different integer than
  PostgreSQL's `bit(32)` of the same prefix — `3558706393` vs `3649838548` on the
  empty string. The ClickHouse side rebuilds the prefix in reversed byte order,
  checked equal on three inputs and then on a 1M-row sum, where both engines
  returned `2148800382821315` for the same million rows.
- **`toString(Decimal(18,4))` prints `1234.56` where PostgreSQL prints
  `1234.5600`.** A decimal therefore enters the digest as a scaled integer
  (`value × 10000`) on both sides. The first version of that aggregate also routed
  the decimal through a float, and the per-column sums disagreed by 54 over 1000
  rows — which is what caught it.
- **`ifNull` does not rescue `CAST(x AS String)` in ClickHouse.** The cast is
  evaluated first and a NULL column raises "Cannot convert NULL value to
  non-Nullable type" before `ifNull` is reached. The seed has NULLs in `json_val`
  on ~1% of rows, so this fired on every query. The column renders as
  `ifNull(toString(json_val), '<NUL>')`, which stays Nullable all the way.
- **ClickHouse renders Bool as `true`/`false` and PostgreSQL as `t`/`f`,** so the
  flag is normalised to `1`/`0` on both sides.

Also recorded, because a schema difference is a result and not a nuisance: apitap
lands the 15 columns as `Int32 / Nullable(String) / Nullable(Int16) /
Nullable(Int32) / Nullable(Int64) / Nullable(Float64) / Nullable(Decimal(18,4)) /
Nullable(UInt8) / Nullable(Date32) / Nullable(DateTime64(6)) /
Nullable(DateTime64(6,'UTC')) / Nullable(String)` on a plain `MergeTree ORDER BY
id`. walshadow lands the same values with `bool_val` as `Nullable(Bool)`,
`ts_val` as `Nullable(DateTime64(6,'UTC'))`, **four extra columns of its own**
(`_lsn`, `_xid`, `_commit_ts`, `_is_deleted`) and
`ReplacingMergeTree(_lsn, _is_deleted)`. Both validate against the same 15-column
source aggregate; the extra columns are outside the compared set.

## Interleaving, host state, and the rig

Three rounds, arms interleaved **apitap · walshadow-default · walshadow-tuned**,
because this box carries other containers and a block of tools would confound host
drift with engine identity. `loadavg` (1-min) around each leg ranged **2.75 – 7.68**
and `MemAvailable` sat between **46.7 GiB and 47.5 GiB** throughout; every
before/after pair is in the raw log. At a 0.5-CPU quota, CPU is 0.5 × wall for any
leg that saturates the quota, so wall and CPU are the same measurement here —
reported once.

Dropped after every verification: all 10 destination tables. Final state: **0
leftovers, 10 seeds intact at 1,000,000 rows × 15 columns, 0 replication slots** on
the source.

## What this does and does not prove

**It does prove** that at 0.5 CPU / 256 MB, on PostgreSQL 18 → ClickHouse 24.8,
with these ten tables, apitap 0.57.0 completes the whole concurrent job in a median
of 18.4 s at a 119 MB peak with every table checksum-verified, and that
walshadow 0.1.2 is OOM-killed doing it in both its shipped configuration and the
one its own documentation prescribes for a small box, landing nothing, 6 of 6.

**It does not prove** any of the following, and none of it should be read into
the table above:

- **The cap is per tool, and the destination is uncapped.** Only the loader is in a
  cage. Both tools wrote into the same full-size ClickHouse, so these numbers are
  "what each tool can do while the database is free to help" — the deployment
  shape being asked about — but not a claim about a capped database.
- **The two tools are not doing the same job, and this is the biggest caveat.**
  apitap is a one-shot multi-table load: 10 concurrent `COPY` streams, done when
  it returns. walshadow is a CDC daemon measured as **catch-up** — it takes a
  physical base backup of the source, replays WAL through a shadow PostgreSQL it
  owns, and then keeps running. The comparison is catch-up-time vs catch-up-time,
  both from an empty destination and a pre-seeded source, and that is the fairest
  comparison available between a loader and a daemon. It is not a claim that one
  tool is 3× "faster at loading": walshadow's 1 GB catch-up includes a 2.5 GB
  physical backup, which a loader never performs.
- **For walshadow the cage contains a database; for apitap it does not.** The cap
  covers the daemon *and* the shadow PostgreSQL walshadow launches, because that
  is upstream's own deployment unit (one compose service). That is not neutral:
  it works against walshadow in principle. It did not decide the outcome — the
  attribution run shows the daemon alone at 550 MB before any PostgreSQL process
  exists — but a reader should not have to take that on trust.
- **walshadow's own defaults cannot fit this tier, which is a configuration fact,
  not a tuning failure.** `resident_payload_max` has a documented 512 MiB floor and
  `byte_budget` defaults to 256 MiB, on a cage of 256 MB. The tuned arm sets both,
  plus a 32 MiB transaction buffer and 2+2 pools, and is still killed in 5 s.
- **The uncapped and 1 GB numbers are not a claim about the capped tier.** They
  exist so "it OOMs" can be read as "and here is the smallest box where it does
  not" rather than as a verdict with nothing behind it.
- **One rig, one schema, one day.** 10M rows of a specific 15-column shape
  (~258 B/row), no TOASTed values, a single-node ClickHouse, and a source whose
  `shared_buffers` is 32 MB. A wider table, a machine with different page-cache
  behaviour, or a `jsonb`/`numeric`-heavy schema would move the seconds and could
  move the ratio. They would not move the OOM verdict, which reproduced 6 of 6 here
  and again on the 512 MB rung.
- **apitap's floor is inside its number.** The 18.4 s includes ~15.7 MB and
  ~0.6 s of wheel import inside the cage; the transfer-only column is printed
  beside it, and the probe above is the same measurement in isolation.
- **This is not the MySQL arm's number.** apitap's median here is 18.4 s against
  36.1 s on MySQL → ClickHouse. Different source, different cost; the two
  campaigns' apitap figures should not be compared with each other, only within
  their own arm.
- **The seed was reused, the destination never was.** Cache warmth between
  consecutive legs is the largest source of run-to-run spread, and the harness
  interleaves rather than randomises, so a systematic drift within a round is not
  ruled out — only bounded by the logged load.

## Reproducing

```bash
# on the bench VPS, from the repo
rsync -a -e "ssh -i ~/.ssh/apitap_vps" --exclude .git --exclude target \
      benchmarks/ ubuntu@<vps>:~/apitap-lib/benchmarks/
cd ~/apitap-lib/benchmarks

# the rig: a dedicated PostgreSQL 18 with wal_level=logical and a pg_hba that
# trusts the bridge for replication, and the walshadow image from its release
docker build -t apitap-bench-ws:0.1.2 -f bench-capped-pg-ch-ws-image.Dockerfile ~/ws-dl/build

bash bench-capped-pg-ch-0.57.sh seed        # 10 tables, ~64 s, kept between runs
bash bench-capped-pg-ch-0.57.sh srcsum      # source aggregates, cached
bash bench-capped-pg-ch-control.sh           # GREEN x3 before anything is timed
bash bench-capped-pg-ch-0.57.sh startup     # what each tool pays in the cage
ROUNDS=3 LEG_TIMEOUT=1800 bash bench-capped-pg-ch-0.57.sh run
LEG_TIMEOUT=3600 bash bench-capped-pg-ch-0.57.sh wsceiling uncapped 512m 1g 2g
bash bench-capped-pg-ch-attrib.sh uncapped  # whose memory is it
bash bench-capped-pg-ch-0.57.sh state       # dest clean? seeds intact? disk?
bash bench-capped-pg-ch-raw.sh bench-capped-pg-ch-0.57-raw.log
```

`LEG_TIMEOUT` (default 1800 s) is the per-leg deadline: hitting it is recorded as
a result (`deadline_…s`) with the kernel's counters and whatever landed in the
destination captured *before* the container goes away. No leg in this campaign
needed it — every walshadow arm killed itself first.