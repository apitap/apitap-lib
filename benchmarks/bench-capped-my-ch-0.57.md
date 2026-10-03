# Capped tier, MySQL → ClickHouse: apitap 0.57.0 vs ingestr 1.1.61

The head-to-head at the tier people actually deploy: **0.5 CPU and 256 MB of
RAM per job**, ten tables of a million rows each, all ten synced concurrently in
one job. apitap is the PyPI wheel **0.57.0**; ingestr is its **latest release,
1.1.61**, installed with `pip` into its own venv. Both tools ran inside one
container each, on the same rig, in three interleaved rounds, and every landed
table was checksum-validated against MySQL before any number counted.

The short version: **apitap moved all 10,000,000 rows in a median of 36.1 s at a
99 MB peak, 30 of 30 tables checksum-MATCH. ingestr was OOM-killed in all six
of its legs and landed zero rows.**

Rig: OVH bench VPS (16 vCPU / 61 GB, shared with production), source
`apitap-bench-my` (`mysql:8.0` on 127.0.0.1:3307, db `bench`), destination
`apitap-bench-ch` (`clickhouse-server:24.8` on 127.0.0.1:8124 / native 9124).
Raw receipts: [bench-capped-my-ch-0.57-raw.log](bench-capped-my-ch-0.57-raw.log).

## The seed — 10 identical tables, kept between runs

`bench.cmp_my_t01` … `cmp_my_t10`, each cloned from the rig's existing
1M-row 15-column `bench.bench_my_1m` with `CREATE TABLE … LIKE` +
`INSERT … SELECT *`, so all ten are byte-identical:

```
id int · small_str varchar(20) · medium_str varchar(100) · large_str varchar(500)
tiny_int smallint · regular_int int · big_int bigint · float_val double
decimal_val decimal(18,4) · bool_val tinyint(1) · date_val date
ts_val datetime(6) · ts_tz_val datetime(6) · json_val json · extra_text longtext
PRIMARY KEY (id)
```

Verified after seeding: **10 tables × 15 columns × exactly 1,000,000 rows** each
(~511 MB per table). Seeding took **232 s for all ten** and is excluded from every
measurement. The seeds were re-checked intact at the end of the campaign and
were never dropped; every destination table was dropped after each verification,
including ingestr's `_bruin_staging` leftovers.

## Results

Every arm = the same 10 tables, the same cap, one container, one job.

| arm | round | wall in container | transfer only | cgroup `memory.peak` | docker state | rows landed | checksum |
|---|---|---|---|---|---|---|---|
| **apitap 0.57.0** (1 process, `tables=[…]`) | 1 | **36.1 s** | 33.9 s | 99.1 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | 2 | **36.1 s** | 34.7 s | 99.1 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | 3 | **38.1 s** | 36.2 s | 97.7 MB | exit 0, not OOM-killed | 10,000,000 | **10/10 MATCH** |
| | **median** | **36.1 s** | 34.7 s | **99.1 MB** | | **10,000,000** | **30/30 MATCH** |
| **ingestr 1.1.61** (10 concurrent processes) | 1 | 14.0 s → killed | — | 257.1 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |
| | 2 | 19.1 s → killed | — | 260.6 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |
| | 3 | 19.2 s → killed | — | 264.1 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |
| **ingestr 1.1.61** (1 process at a time) | 1 | 44.2 s → killed | — | 256.3 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |
| | 2 | 39.1 s → killed | — | 256.2 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |
| | 3 | 46.2 s → killed | — | 256.4 MB | **OOMKilled=true, exit 1** | 0 | 0/10 |

There is no completion time to report for ingestr, so its wall column is
**time-to-kill**, not a duration: the kernel killed the container 14–46 s in,
with all ten of its per-table processes reporting `rc=137` (SIGKILL) every time
— 60 of 60 across the six legs. A median of survival time is a weak statistic
and is printed only so nobody has to reconstruct it; the result is the outcome,
not the number.

**Repeat, for the OOM conclusion.** An earlier full campaign on the same rig with
the same harness logic (only the file names differ; the three in-container leg
scripts are md5-identical) also OOM-killed ingestr in 6 of 6 legs with 0 rows.
What varied between the two campaigns is *how long* it took to die — 14 s to
1102 s — which is page-cache warmth and host state, not a different verdict. That
spread is itself the honest detail: **the failure is reproducible, the countdown
is not.**

## The exact commands

The cap, identical for every arm:

```
--network=host --cpus=0.5 --memory=256m --memory-swap=256m      (base: python:3.13-slim)
```

**apitap** — the multi-table path, one process, one shared pipe budget, no knobs
(`parallel=`, `chunk_bytes` and every env lever left alone; the wheel is mounted
read-only and put on `PYTHONPATH` exactly like the repo's `e2e_parquet_capped.py`
does, so the PyPI wheel is what runs and nothing is built):

```bash
docker run --name cmp-apitap-r1 --network=host $CAP \
    -v /home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages:/py:ro \
    -e PYTHONPATH=/py -v ~/apitap-lib/benchmarks:/job:ro \
    python:3.13-slim sh /job/bench-capped-my-ch-leg-apitap.sh
```

```python
apitap.transfer('mysql://root:bench@127.0.0.1:3307/bench',
                'clickhouse://default:bench@127.0.0.1:8124/default',
                tables=['cmp_my_t01', … 'cmp_my_t10'])
```

It auto-sized to a **shared pipe budget of 8** for the whole job in every round
(read off the cgroup limit, no flag), and each table reports the pipes it drew.

**ingestr** — its own documented invocation, its own defaults (page-size 25000,
batch-size 512 MiB, extract-parallelism 5), nothing tuned apitap's way. A
multi-table run is a CDC-source feature, so its native model for many tables is
one `--source-table` per invocation; the concurrent arm runs **10 of those at
once inside the same capped container**:

```bash
ingestr ingest \
  --source-uri 'mysql://root:bench@127.0.0.1:3307/bench' \
  --source-table 'cmp_my_tNN' \
  --dest-uri   'clickhouse://default:bench@127.0.0.1:9124?http_port=8124' \
  --dest-table 'cmp_my_tNN' \
  --yes --full-refresh --progress log
```

ingestr 1.1.x is no longer a Python program: the `pip` package is a thin wrapper
that downloads and execs a **254 MB native binary**
(`~/.cache/ingestr/bin/v1.1.61/Linux_x86_64/ingestr`). That exact executable is
bind-mounted into the cage, so nothing is downloaded inside a measurement.

**Provenance, verified before the first leg:** `apitap.__version__ == "0.57.0"`
and `apitap/_apitap.abi3.so` md5 `41e9f7c252d3e1eb5403b70f87bf5435`;
`pip show ingestr` → `1.1.61`.

## What each tool costs before it moves a row

Same container, same cap, no work at all — the floor each tool pays to load:

| probe | wall | cgroup `memory.peak` | of which file-backed |
|---|---|---|---|
| `import apitap` | 2.0 s | **16.3 MB** | 1.7 MB |
| `ingestr --version` | 2.0 s | **71.2 MB** | 30.7 MB |

ingestr's floor is 4.4× apitap's on this probe, and most of it is the pages of
its own mapped binary. Across repeated probes that figure swung between **39 MB
and 167 MB** purely with page-cache warmth — which is also why the concurrent arm
sometimes dies in 14 s and sometimes in 18 minutes. Every ingestr leg reported
here ran with the binary **warm**, i.e. the case that favours it.

## How far ingestr gets at this cap

Rather than only "it failed", the same container, same cap, one process, and
ingestr's own `--sql-limit`, validating each landing against the matching MySQL
subset (`WHERE id <= N`):

| rows requested | outcome | cgroup peak | verdict |
|---|---|---|---|
| 100,000 | completes in ~4.5 s | 222.7 MB | **checksum MATCH** (`100000\|214770805050524\|214644031908969\|1\|100000`) |
| 200,000 | **OOM-killed** (twice), passed once at 253.3 MB | 253–256 MB | not reproducible at the edge |
| 400,000 | **OOM-killed** (3 of 3) | 256.0 MB | 0 rows |

So at 0.5 CPU / 256 MB ingestr reliably lands ~100k rows of a 15-column table,
sometimes 200k, and never 400k in three attempts. **The 1M-row tables in this
campaign are 5–10× over that ceiling**, which is the whole story of the six
OOM-killed legs.

ingestr's own progress log shows the mechanism, from inside the cage. Two
excerpts, from two different table runs of the single-process arm:

```
# table 01, killed 3 s later
[STRATEGY] Using staging table: _bruin_staging.cmp_my_t01_staging_badd8a86…
[PROGRESS] Rows: 125,000 | Rate: 40544 rows/s | CPU: 42.6% | Mem: 266 MB
Killed

# table 10, killed 212 s later — one process, decaying in place
[PROGRESS] 16:55:50 | Rows: 100,000 | Rate: 8123 rows/s | Mem: 268 MB
[PROGRESS] 16:56:50 | Rows: 100,000 | Rate: 1380 rows/s | Mem: 268 MB
[PROGRESS] 16:57:22 | Rows: 100,000 | Rate:  960 rows/s | Mem: 268 MB
Killed
```

The Go runtime reports **266–269 MB of its own heap inside a 256 MB cage** — it
does not see the limit — so the cgroup pins at `memory.max` and the kernel
throttles reclaim on every allocation. In the second excerpt the same process
decayed from 8,123 rows/s to 960 rows/s over 92 s, **8.5× slower with the row
count not moving at all**, and then the OOM killer took it. That is the whole
failure in one log: the tool is not spending the CPU, it is waiting on memory.

The kernel's own counters agree: docker reports `State.OOMKilled=true` on all
six legs, and on the one run where the cgroup was polled live while it died,
`memory.events` read `oom 2, oom_kill 1`.

## Validation

A number without a verified checksum is not a result, so the validator is
[`bench-capped-my-ch-validator.sh`](bench-capped-my-ch-validator.sh) — one
definition per engine, used for the source, apitap's landing and ingestr's
landing. Per table it is order-independent and cheap: row count, an
order-independent `SUM(CRC32(...))` over 14 of the 15 columns, an
order-independent `SUM(CRC32(...))` over the raw JSON text, and `MIN`/`MAX` of
the primary key. No `string_agg` — that is what breaks past ~100M rows. Source
sums are cached per table and invalidated by a hash of the validator text, so the
1m32s MySQL scan was paid once for the whole campaign.

A validator that has only ever agreed is untested.
[`bench-capped-my-ch-control.sh`](bench-capped-my-ch-control.sh) proves it both
ways on a 1000-row control table before any timed leg, and it ran GREEN:

1. **agreement** — apitap's landing digest equals the source digest, exactly;
2. **sensitivity** — changing one value in one row (`extra_text` of `id=777`)
   changes the digest (`2168502783323` → `2170267471401`), so a MATCH means
   something;
3. **the rival validates under the same rules** — ingestr's own landing passes
   the same validator.

Getting there took three traps that would each have produced confident nonsense,
all caught by diffing per column instead of staring at a mismatch:

- **This ClickHouse build returns an empty string from `JSONExtract*`**, even for
  a literal. The JSON column is therefore validated by CRC32 over its raw text,
  which both engines render byte-identically (verified with `hex()`).
- **MySQL reads `'\x1f'` as the four characters `\x1f`** while ClickHouse reads
  it as the single byte `0x1f`. The separator is built as `CHAR(31)` and
  `unhex('1f')` for exactly that reason; the per-column diff is what caught it,
  because all 30 per-column aggregates agreed while every row digest differed.
- **The table placeholder is `@TBL@`, not `%s`** — the MySQL aggregate carries
  `%s` inside `DATE_FORMAT('%H:%i:%s.%f')`, and a `%s` placeholder silently ate
  it, which would have scored the campaign against a validator that dropped the
  seconds.

[`bench-capped-my-ch-mismatch.sh`](bench-capped-my-ch-mismatch.sh) ships those
tools (`bisect` a range down to the first bad row, `row` one row's digest field
by field, `cols` the 30 per-column diff) because the next person to hit a
mismatch will need them.

Also recorded, because a schema difference is a result and not a nuisance:
apitap lands the 15 columns as `Int32 / Nullable(String) / Nullable(Int16) /
Nullable(Int32) / Nullable(Int64) / Nullable(Float64) / Nullable(Decimal(18,4)) /
Nullable(Int8) / Nullable(Date32) / Nullable(DateTime64(6)) / Nullable(String)`;
ingestr lands the same values with `bool_val` as `Nullable(Int16)`, `date_val` as
`Nullable(Date)`, a `ReplacingMergeTree` engine, and **two extra columns of its
own** (`_ingestr_loaded_at`, `_ingestr_run_id`). Both validate against the same
15-column source aggregate; the extra columns are outside the compared set.

## Interleaving, host state, and the rig

Three rounds, arms interleaved **A B C A B C**, because this box carries other
containers and a block of tools would confound host drift with engine identity.
`loadavg` (1-min) before each leg ranged **2.36 – 7.19** and `MemAvailable` sat
between **47.0 and 47.5 GB** throughout; every before/after pair is in the raw
log. At a 0.5-CPU quota, CPU is 0.5 × wall for any leg that saturates the quota,
so wall and CPU are the same measurement here — reported once.

Dropped after every verification: all 10 destination tables, in `default` and in
`_bruin_staging`. Final state: **0 leftovers, 10 seeds intact at 1,000,000 rows ×
15 columns.**

## What this does and does not prove

**It does prove** that at 0.5 CPU / 256 MB, on MySQL 8.0 → ClickHouse 24.8, with
these ten tables, apitap 0.57.0 completes the whole concurrent job in ~36 s at a
99 MB peak with every table checksum-verified, and that ingestr 1.1.61 is
OOM-killed doing it in both its native concurrent model and its leanest one,
landing nothing.

**It does not prove** any of the following, and none of it should be read into
the table above:

- **The cap is per tool, and the destination is uncapped.** Only the loader is in
  a cage. Both tools wrote into the same full-size ClickHouse, so these numbers
  are "what each loader can do while the database is free to help", which is the
  deployment shape being asked about — but it is not a claim about a capped
  database.
- **The concurrency models differ by design, and this is a 10-way concurrency
  question, not a 1-way one.** apitap runs one process that shares one pipe
  budget across the ten tables (8 pipes here, auto-sized). ingestr's documented
  batch model is one process per table, so 10 tables means 10 runtimes; its
  sequential arm exists precisely so the failure is not dismissed as "you gave it
  too many processes", and it fails too. What this does **not** isolate is how
  much of the gap would remain on a single table at a generous cap — the
  [README](README.md) tables cover that uncapped.
- **One rig, one schema, one day.** 10M rows of a specific 15-column shape
  (~350 B/row). A narrower table, a machine with a different page-cache
  behaviour, or a ClickHouse with a different `max_memory_usage` could move the
  seconds; they would not move the OOM verdict, which reproduced 6 of 6 here and
  6 of 6 in the earlier repeat.
- **ingestr's ceiling is a range, not a line.** 200k rows passed once and was
  OOM-killed twice. Treat "between 200k and 400k" as the honest statement.
- **The seed was reused, the destination never was.** Cache warmth between
  consecutive legs is the largest source of run-to-run spread we saw (it is also
  what produced ingestr's 14 s vs 1102 s), and the harness interleaves rather
  than randomises, so a systematic drift within a round is not ruled out — only
  bounded by the logged load.
- **apitap's own floor is inside its number.** The 36.1 s includes ~16 MB and
  ~0.7 s of wheel import inside the cage; the transfer-only column is printed
  beside it, and the probe above is the same measurement in isolation.

## Reproducing

```bash
# on the bench VPS, from the repo
rsync -a --exclude .git --exclude target --exclude dist \
      benchmarks/ ubuntu@<vps>:~/apitap-lib/benchmarks/
cd ~/apitap-lib/benchmarks
bash bench-capped-my-ch-0.57.sh seed        # 10 tables, ~232 s, kept between runs
bash bench-capped-my-ch-0.57.sh srcsum      # source aggregates, cached
bash bench-capped-my-ch-control.sh          # GREEN x3 before anything is timed
bash bench-capped-my-ch-0.57.sh startup     # what each tool pays in the cage
ROUNDS=3 LEG_TIMEOUT=1200 bash bench-capped-my-ch-0.57.sh run
bash bench-capped-my-ch-0.57.sh ceiling     # how many rows one ingestr process lands
bash bench-capped-my-ch-0.57.sh state       # dest clean? seeds intact?
bash bench-capped-my-ch-raw.sh capped-my-ch-raw.log
```

`LEG_TIMEOUT` (default 1200 s) is the per-leg deadline: hitting it is recorded as
a result (`stopped_by deadline_1200s`) with the kernel's `memory.events` and
whatever landed in the destination captured *before* the container goes away. No
leg in this campaign needed it — every ingestr arm killed itself first.
