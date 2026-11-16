# apitap.sql() at TPC-H SF50 across two GCP servers — results + handoff

Status 2026-11-09. Read this before touching the GCP TPC-H rig or quoting any of
these numbers. Everything below was measured, not estimated, unless it says so.

## 1. What exists

| Piece | Where | Notes |
|---|---|---|
| `tpch-pg` | GCP `northamerica-northeast1-b`, c3-standard-8-lssd (8 vCPU, 32 GB, 2×375 GB local NVMe RAID0) | Postgres 16, db `tpch`, the WHOLE TPC-H schema at SF50 = 70 GB. Tables are UNLOGGED. |
| `tpch-my` | same zone, same machine type | MySQL 8.0, db `tpch`, the whole schema at SF50 = 68.5 GB, InnoDB with PKs. |
| `tpch-app` | same zone, c3-standard-8 (8 vCPU, 32 GB, 30 GB boot disk) | Docker + `apitap-bench` image (apitap from PyPI, 0.66.0) + `duckdb-bench` image (DuckDB 2.0.0 + postgres/mysql extensions). |
| Row counts | both servers, identical | lineitem 300,005,811 · orders 75,000,000 · partsupp 40,000,000 · part 10,000,000 · customer 7,500,000 · supplier 500,000 · nation 25 · region 5 (validated against the dbgen files at load time). |
| Scripts | apitap repo `extract/gcp/` (uncommitted) | `up.sh small|full`, `pause.sh`, `down.sh`, `app.sh up|test|results|down`, `rotate-ro.sh`, `secret-merge.sh`, `logged.sh`, `vm-startup.sh`, `bench.py`, `tpch_queries.json`. Run on the VPS (`~/apitap/extract/gcp`), with the apitap-dev SA key `~/apitap.json`. |
| Live log | `https://apitap.dev/extract#/ops?key=<GCP_ACCESS_KEY>` | Everything run through `logged.sh` lands there. Key: `secret-merge.sh --get GCP_ACCESS_KEY` on the VPS. |
| Web | SQL tab, private section | `gcp_pg` / `gcp_my` aliases, visible only with the key (egress over the internet is billed). |

Query texts: `tpch_queries.json` = the README's 8 TPC-H texts with the README's
split (lineitem, partsupp, customer from Postgres; orders, part, supplier,
nation, region from MySQL). 7 of 8 are cross-database joins; Q6 is lineitem only.

## 2. Results — 30 GB / 8 vCPU cage, both engines, n = 1

Each query in its own `docker run --memory=30g --memory-swap=30g --cpus=8` on
`tpch-app`, reading both servers over the VPC (10.162.0.15 / .16, TLS).
Truth = the same text run natively on `tpch-pg` (all tables local, 8 parallel
workers). Every OK row below matched the truth value by value ("exact").

| Query | Postgres native | apitap 0.66.0 | apitap peak | DuckDB 2.0.0 | DuckDB peak |
|---|---|---|---|---|---|
| Q6 | 34.6 s | 32.2 s | 204 MB | 31.9 s | 39 MB |
| Q12 | 41.5 s | **66.1 s** | 1.6 GB | 87.4 s | 5.4 GB |
| Q14 | 33.9 s | 57.6 s | 463 MB | **44.3 s** | 903 MB |
| Q3 | 41.6 s | **73.0 s** | 2.6 GB | 101.3 s | 2.5 GB |
| Q5 | 40.6 s | **68.1 s** | 888 MB | **FAILED** after 180 s (OOM, see §3) | 21.4 GiB |
| Q10 | 44.6 s | 73.0 s | 1.3 GB | **51.3 s** | 2.2 GB |
| Q18 | 207.4 s | **131.6 s**¹ | 4.3 GB | 189.9 s | 8.5 GB |
| Q9 | 108.3 s | **106.6 s** | 4.7 GB | 166.6 s | 7.4 GB |

¹ Q18 and the first-pass Q5/Q10 apitap runs overlapped an orphaned Postgres
truth query (see §4, trap 3). Q6/Q12/Q14/Q3/Q5/Q9 were re-run clean and moved by
≤ 12 %; Q18 and Q10 were not re-run.

Average cores busy (cgroup CPU time / wall, recorded only from the last runs):
apitap Q5 0.9 cores, apitap Q9 1.2 cores, DuckDB Q9 0.9 cores. Spot samples:
apitap Q5 ~1.3–1.5 cores, DuckDB Q9 ~3.7 cores.

## 3. What the numbers say (and do not say)

1. **Source-bound, not engine-bound.** During the runs `tpch-pg` sat at 50–75 %
   iowait: 46 GB of lineitem does not fit in 31 GB of RAM, so every query
   re-reads it from NVMe at ~230 MB/s per stream. apitap used ~1 of 8 cores.
   These numbers measure the source servers as much as the engines. To see
   the engines, give the sources more RAM than data (c3-highmem-8 /
   c3-standard-22) or run each query twice and keep the warm run.
2. **apitap: 8/8 exact, 5 of 8 faster than DuckDB, 2–3× less memory** on the
   heavy joins (Q12 1.6 vs 5.4 GB, Q18 4.3 vs 8.5 GB, Q9 4.7 vs 7.4 GB).
   DuckDB wins Q14 and Q10. Q6 is a tie.
3. **DuckDB Q5 failure** = `Out of Memory Error: failed to offload data block
   of size 96.0 KiB (21.4 GiB/21.4 GiB used)`. Its temp directory was on the
   30 GB boot disk of `tpch-app`, so spilling had little room. Honest claim:
   "DuckDB needed > 21 GB RAM plus spill for Q5; apitap did it in 0.9 GB".
   NOT "DuckDB cannot run Q5". To settle it: point `temp_directory` at a big
   disk (local SSD on the app VM) and re-run Q5.
4. **Fairness checks done:** both engines push WHERE filters to the servers
   (seen in `pg_stat_activity`: identical `COPY (SELECT cols FROM lineitem
   WHERE ctid … AND l_shipdate …)` shapes); neither server ran a join for
   apitap (verified). **Not done:** DuckDB's `mysql_enable_remote_pushdown =
   true` may push joins among MySQL-only tables (Q9: part, supplier, orders,
   nation) into MySQL. Check `information_schema.processlist` during DuckDB
   Q5/Q9/Q10; if it does, re-run those with it off before publishing.
5. n = 1 everywhere. Not publishable as-is (see memory
   `benchmark-fairness-rules`): needs n ≥ 3, warm/cold stated, and item 4.

## 4. Traps hit (each one cost time; the scripts now guard against them)

1. Zone `-a` out of e2 capacity → `up.sh` walks zones and machine types.
2. The apitap-dev SA cannot attach the default compute SA → VMs use
   `--no-service-account --no-scopes`.
3. **Stopping a test does not stop Postgres.** Killing the client left the
   Q9 truth query running 10 min with 8 workers, eating the disk and the
   2 GB `/dev/shm` (which made the second Q9 truth fail). Cancel server-side
   (`pg_terminate_backend`) whenever a run is interrupted. TODO: `app.sh`
   should do this itself on stop.
4. MySQL `LOAD DATA … LINES TERMINATED BY '|\n'` loaded 1 row per file, silently
   → strip the trailing `|` with sed; load validates counts against the files.
5. **Resume locked MySQL out:** a fresh random root password in the env no
   longer matched the data dir → the startup script decided "data gone" and
   began a reload (root could not DROP, so nothing was lost). Fixed: root's
   password persists on the data disk (`/mnt/data/.myroot`) and is re-applied
   with `--init-file`.
6. Resume must ASSEMBLE the RAID, never re-create it; DB containers are
   stopped with `docker stop -t 600`, never killed (UNLOGGED tables are
   emptied after a crash). Both are in `vm-startup.sh` / `pause.sh`.
7. `pkill -f <pattern>` over ssh kills its own shell when the pattern is in
   the command line (memory `verify-by-artifact`). Kill by PID or `pkill -x`.
8. DuckDB's MySQL client rejects the server's self-signed CA under
   `ssl_mode=required`; `preferred` connects (still TLS: `extract_ro` is
   `REQUIRE SSL`).
9. **Engines echo DSNs in errors.** A DuckDB error put the `extract_ro`
   password into the ops log. Scrubbed, `bench.py` now redacts, and the
   password was rotated (`rotate-ro.sh`). Never forward raw stderr to a log.
10. `sed -i` on a log that a running `tee -a` writes to swaps the inode: the
    writer keeps the deleted file. Rewrite in place (`cat tmp > log`).
11. gcloud warns that `--filter labels.purpose=apitap-tpch` semantics will
    change; check the filters in up/pause/down/app before relying on them.

## 5. Open items, in order

1. Decide the servers' fate: **they are running** (3 VMs ≈ $1.8/h, estimate).
   `pause.sh` (keeps 70 GB, ~$0.2/h for disks) or `down.sh` (deletes).
2. Q9 at 256 MB / 0.5 vCPU (the README cage, and the public web sandbox)
   OOMs: 172 MB peak at the 10 GB bench, > 256 MB at SF50 — apitap's memory
   grows with lineitem rows on Q9. The 256 MB tier was skipped at the user's
   request this round; measure all 8 there (`TIERS=256m:0.5 app.sh test`)
   before working on the engine (memory `optimize-measure-all-first`).
3. DuckDB remote-pushdown check (§3.4) and Q5 with a big temp dir (§3.3).
4. Warm-cache or big-RAM sources so the engines, not the disks, are measured.
5. `bench.py`: record network bytes per query (bytes actually moved).
6. Commit the `extract/gcp/` scripts, runner/web changes (apitap repo) — only
   when the user asks.

## 6. Work orders for the next agent

Ground rules (from memory, all HARD): engine work lives in Rust in
`crates/apitap-core` (never the Python shim); cargo builds/tests only on the
VPS; measure ALL queries first, design general fixes, one build, retest all
(never one query at a time); no heavy pushdown (servers only scan + filter +
send); a fix is not done until its test was seen RED without it; commit only
when the user asks. The rig: §1. Bring it back with `up.sh full` (resume, ~1
min, no reload) + `app.sh up`; take it down with `pause.sh`.

### W1 — P0 engine: bounded memory at SF50 (the 256 MB / 0.5 vCPU cage)
- Symptom: Q9 OOM-killed at 256 MB after 43 s (web sandbox, SF50); peak was
  172 MB on the 10 GB bench, 4.7 GB at SF50 in the 30 GB cage. apitap's memory
  grows with lineitem rows — this contradicts the bounded-memory claim.
- Step 1 (measure all): `TIERS=256m:0.5 TRUTH=0 logged.sh app.sh test` for
  all 8 (truths are cached in `/root/bench/results.json` on tpch-app). Record
  which OOM and the 30 GB-cage peaks (§2) for every query.
- Step 2: profile on the VPS with the SF50 shape (or a scaled local seed):
  which structure grows — hash build side, group table, bucket/partition
  buffers, the semi-join key set (Q18)? See memories `tpch-lake-demo` (4
  memory walls), `sql-engine-program`, `join-engine-research-2026-11`.
- Fix: spill or partition so peak is set by the budget, not by row count
  (APITAP_MEM_BUDGET / cgroup-derived); must scale with memory AND cores
  (memory `scale-with-resources`).
- Done when: all 8 TPC-H queries finish exact at 256m:0.5 on SF50, and
  the 30 GB-cage times do not regress > 10 %.

### W2 — engine throughput: use the cores the cage has
- Symptom: in the 8-vCPU cage apitap averaged 0.9–1.2 cores; DuckDB ~3.7
  cores at peak. DuckDB beat apitap on Q14 (44.3 vs 57.6 s) and Q10 (51.3 vs
  73.0 s); Postgres native beats apitap on Q12/Q14/Q3/Q5/Q10.
- First separate source from engine: (a) warm the sources (run twice) or
  move them to c3-standard-22 (88 GB RAM > 70 GB data); (b) count apitap's
  parallel streams per table in `pg_stat_activity` / processlist (seen: 8
  ctid ranges on lineitem; DuckDB uses up to 12 pooled connections).
- Fix only what the profile shows (stream count vs core count, decode,
  join/agg parallelism). Done when: Q14 and Q10 ≤ DuckDB in the same cage,
  measured warm, n ≥ 3, exact.

### W3 — bench fairness before any number is published
- DuckDB `mysql_enable_remote_pushdown=true` may push MySQL-only joins (Q9:
  part × supplier × orders × nation) into MySQL. Watch
  `information_schema.processlist` during DuckDB Q5/Q9/Q10; if it pushes
  joins, re-run with `SET mysql_enable_remote_pushdown=false` and report both.
- DuckDB Q5: re-run with `temp_directory` on a large disk (mount local SSD or
  a bigger pd on tpch-app). Report its peak and whether it finishes.
- n ≥ 3 per cell, cold vs warm stated, versions pinned (apitap 0.66.0, DuckDB
  2.0.0, Postgres 16, MySQL 8.0). Checklist: memory `benchmark-fairness-rules`.
- `bench.py`: add bytes received per query (container net rx) next to
  time/peak/cores.

### W4 — rig scripts
- `app.sh`: on stop/interrupt, terminate the Postgres truth backend
  (`pg_terminate_backend` for `usename='extract_ro'` and the truth text) and
  remove containers — trap 3 cost a contaminated round.
- gcloud filter warning on `labels.purpose=apitap-tpch` (trap 11): switch to
  the documented syntax and re-test up/pause/down/app.
- Optional: Postgres `--shm-size` 2g → 8g in `vm-startup.sh` (needed only by
  the native truth queries; apitap never uses it).
