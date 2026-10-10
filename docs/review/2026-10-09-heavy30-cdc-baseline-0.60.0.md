# Heavy-row CDC baseline on 0.60.0 — 30 tables x 1M rows (15 cols, ~1KB `large_str`), one group, 256 MB / 0.5 CPU

The owner's shape: 30 tables in ONE `apitap.transfer()` run, each table
1,000,000 rows across 15 columns with one heavy column (`large_str`, 900 +
id%200 chars of md5-derived text, avg ~1.0 KB). First load is a full INSERT
seed; then two MIXED CDC rounds, each 30,000,000 changes (1M per table) —
round A 700k U + 150k I + 150k D, round B 400k U + 300k I + 300k D per table
(insert+delete pairs are net-zero; the proven writer shape). Sources:
`apitap-bench-pg-src` (PG) and `apitap-bench-my` (MySQL 8.0, ROW/FULL binlog).
Destination: `apitap-bench-ch3` (ClickHouse 25.8.29, `:8126`). Drain: the
0.60.0 PyPI wheel (`.so` md5 `62facb07f983ce1fc435268c0e41d5a7`) in a cgroup of
**0.5 CPU / 256 MB** (`--cpus=0.5 --memory=256m --memory-swap=256m`), one
`apitap.transfer(mode="log_based")` group per pass (leg.sh loop), Postgres
`slots="auto"` (2 slots at this cage), MySQL single stream.

Generation is server-side SQL (1000-change transactions; 1000 tx per table per
round), so the offer is real logged changes, witnessed on the server
(PG: `pg_current_wal_lsn()` bracket + `pg_stat_user_tables` counters; MySQL:
`SHOW MASTER STATUS` bracket + `Innodb_rows_*` + total binlog bytes). Digests:
the campaign validators per lane, 30/30 tables (PG md5-prefix over 15 cols;
MySQL CRC32 over 14 cols + json), on the exact wheel.

Scripts: `benchmarks/cdc-steady-profile/heavy30-{seed,boot,seq}.sh`
(VPS `~/apitap-lib/benchmarks/cdc-steady-profile/`). Raw artifacts on the
bench VPS under `~/bench-cdc-steady/logs/` (`.witness`, `.drain.log`,
`.summary` per tag; `.converge.log` for boots).

## 1. Results

| | PostgreSQL -> ClickHouse | MySQL -> ClickHouse |
|---|---|---|
| bootstrap 30M rows (cage 256 MB, 0.5 CPU) | 235 s (3.9 min) | 262 s (4.4 min) |
| bootstrap MEMPEAK | 192-197 MB | 136.7 MB |
| offered changes | **60,000,000** (2 x 30M) | **60,000,000** (2 x 30M) |
| applied | **60,000,000 (100%)** | **60,000,000 (100%)** |
| busy apply rate | **23,879 /s** | **17,142 /s** |
| overall rate (incl. tails) | 20.5k/s (2,923 s) | 15.5k/s (3,864 s) |
| MEMPEAK during CDC | **99.8 MB / 256** | **145.0 MB / 256** |
| CPU cap_frac / avg cores | 0.596 / 0.298 | 0.715 / 0.357 |
| OOM events | 0 | 0 (`oom_kill 0`) |
| 30/30 checksum | MATCH | MATCH |
| round generation (offer rate) | A 1,201 s (25.0k/s), B 1,268 s (23.7k/s) | A 1,829 s (16.4k/s), B 1,657 s (18.1k/s) |

Witness (server-side, deltas = exactly 30M per round on both lanes; MySQL
`Innodb_rows_*` lines are in MySQL's `SHOW GLOBAL STATUS` alpha order —
deleted, inserted, updated):

```
pg  W0 34/81944958 | upd 7210000,  ins 42545000, del 780000
pg  W1 44/93691480 | upd 28210000, ins 47045000, del 3030000
pg  W2 50/E819D2D0 | upd 40210000, ins 56045000, del 7530000
my  W0 binlog.000088:29114      | del 8516451,  ins 171191002, upd 45477292  | binlogbytes 29114
my  W1 binlog.000148:64598941   | del 13016451, ins 175691008, upd 66477292  | binlogbytes 30151351765
my  W2 binlog.000197:565553401  | del 22016451, ins 184691008, upd 78477292  | binlogbytes 83311261474
```

Binlog volume: round A 30.1 GB / 30M changes (~1.0 KB/change), round B
53.2 GB (~1.77 KB/change; insert-heavy mixes carry the full 1KB row image).
Total 83.3 GB of binlog for the two rounds; mid-run purges kept the disk
fed (purge policy: keep the newest files, the drain stays near the tail —
its lag was ~1 GB at the point of the first purge).

## 2. Findings

1. **A fat transaction OOM-kills the drain at 256 MB.** The first attempt
   generated each round as 10 x 100k-change transactions (same 30M/round).
   The drain (exit 137) died during round A: one 100k-change tx at ~1KB/row
   is ~130 MB of decoded window against a 256 MB cage with its own buffers;
   no `oom_kill` event fired on the cgroup (the container's peak crossed the
   cap between samples). With 1000-change transactions (the steady rig's
   proven shape) the same volume runs at a 99.8 MB peak. The engine must
   bound memory per transaction (streaming/windowed tx decode), not assume
   small txs — production CDC sees fat batches.
2. **Heavy rows halve the rate.** 0.60.0's headline on the steady rig was
   49.3k delivered/s (2 tables, `slots=2`, ~460 B rows) at this cage. The
   same cage with 30 tables and ~1KB rows measures 23.9k/s (PG) — per-change
   memory movement, not the destination, dominates (probed: the drain never
   reached the statement-time wall of the 0.58 census).
3. **The generation ceiling is the host, not apitap.** Sequential server-side
   SQL produced 16-25k changes/s (PG) and 16-18k/s (MySQL) for this shape:
   "30M changes in 1 minute" (500k/s offered) is not physically offerable by
   this bench VM (~650 MB/s WAL+heap+binlog IO for 1KB rows). The number that
   matters and can be pushed is the APPLY side.
4. **MySQL is the slower, heavier lane** (17.1k/s vs 23.9k/s; 145 MB vs
   100 MB peak; cap_frac 0.72 vs 0.60). Consistent with the 0.58 finding
   (MySQL ~17x behind PG pre-G1.2, and still behind after the 0.60 windows).
5. **Both lanes are checksum-exact with zero OOM at 256 MB** on 60M changes
   per lane — the caged baseline is solid; the gap to the owner's 100k/s
   goal is ~4.2x (PG) and ~5.8x (MySQL).

## 3. Next: the 100k/s optimization campaign

Ordered, measured, A/B-first (candidates, each with the harness above):

1. Freeze + A/B the existing knobs on this exact shape:
   `APITAP_FOLLOW_SECS` (kill the per-pass setup — 0.60 measured 101 passes
   per 180 s on the old shape), `APITAP_CDC_WINDOW_BYTES`,
   `APITAP_CDC_APPLY_LANES`, `slots` > auto at this cage.
2. Per-change CPU work (the real wall): the zero-copy plan of
   `docs/design/2026-10-08-mesin-3jt-per-menit-zero-copy.md` (P1 read path,
   P2 dense collapse, L1b/L3/L4), re-targeted at ~1KB rows where the cost is
   memory movement.
3. Bounded memory for fat transactions (finding 1) so the engine holds a
   100k-change tx at 256 MB.
4. MySQL parity: the MySQL lane apply path (windows + follow) has the most
   headroom (17.1k/s, 0.72 cap — it is the only lane with both spare quota
   AND the lowest rate).

## 4. Knob sweep (2026-10-10, PG lane, PREBUILD backlog arms)

`benchmarks/cdc-steady-profile/heavy30-ab.sh`: each arm generates 6M changes
(NTX=100 per round, both rounds, no drain — a real backlog), then starts the
capped drain and measures pure catch-up. Same cage, same wheel, same shape.

| arm | catch-up rate | MEMPEAK | cap_frac | note |
|---|---|---|---|---|
| base1 (defaults) | 24.6k/s | 89.6 MB | 0.66 | |
| `FOLLOW_SECS=30` | 17.7k/s | 105.1 MB | 0.48 | **-30%: follow parks between floors** |
| `CDC_WINDOW_BYTES=128MiB` | **OOM-killed (137)** | - | - | window > the 256 MB cage |
| `slots=4` | 24.7k/s | 96.5 MB | 0.65 | neutral (client, not decode) |
| `CDC_APPLY_LANES=16` | 27.1k/s | 97.6 MB | 0.70 | inside the noise |
| base2 (defaults repeat) | 27.0k/s | 98.0 MB | 0.70 | **noise band ~+/-5%** |

**Verdict: the knob space is exhausted.** Every arm except the two negative
results sits inside the base1/base2 noise band (24.6 vs 27.0k/s on identical
configs). The wall at this shape is the per-change client cost with ~1KB rows
(memory movement), exactly the target of the zero-copy plan — the 100k/s goal
needs engine work (P1/P2/L1b/L3/L4), not configuration.
