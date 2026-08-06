# 0.28.0 — the CDC apply campaign, end to end

Three knives shipped, one profile that overturned the plan, and four
hypotheses killed before any of them cost a build.

## What ships

| knife | what changed |
|---|---|
| key-table reuse | `CREATE IF NOT EXISTS` + `TRUNCATE` per window instead of `DROP → CREATE → … → DROP` |
| DDL memoization | the connection remembers what it created; the state table and key table are built once per run, with a `DROP` on the first-time path so a changed PK still self-heals |
| Bytes cells | `pgoutput::Cell::Text` is a refcounted slice of its frame, not its own `Vec<u8>` — 22.5M allocation pairs removed per 1.35M changes; the XLogData header drop stops memmoving every payload |

## The release number

0.27.0 PGO against 0.28.0 PGO — same build kind, same wide workload (10 tables
× 15 columns × ~607 B/row, 1,500,000 mixed changes, 60/30/10 insert/update/
delete in 10k-row transactions), 0.5 cpu / 256 MB, destination dropped and
reseeded before every leg, three interleaved rounds:

| round | 0.27.0 | 0.28.0 | delta |
|---|---|---|---|
| 1 | 52.8 s | 50.0 s | −5.3% |
| 2 | 56.4 s | 44.5 s | −21.1% |
| 3 | 54.9 s | 47.9 s | −12.8% |
| **median** | **54.9 s** | **47.9 s** | **−12.8%** |

**27,300 → 31,300 changes/s**, i.e. 1.64M → 1.88M changes per minute on half a
core. Wins 3 of 3. Destination row state verified against the source on ten
tables and six aggregates.

The mechanism is visible in the saturation column rather than the CPU column:
0.28.0 sits at **96-97%** of its 0.5-core quota where 0.27.0 sits at **79-84%**.
CPU-seconds barely move. The win is in **not blocking** — which is why every
glibc tuning knob failed and only removing the allocations worked.

Do not compare the absolute seconds here against the mid-day A/B in
`cdc-apply-profile.md`: the rig's ClickHouse destination grew all day and the
whole board drifted slower. Interleaved deltas are comparable; absolutes are
not.

## Gate

Nine e2e suites, run twice — once on the plain-release wheel, once on the PGO
wheel that actually ships:

log_based → postgres · clickhouse · mysql · iceberg · multi-table ·
**MySQL binlog CDC** · read (arrow/polars) · read mysql · clickhouse source.

All green on both. The gate proved it can go red first: a leftover
`dlt_bench_slot` from the dlt race tripped the multi-table slot assertion, and
the run was re-done clean rather than reasoned away.

## Review

Five independent lenses over the branch diff, each finding then attacked by a
skeptic that defaults to refuting. **16 findings refuted, 2 confirmed** — and
both were things a knife took away without its commit noticing:

- Memoizing the key-table DDL also froze its **column set**. A source primary
  key gaining a column would have wedged every window forever against a stale
  table, where the old per-window `DROP` self-healed. Fixed with one `DROP` per
  run.
- `Bytes::from(Vec<u8>)` is only free when `len == capacity`. The MySQL binlog
  lane builds temporal and DECIMAL values with `format!`, which leaves slack —
  so the conversion **added** an allocation on the one CDC path that has no
  shared frame to slice from. Fixed with `into_boxed_slice()`. The
  pg→ClickHouse A/B could never have seen it.

## Not shipped, with receipts

Window sizing · parallel apply · RowBinary for the CDC lane · a tokio worker
cap (the runtime spawns lazily; a real drain has 3 OS threads, not 16) ·
`M_MMAP_THRESHOLD` · `M_TRIM_THRESHOLD` at 64M and 128M · both together.

Details and the three thrown-away measurements are in
[cdc-apply-profile.md](cdc-apply-profile.md).
