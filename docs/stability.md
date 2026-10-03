# What you may depend on, and what may still move

apitap reached v0.55.1 in under four months. At that pace a version number stops
carrying information, and "0.x" tells you only that we have not committed to
anything — which is the opposite of useful if you are deciding whether to put
this in a pipeline. So here is the commitment, written down.

## The public surface

These are covered. A breaking change to anything in this section gets a MINOR
bump while we are pre-1.0, and a MAJOR bump after 1.0 — never a patch, and
never silently.

| surface | what is stable |
|---|---|
| `apitap.transfer(...)` | the function, its positional arguments (`src`, `dst`, `table`), and every keyword listed in [the manual](usage.md#api) |
| `apitap.read(...)` | the function, `table=` (with `cursor=`/`parallel=`/`columns=`), and the Arrow/Polars handoff via `Reader`. `query=` is reserved in the signature but **refused today** — passing it raises `ValueError`; raw-SQL reads are a roadmap item, not a commitment |
| `mode=` values | `"replace"`, `"append"`, `"merge"`, `"log_based"` — spellings and meanings |
| URL schemes | `postgres://`, `postgresql://`, `mysql://`, `clickhouse://`, `clickhouse+https://`, `bigquery://`, `gcs://`, `s3://`, `iceberg://`, `gsheets://`, `github://`, `github+api://` |
| `apitap.request_stop()` | the function and its meaning: a running `mode="log_based"` drain stops at its next safe point and returns normally. It is a no-op for a bulk transfer and for a CDC table's first (bootstrap) run — neither has a safe point to stop at |
| `TransferReport` | `rows`, `elapsed_ms`, `parallel` — fields are added, never removed or repurposed |
| multi-table results | there is no separate report class: `TransferReport.tables` holds the per-table outcomes (`None` on a single-table run), each `TableResult`'s `table`/`rows`/`elapsed_ms`/`parallel`/`error` is committed, and partial failure raises `MultiTransferError` whose `report` carries that same `TransferReport` |
| destination artifacts | the `_apitap_state` table's columns, and the `__apitap_cl` / `__current` changelog shapes — a run of an older apitap must not choke on a newer one's state. `_apitap_cdc_pending` gains `seq_base, end_lsn, events` on ClickHouse (added in place; rows written by 0.56.0 read `events = 0`) and exists on BigQuery from 0.57.0 (clustered, INSERT-only). Rolling back to 0.56.0 after a 0.57.0 run crashed mid-window can re-append that one window with repeated pairs. And, from 0.57.0, the guard artifacts: a run of a supported older apitap (0.55.1, 0.56.0) refuses beside a newer one's run and vice versa, except that a 0.55.1 *drain* is invisible to everything ([upgrading and rolling back](failure-modes.md#upgrading-and-rolling-back)) |
| exit behaviour | invalid input raises `ValueError`, a failed transfer raises `RuntimeError`, and a failed transfer never leaves the destination table changed |
| one run per destination table | **0.55.0+ for bulk modes** (`replace`, `append`, `merge`), **0.56.0+ for `log_based`** on Postgres, MySQL, ClickHouse and BigQuery destinations — a drain and a bulk run refuse each other in both directions, and so do two drains — on a fleet that is entirely 0.56.0+, and from 0.57.0 also beside 0.55.1 bulk runs; a 0.55.1 drain is visible to nothing, so upgrade drains before bulk jobs, and stop 0.56.0 BigQuery drains before 0.57.0 runs start ([the procedure](failure-modes.md#upgrading-and-rolling-back)). Iceberg drains are the exception and are not guarded. The refusal happens at `prepare`, before a row moves, and never touches the destination. Fan-in — two `append` runs from *different* sources into one table — stays allowed; that is a capability, not a collision. Two runs starting in the SAME INSTANT can no longer both proceed; what is committed is that at most one does, and that when neither does, nothing was written. A killed drain's lock **clears itself** once its lease lapses (`APITAP_LEASE_TTL_SECS`, 300s by default) on every guarded destination; an evicted drain writes nothing more on Postgres, MySQL and BigQuery and at most one statement on ClickHouse (each proven by the gate's eviction legs), and once a peer has collected a drain's claim that drain writes nothing more on any guarded destination, watermark included; a killed bulk run's staging still does not. On ClickHouse a 0.56.0 collector against a 0.56.0 victim keeps the pre-0.57 behaviour, and a dead 0.56.0 Postgres drain under a non-`public` search_path leaves a lease 0.57.0 cannot find — drop its lock by hand, once. [The matrix, the window, and the lease](failure-modes.md#two-runs-one-table) |
| `apitap.LockedError` | **0.55.0+.** The refusal above raises this type, not a bare `RuntimeError` — so a scheduler branches on a class instead of matching message text. It subclasses `RuntimeError`, and that subclassing is part of the commitment: code written before it keeps catching it |

## What is explicitly NOT stable

- **Environment variables.** `APITAP_PROGRESS`, `APITAP_CH_MAX_BODY`,
  `APITAP_PG_BINARY`, `APITAP_SLOT_WAL_WARN`, `APITAP_MEM_BUDGET`,
  `APITAP_GRACEFUL_STOP` and friends are
  operational escape hatches. They may be renamed or retired when the default
  gets good enough to make them pointless. Nothing you *need* lives only in an
  env var.
- **Log and progress text.** The `key=value` and JSON progress records are meant
  for humans and dashboards, not parsers-of-record. Field names will be added.
- **Performance numbers.** They are measurements, not promises, and every one is
  dated in the ledgers.
- **Anything the manual marks as a caveat or a roadmap item.**

## Pinning

Pin exactly while we are pre-1.0:

```
apitap==0.56.0
```

Not `>=`. The surface above is committed, but the release cadence is fast enough
that you want to choose when you move, and the wheel is a compiled artifact — a
pin is also what makes a rollback one line.

## The road to 1.0

1.0 is not a feature list, it is a promise we can keep. What it waits on:

1. **The release gate runs itself.** *Half done.* A suite whose coverage is
   declared runs before every tag: `benchmarks/gate.py --matrix` prints every
   published claim × engine and the leg that proves it by asking the server,
   prints every waived cell with the caveat published beside the claim, and exits
   non-zero when a claim in these pages has no leg. It runs a second wheel for
   the upgrade legs and pauses drains for the eviction legs. A human still starts
   it. Until CI enforces that, the version number depends on someone
   remembering.
2. **Wheels for the platforms people actually evaluate on.** aarch64 and macOS,
   plus an sdist. Shipping only `manylinux_x86_64` while selling "cheap small
   boxes" is a contradiction, given ARM *is* the cheap box.
3. **Failure modes proven, not just claimed.** *Largely done, and the page is
   the receipt:* atomic swap, the 0-row guard, the watermark committed with the
   data, the retention refusals and the concurrency refusal are each demonstrated
   under kill-9, a dropped connection and a schema change mid-run, with the
   leftover state and the recovery written down in
   [failure-modes.md](failure-modes.md). Each row there was produced by causing
   the failure on purpose against live servers. What is still missing: a full
   disk, and the object-store destinations under the same treatment.
4. **A soak.** *Started:* 24 hours of Postgres → ClickHouse CDC, a drain every
   30 s against a live writer, with no fd growth, no slot growth and no watermark
   drift ([the numbers](failure-modes.md#the-24-hour-soak-and-what-it-did-and-did-not-settle)).
   One shape at one rate on one rig is not the days-and-many-shapes this item
   asks for, so it stays open — but it is no longer unmeasured.

Until those four are done, treat apitap as excellent at work you can re-run —
backfills, migrations, warehouse rebuilds, `read()` for analysis — and pin it if
it sits anywhere you cannot re-run.
