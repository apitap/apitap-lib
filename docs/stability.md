# What you may depend on, and what may still move

apitap reached v0.41.0 in five weeks. At that pace a version number stops
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
| destination artifacts | the `_apitap_state` table's columns, and the `__apitap_cl` / `__current` changelog shapes — a run of an older apitap must not choke on a newer one's state |
| exit behaviour | invalid input raises `ValueError`, a failed transfer raises `RuntimeError`, and a failed transfer never leaves the destination table changed |

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
apitap==0.41.0
```

Not `>=`. The surface above is committed, but the release cadence is fast enough
that you want to choose when you move, and the wheel is a compiled artifact — a
pin is also what makes a rollback one line.

## The road to 1.0

1.0 is not a feature list, it is a promise we can keep. What it waits on:

1. **The release gate runs itself.** Today a 25-leg suite against live Postgres,
   MySQL, MariaDB, ClickHouse, BigQuery, Iceberg and a 2-node ClickHouse cluster
   runs before every tag — but a human starts it. Until CI enforces it, the
   version number depends on someone remembering.
2. **Wheels for the platforms people actually evaluate on.** aarch64 and macOS,
   plus an sdist. Shipping only `manylinux_x86_64` while selling "cheap small
   boxes" is a contradiction, given ARM *is* the cheap box.
3. **Failure modes proven, not just claimed.** Atomic swap, the 0-row guard, the
   watermark committed with the data, the retention refusals — each demonstrated
   under kill-9, a dropped connection, a full disk, and schema change mid-run,
   with the leftover state and the recovery written down.
4. **A soak.** Days, not minutes: file descriptors, replication-slot growth,
   watermark drift. Those bugs only exist in duration.

Until those four are done, treat apitap as excellent at work you can re-run —
backfills, migrations, warehouse rebuilds, `read()` for analysis — and pin it if
it sits anywhere you cannot re-run.
