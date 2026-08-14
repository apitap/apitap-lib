# `changelog=True` vs replica: what append-only CDC costs (2026-08-14)

Two questions, one rig: does turning the destination into an append-only
changelog cost throughput, and where does the recorded **132–135K changes/s**
headline actually come from?

Everything below runs the CDC side in **0.5 CPU / 256 MB** (`--cpus=0.5
--memory=256m --memory-swap=256m`), writer unconstrained on the host, on the
OVH bench VPS. Every leg is verified after the drain: `count` and
`sum(cust_id)` from the source against the destination — the changelog legs
against `<table>__current`, so the two modes are held to the *same* answer, not
just to their own.

## The rig

5 columns — `id bigint PK, cust_id int, payload text, amount numeric(12,2), ts
timestamptz` (MySQL: `DATETIME`) — 500,000 rows, and a backlog of **500,000
UPDATEs committed in 200 transactions of 2,500 rows**. Same table, same
backlog, both modes, both capture planes. Scripts: `znarrow.sh` +
`caged_narrow.py` shape, reproduced in this directory's harness.

100% UPDATEs is the *expensive* end of the mix on purpose: an update carries a
before-image and an after-image, so it is the event type that costs the most
per change on both planes.

| lane | mode | wall | changes/s | CPU (% of quota) | peak RSS |
|---|---|---|---|---|---|
| Postgres → ClickHouse | replica | 9.8 s | **51,134** | 4.8 s (98%) | 61 MB |
| Postgres → ClickHouse | changelog | 9.9 s | **50,576** | 4.8 s (98%) | 58 MB |
| MySQL → ClickHouse | replica | 5.9 s | **84,630** | 2.7 s (92%) | 66 MB |
| MySQL → ClickHouse | changelog | 4.4 s | **113,801** | 1.9 s (84%) | 46 MB |

All four legs verified `500000|25000250000` against their source.

## What the numbers say

**Changelog is free on Postgres and a 34% win on MySQL.** The mode removes real
work — no collapse pass, no per-window key delete, one plain INSERT — but that
work only shows up as speed when something else isn't already the wall. The
Postgres lane sits at **98% of its CPU quota** in both modes: it is saturated
decoding WAL, so an apply-side saving has nowhere to land. The MySQL lane
decodes a binlog that carries only row images, leaves ~8% of the quota unused in
replica mode, and converts the saved apply work straight into wall time —
113,801 vs 84,630 changes/s, at **46 MB instead of 66 MB**.

**What this rig can and cannot say.** 500,000 UPDATEs over 500,000 rows is ONE
update per key — the distribution in which the replica path's collapser folds
N events to N rows and therefore saves nothing. That is the fair worst case for
changelog (the rival mode's main optimization has nothing to work with) and the
fair worst case for the replica too, so the tie is real; but it is NOT evidence
about a skewed window. Where many events hit the SAME key inside one window, the
replica collapses them to one row and the changelog writes all of them, so the
changelog does more destination work by design — that is what "keeps the
history" costs. The rule this rig supports is the narrower one:

> **On one-update-per-key traffic changelog is free, and it pays wherever the
> capture plane is not already the wall.** A skewed window trades throughput for
> the history it keeps; that shape has not been measured here.

## Where "132K changes/s" came from

The README and `docs/vs.md` headline **34–132K changes/s** and attribute the
top of the range to "5-column rows". The receipt behind it is round 3
(`ch-ingest-r3.md`): **cdc-my 11.9 s → ~7.4 s for 1M changes ≈ 135K
changes/s** — the **MySQL** lane, on a mix that round-3's own notes describe as
having *no update-heavy my workload*. The Postgres number in that same round is
**~36K changes/s** on wide 15-column rows.

Set against the two ledgers with full receipts, the range is consistent and the
attribution was not:

| rig | Postgres → CH | MySQL → CH |
|---|---|---|
| `cdc-stress.md` — 10 tables, 80M changes total | 51–56K/s | 82–86K/s |
| this rig — 5 columns, 100% UPDATEs, replica | 51,134/s | 84,630/s |
| this rig — 5 columns, 100% UPDATEs, changelog | 50,576/s | **113,801/s** |
| `ch-ingest-r3.md` — 1M changes, insert-heavy | ~36K/s (15 wide cols) | ~135K/s |

The 51K Postgres figure reproduces the 80M-change stress ledger almost exactly,
three separate rigs apart. **132K was never a Postgres number**, and it was
never an update-heavy one; it is the MySQL binlog lane on a friendlier mix. The
top of the published range now belongs to a leg that is both reproduced and
verified: MySQL → ClickHouse changelog at **113.8K changes/s on
update-only traffic**, with 135K standing for insert-heavy mixes.

## Reproducing

```bash
IMG=apitap-narrow:<tag> bash znarrow.sh      # 4 legs, each verified
```

Both capture planes' correctness (every op captured, PK-changing update as
`D`-then-`U`, `__current` == source, empty drain appends nothing, both shape
guards) is covered by `e2e_changelog_ch.py` (Postgres), `e2e_changelog_my.py`
(MySQL binlog) and `e2e_changelog_bq.py` (BigQuery);
`e2e_changelog_group.py` covers a multi-table group on one slot for both
analytical destinations.

## What these numbers do NOT include

The measured rig has no TOASTed columns. An UPDATE that leaves an out-of-line
column untouched omits it from the WAL, and a changelog cannot store that hole
as NULL without blanking the column for every reader of `__current` — so the
apply reconstructs the value, from the window itself where possible and
otherwise from one readback per window against `__current`. Windows that
actually carry such a column therefore pay one extra destination query; windows
that do not (every leg above) pay nothing, which is why the flag that triggers
it is set during capture rather than probed at apply time.
