//! The `mode="log_based"` task runner (docs/design/log_based.md).
//!
//! Runs a GROUP of tables over ONE replication slot (a single table is a
//! group of one — its slot/state naming is unchanged from the single-table
//! era). First run (no state anywhere): create the slot with
//! EXPORT_SNAPSHOT, full-load every table pinned to that one snapshot
//! (gap-free AND duplicate-free for the whole group), store the slot's
//! consistent_point as each table's LSN watermark. Every later run: drain
//! the slot ONCE from the group's minimum watermark to a stop-line in
//! memory-bounded windows; each window applies per table together with the
//! watermark — and only after every member committed is Postgres told the
//! WAL may go. Recovery from a crash between two tables' commits is the
//! min-watermark re-drain: the apply paths are idempotent, so the tables
//! that were already ahead converge.

use crate::error::{Error, Result};
use crate::logbased::dest_ch::ChDest;
use crate::logbased::dest_ice::IceDest;
use crate::logbased::dest_my::MyDest;
use crate::logbased::dest_pg::{quote_ident, quote_table, PgDest};
use crate::logbased::drain::{drain, DrainOutcome, DrainSession};
use crate::wire::pgoutput::lsn_from_string;
use crate::wire::walsender::Walsender;
use crate::{Mode, MultiReport, TableResult, TransferOptions, TransferReport};
use md5::{Digest as _, Md5};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

/// One destination engine for the log_based apply path.
enum Dest {
    Pg(PgDest),
    Ch(ChDest),
    My(MyDest),
    Ice(IceDest),
}

impl Dest {
    async fn connect(dst_url: &str) -> Result<Dest> {
        match crate::pipeline::norm(scheme(dst_url)) {
            "postgres" => Ok(Dest::Pg(PgDest::connect(dst_url).await?)),
            "clickhouse" => Ok(Dest::Ch(ChDest::connect(dst_url)?)),
            "mysql" => Ok(Dest::My(MyDest::connect(dst_url)?)),
            "iceberg" => Ok(Dest::Ice(IceDest::connect(dst_url).await?)),
            other => Err(Error::InvalidInput(format!(
                "log_based: unsupported destination scheme '{other}' — use \
                 postgres, clickhouse, mysql or iceberg"
            ))),
        }
    }

    async fn read_state(&self, dest_table: &str, source_id: &str) -> Result<Option<u64>> {
        match self {
            Dest::Pg(d) => d.read_state(dest_table, source_id).await,
            Dest::Ch(d) => d.read_state(dest_table, source_id).await,
            Dest::My(d) => d.read_state(dest_table, source_id).await,
            Dest::Ice(d) => d.read_state(dest_table, source_id).await,
        }
    }

    /// Destination-specific knobs for the bootstrap's full load.
    fn tweak_bootstrap_opts(&self, o2: &mut TransferOptions, pk_cols: &[String]) {
        match self {
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) => {}
            Dest::Ch(d) => d.tweak_bootstrap_opts(o2, pk_cols),
        }
    }

    /// After the bootstrap's full load landed: add identity where the engine
    /// needs one, then write the state row.
    async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        match self {
            Dest::Pg(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
            Dest::Ch(d) => d.write_state(dest_table, source_id, lsn, rows).await,
            Dest::My(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
            Dest::Ice(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
        }
    }

    async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
        src: &PgPool,
    ) -> Result<u64> {
        match self {
            Dest::Pg(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ch(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::My(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ice(d) => {
                d.apply(dest_table, qualified_src, pk_cols, outcome, source_id, src).await
            }
        }
    }
}

/// One member of the slot group.
struct TableCtx {
    /// The table argument as the caller gave it (drives the bootstrap's
    /// recursive `transfer`).
    table_arg: String,
    /// Resolved "schema.table" on the source (matches pgoutput's Relation).
    qualified: String,
    dest_table: String,
    pk_cols: Vec<String>,
    /// Per-table state key — the same identity a single-table run would use,
    /// so state stays discoverable regardless of grouping.
    source_id: String,
}

/// Bytes of row data one drain window may buffer before it must apply.
/// Derived from the cgroup memory limit so CDC fits the same containers the
/// bulk paths fit (44 MB is the measured single-pipe floor). The runtime
/// baseline (interpreter + tokio + connection buffers) is reserved first;
/// the collapsed window's REAL footprint runs ~3× the byte counter (hash
/// map + Vec overhead) and the apply renders one body copy — hence /8 of
/// what remains. No cgroup limit = 256 MiB, still bounded on big boxes.
/// A single transaction always buffers whole regardless of the budget
/// (pgoutput v1 only ships a transaction after its commit).
fn window_budget() -> usize {
    const DEFAULT: usize = 256 << 20;
    const BASELINE: u64 = 24 << 20;
    match crate::pipeline::mem_limit_bytes() {
        Some(m) => ((m.saturating_sub(BASELINE) / 8) as usize).clamp(2 << 20, DEFAULT),
        None => DEFAULT,
    }
}

pub(crate) async fn run_task(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
) -> Result<TransferReport> {
    let started = std::time::Instant::now();
    let (rows, parallel) =
        run_group(src_url, dst_url, std::slice::from_ref(&table.to_string()), opts).await
            .map(|mut v| v.pop().expect("one result per table"))?;
    Ok(TransferReport {
        rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel,
    })
}

pub(crate) async fn run_many(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
) -> Result<MultiReport> {
    let started = std::time::Instant::now();
    if opts.dest_table.is_some() {
        return Err(Error::InvalidInput(
            "dest_table applies to single-table transfers — multi-table runs \
             keep the source names"
                .into(),
        ));
    }
    let per_table_started = std::time::Instant::now();
    let results = run_group(src_url, dst_url, tables, opts).await?;
    let elapsed = per_table_started.elapsed().as_millis() as u64;
    let budget = results.iter().map(|(_, p)| *p).max().unwrap_or(1);
    Ok(MultiReport {
        rows: results.iter().map(|(r, _)| *r).sum(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        budget,
        tables: tables
            .iter()
            .zip(results)
            .map(|(t, (rows, parallel))| TableResult {
                table: t.clone(),
                rows,
                elapsed_ms: elapsed,
                parallel,
                error: None,
            })
            .collect(),
    })
}

/// Run one slot group. Returns (rows, parallel) per table, caller order.
/// A failure anywhere fails the WHOLE group — the slot is only confirmed
/// past windows every member committed, so nothing is ever lost.
async fn run_group(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
) -> Result<Vec<(u64, usize)>> {
    if tables.is_empty() {
        return Err(Error::InvalidInput("tables list is empty".into()));
    }
    if !matches!(scheme(src_url), "postgres" | "postgresql") {
        return Err(Error::InvalidInput(
            "log_based needs a Postgres source (logical replication)".into(),
        ));
    }
    let dest = Dest::connect(dst_url).await?;

    let src = PgPoolOptions::new()
        .max_connections(2)
        .connect(src_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: source connect: {e}")))?;

    // Resolve every member: real (schema, name) + PK on the source.
    let single = tables.len() == 1;
    let mut ctxs = Vec::with_capacity(tables.len());
    for t in tables {
        let (schema_name, bare) = resolve_table(&src, t).await?;
        let qualified = format!("{schema_name}.{bare}");
        let pk_cols = pk_columns(&src, &qualified).await?;
        if pk_cols.is_empty() {
            return Err(Error::InvalidInput(format!(
                "log_based: {qualified} has no primary key — updates/deletes \
                 need an identity. Add a PK (or ask for REPLICA IDENTITY FULL \
                 support)"
            )));
        }
        let dest_table = if single {
            opts.dest_table.clone().unwrap_or_else(|| bare.clone())
        } else {
            bare.clone()
        };
        ctxs.push(TableCtx {
            table_arg: t.clone(),
            qualified,
            dest_table,
            source_id: crate::pipeline::source_identity(src_url, t),
            pk_cols,
        });
    }
    {
        let mut dests: Vec<&str> = ctxs.iter().map(|c| c.dest_table.as_str()).collect();
        dests.sort_unstable();
        dests.dedup();
        if dests.len() != ctxs.len() {
            return Err(Error::InvalidInput(
                "log_based: two tables resolve to the same destination name".into(),
            ));
        }
    }

    // Stable slot/publication names. A group of ONE keeps the historical
    // single-table naming so existing slots stay owned; bigger groups hash
    // their sorted membership — changing membership means a NEW slot (and a
    // loud partial-state error below until the old state is cleared).
    let slot = if single {
        let c = &ctxs[0];
        format!("apitap_{}", hex_prefix(&format!("{}\u{1f}{}", c.source_id, c.dest_table), 12))
    } else {
        let mut pairs: Vec<String> = ctxs
            .iter()
            .map(|c| format!("{}\u{1f}{}", c.source_id, c.dest_table))
            .collect();
        pairs.sort_unstable();
        format!("apitap_g{}", hex_prefix(&pairs.join("\u{1e}"), 11))
    };
    let publication = format!("{slot}_pub");

    let qualified_all: Vec<&str> = ctxs.iter().map(|c| c.qualified.as_str()).collect();
    ensure_publication(&src, &publication, &qualified_all).await?;

    // Per-table watermarks: all absent = fresh bootstrap; all present = drain
    // from the group minimum; a mix is a torn group — refuse loudly.
    let mut wms = Vec::with_capacity(ctxs.len());
    for c in &ctxs {
        wms.push(dest.read_state(&c.dest_table, &c.source_id).await?);
    }
    let have: Vec<&TableCtx> =
        ctxs.iter().zip(&wms).filter(|(_, w)| w.is_some()).map(|(c, _)| c).collect();
    if !have.is_empty() && have.len() != ctxs.len() {
        let missing: Vec<&str> = ctxs
            .iter()
            .zip(&wms)
            .filter(|(_, w)| w.is_none())
            .map(|(c, _)| c.dest_table.as_str())
            .collect();
        return Err(Error::InvalidInput(format!(
            "log_based: the group has state for {} of {} tables (missing: {}) — \
             group membership changed, or a bootstrap was interrupted. Clear \
             the group's state rows (and drop slot {slot}) to re-bootstrap",
            have.len(),
            ctxs.len(),
            missing.join(", ")
        )));
    }

    if have.is_empty() {
        bootstrap_group(src_url, dst_url, opts, &dest, &src, &slot, &ctxs).await
    } else {
        let wm = wms.iter().map(|w| w.expect("all present")).min().expect("nonempty");
        drain_group(src_url, &src, dest, &slot, &publication, &ctxs, wm).await
    }
}

// ── first run: one slot, every table pinned to its snapshot ─────────────────

async fn bootstrap_group(
    src_url: &str,
    dst_url: &str,
    opts: &TransferOptions,
    dest: &Dest,
    src: &PgPool,
    slot: &str,
    ctxs: &[TableCtx],
) -> Result<Vec<(u64, usize)>> {
    // A slot with no matching state is a leftover from an aborted bootstrap —
    // start fresh (refuse if something is actively draining it).
    let stale: Option<(bool,)> =
        sqlx::query_as("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot)
            .fetch_optional(src)
            .await
            .map_err(db_err)?;
    if let Some((active,)) = stale {
        if active {
            return Err(Error::Transfer(format!(
                "log_based: slot {slot} is ACTIVE but the destination has no \
                 state — another process is using it; stop it first"
            )));
        }
        sqlx::query("SELECT pg_drop_replication_slot($1)")
            .bind(slot)
            .execute(src)
            .await
            .map_err(db_err)?;
    }

    let mut ws = Walsender::connect(src_url).await?;
    let rows = ws
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT \"{slot}\" LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let consistent_point = rows
        .first()
        .and_then(|r| r.get(1).cloned().flatten())
        .ok_or_else(|| Error::Transfer("log_based: slot creation returned no LSN".into()))?;
    let snapshot = rows
        .first()
        .and_then(|r| r.get(2).cloned().flatten())
        .ok_or_else(|| Error::Transfer("log_based: slot creation exported no snapshot".into()))?;
    let lsn = lsn_from_string(&consistent_point)?;

    // Full loads pinned to the ONE exported snapshot — every member sees the
    // same instant, so the whole group hands off gap-free. Sequential: each
    // table still gets the full pipe budget, and per-table destination knobs
    // (ClickHouse ORDER BY = that table's PK) stay per-table. The walsender
    // session stays open for the whole load (idle; the slot retains WAL).
    let sep = if src_url.contains('?') { '&' } else { '?' };
    let pinned_url = format!("{src_url}{sep}__apitap_snapshot={snapshot}");
    let mut out = Vec::with_capacity(ctxs.len());
    for c in ctxs {
        let mut o2 = opts.clone();
        o2.mode = Mode::Replace;
        o2.dest_table = Some(c.dest_table.clone());
        dest.tweak_bootstrap_opts(&mut o2, &c.pk_cols);
        let report =
            match Box::pin(crate::transfer(&pinned_url, dst_url, &c.table_arg, &o2)).await {
                Ok(r) => r,
                Err(e) => {
                    // Failed bootstrap leaves no STATE behind: drop the slot;
                    // already-loaded members are plain tables the re-run will
                    // replace again.
                    let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
                        .bind(slot)
                        .execute(src)
                        .await;
                    return Err(Error::Transfer(format!(
                        "log_based: bootstrap of {} failed (group rolled back to \
                         no-state; re-run re-bootstraps all): {e}",
                        c.qualified
                    )));
                }
            };
        out.push((report.rows, report.parallel));
    }
    ws.stop_replication().await.ok();

    for (c, (rows, _)) in ctxs.iter().zip(&out) {
        dest.bootstrap_finish(&c.dest_table, &c.source_id, &c.pk_cols, lsn, *rows).await?;
    }
    Ok(out)
}

// ── every later run: ONE windowed drain, applies fan out per table ──────────

async fn drain_group(
    src_url: &str,
    src: &PgPool,
    dest: Dest,
    slot: &str,
    publication: &str,
    ctxs: &[TableCtx],
    wm: u64,
) -> Result<Vec<(u64, usize)>> {
    // Reconcile against the slot before doing anything.
    let slot_row: Option<(Option<String>, bool)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text, active FROM pg_replication_slots \
         WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_optional(src)
    .await
    .map_err(db_err)?;
    let Some((confirmed, active)) = slot_row else {
        return Err(Error::Transfer(format!(
            "log_based: destination has a watermark but slot {slot} is GONE on \
             the source — WAL continuity is lost. Re-run after clearing the \
             state row (the next run re-bootstraps with a full load)"
        )));
    };
    if active {
        return Err(Error::Transfer(format!(
            "log_based: slot {slot} is already active — another drain is running"
        )));
    }
    if let Some(c) = confirmed {
        let c = lsn_from_string(&c)?;
        if wm < c {
            return Err(Error::Transfer(format!(
                "log_based: destination watermark {wm} is BEHIND the slot's \
                 confirmed LSN {c} — that WAL is gone; state was tampered with \
                 or restored from backup. Clear the state row to re-bootstrap"
            )));
        }
    }

    let stop_line: (String,) = sqlx::query_as("SELECT pg_current_wal_lsn()::text")
        .fetch_one(src)
        .await
        .map_err(db_err)?;
    let stop_line = lsn_from_string(&stop_line.0)?;

    let mut key_cols = HashMap::new();
    for c in ctxs {
        key_cols.insert(c.qualified.clone(), c.pk_cols.clone());
    }

    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    // Two windows are resident under overlap (one applying, one draining) —
    // the budget halves so peak memory stays at the single-window ceiling.
    let budget = (window_budget() / 2).max(1 << 20);
    let mut ws = Walsender::connect(src_url).await?;
    ws.start_replication(slot, wm, publication).await?;

    // Overlapped windows (ape-dts's daemon trick, batch-shaped): the drain
    // task keeps the walsender and decodes window N+1 WHILE a spawned apply
    // task lands window N. The slot is confirmed only after the apply task
    // reports a window fully committed (watch channel carries the last
    // committed end_lsn back) — never past unapplied WAL, exactly like the
    // serial loop, just off the clock.
    let (win_tx, mut win_rx) = tokio::sync::mpsc::channel::<DrainOutcome>(1);
    let (applied_tx, mut applied_rx) = tokio::sync::watch::channel::<u64>(wm);
    let actxs: Vec<(String, String, Vec<String>, String)> = ctxs
        .iter()
        .map(|c| {
            (c.dest_table.clone(), c.qualified.clone(), c.pk_cols.clone(), c.source_id.clone())
        })
        .collect();
    let apool = src.clone();
    let apply_task: tokio::task::JoinHandle<Result<Vec<u64>>> = tokio::spawn(async move {
        let mut rows_per = vec![0u64; actxs.len()];
        while let Some(o) = win_rx.recv().await {
            let t_apply = std::time::Instant::now();
            for (i, (dest_table, qualified, pk_cols, source_id)) in actxs.iter().enumerate() {
                rows_per[i] +=
                    dest.apply(dest_table, qualified, pk_cols, &o, source_id, &apool).await?;
            }
            if std::env::var("APITAP_DEBUG").is_ok() {
                let events: u64 = o.tables.values().map(|c| c.events).sum();
                eprintln!(
                    "[log_based] applied lsn={} events={events} in {:.1}s",
                    o.end_lsn,
                    t_apply.elapsed().as_secs_f64(),
                );
            }
            // Receiver may be gone on a drain-side abort — nothing to do.
            let _ = applied_tx.send(o.end_lsn);
        }
        Ok(rows_per)
    });

    let mut sess = DrainSession::default();
    let mut cur = wm;
    let mut windows = 0u32;
    // The previous window's end_lsn: sent to the applier, not yet confirmed.
    let mut pending: Option<u64> = None;
    let mut drain_err: Option<Error> = None;
    loop {
        let t_drain = std::time::Instant::now();
        let outcome = match drain(
            &mut ws, &mut sess, cur, stop_line, &key_cols, 3600, budget, &applied_rx,
        )
        .await
        {
            Ok(o) => o,
            Err(e) => {
                drain_err = Some(e);
                break;
            }
        };
        windows += 1;
        if dbg {
            let events: u64 = outcome.tables.values().map(|c| c.events).sum();
            eprintln!(
                "[log_based] window={windows} tables={} drain={:.1}s events={events} \
                 budget_hit={}",
                outcome.tables.len(),
                t_drain.elapsed().as_secs_f64(),
                outcome.hit_budget,
            );
        }
        let end = outcome.end_lsn;
        let hit = outcome.hit_budget;
        if end > cur {
            if win_tx.send(outcome).await.is_err() {
                // Apply task died — its JoinHandle carries the real error.
                break;
            }
        }
        // Confirm the PREVIOUS window once applied (bounds resident windows
        // to two and keeps the slot's confirmed LSN strictly behind commits).
        if let Some(p) = pending.take() {
            if applied_rx.wait_for(|&a| a >= p).await.is_err() {
                break;
            }
            ws.standby_status(p, false).await?;
        }
        if end > cur {
            pending = Some(end);
            cur = end;
        }
        if !hit {
            break;
        }
    }
    drop(win_tx);
    // Wait for the final in-flight window, confirm, then collect the applier.
    if drain_err.is_none() {
        if let Some(p) = pending {
            if applied_rx.wait_for(|&a| a >= p).await.is_ok() {
                ws.standby_status(p, false).await?;
            }
        }
    }
    let joined = apply_task.await;
    ws.stop_replication().await.ok();
    if let Some(e) = drain_err {
        return Err(e);
    }
    let rows_per = match joined {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err(e),
        Err(j) => {
            return Err(Error::Transfer(format!("log_based: apply task panicked: {j}")))
        }
    };

    Ok(rows_per.into_iter().map(|r| (r, 1)).collect())
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn scheme(url: &str) -> &str {
    url.split("://").next().unwrap_or("")
}

fn hex_prefix(s: &str, n: usize) -> String {
    hex::encode(Md5::digest(s))[..n].to_string()
}

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based: {e}"))
}

async fn resolve_table(src: &PgPool, table: &str) -> Result<(String, String)> {
    let row: (String, String) = sqlx::query_as(
        "SELECT n.nspname, c.relname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::regclass",
    )
    .bind(table)
    .fetch_one(src)
    .await
    .map_err(|e| Error::InvalidInput(format!("log_based: table '{table}': {e}")))?;
    Ok(row)
}

async fn pk_columns(src: &PgPool, qualified: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT a.attname FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = $1::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey, a.attnum)",
    )
    .bind(qualified)
    .fetch_all(src)
    .await
    .map_err(db_err)?;
    Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
}

/// Create the publication carrying EVERY member, or — when it already
/// exists — verify each member's MEMBERSHIP and re-add the missing (a
/// dropped-and-recreated source table silently leaves its publication).
async fn ensure_publication(
    src: &PgPool,
    publication: &str,
    qualified_tables: &[&str],
) -> Result<()> {
    let exists: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM pg_publication WHERE pubname = $1")
            .bind(publication)
            .fetch_optional(src)
            .await
            .map_err(db_err)?;
    if exists.is_none() {
        let list = qualified_tables
            .iter()
            .map(|q| format!("ONLY {}", quote_table(q)))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!(
            "CREATE PUBLICATION {} FOR TABLE {list}",
            quote_ident(publication)
        ))
        .execute(src)
        .await
        .map_err(|e| {
            Error::Transfer(format!(
                "log_based: CREATE PUBLICATION failed (needs table ownership or \
                 superuser): {e}"
            ))
        })?;
        return Ok(());
    }
    for qualified in qualified_tables {
        let (schema, bare) = qualified.split_once('.').unwrap_or(("public", qualified));
        let member: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
        )
        .bind(publication)
        .bind(schema)
        .bind(bare)
        .fetch_optional(src)
        .await
        .map_err(db_err)?;
        if member.is_none() {
            sqlx::query(&format!(
                "ALTER PUBLICATION {} ADD TABLE ONLY {}",
                quote_ident(publication),
                quote_table(qualified)
            ))
            .execute(src)
            .await
            .map_err(|e| {
                Error::Transfer(format!(
                    "log_based: publication {publication} exists but no longer \
                     carries {qualified} (source table dropped and recreated?) — \
                     re-adding it failed: {e}"
                ))
            })?;
        }
    }
    Ok(())
}
