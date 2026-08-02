//! The `mode="log_based"` task runner (docs/design/log_based.md).
//!
//! First run (no state): create the slot with EXPORT_SNAPSHOT, full-load the
//! table pinned to that snapshot (gap-free AND duplicate-free), store the
//! slot's consistent_point as the LSN watermark. Every later run: drain the
//! slot from the watermark to a stop-line in MEMORY-BOUNDED windows; each
//! window applies set-based at the destination together with the watermark —
//! and only then is Postgres told the WAL may go.
//!
//! Source is always Postgres (logical replication). Destinations dispatch
//! through [`Dest`]: Postgres applies in one transaction; ClickHouse has no
//! transactions, so its window apply is idempotent instead (state row last).

use crate::error::{Error, Result};
use crate::logbased::dest_ch::ChDest;
use crate::logbased::dest_ice::IceDest;
use crate::logbased::dest_my::MyDest;
use crate::logbased::dest_pg::{quote_ident, quote_table, PgDest};
use crate::logbased::drain::{drain, DrainOutcome, DrainSession};
use crate::wire::pgoutput::lsn_from_string;
use crate::wire::walsender::Walsender;
use crate::{Mode, TransferOptions, TransferReport};
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

    /// `src` is the SOURCE pool — iceberg refetches masked-TOAST rows from
    /// it (immutable snapshots have no destination row to read back); the
    /// SQL destinations ignore it.
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

    // Resolve the table's real (schema, name) + PK + column order on the source.
    let (schema_name, bare) = resolve_table(&src, table).await?;
    let qualified = format!("{schema_name}.{bare}");
    let pk_cols = pk_columns(&src, &qualified).await?;
    if pk_cols.is_empty() {
        return Err(Error::InvalidInput(format!(
            "log_based: {qualified} has no primary key — updates/deletes need an \
             identity. Add a PK (or ask for REPLICA IDENTITY FULL support)"
        )));
    }
    let dest_table = opts.dest_table.clone().unwrap_or_else(|| bare.clone());
    let source_id = crate::pipeline::source_identity(src_url, table);

    // Stable slot/publication names for this sync pair.
    let pair_hash = hex_prefix(&format!("{source_id}\u{1f}{dest_table}"), 12);
    let slot = format!("apitap_{pair_hash}");
    let publication = format!("{slot}_pub");

    ensure_publication(&src, &publication, &qualified).await?;
    let wm = dest.read_state(&dest_table, &source_id).await?;

    match wm {
        None => {
            bootstrap(
                src_url, dst_url, table, opts, &dest, &src, &slot, &dest_table,
                &source_id, &pk_cols, started,
            )
            .await
        }
        Some(wm) => {
            drain_run(
                src_url, &src, &dest, &slot, &publication, &qualified, &pk_cols,
                &dest_table, &source_id, wm, started,
            )
            .await
        }
    }
}

// ── first run: slot + pinned full load ──────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn bootstrap(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
    dest: &Dest,
    src: &PgPool,
    slot: &str,
    dest_table: &str,
    source_id: &str,
    pk_cols: &[String],
    started: std::time::Instant,
) -> Result<TransferReport> {
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

    // Full load pinned to the exported snapshot. The walsender session must
    // stay open until the load's workers have attached the snapshot — we
    // keep it open for the whole load (it is idle; the slot retains WAL).
    let mut o2 = opts.clone();
    o2.mode = Mode::Replace;
    dest.tweak_bootstrap_opts(&mut o2, pk_cols);
    let sep = if src_url.contains('?') { '&' } else { '?' };
    let pinned_url = format!("{src_url}{sep}__apitap_snapshot={snapshot}");
    let report = match Box::pin(crate::transfer(&pinned_url, dst_url, table, &o2)).await {
        Ok(r) => r,
        Err(e) => {
            // Failed bootstrap leaves nothing behind: drop the slot.
            let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
                .bind(slot)
                .execute(src)
                .await;
            return Err(e);
        }
    };
    ws.stop_replication().await.ok();

    dest.bootstrap_finish(dest_table, source_id, pk_cols, lsn, report.rows).await?;

    Ok(TransferReport {
        rows: report.rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: report.parallel,
    })
}

// ── every later run: windowed drain + set-based apply ───────────────────────

#[allow(clippy::too_many_arguments)]
async fn drain_run(
    src_url: &str,
    src: &PgPool,
    dest: &Dest,
    slot: &str,
    publication: &str,
    qualified: &str,
    pk_cols: &[String],
    dest_table: &str,
    source_id: &str,
    wm: u64,
    started: std::time::Instant,
) -> Result<TransferReport> {
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
    key_cols.insert(qualified.to_string(), pk_cols.to_vec());

    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    let budget = window_budget();
    let mut ws = Walsender::connect(src_url).await?;
    ws.start_replication(slot, wm, publication).await?;

    // Windowed drain: each window applies (with its watermark) before the
    // next one buffers, so peak memory is the window budget — not the lag.
    // The session carries the Relation registry across windows (pgoutput
    // announces a relation once per stream).
    let mut sess = DrainSession::default();
    let mut cur = wm;
    let mut total_rows = 0u64;
    let mut windows = 0u32;
    loop {
        let t_drain = std::time::Instant::now();
        let outcome =
            drain(&mut ws, &mut sess, cur, stop_line, &key_cols, 3600, budget).await?;
        let t_drain = t_drain.elapsed();

        let t_apply = std::time::Instant::now();
        let (rows, applied_lsn) = if outcome.end_lsn > cur {
            let n = dest
                .apply(dest_table, qualified, pk_cols, &outcome, source_id, src)
                .await?;
            (n, outcome.end_lsn)
        } else {
            (0, cur)
        };
        total_rows += rows;
        windows += 1;
        if dbg {
            let c = outcome.tables.get(qualified);
            eprintln!(
                "[log_based] window={windows} drain={:.1}s apply={:.1}s events={} \
                 deletes={} upserts={} residue={} budget_hit={}",
                t_drain.as_secs_f64(),
                t_apply.elapsed().as_secs_f64(),
                rows,
                c.map_or(0, |c| c.deletes.len()),
                c.map_or(0, |c| c.upserts.len()),
                c.map_or(0, |c| c.residue.len()),
                outcome.hit_budget,
            );
        }

        // Destination committed — NOW the source may discard this window's WAL.
        ws.standby_status(applied_lsn, false).await?;
        cur = applied_lsn;
        if !outcome.hit_budget {
            break;
        }
    }
    ws.stop_replication().await.ok();

    Ok(TransferReport {
        rows: total_rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: 1,
    })
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

async fn ensure_publication(src: &PgPool, publication: &str, qualified: &str) -> Result<()> {
    let exists: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM pg_publication WHERE pubname = $1")
            .bind(publication)
            .fetch_optional(src)
            .await
            .map_err(db_err)?;
    if exists.is_none() {
        sqlx::query(&format!(
            "CREATE PUBLICATION {} FOR TABLE ONLY {}",
            quote_ident(publication),
            quote_table(qualified)
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
    // The publication existing is NOT enough: dropping and recreating the
    // source table silently empties it, and an empty publication streams
    // Begin/Commit pairs and no rows. Verify membership and re-add.
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
    Ok(())
}
