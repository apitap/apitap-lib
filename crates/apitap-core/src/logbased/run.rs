//! The `mode="log_based"` task runner (docs/design/log_based.md).
//!
//! First run (no state): create the slot with EXPORT_SNAPSHOT, full-load the
//! table pinned to that snapshot (gap-free AND duplicate-free), store the
//! slot's consistent_point as the LSN watermark. Every later run: drain the
//! slot from the watermark to a stop-line, collapse, apply set-based in ONE
//! destination transaction that also advances the watermark — and only then
//! tell Postgres the WAL may go.

use crate::error::{Error, Result};
use crate::logbased::collapse::{Collapsed, ResidueOp};
use crate::logbased::drain::{drain, DrainOutcome};
use crate::wire::pgoutput::{lsn_from_string, Cell};
use crate::wire::walsender::Walsender;
use crate::{Mode, TransferOptions, TransferReport};
use md5::{Digest as _, Md5};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool, Row};
use std::collections::HashMap;

const STATE_CURSOR: &str = "_lsn";

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
    if !matches!(scheme(dst_url), "postgres" | "postgresql") {
        return Err(Error::InvalidInput(
            "log_based supports Postgres destinations first — mysql and iceberg \
             land next"
                .into(),
        ));
    }

    let src = PgPoolOptions::new()
        .max_connections(2)
        .connect(src_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: source connect: {e}")))?;
    let dst = PgPoolOptions::new()
        .max_connections(2)
        .connect(dst_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: dest connect: {e}")))?;

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
    let wm = read_state(&dst, &dest_table, &source_id).await?;

    match wm {
        None => bootstrap(src_url, dst_url, table, opts, &dst, &src, &slot, &publication, &dest_table, &source_id, started).await,
        Some(wm) => {
            drain_run(
                src_url, &src, &dst, &slot, &publication, &qualified, &pk_cols,
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
    dst: &PgPool,
    src: &PgPool,
    slot: &str,
    publication: &str,
    dest_table: &str,
    source_id: &str,
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

    ensure_state_table(dst).await?;
    write_state(dst, dest_table, source_id, lsn, report.rows).await?;
    let _ = publication; // already ensured by the caller

    Ok(TransferReport {
        rows: report.rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: report.parallel,
    })
}

// ── every later run: drain + set-based apply ────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn drain_run(
    src_url: &str,
    src: &PgPool,
    dst: &PgPool,
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

    let mut ws = Walsender::connect(src_url).await?;
    ws.start_replication(slot, wm, publication).await?;
    let outcome = drain(&mut ws, wm, stop_line, &key_cols, 3600).await?;

    let (rows, applied_lsn) = if outcome.end_lsn > wm {
        let n = apply_pg(dst, dest_table, qualified, pk_cols, &outcome, source_id).await?;
        (n, outcome.end_lsn)
    } else {
        (0, wm)
    };

    // Destination committed — NOW the source may discard WAL.
    ws.standby_status(applied_lsn, false).await?;
    ws.stop_replication().await.ok();

    Ok(TransferReport {
        rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: 1,
    })
}

/// Apply one collapsed window for one table in ONE destination transaction
/// (truncate → deletes → upserts → residue → watermark).
async fn apply_pg(
    dst: &PgPool,
    dest_table: &str,
    qualified_src: &str,
    pk_cols: &[String],
    outcome: &DrainOutcome,
    source_id: &str,
) -> Result<u64> {
    let Some(c) = outcome.tables.get(qualified_src) else {
        // Foreign-table traffic only: nothing for our table, still advance.
        ensure_state_table(dst).await?;
        let mut tx = dst.begin().await.map_err(db_err)?;
        upsert_state_tx(&mut tx, dest_table, source_id, outcome.end_lsn, 0).await?;
        tx.commit().await.map_err(db_err)?;
        return Ok(0);
    };
    let wal_cols = outcome
        .wal_cols
        .get(qualified_src)
        .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;

    ensure_state_table(dst).await?;
    let ft = quote_table(dest_table);
    let collist = wal_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let pklist = pk_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");

    let mut tx = dst.begin().await.map_err(db_err)?;

    if c.truncate {
        tx.execute(format!("TRUNCATE {ft}").as_str()).await.map_err(db_err)?;
    }

    // Delete phase (before upserts — the collapse counts on this order).
    if !c.deletes.is_empty() {
        tx.execute(
            format!(
                "CREATE TEMP TABLE _ap_del ON COMMIT DROP AS \
                 SELECT {pklist} FROM {ft} WHERE false"
            )
            .as_str(),
        )
        .await
        .map_err(db_err)?;
        let mut copy = tx
            .copy_in_raw(&format!("COPY _ap_del ({pklist}) FROM STDIN"))
            .await
            .map_err(db_err)?;
        let mut buf = Vec::with_capacity(1 << 20);
        for key in &c.deletes {
            render_copy_row_keys(key, &mut buf);
            if buf.len() > 4 << 20 {
                copy.send(std::mem::take(&mut buf)).await.map_err(db_err)?;
            }
        }
        if !buf.is_empty() {
            copy.send(buf).await.map_err(db_err)?;
        }
        copy.finish().await.map_err(db_err)?;
        let join = pk_cols
            .iter()
            .map(|k| format!("{ft}.{k} = _ap_del.{k}", k = quote_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        tx.execute(format!("DELETE FROM {ft} USING _ap_del WHERE {join}").as_str())
            .await
            .map_err(db_err)?;
    }

    // Upsert phase: COPY into a temp twin, then one INSERT … ON CONFLICT.
    if !c.upserts.is_empty() {
        tx.execute(
            format!(
                "CREATE TEMP TABLE _ap_up ON COMMIT DROP AS \
                 SELECT {collist} FROM {ft} WHERE false"
            )
            .as_str(),
        )
        .await
        .map_err(db_err)?;
        let mut copy = tx
            .copy_in_raw(&format!("COPY _ap_up ({collist}) FROM STDIN"))
            .await
            .map_err(db_err)?;
        let mut buf = Vec::with_capacity(4 << 20);
        for row in &c.upserts {
            render_copy_row(row, &mut buf)?;
            if buf.len() > 4 << 20 {
                copy.send(std::mem::take(&mut buf)).await.map_err(db_err)?;
            }
        }
        if !buf.is_empty() {
            copy.send(buf).await.map_err(db_err)?;
        }
        copy.finish().await.map_err(db_err)?;
        let updates = wal_cols
            .iter()
            .filter(|cname| !pk_cols.contains(cname))
            .map(|cname| format!("{q} = EXCLUDED.{q}", q = quote_ident(cname)))
            .collect::<Vec<_>>()
            .join(", ");
        let action = if updates.is_empty() {
            "DO NOTHING".to_string()
        } else {
            format!("DO UPDATE SET {updates}")
        };
        tx.execute(
            format!(
                "INSERT INTO {ft} ({collist}) SELECT {collist} FROM _ap_up \
                 ON CONFLICT ({pklist}) {action}"
            )
            .as_str(),
        )
        .await
        .map_err(db_err)?;
    }

    // Residue tail: serial, ordered (masked updates and their followers).
    for op in &c.residue {
        let sql = match op {
            ResidueOp::MaskedUpdate { key, row } => {
                let sets = wal_cols
                    .iter()
                    .zip(row.iter())
                    .filter(|(cname, cell)| {
                        !matches!(cell, Cell::UnchangedToast) && !pk_cols.contains(cname)
                    })
                    .map(|(cname, cell)| {
                        format!("{} = {}", quote_ident(cname), cell_literal(cell))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if sets.is_empty() {
                    continue;
                }
                format!("UPDATE {ft} SET {sets} WHERE {}", key_pred(pk_cols, key))
            }
            ResidueOp::Upsert { row } => {
                let vals = row.iter().map(cell_literal).collect::<Vec<_>>().join(", ");
                let updates = wal_cols
                    .iter()
                    .filter(|cname| !pk_cols.contains(cname))
                    .map(|cname| format!("{q} = EXCLUDED.{q}", q = quote_ident(cname)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let action = if updates.is_empty() {
                    "DO NOTHING".to_string()
                } else {
                    format!("DO UPDATE SET {updates}")
                };
                format!(
                    "INSERT INTO {ft} ({collist}) VALUES ({vals}) \
                     ON CONFLICT ({pklist}) {action}"
                )
            }
            ResidueOp::Delete { key } => {
                format!("DELETE FROM {ft} WHERE {}", key_pred(pk_cols, key))
            }
        };
        tx.execute(sql.as_str()).await.map_err(db_err)?;
    }

    upsert_state_tx(&mut tx, dest_table, source_id, outcome.end_lsn, c.events).await?;
    tx.commit().await.map_err(db_err)?;
    Ok(c.events)
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

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_table(t: &str) -> String {
    t.split('.').map(quote_ident).collect::<Vec<_>>().join(".")
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
    }
    Ok(())
}

async fn ensure_state_table(dst: &PgPool) -> Result<()> {
    dst.execute(
        "CREATE TABLE IF NOT EXISTS _apitap_state (\
           dest_table  text NOT NULL, \
           source_id   text NOT NULL, \
           cursor_col  text NOT NULL, \
           watermark   text, \
           mode        text NOT NULL, \
           last_rows   bigint NOT NULL DEFAULT 0, \
           synced_at   timestamptz NOT NULL DEFAULT now(), \
           PRIMARY KEY (dest_table, source_id))",
    )
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn read_state(dst: &PgPool, dest_table: &str, source_id: &str) -> Result<Option<u64>> {
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT watermark, cursor_col FROM _apitap_state \
         WHERE dest_table = $1 AND source_id = $2 AND mode = 'log_based'",
    )
    .bind(dest_table)
    .bind(source_id)
    .fetch_optional(dst)
    .await
    .or_else(|e| match &e {
        // No state table at all = fresh destination.
        sqlx::Error::Database(d) if d.code().as_deref() == Some("42P01") => Ok(None),
        _ => Err(db_err(e)),
    })?;
    match row {
        None => Ok(None),
        Some((wm, cursor)) => {
            if cursor != STATE_CURSOR {
                return Err(Error::InvalidInput(format!(
                    "log_based: state row for this table tracks cursor '{cursor}', \
                     not an LSN — it was written by mode append/merge. Use a \
                     different dest_table or clear the state row"
                )));
            }
            let wm = wm.ok_or_else(|| {
                Error::Transfer("log_based: state row has NULL watermark".into())
            })?;
            wm.parse::<u64>()
                .map(Some)
                .map_err(|_| Error::Transfer(format!("log_based: bad LSN state '{wm}'")))
        }
    }
}

async fn write_state(
    dst: &PgPool,
    dest_table: &str,
    source_id: &str,
    lsn: u64,
    rows: u64,
) -> Result<()> {
    let mut tx = dst.begin().await.map_err(db_err)?;
    upsert_state_tx(&mut tx, dest_table, source_id, lsn, rows).await?;
    tx.commit().await.map_err(db_err)
}

async fn upsert_state_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dest_table: &str,
    source_id: &str,
    lsn: u64,
    rows: u64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO _apitap_state \
           (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
         VALUES ($1, $2, $3, $4, 'log_based', $5, now()) \
         ON CONFLICT (dest_table, source_id) DO UPDATE SET \
           cursor_col = EXCLUDED.cursor_col, watermark = EXCLUDED.watermark, \
           mode = EXCLUDED.mode, last_rows = EXCLUDED.last_rows, synced_at = now()",
    )
    .bind(dest_table)
    .bind(source_id)
    .bind(STATE_CURSOR)
    .bind(lsn.to_string())
    .bind(rows as i64)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

/// COPY text-format rendering of one full row.
fn render_copy_row(row: &[Cell], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) => copy_escape(t, out),
            Cell::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell reached the upsert path — \
                     collapse bug"
                        .into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

fn render_copy_row_keys(key: &[Vec<u8>], out: &mut Vec<u8>) {
    for (i, k) in key.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        copy_escape(k, out);
    }
    out.push(b'\n');
}

fn copy_escape(v: &[u8], out: &mut Vec<u8>) {
    for &b in v {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            _ => out.push(b),
        }
    }
}

/// SQL literal for a residue value (untyped literal — the column's type
/// drives the parse, exactly like a hand-written UPDATE).
fn cell_literal(cell: &Cell) -> String {
    match cell {
        Cell::Null | Cell::UnchangedToast => "NULL".into(),
        Cell::Text(t) => format!("'{}'", String::from_utf8_lossy(t).replace('\'', "''")),
    }
}

fn key_pred(pk_cols: &[String], key: &[Vec<u8>]) -> String {
    pk_cols
        .iter()
        .zip(key.iter())
        .map(|(c, v)| {
            format!(
                "{} = '{}'",
                quote_ident(c),
                String::from_utf8_lossy(v).replace('\'', "''")
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Suppress dead-code noise from `Collapsed` fields only read here.
#[allow(dead_code)]
fn _touch(c: &Collapsed) -> u64 {
    c.events
}
