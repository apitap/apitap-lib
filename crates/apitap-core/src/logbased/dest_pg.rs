//! log_based apply into Postgres — the reference destination: one
//! transaction carries truncate → deletes → plain bulk insert → residue →
//! watermark, and the slot is only confirmed after that commit.

use crate::error::{Error, Result};
use crate::logbased::collapse::ResidueOp;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::rowtext::{copy_escape, pk_indices, render_copy_row, row_key_refs};
use crate::wire::pgoutput::Cell;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};

const STATE_CURSOR: &str = "_lsn";

pub(crate) struct PgDest {
    pool: PgPool,
}

impl PgDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await
            .map_err(|e| Error::Transfer(format!("log_based: dest connect: {e}")))?;
        Ok(Self { pool })
    }

    /// The bootstrap's replace path lands data without constraints; the
    /// drain's apply needs the identity — add it, then write the state row.
    pub(crate) async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        let pklist = pk_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        sqlx::query(&format!(
            "ALTER TABLE {} ADD PRIMARY KEY ({pklist})",
            quote_table(dest_table)
        ))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        self.ensure_state_table().await?;
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        upsert_state_tx(&mut tx, dest_table, source_id, lsn, rows).await?;
        tx.commit().await.map_err(db_err)
    }

    async fn ensure_state_table(&self) -> Result<()> {
        self.pool
            .execute(
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

    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<u64>> {
        let row: Option<(Option<String>, String)> = sqlx::query_as(
            "SELECT watermark, cursor_col FROM _apitap_state \
             WHERE dest_table = $1 AND source_id = $2 AND mode = 'log_based'",
        )
        .bind(dest_table)
        .bind(source_id)
        .fetch_optional(&self.pool)
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

    /// Apply one collapsed window for one table in ONE destination transaction
    /// (truncate → deletes → upserts → residue → watermark).
    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let dst = &self.pool;
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            self.ensure_state_table().await?;
            let mut tx = dst.begin().await.map_err(db_err)?;
            upsert_state_tx(&mut tx, dest_table, source_id, outcome.end_lsn, 0).await?;
            tx.commit().await.map_err(db_err)?;
            return Ok(0);
        };
        let wal_cols = outcome
            .wal_cols
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;

        self.ensure_state_table().await?;
        let ft = quote_table(dest_table);
        let collist = wal_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let pklist = pk_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");

        let mut tx = dst.begin().await.map_err(db_err)?;

        if c.truncate {
            tx.execute(format!("TRUNCATE {ft}").as_str()).await.map_err(db_err)?;
        }

        // Delete phase covers the delete-set UNION every upsert's key: clearing
        // the way first turns 450K index-probing ON CONFLICT upserts into 450K
        // plain inserts (ape-dts's rdb_merge trick — measured 5x here).
        let pk_idx = pk_indices(pk_cols, wal_cols)?;
        let clear_keys = !c.deletes.is_empty() || !c.upserts.is_empty();
        if clear_keys {
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
            let mut buf = Vec::with_capacity(4 << 20);
            for key in &c.deletes {
                let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
                render_key_row(&refs, &mut buf);
                if buf.len() > 4 << 20 {
                    copy.send(std::mem::take(&mut buf)).await.map_err(db_err)?;
                }
            }
            for row in &c.upserts {
                render_key_row(&row_key_refs(row, &pk_idx), &mut buf);
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

        // Upsert phase: COPY into a temp twin, then one plain INSERT.
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
            // No ON CONFLICT: the delete phase already removed every one of
            // these keys, so this is a straight bulk insert.
            tx.execute(
                format!("INSERT INTO {ft} ({collist}) SELECT {collist} FROM _ap_up").as_str(),
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
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based: {e}"))
}

pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) fn quote_table(t: &str) -> String {
    t.split('.').map(quote_ident).collect::<Vec<_>>().join(".")
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

fn render_key_row(key: &[&[u8]], out: &mut Vec<u8>) {
    for (i, k) in key.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        copy_escape(k, out);
    }
    out.push(b'\n');
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
