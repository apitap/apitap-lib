//! log_based apply into MySQL — one InnoDB transaction carries the key
//! delete-join, the plain bulk insert (LOAD DATA into a TEMPORARY twin), the
//! residue and the watermark, so the guarantee matches the Postgres apply.
//! The only statement outside the transaction is a WAL-captured TRUNCATE
//! (DDL implicitly commits in MySQL) — replay-safe because re-running the
//! window re-truncates before re-applying.

use crate::dialect::mysql::my_ident;
use crate::error::{Error, Result};
use crate::logbased::collapse::ResidueOp;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::rowtext::{
    bytea_hex, pk_indices, render_my_key, render_my_row, row_key_refs, strip_utc_offset,
    BOOL_OID, BYTEA_OID, TIMESTAMPTZ_OID, TIMETZ_OID,
};
use crate::sink::mysql::{sql_lit, MySqlShared, MySqlSink};
use crate::wire::pgoutput::Cell;
use mysql_async::prelude::Queryable;
use mysql_async::TxOpts;
use std::collections::HashMap;

const STATE_CURSOR: &str = "_lsn";

pub(crate) struct MyDest {
    shared: MySqlShared,
}

/// Fail if the statement just run raised warnings.
///
/// `LOAD DATA LOCAL INFILE` is the exception to strict mode, by design and by
/// documentation: with LOCAL the server cannot stop the client mid-file, so it
/// behaves "as if IGNORE were specified" and downgrades every data error to a
/// warning — including "Data too long for column". Strict mode does not change
/// that. So a CDC apply into a destination column narrower than the value can
/// TRUNCATE and still report success, which is precisely the silent corruption
/// the strict session was set for.
///
/// Reading the warning count costs one round-trip per load and turns that back
/// into an error, quoting what the server said.
async fn no_warnings<'a, T>(q: &mut T, what: &str) -> Result<()>
where
    T: mysql_async::prelude::Queryable,
{
    let n: Option<u32> = q
        .query_first("SELECT @@warning_count")
        .await
        .map_err(|e| Error::Transfer(format!("log_based: mysql warning probe: {e}")))?;
    if n.unwrap_or(0) == 0 {
        return Ok(());
    }
    let rows: Vec<(String, u32, String)> = q
        .query("SHOW WARNINGS")
        .await
        .map_err(|e| Error::Transfer(format!("log_based: mysql SHOW WARNINGS: {e}")))?;
    let said = rows
        .iter()
        .take(3)
        .map(|(lvl, code, msg)| format!("{lvl} {code}: {msg}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(Error::Transfer(format!(
        "log_based: the {what} raised {} warning(s), and a warning here means a \
         value was CHANGED to fit — LOAD DATA LOCAL cannot refuse a row, it can \
         only report afterwards. Refusing the window rather than leaving a \
         value the source never had. MySQL said: {said}",
        n.unwrap_or(0)
    )))
}

impl MyDest {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        Ok(Self { shared: MySqlSink::shared_pool(url)? })
    }

    fn fq(&self, table: &str) -> String {
        format!("{}.{}", my_ident(self.shared.db()), my_ident(table))
    }

    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<u64>> {
        let mut conn = self.shared.conn().await?;
        let row: Option<(Option<String>, Option<String>)> = match conn
            .exec_first(
                format!(
                    "SELECT watermark, cursor_col FROM {} \
                     WHERE dest_table = ? AND source_id = ? AND mode = 'log_based'",
                    self.fq("_apitap_state")
                ),
                (dest_table, source_id),
            )
            .await
        {
            Ok(r) => r,
            // No state table at all = fresh destination.
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => return Ok(None),
            Err(e) => return Err(Error::Transfer(format!("log_based: mysql state: {e}"))),
        };
        match row {
            None => Ok(None),
            Some((wm, cursor)) => {
                if cursor.as_deref() != Some(STATE_CURSOR) {
                    return Err(Error::InvalidInput(format!(
                        "log_based: state row for this table tracks cursor '{}', \
                         not an LSN — it was written by another mode. Use a \
                         different dest_table or clear the state row",
                        cursor.unwrap_or_default()
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

    /// The bootstrap's replace path may or may not have carried the PK into
    /// the created table — ensure it, then write the state row.
    pub(crate) async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        let mut conn = self.shared.conn().await?;
        let has_pk: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.table_constraints \
                 WHERE table_schema = ? AND table_name = ? \
                   AND constraint_type = 'PRIMARY KEY'",
                (self.shared.db(), dest_table),
            )
            .await
            .map_err(|e| Error::Transfer(format!("log_based: mysql pk probe: {e}")))?;
        if has_pk.unwrap_or(0) == 0 {
            let pklist =
                pk_cols.iter().map(|c| my_ident(c)).collect::<Vec<_>>().join(", ");
            conn.query_drop(format!(
                "ALTER TABLE {} ADD PRIMARY KEY ({pklist})",
                self.fq(dest_table)
            ))
            .await
            .map_err(|e| Error::Transfer(format!("log_based: mysql add pk: {e}")))?;
        }
        self.ensure_state_table(&mut conn).await?;
        conn.query_drop(state_upsert_sql(
            &self.fq("_apitap_state"),
            dest_table,
            source_id,
            lsn,
            rows,
        ))
        .await
        .map_err(|e| Error::Transfer(format!("log_based: mysql state write: {e}")))
    }

    /// Remove this table's watermark row — a failed group bootstrap must leave
    /// no state, or the next run refuses the group as torn.
    pub(crate) async fn clear_state(&self, dest_table: &str, source_id: &str) -> Result<()> {
        use mysql_async::prelude::Queryable as _;
        let mut conn = self.shared.conn().await?;
        match conn
            .exec_drop(
                format!(
                    "DELETE FROM {} WHERE dest_table = ? AND source_id = ?",
                    self.fq("_apitap_state")
                ),
                (dest_table, source_id),
            )
            .await
        {
            Ok(()) => Ok(()),
            // No state table at all: nothing to clear.
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(()),
            Err(e) => Err(Error::Transfer(format!("log_based: clear state: {e}"))),
        }
    }

    /// Write a bare state row — the source-identity marker rides in an
    /// ordinary row under a reserved `source_id`, so the state table needs no
    /// new column and older deployments need no migration.
    pub(crate) async fn write_marker(
        &self,
        dest_table: &str,
        source_id: &str,
        value: u64,
    ) -> Result<()> {
        let mut conn = self.shared.conn().await?;
        self.ensure_state_table(&mut conn).await?;
        let sql = state_upsert_sql(&self.fq("_apitap_state"), dest_table, source_id, value, 0);
        conn.query_drop(sql)
            .await
            .map_err(|e| Error::Transfer(format!("log_based: mysql marker: {e}")))
    }

    async fn ensure_state_table(&self, conn: &mut mysql_async::Conn) -> Result<()> {
        conn.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                dest_table VARCHAR(255) NOT NULL, \
                source_id VARCHAR(512) NOT NULL, \
                cursor_col VARCHAR(255), \
                watermark VARCHAR(255), \
                mode VARCHAR(16), \
                last_rows BIGINT, \
                synced_at DATETIME(6), \
                PRIMARY KEY (dest_table, source_id)\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            self.fq("_apitap_state")
        ))
        .await
        .map_err(|e| Error::Transfer(format!("log_based: mysql state ddl: {e}")))
    }

    /// Apply one collapsed window in ONE InnoDB transaction (see module docs
    /// for the TRUNCATE exception).
    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let state = self.fq("_apitap_state");
        let mut conn = self.shared.conn().await?;
        self.ensure_state_table(&mut conn).await?;

        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            conn.query_drop(state_upsert_sql(&state, dest_table, source_id, outcome.end_lsn, 0))
                .await
                .map_err(|e| Error::Transfer(format!("log_based: mysql state write: {e}")))?;
            return Ok(0);
        };
        let wal_cols = outcome
            .wal_cols
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;
        let oids = outcome
            .wal_oids
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL type list".into()))?;
        let ft = self.fq(dest_table);
        let pk_idx = pk_indices(pk_cols, wal_cols)?;
        let pk_oids: Vec<u32> = pk_idx.iter().map(|&i| oids[i]).collect();

        let my_err = |what: &'static str| {
            move |e: mysql_async::Error| Error::Transfer(format!("log_based: mysql {what}: {e}"))
        };

        // The bulk loader writes into a staging table it created itself, so
        // it can afford a relaxed session. This one writes into the USER's
        // table, and the two settings that were copied across from it are the
        // wrong ones here:
        //
        // * `sql_mode=''` makes an over-long or out-of-range value TRUNCATE
        //   with a warning nobody reads. A CDC apply that silently shortens a
        //   value is exactly the failure this lane exists to avoid, so it runs
        //   STRICT_ALL_TABLES and lets the error surface. (STRICT alone does
        //   not add NO_ZERO_DATE, so a MySQL source's '0000-00-00' still
        //   applies as it always did.)
        // * `unique_checks=0` tells InnoDB it may skip uniqueness enforcement
        //   on secondary indexes — on a destination that HAS unique keys, a
        //   duplicate then lands instead of raising. The bulk staging table
        //   has no keys to check, so it lost nothing; here it did.
        //
        // `foreign_key_checks=0` stays: a window applies each table
        // independently, and enforcing FKs would refuse an order that is
        // perfectly valid once the whole window has landed.
        conn.query_drop(
            "SET time_zone='+00:00', foreign_key_checks=0, sql_mode='STRICT_ALL_TABLES'",
        )
        .await
        .map_err(my_err("session"))?;

        // WAL-captured TRUNCATE: DDL implicitly commits, so it cannot ride
        // the transaction — replay re-truncates, so this stays idempotent.
        if c.truncate {
            conn.query_drop(format!("TRUNCATE TABLE {ft}")).await.map_err(my_err("truncate"))?;
        }

        let clear = !c.deletes.is_empty() || !c.upserts.is_empty();
        if clear {
            // Key twin with the destination's own column types.
            let col_types: Vec<(String, String)> = conn
                .exec(
                    "SELECT column_name, column_type FROM information_schema.columns \
                     WHERE table_schema = ? AND table_name = ?",
                    (self.shared.db(), dest_table),
                )
                .await
                .map_err(my_err("column types"))?;
            let type_of: HashMap<&str, &str> = col_types
                .iter()
                .map(|(n, t)| (n.as_str(), t.as_str()))
                .collect();
            let ddl = pk_cols
                .iter()
                .map(|k| {
                    let ty = type_of.get(k.as_str()).ok_or_else(|| {
                        Error::Transfer(format!(
                            "log_based: PK column '{k}' missing at the MySQL destination"
                        ))
                    })?;
                    Ok(format!("{} {ty}", my_ident(k)))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            conn.query_drop("DROP TEMPORARY TABLE IF EXISTS _ap_del")
                .await
                .map_err(my_err("temp ddl"))?;
            conn.query_drop(format!("CREATE TEMPORARY TABLE _ap_del ({ddl}) ENGINE=InnoDB"))
                .await
                .map_err(my_err("temp ddl"))?;
        }
        if !c.upserts.is_empty() {
            conn.query_drop("DROP TEMPORARY TABLE IF EXISTS _ap_up")
                .await
                .map_err(my_err("temp ddl"))?;
            conn.query_drop(format!("CREATE TEMPORARY TABLE _ap_up LIKE {ft}"))
                .await
                .map_err(my_err("temp ddl"))?;
        }

        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(my_err("begin"))?;

        if clear {
            let mut body = Vec::with_capacity(1 << 20);
            for key in &c.deletes {
                let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
                render_my_key(&refs, &pk_oids, &mut body)?;
            }
            for row in &c.upserts {
                render_my_key(&row_key_refs(row, &pk_idx), &pk_oids, &mut body)?;
            }
            let id = self.shared.register_infile(body);
            let load = load_sql(id, "_ap_del", pk_cols, &pk_oids);
            if let Err(e) = tx.query_drop(&load).await {
                self.shared.forget_infile(id);
                return Err(my_err("load keys")(e));
            }
            no_warnings(&mut tx, "key load").await?;
            let join = pk_cols
                .iter()
                .map(|k| format!("t.{k} = d.{k}", k = my_ident(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            tx.query_drop(format!("DELETE t FROM {ft} t JOIN _ap_del d ON {join}"))
                .await
                .map_err(my_err("delete join"))?;
        }

        if !c.upserts.is_empty() {
            let mut body = Vec::with_capacity(4 << 20);
            for row in &c.upserts {
                render_my_row(row, oids, &mut body)?;
            }
            let id = self.shared.register_infile(body);
            let load = load_sql(id, "_ap_up", wal_cols, oids);
            if let Err(e) = tx.query_drop(&load).await {
                self.shared.forget_infile(id);
                return Err(my_err("load rows")(e));
            }
            no_warnings(&mut tx, "row load").await?;
            let collist =
                wal_cols.iter().map(|c| my_ident(c)).collect::<Vec<_>>().join(", ");
            // No ON DUPLICATE KEY: the delete phase already cleared every key.
            tx.query_drop(format!(
                "INSERT INTO {ft} ({collist}) SELECT {collist} FROM _ap_up"
            ))
            .await
            .map_err(my_err("bulk insert"))?;
        }

        // Residue tail: serial, ordered.
        for op in &c.residue {
            let sql = match op {
                ResidueOp::MaskedUpdate { key, row } => {
                    let sets = wal_cols
                        .iter()
                        .zip(row.iter().zip(oids.iter()))
                        .filter(|(cname, (cell, _))| {
                            !matches!(cell, Cell::UnchangedToast) && !pk_cols.contains(cname)
                        })
                        .map(|(cname, (cell, &oid))| {
                            Ok(format!("{} = {}", my_ident(cname), my_literal(cell, oid)?))
                        })
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    if sets.is_empty() {
                        continue;
                    }
                    format!(
                        "UPDATE {ft} SET {sets} WHERE {}",
                        key_pred(pk_cols, key, &pk_oids)?
                    )
                }
                ResidueOp::Upsert { row } => {
                    let collist =
                        wal_cols.iter().map(|c| my_ident(c)).collect::<Vec<_>>().join(", ");
                    let vals = row
                        .iter()
                        .zip(oids.iter())
                        .map(|(cell, &oid)| my_literal(cell, oid))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    let updates = wal_cols
                        .iter()
                        .filter(|cname| !pk_cols.contains(cname))
                        .map(|cname| format!("{q} = VALUES({q})", q = my_ident(cname)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    if updates.is_empty() {
                        format!("INSERT IGNORE INTO {ft} ({collist}) VALUES ({vals})")
                    } else {
                        format!(
                            "INSERT INTO {ft} ({collist}) VALUES ({vals}) \
                             ON DUPLICATE KEY UPDATE {updates}"
                        )
                    }
                }
                ResidueOp::Delete { key } => {
                    format!("DELETE FROM {ft} WHERE {}", key_pred(pk_cols, key, &pk_oids)?)
                }
            };
            tx.query_drop(sql).await.map_err(my_err("residue"))?;
        }

        tx.query_drop(state_upsert_sql(&state, dest_table, source_id, outcome.end_lsn, c.events))
            .await
            .map_err(my_err("state write"))?;
        tx.commit().await.map_err(my_err("commit"))?;

        // The pooled connection outlives this apply — don't leak the twins.
        conn.query_drop("DROP TEMPORARY TABLE IF EXISTS _ap_del, _ap_up")
            .await
            .map_err(my_err("temp cleanup"))?;
        Ok(c.events)
    }
}

/// The LOAD DATA statement for one staging twin: binary (bytea) columns ride
/// as hex through a synthetic positional user var and UNHEX back (same move
/// as the bulk loader).
fn load_sql(id: u64, table: &str, cols: &[String], oids: &[u32]) -> String {
    let mut names = Vec::new();
    let mut sets = Vec::new();
    for (i, (name, &oid)) in cols.iter().zip(oids.iter()).enumerate() {
        if oid == BYTEA_OID {
            let var = format!("@apitap_{i}");
            names.push(var.clone());
            sets.push(format!("{} = UNHEX({var})", my_ident(name)));
        } else {
            names.push(my_ident(name));
        }
    }
    let set_clause =
        if sets.is_empty() { String::new() } else { format!(" SET {}", sets.join(", ")) };
    format!(
        "LOAD DATA LOCAL INFILE 'apitap:{id}' INTO TABLE {table} \
         CHARACTER SET utf8mb4 \
         FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' \
         LINES TERMINATED BY '\\n' ({cols}){set_clause}",
        cols = names.join(", "),
    )
}

fn state_upsert_sql(
    state: &str,
    dest_table: &str,
    source_id: &str,
    lsn: u64,
    rows: u64,
) -> String {
    format!(
        "INSERT INTO {state} \
           (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
         VALUES ('{dt}','{sid}','{STATE_CURSOR}','{lsn}','log_based',{rows},UTC_TIMESTAMP(6)) \
         ON DUPLICATE KEY UPDATE cursor_col=VALUES(cursor_col), \
           watermark=VALUES(watermark), mode=VALUES(mode), \
           last_rows=VALUES(last_rows), synced_at=VALUES(synced_at)",
        dt = sql_lit(dest_table),
        sid = sql_lit(source_id),
    )
}

/// MySQL SQL literal for a residue value, typed by OID.
fn my_literal(cell: &Cell, oid: u32) -> Result<String> {
    let Cell::Text(t) = cell else {
        return Ok("NULL".into());
    };
    Ok(match oid {
        BYTEA_OID => format!(
            "UNHEX('{}')",
            std::str::from_utf8(bytea_hex(t)?)
                .map_err(|_| Error::Transfer("log_based: non-UTF8 bytea hex".into()))?
        ),
        BOOL_OID => (if &t[..] == b"t" { "1" } else { "0" }).into(),
        TIMESTAMPTZ_OID | TIMETZ_OID => {
            let s = strip_utc_offset(t)?;
            format!("'{}'", sql_lit(&String::from_utf8_lossy(s)))
        }
        _ => format!("'{}'", sql_lit(&String::from_utf8_lossy(t))),
    })
}

fn key_pred(pk_cols: &[String], key: &[Vec<u8>], pk_oids: &[u32]) -> Result<String> {
    Ok(pk_cols
        .iter()
        .zip(key.iter().zip(pk_oids.iter()))
        .map(|(c, (v, &oid))| {
            Ok(format!(
                "{} = {}",
                my_ident(c),
                my_literal(&Cell::Text(bytes::Bytes::from(v.clone())), oid)?
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(" AND "))
}
