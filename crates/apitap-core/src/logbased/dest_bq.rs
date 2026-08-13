//! log_based apply into BigQuery.
//!
//! Each drained window lands in an all-STRING staging table (loaded with
//! WRITE_TRUNCATE, so a replayed window is idempotent by construction) and is
//! applied to the target with ONE `MERGE` inside a multi-statement transaction
//! that also advances the `_apitap_state` watermark — data and watermark commit
//! atomically. Column types come from the DESTINATION's own DDL, not from WAL
//! type OIDs, so a MySQL binlog source (every oid 0) applies through the exact
//! same path; the apply therefore needs neither a source pool nor a destination
//! readback, and MySQL → BigQuery CDC works too.
//!
//! Unchanged-TOAST cells that can't be patched from an in-window base ride a
//! per-row mask column: the masked columns are omitted from the staging row and
//! the MERGE keeps the target's current value (`IF(masked, T.c, S.c)`).
//!
//! Because CDC needs row-level DELETE/UPDATE (DML), log_based → BigQuery
//! requires a project with billing enabled; sandbox/free-tier projects reject
//! DML and the first MERGE will fail loudly.

use crate::error::{Error, Result};
use crate::logbased::collapse::Key;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::resolve::{resolve_window, Fin};
use crate::logbased::rowtext::pk_indices;
use crate::sink::bigquery::{sql_str, BqConn};
use crate::wire::pgoutput::Cell;
use serde_json::{json, Map, Value};
use std::collections::HashSet;

const OP_COL: &str = "_apitap_op";
const MASK_COL: &str = "_apitap_mask";

pub(crate) struct BqDest {
    conn: BqConn,
}

/// `dest_table` may arrive schema-qualified; the BigQuery dataset comes from the
/// URL, so only the bare name addresses the table (same trim as the sink).
fn bare(dest_table: &str) -> &str {
    dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t)
}

fn cdc_staging(table: &str) -> String {
    format!("{table}__apitap_cdc")
}

impl BqDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        Ok(Self { conn: BqConn::parse(url).await? })
    }

    pub(crate) async fn read_state(&self, dest_table: &str, source_id: &str) -> Result<Option<u64>> {
        let table = bare(dest_table);
        if !self.conn.cdc_state_table_exists().await? {
            return Ok(None);
        }
        // Newest state row for THIS source that lands AFTER the most recent `*`
        // replace-barrier (a later bulk replace invalidates the CDC watermark).
        let sql = format!(
            "WITH s AS (SELECT * FROM {state} WHERE dest_table = '{dt}'), \
             b AS (SELECT IFNULL(MAX(synced_at), TIMESTAMP '1970-01-01') AS ts \
                   FROM s WHERE source_id = '*') \
             SELECT watermark, cursor_col, mode FROM s, b \
             WHERE source_id = '{sid}' AND synced_at > b.ts \
             ORDER BY synced_at DESC LIMIT 1",
            state = self.conn.state_fq(),
            dt = sql_str(table),
            sid = sql_str(source_id),
        );
        let rows = self.conn.cdc_query(&sql).await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let watermark = row.first().cloned().flatten();
        let cursor_col = row.get(1).cloned().flatten();
        let mode = row.get(2).cloned().flatten();
        if cursor_col.as_deref() != Some("_lsn") || mode.as_deref() != Some("log_based") {
            return Err(Error::InvalidInput(format!(
                "log_based: BigQuery target {table} has state written by a different \
                 mode (cursor_col={cursor_col:?}, mode={mode:?}) — run once with \
                 mode='replace' to realign, or clear its _apitap_state rows"
            )));
        }
        match watermark {
            None => Ok(None),
            Some(w) => w.parse::<u64>().map(Some).map_err(|_| {
                Error::Transfer(format!("log_based: BigQuery watermark '{w}' is not an LSN"))
            }),
        }
    }

    /// The bootstrap's replace just (re)created the target and wrote a `*`
    /// barrier. CLUSTER the target on its PK (so every window's MERGE prunes to
    /// the touched blocks instead of full-scanning — the MERGE is the dominant
    /// per-window cost), heal any stale staging, then stamp the slot's LSN as
    /// the CDC watermark with a server-clock timestamp so it sorts AFTER that
    /// barrier.
    pub(crate) async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        let table = bare(dest_table);
        self.conn.cdc_delete_table(&cdc_staging(table)).await?;
        self.conn.cdc_delete_table(&format!("{table}__apitap_cl")).await?;
        self.cluster_target(table, pk_cols, rows).await?;
        self.conn.cdc_ensure_state_table().await?;
        self.write_state(table, source_id, lsn, rows).await
    }

    /// Rewrite the freshly-bootstrapped target clustered on its PK (up to 4
    /// columns — BigQuery's max). One full-table rewrite, once, so every later
    /// MERGE prunes the target scan. Only worth it once the scan dominates the
    /// MERGE's ~fixed BigQuery job floor — measured NEUTRAL below ~1M rows, so
    /// small tables skip it (and skip its one-time rewrite cost). Kill switch
    /// `APITAP_BQ_CLUSTER=0`; skips if already clustered on the same keys.
    async fn cluster_target(&self, table: &str, pk_cols: &[String], rows: u64) -> Result<()> {
        const CLUSTER_MIN_ROWS: u64 = 1_000_000;
        if std::env::var("APITAP_BQ_CLUSTER").as_deref() == Ok("0") || rows < CLUSTER_MIN_ROWS {
            return Ok(());
        }
        let keys: Vec<&String> = pk_cols.iter().take(4).collect();
        if keys.is_empty() {
            return Ok(());
        }
        if let Some(meta) = self.conn.table_get(table).await? {
            if let Some(cur) = meta["clustering"]["fields"].as_array() {
                let cur: Vec<&str> = cur.iter().filter_map(|f| f.as_str()).collect();
                let want: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                if cur == want {
                    return Ok(());
                }
            }
        }
        let cl = keys.iter().map(|k| format!("`{k}`")).collect::<Vec<_>>().join(", ");
        let tmp = format!("{table}__apitap_cl");
        let sql = format!(
            "CREATE OR REPLACE TABLE {tmp} CLUSTER BY {cl} AS SELECT * FROM {t};\n\
             DROP TABLE {t};\n\
             ALTER TABLE {tmp} RENAME TO `{table}`;",
            tmp = self.conn.fq(&tmp),
            t = self.conn.fq(table),
        );
        self.conn.cdc_script(&sql).await
    }

    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let table = bare(dest_table);
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            self.write_state(table, source_id, outcome.end_lsn, 0).await?;
            return Ok(0);
        };
        let wal_cols = outcome
            .wal_cols
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;
        for name in wal_cols {
            if name == OP_COL || name == MASK_COL {
                return Err(Error::InvalidInput(format!(
                    "log_based: source column '{name}' collides with a reserved BigQuery \
                     CDC staging column — rename it at the source or alias it in a view"
                )));
            }
        }
        let oids = outcome
            .wal_oids
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL type list".into()))?;
        let pk_idx = pk_indices(pk_cols, wal_cols)?;

        // The target's declared column types drive every cast — read once.
        let meta = self.conn.table_get(table).await?.ok_or_else(|| {
            Error::Transfer(format!(
                "log_based: BigQuery target {table} does not exist — the bootstrap must \
                 run before a CDC window can apply"
            ))
        })?;
        let types = column_types(&meta)?;
        let plan = ApplyPlan::build(table, wal_cols, oids, pk_cols, &types)?;

        // Fold the window to one final image per key.
        let finals = resolve_window(c, &pk_idx);

        // Build the staging body: upserts (op='U', maybe masked) and deletes
        // (op='D', PK columns only). A key that is both deleted and re-landed
        // rides as one 'U' row — never emit a second 'D' for it (the MERGE
        // requires at most one source row per target row).
        let landed: HashSet<Key> = finals
            .iter()
            .filter(|(_, f)| !matches!(f, Fin::Gone))
            .map(|(k, _)| k.clone())
            .collect();
        let mut ndjson: Vec<u8> = Vec::new();
        let mut staged = 0u64;
        for (key, fin) in &finals {
            match fin {
                Fin::Row(cells) => {
                    push_upsert(&mut ndjson, wal_cols, cells, None)?;
                    staged += 1;
                }
                Fin::Owned(cells) => {
                    push_upsert(&mut ndjson, wal_cols, cells, None)?;
                    staged += 1;
                }
                Fin::Refetch(cells) => {
                    let mask: String = cells
                        .iter()
                        .map(|c| if matches!(c, Cell::UnchangedToast) { '1' } else { '0' })
                        .collect();
                    push_upsert(&mut ndjson, wal_cols, cells, Some(&mask))?;
                    staged += 1;
                }
                Fin::Gone => {
                    push_delete(&mut ndjson, pk_cols, key)?;
                    staged += 1;
                }
            }
        }
        if !c.truncate {
            for key in &c.deletes {
                // A key also re-landed as an upsert rides as that one 'U' row.
                if landed.contains(key) {
                    continue;
                }
                push_delete(&mut ndjson, pk_cols, key)?;
                staged += 1;
            }
        }

        let state_sql = self.state_insert_sql(table, source_id, outcome.end_lsn, c.events);

        if staged == 0 {
            // Nothing to merge. A TRUNCATE window still empties the target.
            if c.truncate {
                let sql = format!(
                    "BEGIN TRANSACTION;\nDELETE FROM {t} WHERE TRUE;\n{state};\nCOMMIT TRANSACTION;",
                    t = self.conn.fq(table),
                    state = state_sql,
                );
                self.conn.cdc_script(&sql).await?;
            } else {
                self.conn.cdc_script(&format!("{state_sql};")).await?;
            }
            return Ok(c.events);
        }

        // Land the window in the staging table, then MERGE + watermark in one tx.
        let dbg = std::env::var("APITAP_DEBUG").is_ok();
        let nbytes = ndjson.len();
        let t_load = std::time::Instant::now();
        self.conn
            .cdc_load_ndjson(&cdc_staging(table), &plan.staging_fields(), ndjson)
            .await?;
        let load_s = t_load.elapsed().as_secs_f64();
        let sql = format!(
            "BEGIN TRANSACTION;\n{merge};\n{state};\nCOMMIT TRANSACTION;",
            merge = plan.merge_sql(&self.conn, table, c.truncate),
            state = state_sql,
        );
        let t_merge = std::time::Instant::now();
        self.conn.cdc_script(&sql).await?;
        if dbg {
            eprintln!(
                "[bq apply] {table}: {staged} staging rows / {:.1}MB, load={load_s:.1}s \
                 merge={:.1}s",
                nbytes as f64 / (1 << 20) as f64,
                t_merge.elapsed().as_secs_f64(),
            );
        }
        Ok(c.events)
    }

    async fn write_state(&self, table: &str, source_id: &str, lsn: u64, rows: u64) -> Result<()> {
        self.conn
            .cdc_script(&format!("{};", self.state_insert_sql(table, source_id, lsn, rows)))
            .await
    }

    fn state_insert_sql(&self, table: &str, source_id: &str, lsn: u64, rows: u64) -> String {
        format!(
            "INSERT INTO {state} \
             (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ('{dt}', '{sid}', '_lsn', '{lsn}', 'log_based', {rows}, CURRENT_TIMESTAMP())",
            state = self.conn.state_fq(),
            dt = sql_str(table),
            sid = sql_str(source_id),
        )
    }
}

// ── the per-table apply plan (cast expressions from the target DDL) ──────────

struct ApplyPlan {
    cols: Vec<String>,
    /// Per-column SELECT expression casting the STRING staging value to the
    /// target's declared type; index-parallel to `cols`.
    cast: Vec<String>,
    pk: Vec<String>,
}

impl ApplyPlan {
    fn build(
        table: &str,
        wal_cols: &[String],
        oids: &[u32],
        pk_cols: &[String],
        types: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let mut cast = Vec::with_capacity(wal_cols.len());
        for (i, name) in wal_cols.iter().enumerate() {
            let ty = types.get(name).ok_or_else(|| {
                Error::InvalidInput(format!(
                    "log_based: column '{name}' is in the WAL but not in the BigQuery target \
                     {table} — run once with mode='replace' to realign the schema"
                ))
            })?;
            // OID 0 (MySQL binlog) falls straight through to the target-type cast.
            let oid = oids.get(i).copied().unwrap_or(0);
            cast.push(cast_expr(name, oid, ty)?);
        }
        Ok(Self { cols: wal_cols.to_vec(), cast, pk: pk_cols.to_vec() })
    }

    fn staging_fields(&self) -> Value {
        let mut fields = vec![
            json!({"name": OP_COL, "type": "STRING", "mode": "REQUIRED"}),
            json!({"name": MASK_COL, "type": "STRING", "mode": "NULLABLE"}),
        ];
        for name in &self.cols {
            fields.push(json!({"name": name, "type": "STRING", "mode": "NULLABLE"}));
        }
        Value::Array(fields)
    }

    fn merge_sql(&self, conn: &BqConn, table: &str, truncate: bool) -> String {
        let bt = |c: &str| format!("`{c}`");
        let using: Vec<String> = self
            .cols
            .iter()
            .zip(&self.cast)
            .map(|(c, expr)| format!("    {expr} AS {}", bt(c)))
            .collect();
        let on = self
            .pk
            .iter()
            .map(|c| format!("T.{0} = S.{0}", bt(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let non_pk: Vec<&String> = self.cols.iter().filter(|c| !self.pk.contains(c)).collect();
        let set = non_pk
            .iter()
            .map(|c| {
                let pos = self.cols.iter().position(|x| &x == c).expect("col") + 1;
                format!(
                    "    {c} = IF(S.{mask} IS NULL OR SUBSTR(S.{mask}, {pos}, 1) = '0', S.{c}, T.{c})",
                    c = bt(c),
                    mask = MASK_COL,
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        let insert_cols = self.cols.iter().map(|c| bt(c)).collect::<Vec<_>>().join(", ");
        let insert_vals = self.cols.iter().map(|c| format!("S.{}", bt(c))).collect::<Vec<_>>().join(", ");

        let mut merge = String::new();
        merge.push_str(&format!("MERGE {} T\nUSING (\n  SELECT {}, {},\n{}\n  FROM {}\n) S\nON {}\n",
            conn.fq(table), OP_COL, MASK_COL, using.join(",\n"), conn.fq(&cdc_staging(table)), on));
        merge.push_str(&format!("WHEN MATCHED AND S.{OP_COL} = 'D' THEN\n  DELETE\n"));
        if !non_pk.is_empty() {
            merge.push_str(&format!("WHEN MATCHED THEN\n  UPDATE SET\n{set}\n"));
        }
        merge.push_str(&format!(
            "WHEN NOT MATCHED BY TARGET AND S.{OP_COL} = 'U' AND S.{MASK_COL} IS NULL THEN\n  \
             INSERT ({insert_cols}) VALUES ({insert_vals})\n"
        ));
        merge.push_str(&format!(
            "WHEN NOT MATCHED BY TARGET AND S.{OP_COL} = 'U' THEN\n  INSERT ({first}) VALUES \
             (ERROR('log_based: masked update for a row missing at the BigQuery target — \
             window replay out of order?'))\n",
            first = bt(&self.cols[0]),
        ));
        if truncate {
            merge.push_str("WHEN NOT MATCHED BY SOURCE THEN\n  DELETE\n");
        }
        merge
    }
}

const BOOL_OID: u32 = 16;

/// The cast expression turning a STRING staging value `S.c` into the target's
/// declared BigQuery type. Keyed on the target type read from the target's own
/// schema, PLUS the source WAL OID for the one case where the two disagree: a
/// Postgres `boolean` arrives as `t`/`f` in the WAL but the bulk bootstrap
/// stored it as the target's numeric form (bool → INT64, value 1/0), so the CDC
/// path must translate `t`/`f` to match. MySQL binlog columns carry OID 0 and
/// fall through to the target-type cast (their bool is already `1`/`0`).
fn cast_expr(name: &str, oid: u32, ty: &str) -> Result<String> {
    let c = format!("`{name}`");
    if oid == BOOL_OID {
        return Ok(match ty {
            "BOOL" | "BOOLEAN" => format!(
                "CASE WHEN {c} IS NULL THEN NULL \
                 WHEN {c} IN ('t','true','TRUE','1') THEN TRUE \
                 WHEN {c} IN ('f','false','FALSE','0') THEN FALSE \
                 ELSE ERROR(FORMAT('log_based: bad bool text %s for column {name}', {c})) END"
            ),
            "INT64" | "NUMERIC" | "BIGNUMERIC" | "FLOAT64" => format!(
                "CASE WHEN {c} IS NULL THEN NULL \
                 WHEN {c} IN ('t','true','TRUE','1') THEN 1 \
                 WHEN {c} IN ('f','false','FALSE','0') THEN 0 \
                 ELSE ERROR(FORMAT('log_based: bad bool text %s for column {name}', {c})) END"
            ),
            "STRING" => c,
            other => {
                return Err(Error::InvalidInput(format!(
                    "log_based: boolean column '{name}' maps to BigQuery type {other}, which the \
                     CDC path can't fill — run mode='replace' once so apitap owns the DDL"
                )))
            }
        });
    }
    Ok(match ty {
        "STRING" => c,
        "INT64" => format!("CAST({c} AS INT64)"),
        "FLOAT64" => format!("CAST({c} AS FLOAT64)"),
        "NUMERIC" => format!("CAST({c} AS NUMERIC)"),
        "BIGNUMERIC" => format!("CAST({c} AS BIGNUMERIC)"),
        "BOOL" | "BOOLEAN" => format!(
            "CASE WHEN {c} IS NULL THEN NULL \
             WHEN {c} IN ('t','true','TRUE','1') THEN TRUE \
             WHEN {c} IN ('f','false','FALSE','0') THEN FALSE \
             ELSE ERROR(FORMAT('log_based: bad bool text %s for column {name}', {c})) END"
        ),
        "BYTES" => format!("IF({c} IS NULL, NULL, FROM_HEX(SUBSTR({c}, 3)))"),
        "DATE" => format!("CAST({c} AS DATE)"),
        "TIMESTAMP" => format!("CAST({c} AS TIMESTAMP)"),
        "DATETIME" => format!("CAST(REGEXP_REPLACE({c}, r'\\+00(:00)?$', '') AS DATETIME)"),
        "TIME" => format!("CAST(REGEXP_REPLACE({c}, r'\\+00(:00)?$', '') AS TIME)"),
        other => {
            return Err(Error::InvalidInput(format!(
                "log_based: BigQuery column '{name}' has type {other}, which the CDC apply \
                 path can't cast into from WAL text — run mode='replace' once so apitap owns \
                 the DDL, or drop the column from replication"
            )))
        }
    })
}

/// name → declared BigQuery type, from a `tables.get` response. The REST API
/// reports the LEGACY type spellings (INTEGER/FLOAT/BOOLEAN), not the standard
/// SQL ones (INT64/FLOAT64/BOOL) — canonicalize so `cast_expr` sees one name.
fn column_types(meta: &Value) -> Result<std::collections::HashMap<String, String>> {
    let fields = meta["schema"]["fields"]
        .as_array()
        .ok_or_else(|| Error::Transfer("log_based: BigQuery table has no schema".into()))?;
    let mut out = std::collections::HashMap::with_capacity(fields.len());
    for f in fields {
        let (Some(n), Some(t)) = (f["name"].as_str(), f["type"].as_str()) else {
            continue;
        };
        out.insert(n.to_string(), canonical_type(t).to_string());
    }
    Ok(out)
}

/// Legacy BigQuery type spelling → standard SQL spelling.
fn canonical_type(t: &str) -> &str {
    match t {
        "INTEGER" => "INT64",
        "FLOAT" => "FLOAT64",
        "BOOLEAN" => "BOOL",
        other => other,
    }
}

fn push_upsert(out: &mut Vec<u8>, cols: &[String], cells: &[Cell], mask: Option<&str>) -> Result<()> {
    let mut obj = Map::new();
    obj.insert(OP_COL.to_string(), json!("U"));
    if let Some(m) = mask {
        obj.insert(MASK_COL.to_string(), json!(m));
    }
    for (i, cell) in cells.iter().enumerate() {
        match cell {
            // NULL and masked-TOAST columns are omitted: BigQuery loads a
            // missing NDJSON field as NULL, and the MERGE's mask keeps the
            // target value for masked ones.
            Cell::Null | Cell::UnchangedToast => {}
            Cell::Text(t) => {
                let s = std::str::from_utf8(t).map_err(|_| {
                    Error::Transfer(format!(
                        "log_based: column '{}' is not valid UTF-8 — a SQL_ASCII source can't \
                         land in BigQuery (which is UTF-8 only)",
                        cols[i]
                    ))
                })?;
                obj.insert(cols[i].clone(), json!(s));
            }
        }
    }
    serde_json::to_writer(&mut *out, &Value::Object(obj))
        .map_err(|e| Error::Transfer(format!("log_based: NDJSON encode: {e}")))?;
    out.push(b'\n');
    Ok(())
}

fn push_delete(out: &mut Vec<u8>, pk_cols: &[String], key: &Key) -> Result<()> {
    let mut obj = Map::new();
    obj.insert(OP_COL.to_string(), json!("D"));
    for (j, col) in pk_cols.iter().enumerate() {
        let s = std::str::from_utf8(&key[j])
            .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
        obj.insert(col.clone(), json!(s));
    }
    serde_json::to_writer(&mut *out, &Value::Object(obj))
        .map_err(|e| Error::Transfer(format!("log_based: NDJSON encode: {e}")))?;
    out.push(b'\n');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs.iter().map(|(n, t)| (n.to_string(), t.to_string())).collect()
    }

    #[test]
    fn cast_table_covers_the_bq_type_of_outputs() {
        for ty in ["STRING", "INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOL", "BYTES", "DATE", "TIMESTAMP", "DATETIME"] {
            assert!(cast_expr("c", 25, ty).is_ok(), "{ty} should be castable");
        }
        assert!(cast_expr("c", 25, "JSON").is_err());
        assert!(cast_expr("c", 25, "GEOGRAPHY").is_err());
        // A pg boolean stored as INT64 translates t/f → 1/0, not a failing CAST.
        let b = cast_expr("flag", BOOL_OID, "INT64").unwrap();
        assert!(b.contains("THEN 1") && b.contains("THEN 0") && !b.contains("CAST"), "{b}");
    }

    #[test]
    fn merge_shapes_pk_and_nonpk_and_truncate() {
        let wal = vec!["id".to_string(), "v".to_string(), "flag".to_string()];
        let pk = vec!["id".to_string()];
        let ty = types(&[("id", "INT64"), ("v", "STRING"), ("flag", "BOOL")]);
        let plan = ApplyPlan::build("orders", &wal, &[23, 25, 16], &pk, &ty).unwrap();
        let conn = BqConn::fake("proj", "ds");
        let sql = plan.merge_sql(&conn, "orders", false);
        assert!(sql.contains("MERGE `proj.ds.orders` T"), "{sql}");
        assert!(sql.contains("FROM `proj.ds.orders__apitap_cdc`"), "{sql}");
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        // PK is never in the UPDATE SET; both non-PK columns are.
        assert!(sql.contains("SUBSTR(S._apitap_mask, 2, 1)"), "{sql}"); // v at pos 2
        assert!(sql.contains("SUBSTR(S._apitap_mask, 3, 1)"), "{sql}"); // flag at pos 3
        assert!(!sql.contains("`id` = IF"), "PK must not be updated: {sql}");
        assert!(!sql.contains("NOT MATCHED BY SOURCE"), "no truncate clause: {sql}");
        let sqlt = plan.merge_sql(&conn, "orders", true);
        assert!(sqlt.contains("WHEN NOT MATCHED BY SOURCE THEN\n  DELETE"), "{sqlt}");
    }

    #[test]
    fn all_pk_table_drops_the_update_clause() {
        let wal = vec!["a".to_string(), "b".to_string()];
        let pk = vec!["a".to_string(), "b".to_string()];
        let ty = types(&[("a", "INT64"), ("b", "STRING")]);
        let plan = ApplyPlan::build("j", &wal, &[23, 25], &pk, &ty).unwrap();
        let sql = plan.merge_sql(&BqConn::fake("p", "d"), "j", false);
        assert!(!sql.contains("UPDATE SET"), "no non-PK cols → no UPDATE clause: {sql}");
        assert!(sql.contains("ON T.`a` = S.`a` AND T.`b` = S.`b`"), "{sql}");
    }

    #[test]
    fn upsert_omits_null_and_masked_columns() {
        let cols = vec!["id".to_string(), "v".to_string(), "big".to_string()];
        let mut out = Vec::new();
        push_upsert(&mut out, &cols, &[Cell::Text("7".into()), Cell::Null, Cell::UnchangedToast], Some("001")).unwrap();
        let line = String::from_utf8(out).unwrap();
        assert!(line.contains("\"_apitap_op\":\"U\""), "{line}");
        assert!(line.contains("\"_apitap_mask\":\"001\""), "{line}");
        assert!(line.contains("\"id\":\"7\""), "{line}");
        assert!(!line.contains("\"v\""), "null omitted: {line}");
        assert!(!line.contains("\"big\""), "masked omitted: {line}");
    }

    #[test]
    fn delete_carries_only_pk() {
        let mut out = Vec::new();
        push_delete(&mut out, &["id".to_string()], &vec![b"42".to_vec()]).unwrap();
        let line = String::from_utf8(out).unwrap();
        assert!(line.contains("\"_apitap_op\":\"D\""), "{line}");
        assert!(line.contains("\"id\":\"42\""), "{line}");
    }
}
