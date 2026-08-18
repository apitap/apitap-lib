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

// ── changelog mode (`changelog=true`) ───────────────────────────────────────
// Same shape and the same column names as the ClickHouse changelog, so one
// downstream query works against either engine: every captured operation is
// INSERTed, nothing is ever MERGEd, and `<table>__current` derives the current
// state. On BigQuery this also sidesteps the MERGE's ~7.3 s fixed job cost —
// the window becomes a load job plus one INSERT … SELECT.
const CL_LSN: &str = "_apitap_lsn";
const CL_SEQ: &str = "_apitap_seq";
const CL_AT: &str = "_apitap_at";
const CL_BASELINE: &str = "B";

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

    /// The destination's SHAPE must match the mode — see the ClickHouse twin.
    /// Checked at run start, because an empty drain never reaches the apply.
    pub(crate) async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
        let table = bare(dest_table);
        // Runs once per member table before anything is written, so the
        // destination name is vetted before any statement quotes it.
        crate::sink::bigquery::bq_ident("table", table)?;
        let is_cl = match self.conn.table_get(table).await? {
            Some(meta) => column_types(&meta)?.contains_key(OP_COL),
            // No table at all: nothing to disagree with.
            None => return Ok(()),
        };
        crate::logbased::dest_ch::is_shape_ok(is_cl, changelog, "BigQuery", table)
    }

    /// Same pre-flight as the ClickHouse twin, through the validators the
    /// rebuild would use anyway — so a wrong column is refused for the WHOLE
    /// group before any member commits a watermark.
    pub(crate) async fn validate_changelog_ddl(
        &self,
        dest_table: &str,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        if partition_by.is_none() && order_by.is_none() {
            return Ok(());
        }
        let table = bare(dest_table);
        let Some(meta) = self.conn.table_get(table).await? else { return Ok(()) };
        let mut types = column_types(&meta)?;
        types.insert(OP_COL.to_string(), "STRING".into());
        types.insert(CL_LSN.to_string(), "INT64".into());
        types.insert(CL_SEQ.to_string(), "INT64".into());
        types.insert(CL_AT.to_string(), "TIMESTAMP".into());
        if let Some(col) = partition_by {
            bq_partition_expr(col, &types)?;
        }
        if let Some(spec) = order_by {
            bq_cluster_list(spec, &types)?;
        }
        Ok(())
    }

    /// Remove this table's watermark rows — the state table is append-only with
    /// a `synced_at` tiebreak, so every row for the pair has to go.
    pub(crate) async fn clear_state(&self, dest_table: &str, source_id: &str) -> Result<()> {
        if !self.conn.cdc_state_table_exists().await? {
            return Ok(());
        }
        self.conn
            .cdc_script(&format!(
                "DELETE FROM {state} WHERE dest_table = '{t}' AND source_id = '{s}';",
                state = self.conn.state_fq(),
                t = sql_str(bare(dest_table)),
                s = sql_str(source_id),
            ))
            .await
    }

    /// changelog=true, once, right after the bootstrap's bulk load: rebuild the
    /// target as an append-only changelog and stamp the loaded rows `B`.
    ///
    /// A REBUILD for the same reason ClickHouse needs one — BigQuery cannot add
    /// partitioning to an existing table — and the CTAS gives the baseline rows
    /// a real op, the slot's LSN and a server timestamp instead of NULLs.
    ///
    /// The default partition is MONTHLY (`TIMESTAMP_TRUNC(_apitap_at, MONTH)`):
    /// a changelog is written forever, and daily partitions would hit
    /// BigQuery's per-table partition limit inside 30 years while monthly
    /// leaves centuries of headroom. `partition_by`/`order_by` override it —
    /// `order_by` maps to CLUSTER BY, BigQuery's only physical ordering.
    pub(crate) async fn changelog_bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        let table = bare(dest_table);
        self.conn.cdc_delete_table(&cdc_staging(table)).await?;
        self.conn.cdc_ensure_state_table().await?;
        let meta = match self.conn.table_get(table).await? {
            Some(m) => m,
            // A previous attempt died between the DROP and the RENAME: the
            // rebuilt table is sitting there under its temp name, complete.
            // Finish the move rather than declaring the destination lost.
            None => {
                let tmp = format!("{table}__apitap_cl");
                if self.conn.table_get(&tmp).await?.is_some() {
                    self.conn
                        .cdc_script(&format!(
                            "ALTER TABLE {} RENAME TO `{table}`",
                            self.conn.fq(&tmp)
                        ))
                        .await?;
                    self.ensure_current_view(table, pk_cols).await?;
                    return self.write_state(table, source_id, lsn, rows).await;
                }
                return Err(Error::Transfer(format!(
                    "log_based changelog: BigQuery target {table} does not exist — the \
                     bootstrap must run first"
                )));
            }
        };
        // Already a changelog (a re-bootstrap of a table we own)? Leave it.
        // ALL FOUR meta columns, never just `_apitap_op` — a source column that
        // legitimately owns that name would otherwise skip the rebuild and then
        // fail on every window forever, with the slot pinning WAL throughout.
        let types0 = column_types(&meta)?;
        let meta_cols = [OP_COL, CL_LSN, CL_SEQ, CL_AT];
        let n = meta_cols.iter().filter(|c| types0.contains_key(**c)).count();
        match n {
            0 => {}
            4 => return self.write_state(table, source_id, lsn, rows).await,
            n => {
                return Err(Error::InvalidInput(format!(
                    "log_based changelog: BigQuery target {table} already has {n} of the \
                     four reserved changelog columns ({OP_COL}, {CL_LSN}, {CL_SEQ}, \
                     {CL_AT}) — a source column is colliding with them. Rename it at the \
                     source or alias it in a view"
                )))
            }
        }

        let sel = changelog_select_list(&meta)?;
        let part = match partition_by {
            None => format!("TIMESTAMP_TRUNC({CL_AT}, MONTH)"),
            // The rebuild's own meta columns are legal partition targets — and
            // `_apitap_at` is the DOCUMENTED default — but they do not exist on
            // the pre-rebuild table this schema was read from, so they are
            // added before the lookup.
            Some(col) => {
                let mut t = types0.clone();
                t.insert(OP_COL.to_string(), "STRING".into());
                t.insert(CL_LSN.to_string(), "INT64".into());
                t.insert(CL_SEQ.to_string(), "INT64".into());
                t.insert(CL_AT.to_string(), "TIMESTAMP".into());
                bq_partition_expr(col, &t)?
            }
        };
        // BigQuery clusters on at most 4 columns, and only on plain column
        // references — hence the PK prefix rather than ClickHouse's key tuple.
        let cluster = match order_by {
            Some(spec) => bq_cluster_list(spec, &types0)?,
            None => pk_cols.iter().take(4).map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", "),
        };
        let cluster_sql =
            if cluster.trim().is_empty() { String::new() } else { format!("CLUSTER BY {cluster} ") };
        // THREE separate jobs, never one script.
        //
        // BigQuery DDL is not transactional, and `cdc_script` retries the whole
        // script text on a transient error. As one script, a blip on the RENAME
        // would re-run a CREATE … AS SELECT FROM <t> whose source the DROP had
        // already removed — the destination table simply gone. Split, each step
        // is separately retryable and the worst case is a loud failure with the
        // data intact under one name or the other.
        let tmp = format!("{table}__apitap_cl");
        let tmpfq = self.conn.fq(&tmp);
        let t = self.conn.fq(table);
        self.conn
            .cdc_script(&format!(
                "CREATE OR REPLACE TABLE {tmpfq} PARTITION BY {part} {cluster_sql}AS \
                 SELECT {sel}, \
                 '{b}' AS {OP_COL}, \
                 CAST({lsn} AS INT64) AS {CL_LSN}, \
                 CAST(0 AS INT64) AS {CL_SEQ}, \
                 CURRENT_TIMESTAMP() AS {CL_AT} \
                 FROM {t}",
                b = sql_str(CL_BASELINE),
            ))
            .await?;
        self.conn.cdc_script(&format!("DROP TABLE IF EXISTS {t}")).await?;
        self.conn
            .cdc_script(&format!("ALTER TABLE {tmpfq} RENAME TO `{table}`"))
            .await?;
        self.ensure_current_view(table, pk_cols).await?;
        self.write_state(table, source_id, lsn, rows).await
    }

    /// `<table>__current`: the current state derived from the log.
    ///
    /// The ClickHouse view's three rules, in BigQuery's dialect: drop everything
    /// at or below the newest `T`; take the latest record per key by the PAIR
    /// `(lsn, seq)` — one window stamps its end-LSN on every row it lands, so
    /// `seq` is what orders events inside a window; then drop keys whose newest
    /// record is `D`, AFTER the pick, or the delete would be skipped and the
    /// previous version would resurrect. BigQuery has no row-value comparison,
    /// so the pair test is spelled out.
    async fn ensure_current_view(&self, table: &str, pk_cols: &[String]) -> Result<()> {
        let t = self.conn.fq(table);
        let keys = pk_cols.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", ");
        // Every alias is `_apitap_`-prefixed and the PARTITION BY is qualified:
        // a table whose PK is called `s` or `l` would otherwise make the range
        // variables ambiguous and the view refuse to create.
        let keys_q = pk_cols
            .iter()
            .map(|c| format!("_apitap_s.`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = &keys;
        let sql = format!(
            "CREATE OR REPLACE VIEW {v} AS \
             SELECT * EXCEPT(_apitap_tr_l, _apitap_tr_s) FROM ( \
               SELECT _apitap_s.*, _apitap_tr.l AS _apitap_tr_l, _apitap_tr.s AS _apitap_tr_s \
               FROM {t} _apitap_s CROSS JOIN ( \
                 SELECT IFNULL(MAX({CL_LSN}), 0) AS l, IFNULL(MAX({CL_SEQ}), 0) AS s \
                 FROM {t} WHERE {OP_COL} = 'T' \
                   AND {CL_LSN} = (SELECT MAX({CL_LSN}) FROM {t} WHERE {OP_COL} = 'T') \
               ) _apitap_tr \
               WHERE _apitap_s.{CL_LSN} > _apitap_tr.l \
                  OR (_apitap_s.{CL_LSN} = _apitap_tr.l AND _apitap_s.{CL_SEQ} > _apitap_tr.s) \
               QUALIFY ROW_NUMBER() OVER ( \
                 PARTITION BY {keys_q} \
                 ORDER BY _apitap_s.{CL_LSN} DESC, _apitap_s.{CL_SEQ} DESC) = 1 \
             ) WHERE {OP_COL} != 'D'",
            v = self.conn.fq(&format!("{table}__current")),
        );
        self.conn.cdc_script(&sql).await
    }

    /// Apply ONE table's changelog window.
    pub(crate) async fn apply_changelog(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let one = [(
            dest_table.to_string(),
            qualified_src.to_string(),
            pk_cols.to_vec(),
            source_id.to_string(),
        )];
        Ok(self.apply_group_changelog(&one, outcome, 1).await?[0])
    }

    /// A whole group's changelog window: stage every table concurrently, then
    /// commit every INSERT plus every watermark in as few script jobs as
    /// possible — the same batching the MERGE path needs, for the same reason.
    ///
    /// Replay-safe without a dedup pass: the INSERT and the window's watermark
    /// row commit inside ONE transaction, so a window either landed whole or
    /// not at all, and a re-drained window re-lands from the same LSN.
    pub(crate) async fn apply_group_changelog(
        &self,
        ctxs: &[(String, String, Vec<String>, String)],
        outcome: &DrainOutcome,
        lanes: usize,
    ) -> Result<Vec<u64>> {
        use futures::stream::{StreamExt as _, TryStreamExt as _};
        let (me, cref, oref) = (self, ctxs, outcome);
        let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
            .map(|i| async move {
                let (dt, q, pk, sid) = &cref[i];
                me.stage_changelog(dt, q, pk, oref, sid).await.map(|(ev, sql)| (i, ev, sql))
            })
            .buffer_unordered(lanes.max(1))
            .try_collect()
            .await?;

        let mut rows = vec![0u64; ctxs.len()];
        let mut stmts: Vec<String> = Vec::new();
        for (i, ev, sql) in staged {
            rows[i] = ev;
            stmts.extend(sql);
        }
        if stmts.is_empty() {
            return Ok(rows);
        }
        const CHUNK_BYTES: usize = 256 << 10;
        let (mut batch, mut len) = (Vec::new(), 0usize);
        for s in stmts {
            if len + s.len() > CHUNK_BYTES && !batch.is_empty() {
                self.commit_batch(&batch).await?;
                batch.clear();
                len = 0;
            }
            len += s.len();
            batch.push(s);
        }
        if !batch.is_empty() {
            self.commit_batch(&batch).await?;
        }
        Ok(rows)
    }

    /// One readback per window: the current value of every masked column for
    /// every key that needs one. `__current` filters the log by key first and
    /// the table is CLUSTERed on the PK, so this prunes rather than scans.
    async fn read_current(
        &self,
        table: &str,
        pk_cols: &[String],
        keys: &[crate::logbased::changelog::CKey],
        cols: &[usize],
        wal_cols: &[String],
    ) -> Result<std::collections::HashMap<crate::logbased::changelog::CKey, Vec<Option<bytes::Bytes>>>>
    {
        let bt = |c: &str| format!("`{c}`");
        let sel = pk_cols
            .iter()
            .map(|c| format!("CAST({} AS STRING)", bt(c)))
            .chain(cols.iter().map(|&i| format!("CAST({} AS STRING)", bt(&wal_cols[i]))))
            .collect::<Vec<_>>()
            .join(", ");
        let mut preds = Vec::with_capacity(keys.len());
        for k in keys {
            let mut parts = Vec::with_capacity(pk_cols.len());
            for (c, v) in pk_cols.iter().zip(k.iter()) {
                let txt = std::str::from_utf8(v)
                    .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
                parts.push(format!("CAST({} AS STRING) = '{}'", bt(c), sql_str(txt)));
            }
            preds.push(format!("({})", parts.join(" AND ")));
        }
        let rows = self
            .conn
            .cdc_query(&format!(
                "SELECT {sel} FROM {v} WHERE {p}",
                v = self.conn.fq(&format!("{table}__current")),
                p = preds.join(" OR "),
            ))
            .await?;
        let np = pk_cols.len();
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        for row in rows {
            if row.len() != np + cols.len() {
                return Err(Error::Transfer(
                    "log_based changelog: masked readback column count mismatch".into(),
                ));
            }
            let key: crate::logbased::changelog::CKey = row[..np]
                .iter()
                .map(|x| x.clone().unwrap_or_default().into_bytes())
                .collect();
            let vals = row[np..]
                .iter()
                .map(|x| x.clone().map(bytes::Bytes::from))
                .collect();
            out.insert(key, vals);
        }
        Ok(out)
    }

    /// One table's changelog window: every captured event as a staging row,
    /// loaded, then handed back as the INSERT + watermark the group commits.
    async fn stage_changelog(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<(u64, Vec<String>)> {
        let table = bare(dest_table);
        let state_sql = |ev| self.state_insert_sql(table, source_id, outcome.end_lsn, ev);
        let Some(c) = outcome.changes.get(qualified_src) else {
            return Ok((0, vec![format!("{};", state_sql(0))]));
        };
        if c.events.is_empty() {
            return Ok((0, vec![format!("{};", state_sql(0))]));
        }
        let wal_cols = outcome
            .wal_cols
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;
        for name in wal_cols {
            if matches!(name.as_str(), OP_COL | MASK_COL | CL_LSN | CL_SEQ | CL_AT) {
                return Err(Error::InvalidInput(format!(
                    "log_based changelog: source column '{name}' collides with a reserved \
                     changelog column — rename it at the source or alias it in a view"
                )));
            }
        }
        let oids = outcome
            .wal_oids
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL type list".into()))?;
        let meta = self.conn.table_get(table).await?.ok_or_else(|| {
            Error::Transfer(format!(
                "log_based changelog: BigQuery target {table} does not exist — the \
                 bootstrap must run before a CDC window can apply"
            ))
        })?;
        let types = column_types(&meta)?;
        let plan = ApplyPlan::build(table, wal_cols, oids, &[], &types)?;

        // Rebuild unchanged-TOAST cells before anything is staged: writing them
        // as NULL would silently blank the column for every reader of
        // `__current`. One extra query per window, only when a mask is present.
        let patched = if c.masked {
            let pk_idx = pk_indices(pk_cols, wal_cols)?;
            let (keys, cols) = c.mask_plan(&pk_idx);
            let base = if keys.is_empty() || cols.is_empty() {
                std::collections::HashMap::new()
            } else {
                self.read_current(table, pk_cols, &keys, &cols, wal_cols).await?
            };
            c.resolve_masked(&pk_idx, &cols, &base, wal_cols)?
        } else {
            std::collections::HashMap::new()
        };

        let mut ndjson: Vec<u8> = Vec::new();
        for (seq, ev) in c.events.iter().enumerate() {
            let row = patched.get(&seq).or(ev.row.as_ref());
            push_change(&mut ndjson, wal_cols, row, ev.op.code(), seq)?;
        }
        self.conn
            .cdc_load_ndjson(&cdc_staging(table), &plan.staging_fields_changelog(), ndjson)
            .await?;

        let bt = |c: &str| format!("`{c}`");
        let into = wal_cols
            .iter()
            .map(|c| bt(c))
            .chain([bt(OP_COL), bt(CL_LSN), bt(CL_SEQ), bt(CL_AT)])
            .collect::<Vec<_>>()
            .join(", ");
        let sel = plan
            .cast
            .iter()
            .cloned()
            .chain([
                bt(OP_COL),
                format!("CAST({} AS INT64)", outcome.end_lsn),
                format!("CAST({} AS INT64)", bt(CL_SEQ)),
                // One stamp for the whole window: it is the PARTITION and
                // retention key, never an ordering key — `(lsn, seq)` orders.
                "CURRENT_TIMESTAMP()".to_string(),
            ])
            .collect::<Vec<_>>()
            .join(", ");
        Ok((
            c.count,
            vec![
                format!(
                    "INSERT INTO {t} ({into}) SELECT {sel} FROM {s};",
                    t = self.conn.fq(table),
                    s = self.conn.fq(&cdc_staging(table)),
                ),
                format!("{};", state_sql(c.count)),
            ],
        ))
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

    /// Apply ONE table's window. Kept for the single-table paths; it is
    /// `apply_group` over a one-element group.
    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let one = [(
            dest_table.to_string(),
            qualified_src.to_string(),
            pk_cols.to_vec(),
            source_id.to_string(),
        )];
        Ok(self.apply_group(&one, outcome, 1).await?[0])
    }

    /// Apply a whole group's window: STAGE every table concurrently (each is a
    /// load job), then COMMIT them all in as few script jobs as possible.
    ///
    /// This is the shape that matters on BigQuery. A MERGE costs ~7.3 s of pure
    /// job overhead plus ~0.08 s per 1000 staged rows (measured: 2K rows →
    /// 7.8 s, 52K rows → 11.5 s), so a 10-table group applied as 10 separate
    /// scripts paid that 7.3 s floor TEN times. Batched into one
    /// `BEGIN … COMMIT` the floor is paid once, the group's watermark rows go in
    /// with it, and the concurrent-update contention on `_apitap_state`
    /// disappears because there is only one transaction.
    pub(crate) async fn apply_group(
        &self,
        ctxs: &[(String, String, Vec<String>, String)],
        outcome: &DrainOutcome,
        lanes: usize,
    ) -> Result<Vec<u64>> {
        use futures::stream::{StreamExt as _, TryStreamExt as _};
        let (me, cref, oref) = (self, ctxs, outcome);
        let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
            .map(|i| async move {
                let (dt, q, pk, sid) = &cref[i];
                me.stage(dt, q, pk, oref, sid).await.map(|(ev, sql)| (i, ev, sql))
            })
            .buffer_unordered(lanes.max(1))
            .try_collect()
            .await?;

        let mut rows = vec![0u64; ctxs.len()];
        let mut stmts: Vec<String> = Vec::new();
        for (i, ev, sql) in staged {
            rows[i] = ev;
            stmts.extend(sql);
        }
        if stmts.is_empty() {
            return Ok(rows);
        }
        // Chunk by size: BigQuery caps a query at ~1 MB, and a 100-table group's
        // MERGEs would approach it. Each chunk is its own transaction — the
        // window is replay-idempotent, so a crash between chunks converges.
        const CHUNK_BYTES: usize = 256 << 10;
        let dbg = std::env::var("APITAP_DEBUG").is_ok();
        let t_merge = std::time::Instant::now();
        let (mut batch, mut len, mut jobs) = (Vec::new(), 0usize, 0u32);
        for s in stmts {
            if len + s.len() > CHUNK_BYTES && !batch.is_empty() {
                self.commit_batch(&batch).await?;
                jobs += 1;
                batch.clear();
                len = 0;
            }
            len += s.len();
            batch.push(s);
        }
        if !batch.is_empty() {
            self.commit_batch(&batch).await?;
            jobs += 1;
        }
        if dbg {
            eprintln!(
                "[bq apply] group of {} table(s) committed in {jobs} script job(s), {:.1}s",
                ctxs.len(),
                t_merge.elapsed().as_secs_f64()
            );
        }
        Ok(rows)
    }

    async fn commit_batch(&self, stmts: &[String]) -> Result<()> {
        self.conn
            .cdc_script(&format!(
                "BEGIN TRANSACTION;\n{}\nCOMMIT TRANSACTION;",
                stmts.join("\n")
            ))
            .await
    }

    /// Build one table's window body, load it into the staging table, and
    /// return the statements it contributes to the group's commit script.
    async fn stage(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<(u64, Vec<String>)> {
        let table = bare(dest_table);
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            return Ok((
                0,
                vec![self.state_insert_sql(table, source_id, outcome.end_lsn, 0)],
            ));
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
        // A replica window must not land on a table apitap already made into a
        // changelog: the MERGE would UPDATE historical records in place and
        // DELETE past events, quietly destroying the log it found. Free to
        // check — the target's schema is already in hand.
        if types.contains_key(CL_LSN) {
            return Err(Error::InvalidInput(format!(
                "log_based: BigQuery target {table} is a CHANGELOG (it has an \
                 {CL_LSN} column) but this run asked for a replica — pass \
                 changelog=True, or point at a different dest_table"
            )));
        }
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
            let mut out = Vec::new();
            if c.truncate {
                out.push(format!("DELETE FROM {} WHERE TRUE;", self.conn.fq(table)));
            }
            out.push(format!("{state_sql};"));
            return Ok((c.events, out));
        }

        // Land the window in this table's staging table (its own load job), then
        // hand the MERGE + watermark back so the GROUP commits them in one job.
        let dbg = std::env::var("APITAP_DEBUG").is_ok();
        let nbytes = ndjson.len();
        let t_load = std::time::Instant::now();
        self.conn
            .cdc_load_ndjson(&cdc_staging(table), &plan.staging_fields(), ndjson)
            .await?;
        if dbg {
            eprintln!(
                "[bq stage] {table}: {staged} staging rows / {:.1}MB, load={:.1}s",
                nbytes as f64 / (1 << 20) as f64,
                t_load.elapsed().as_secs_f64(),
            );
        }
        Ok((
            c.events,
            vec![
                format!("{};", plan.merge_sql(&self.conn, table, c.truncate)),
                format!("{state_sql};"),
            ],
        ))
    }

    /// The source-identity marker: an ordinary state row under a reserved
    /// `source_id`, so nothing about the state table has to change.
    pub(crate) async fn write_marker(
        &self,
        dest_table: &str,
        source_id: &str,
        value: u64,
    ) -> Result<()> {
        self.write_state(bare(dest_table), source_id, value, 0).await
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
        // Every statement this struct produces pastes these names between
        // backticks, so they are vetted once here rather than at each of the
        // dozen sites that format them.
        crate::sink::bigquery::bq_ident("table", table)?;
        for name in wal_cols.iter().chain(pk_cols.iter()) {
            crate::sink::bigquery::bq_ident("column", name)?;
        }
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

    /// Staging schema for a changelog window: the op and the in-window sequence
    /// alongside the data columns. The window's LSN and timestamp are constant
    /// for the whole window, so they go in the INSERT's SELECT list instead of
    /// being repeated on every staged row.
    fn staging_fields_changelog(&self) -> Value {
        let mut fields = vec![
            json!({"name": OP_COL, "type": "STRING", "mode": "REQUIRED"}),
            json!({"name": CL_SEQ, "type": "STRING", "mode": "REQUIRED"}),
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

/// One changelog record: the op, its in-window sequence, and whatever the
/// event's row image carries. A delete's old image IS the delete record, so it
/// renders like any other row; a TRUNCATE has no row at all and every data
/// column is simply absent (BigQuery loads a missing NDJSON field as NULL).
///
/// An unchanged-TOAST cell also lands NULL — honest information in a log:
/// "this update did not carry that column". The `U` record's presence is what
/// tells the reader the row changed.
fn push_change(
    out: &mut Vec<u8>,
    cols: &[String],
    row: Option<&crate::wire::pgoutput::Tuple>,
    op: &str,
    seq: usize,
) -> Result<()> {
    use crate::wire::pgoutput::Cellv;
    let mut obj = Map::new();
    obj.insert(OP_COL.to_string(), json!(op));
    obj.insert(CL_SEQ.to_string(), json!(seq.to_string()));
    if let Some(row) = row {
        for (i, name) in cols.iter().enumerate() {
            match row.get(i) {
                Some(Cellv::Text(t)) => {
                    let s = std::str::from_utf8(t).map_err(|_| {
                        Error::Transfer(format!(
                            "log_based: column '{name}' is not valid UTF-8 — a SQL_ASCII \
                             source can't land in BigQuery (which is UTF-8 only)"
                        ))
                    })?;
                    obj.insert(name.clone(), json!(s));
                }
                Some(Cellv::Null) | Some(Cellv::UnchangedToast) | None => {}
            }
        }
    }
    serde_json::to_writer(&mut *out, &Value::Object(obj))
        .map_err(|e| Error::Transfer(format!("log_based: NDJSON encode: {e}")))?;
    out.push(b'\n');
    Ok(())
}

/// `order_by` becomes BigQuery's CLUSTER BY, which takes COLUMN NAMES — never
/// an expression. Validated against the target's real columns and re-quoted
/// here rather than interpolated: `cdc_script` runs a multi-statement script,
/// so a `;` in this value would chain statements of the caller's choosing.
fn bq_cluster_list(
    spec: &str,
    types: &std::collections::HashMap<String, String>,
) -> Result<String> {
    let mut out = Vec::new();
    for raw in spec.split(',') {
        let name = raw.trim().trim_matches('`').trim();
        if name.is_empty() {
            continue;
        }
        if !types.contains_key(name) {
            return Err(Error::InvalidInput(format!(
                "log_based changelog: order_by='{spec}' — BigQuery clusters on COLUMN \
                 NAMES, and '{name}' is not a column of the target. Give up to four of \
                 its own columns, comma-separated (expressions are ClickHouse-only)"
            )));
        }
        out.push(format!("`{name}`"));
    }
    if out.is_empty() {
        return Err(Error::InvalidInput(format!(
            "log_based changelog: order_by='{spec}' names no columns"
        )));
    }
    if out.len() > 4 {
        return Err(Error::InvalidInput(format!(
            "log_based changelog: order_by names {} columns — BigQuery clusters on at \
             most 4",
            out.len()
        )));
    }
    Ok(out.join(", "))
}

/// `partition_by` for BigQuery is a COLUMN NAME, not an expression — BigQuery
/// only partitions on a real column, and the DDL it needs depends on that
/// column's declared type. Everything lands MONTHLY, the same granularity as
/// the ClickHouse default and for the same reason: a changelog outlives daily
/// partitioning long before it outlives monthly.
///
/// A `DATE` column gets `DATE_TRUNC(c, MONTH)` rather than being used bare —
/// bare is DAILY, which is the time bomb monthly exists to defuse.
fn bq_partition_expr(
    col: &str,
    types: &std::collections::HashMap<String, String>,
) -> Result<String> {
    let ty = types.get(col).map(String::as_str).ok_or_else(|| {
        Error::InvalidInput(format!(
            "log_based changelog: partition_by='{col}' is not a column of the BigQuery \
             target — partition on one of its own columns, or leave it unset for \
             monthly on {CL_AT}"
        ))
    })?;
    match ty {
        "DATE" => Ok(format!("DATE_TRUNC(`{col}`, MONTH)")),
        "TIMESTAMP" => Ok(format!("TIMESTAMP_TRUNC(`{col}`, MONTH)")),
        "DATETIME" => Ok(format!("DATETIME_TRUNC(`{col}`, MONTH)")),
        other => Err(Error::InvalidInput(format!(
            "log_based changelog: BigQuery cannot partition on column '{col}' of type \
             {other} — partitioning must be by time (DATE/TIMESTAMP/DATETIME). Put \
             '{col}' in order_by instead (it becomes the cluster key, which prunes \
             just as well), or leave partition_by unset for monthly on {CL_AT}"
        ))),
    }
}

/// The SELECT list that carries a bootstrapped table's own columns into the
/// changelog rebuild. Every data column must accept NULL — a delete carries
/// only the key, a truncate carries nothing — and a CAST is what makes a
/// REQUIRED column NULLABLE in the CTAS output. Columns apitap can't cast
/// (RECORD/REPEATED) ride through as plain references: they are already
/// NULLABLE unless a user declared otherwise, and a bad DDL fails loudly here
/// rather than silently later.
fn changelog_select_list(meta: &Value) -> Result<String> {
    let fields = meta["schema"]["fields"]
        .as_array()
        .ok_or_else(|| Error::Transfer("log_based: BigQuery table has no schema".into()))?;
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        let Some(n) = f["name"].as_str() else { continue };
        let ty = f["type"].as_str().unwrap_or("");
        let required = f["mode"].as_str() == Some("REQUIRED");
        let scalar = !matches!(ty, "RECORD" | "STRUCT") && f["mode"].as_str() != Some("REPEATED");
        if required && scalar {
            out.push(format!("CAST(`{n}` AS {}) AS `{n}`", canonical_type(ty)));
        } else {
            out.push(format!("`{n}`"));
        }
    }
    if out.is_empty() {
        return Err(Error::Transfer(
            "log_based changelog: BigQuery target has no columns — the bootstrap must run first"
                .into(),
        ));
    }
    Ok(out.join(", "))
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
    fn cluster_list_takes_column_names_and_refuses_sql() {
        let t = types(&[("id", "INT64"), ("cust", "STRING"), ("v", "STRING"), ("w", "STRING"), ("x", "STRING")]);
        assert_eq!(bq_cluster_list("id, cust", &t).unwrap(), "`id`, `cust`");
        assert_eq!(bq_cluster_list("`id`", &t).unwrap(), "`id`");
        // A `;` would otherwise chain statements inside the rebuild script.
        assert!(bq_cluster_list("id; DROP TABLE x", &t).is_err());
        // ClickHouse-style expressions are not BigQuery cluster keys.
        assert!(bq_cluster_list("toYYYYMM(ts)", &t).is_err());
        assert!(bq_cluster_list("nope", &t).is_err());
        assert!(bq_cluster_list("", &t).is_err());
        // BigQuery clusters on at most four columns.
        assert!(bq_cluster_list("id, cust, v, w, x", &t).is_err());
    }

    #[test]
    fn partition_by_is_a_time_column_and_always_monthly() {
        let t = types(&[
            ("_apitap_at", "TIMESTAMP"),
            ("d", "DATE"),
            ("dt", "DATETIME"),
            ("name", "STRING"),
            ("n", "INT64"),
        ]);
        // A DATE column is NOT used bare — bare is daily.
        assert_eq!(bq_partition_expr("d", &t).unwrap(), "DATE_TRUNC(`d`, MONTH)");
        assert_eq!(bq_partition_expr("dt", &t).unwrap(), "DATETIME_TRUNC(`dt`, MONTH)");
        assert_eq!(
            bq_partition_expr("_apitap_at", &t).unwrap(),
            "TIMESTAMP_TRUNC(`_apitap_at`, MONTH)"
        );
        // BigQuery cannot partition on a STRING — the op column belongs in
        // the cluster key, and saying so beats a raw BigQuery DDL error.
        assert!(bq_partition_expr("name", &t).is_err());
        assert!(bq_partition_expr("n", &t).is_err());
        assert!(bq_partition_expr("nope", &t).is_err());
    }

    #[test]
    fn changelog_rebuild_forces_every_data_column_nullable() {
        // A delete carries only the key and a truncate carries nothing, so a
        // REQUIRED column would reject its own changelog.
        let meta = json!({"schema": {"fields": [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "note", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        ]}});
        assert_eq!(
            changelog_select_list(&meta).unwrap(),
            "CAST(`id` AS INT64) AS `id`, `note`, `tags`"
        );
    }

    #[test]
    fn changelog_staging_carries_the_op_and_the_sequence() {
        let plan = ApplyPlan::build(
            "t",
            &["id".into(), "v".into()],
            &[23, 25],
            &["id".into()],
            &types(&[("id", "INT64"), ("v", "STRING")]),
        )
        .unwrap();
        let f = plan.staging_fields_changelog();
        let names: Vec<&str> =
            f.as_array().unwrap().iter().map(|x| x["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["_apitap_op", "_apitap_seq", "id", "v"]);
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
