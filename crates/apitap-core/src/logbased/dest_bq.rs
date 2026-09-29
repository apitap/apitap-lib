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
//! Every transaction is fenced on the run's own table, `_apitap_fence<token>`:
//! its first statement updates that table's one row `WHERE NOT claimed`, and a
//! collector marks it claimed before it proceeds, so a script that overlaps a
//! claim rolls back whole. Bootstrap DDL, which no transaction can hold, is
//! guarded by a server-side ASSERT in the same script. The staging and rebuild
//! tables carry the run's token, so two drains sharing a dataset never share
//! one. Every statement that reaches BigQuery is in `mod store`; the apply
//! bodies write through the `BqUnit` they are handed.
//!
//! Because CDC needs row-level DELETE/UPDATE (DML), log_based → BigQuery
//! requires a project with billing enabled; sandbox/free-tier projects reject
//! DML and the first MERGE will fail loudly.

use crate::error::{Error, Result};
use crate::lease::Watermark;
use crate::logbased::collapse::Key;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::resolve::{resolve_window, Fin};
use crate::logbased::rowtext::pk_indices;
use crate::sink::bigquery::sql_str;
use crate::wire::pgoutput::Cell;
use serde_json::{json, Map, Value};
use std::collections::HashSet;

pub(crate) use store::{BqStore, BqUnit};

const OP_COL: &str = "_apitap_op";
const MASK_COL: &str = "_apitap_mask";

/// One chunk is one BigQuery transaction, so this is what decides which
/// statements are atomic with which. BigQuery caps a query at ~1 MB, and a
/// 100-table group's MERGEs would approach it.
const CHUNK_BYTES: usize = 256 << 10;

/// Pack statement GROUPS into transaction-sized batches without ever splitting
/// a group.
///
/// A group is one table's statements plus its own watermark row. Packing the
/// flattened statements by byte size — what this did until 0.56.0 — let a
/// boundary fall between those two, which committed a changelog window's rows
/// in one transaction and its watermark in the next; any failure in between
/// replayed the whole window into an append-only table. A group larger than
/// `limit` still goes out whole: an oversized transaction is correct, a split
/// one is not. The replica path packs the same way — its MERGE and its
/// watermark are one group too.
fn pack_whole_groups(groups: Vec<Vec<String>>, limit: usize) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let (mut batch, mut len) = (Vec::new(), 0usize);
    for g in groups {
        let glen: usize = g.iter().map(String::len).sum();
        if len + glen > limit && !batch.is_empty() {
            out.push(std::mem::take(&mut batch));
            len = 0;
        }
        len += glen;
        batch.extend(g);
    }
    if !batch.is_empty() {
        out.push(batch);
    }
    out
}

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

/// One CDC member: (destination table, qualified source, key columns,
/// source id) — the shape `run.rs` hands a group in.
pub(crate) type Member = (String, String, Vec<String>, String);

pub(crate) struct BqDest {
    store: BqStore,
}

/// `dest_table` may arrive schema-qualified; the BigQuery dataset comes from the
/// URL, so only the bare name addresses the table (same trim as the sink).
fn bare(dest_table: &str) -> &str {
    dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t)
}

impl BqDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        Ok(Self { store: BqStore::connect(url).await? })
    }

    /// The store: the lease and its fence table, the guard, and the units a
    /// run's tenure takes. One run's closes are serialized by the tenure
    /// (`Fence::serial_commit`): each updates the same fence row.
    pub(crate) fn store(&self) -> &BqStore {
        &self.store
    }

    /// The drain's watermark, through the one verdict both lanes share. A
    /// NULL watermark on the drain's own row is now an error here too: read
    /// as "no state" it re-bootstrapped a table whose slot had moved on.
    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<crate::naming::CdcWatermark>> {
        let t = bare(dest_table);
        crate::naming::cdc_watermark(t, self.store.read_state(t, source_id).await?)
    }

    /// The destination's SHAPE must match the mode — see the ClickHouse twin.
    /// Checked at run start, because an empty drain never reaches the apply.
    pub(crate) async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
        let table = bare(dest_table);
        // Runs once per member table before anything is written, so the
        // destination name is vetted before any statement quotes it.
        crate::sink::bigquery::bq_ident("table", table)?;
        let is_cl = match self.store.table_meta(table).await? {
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
        let Some(meta) = self.store.table_meta(table).await? else { return Ok(()) };
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

    /// The bootstrap's replace just (re)created the target and wrote a `*`
    /// barrier. Inside the unit whose close stamps the slot's LSN (with a
    /// server-clock timestamp, so it sorts AFTER that barrier): drop what older
    /// releases left, and CLUSTER the target on its PK, so every window's MERGE
    /// prunes to the touched blocks instead of full-scanning — the MERGE is the
    /// dominant per-window cost.
    pub(crate) async fn bootstrap_finish(
        &self,
        u: &mut BqUnit<'_>,
        dest_table: &str,
        pk_cols: &[String],
        rows: u64,
    ) -> Result<()> {
        bootstrap_unit(u, dest_table, pk_cols, rows).await
    }

    /// changelog=true, once, right after the bootstrap's bulk load — see
    /// `changelog_bootstrap_unit`.
    pub(crate) async fn changelog_bootstrap_finish(
        &self,
        u: &mut BqUnit<'_>,
        dest_table: &str,
        pk_cols: &[String],
        lsn: u64,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        changelog_bootstrap_unit(u, dest_table, pk_cols, lsn, partition_by, order_by).await
    }

    /// A whole group's window in one unit — see `apply_group_unit` and
    /// `apply_group_changelog_unit`. Each member's watermark comes back for
    /// the unit's close, which commits it in that member's own group.
    pub(crate) async fn apply_group(
        &self,
        u: &mut BqUnit<'_>,
        ctxs: &[Member],
        outcome: &DrainOutcome,
        lanes: usize,
        changelog: bool,
    ) -> Result<Vec<(u64, Watermark)>> {
        if changelog {
            apply_group_changelog_unit(u, ctxs, outcome, lanes).await
        } else {
            apply_group_unit(u, ctxs, outcome, lanes).await
        }
    }
}

/// The PK columns go straight into DDL from here — the clustering list, the
/// current-view, the changelog rebuild — all of which run BEFORE the first
/// window, and so before `ApplyPlan::build` (which vets them for the apply)
/// has ever seen them.
fn vet_pk(pk_cols: &[String]) -> Result<()> {
    for k in pk_cols {
        crate::sink::bigquery::bq_ident("primary key column", k)?;
    }
    Ok(())
}

/// A replica bootstrap's finish, inside its unit: drop what older releases
/// left untokenized, and cluster the fresh target. The watermark is the
/// unit's close.
async fn bootstrap_unit(u: &mut BqUnit<'_>, dest_table: &str, pk_cols: &[String], rows: u64) -> Result<()> {
    vet_pk(pk_cols)?;
    let table = bare(dest_table);
    u.drop_legacy_scratch(table).await?;
    cluster_target(u, table, pk_cols, rows).await
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
async fn changelog_bootstrap_unit(
    u: &mut BqUnit<'_>,
    dest_table: &str,
    pk_cols: &[String],
    lsn: u64,
    partition_by: Option<&str>,
    order_by: Option<&str>,
) -> Result<()> {
    vet_pk(pk_cols)?;
    let table = bare(dest_table);
    let meta = match u.table_get(table).await? {
        Some(m) => m,
        // A previous attempt died between the DROP and the RENAME: the
        // rebuilt table is sitting there under its temp name, complete.
        // Finish the move rather than declaring the destination lost.
        None => {
            if u.heal(table).await? {
                return ensure_current_view(u, table, pk_cols).await;
            }
            return Err(Error::Transfer(format!(
                "log_based changelog: BigQuery target {table} does not exist — the \
                 bootstrap must run first"
            )));
        }
    };
    // After the heal check: the legacy temp may be the only copy it finds.
    u.drop_legacy_scratch(table).await?;
    // Already a changelog (a re-bootstrap of a table we own)? Leave it.
    // ALL FOUR meta columns, never just `_apitap_op` — a source column that
    // legitimately owns that name would otherwise skip the rebuild and then
    // fail on every window forever, with the slot pinning WAL throughout.
    let types0 = column_types(&meta)?;
    let meta_cols = [OP_COL, CL_LSN, CL_SEQ, CL_AT];
    match meta_cols.iter().filter(|c| types0.contains_key(**c)).count() {
        0 => {}
        4 => return Ok(()),
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
    let cluster_sql = if cluster.trim().is_empty() { String::new() } else { format!("CLUSTER BY {cluster} ") };
    // TWO guarded scripts, never one: BigQuery DDL is not transactional, and
    // `cdc_script` retries the whole script text on a transient error. The
    // CTAS reads the target and writes only this run's temp, so repeating it
    // is harmless; the swap (`BqUnit::swap`) moves the temp only while it
    // exists, so repeating THAT is harmless too. As one script, a blip after
    // the DROP would re-run a CTAS whose source was already gone.
    let tmp = u.tmp(table);
    u.ddl(&format!(
        "CREATE OR REPLACE TABLE {tmpfq} PARTITION BY {part} {cluster_sql}AS \
         SELECT {sel}, \
         '{b}' AS {OP_COL}, \
         CAST({lsn} AS INT64) AS {CL_LSN}, \
         CAST(0 AS INT64) AS {CL_SEQ}, \
         CURRENT_TIMESTAMP() AS {CL_AT} \
         FROM {t};",
        tmpfq = u.fq(&tmp),
        t = u.fq(table),
        b = sql_str(CL_BASELINE),
    ))
    .await?;
    u.swap(&tmp, table).await?;
    // The swap's IF reads the catalog; if it ever read it stale, the table
    // would still be the replica. Refused here rather than on the first window.
    let rebuilt = u.table_get(table).await?.map(|m| column_types(&m)).transpose()?;
    if !rebuilt.is_some_and(|t| t.contains_key(CL_LSN)) {
        return Err(Error::Transfer(format!(
            "log_based changelog: rebuilding BigQuery target {table} did not take — \
             {tmp} was not swapped in. Re-run to retry the bootstrap"
        )));
    }
    ensure_current_view(u, table, pk_cols).await
}

/// `<table>__current`: the current state derived from the log.
///
/// The ClickHouse view's three rules, in BigQuery's dialect: drop everything
/// at or below the newest `T`; take the latest record per key by the PAIR
/// `(lsn, seq)` — one window stamps its START-LSN on every row it lands, so
/// `seq` is what orders events inside a window; then drop keys whose newest
/// record is `D`, AFTER the pick, or the delete would be skipped and the
/// previous version would resurrect. BigQuery has no row-value comparison,
/// so the pair test is spelled out.
async fn ensure_current_view(u: &mut BqUnit<'_>, table: &str, pk_cols: &[String]) -> Result<()> {
    let sql = current_view_sql(&u.fq(&format!("{table}__current")), &u.fq(table), pk_cols);
    u.ddl(&sql).await
}

fn current_view_sql(v: &str, t: &str, pk_cols: &[String]) -> String {
    // Every alias is `_apitap_`-prefixed and the PARTITION BY is qualified:
    // a table whose PK is called `s` or `l` would otherwise make the range
    // variables ambiguous and the view refuse to create.
    let keys_q = pk_cols
        .iter()
        .map(|c| format!("_apitap_s.`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
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
             ORDER BY _apitap_s.{CL_LSN} DESC, _apitap_s.{CL_SEQ} DESC, \
                      _apitap_s.{OP_COL} = '{base}' ASC) = 1 \
         ) WHERE {OP_COL} != 'D';",
        // The tie-break, load-bearing since 0.56.0: a window is stamped with
        // the watermark it was drained FROM, and the first window after a
        // bootstrap starts exactly where the baseline snapshot was taken —
        // so a baseline row (always seq 0) and that window's first event for
        // the same key can carry the identical (lsn, seq). A real change
        // always outranks the snapshot it changed.
        base = CL_BASELINE,
    )
}

/// A whole group's changelog window: stage every table concurrently (one
/// load job each), then hand each table's INSERT to its own group of the unit
/// — whose close appends that table's watermark row to the SAME group and
/// commits whole groups, as few transactions as fit.
///
/// Replay-safe without a dedup pass: the INSERT and the window's watermark
/// row commit inside ONE transaction, so a window either landed whole or
/// not at all, and a re-drained window re-lands from the same LSN.
///
/// Both halves of that were untrue until 0.56.0. The chunker packed a FLAT
/// list of statements by byte size, so a boundary could fall between a
/// table's INSERT and its watermark and put them in two transactions; it
/// packs whole groups now. And the stamp was `end_lsn`, which a re-drain
/// recomputes — so "re-lands from the same LSN" was false and `(lsn, seq)`
/// could not be used to de-duplicate. It is `start_lsn` now.
pub(crate) async fn apply_group_changelog_unit(
    u: &mut BqUnit<'_>,
    ctxs: &[Member],
    outcome: &DrainOutcome,
    lanes: usize,
) -> Result<Vec<(u64, Watermark)>> {
    use futures::stream::{StreamExt as _, TryStreamExt as _};
    let (ur, cref, oref): (&BqUnit<'_>, _, _) = (&*u, ctxs, outcome);
    let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
        .map(|i| async move {
            let (dt, q, pk, _) = &cref[i];
            stage_changelog(ur, dt, q, pk, oref).await.map(|(ev, sql)| (i, ev, sql))
        })
        .buffer_unordered(lanes.max(1))
        .try_collect()
        .await?;
    Ok(into_groups(u, ctxs, outcome, staged))
}

/// A whole group's replica window: STAGE every table concurrently (each is a
/// load job), then hand each table's MERGE to its own group of the unit.
///
/// This is the shape that matters on BigQuery. A MERGE costs ~7.3 s of pure
/// job overhead plus ~0.08 s per 1000 staged rows (measured: 2K rows →
/// 7.8 s, 52K rows → 11.5 s), so a 10-table group applied as 10 separate
/// scripts paid that 7.3 s floor TEN times. The unit's close commits the
/// group's MERGEs and watermark rows in as few transactions as fit, and a
/// table's MERGE and its watermark are always in the same one.
pub(crate) async fn apply_group_unit(
    u: &mut BqUnit<'_>,
    ctxs: &[Member],
    outcome: &DrainOutcome,
    lanes: usize,
) -> Result<Vec<(u64, Watermark)>> {
    use futures::stream::{StreamExt as _, TryStreamExt as _};
    let (ur, cref, oref): (&BqUnit<'_>, _, _) = (&*u, ctxs, outcome);
    let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
        .map(|i| async move {
            let (dt, q, pk, _) = &cref[i];
            stage(ur, dt, q, pk, oref).await.map(|(ev, sql)| (i, ev, sql))
        })
        .buffer_unordered(lanes.max(1))
        .try_collect()
        .await?;
    Ok(into_groups(u, ctxs, outcome, staged))
}

/// Each staged table's statements into its own group, in member order, and
/// the watermark its close writes. Every member gets a mark — a table with
/// no traffic in the window still advances.
fn into_groups(
    u: &mut BqUnit<'_>,
    ctxs: &[Member],
    outcome: &DrainOutcome,
    mut staged: Vec<(usize, u64, Vec<String>)>,
) -> Vec<(u64, Watermark)> {
    staged.sort_by_key(|s| s.0);
    let mut events = vec![0u64; ctxs.len()];
    for (i, ev, sql) in staged {
        events[i] = ev;
        u.push(&ctxs[i].0, sql);
    }
    ctxs.iter()
        .zip(events)
        .map(|((dt, _, _, sid), ev)| {
            (ev, Watermark::Set { table: dt.clone(), source_id: sid.clone(), lsn: outcome.end_lsn, rows: ev })
        })
        .collect()
}

/// One readback per window: the current value of every masked column for
/// every key that needs one. `__current` filters the log by key first and
/// the table is CLUSTERed on the PK, so this prunes rather than scans.
async fn read_current(
    u: &BqUnit<'_>,
    table: &str,
    pk_cols: &[String],
    keys: &[crate::logbased::changelog::CKey],
    cols: &[usize],
    wal_cols: &[String],
) -> Result<std::collections::HashMap<crate::logbased::changelog::CKey, Vec<Option<bytes::Bytes>>>> {
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
            let txt = std::str::from_utf8(v).map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
            parts.push(format!("CAST({} AS STRING) = '{}'", bt(c), sql_str(txt)));
        }
        preds.push(format!("({})", parts.join(" AND ")));
    }
    let rows = u
        .query(&format!(
            "SELECT {sel} FROM {v} WHERE {p}",
            v = u.fq(&format!("{table}__current")),
            p = preds.join(" OR "),
        ))
        .await?;
    let np = pk_cols.len();
    let mut out = std::collections::HashMap::with_capacity(keys.len());
    for row in rows {
        if row.len() != np + cols.len() {
            return Err(Error::Transfer("log_based changelog: masked readback column count mismatch".into()));
        }
        let key: crate::logbased::changelog::CKey =
            row[..np].iter().map(|x| x.clone().unwrap_or_default().into_bytes()).collect();
        let vals = row[np..].iter().map(|x| x.clone().map(bytes::Bytes::from)).collect();
        out.insert(key, vals);
    }
    Ok(out)
}

/// One table's changelog window: every captured event as a staging row,
/// loaded, then handed back as the INSERT its group commits. The watermark is
/// not here: the unit's close appends it to this table's group.
async fn stage_changelog(
    u: &BqUnit<'_>,
    dest_table: &str,
    qualified_src: &str,
    pk_cols: &[String],
    outcome: &DrainOutcome,
) -> Result<(u64, Vec<String>)> {
    let table = bare(dest_table);
    let Some(c) = outcome.changes.get(qualified_src) else {
        return Ok((0, Vec::new()));
    };
    if c.events.is_empty() {
        return Ok((0, Vec::new()));
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
    let meta = u.table_get(table).await?.ok_or_else(|| {
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
            read_current(u, table, pk_cols, &keys, &cols, wal_cols).await?
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
    u.load(table, &plan.staging_fields_changelog(), ndjson).await?;

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
            // The window's START, not its end. `end_lsn` is recomputed by
            // every re-drain, so the same event came back under a different
            // `_apitap_lsn` and `(lsn, seq)` was useless as a de-duplication
            // key on a log that this path CAN replay (see the chunking note
            // on `apply_group_changelog_unit`).
            format!("CAST({} AS INT64)", outcome.start_lsn),
            format!("CAST({} AS INT64)", bt(CL_SEQ)),
            // One stamp for the whole window: it is the PARTITION and
            // retention key, never an ordering key — `(lsn, seq)` orders.
            "CURRENT_TIMESTAMP()".to_string(),
        ])
        .collect::<Vec<_>>()
        .join(", ");
    Ok((
        c.count,
        vec![format!("INSERT INTO {t} ({into}) SELECT {sel} FROM {s};", t = u.fq(table), s = u.staging_fq(table))],
    ))
}

/// Rewrite the freshly-bootstrapped target clustered on its PK (up to 4
/// columns — BigQuery's max). One full-table rewrite, once, so every later
/// MERGE prunes the target scan. Only worth it once the scan dominates the
/// MERGE's ~fixed BigQuery job floor — measured NEUTRAL below ~1M rows, so
/// small tables skip it (and skip its one-time rewrite cost). Kill switch
/// `APITAP_BQ_CLUSTER=0`; skips if already clustered on the same keys.
///
/// The same two guarded scripts as the changelog rebuild: a CTAS into this
/// run's temp, then the idempotent swap.
async fn cluster_target(u: &mut BqUnit<'_>, table: &str, pk_cols: &[String], rows: u64) -> Result<()> {
    const CLUSTER_MIN_ROWS: u64 = 1_000_000;
    if std::env::var("APITAP_BQ_CLUSTER").as_deref() == Ok("0") || rows < CLUSTER_MIN_ROWS {
        return Ok(());
    }
    let keys: Vec<&String> = pk_cols.iter().take(4).collect();
    if keys.is_empty() {
        return Ok(());
    }
    if let Some(meta) = u.table_get(table).await? {
        if let Some(cur) = meta["clustering"]["fields"].as_array() {
            let cur: Vec<&str> = cur.iter().filter_map(|f| f.as_str()).collect();
            let want: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            if cur == want {
                return Ok(());
            }
        }
    }
    let cl = keys.iter().map(|k| format!("`{k}`")).collect::<Vec<_>>().join(", ");
    let tmp = u.tmp(table);
    u.ddl(&format!(
        "CREATE OR REPLACE TABLE {tmpfq} CLUSTER BY {cl} AS SELECT * FROM {t};",
        tmpfq = u.fq(&tmp),
        t = u.fq(table),
    ))
    .await?;
    u.swap(&tmp, table).await
}

/// Build one table's window body, load it into this run's staging table, and
/// return the statements it contributes to its group. The watermark is not
/// here: the unit's close appends it to the same group.
async fn stage(
    u: &BqUnit<'_>,
    dest_table: &str,
    qualified_src: &str,
    pk_cols: &[String],
    outcome: &DrainOutcome,
) -> Result<(u64, Vec<String>)> {
    let table = bare(dest_table);
    let Some(c) = outcome.tables.get(qualified_src) else {
        // Foreign-table traffic only: nothing for our table, still advance.
        return Ok((0, Vec::new()));
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
    let meta = u.table_get(table).await?.ok_or_else(|| {
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
        for key in c.deletes.iter() {
            // A key also re-landed as an upsert rides as that one 'U' row.
            if landed.contains(key) {
                continue;
            }
            push_delete(&mut ndjson, pk_cols, key)?;
            staged += 1;
        }
    }

    if staged == 0 {
        // Nothing to merge. A TRUNCATE window still empties the target.
        let mut out = Vec::new();
        if c.truncate {
            out.push(format!("DELETE FROM {} WHERE TRUE;", u.fq(table)));
        }
        return Ok((c.events, out));
    }

    // Land the window in this run's staging table for it (its own load job),
    // then hand the MERGE back so the GROUP commits it with its watermark.
    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    let nbytes = ndjson.len();
    let t_load = std::time::Instant::now();
    u.load(table, &plan.staging_fields(), ndjson).await?;
    if dbg {
        eprintln!(
            "[bq stage] {table}: {staged} staging rows / {:.1}MB, load={:.1}s",
            nbytes as f64 / (1 << 20) as f64,
            t_load.elapsed().as_secs_f64(),
        );
    }
    Ok((c.events, vec![format!("{};", plan.merge_sql(&u.fq(table), &u.staging_fq(table), c.truncate))]))
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

    /// The MERGE of this run's staging (`staging_fq`) into `target_fq`.
    fn merge_sql(&self, target_fq: &str, staging_fq: &str, truncate: bool) -> String {
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
            target_fq, OP_COL, MASK_COL, using.join(",\n"), staging_fq, on));
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

/// Everything that reaches BigQuery. See the module doc.
mod store {
    use super::{bare, pack_whole_groups, CHUNK_BYTES};
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{no_longer_holds, owned_margin_secs, Fence, LeaseStore, Watermark, LEASE_TABLE, LOST_MARK};
    use crate::naming::{artifact_ident, artifact_ident_tok, fence_ident, Artifact, ROOMY};
    use crate::sink::bigquery::{fence_fq, sql_str, BqConn, BqGuard};
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(crate) struct BqStore {
        conn: BqConn,
        /// `_apitap_state` is known to exist (one REST probe per run, not per
        /// window).
        state_ready: AtomicBool,
    }

    /// One unit: the run's keys, and the statements each table contributes,
    /// grouped by table. Nothing reaches BigQuery's tables of record until the
    /// close, which commits the groups — every table's watermark in its own
    /// group — inside fenced transactions.
    pub(crate) struct BqUnit<'a> {
        s: &'a BqStore,
        token: String,
        keys: Vec<String>,
        groups: Vec<(String, Vec<String>)>,
    }

    /// One fenced transaction: the fence first, then `chunk`, then COMMIT.
    ///
    /// The fence UPDATE is the transaction's first write, on this run's own
    /// table. A claim committed before it leaves no unclaimed row, the IF
    /// raises, and BigQuery rolls the transaction back whole. A claim that
    /// commits after BEGIN but before COMMIT conflicts with this write, which
    /// BigQuery resolves by cancelling one side; the retry then meets the
    /// claim. A claim that arrives after the fence UPDATE is standalone DML,
    /// queued behind this transaction — so a script that committed did so as
    /// the owner. `_apitap_lease` is never written here: the keepers of every
    /// drain in the dataset write it, and a transaction that did would
    /// conflict with each of them.
    pub(super) fn fenced_script(fence_fq: &str, token: &str, chunk: &[String]) -> String {
        format!(
            "BEGIN TRANSACTION;\n\
             UPDATE {fence_fq} SET n = n + 1 WHERE NOT claimed;\n\
             IF @@row_count <> 1 THEN RAISE USING MESSAGE = '{LOST_MARK}: run {tok} no longer holds its tables'; END IF;\n\
             {body}\n\
             COMMIT TRANSACTION;",
            tok = sql_str(token),
            body = chunk.join("\n"),
        )
    }

    /// Bootstrap DDL, which no transaction can hold: a server-side ASSERT in
    /// the same script, so no client pause can come between the check and the
    /// statement. Owner = the fence is unclaimed and every key's lease row is
    /// uncollected with more than the margin left — the time fence, because a
    /// DDL is not undone by a later conflict the way a transaction is.
    pub(super) fn guarded_ddl_script(
        fence_fq: &str,
        lease_fq: &str,
        token: &str,
        keys: &[String],
        margin: u64,
        body: &str,
    ) -> String {
        let mut distinct: Vec<&String> = keys.iter().collect();
        distinct.sort();
        distinct.dedup();
        let inlist = distinct.iter().map(|k| format!("'{}'", sql_str(k))).collect::<Vec<_>>().join(", ");
        format!(
            "ASSERT (SELECT COUNT(*) FROM {fence_fq} WHERE NOT claimed) = 1\n\
               AND (SELECT COUNT(*) FROM {lease_fq} WHERE token = '{tok}' AND dest_key IN ({inlist})\n\
                    AND NOT collected\n\
                    AND expires_at > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {margin} SECOND)) = {n}\n\
               AS '{LOST_MARK}: run {tok} no longer holds {keys} with {margin}s to spare';\n\
             {body}",
            tok = sql_str(token),
            n = distinct.len(),
            keys = sql_str(&distinct.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")),
        )
    }

    /// Move `tmp` onto `table` — only while `tmp` exists, so `cdc_script`'s
    /// whole-script retry can repeat it: a retry after the RENAME finds no
    /// temp and does nothing, one after the DROP finishes the move.
    pub(super) fn swap_sql(dataset_fq: &str, tmp: &str, tmp_fq: &str, t_fq: &str, table: &str) -> String {
        format!(
            "IF (SELECT COUNT(*) FROM {dataset_fq}.INFORMATION_SCHEMA.TABLES WHERE table_name = '{tmp_s}') = 1 THEN\n\
               DROP TABLE IF EXISTS {t_fq};\n\
               ALTER TABLE {tmp_fq} RENAME TO `{table}`;\n\
             END IF;",
            tmp_s = sql_str(tmp),
        )
    }

    /// `stmts` onto `table`'s group, the group created if absent.
    fn append(groups: &mut Vec<(String, Vec<String>)>, table: &str, stmts: Vec<String>) {
        match groups.iter_mut().find(|(t, _)| t == table) {
            Some((_, g)) => g.extend(stmts),
            None => groups.push((table.to_string(), stmts)),
        }
    }

    /// The unit's groups with each state statement appended to ITS TABLE'S
    /// group — never a group of its own, which `pack_whole_groups` could put
    /// in the next transaction, away from the data it records.
    pub(super) fn close_groups(mut groups: Vec<(String, Vec<String>)>, states: Vec<(String, String)>) -> Vec<Vec<String>> {
        for (t, sql) in states {
            append(&mut groups, &t, vec![sql]);
        }
        groups.into_iter().map(|(_, g)| g).filter(|g| !g.is_empty()).collect()
    }

    pub(super) fn state_insert_sql(state_fq: &str, table: &str, source_id: &str, lsn: u64, rows: u64) -> String {
        format!(
            "INSERT INTO {state_fq} \
             (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ('{dt}', '{sid}', '_lsn', '{lsn}', 'log_based', {rows}, CURRENT_TIMESTAMP());",
            dt = sql_str(table),
            sid = sql_str(source_id),
        )
    }

    /// The scratch tables drains before 0.57.0 named, untokenized: staging and
    /// the rebuild temp, in both spellings they used (the literal suffix, and
    /// `artifact_ident`'s, which differs only for an over-long name).
    fn legacy_scratch(table: &str) -> Vec<String> {
        let mut v = vec![
            format!("{table}__apitap_cdc"),
            artifact_ident(table, Artifact::CdcStaging, ROOMY),
            format!("{table}__apitap_cl"),
            artifact_ident(table, Artifact::ChangelogTmp, ROOMY),
        ];
        v.sort();
        v.dedup();
        v
    }

    impl BqStore {
        pub(crate) async fn connect(url: &str) -> Result<Self> {
            Ok(Self { conn: BqConn::parse(url).await?, state_ready: AtomicBool::new(false) })
        }

        pub(crate) fn bq_guard(&self) -> BqGuard {
            BqGuard::new(self.conn.clone())
        }

        /// A table's metadata (`tables.get`), for the parent's shape checks.
        pub(crate) async fn table_meta(&self, table: &str) -> Result<Option<Value>> {
            self.conn.table_get(table).await
        }

        /// This table's newest state row for this source past the last
        /// replace barrier, whichever lane wrote it.
        pub(crate) async fn read_state(
            &self,
            table: &str,
            source_id: &str,
        ) -> Result<Option<crate::naming::StateRow>> {
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
            // That SELECT paid for the table's whole append-only history (one row
            // per applied window, forever); past the threshold, delete the rows
            // a newer row of their own key supersedes. Best-effort — an error is
            // noted inside and never fails the run. It is a read-side chore,
            // outside any unit, and safe there only because it can never remove
            // a row some sibling committed meanwhile (see `compact_state`): this
            // runs before a tenure on the MySQL path, beside every drain of the
            // dataset.
            self.conn.compact_state_if_bloated().await;
            Ok(rows.into_iter().next().map(|row| {
                let cell = |i: usize| row.get(i).cloned().flatten();
                crate::naming::StateRow::new(cell(0), cell(1), cell(2))
            }))
        }

        async fn ensure_state_table(&self) -> Result<()> {
            if !self.state_ready.load(Ordering::Relaxed) {
                self.conn.cdc_ensure_state_table().await?;
                self.state_ready.store(true, Ordering::Relaxed);
            }
            Ok(())
        }

        /// A script run as the owner of `keys`: a fence that fired, or a fence
        /// table a collector already deleted, is this run no longer holding
        /// them — never an error to retry or to report as BigQuery's.
        async fn run_owned(&self, sql: &str, keys: &[String], token: &str) -> Result<()> {
            match self.conn.cdc_script(sql).await {
                Ok(()) => Ok(()),
                Err(Error::Locked(_)) => Err(no_longer_holds(keys)),
                Err(Error::Transfer(m)) if m.contains("Not found") && m.contains(&fence_ident(token)) => {
                    Err(no_longer_holds(keys))
                }
                Err(e) => Err(e),
            }
        }
    }

    impl BqUnit<'_> {
        pub(crate) fn fq(&self, table: &str) -> String {
            self.s.conn.fq(table)
        }

        /// This run's staging table for `table` — tokenized, so two drains in
        /// one dataset never load into one table, and a collector sweeps a
        /// dead run's by its exact token.
        fn staging(&self, table: &str) -> String {
            artifact_ident_tok(table, Artifact::CdcStaging, ROOMY, &self.token)
        }

        pub(crate) fn staging_fq(&self, table: &str) -> String {
            self.fq(&self.staging(table))
        }

        /// This run's rebuild temp for `table` (changelog rebuild, clustering).
        pub(crate) fn tmp(&self, table: &str) -> String {
            artifact_ident_tok(table, Artifact::ChangelogTmp, ROOMY, &self.token)
        }

        pub(crate) async fn table_get(&self, table: &str) -> Result<Option<Value>> {
            self.s.conn.table_get(table).await
        }

        /// A read (small results only).
        pub(crate) async fn query(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
            self.s.conn.cdc_query(sql).await
        }

        /// One window's rows into this run's staging table for `table`,
        /// WRITE_TRUNCATE — scratch, not the table of record.
        pub(crate) async fn load(&self, table: &str, fields: &Value, ndjson: Vec<u8>) -> Result<()> {
            self.s.conn.cdc_load_ndjson(&self.staging(table), fields, ndjson).await
        }

        /// `stmts` join `table`'s group, which commits with its watermark.
        pub(crate) fn push(&mut self, table: &str, stmts: Vec<String>) {
            if !stmts.is_empty() {
                append(&mut self.groups, bare(table), stmts);
            }
        }

        /// Bootstrap DDL, guarded server-side (`guarded_ddl_script`).
        pub(crate) async fn ddl(&self, body: &str) -> Result<()> {
            let sql = guarded_ddl_script(
                &fence_fq(&self.s.conn, &self.token),
                &self.s.conn.fq(LEASE_TABLE),
                &self.token,
                &self.keys,
                owned_margin_secs(),
                body,
            );
            self.s.run_owned(&sql, &self.keys, &self.token).await
        }

        /// `tmp` onto `table`, guarded and idempotent (`swap_sql`).
        pub(crate) async fn swap(&self, tmp: &str, table: &str) -> Result<()> {
            let ds = format!("`{}.{}`", self.s.conn.project, self.s.conn.dataset);
            self.ddl(&swap_sql(&ds, tmp, &self.fq(tmp), &self.fq(table), table)).await
        }

        /// A rebuild whose swap never finished leaves the new table under its
        /// temp name and no target. This run's own temp, or the one a 0.56.0
        /// run left untokenized, is moved into place. `true` = healed.
        pub(crate) async fn heal(&self, table: &str) -> Result<bool> {
            let mut candidates = vec![self.tmp(table)];
            candidates.extend(legacy_scratch(table).into_iter().filter(|n| n.ends_with(Artifact::ChangelogTmp.suffix())));
            for c in candidates {
                if self.table_get(&c).await?.is_some() {
                    self.swap(&c, table).await?;
                    return Ok(self.table_get(table).await?.is_some());
                }
            }
            Ok(false)
        }

        /// The scratch names releases before 0.57.0 used, untokenized. Only a
        /// bootstrap drops them: while a table bootstraps the guard excludes
        /// every live drain of it, so nothing else can be using them.
        pub(crate) async fn drop_legacy_scratch(&self, table: &str) -> Result<()> {
            for n in legacy_scratch(table) {
                self.s.conn.table_delete(&n).await?;
            }
            Ok(())
        }
    }

    impl LeaseStore for BqStore {
        fn lease_key(&self, dest_table: &str) -> String {
            format!("{}.{}", self.conn.dataset, bare(dest_table))
        }

        async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
            crate::sink::bigquery::lease_open(&self.conn, keys, token).await
        }

        async fn lease_renew(&self, _keys: &[String], token: &str) -> Result<u64> {
            // One statement for the whole group: every row this run owns.
            crate::sink::bigquery::lease_renew(&self.conn, token).await
        }

        async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
            crate::sink::bigquery::lease_unclaimed(&self.conn, token).await
        }

        /// The run's fence table. Best-effort: a leftover one is inert, and a
        /// collector deletes it anyway.
        async fn close_run(&self, token: &str) {
            let _ = self.conn.table_delete(&fence_ident(token)).await;
        }
    }

    impl Fence for BqStore {
        type Unit<'a> = BqUnit<'a>;

        fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
            (Box::new(self.bq_guard()), bare(dest_table).to_string())
        }

        /// Every close of one run updates the same fence row; serialized, they
        /// queue instead of cancelling each other.
        fn serial_commit(&self) -> bool {
            true
        }

        /// No I/O: nothing is written until the close, and the close is fenced.
        async fn open_unit<'a>(&'a self, keys: &[String], token: &str) -> Result<BqUnit<'a>> {
            Ok(BqUnit { s: self, token: token.to_string(), keys: keys.to_vec(), groups: Vec::new() })
        }

        /// Each mark into its table's group, then the groups in as few fenced
        /// transactions as fit. Each chunk is its own transaction: a fence
        /// failure on chunk 2 leaves chunk 1 committed — as the owner, before
        /// any claim — and the next owner replays the rest.
        async fn close_unit<'a>(&'a self, u: BqUnit<'a>, token: &str, marks: Vec<Watermark>) -> Result<()> {
            let BqUnit { keys, groups, .. } = u;
            let state = self.conn.state_fq();
            let mut states = Vec::with_capacity(marks.len());
            for m in &marks {
                match m {
                    Watermark::Set { table, source_id, lsn, rows } => {
                        self.ensure_state_table().await?;
                        states.push((bare(table).to_string(), state_insert_sql(&state, bare(table), source_id, *lsn, *rows)));
                    }
                    Watermark::Clear { table, source_id } => {
                        // No state table: nothing to clear.
                        if self.conn.cdc_state_table_exists().await? {
                            states.push((
                                bare(table).to_string(),
                                format!(
                                    "DELETE FROM {state} WHERE dest_table = '{}' AND source_id = '{}';",
                                    sql_str(bare(table)),
                                    sql_str(source_id)
                                ),
                            ));
                        }
                    }
                }
            }
            let chunks = pack_whole_groups(close_groups(groups, states), CHUNK_BYTES);
            // Every statement must carry its own terminator, because they are
            // concatenated. One that does not absorbs the next line — and the
            // last one's next line is COMMIT, so the whole transaction fails to
            // parse with an error that names neither the statement nor the
            // caller. Caught here, where the offender is still identifiable.
            if let Some(bad) = chunks.iter().flatten().find(|s| !s.trim_end().ends_with(';')) {
                return Err(Error::Transfer(format!(
                    "log_based: a BigQuery statement is missing its ';' and would \
                     swallow the COMMIT that follows it: {}",
                    &bad[..bad.len().min(120)]
                )));
            }
            let (t0, jobs) = (std::time::Instant::now(), chunks.len());
            let fence = fence_fq(&self.conn, token);
            for chunk in chunks {
                self.run_owned(&fenced_script(&fence, token, &chunk), &keys, token).await?;
            }
            if std::env::var("APITAP_DEBUG").is_ok() && jobs > 0 {
                eprintln!(
                    "[bq apply] {} table(s) committed in {jobs} fenced script job(s), {:.1}s",
                    keys.len(),
                    t0.elapsed().as_secs_f64()
                );
            }
            Ok(())
        }
    }
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
        let stg = "`proj.ds.orders_0000000l000abcd__apitap_cdc`";
        let sql = plan.merge_sql("`proj.ds.orders`", stg, false);
        assert!(sql.contains("MERGE `proj.ds.orders` T"), "{sql}");
        assert!(sql.contains(&format!("FROM {stg}")), "{sql}");
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        // PK is never in the UPDATE SET; both non-PK columns are.
        assert!(sql.contains("SUBSTR(S._apitap_mask, 2, 1)"), "{sql}"); // v at pos 2
        assert!(sql.contains("SUBSTR(S._apitap_mask, 3, 1)"), "{sql}"); // flag at pos 3
        assert!(!sql.contains("`id` = IF"), "PK must not be updated: {sql}");
        assert!(!sql.contains("NOT MATCHED BY SOURCE"), "no truncate clause: {sql}");
        let sqlt = plan.merge_sql("`proj.ds.orders`", stg, true);
        assert!(sqlt.contains("WHEN NOT MATCHED BY SOURCE THEN\n  DELETE"), "{sqlt}");
    }

    #[test]
    fn all_pk_table_drops_the_update_clause() {
        let wal = vec!["a".to_string(), "b".to_string()];
        let pk = vec!["a".to_string(), "b".to_string()];
        let ty = types(&[("a", "INT64"), ("b", "STRING")]);
        let plan = ApplyPlan::build("j", &wal, &[23, 25], &pk, &ty).unwrap();
        let sql = plan.merge_sql("`p.d.j`", "`p.d.j_0000000l000abcd__apitap_cdc`", false);
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

    /// Each batch is one BigQuery transaction. A changelog table's INSERT and
    /// its own watermark row must land in the SAME one — split across two, a
    /// failure between them replays the window into an append-only table.
    /// The OLD packer, kept here as the control: it is the reason this test
    /// exists, and without it "the pairs are together" proves only that the new
    /// code agrees with itself.
    fn pack_flat(groups: Vec<Vec<String>>, limit: usize) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        let (mut batch, mut len) = (Vec::new(), 0usize);
        for s in groups.into_iter().flatten() {
            if len + s.len() > limit && !batch.is_empty() {
                out.push(std::mem::take(&mut batch));
                len = 0;
            }
            len += s.len();
            batch.push(s);
        }
        if !batch.is_empty() {
            out.push(batch);
        }
        out
    }

    /// Every batch is one BigQuery transaction, so a changelog table's INSERT
    /// and its own watermark row must land in the SAME one. Split across two, a
    /// failure in between commits the log rows without the watermark and the
    /// next run replays the whole window into an append-only table.
    #[test]
    fn a_tables_insert_and_its_watermark_are_never_split_across_transactions() {
        // Sized so a pair straddles the limit: the INSERT fits alone, the
        // watermark row does not fit beside it. That is the boundary the old
        // packer cut through.
        let pair = |i: usize| vec![format!("INSERT t{i} {}", "x".repeat(280)),
                                   format!("STATE t{i}")];
        let groups: Vec<Vec<String>> = (0..7).map(pair).collect();
        const LIMIT: usize = 295;

        let split = |batches: &Vec<Vec<String>>| {
            batches.iter().any(|b| b.len() % 2 != 0
                || b.chunks(2).any(|p| p[1] != format!("STATE {}",
                    p[0].split_whitespace().nth(1).unwrap())))
        };

        // Control: the flat packer really does cut a pair in half here.
        assert!(split(&pack_flat(groups.clone(), LIMIT)),
                "the control did not reproduce the split — the sizes stopped exercising it");

        let batches = pack_whole_groups(groups, LIMIT);
        assert!(!split(&batches), "a pair was separated: {batches:?}");
        let n: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(n, 14, "every statement still goes out exactly once");

        // A single pair bigger than the whole limit is still emitted whole: an
        // oversized transaction is correct, a split one is not.
        let one = pack_whole_groups(vec![pair(9)], 8);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].len(), 2);
    }

    /// Every apply transaction is fenced on the run's own table, first. The
    /// fence UPDATE is the first statement after BEGIN (a claim committed
    /// before it must make the script raise, not write), the chunk sits
    /// between it and COMMIT, and the shared `_apitap_lease` — which every
    /// drain's keeper writes — is never touched, or sibling drains' keepers
    /// would cancel each other's transactions.
    #[test]
    fn fenced_script_shape() {
        let tok = "_0abcdefl000beef";
        let fence = format!("`p.d.{}`", crate::naming::fence_ident(tok));
        let chunk = vec!["MERGE `p.d.t` T USING x S ON TRUE WHEN MATCHED THEN DELETE;".to_string(),
                         "INSERT INTO `p.d._apitap_state` (a) VALUES (1);".to_string()];
        let sql = store::fenced_script(&fence, tok, &chunk);
        let stmts: Vec<&str> = sql.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
        assert_eq!(stmts[0], "BEGIN TRANSACTION;", "{sql}");
        assert_eq!(stmts[1], format!("UPDATE {fence} SET n = n + 1 WHERE NOT claimed;"), "{sql}");
        assert!(stmts[2].starts_with("IF @@row_count <> 1 THEN RAISE") && stmts[2].contains(crate::lease::LOST_MARK),
                "{sql}");
        assert_eq!(&stmts[3..5], &[chunk[0].as_str(), chunk[1].as_str()], "{sql}");
        assert_eq!(*stmts.last().unwrap(), "COMMIT TRANSACTION;", "{sql}");
        assert_eq!(stmts.len(), 6, "{sql}");
        assert!(!sql.contains(crate::lease::LEASE_TABLE), "{sql}");
    }

    /// A table's watermark row commits in the SAME transaction as its data.
    /// T1's MERGE alone is past the chunk limit, so a state row packed as a
    /// group of its own would land in the next transaction — a failure between
    /// the two commits the rows and loses the watermark (a changelog replays
    /// the whole window into an append-only table), or the reverse.
    #[test]
    fn close_keeps_state_in_its_group() {
        let merge1 = format!("MERGE t1 {};", "x".repeat(300 << 10));
        let merge2 = "MERGE t2;".to_string();
        let groups = vec![("t1".to_string(), vec![merge1.clone()]), ("t2".to_string(), vec![merge2.clone()])];
        let st = |t: &str| store::state_insert_sql("`p.d._apitap_state`", t, "sid", 7, 1);
        // A member with no traffic has no group yet: its state makes one.
        let states = vec![("t1".to_string(), st("t1")), ("t2".to_string(), st("t2")), ("t3".to_string(), st("t3"))];
        let chunks = pack_whole_groups(store::close_groups(groups, states), CHUNK_BYTES);
        for (t, m) in [("t1", &merge1), ("t2", &merge2)] {
            let with_data: Vec<&Vec<String>> = chunks.iter().filter(|c| c.contains(m)).collect();
            assert_eq!(with_data.len(), 1, "{t}'s MERGE went out once");
            assert!(with_data[0].contains(&st(t)), "{t}'s state is not in its MERGE's transaction");
        }
        assert_eq!(chunks.iter().flatten().filter(|s| s.contains("INSERT INTO")).count(), 3,
                   "every state row goes out exactly once");
    }

    /// The bootstrap DDL guard names this run's fence and every key, with the
    /// margin, and carries the lease-lost mark a script error is classified by.
    #[test]
    fn guarded_ddl_asserts_fence_and_every_key() {
        let keys = vec!["d.b".to_string(), "d.a".to_string(), "d.b".to_string()];
        let sql = store::guarded_ddl_script("`p.d._apitap_fence_x`", "`p.d._apitap_lease`", "_x", &keys, 150,
                                            "DROP TABLE `p.d.t`;");
        assert!(sql.starts_with("ASSERT (SELECT COUNT(*) FROM `p.d._apitap_fence_x` WHERE NOT claimed) = 1"), "{sql}");
        assert!(sql.contains("dest_key IN ('d.a', 'd.b')") && sql.contains(") = 2"), "{sql}");
        assert!(sql.contains("INTERVAL 150 SECOND"), "{sql}");
        assert!(sql.contains(crate::lease::LOST_MARK), "{sql}");
        assert!(sql.trim_end().ends_with("DROP TABLE `p.d.t`;"), "{sql}");
        let sw = store::swap_sql("`p.d`", "t_x__apitap_cl", "`p.d.t_x__apitap_cl`", "`p.d.t`", "t");
        assert!(sw.starts_with("IF (SELECT COUNT(*) FROM `p.d`.INFORMATION_SCHEMA.TABLES WHERE table_name = 't_x__apitap_cl') = 1"),
                "{sw}");
    }
}
