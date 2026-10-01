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
//! the MERGE keeps the target's current value (`IF(masked, T.c, S.c)`). A row
//! the window moved to a new key has no target row there; it names its old key
//! (`_apitap_from_*`) and the MERGE reads its holes at that key instead.
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
use crate::logbased::changelog::Changes;
use crate::logbased::collapse::{Collapsed, Key};
use crate::logbased::replay::{Ask, Memo, ReplayPlan, StampFacts, WindowId};
use crate::logbased::resolve::{resolve_window, Image};
use crate::logbased::window::{Bodies, DrainOutcome, TableWindow};
use std::collections::HashMap;
use crate::sink::bigquery::sql_str;
use crate::wire::pgoutput::Cell;
use serde_json::{json, Map, Value};

pub(crate) use store::{BqStore, BqUnit};

const OP_COL: &str = "_apitap_op";
const MASK_COL: &str = "_apitap_mask";
/// `_apitap_from_<j>`: key part `j` of the key a staged row lived at before
/// its window moved it, when its image still has a hole (see `merge_sql`).
const FROM_PREFIX: &str = "_apitap_from_";

fn from_col(j: usize) -> String {
    format!("{FROM_PREFIX}{j}")
}

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

/// One CDC member: (destination table, qualified source, source id) — the
/// shape `run.rs` hands a group in. The key columns are each window's own
/// (`Layout::key_cols`), the names its rows were keyed by.
pub(crate) type Member = (String, String, String);

pub(crate) struct BqDest {
    store: BqStore,
    /// What this run knows of each changelog table's stamps (`Memo`).
    memo: Memo,
    /// A changelog table's plan between its apply and its unit's commit: the
    /// memo learns a marker only once the transaction that wrote it committed.
    staged: std::sync::Mutex<HashMap<(String, String), ReplayPlan>>,
}

/// `dest_table` may arrive schema-qualified; the BigQuery dataset comes from the
/// URL, so only the bare name addresses the table (same trim as the sink).
fn bare(dest_table: &str) -> &str {
    dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t)
}

impl BqDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        Ok(Self { store: BqStore::connect(url).await?, memo: Memo::default(), staged: Default::default() })
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
    /// `changelog_bootstrap_unit`. The table's `_apitap_cdc_pending` rows go
    /// in the same unit (brief R-B2): they describe the stream the bootstrap
    /// replaced, and a marker past the new stream's start would refuse every
    /// window as a rewind.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn changelog_bootstrap_finish(
        &self,
        u: &mut BqUnit<'_>,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        let table = bare(dest_table);
        self.memo.forget(table, source_id);
        changelog_bootstrap_unit(u, dest_table, pk_cols, lsn, partition_by, order_by).await?;
        u.clear_pending(table, source_id).await
    }

    /// A whole group's window in one unit — see `apply_group_unit` and
    /// `apply_group_changelog_unit`. Each member's watermark comes back for
    /// the unit's close, which commits it in that member's own group. A
    /// changelog member's plan waits in `staged` for that commit (`settle`).
    pub(crate) async fn apply_group(
        &self,
        u: &mut BqUnit<'_>,
        ctxs: &[Member],
        outcome: &DrainOutcome,
        lanes: usize,
    ) -> Result<Vec<(u64, Watermark)>> {
        match &outcome.bodies {
            Bodies::Replica(w) => apply_group_unit(u, ctxs, w, &outcome.id, lanes).await,
            Bodies::Changelog(w) => {
                let (out, plans) = apply_group_changelog_unit(u, &self.memo, ctxs, w, &outcome.id, lanes).await?;
                let mut staged = self.staged.lock().unwrap();
                for ((dt, _, sid), plan) in ctxs.iter().zip(plans) {
                    staged.insert((bare(dt).to_string(), sid.clone()), plan);
                }
                Ok(out)
            }
        }
    }

    /// The unit that held `dest_table`'s window is closed: `ok` when its
    /// transaction committed. Only then does the memo take the marker the
    /// window wrote (brief §0 L14); any failure, the apply's or the commit's,
    /// makes the table unknown again, and its next window probes. A group
    /// split across transactions reports one verdict for all of them: a
    /// member whose own transaction did commit is merely probed again.
    pub(crate) fn settle(&self, dest_table: &str, source_id: &str, ok: bool) {
        let table = bare(dest_table);
        let plan = self.staged.lock().unwrap().remove(&(table.to_string(), source_id.to_string()));
        match (ok, plan) {
            (true, Some(p)) => self.memo.committed(table, source_id, &p),
            (true, None) => {}
            (false, _) => self.memo.forget(table, source_id),
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

/// A whole group's changelog window: plan every member, stage the ones that
/// append concurrently (one load job each), then hand each table's
/// statements to its own group of the unit — whose close appends that
/// table's watermark row to the SAME group and commits whole groups, as few
/// transactions as fit.
///
/// **Replay.** A group is packed into several transactions (`CHUNK_BYTES`),
/// so one member's window can commit while a sibling's fails, and the next
/// run re-drains every member from the group minimum — that member's window
/// again, from the same start. The stamp is the window's START, the one
/// position a re-drain reproduces, so `(lsn, seq)` is an event's identity;
/// each transaction records its attempt in `_apitap_cdc_pending` beside its
/// rows (start, seq base, end, events); and `replay_plan` decides from both
/// what this attempt trims, marks and appends (brief §2.B §3.D). A table's
/// trim, marker, rows and watermark are one transaction, so an attempt is
/// never torn here: a replay resumes past a whole attempt, or trims the tail
/// a shorter replay does not carry.
///
/// The reads are one query job per GROUP, not per table: the probe once per
/// run (the memo answers every later window with nothing), the facts only on
/// a window a replay or another writer's rows can reach.
pub(crate) async fn apply_group_changelog_unit(
    u: &mut BqUnit<'_>,
    memo: &Memo,
    ctxs: &[Member],
    windows: &HashMap<String, TableWindow<Changes>>,
    id: &WindowId,
    lanes: usize,
) -> Result<(Vec<(u64, Watermark)>, Vec<ReplayPlan>)> {
    use futures::stream::{StreamExt as _, TryStreamExt as _};
    let keys: Vec<(&str, &str)> = ctxs.iter().map(|(dt, _, sid)| (bare(dt), sid.as_str())).collect();
    let unseen: Vec<(&str, &str)> = keys.iter().copied().filter(|(t, s)| memo.ask(t, s, id) == Ask::Probe).collect();
    if !unseen.is_empty() {
        for ((t, s), (pending, ceiling)) in unseen.iter().zip(u.probe_group(&unseen).await?) {
            memo.probed(t, s, pending, ceiling);
        }
    }
    let asks: Vec<(&str, &str, u32)> = keys
        .iter()
        .filter_map(|&(t, s)| match memo.ask(t, s, id) {
            Ask::Facts { base } => Some((t, s, base)),
            Ask::Nothing | Ask::Probe => None,
        })
        .collect();
    let facts: HashMap<String, StampFacts> =
        if asks.is_empty() { HashMap::new() } else { u.facts_group(id.start(), &asks).await? };
    // A member this window does not carry still runs the rule: an earlier
    // attempt at this start may have committed rows for it.
    let plans = ctxs
        .iter()
        .zip(&keys)
        .map(|((_, q, _), &(t, s))| {
            let events = windows.get(q).map_or(0, |w| w.body().events.len());
            memo.plan(t, s, facts.get(t), id, events)
        })
        .collect::<Result<Vec<_>>>()?;
    let (ur, cref, pref): (&BqUnit<'_>, _, _) = (&*u, ctxs, &plans);
    let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
        .map(|i| async move {
            let (dt, q, sid) = &cref[i];
            stage_changelog(ur, dt, sid, windows.get(q), &pref[i]).await.map(|(ev, sql)| (i, ev, sql))
        })
        .buffer_unordered(lanes.max(1))
        .try_collect()
        .await?;
    let ends: Vec<u64> = plans.iter().map(ReplayPlan::watermark).collect();
    Ok((into_groups(u, ctxs, &ends, staged), plans))
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
    windows: &HashMap<String, TableWindow<Collapsed>>,
    id: &WindowId,
    lanes: usize,
) -> Result<Vec<(u64, Watermark)>> {
    use futures::stream::{StreamExt as _, TryStreamExt as _};
    let (ur, cref): (&BqUnit<'_>, _) = (&*u, ctxs);
    let staged: Vec<(usize, u64, Vec<String>)> = futures::stream::iter(0..cref.len())
        .map(|i| async move {
            let (dt, q, _) = &cref[i];
            stage(ur, dt, windows.get(q)).await.map(|(ev, sql)| (i, ev, sql))
        })
        .buffer_unordered(lanes.max(1))
        .try_collect()
        .await?;
    Ok(into_groups(u, ctxs, &vec![id.end(); ctxs.len()], staged))
}

/// Each staged table's statements into its own group, in member order, and
/// the watermark its close writes (`ends`, per member: a replica window's
/// end, a changelog plan's watermark). Every member gets a mark — a table
/// with no traffic in the window still advances.
fn into_groups(
    u: &mut BqUnit<'_>,
    ctxs: &[Member],
    ends: &[u64],
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
        .zip(ends)
        .map(|(((dt, _, sid), ev), &lsn)| {
            (ev, Watermark::Set { table: dt.clone(), source_id: sid.clone(), lsn, rows: ev })
        })
        .collect()
}

/// The rows that were in the log before the window `plan` appends: every row
/// at an earlier stamp, and at its own stamp the baseline and the rows below
/// its seq base (another writer's, which R1 numbers above). What sits at the
/// stamp from the base up is this window's own earlier attempt.
fn before_window(plan: &ReplayPlan, alias: &str) -> String {
    format!(
        "({a}{CL_LSN} < {s} OR ({a}{CL_LSN} = {s} AND ({a}{CL_SEQ} < {b} OR {a}{OP_COL} = '{base}')))",
        a = alias,
        s = plan.stamp(),
        b = plan.seq_base(),
        base = CL_BASELINE,
    )
}

/// One readback per window: what every masked column held BEFORE this window,
/// for every key that needs one — `<table>__current`'s three rules (the
/// newest `T`, the newest record per key by `(lsn, seq)`, a `D` dropped after
/// the pick) over the rows `before_window` keeps.
///
/// Not the view itself: a replay reads back after its own earlier attempt
/// committed, and the view shows that attempt — a re-key's `D` half hides
/// the old key's row, and the `U` half found no cell to carry ("torn") on
/// every run. The window's own events are the carry's
/// (`Changes::resolve_masked`); the destination only answers for what came
/// before them. Filtered by key, and the table is CLUSTERed on the PK, so
/// this prunes rather than scans; the `T` bound reads three columns.
async fn read_base(
    u: &BqUnit<'_>,
    table: &str,
    cast: &ApplyPlan,
    keys: &[crate::logbased::changelog::CKey],
    cols: &[usize],
    plan: &ReplayPlan,
) -> Result<std::collections::HashMap<crate::logbased::changelog::CKey, Vec<Option<bytes::Bytes>>>> {
    let rows = u.query(&read_base_sql(&u.fq(table), cast, keys, cols, plan)?).await?;
    let np = cast.pk.len();
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

/// `read_base`'s query over the table `t` (fully qualified). Every masked
/// cell is selected as the staging text its column's cast reads
/// (`base_expr_of`), never as the destination's own string rendering.
fn read_base_sql(
    t: &str,
    cast: &ApplyPlan,
    keys: &[crate::logbased::changelog::CKey],
    cols: &[usize],
    plan: &ReplayPlan,
) -> Result<String> {
    let bt = |c: &str| format!("`{c}`");
    let pk_cols = &cast.pk;
    let mut sel: Vec<String> = pk_cols
        .iter()
        .map(|c| format!("CAST({} AS STRING)", bt(c)))
        .collect();
    for &i in cols {
        let c = &cast.cols[i];
        let (oid, ty) = &cast.typed[i];
        sel.push(base_expr_of(&format!("{}", bt(c)), c, *oid, ty)?);
    }
    let sel = sel.join(", ");
    let mut preds = Vec::with_capacity(keys.len());
    for k in keys {
        let mut parts = Vec::with_capacity(pk_cols.len());
        for (c, v) in pk_cols.iter().zip(k.iter()) {
            let txt = std::str::from_utf8(v).map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
            parts.push(format!("CAST(_apitap_p.{} AS STRING) = '{}'", bt(c), sql_str(txt)));
        }
        preds.push(format!("({})", parts.join(" AND ")));
    }
    // Every alias `_apitap_`-prefixed and every column qualified, as in the
    // view: a key called `l` or `s` must not turn ambiguous.
    let keys_q = pk_cols.iter().map(|c| format!("_apitap_p.{}", bt(c))).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "WITH _apitap_pre AS (SELECT * FROM {t} _apitap_p WHERE {pre}), \
         _apitap_tr AS (SELECT MAX({CL_LSN}) AS _apitap_l FROM _apitap_pre WHERE {OP_COL} = 'T'), \
         _apitap_trs AS (SELECT MAX(_apitap_p.{CL_SEQ}) AS _apitap_s \
           FROM _apitap_pre _apitap_p CROSS JOIN _apitap_tr \
           WHERE _apitap_p.{OP_COL} = 'T' AND _apitap_p.{CL_LSN} = _apitap_tr._apitap_l) \
         SELECT {sel} FROM ( \
           SELECT _apitap_p.* FROM _apitap_pre _apitap_p CROSS JOIN _apitap_tr CROSS JOIN _apitap_trs \
           WHERE ({p}) \
             AND (_apitap_tr._apitap_l IS NULL OR _apitap_p.{CL_LSN} > _apitap_tr._apitap_l \
                  OR (_apitap_p.{CL_LSN} = _apitap_tr._apitap_l AND _apitap_p.{CL_SEQ} > _apitap_trs._apitap_s)) \
           QUALIFY ROW_NUMBER() OVER ( \
             PARTITION BY {keys_q} \
             ORDER BY _apitap_p.{CL_LSN} DESC, _apitap_p.{CL_SEQ} DESC, _apitap_p.{OP_COL} = '{base}' ASC) = 1 \
         ) WHERE {OP_COL} != 'D'",
        pre = before_window(plan, "_apitap_p."),
        p = preds.join(" OR "),
        base = CL_BASELINE,
    ))
}

/// Where one changelog table's statements write: the table, the marker
/// table, and the partition bound (`BqUnit::prune`).
struct ClTarget<'a> {
    table: &'a str,
    source_id: &'a str,
    table_fq: String,
    pending_fq: String,
    prune: String,
}

/// One changelog table's share of its group's transaction, in the only order
/// it runs: the trim, the marker, the rows. The watermark is not here: the
/// unit's close appends it to the same group (brief §0 L13), so the four
/// commit together or not at all. `insert` is the rows' INSERT … SELECT from
/// this run's staging, present exactly when the plan appends.
///
/// The marker is never after the rows: a transaction that could commit rows
/// without it would leave rows no marker names, which the next run reads as
/// another writer's and numbers above — every event twice.
fn changelog_group_sql(plan: &ReplayPlan, at: &ClTarget<'_>, insert: Option<String>) -> Vec<String> {
    debug_assert_eq!(insert.is_some(), !plan.to_append().is_empty(), "{plan:?}");
    let mut v = Vec::with_capacity(3);
    if let Some(x) = plan.trim_from() {
        v.push(format!(
            "DELETE FROM {t} WHERE {CL_LSN} = {s} AND {OP_COL} != '{b}' AND {CL_SEQ} >= {x}{prune};",
            t = at.table_fq,
            s = plan.stamp(),
            b = CL_BASELINE,
            prune = at.prune,
        ));
    }
    if let Some(m) = plan.marker() {
        v.push(format!(
            "INSERT INTO {p} (dest_table, source_id, lsn, seq_base, end_lsn, events, `at`) \
             VALUES ('{t}', '{sid}', {}, {}, {}, {}, CURRENT_TIMESTAMP());",
            m.start,
            m.seq_base,
            m.end,
            m.events,
            p = at.pending_fq,
            t = sql_str(at.table),
            sid = sql_str(at.source_id),
        ));
    }
    v.extend(insert);
    v
}

/// One table's changelog window under its plan: the events the plan appends
/// as staging rows, loaded, and the table's statements for its group
/// (`changelog_group_sql`). A plan that appends nothing — a replay whose
/// attempt already committed, an absent member — loads nothing and may still
/// trim. The watermark is not here: the unit's close appends it.
async fn stage_changelog(
    u: &BqUnit<'_>,
    dest_table: &str,
    source_id: &str,
    w: Option<&TableWindow<Changes>>,
    plan: &ReplayPlan,
) -> Result<(u64, Vec<String>)> {
    let table = bare(dest_table);
    let at = ClTarget {
        table,
        source_id,
        table_fq: u.fq(table),
        pending_fq: u.pending_fq(),
        prune: u.prune(table, source_id),
    };
    if let Some(w) = w {
        for name in w.layout().cols() {
            if matches!(name.as_str(), OP_COL | MASK_COL | CL_LSN | CL_SEQ | CL_AT) {
                return Err(Error::InvalidInput(format!(
                    "log_based changelog: source column '{name}' collides with a reserved \
                     changelog column — rename it at the source or alias it in a view"
                )));
            }
        }
    }
    let rows = w.map_or(0, |w| w.body().count);
    let range = plan.to_append();
    if range.is_empty() {
        return Ok((rows, changelog_group_sql(plan, &at, None)));
    }
    let w = w.ok_or_else(|| Error::Transfer(format!("log_based: {table}: internal: rows to append from no window")))?;
    let (c, l) = (w.body(), w.layout());
    let (wal_cols, oids) = (l.cols(), l.oids());
    let meta = u.table_get(table).await?.ok_or_else(|| {
        Error::Transfer(format!(
            "log_based changelog: BigQuery target {table} does not exist — the \
             bootstrap must run before a CDC window can apply"
        ))
    })?;
    let types = column_types(&meta)?;
    let cast = ApplyPlan::build(table, wal_cols, oids, l.key_cols(), &types)?;

    // Rebuild unchanged-TOAST cells before anything is staged: writing them
    // as NULL would silently blank the column for every reader of
    // `__current`. One extra query per window, only when a mask is present.
    let patched = if c.masked {
        let (keys, cols) = c.mask_plan();
        let base = if keys.is_empty() || cols.is_empty() {
            std::collections::HashMap::new()
        } else {
            read_base(u, table, &cast, &keys, &cols, plan).await?
        };
        c.resolve_masked(&cols, &base)?
    } else {
        std::collections::HashMap::new()
    };

    let mut ndjson: Vec<u8> = Vec::new();
    for (i, ev) in c.events.iter().enumerate().take(range.end).skip(range.start) {
        let row = patched.get(&i).or(ev.row.as_ref());
        push_change(&mut ndjson, wal_cols, row, ev.op.code(), plan.seq_of(i))?;
    }
    u.load(table, &cast.staging_fields_changelog(), ndjson).await?;

    let bt = |c: &str| format!("`{c}`");
    let into = wal_cols
        .iter()
        .map(|c| bt(c))
        .chain([bt(OP_COL), bt(CL_LSN), bt(CL_SEQ), bt(CL_AT)])
        .collect::<Vec<_>>()
        .join(", ");
    let sel = cast
        .cast
        .iter()
        .cloned()
        .chain([
            bt(OP_COL),
            // The window's START, the one position a re-drain reproduces.
            format!("CAST({} AS INT64)", plan.stamp()),
            format!("CAST({} AS INT64)", bt(CL_SEQ)),
            // One stamp for the whole window: it is the PARTITION and
            // retention key, never an ordering key — `(lsn, seq)` orders.
            "CURRENT_TIMESTAMP()".to_string(),
        ])
        .collect::<Vec<_>>()
        .join(", ");
    let insert = format!("INSERT INTO {} ({into}) SELECT {sel} FROM {};", at.table_fq, u.staging_fq(table));
    Ok((rows, changelog_group_sql(plan, &at, Some(insert))))
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
    w: Option<&TableWindow<Collapsed>>,
) -> Result<(u64, Vec<String>)> {
    let table = bare(dest_table);
    let Some(w) = w else {
        // Foreign-table traffic only: nothing for our table, still advance.
        return Ok((0, Vec::new()));
    };
    let (c, l) = (w.body(), w.layout());
    let (wal_cols, oids, pk_cols) = (l.cols(), l.oids(), l.key_cols());
    for name in wal_cols {
        if name == OP_COL || name == MASK_COL || name.starts_with(FROM_PREFIX) {
            return Err(Error::InvalidInput(format!(
                "log_based: source column '{name}' collides with a reserved BigQuery \
                 CDC staging column — rename it at the source or alias it in a view"
            )));
        }
    }
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

    // Build the staging body from the window folded to one entry per key:
    // its final image as op='U' (maybe masked), or op='D' (PK columns only).
    // One staging row per key is what the MERGE requires — at most one source
    // row per target row — and `rows()` yields each key exactly once. A masked
    // row that moved names where it was (`_apitap_from_*`): its holes are
    // there on the target, not at its new key.
    let resolved = resolve_window(w);
    let moved = resolved.any_moved_mask();
    let mut ndjson: Vec<u8> = Vec::new();
    let mut staged = 0u64;
    for (key, image) in resolved.rows() {
        match image {
            Image::Row(cells) => push_upsert(&mut ndjson, wal_cols, cells, None, None)?,
            Image::Masked { row, moved_from } => {
                let mask: String = row
                    .iter()
                    .map(|c| if matches!(c, Cell::UnchangedToast) { '1' } else { '0' })
                    .collect();
                push_upsert(&mut ndjson, wal_cols, row, Some(&mask), moved_from)?;
            }
            Image::Delete => push_delete(&mut ndjson, pk_cols, key)?,
        }
        staged += 1;
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
    Ok((c.events, vec![format!("{};", plan.merge_sql(&u.fq(table), &u.staging_fq(table), c.truncate, moved))]))
}

// ── the per-table apply plan (cast expressions from the target DDL) ──────────

struct ApplyPlan {
    cols: Vec<String>,
    /// Per-column SELECT expression casting the STRING staging value to the
    /// target's declared type; index-parallel to `cols`.
    cast: Vec<String>,
    /// `cast` of the staging alias's column (`S.c`), for the MERGE form that
    /// joins the target in (`merge_sql` with `moved`); index-parallel too.
    cast_s: Vec<String>,
    /// That form's join: each key column of the target equal to the staged
    /// `_apitap_from_<j>`, cast by the key column's own type and OID.
    from_on: Vec<String>,
    pk: Vec<String>,
    /// `(OID, target type)` per column, index-parallel to `cols`: what the
    /// masked readback's value expressions are built from.
    typed: Vec<(u32, String)>,
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
        let mut cast_s = Vec::with_capacity(wal_cols.len());
        let mut typed = Vec::with_capacity(wal_cols.len());
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
            cast_s.push(cast_expr_of(&format!("S.`{name}`"), name, oid, ty)?);
            typed.push((oid, ty.clone()));
        }
        // A key column is one of the window's columns (the layout keys by
        // position); an old key is staged as text like any cell, so it is cast
        // exactly as that column is.
        let mut from_on = Vec::with_capacity(pk_cols.len());
        for (j, k) in pk_cols.iter().enumerate() {
            let Some(i) = wal_cols.iter().position(|c| c == k) else {
                return Err(Error::Transfer(format!(
                    "log_based: {table}: key column '{k}' is not among the window's columns"
                )));
            };
            let (oid, ty) = &typed[i];
            let from = cast_expr_of(&format!("S.`{}`", from_col(j)), k, *oid, ty)?;
            from_on.push(format!("F.`{k}` = {from}"));
        }
        Ok(Self { cols: wal_cols.to_vec(), cast, cast_s, from_on, pk: pk_cols.to_vec(), typed })
    }

    fn staging_fields(&self) -> Value {
        let mut fields = vec![
            json!({"name": OP_COL, "type": "STRING", "mode": "REQUIRED"}),
            json!({"name": MASK_COL, "type": "STRING", "mode": "NULLABLE"}),
        ];
        for name in &self.cols {
            fields.push(json!({"name": name, "type": "STRING", "mode": "NULLABLE"}));
        }
        // Always there, NULL but on a moved masked row: a staging schema that
        // depended on the window would be one more thing to get wrong.
        for j in 0..self.pk.len() {
            fields.push(json!({"name": from_col(j), "type": "STRING", "mode": "NULLABLE"}));
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
    ///
    /// A masked row keeps the target's value for each hole (`UPDATE SET …
    /// T.c`), which is right while the row stays at its key. A row the window
    /// MOVED (`UPDATE … SET id = 9 WHERE id = 1`, its TOASTed body untouched)
    /// has no target row at its new key: 0.56.0 sent it to the ERROR arm on
    /// every retry, and the table never moved again. With `moved` (the window
    /// holds such a row, `Resolved::any_moved_mask`) the USING body joins the
    /// target at the row's OLD key, `_apitap_from_*`, and fills each hole from
    /// there, so the row arrives whole (mask NULL) and inserts; an old key with
    /// no row is a replay (`moved_source`). The join reads
    /// the target as it was before this MERGE, so key 1's row is still there
    /// for key 9 while key 1's own 'D' deletes it. A window without such a row
    /// gets the plain form, byte for byte, and pays for no join.
    fn merge_sql(&self, target_fq: &str, staging_fq: &str, truncate: bool, moved: bool) -> String {
        let bt = |c: &str| format!("`{c}`");
        // A move is a change of key, so a keyless plan has none to join on.
        let source = if moved && !self.pk.is_empty() {
            self.moved_source(target_fq, staging_fq)
        } else {
            let using: Vec<String> = self
                .cols
                .iter()
                .zip(&self.cast)
                .map(|(c, expr)| format!("    {expr} AS {}", bt(c)))
                .collect();
            format!(
                "  SELECT {}, {},\n{}\n  FROM {}\n",
                OP_COL,
                MASK_COL,
                using.join(",\n"),
                staging_fq
            )
        };
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
        merge.push_str(&format!("MERGE {} T\nUSING (\n{}) S\nON {}\n", target_fq, source, on));
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

    /// The USING body of the `moved` form (see `merge_sql`). Only a staged row
    /// that names an old key (`_apitap_from_*`, a moved masked row) can meet a
    /// target row `F`, and a row that exists has a key, so `F.pk IS NOT NULL`
    /// is "the old key has a row": each column the mask marks comes from `F`,
    /// and the row leaves with no mask.
    ///
    /// An old key with no row keeps the mask, and the plain arms decide. That
    /// is a replay: a group commits in several transactions (`CHUNK_BYTES`),
    /// and when a later one fails the next run re-drains every member from
    /// the group minimum, so a member whose transaction committed meets this
    /// window again with the row already whole at its new key and nothing at
    /// the old one. Kept masked, it matches there and keeps its own cells, as
    /// every other replayed row converges; raising there instead failed that
    /// window on every run and wedged the group. A row at neither
    /// key still fails loudly, in the plain form's NOT MATCHED arm.
    fn moved_source(&self, target_fq: &str, staging_fq: &str) -> String {
        let found = format!("F.`{}` IS NOT NULL", self.pk[0]);
        let cols: Vec<String> = self
            .cols
            .iter()
            .zip(&self.cast_s)
            .enumerate()
            .map(|(i, (c, cast))| {
                format!(
                    "    IF({found} AND SUBSTR(S.{MASK_COL}, {pos}, 1) = '1', F.`{c}`,\n       \
                     {cast}) AS `{c}`",
                    pos = i + 1,
                )
            })
            .collect();
        format!(
            "  SELECT S.{OP_COL},\n    IF({found}, NULL, S.{MASK_COL}) AS {MASK_COL},\n{}\n  \
             FROM {staging_fq} S\n  LEFT JOIN {target_fq} F\n    ON {}\n",
            cols.join(",\n"),
            self.from_on.join(" AND "),
        )
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
    cast_expr_of(&format!("`{name}`"), name, oid, ty)
}

/// `cast_expr` of any STRING expression `src` that holds column `name`'s
/// text — a staging column under an alias, or a staged old key: the moved-row
/// MERGE casts `S.c` and `S._apitap_from_<j>` exactly as the plain form casts
/// `c`. `name` is only for the messages.
fn cast_expr_of(src: &str, name: &str, oid: u32, ty: &str) -> Result<String> {
    let c = src.to_string();
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

/// The inverse of `cast_expr_of` for a cell that is already typed in the
/// destination: renders column `src` as the STRING staging text that column's
/// cast expects, so a masked cell read back before the window re-enters
/// through exactly the conversion the WAL lane's text does. `NULL` stays NULL.
///
/// The destination's own string rendering is NOT that text for every type,
/// and using it is what silently corrupted a masked BYTES cell: BigQuery's
/// `CAST(bytes AS STRING)` is the UTF-8 reading (an error for binary), while
/// the WAL lane and the apply's BYTES cast speak `\x` + hex, and only the
/// latter round-trips. BOOL likewise: the apply reads WAL `t`/`f` (MySQL's
/// binlog spells `1`/`0`), never BigQuery's `true`/`false`.
fn base_expr_of(src: &str, name: &str, oid: u32, ty: &str) -> Result<String> {
    let c = src.to_string();
    if oid == BOOL_OID {
        return Ok(match ty {
            // `t`/`f` is what a Postgres boolean is in the WAL; every target
            // type `cast_expr_of` accepts for it reads either spelling.
            "BOOL" | "BOOLEAN" | "INT64" | "NUMERIC" | "BIGNUMERIC" | "FLOAT64" | "STRING" => {
                format!("IF({c} IS NULL, NULL, IF({c}, 't', 'f'))")
            }
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
        "INT64" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC" | "DATE" | "TIMESTAMP" | "DATETIME"
        | "TIME" => format!("CAST({c} AS STRING)"),
        // OID 0 (MySQL binlog) booleans are `1`/`0` text.
        "BOOL" | "BOOLEAN" => format!("IF({c} IS NULL, NULL, IF({c}, '1', '0'))"),
        // The WAL's bytea spelling: `\x` + lowercase hex. TO_HEX is uppercase
        // and FROM_HEX accepts either, but the text must match the WAL lane's
        // byte for byte — it is what the staging column carries.
        "BYTES" => format!("IF({c} IS NULL, NULL, CONCAT(r'\\x', LOWER(TO_HEX({c}))))"),
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

/// One upsert row. `from` is the key a masked row lived at before its window
/// moved it, staged as `_apitap_from_<j>` for the MERGE to read its holes
/// there (see `ApplyPlan::merge_sql`).
fn push_upsert(
    out: &mut Vec<u8>,
    cols: &[String],
    cells: &[Cell],
    mask: Option<&str>,
    from: Option<&Key>,
) -> Result<()> {
    let mut obj = Map::new();
    obj.insert(OP_COL.to_string(), json!("U"));
    if let Some(m) = mask {
        obj.insert(MASK_COL.to_string(), json!(m));
    }
    for (j, part) in from.into_iter().flatten().enumerate() {
        let s = std::str::from_utf8(part)
            .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
        obj.insert(from_col(j), json!(s));
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

/// One changelog record: the op, its `_apitap_seq`, and whatever the
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
    seq: u32,
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
    use super::{bare, pack_whole_groups, CHUNK_BYTES, CL_LSN, CL_SEQ, OP_COL};
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{no_longer_holds, owned_margin_secs, Fence, LeaseStore, Watermark, LEASE_TABLE, LOST_MARK};
    use crate::logbased::replay::{parse_ceiling, parse_facts, parse_pending, Pending, StampFacts};
    use crate::naming::{artifact_ident, artifact_ident_tok, fence_ident, Artifact, CDC_PENDING_TABLE, ROOMY};
    use crate::sink::bigquery::{fence_fq, sql_str, BqConn, BqGuard};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(crate) struct BqStore {
        conn: BqConn,
        /// `_apitap_state` is known to exist (one REST probe per run, not per
        /// window).
        state_ready: AtomicBool,
        /// `_apitap_cdc_pending` is known to exist.
        pending_ready: AtomicBool,
        /// This run already looked at `_apitap_cdc_pending`'s size.
        pending_compacted: AtomicBool,
        /// Per (table, source): when the watermark this run started from was
        /// committed (`read_state`), the lower bound of every changelog row
        /// this run can find at its stamps (`BqUnit::prune`).
        hint: std::sync::Mutex<HashMap<(String, String), String>>,
    }

    /// Rows past which a probe compacts `_apitap_cdc_pending`. One row per
    /// changelog table per window that carried events, so it grows with
    /// traffic; only the newest per table is ever read.
    const PENDING_COMPACT_ROWS: u64 = 4096;

    /// `_apitap_cdc_pending` on BigQuery: one row per append attempt, written
    /// inside the attempt's transaction, never updated. Clustered so a
    /// probe's join on (dest_table, source_id) prunes. The columns are the
    /// ClickHouse marker table's; `at` is a keyword in BigQuery's grammar, so
    /// it is quoted wherever it is spelled.
    pub(super) fn pending_ddl(p: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {p} (dest_table STRING NOT NULL, source_id STRING NOT NULL, \
             lsn INT64 NOT NULL, seq_base INT64 NOT NULL, end_lsn INT64 NOT NULL, events INT64 NOT NULL, \
             `at` TIMESTAMP NOT NULL) CLUSTER BY dest_table, source_id;"
        )
    }

    /// The run's first look at a group's changelog tables, in ONE query job:
    /// per member its newest marker (`'m'`, ranked by `(lsn, at)` — a
    /// table's marker starts only rise, so a clock step cannot pick an older
    /// one) and its log's non-baseline row count and highest stamp (`'c'`,
    /// the ceiling `replay::Memo` bounds earlier writers by). `members` are
    /// (table, source, table_fq, prune).
    pub(super) fn probe_group_sql(p: &str, members: &[(&str, &str, String, String)]) -> String {
        let k = members
            .iter()
            .map(|(t, s, ..)| format!("STRUCT('{}' AS t, '{}' AS s)", sql_str(t), sql_str(s)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "WITH k AS (SELECT * FROM UNNEST([{k}])), \
             m AS (SELECT p.dest_table, p.lsn, p.seq_base, p.events, \
                   ROW_NUMBER() OVER (PARTITION BY p.dest_table, p.source_id ORDER BY p.lsn DESC, p.`at` DESC) AS rn \
                   FROM {p} p JOIN k ON p.dest_table = k.t AND p.source_id = k.s) \
             SELECT 'm', dest_table, CAST(lsn AS STRING), CAST(seq_base AS STRING), CAST(events AS STRING) \
             FROM m WHERE rn = 1"
        );
        for (t, _, fq, prune) in members {
            sql.push_str(&format!(
                " UNION ALL SELECT 'c', '{t}', CAST(COUNT(*) AS STRING), \
                 CAST(IFNULL(MAX({CL_LSN}), -1) AS STRING), CAST(NULL AS STRING) \
                 FROM {fq} WHERE {OP_COL} != 'B'{prune}",
                t = sql_str(t),
            ));
        }
        sql
    }

    /// What is at one stamp already, per asked table, in ONE query job: every
    /// non-baseline row, the ones from the table's base up, and how many
    /// distinct seqs (`replay::parse_facts`). `asks` are (table, table_fq,
    /// base, prune).
    pub(super) fn facts_group_sql(stamp: u64, asks: &[(&str, String, u32, String)]) -> String {
        asks.iter()
            .map(|(t, fq, b, prune)| {
                format!(
                    "SELECT '{t}', CAST(COUNT(*) AS STRING), CAST(IFNULL(MAX({CL_SEQ}), -1) AS STRING), \
                     CAST(COUNTIF({CL_SEQ} >= {b}) AS STRING), \
                     CAST(IFNULL(MAX(IF({CL_SEQ} >= {b}, {CL_SEQ}, NULL)), -1) AS STRING), \
                     CAST(COUNT(DISTINCT {CL_SEQ}) AS STRING) \
                     FROM {fq} WHERE {CL_LSN} = {stamp} AND {OP_COL} != 'B'{prune}",
                    t = sql_str(t),
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }

    /// The markers every newer marker of their own table supersedes, and only
    /// those older than a week: the probe reads the newest per table, and a
    /// marker is never read once its window committed. A DELETE decides on
    /// its own snapshot, so a marker appended meanwhile — an INSERT never
    /// conflicts with it — survives; never a WRITE_TRUNCATE rewrite, which
    /// would lose one (the lesson of `compact_state`). The newest time per
    /// table is aggregated first: one equality join, linear in the table.
    pub(super) fn compact_pending_sql(p: &str) -> String {
        format!(
            "DELETE FROM {p} x WHERE x.`at` < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) \
             AND EXISTS (SELECT 1 FROM (SELECT dest_table, source_id, MAX(`at`) AS newest FROM {p} \
                         GROUP BY dest_table, source_id) y \
                         WHERE y.dest_table = x.dest_table AND y.source_id = x.source_id AND x.`at` < y.newest)"
        )
    }

    /// A table's markers, for whoever drops its state or starts its stream
    /// again (brief R-B2).
    pub(super) fn clear_pending_sql(p: &str, table: &str, source_id: &str) -> String {
        format!(
            "DELETE FROM {p} WHERE dest_table = '{}' AND source_id = '{}';",
            sql_str(table),
            sql_str(source_id)
        )
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
            Ok(Self {
                conn: BqConn::parse(url).await?,
                state_ready: AtomicBool::new(false),
                pending_ready: AtomicBool::new(false),
                pending_compacted: AtomicBool::new(false),
                hint: Default::default(),
            })
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
                 SELECT watermark, cursor_col, mode, \
                        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S+00', synced_at, 'UTC') FROM s, b \
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
                // Pasted into SQL as a literal later: kept only if it is the
                // timestamp that was asked for.
                if let Some(at) = cell(3).filter(|a| a.bytes().all(|b| b.is_ascii_digit() || b" -:.+".contains(&b))) {
                    self.hint.lock().unwrap().insert((table.to_string(), source_id.to_string()), at);
                }
                crate::naming::StateRow::new(cell(0), cell(1), cell(2))
            }))
        }

        /// `_apitap_cdc_pending`, created on first use — outside any
        /// transaction (DDL cannot be in one), once per run.
        async fn ensure_pending_table(&self) -> Result<()> {
            if self.pending_ready.load(Ordering::Relaxed) {
                return Ok(());
            }
            if self.conn.table_get(CDC_PENDING_TABLE).await?.is_none() {
                self.conn.cdc_script(&pending_ddl(&self.conn.fq(CDC_PENDING_TABLE))).await?;
            }
            self.pending_ready.store(true, Ordering::Relaxed);
            Ok(())
        }

        /// Whether a table can have markers to clear: a dataset that never had
        /// a changelog has no marker table.
        async fn pending_exists(&self) -> Result<bool> {
            if self.pending_ready.load(Ordering::Relaxed) {
                return Ok(true);
            }
            let there = self.conn.table_get(CDC_PENDING_TABLE).await?.is_some();
            self.pending_ready.store(there, Ordering::Relaxed);
            Ok(there)
        }

        /// Best-effort, once per run, outside any script (brief §0 L11): an
        /// error is noted and never fails the run, and the next run's probe
        /// tries again.
        async fn compact_pending_if_bloated(&self) {
            if self.pending_compacted.swap(true, Ordering::Relaxed) {
                return;
            }
            let r = async {
                let Some(meta) = self.conn.table_get(CDC_PENDING_TABLE).await? else { return Ok(()) };
                // numRows can lag a hair behind recent jobs; for a bloat
                // threshold, exact is not interesting.
                let rows: u64 = meta["numRows"].as_str().and_then(|n| n.parse().ok()).unwrap_or(0);
                if rows <= PENDING_COMPACT_ROWS {
                    return Ok(());
                }
                self.conn.cdc_script(&compact_pending_sql(&self.conn.fq(CDC_PENDING_TABLE))).await
            }
            .await;
            if let Err(e) = r {
                crate::progress::note(&format!(
                    "_apitap_cdc_pending compaction skipped (replays are unaffected; the next run \
                     retries): {e}"
                ));
            }
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

        /// `_apitap_cdc_pending`, fully qualified.
        pub(crate) fn pending_fq(&self) -> String {
            self.fq(CDC_PENDING_TABLE)
        }

        /// ` AND _apitap_at >= <a constant>`: the partition bound of every
        /// changelog row this run can find at its stamps, or nothing when the
        /// run started from no watermark.
        ///
        /// Every row at a stamp at or past this run's first window start was
        /// committed in or after the transaction that wrote the watermark it
        /// started from (one transaction holds a window's rows, marker and
        /// state; an older version's rows at that stamp were its watermark's
        /// own window). A month of slack on top, and the bound is a literal,
        /// so the default monthly `_apitap_at` partitions prune.
        pub(crate) fn prune(&self, table: &str, source_id: &str) -> String {
            match self.s.hint.lock().unwrap().get(&(table.to_string(), source_id.to_string())) {
                Some(at) => format!(" AND _apitap_at >= TIMESTAMP_SUB(TIMESTAMP '{at}', INTERVAL 31 DAY)"),
                None => String::new(),
            }
        }

        /// The newest marker and the ceiling of each member (`probe_group_sql`),
        /// in `members` order; the marker table made first if this is its
        /// first use, and compacted after if it grew (best-effort).
        pub(crate) async fn probe_group(&self, members: &[(&str, &str)]) -> Result<Vec<(Option<Pending>, Option<u64>)>> {
            self.s.ensure_pending_table().await?;
            let m: Vec<(&str, &str, String, String)> =
                members.iter().map(|&(t, s)| (t, s, self.fq(t), self.prune(t, s))).collect();
            let rows = self.query(&probe_group_sql(&self.pending_fq(), &m)).await?;
            let unreadable = || Error::Transfer(format!("log_based changelog: unreadable probe {rows:?}"));
            let (mut marks, mut ceils) = (HashMap::new(), HashMap::new());
            for r in &rows {
                let c = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
                match c(0).as_str() {
                    "m" => {
                        marks.insert(c(1), parse_pending("1", &c(2), &c(3), &c(4))?);
                    }
                    "c" => {
                        ceils.insert(c(1), parse_ceiling(&c(2), &c(3))?);
                    }
                    _ => return Err(unreadable()),
                }
            }
            self.s.compact_pending_if_bloated().await;
            members
                .iter()
                .map(|(t, _)| match ceils.get(*t) {
                    Some(ceiling) => Ok((marks.get(*t).copied().flatten(), *ceiling)),
                    // Every member has a count row; one missing is a misread.
                    None => Err(unreadable()),
                })
                .collect()
        }

        /// The facts at `stamp` of each asked (table, source, base), by table.
        pub(crate) async fn facts_group(&self, stamp: u64, asks: &[(&str, &str, u32)]) -> Result<HashMap<String, StampFacts>> {
            let a: Vec<(&str, String, u32, String)> =
                asks.iter().map(|&(t, s, b)| (t, self.fq(t), b, self.prune(t, s))).collect();
            let mut out = HashMap::with_capacity(asks.len());
            for r in self.query(&facts_group_sql(stamp, &a)).await? {
                let cells: Vec<String> = r.into_iter().map(Option::unwrap_or_default).collect();
                let Some((t, rest)) = cells.split_first() else { continue };
                out.insert(t.clone(), parse_facts(&rest.iter().map(String::as_str).collect::<Vec<_>>())?);
            }
            if let Some((t, ..)) = asks.iter().find(|(t, ..)| !out.contains_key(*t)) {
                return Err(Error::Transfer(format!("log_based changelog: {t}: no facts came back for its stamp")));
            }
            Ok(out)
        }

        /// `table`'s markers go with its state, in its group (brief R-B2): a
        /// changelog bootstrap starts a new stream, and a marker past the new
        /// start would refuse every window as a rewind.
        pub(crate) async fn clear_pending(&mut self, table: &str, source_id: &str) -> Result<()> {
            if self.s.pending_exists().await? {
                let sql = clear_pending_sql(&self.pending_fq(), table, source_id);
                self.push(table, vec![sql]);
            }
            Ok(())
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

        /// The run's fence table (`table_delete` reads a 404 as gone). A
        /// failure keeps every member's claim (`lease::give_back_run`), and
        /// the collector that takes one deletes the fence (`lease_claim`).
        async fn close_run(&self, token: &str) -> Result<()> {
            self.conn.table_delete(&fence_ident(token)).await
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
                        // The markers go with the state (brief R-B2), in the
                        // same group: a marker past the next stream's start
                        // is a rewind.
                        if self.pending_exists().await? {
                            let sql = clear_pending_sql(&self.conn.fq(CDC_PENDING_TABLE), bare(table), source_id);
                            states.push((bare(table).to_string(), sql));
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
    use crate::logbased::replay::replay_plan;

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

    /// The masked readback must hand the staging column the text its cast
    /// reads, not the destination's string rendering. The BYTES cast is
    /// `FROM_HEX(SUBSTR(c, 3))` (WAL `\x` + hex); BigQuery's
    /// `CAST(bytes AS STRING)` is the UTF-8 reading — an error for binary
    /// input, and for text-like bytes the third character on, nonsense. The
    /// old readback selected the latter and silently re-inserted corrupt
    /// bytes (`deadbeef…` lost its leading `de`).
    #[test]
    fn readback_bytes_value_reenters_as_wal_hex() {
        let plan = ApplyPlan::build(
            "t",
            &["id".into(), "blob".into()],
            &[23, 17],
            &["id".into()],
            &types(&[("id", "INT64"), ("blob", "BYTES")]),
        )
        .unwrap();
        let keys = vec![vec![b"7".to_vec()]];
        let replay = replay_plan("t", None, None, &WindowId::new(1, 2), 1).unwrap();
        let sql = read_base_sql("`p.d.t`", &plan, &keys, &[1], &replay).unwrap();
        assert!(
            sql.contains("IF(`blob` IS NULL, NULL, CONCAT(r'\\x', LOWER(TO_HEX(`blob`))))"),
            "BYTES must come back as the WAL's \\x-hex, not a string rendering: {sql}"
        );
        assert!(!sql.contains("CAST(`blob` AS STRING)"), "{sql}");
    }

    /// A masked cell that was NULL in the pre-window row must stay NULL: the
    /// readback runs before the row's cast, and NULL has no text spelling.
    /// Every type either guards explicitly (BOOL, BYTES) or rides a CAST
    /// whose NULL argument is NULL (CAST(NULL AS STRING) is NULL).
    #[test]
    fn readback_null_values_stay_null() {
        let plan = ApplyPlan::build(
            "t",
            &["id".into(), "flag".into(), "blob".into()],
            &[23, 16, 17],
            &["id".into()],
            &types(&[("id", "INT64"), ("flag", "BOOL"), ("blob", "BYTES")]),
        )
        .unwrap();
        let keys = vec![vec![b"7".to_vec()]];
        let replay = replay_plan("t", None, None, &WindowId::new(1, 2), 1).unwrap();
        let sql = read_base_sql("`p.d.t`", &plan, &keys, &[0, 1, 2], &replay).unwrap();
        assert!(sql.contains("CAST(`id` AS STRING)"), "{sql}");
        assert!(sql.contains("IF(`flag` IS NULL, NULL, IF(`flag`, 't', 'f'))"), "{sql}");
        assert!(
            sql.contains("IF(`blob` IS NULL, NULL, CONCAT(r'\\x', LOWER(TO_HEX(`blob`))))"),
            "{sql}"
        );
    }

    #[test]
    fn merge_shapes_pk_and_nonpk_and_truncate() {
        let wal = vec!["id".to_string(), "v".to_string(), "flag".to_string()];
        let pk = vec!["id".to_string()];
        let ty = types(&[("id", "INT64"), ("v", "STRING"), ("flag", "BOOL")]);
        let plan = ApplyPlan::build("orders", &wal, &[23, 25, 16], &pk, &ty).unwrap();
        let stg = "`proj.ds.orders_0000000l000abcd__apitap_cdc`";
        let sql = plan.merge_sql("`proj.ds.orders`", stg, false, false);
        assert!(sql.contains("MERGE `proj.ds.orders` T"), "{sql}");
        assert!(sql.contains(&format!("FROM {stg}")), "{sql}");
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        // PK is never in the UPDATE SET; both non-PK columns are.
        assert!(sql.contains("SUBSTR(S._apitap_mask, 2, 1)"), "{sql}"); // v at pos 2
        assert!(sql.contains("SUBSTR(S._apitap_mask, 3, 1)"), "{sql}"); // flag at pos 3
        assert!(!sql.contains("`id` = IF"), "PK must not be updated: {sql}");
        assert!(!sql.contains("NOT MATCHED BY SOURCE"), "no truncate clause: {sql}");
        let sqlt = plan.merge_sql("`proj.ds.orders`", stg, true, false);
        assert!(sqlt.contains("WHEN NOT MATCHED BY SOURCE THEN\n  DELETE"), "{sqlt}");
    }

    #[test]
    fn all_pk_table_drops_the_update_clause() {
        let wal = vec!["a".to_string(), "b".to_string()];
        let pk = vec!["a".to_string(), "b".to_string()];
        let ty = types(&[("a", "INT64"), ("b", "STRING")]);
        let plan = ApplyPlan::build("j", &wal, &[23, 25], &pk, &ty).unwrap();
        let sql = plan.merge_sql("`p.d.j`", "`p.d.j_0000000l000abcd__apitap_cdc`", false, false);
        assert!(!sql.contains("UPDATE SET"), "no non-PK cols → no UPDATE clause: {sql}");
        assert!(sql.contains("ON T.`a` = S.`a` AND T.`b` = S.`b`"), "{sql}");
    }

    #[test]
    fn upsert_omits_null_and_masked_columns() {
        let cols = vec!["id".to_string(), "v".to_string(), "big".to_string()];
        let mut out = Vec::new();
        push_upsert(&mut out, &cols, &[Cell::Text("7".into()), Cell::Null, Cell::UnchangedToast], Some("001"), None).unwrap();
        let line = String::from_utf8(out).unwrap();
        assert!(line.contains("\"_apitap_op\":\"U\""), "{line}");
        assert!(line.contains("\"_apitap_mask\":\"001\""), "{line}");
        assert!(line.contains("\"id\":\"7\""), "{line}");
        assert!(!line.contains("\"v\""), "null omitted: {line}");
        assert!(!line.contains("\"big\""), "masked omitted: {line}");
    }

    /// The MERGE 0.56.0 wrote, captured from that tree (`merge_sql` of this
    /// plan before the moved form existed). A window with no moved masked row
    /// must still get exactly this: the moved form costs a target read.
    const PLAIN_MERGE: &str = "MERGE `p.d.t` T
USING (
  SELECT _apitap_op, _apitap_mask,
    CAST(`id` AS INT64) AS `id`,
    `v` AS `v`,
    CASE WHEN `flag` IS NULL THEN NULL WHEN `flag` IN ('t','true','TRUE','1') THEN 1 WHEN `flag` IN ('f','false','FALSE','0') THEN 0 ELSE ERROR(FORMAT('log_based: bad bool text %s for column flag', `flag`)) END AS `flag`
  FROM `p.d.t_stg`
) S
ON T.`id` = S.`id`
WHEN MATCHED AND S._apitap_op = 'D' THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET
    `v` = IF(S._apitap_mask IS NULL OR SUBSTR(S._apitap_mask, 2, 1) = '0', S.`v`, T.`v`),
    `flag` = IF(S._apitap_mask IS NULL OR SUBSTR(S._apitap_mask, 3, 1) = '0', S.`flag`, T.`flag`)
WHEN NOT MATCHED BY TARGET AND S._apitap_op = 'U' AND S._apitap_mask IS NULL THEN
  INSERT (`id`, `v`, `flag`) VALUES (S.`id`, S.`v`, S.`flag`)
WHEN NOT MATCHED BY TARGET AND S._apitap_op = 'U' THEN
  INSERT (`id`) VALUES (ERROR('log_based: masked update for a row missing at the BigQuery target — window replay out of order?'))
";

    /// C3: `UPDATE t SET id = 9 WHERE id = 1` with an untouched TOASTed body
    /// stages key 9 masked, and key 9 has no target row: 0.56.0's MERGE sent
    /// it to the ERROR arm on every retry. The moved form reads the hole from
    /// the target at key 1, `_apitap_from_*`, joined as the key's own type.
    #[test]
    fn merge_sql_moved_form() {
        let wal = vec!["id".to_string(), "v".to_string(), "flag".to_string()];
        let ty = types(&[("id", "INT64"), ("v", "STRING"), ("flag", "INT64")]);
        let plan = ApplyPlan::build("t", &wal, &[23, 25, 16], &["id".to_string()], &ty).unwrap();
        let (t, stg) = ("`p.d.t`", "`p.d.t_stg`");
        assert_eq!(plan.merge_sql(t, stg, false, false), PLAIN_MERGE, "moved=false is 0.56.0's text");
        assert_eq!(
            plan.merge_sql(t, stg, true, false),
            format!("{PLAIN_MERGE}WHEN NOT MATCHED BY SOURCE THEN\n  DELETE\n")
        );

        let m = plan.merge_sql(t, stg, false, true);
        assert!(
            m.contains("  FROM `p.d.t_stg` S\n  LEFT JOIN `p.d.t` F\n    ON F.`id` = CAST(S.`_apitap_from_0` AS INT64)\n) S\n"),
            "the old key, joined and cast as the key column is: {m}"
        );
        assert!(
            m.contains("  SELECT S._apitap_op,\n    IF(F.`id` IS NOT NULL, NULL, S._apitap_mask) AS _apitap_mask,\n"),
            "a row found at its old key leaves whole, without its mask: {m}"
        );
        assert!(
            m.contains(
                "    IF(F.`id` IS NOT NULL AND SUBSTR(S._apitap_mask, 2, 1) = '1', F.`v`,\n       S.`v`) AS `v`,\n"
            ),
            "a hole of a row found at its old key comes from there: {m}"
        );
        // A replay: the group's earlier transaction already moved the row, so
        // the old key has none. Raising there wedged the window for good; the
        // row keeps its mask and the plain arms decide — its own cells where
        // it already is, and the NOT MATCHED arm's ERROR where it is not.
        assert!(!m.contains("ERROR('log_based: t:"), "no error for an empty old key: {m}");
        assert!(!m.contains("IF(S.`_apitap_from_0` IS NOT NULL, NULL"), "a mask is dropped only where F was found: {m}");
        assert!(
            m.contains("       CASE WHEN S.`flag` IS NULL THEN NULL WHEN S.`flag` IN ('t','true','TRUE','1') THEN 1"),
            "every other cell is cast exactly as the plain form casts it: {m}"
        );
        // From the ON on, the two forms are one MERGE.
        let from_on = |s: &str| s[s.find(") S\nON ").expect("ON")..].to_string();
        assert_eq!(from_on(&m), from_on(PLAIN_MERGE));

        // A composite key joins on every part, each by its own type.
        let wal2 = vec!["a".to_string(), "b".to_string(), "body".to_string()];
        let ty2 = types(&[("a", "INT64"), ("b", "STRING"), ("body", "STRING")]);
        let pk2 = vec!["a".to_string(), "b".to_string()];
        let plan2 = ApplyPlan::build("j", &wal2, &[23, 25, 25], &pk2, &ty2).unwrap();
        let m2 = plan2.merge_sql("`p.d.j`", "`p.d.j_stg`", false, true);
        assert!(m2.contains("ON F.`a` = CAST(S.`_apitap_from_0` AS INT64) AND F.`b` = S.`_apitap_from_1`\n"), "{m2}");
        let names: Vec<String> = plan2.staging_fields().as_array().unwrap().iter()
            .map(|f| f["name"].as_str().unwrap().to_string()).collect();
        assert_eq!(names, ["_apitap_op", "_apitap_mask", "a", "b", "body", "_apitap_from_0", "_apitap_from_1"]);

        // The staged row names its old key; a row that did not move names none.
        let mut out = Vec::new();
        let old: Key = vec![b"1".to_vec(), b"x".to_vec()];
        push_upsert(&mut out, &wal2, &[Cell::Text("9".into()), Cell::Text("x".into()), Cell::UnchangedToast],
                    Some("001"), Some(&old)).unwrap();
        push_upsert(&mut out, &wal2, &[Cell::Text("3".into()), Cell::Text("y".into()), Cell::UnchangedToast],
                    Some("001"), None).unwrap();
        let lines = String::from_utf8(out).unwrap();
        let (moved, stayed) = lines.split_once('\n').unwrap();
        assert!(moved.contains("\"_apitap_from_0\":\"1\"") && moved.contains("\"_apitap_from_1\":\"x\""), "{moved}");
        assert!(!stayed.contains("_apitap_from_"), "{stayed}");
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

    /// D8, BigQuery's twin: a changelog table's share of its group's
    /// transaction is the trim, then the marker, then the rows, for every
    /// rule — and the state row the unit's close appends lands after them, in
    /// the same group, so one transaction holds all four. A marker after the
    /// rows, or none, lets rows commit that no marker names: the next run
    /// reads them as another writer's and numbers above them, every event
    /// twice. A trim after the marker deletes nothing the marker describes
    /// wrongly, but it is no longer the first write at the stamp.
    #[test]
    fn changelog_group_sql_order() {
        use crate::logbased::replay::{replay_plan, Landed, MarkerRow, Pending, StampFacts};
        #[derive(Debug, PartialEq)]
        enum St {
            Trim(u32),
            Mark(MarkerRow),
            Append,
        }
        let id = WindowId::new(100, 200);
        let at = |count: u64, max: Option<u32>| {
            let l = Landed { count, max_seq: max };
            StampFacts { all: l, from_base: l, distinct_seq: count }
        };
        let torn = StampFacts {
            all: Landed { count: 2, max_seq: Some(4) },
            from_base: Landed { count: 2, max_seq: Some(4) },
            distinct_seq: 2,
        };
        let mark = |seq_base, events| St::Mark(MarkerRow { start: 100, seq_base, end: 200, events });
        let here = Some(Pending::recorded(100, 0));
        let target = ClTarget {
            table: "t",
            source_id: "s'1",
            table_fq: "`p.d.t`".into(),
            pending_fq: "`p.d._apitap_cdc_pending`".into(),
            prune: " AND _apitap_at >= TIMESTAMP_SUB(TIMESTAMP '2026-09-01 00:00:00.000000+00', INTERVAL 31 DAY)".into(),
        };
        let insert = "INSERT INTO `p.d.t` (`id`) SELECT `id` FROM `p.d.t_x__apitap_cdc`;";
        let num = |sql: &str, after: &str| -> u64 {
            let rest = &sql[sql.find(after).unwrap_or_else(|| panic!("{after} in {sql}")) + after.len()..];
            rest.trim_start().split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
        };
        let parse = |sql: &str| -> St {
            if sql.starts_with("DELETE FROM `p.d.t` ") {
                assert!(sql.contains("_apitap_lsn = 100 AND _apitap_op != 'B'") && sql.contains(&target.prune), "{sql}");
                St::Trim(num(sql, "_apitap_seq >=") as u32)
            } else if sql.starts_with("INSERT INTO `p.d._apitap_cdc_pending` ") {
                assert!(sql.contains("VALUES ('t', 's\\'1', "), "{sql}");
                let v: Vec<u64> = sql[sql.find("'s\\'1', ").unwrap() + 8..]
                    .split(", ")
                    .take(4)
                    .map(|x| x.trim().parse().unwrap())
                    .collect();
                St::Mark(MarkerRow { start: v[0], seq_base: v[1] as u32, end: v[2], events: v[3] })
            } else if sql == insert {
                St::Append
            } else {
                panic!("a statement no step writes: {sql}")
            }
        };
        let cases: Vec<(&str, Option<Pending>, Option<StampFacts>, usize, Vec<St>)> = vec![
            ("R1, another writer's rows", None, Some(at(3, Some(2))), 5, vec![mark(3, 5), St::Append]),
            ("R1, nothing at the stamp", None, None, 5, vec![mark(0, 5), St::Append]),
            ("R2 torn", here, Some(torn), 5, vec![St::Trim(0), mark(0, 5), St::Append]),
            ("R2 intact, resumed", here, Some(at(2, Some(1))), 5, vec![mark(0, 5), St::Append]),
            ("R2 intact, all landed", here, Some(at(5, Some(4))), 5, vec![]),
            ("R2 shorter replay", here, Some(at(8, Some(7))), 5, vec![St::Trim(5)]),
            ("R2 absent member", here, Some(at(8, Some(7))), 0, vec![St::Trim(0)]),
            ("R2 torn, absent member", here, Some(torn), 0, vec![St::Trim(0)]),
            ("absent, no attempt here", None, None, 0, vec![]),
        ];
        for (what, p, f, n_ev, want) in cases {
            let plan = replay_plan("t", p, f.as_ref(), &id, n_ev).unwrap();
            let ins = (!plan.to_append().is_empty()).then(|| insert.to_string());
            let sql = changelog_group_sql(&plan, &target, ins);
            let got: Vec<St> = sql.iter().map(|s| parse(s)).collect();
            assert_eq!(got, want, "{what}: {sql:#?}");
            assert!(sql.iter().all(|s| s.ends_with(';')), "{what}: {sql:?}");

            // The close puts the watermark in the same group, last, and the
            // packer never splits a group: one transaction.
            let st = store::state_insert_sql("`p.d._apitap_state`", "t", "s'1", plan.watermark(), 1);
            let chunks = pack_whole_groups(
                store::close_groups(vec![("t".to_string(), sql.clone())], vec![("t".to_string(), st.clone())]),
                CHUNK_BYTES,
            );
            assert_eq!(chunks.len(), 1, "{what}: {chunks:?}");
            assert_eq!(chunks[0].last(), Some(&st), "{what}: the watermark is not the group's last write");
            assert_eq!(chunks[0].len(), sql.len() + 1, "{what}: {chunks:?}");
        }
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
