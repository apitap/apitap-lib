//! log_based apply into ClickHouse.
//!
//! ClickHouse has no multi-statement transactions, so the WINDOW is the
//! atom instead: the apply order (truncate → clear keys → plain insert →
//! residue → state row LAST) makes replaying the same window idempotent —
//! the delete phase clears every key the insert phase lands, so a crash
//! between insert and state write converges on the re-run, exactly like the
//! slot re-drain does. Deletes are lightweight `DELETE FROM` joined against
//! a key table (synchronous on the issuing replica by default).
//!
//! And no row lock, so no fence to hold: every statement that writes carries
//! its own ownership predicate instead (`store::owner_pred` — this run's lease
//! row exists, is not collected, and has more than half its TTL left, measured
//! against a deadline pinned for the unit so the fence never reopens inside
//! it), and is bounded server-side to that same half. An evicted drain lands
//! at most the one statement already executing; its watermark INSERT then
//! writes no row, and the drain stops. Every statement that reaches the server is in
//! `mod store`; the apply bodies write through the `ChUnit` they are handed.

use crate::error::{Error, Result};
use crate::lease::Watermark;
use crate::logbased::changelog::Changes;
use crate::logbased::collapse::{Collapsed, ResidueOp};
use crate::logbased::replay::WindowId;
use crate::logbased::rowtext::{
    ch_key_literal, render_ch_key, render_ch_row, render_ch_row_cells,
    render_ch_value, row_key_refs, row_key_refs_cells, tsv_unescape,
};
use crate::logbased::window::TableWindow;
use crate::sink::clickhouse::{ch_ident, ch_str};
use crate::wire::pgoutput::Cell;

pub(crate) use store::{ChStore, ChUnit};

use crate::naming::STATE_CURSOR_LSN as STATE_CURSOR;

// ── changelog mode (`changelog=true`) ───────────────────────────────────────
// The destination stops being a replica and becomes an append-only audit trail:
// every captured operation is INSERTed with the meta columns below, nothing is
// ever updated or deleted, and `<table>__current` derives the current state.
// ClickHouse is built for exactly this shape — no mutations, no part rewrites.
/// The changelog append's intent marker — see `ChStore::ensure_pending_table`.
/// The bare name lives in `naming` so table discovery excludes it along with
/// `_apitap_state`; this is only its quoted spelling.
const PENDING: &str = "`_apitap_cdc_pending`";
const _: () = assert!(
    // A rename that forgot the other half would make apitap replicate its own
    // bookkeeping out of a ClickHouse source.
    matches!(crate::naming::CDC_PENDING_TABLE.as_bytes(), b"_apitap_cdc_pending")
);

pub(crate) const CL_OP: &str = "_apitap_op";
pub(crate) const CL_LSN: &str = "_apitap_lsn";
pub(crate) const CL_SEQ: &str = "_apitap_seq";
pub(crate) const CL_AT: &str = "_apitap_at";
/// The op stamped on rows the BOOTSTRAP loaded: they were never observed as
/// change events, they are the baseline the log starts from. Explicit rather
/// than NULL — `NULL != 'D'` is NULL in SQL, which would silently drop every
/// baseline row out of the `__current` view.
pub(crate) const CL_BASELINE: &str = "B";

pub(crate) struct ChDest {
    store: ChStore,
}

impl ChDest {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        Ok(Self { store: ChStore::connect(url)? })
    }

    /// The store: the lease, the guard and the units a run's tenure takes.
    pub(crate) fn store(&self) -> &ChStore {
        &self.store
    }

    /// Default the created table's ORDER BY to the PK so the per-window
    /// key-join delete probes the sorting key instead of scanning.
    pub(crate) fn tweak_bootstrap_opts(&self, o2: &mut crate::TransferOptions, pk_cols: &[String]) {
        if o2.order_by.is_none() {
            o2.order_by = Some(pk_cols.join(", "));
        }
    }

    /// The drain's watermark, through the one verdict both lanes share.
    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<crate::naming::CdcWatermark>> {
        crate::naming::cdc_watermark(dest_table, self.store.read_state(dest_table, source_id).await?)
    }

    pub(crate) async fn validate_changelog_ddl(
        &self,
        dest_table: &str,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        self.store.validate_changelog_ddl(dest_table, partition_by, order_by).await
    }

    pub(crate) async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
        self.store.precheck_mode(dest_table, changelog).await
    }

    /// After a replica bootstrap, inside the unit whose close writes the state
    /// row: drop the scratch names older releases left untokenized.
    pub(crate) async fn bootstrap_finish(&self, u: &mut ChUnit<'_>, dest_table: &str) -> Result<()> {
        u.drop_legacy_scratch(dest_table).await
    }

    /// changelog=true, once, right after the bootstrap's bulk load: rebuild the
    /// table as an append-only changelog and stamp the loaded rows `B`.
    ///
    /// It has to be a REBUILD, not an `ALTER … ADD COLUMN`: ClickHouse cannot
    /// change a table's PARTITION BY after creation, and the changelog wants a
    /// time partition it can drop for retention. Doing it here also fixes the
    /// baseline rows properly — they get a real op, the slot's LSN and the
    /// bootstrap time instead of NULLs, so nothing downstream has to reason
    /// about NULL ops or NULL partitions.
    ///
    /// Data columns become Nullable on the way: a `D` record carries only the
    /// key and a `T` carries no row at all, so partial rows are inherent to a
    /// changelog.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn changelog_bootstrap_finish(
        &self,
        u: &mut ChUnit<'_>,
        dest_table: &str,
        pk_cols: &[String],
        lsn: u64,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        // The rebuild below EXCHANGEs the destination table itself — on a
        // cluster that would swap it on ONE node. Refused first.
        u.refuse_clustered(dest_table).await?;
        u.drop_legacy_scratch(dest_table).await?;
        // Already a changelog (a re-bootstrap of a table we own)? Leave it.
        //
        // "Already" means ALL FOUR meta columns, never just `_apitap_op`: a
        // source table that legitimately owns a column by that name would
        // otherwise skip the rebuild here and then fail on every window
        // forever, with the slot pinning WAL the whole time.
        let has = u
            .read(&format!(
                "SELECT count() FROM system.columns WHERE database = currentDatabase() \
                 AND table = '{t}' AND name IN ('{a}', '{b}', '{c}', '{d}')",
                t = ch_str(dest_table),
                a = ch_str(CL_OP),
                b = ch_str(CL_LSN),
                c = ch_str(CL_SEQ),
                d = ch_str(CL_AT),
            ))
            .await?;
        match has.trim() {
            "0" => {}
            "4" => return Ok(()),
            n => {
                return Err(Error::InvalidInput(format!(
                    "log_based changelog: ClickHouse target {dest_table} already has {n} of \
                     the four reserved changelog columns ({CL_OP}, {CL_LSN}, {CL_SEQ}, \
                     {CL_AT}) — a source column is colliding with them. Rename it at the \
                     source or alias it in a view"
                )))
            }
        }

        // Existing columns, in order, so the rebuild can widen them to Nullable.
        let cols = u.columns(dest_table).await?;
        if cols.is_empty() {
            return Err(Error::Transfer(format!(
                "log_based changelog: ClickHouse table {dest_table} has no columns — \
                 the bootstrap must run first"
            )));
        }
        let part = ch_partition_expr(partition_by);
        let order = order_by.map(str::to_string).unwrap_or_else(|| {
            let mut k: Vec<String> = pk_cols.iter().map(|c| ch_ident(c)).collect();
            k.push(CL_LSN.to_string());
            k.push(CL_SEQ.to_string());
            k.join(", ")
        });
        let sel = cols
            .iter()
            .map(|(n, ty)| {
                let q = ch_ident(n);
                match cl_nullable(ty) {
                    Some(w) => format!("CAST({q} AS {w}) AS {q}"),
                    None => q,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sel = format!(
            "{sel}, CAST('{op}' AS String) AS {CL_OP}, CAST({lsn} AS UInt64) AS {CL_LSN}, \
             CAST(0 AS UInt32) AS {CL_SEQ}, now64(3) AS {CL_AT}",
            op = ch_str(CL_BASELINE),
        );
        u.changelog_rebuild(dest_table, &sel, &part, &order).await?;
        u.current_view(dest_table, pk_cols).await
    }

    /// changelog=true apply — see `apply_changelog_unit`.
    pub(crate) async fn apply_changelog(
        &self,
        u: &mut ChUnit<'_>,
        dest_table: &str,
        w: Option<&TableWindow<Changes>>,
        id: &WindowId,
        source_id: &str,
    ) -> Result<(u64, Watermark)> {
        apply_changelog_unit(u, dest_table, w, id, source_id).await
    }

    /// Apply one collapsed window. The state is written LAST, by the unit's
    /// close — a re-run of the same window is idempotent (see module docs).
    pub(crate) async fn apply(
        &self,
        u: &mut ChUnit<'_>,
        dest_table: &str,
        w: Option<&TableWindow<Collapsed>>,
        id: &WindowId,
        source_id: &str,
    ) -> Result<(u64, Watermark)> {
        apply_unit(u, dest_table, w, id, source_id).await
    }
}

/// `<table>__current`: the current state derived from the log.
///
/// Three things it has to get right, in this order:
/// 1. **TRUNCATE.** A `T` record means everything logged before it is gone,
///    so the view first drops every row at or below the newest `T`.
/// 2. **Latest version per key.** Ordering is the PAIR `(lsn, seq)`, never
///    `lsn` alone: one window stamps one LSN on every row it lands, so `seq`
///    is what orders events inside a window.
/// 3. **Deletes.** A key whose newest record is `D` is gone — filtered AFTER
///    the pick, not before, or the delete would be skipped and the previous
///    version would resurrect.
///
/// Baseline (`B`) rows carry the slot's consistent-point LSN, so any later
/// change outranks them.
fn current_view_sql(dest_table: &str, pk_cols: &[String]) -> String {
    let keys = pk_cols.iter().map(|c| ch_ident(c)).collect::<Vec<_>>().join(", ");
    let view = ch_ident(&format!("{dest_table}__current"));
    let t = ch_ident(dest_table);
    format!(
        "CREATE OR REPLACE VIEW {view} AS SELECT * FROM ( \
           SELECT * FROM {t} \
           WHERE ({CL_LSN}, {CL_SEQ}) > ( \
             SELECT ifNull(max(({CL_LSN}, {CL_SEQ})), (toUInt64(0), toUInt32(0))) \
             FROM {t} WHERE {CL_OP} = '{tr}' \
           ) \
           ORDER BY {CL_LSN} DESC, {CL_SEQ} DESC, {CL_OP} = '{base}' ASC \
           LIMIT 1 BY {keys} \
         ) WHERE {CL_OP} != '{del}'",
        tr = ch_str("T"),
        del = ch_str("D"),
        // The tie-break, and it became load-bearing in 0.56.0: a window is
        // stamped with the watermark it was drained FROM, and the FIRST window
        // after a bootstrap starts exactly where the baseline snapshot was
        // taken. So a baseline row and that window's first event for the same
        // key can carry the identical (lsn, seq) — every baseline row is
        // written with seq 0 — and without this the winner of that tie is
        // arbitrary. A real change always outranks the snapshot it changed.
        base = ch_str(CL_BASELINE),
    )
}

/// One readback per window: the current value of every masked column, for
/// every key that needs one, from `<table>__current`. The view filters the
/// base table by key first, so this probes the sorting key rather than
/// scanning the log.
async fn read_current(
    u: &mut ChUnit<'_>,
    dest_table: &str,
    pk_cols: &[String],
    pk_oids: &[u32],
    keys: &[crate::logbased::changelog::CKey],
    cols: &[usize],
    wal_cols: &[String],
) -> Result<std::collections::HashMap<crate::logbased::changelog::CKey, Vec<Option<bytes::Bytes>>>> {
    let view = ch_ident(&format!("{dest_table}__current"));
    let sel = pk_cols
        .iter()
        .map(|c| ch_ident(c))
        .chain(cols.iter().map(|&i| ch_ident(&wal_cols[i])))
        .collect::<Vec<_>>()
        .join(", ");
    let mut preds = Vec::with_capacity(keys.len());
    for k in keys {
        preds.push(format!("({})", key_pred(pk_cols, k, pk_oids)?));
    }
    let body = u.read(&format!("SELECT {sel} FROM {view} WHERE {} FORMAT TabSeparated", preds.join(" OR "))).await?;
    let np = pk_cols.len();
    let mut out = std::collections::HashMap::with_capacity(keys.len());
    for line in body.lines().filter(|l| !l.is_empty()) {
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() != np + cols.len() {
            return Err(Error::Transfer("log_based changelog: masked readback column count mismatch".into()));
        }
        let key: crate::logbased::changelog::CKey = f[..np].iter().map(|x| tsv_unescape(x).unwrap_or_default()).collect();
        let vals = f[np..].iter().map(|x| tsv_unescape(x).map(bytes::Bytes::from)).collect();
        out.insert(key, vals);
    }
    Ok(out)
}

/// changelog=true apply: ONE plain INSERT of every captured operation.
///
/// No delete-set, no key table, no DELETE, no TRUNCATE — ClickHouse never
/// writes a mutation, so the destination never rewrites parts.
///
/// **Replay.** The INSERT and the watermark are two round-trips and ClickHouse
/// has no transaction to hold them together, so a window CAN be re-drained
/// after its rows landed: the process dies in between, or a sibling table in
/// the same group fails its apply and the next run restarts from the group
/// minimum. Two things make that safe.
///
/// 1. The stamp is the window's START — the watermark it was drained
///    FROM, the one position that is identical on a replay, so `(lsn, seq)`
///    is a real event identity a consumer can de-duplicate on.
/// 2. `_apitap_cdc_pending` records the window we are ABOUT to append. If the
///    next attempt opens on the same start, the rows already in the table at
///    that stamp are counted and skipped — so the ordinary replay appends
///    nothing twice at all.
async fn apply_changelog_unit(
    u: &mut ChUnit<'_>,
    dest_table: &str,
    w: Option<&TableWindow<Changes>>,
    id: &WindowId,
    source_id: &str,
) -> Result<(u64, Watermark)> {
    let set = |rows: u64| Watermark::Set {
        table: dest_table.to_string(),
        source_id: source_id.to_string(),
        lsn: id.end(),
        rows,
    };
    // Memoized no-op in steady state; here so no window ever writes to a
    // table that turned Replicated under us mid-run.
    u.refuse_clustered(dest_table).await?;
    let Some(w) = w else {
        return Ok((0, set(0)));
    };
    let (c, l) = (w.body(), w.layout());
    let (wal_cols, oids) = (l.cols(), l.oids());
    for name in wal_cols {
        if matches!(name.as_str(), CL_OP | CL_LSN | CL_SEQ | CL_AT) {
            return Err(Error::InvalidInput(format!(
                "log_based changelog: source column '{name}' collides with a reserved \
                 changelog column — rename it at the source or alias it in a view"
            )));
        }
    }
    if c.events.is_empty() {
        return Ok((0, set(0)));
    }

    // Unchanged-TOAST cells must be rebuilt before anything is written —
    // writing them as NULL would silently blank the column for every reader of
    // `__current`. Costs one extra query per window, and only when the window
    // actually carries a masked cell.
    let patched = if c.masked {
        let (keys, cols) = c.mask_plan();
        let base = if keys.is_empty() || cols.is_empty() {
            std::collections::HashMap::new()
        } else {
            read_current(u, dest_table, l.key_cols(), &l.key_oids(), &keys, &cols, wal_cols).await?
        };
        c.resolve_masked(&cols, &base)?
    } else {
        std::collections::HashMap::new()
    };

    let cols: Vec<String> = wal_cols
        .iter()
        .cloned()
        .chain([CL_OP, CL_LSN, CL_SEQ, CL_AT].map(String::from))
        .collect();
    // The window's START, not its end: see the replay note above.
    let lsn = id.start();
    // Did a previous attempt at THIS window already append? Only asked when the
    // marker names the same start — on the ordinary path it names the previous
    // window's, and the count below (which scans `_apitap_lsn`, a sorting-key
    // SUFFIX, so it prunes nothing) is never run.
    let mut skip = 0usize;
    if u.pending_window(dest_table, source_id).await? == Some(lsn) {
        match u.appended_prefix(dest_table, lsn).await? {
            Some(n) => skip = n.min(c.events.len()),
            // Not a prefix: the surviving rows have a hole in them, so there is
            // no safe place to resume. Re-append the whole window — the stamps
            // are stable, so the overlap is an exact `(lsn, seq)` duplicate
            // that `__current` and any consumer can collapse, which is the
            // bad-but-honest outcome rather than a silent gap.
            None => {
                eprintln!(
                    "apitap: {dest_table}: a previous append of the window at lsn {lsn} \
                     left an incomplete run of rows, so it cannot be resumed part-way. \
                     Re-appending the whole window; rows carrying a repeated \
                     ({CL_LSN}, {CL_SEQ}) are duplicates of each other and may be \
                     de-duplicated on that pair."
                );
            }
        }
    }
    if skip >= c.events.len() {
        // Everything already landed; only the watermark was missing.
        return Ok((c.count, set(c.count)));
    }
    u.mark_pending_owned(dest_table, source_id, lsn).await?;
    // ONE stamp for the window. It is the PARTITION/retention key, never an
    // ordering key — `(lsn, seq)` orders. Sent explicitly rather than left to a
    // default: the rebuild materialised `_apitap_at` as a plain column, so a
    // NULL would land as the epoch and pile the whole log into a 1970 partition.
    let at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let mut buf = Vec::with_capacity(4 << 20);
    for (seq, ev) in c.events.iter().enumerate().skip(skip) {
        match patched.get(&seq).or(ev.row.as_ref()) {
            // A delete's old image carries the key and NULLs elsewhere — that
            // IS the delete record, so it renders like any row.
            Some(row) => render_ch_row_trim(row, oids, wal_cols.len(), &mut buf)?,
            // TRUNCATE has no row: every data column is \N.
            None => {
                for i in 0..wal_cols.len() {
                    if i > 0 {
                        buf.push(b'\t');
                    }
                    buf.extend_from_slice(b"\\N");
                }
            }
        }
        buf.push(b'\t');
        buf.extend_from_slice(ev.op.code().as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(lsn.to_string().as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(seq.to_string().as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(at.as_bytes());
        buf.push(b'\n');
    }
    u.insert_owned(dest_table, &cols, buf).await?;
    Ok((c.count, set(c.count)))
}

/// Apply one collapsed window through the unit, and name the watermark its
/// close writes. A window with no traffic for this table writes nothing but
/// that mark. Columns, types and keys are the window's own layout.
async fn apply_unit(
    u: &mut ChUnit<'_>,
    dest_table: &str,
    w: Option<&TableWindow<Collapsed>>,
    id: &WindowId,
    source_id: &str,
) -> Result<(u64, Watermark)> {
    let set = |rows: u64| Watermark::Set {
        table: dest_table.to_string(),
        source_id: source_id.to_string(),
        lsn: id.end(),
        rows,
    };
    // Before the window's first DDL: a table that turned Replicated mid-run.
    u.refuse_clustered(dest_table).await?;
    let Some(w) = w else {
        // Foreign-table traffic only: nothing for our table, still advance.
        return Ok((0, set(0)));
    };
    let (c, l) = (w.body(), w.layout());
    let (wal_cols, oids, pk_cols, pk_idx) = (l.cols(), l.oids(), l.key_cols(), l.key_idx());
    let ft = ch_ident(dest_table);
    let pk_oids = l.key_oids();
    let pklist = pk_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");

    if c.truncate {
        u.clear_owned(dest_table).await?;
    }

    // Clear the delete-set ∪ every upsert key first, so the insert phase is a
    // plain bulk INSERT (same move as the pg apply).
    if !c.deletes.is_empty() || !c.upserts.is_empty() {
        let kt = u.key_table_reset(dest_table, pk_cols).await?;
        let mut buf = Vec::with_capacity(1 << 20);
        for key in c.deletes.iter() {
            let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
            render_ch_key(&refs, &pk_oids, &mut buf)?;
        }
        for row in &c.upserts {
            render_ch_key(&row_key_refs(row, pk_idx), &pk_oids, &mut buf)?;
        }
        u.insert_owned(&kt, pk_cols, buf).await?;
        let kq = ch_ident(&kt);
        let pred = if pk_cols.len() == 1 {
            format!("{pklist} IN (SELECT {pklist} FROM {kq})")
        } else {
            format!("({pklist}) IN (SELECT {pklist} FROM {kq})")
        };
        u.delete_owned(dest_table, &pred).await?;
    }

    if !c.upserts.is_empty() {
        let mut buf = Vec::with_capacity(4 << 20);
        for row in &c.upserts {
            render_ch_row(row, oids, &mut buf)?;
        }
        u.insert_owned(dest_table, wal_cols, buf).await?;
    }

    // Residue tail: serial, ordered. Masked TOAST updates read the missing
    // columns back from the destination, then delete + reinsert the patched
    // row (ClickHouse has no cheap row UPDATE).
    for op in &c.residue {
        match op {
            ResidueOp::MaskedUpdate { key, row } => {
                let mut full = row.clone();
                let missing: Vec<usize> = full
                    .iter()
                    .enumerate()
                    .filter(|(_, cell)| matches!(cell, Cell::UnchangedToast))
                    .map(|(i, _)| i)
                    .collect();
                let pred = key_pred(pk_cols, key, &pk_oids)?;
                if !missing.is_empty() {
                    let sel = missing.iter().map(|&i| ch_ident(&wal_cols[i])).collect::<Vec<_>>().join(", ");
                    let body = u.read(&format!("SELECT {sel} FROM {ft} WHERE {pred} FORMAT TabSeparated")).await?;
                    let Some(line) = body.lines().next() else {
                        return Err(Error::Transfer(
                            "log_based: masked update for a row missing at the destination — \
                             window replay out of order?"
                                .into(),
                        ));
                    };
                    let fields: Vec<&str> = line.split('\t').collect();
                    if fields.len() != missing.len() {
                        return Err(Error::Transfer("log_based: masked-update readback column count mismatch".into()));
                    }
                    for (&i, f) in missing.iter().zip(fields.iter()) {
                        // Readback is already destination-dialect: escape it
                        // straight back out, no OID translation.
                        full[i] = match tsv_unescape(f) {
                            None => Cell::Null,
                            Some(v) => Cell::Text(bytes::Bytes::from(v)),
                        };
                    }
                }
                u.delete_owned(dest_table, &pred).await?;
                let mut buf = Vec::new();
                render_residue_row(&full, oids, &missing, &mut buf)?;
                u.insert_owned(dest_table, wal_cols, buf).await?;
            }
            ResidueOp::Upsert { row } => {
                let key: Vec<Vec<u8>> = row_key_refs_cells(row, pk_idx).into_iter().map(|k| k.to_vec()).collect();
                let pred = key_pred(pk_cols, &key, &pk_oids)?;
                u.delete_owned(dest_table, &pred).await?;
                let mut buf = Vec::new();
                render_ch_row_cells(row, oids, &mut buf)?;
                u.insert_owned(dest_table, wal_cols, buf).await?;
            }
            ResidueOp::Delete { key } => {
                let pred = key_pred(pk_cols, key, &pk_oids)?;
                u.delete_owned(dest_table, &pred).await?;
            }
            ResidueOp::Rekey { old_key, row, .. } => {
                // ClickHouse has no cheap row UPDATE, so the move is a readback
                // + delete + insert like MaskedUpdate — except the readback
                // addresses the OLD key. That is the whole difference: the row
                // still exists there, and it is the only place the TOASTed
                // value can be found.
                let mut full = row.clone();
                let missing: Vec<usize> = full
                    .iter()
                    .enumerate()
                    .filter(|(_, cell)| matches!(cell, Cell::UnchangedToast))
                    .map(|(i, _)| i)
                    .collect();
                let old_pred = key_pred(pk_cols, old_key, &pk_oids)?;
                if !missing.is_empty() {
                    let sel = missing.iter().map(|&i| ch_ident(&wal_cols[i])).collect::<Vec<_>>().join(", ");
                    let body =
                        u.read(&format!("SELECT {sel} FROM {ft} WHERE {old_pred} FORMAT TabSeparated")).await?;
                    match body.lines().next() {
                        Some(line) => {
                            let fields: Vec<&str> = line.split('\t').collect();
                            if fields.len() != missing.len() {
                                return Err(Error::Transfer("log_based: re-key readback column count mismatch".into()));
                            }
                            for (&i, f) in missing.iter().zip(fields.iter()) {
                                full[i] = match tsv_unescape(f) {
                                    None => Cell::Null,
                                    Some(v) => Cell::Text(bytes::Bytes::from(v)),
                                };
                            }
                        }
                        // The old key is not there. On a replayed window that is
                        // the expected shape — the move already happened and the
                        // row sits at the new key — so skip rather than fail.
                        None => continue,
                    }
                }
                u.delete_owned(dest_table, &old_pred).await?;
                let mut buf = Vec::new();
                render_residue_row(&full, oids, &missing, &mut buf)?;
                u.insert_owned(dest_table, wal_cols, buf).await?;
            }
        }
    }
    Ok((c.events, set(c.events)))
}

/// Everything that reaches the server. See the module doc.
mod store {
    use super::{ch_engine_ok, current_view_sql, is_shape_ok, ch_partition_expr, CL_AT, CL_BASELINE, CL_LSN, CL_OP, CL_SEQ, PENDING, STATE_CURSOR};
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{no_longer_holds, owned_margin_secs, ttl_secs, Fence, LeaseStore, Watermark};
    use crate::naming::{artifact_ident, artifact_ident_tok, Artifact, ROOMY};
    use crate::sink::clickhouse::{ch_ident, ch_str, ChConn, ChGuard};
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    pub(crate) struct ChStore {
        ch: ChConn,
        /// DDL this connection has already issued. `CREATE TABLE IF NOT
        /// EXISTS` is idempotent but not free: it is a full HTTP round trip
        /// against a window that only has ~7 of them, repeated for every
        /// window of every table. The first window creates; the rest remember.
        ensured: Mutex<HashSet<String>>,
        /// Once-per-run verdict of `patch_ok` (None = not probed yet).
        patch: Mutex<Option<bool>>,
        /// Column types per table, for `input()` structures. Invalidated by
        /// every op that changes a table's shape.
        structures: Mutex<HashMap<String, HashMap<String, String>>>,
        #[cfg(test)]
        pub(super) ops: Mutex<Vec<&'static str>>,
    }

    /// One unit: a set of lease keys and the run that holds them. ClickHouse
    /// has no transaction to open, so the unit is the predicate every write
    /// carries.
    pub(crate) struct ChUnit<'a> {
        s: &'a ChStore,
        keys: Vec<String>,
        token: String,
        pin: Pin,
        /// An owned statement was sent since the pin: the next re-pin owes
        /// the proof that it ran under the pinned deadline (`repin`'s
        /// continuity). With none sent there is nothing to prove, and a
        /// re-pin is a fresh one — the changelog rebuild's CTAS is unfenced
        /// and unbounded, and a CTAS longer than the pin's budget used to fail
        /// a live claim on every run.
        owed: bool,
    }

    /// A unit's time fence, pinned. Every statement of the unit carries
    /// `now + margin < e0` (`pinned_pred`) besides the live `owner_pred`.
    ///
    /// The live margin alone is not monotone: it dips while renewals fail or
    /// the process is stopped, and comes back with the next renewal. A
    /// statement evaluated in the dip writes nothing and says nothing — an
    /// `INSERT … WHERE false` succeeds — so a later statement, the watermark
    /// INSERT included, could pass after an earlier one was fenced out, and
    /// record a window that never landed. Against a fixed `e0` the fence only
    /// ever closes: the first statement that fails it fails every statement
    /// after it, the watermark writes no row, and the window replays.
    ///
    /// Renewals only raise `expires_at`, so while the rows stay unclaimed the
    /// live `expires_at` is at least `e0`, and the pinned test is at least as
    /// strict as the live one.
    struct Pin {
        /// The lowest `expires_at` among the unit's rows, epoch micros by the
        /// SERVER clock, as of the last read that proved no gap.
        e0: u64,
        /// When it was pinned and what it leaves (`e0 - now - margin`), by the
        /// client clock. Only WHEN to re-pin is decided with these, never
        /// whether a statement may write.
        at: std::time::Instant,
        budget: std::time::Duration,
    }

    pub(super) fn pinned_pred(e0: u64, margin_us: u64) -> String {
        format!("toUnixTimestamp64Micro(now64(6)) + {margin_us} < {e0}")
    }

    /// The verdict of a pin read: the server's `now`, the lowest live
    /// `expires_at` among the unit's unclaimed rows and how many of its keys
    /// those are, against the deadline the unit carried until now (`prev`,
    /// none at open). `Some(new e0)` only when every row is still unclaimed
    /// with more than the margin left AND the read itself still passes the old
    /// deadline — which proves every earlier statement (each started before
    /// this read) passed it too. Otherwise one of them may have been fenced
    /// out without a word, and the unit must not go on to a watermark.
    pub(super) fn repin(now: u64, e: u64, owned: usize, keys: usize, margin_us: u64, prev: Option<u64>) -> Option<u64> {
        let live = owned == keys && now.saturating_add(margin_us) < e;
        let continuous = prev.map_or(true, |p| now.saturating_add(margin_us) < p);
        (live && continuous).then_some(e)
    }

    fn margin_us() -> u64 {
        owned_margin_secs() * 1_000_000
    }

    /// Whether a unit re-reads its pin before its next statement: when forced,
    /// or once an EIGHTH of the pin's budget is spent. Early, so an owned
    /// statement starts with most of the budget ahead of it: the re-pin after
    /// it must still pass the old deadline, and a statement that started with
    /// half the budget left (the rule until this) could not run past that half
    /// without failing a live claim — a lightweight DELETE of a minute at TTL
    /// 300 replayed its window for ever. Short windows still never re-read.
    pub(super) fn repin_due(elapsed: std::time::Duration, budget: std::time::Duration, force: bool) -> bool {
        force || elapsed >= budget / 8
    }

    /// The ownership predicate a statement carries: this run's lease row
    /// exists, is not collected, and has more than `margin` seconds of life.
    /// The margin is what makes a predicate evaluated at the statement's START
    /// good for its whole run: the statement is bounded to the same margin
    /// server-side (`owned_settings`), and a claim needs the row LAPSED.
    pub(super) fn owner_pred(key: &str, tok: &str, margin: u64) -> String {
        format!(
            "1 IN (SELECT toUInt8(count() > 0 AND argMax(collected, seq) = 0 AND \
             argMax(expires_at, seq) > now64(6) + INTERVAL {margin} SECOND) \
             FROM `{t}` WHERE dest_key = '{k}' AND token = '{tok}')",
            t = crate::lease::LEASE_TABLE,
            k = ch_str(key),
            tok = ch_str(tok),
        )
    }

    /// Every owned statement ends before its predicate could go stale.
    fn owned_settings() -> Vec<(&'static str, String)> {
        let m = owned_margin_secs().to_string();
        vec![("max_execution_time", m.clone()), ("http_receive_timeout", m)]
    }

    /// `input()`'s structure for `cols`, typed from the table. Quoted as a
    /// string literal: a type carries quotes of its own (`DateTime64(6, 'UTC')`,
    /// `Enum8('a' = 1)`).
    pub(super) fn structure(cols: &[String], types: &HashMap<String, String>) -> Result<String> {
        cols.iter()
            .map(|c| {
                types
                    .get(c)
                    .map(|t| format!("{} {t}", ch_ident(c)))
                    .ok_or_else(|| Error::Transfer(format!("log_based: column {c} is not in the destination table")))
            })
            .collect::<Result<Vec<_>>>()
            .map(|v| v.join(", "))
    }

    pub(super) fn insert_owned_sql(table: &str, cols: &[String], structure: &str, pred: &str) -> String {
        let cl = cols.iter().map(|c| ch_ident(c)).collect::<Vec<_>>().join(", ");
        format!(
            "INSERT INTO {t} ({cl}) SELECT {cl} FROM input('{s}') WHERE {pred} FORMAT TabSeparated",
            t = ch_ident(table),
            s = ch_str(structure),
        )
    }

    impl ChStore {
        pub(crate) fn connect(url: &str) -> Result<Self> {
            Ok(Self {
                ch: ChConn::parse(url)?,
                ensured: Default::default(),
                patch: Default::default(),
                structures: Default::default(),
                #[cfg(test)]
                ops: Default::default(),
            })
        }

        fn note(&self, _op: &'static str) {
            #[cfg(test)]
            self.ops.lock().unwrap().push(_op);
        }

        pub(crate) fn ch_guard(&self) -> ChGuard {
            ChGuard::new(self.ch.clone(), None)
        }

        /// True the FIRST time this connection is asked about `key`.
        fn first_time(&self, key: &str) -> bool {
            self.ensured.lock().unwrap().insert(key.to_string())
        }

        fn patched(&self, table: &str) -> bool {
            self.ensured.lock().unwrap().contains(&format!("\u{1}patch\u{1}{table}"))
        }

        /// Patch-part deletes (`lightweight_delete_mode='lightweight_update'`)
        /// turn the per-window DELETE from a part REWRITE into a patch-part
        /// write. Probed once per run: server >= 25.7 required (the setting
        /// does not exist below), and correctness of OUR predicate shape —
        /// including parts born before the ALTER — was verified against
        /// 25.8.29. 24.8 LTS destinations keep the rewrite path untouched.
        /// `APITAP_PATCH_DELETE=0` is the kill switch (and the A/B lever).
        async fn patch_ok(&self) -> bool {
            if std::env::var("APITAP_PATCH_DELETE").as_deref() == Ok("0") {
                return false;
            }
            if let Some(v) = *self.patch.lock().unwrap() {
                return v;
            }
            let ok = match self.ch.read("SELECT version()").await {
                Ok(body) => {
                    let mut it = body.trim().split('.');
                    let maj: u32 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
                    let min: u32 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
                    maj > 25 || (maj == 25 && min >= 7)
                }
                Err(_) => false,
            };
            *self.patch.lock().unwrap() = Some(ok);
            ok
        }

        async fn ensure_state_table(&self) -> Result<()> {
            if !self.first_time("\u{1}state") {
                return Ok(());
            }
            self.ch
                .exec(
                    "CREATE TABLE IF NOT EXISTS `_apitap_state` (\
                       dest_table String, source_id String, cursor_col String, \
                       watermark String, mode String, last_rows UInt64, \
                       synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) \
                     ENGINE = ReplacingMergeTree(synced_at) ORDER BY (dest_table, source_id)",
                )
                .await?;
            Ok(())
        }

        /// The changelog append's intent marker: "a window starting at `lsn` is
        /// being appended to `dest_table`". Its own table rather than a row in
        /// `_apitap_state`, because that one is `ReplacingMergeTree ORDER BY
        /// (dest_table, source_id)` — a second row for the same pair does not
        /// sit beside the watermark, it REPLACES it.
        async fn ensure_pending_table(&self) -> Result<()> {
            if !self.first_time("\u{1}pending") {
                return Ok(());
            }
            self.ch
                .exec(&format!(
                    "CREATE TABLE IF NOT EXISTS {PENDING} (\
                       dest_table String, source_id String, lsn UInt64, \
                       at DateTime64(6, 'UTC') DEFAULT now64(6)) \
                     ENGINE = ReplacingMergeTree(at) ORDER BY (dest_table, source_id)"
                ))
                .await?;
            Ok(())
        }

        /// Refuse a clustered (Replicated*) destination table, loudly, before
        /// the CDC apply touches it.
        ///
        /// Every object this file creates for itself — the `_apitap_state`
        /// watermark, the per-run key table, the changelog rebuild and its
        /// `__current` view — is node-local DDL, no ON CLUSTER. The bootstrap
        /// rides the bulk sink, which DOES thread on_cluster, so on a cluster
        /// the destination table would exist on every node while apitap's
        /// sidecars existed only on whichever node the balancer routed. A later
        /// drain lands where they are missing — or reads a stale node-local
        /// watermark and silently skips a window. The verdict comes from the
        /// table's ENGINE, not the run's options: a pre-created Replicated
        /// table diverges exactly the same way with no options passed at all.
        /// Memoized per table once the table has been SEEN: a table that does
        /// not exist yet passes, and is asked again by the first state write.
        pub(super) async fn refuse_clustered(&self, dest_table: &str) -> Result<()> {
            let key = format!("\u{1}cluster\u{1}{dest_table}");
            if self.ensured.lock().unwrap().contains(&key) {
                return Ok(());
            }
            let eng = self
                .ch
                .read(&format!(
                    "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '{t}'",
                    t = ch_str(dest_table),
                ))
                .await?;
            ch_engine_ok(dest_table, eng.trim())?;
            if !eng.trim().is_empty() {
                self.ensured.lock().unwrap().insert(key);
            }
            Ok(())
        }

        /// The table's columns, in order, with their types.
        async fn columns(&self, table: &str) -> Result<Vec<(String, String)>> {
            // TabSeparatedRaw, not TabSeparated: TSV escapes single quotes, so a
            // `DateTime64(6, 'UTC')` column would come back as
            // `DateTime64(6, \'UTC\')` and land verbatim in a CAST or input().
            let desc = self
                .ch
                .read(&format!(
                    "SELECT name, type FROM system.columns WHERE database = currentDatabase() \
                     AND table = '{t}' ORDER BY position FORMAT TabSeparatedRaw",
                    t = ch_str(table),
                ))
                .await?;
            let mut cols = Vec::new();
            for line in desc.lines().filter(|l| !l.is_empty()) {
                let mut it = line.splitn(2, '\t');
                let (Some(n), Some(ty)) = (it.next(), it.next()) else { continue };
                cols.push((n.to_string(), ty.to_string()));
            }
            Ok(cols)
        }

        async fn types(&self, table: &str) -> Result<HashMap<String, String>> {
            if let Some(t) = self.structures.lock().unwrap().get(table) {
                return Ok(t.clone());
            }
            let t: HashMap<String, String> = self.columns(table).await?.into_iter().collect();
            self.structures.lock().unwrap().insert(table.to_string(), t.clone());
            Ok(t)
        }

        fn forget_shape(&self, table: &str) {
            self.structures.lock().unwrap().remove(table);
        }

        /// This table's state row, whichever lane wrote it (the one read both
        /// lanes send: `sink::clickhouse::state_read_sql`).
        pub(crate) async fn read_state(
            &self,
            dest_table: &str,
            source_id: &str,
        ) -> Result<Option<crate::naming::StateRow>> {
            // Run admission: read_state is the FIRST thing a run asks this
            // destination, for every table — the moment to notice a clustered
            // target and refuse before a bootstrap or a drain moves any data.
            self.refuse_clustered(dest_table).await?;
            let body = match self.ch.read(&crate::sink::clickhouse::state_read_sql(dest_table, source_id)).await {
                Ok(b) => b,
                // No state table at all = fresh destination.
                Err(Error::Transfer(m)) if m.contains("UNKNOWN_TABLE") || m.contains("doesn't exist") => {
                    return Ok(None)
                }
                Err(e) => return Err(e),
            };
            crate::sink::clickhouse::state_row_of(&body)
        }

        /// Ask ClickHouse itself whether these clauses resolve against the
        /// table's real columns. `SELECT <expr> FROM t LIMIT 0` reads no data
        /// and returns the same `UNKNOWN_IDENTIFIER` the CREATE would, so a
        /// typo or a column only some members of a group own is caught before
        /// anything is written.
        pub(crate) async fn validate_changelog_ddl(
            &self,
            dest_table: &str,
            partition_by: Option<&str>,
            order_by: Option<&str>,
        ) -> Result<()> {
            let ft = ch_ident(dest_table);
            // partition_by is checked in its EXPANDED form — a bare column name
            // is a month, not a raw key — so validation and DDL can never
            // disagree.
            let pb = partition_by.map(|_| ch_partition_expr(partition_by));
            for (what, expr) in [("partition_by", pb.as_deref()), ("order_by", order_by)] {
                let Some(expr) = expr else { continue };
                // The meta columns exist only after the rebuild, so a clause
                // that uses them is checked against them explicitly.
                let probe = format!(
                    "SELECT {expr} FROM (SELECT *, CAST('{b}' AS String) AS {CL_OP}, \
                     CAST(0 AS UInt64) AS {CL_LSN}, CAST(0 AS UInt32) AS {CL_SEQ}, \
                     now64(3) AS {CL_AT} FROM {ft} LIMIT 0) LIMIT 0 FORMAT TabSeparatedRaw",
                    b = ch_str(CL_BASELINE),
                );
                if let Err(e) = self.ch.read(&probe).await {
                    return Err(Error::InvalidInput(format!(
                        "log_based changelog: {what}={expr:?} does not resolve against \
                         {dest_table}. In a multi-table run every table gets this same \
                         clause unless you pass a dict — give {what} per table, e.g. \
                         {what}={{\"orders\": \"…\", \"events\": \"…\"}}. ClickHouse said: {e}"
                    )));
                }
            }
            Ok(())
        }

        /// The destination's SHAPE must match the mode, checked ONCE at run
        /// start on a table that already has state (a fresh bootstrap builds
        /// the right shape by construction). Both directions are damage: a
        /// replica window landing on a changelog destroys the log it found; a
        /// changelog window landing on a replica has nowhere to put its meta
        /// columns.
        pub(crate) async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
            let has = self
                .ch
                .read(&format!(
                    "SELECT count() FROM system.columns WHERE database = currentDatabase() \
                     AND table = '{t}' AND name = '{c}'",
                    t = ch_str(dest_table),
                    c = ch_str(CL_OP),
                ))
                .await?;
            is_shape_ok(has.trim() != "0", changelog, "ClickHouse", dest_table)
        }

        /// Pin (or re-pin, with the deadline carried so far) the unit's time
        /// fence: one read of every key's row, decided by `repin`. Owner = the
        /// row exists, is not collected, and has more than the margin left —
        /// the same test `owner_pred` makes server-side.
        async fn pin(&self, keys: &[String], token: &str, prev: Option<u64>) -> Result<Pin> {
            let inlist = keys.iter().map(|k| format!("'{}'", ch_str(k))).collect::<Vec<_>>().join(", ");
            let body = match self
                .ch
                .read(&format!(
                    "SELECT toUnixTimestamp64Micro(now64(6)), toUnixTimestamp64Micro(min(e)), count() \
                     FROM (SELECT argMax(expires_at, seq) AS e FROM `{t}` \
                           WHERE token = '{tok}' AND dest_key IN ({inlist}) \
                           GROUP BY dest_key HAVING argMax(collected, seq) = 0) \
                     FORMAT TabSeparated",
                    t = crate::lease::LEASE_TABLE,
                    tok = ch_str(token),
                ))
                .await
            {
                Ok(b) => b,
                // No lease store: no row, so not an owner.
                Err(Error::Transfer(m)) if m.contains("UNKNOWN_TABLE") => String::new(),
                Err(e) => return Err(e),
            };
            let mut f = body.trim().split('\t').map(|v| v.trim().parse::<u64>().unwrap_or(0));
            let (now, e, owned) = (f.next().unwrap_or(0), f.next().unwrap_or(0), f.next().unwrap_or(0));
            let distinct = keys.iter().collect::<HashSet<_>>().len();
            match repin(now, e, owned as usize, distinct, margin_us(), prev) {
                Some(e0) => Ok(Pin {
                    e0,
                    at: std::time::Instant::now(),
                    budget: std::time::Duration::from_micros(e0 - now - margin_us()),
                }),
                None => Err(no_longer_holds(keys)),
            }
        }
    }

    impl ChUnit<'_> {
        fn pred(&self) -> String {
            let m = owned_margin_secs();
            let mut p: Vec<String> = self.keys.iter().map(|k| owner_pred(k, &self.token, m)).collect();
            p.push(pinned_pred(self.pin.e0, margin_us()));
            p.join(" AND ")
        }

        /// Keep the pinned deadline ahead of the unit (`repin_due`), and move
        /// it only across no gap: when an owned statement was sent since the
        /// pin, the re-read must itself pass the old deadline. When none was,
        /// nothing written can have been fenced out, and the re-pin is fresh.
        ///
        /// The residual: one owned statement longer than the budget it started
        /// with fails the next re-pin although it may have run whole. Nothing
        /// finer is a proof — a mutation evaluates its predicate part by part,
        /// so a statement that STARTED inside the deadline may still have
        /// skipped its last parts — so the unit refuses and the window
        /// replays, loudly.
        async fn keep(&mut self, force: bool) -> Result<()> {
            if !repin_due(self.pin.at.elapsed(), self.pin.budget, force) {
                return Ok(());
            }
            let prev = self.owed.then_some(self.pin.e0);
            self.pin = self.s.pin(&self.keys, &self.token, prev).await?;
            self.owed = false;
            Ok(())
        }

        async fn exec_owned(&mut self, sql: &str) -> Result<String> {
            self.owed = true;
            let st = owned_settings();
            let st: Vec<(&str, &str)> = st.iter().map(|(k, v)| (*k, v.as_str())).collect();
            self.s.ch.exec_with(sql, &st).await
        }

        async fn written_owned(&mut self, sql: &str) -> Result<Option<u64>> {
            self.owed = true;
            let st = owned_settings();
            let st: Vec<(&str, &str)> = st.iter().map(|(k, v)| (*k, v.as_str())).collect();
            self.s.ch.exec_written(sql, &st).await
        }

        /// A read the server refuses to let write.
        pub(crate) async fn read(&mut self, sql: &str) -> Result<String> {
            self.s.note("read");
            self.s.ch.read(sql).await
        }

        pub(crate) async fn refuse_clustered(&self, dest_table: &str) -> Result<()> {
            self.s.refuse_clustered(dest_table).await
        }

        pub(crate) async fn columns(&self, table: &str) -> Result<Vec<(String, String)>> {
            self.s.columns(table).await
        }

        /// `body` (TabSeparated rows of `cols`) into `table`, through
        /// `input()` so the statement can carry the predicate: an evicted
        /// run's INSERT writes no row.
        pub(crate) async fn insert_owned(&mut self, table: &str, cols: &[String], body: Vec<u8>) -> Result<()> {
            self.s.note("insert");
            self.keep(false).await?;
            let s = structure(cols, &self.s.types(table).await?)?;
            let sql = insert_owned_sql(table, cols, &s, &self.pred());
            let st = owned_settings();
            let st: Vec<(&str, &str)> = st.iter().map(|(k, v)| (*k, v.as_str())).collect();
            self.owed = true;
            self.s.ch.insert_stream_with(&sql, reqwest::Body::from(body), &st).await
        }

        fn delete_mode(&self, table: &str) -> &'static str {
            if self.s.patched(table) {
                " SETTINGS lightweight_delete_mode='lightweight_update'"
            } else {
                ""
            }
        }

        pub(crate) async fn delete_owned(&mut self, table: &str, where_sql: &str) -> Result<()> {
            self.s.note("delete");
            self.keep(false).await?;
            let sql = format!(
                "DELETE FROM {} WHERE ({where_sql}) AND {}{}",
                ch_ident(table),
                self.pred(),
                self.delete_mode(table)
            );
            self.exec_owned(&sql).await.map(|_| ())
        }

        /// A WAL TRUNCATE. A `DELETE` rather than `TRUNCATE TABLE`, because
        /// only a statement with a WHERE can carry the predicate.
        pub(crate) async fn clear_owned(&mut self, table: &str) -> Result<()> {
            self.s.note("clear");
            self.keep(false).await?;
            let sql = format!("DELETE FROM {} WHERE {}{}", ch_ident(table), self.pred(), self.delete_mode(table));
            self.exec_owned(&sql).await.map(|_| ())
        }

        /// This run's key table for `table`, empty and shaped like its key.
        ///
        /// Tokenized, so two runs never share one, and a collected run's is
        /// swept by its collector. Built ONCE per run and truncated per window:
        /// DROP → CREATE per window spent three round trips of ceremony on a
        /// window that only has ~7. The first time DROPs before creating rather
        /// than relying on IF NOT EXISTS — `AS SELECT … WHERE 0` freezes the
        /// key's columns and types at creation.
        pub(crate) async fn key_table_reset(&mut self, table: &str, pk_cols: &[String]) -> Result<String> {
            self.s.note("key_reset");
            let kt = artifact_ident_tok(table, Artifact::CdcDelete, ROOMY, &self.token);
            let (kq, ft) = (ch_ident(&kt), ch_ident(table));
            if self.s.first_time(&format!("\u{1}kt\u{1}{kt}")) {
                let pklist = pk_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");
                self.s.ch.exec(&format!("DROP TABLE IF EXISTS {kq}")).await?;
                self.s
                    .ch
                    .exec(&format!("CREATE TABLE {kq} ENGINE = MergeTree ORDER BY tuple() AS SELECT {pklist} FROM {ft} WHERE 0"))
                    .await?;
                if self.s.patch_ok().await {
                    // Materializes block columns for parts written from here
                    // on; the probe showed pre-ALTER parts patch correctly too.
                    self.s
                        .ch
                        .exec(&format!(
                            "ALTER TABLE {ft} MODIFY SETTING enable_block_number_column=1, enable_block_offset_column=1"
                        ))
                        .await?;
                    self.s.first_time(&format!("\u{1}patch\u{1}{table}"));
                }
            } else {
                self.s.ch.exec(&format!("TRUNCATE TABLE {kq}")).await?;
            }
            self.s.forget_shape(&kt);
            Ok(kt)
        }

        /// The window start the last append ATTEMPT was made at, if any.
        pub(crate) async fn pending_window(&mut self, dest_table: &str, source_id: &str) -> Result<Option<u64>> {
            self.s.ensure_pending_table().await?;
            let body = self
                .read(&format!(
                    "SELECT toString(argMax(lsn, at)) FROM {PENDING} \
                     WHERE dest_table = '{}' AND source_id = '{}' FORMAT TabSeparatedRaw",
                    ch_str(dest_table),
                    ch_str(source_id),
                ))
                .await?;
            Ok(body.trim().parse::<u64>().ok())
        }

        /// How much of a window stamped `lsn` is already in the table, and
        /// whether what is there is an unbroken prefix `seq = 0..n-1`. Rows go
        /// out in `seq` order and ClickHouse commits the blocks it received, so
        /// the survivor of a torn INSERT is normally a prefix — but `count =
        /// max(seq) + 1` is the only thing that PROVES it.
        pub(crate) async fn appended_prefix(&mut self, dest_table: &str, lsn: u64) -> Result<Option<usize>> {
            let body = self
                .read(&format!(
                    // Baseline rows are excluded: the bootstrap stamps them with
                    // its consistent point, and the FIRST window after a
                    // bootstrap starts at exactly that point.
                    "SELECT count(), ifNull(max({CL_SEQ}), 0) FROM {} \
                     WHERE {CL_LSN} = {lsn} AND {CL_OP} != '{b}' FORMAT TabSeparated",
                    ch_ident(dest_table),
                    b = ch_str(CL_BASELINE),
                ))
                .await?;
            let mut f = body.trim().split('\t');
            let n: usize = f.next().unwrap_or("0").trim().parse().unwrap_or(0);
            let max_seq: usize = f.next().unwrap_or("0").trim().parse().unwrap_or(0);
            if n == 0 {
                return Ok(Some(0));
            }
            Ok(if n == max_seq + 1 { Some(n) } else { None })
        }

        /// The changelog's intent marker for the window at `lsn`, written only
        /// by an owner: exactly one row, or this run no longer holds the table.
        pub(crate) async fn mark_pending_owned(&mut self, dest_table: &str, source_id: &str, lsn: u64) -> Result<()> {
            self.s.note("mark_pending");
            self.s.ensure_pending_table().await?;
            self.keep(false).await?;
            let sql = format!(
                "INSERT INTO {PENDING} (dest_table, source_id, lsn) SELECT '{}', '{}', {lsn} WHERE {}",
                ch_str(dest_table),
                ch_str(source_id),
                self.pred()
            );
            match self.written_owned(&sql).await? {
                Some(1) => Ok(()),
                Some(_) => Err(no_longer_holds(&self.keys)),
                // A proxy stripped the summary header: ask the table.
                None => {
                    let body = self
                        .s
                        .ch
                        .read(&format!(
                            "SELECT toString(argMax(lsn, at)) FROM {PENDING} \
                             WHERE dest_table = '{}' AND source_id = '{}' FORMAT TabSeparatedRaw",
                            ch_str(dest_table),
                            ch_str(source_id),
                        ))
                        .await?;
                    if body.trim().parse::<u64>().ok() == Some(lsn) {
                        Ok(())
                    } else {
                        Err(no_longer_holds(&self.keys))
                    }
                }
            }
        }

        /// Rebuild `table` as `SELECT {sel} FROM table` with a new layout,
        /// swapped in atomically. Non-destructive until the swap, and the swap
        /// is taken only by an owner; a run evicted right after it leaves the
        /// old table under its run-scoped name for its collector to sweep.
        ///
        /// The CTAS writes only this run's temp, so it is neither fenced nor
        /// bounded, and it may take minutes on a large table. The check before
        /// the EXCHANGE is therefore a FRESH pin (nothing owned ran before it
        /// in this unit — see `keep`): a live claim passes however long the
        /// copy took, and a claim taken meanwhile still refuses the swap.
        pub(crate) async fn changelog_rebuild(&mut self, table: &str, sel: &str, part: &str, order: &str) -> Result<()> {
            self.s.note("rebuild");
            let tmp = artifact_ident_tok(table, Artifact::ChangelogTmp, ROOMY, &self.token);
            let (tq, ft) = (ch_ident(&tmp), ch_ident(table));
            self.s.ch.exec(&format!("DROP TABLE IF EXISTS {tq}")).await?;
            self.s
                .ch
                .exec(&format!(
                    // allow_nullable_key: a changelog's rows are partial by
                    // nature — a TRUNCATE record carries no row at all, so even
                    // the key columns are Nullable. Without this ClickHouse
                    // refuses the sorting key outright (ILLEGAL_COLUMN 44).
                    "CREATE TABLE {tq} ENGINE = MergeTree PARTITION BY {part} ORDER BY ({order}) \
                     SETTINGS allow_nullable_key = 1 AS SELECT {sel} FROM {ft}"
                ))
                .await?;
            self.keep(true).await?;
            self.exec_owned(&format!("EXCHANGE TABLES {tq} AND {ft}")).await?;
            self.s.forget_shape(table);
            if self.keep(true).await.is_err() {
                return Err(Error::Locked(format!(
                    "{}: this drain lost its claim right after rebuilding {table} as a changelog; \
                     the replaced table is kept as {tmp} for the run that collected it",
                    self.keys.join(", ")
                )));
            }
            self.s.ch.exec(&format!("DROP TABLE {tq}")).await.map(|_| ())
        }

        pub(crate) async fn current_view(&mut self, table: &str, pk_cols: &[String]) -> Result<()> {
            self.s.note("view");
            self.s.ch.exec(&current_view_sql(table, pk_cols)).await.map(|_| ())
        }

        /// The scratch names releases before 0.57.0 used, untokenized. Only a
        /// bootstrap drops them: while a table bootstraps the guard excludes
        /// every live drain, so nothing else can be using them.
        pub(crate) async fn drop_legacy_scratch(&mut self, table: &str) -> Result<()> {
            for a in [Artifact::CdcDelete, Artifact::ChangelogTmp] {
                self.s.ch.exec(&format!("DROP TABLE IF EXISTS {}", ch_ident(&artifact_ident(table, a, ROOMY)))).await?;
            }
            Ok(())
        }
    }

    impl LeaseStore for ChStore {
        fn lease_key(&self, dest_table: &str) -> String {
            format!("{}.{dest_table}", self.ch.database())
        }

        async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
            for k in keys {
                crate::sink::clickhouse::lease_write(&self.ch, k, token, ttl_secs() as i64, 0).await?;
            }
            Ok(())
        }

        async fn lease_renew(&self, keys: &[String], token: &str) -> Result<u64> {
            crate::sink::clickhouse::lease_renew(&self.ch, keys, token).await
        }

        async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
            crate::sink::clickhouse::lease_unclaimed(&self.ch, token).await
        }

        async fn close_run(&self, _token: &str) {}
    }

    impl Fence for ChStore {
        type Unit<'a> = ChUnit<'a>;

        fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
            (Box::new(self.ch_guard()), dest_table.to_string())
        }

        /// No transaction to open: the unit is its keys and its pinned
        /// deadline, read once here so an evicted run stops before its first
        /// statement rather than sending a window of statements that each
        /// write nothing.
        async fn open_unit<'a>(&'a self, keys: &[String], token: &str) -> Result<ChUnit<'a>> {
            let pin = self.pin(keys, token, None).await?;
            Ok(ChUnit { s: self, keys: keys.to_vec(), token: token.to_string(), pin, owed: false })
        }

        /// The watermark, written only by an owner: the state INSERT carries
        /// the predicate — the pinned deadline included — and must write
        /// exactly one row. A window whose statements an eviction or a lapse
        /// emptied therefore never moves the cursor.
        async fn close_unit<'a>(&'a self, mut u: ChUnit<'a>, _token: &str, marks: Vec<Watermark>) -> Result<()> {
            self.ensure_state_table().await?;
            for m in &marks {
                u.keep(false).await?;
                match m {
                    Watermark::Set { table, source_id, lsn, rows } => {
                        // Before the state table's first row for a fresh
                        // bootstrap: the table did not exist at run admission.
                        self.refuse_clustered(table).await?;
                        self.note("state");
                        let sql = format!(
                            "INSERT INTO `_apitap_state` \
                             (dest_table, source_id, cursor_col, watermark, mode, last_rows) \
                             SELECT '{dt}', '{sid}', '{STATE_CURSOR}', '{lsn}', 'log_based', {rows} WHERE {p}",
                            dt = ch_str(table),
                            sid = ch_str(source_id),
                            p = u.pred(),
                        );
                        match u.written_owned(&sql).await? {
                            Some(1) => {}
                            Some(_) => return Err(no_longer_holds(&u.keys)),
                            // A proxy stripped the summary header. A window is
                            // only sent when its end is past the watermark, so
                            // the value itself says whether this row landed.
                            None => {
                                let now = self
                                    .ch
                                    .read(&format!(
                                        "SELECT argMax(watermark, synced_at) FROM `_apitap_state` \
                                         WHERE dest_table = '{}' AND source_id = '{}' FORMAT TabSeparatedRaw",
                                        ch_str(table),
                                        ch_str(source_id),
                                    ))
                                    .await?;
                                if now.trim() != lsn.to_string() {
                                    return Err(no_longer_holds(&u.keys));
                                }
                            }
                        }
                    }
                    Watermark::Clear { table, source_id } => {
                        self.note("clear_state");
                        u.exec_owned(&format!(
                            "ALTER TABLE `_apitap_state` DELETE WHERE dest_table = '{}' \
                             AND source_id = '{}' AND {} SETTINGS mutations_sync = 1",
                            ch_str(table),
                            ch_str(source_id),
                            u.pred(),
                        ))
                        .await?;
                    }
                }
            }
            Ok(())
        }
    }
}

/// The changelog's `PARTITION BY` for ClickHouse.
///
/// A BARE COLUMN NAME means MONTHLY on that column — `"created_at"` becomes
/// `toYYYYMM(created_at)` — so the same `partition_by="created_at"` means the
/// same thing here as it does on BigQuery. Users should not have to remember
/// which engine wants which dialect, and the old literal reading of a bare name
/// was a footgun besides: `PARTITION BY created_at` on a DateTime column is ONE
/// PARTITION PER SECOND, which nobody wants and nothing warns about.
///
/// Anything that is not a plain identifier is passed through untouched, so the
/// full expression escape hatch (`toStartOfWeek(ts)`, `(toYYYYMM(ts), region)`)
/// still works for people who want it.
fn ch_partition_expr(spec: Option<&str>) -> String {
    let Some(spec) = spec.map(str::trim).filter(|s| !s.is_empty()) else {
        return format!("toYYYYMM({CL_AT})");
    };
    let bare = spec.trim_matches('`');
    let is_ident = !bare.is_empty()
        && !bare.starts_with(|c: char| c.is_ascii_digit())
        && bare.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if is_ident {
        format!("toYYYYMM({})", ch_ident(bare))
    } else {
        spec.to_string()
    }
}

/// The refusal itself, split from the probe so it can be unit-tested: given
/// the destination table's engine exactly as `system.tables` reports it, may
/// the CDC apply run against this table? `Replicated*` means a cluster, and
/// this file's sidecars are node-local (see [`ChDest::refuse_clustered`]).
/// An empty string — the table does not exist yet — passes. So does
/// ClickHouse Cloud's `Shared*` family: there the whole service shares one
/// catalog, so a node-local CREATE is visible from every node.
fn ch_engine_ok(dest_table: &str, engine: &str) -> Result<()> {
    if !engine.starts_with("Replicated") {
        return Ok(());
    }
    // The escape hatch exists because the refusal below is broader than the
    // defect. A Replicated destination reached through a BALANCER is genuinely
    // broken — the sidecars land wherever the balancer went. A Replicated
    // destination reached at a FIXED node is not: the data replicates through
    // the engine, the sidecars stay on that node, and every drain finds them
    // because every drain dials the same address. That configuration works
    // today, and refusing it outright would break a working pipeline to
    // protect it from a hazard it does not have.
    //
    // So: refuse by default, because a balancer is the common shape and silent
    // divergence is the worst outcome; and let an operator who knows their URL
    // names one node say so. Deliberately an env var and not a keyword
    // argument — it is a statement about the DEPLOYMENT, not about the
    // transfer, and the next person to write the same call in a different
    // place should not have to remember it.
    if std::env::var("APITAP_CH_CDC_ALLOW_REPLICATED").as_deref() == Ok("1") {
        crate::progress::note(&format!(
            "{dest_table} is {engine}; APITAP_CH_CDC_ALLOW_REPLICATED=1 — apitap's \
             own CDC objects stay node-local, so every run must reach the SAME \
             node or the watermark it reads will not be the one it wrote"
        ));
        return Ok(());
    }
    Err(Error::InvalidInput(format!(
        "log_based: ClickHouse destination {dest_table} is {engine} — a replicated \
         (cluster) table. The bootstrap's bulk load can ride on_cluster, but the \
         CDC apply cannot yet: the objects it creates for itself (the _apitap_state \
         watermark, the per-window __apitap_cdc_del key table, the changelog \
         rebuild and its __current view) are node-local, so on a cluster they \
         would exist only on the node the balancer routed — a later drain lands \
         where they are missing, or reads a stale watermark and silently skips a \
         window. Point log_based at a non-replicated destination table (bulk \
         modes keep full on_cluster support). If your URL names ONE node \
         rather than a balancer, apitap's node-local objects are consistent \
         there and you can set APITAP_CH_CDC_ALLOW_REPLICATED=1"
    )))
}

/// One verdict for "the destination's shape matches the mode", shared by both
/// analytical destinations so the two engines say the same thing.
pub(crate) fn is_shape_ok(
    is_changelog: bool,
    want_changelog: bool,
    engine: &str,
    dest_table: &str,
) -> Result<()> {
    match (is_changelog, want_changelog) {
        (true, true) | (false, false) => Ok(()),
        (true, false) => Err(Error::InvalidInput(format!(
            "log_based: {engine} target {dest_table} is a CHANGELOG (it has an \
             {CL_OP} column) but this run asked for a replica — pass changelog=True, \
             or point at a different dest_table"
        ))),
        (false, true) => Err(Error::InvalidInput(format!(
            "log_based: {engine} target {dest_table} is a REPLICA (no {CL_OP} column) \
             but this run asked for changelog=True — a changelog cannot be grafted \
             onto a replica's history. Use a different dest_table, or drop the table \
             and its _apitap_state row to re-bootstrap as a changelog"
        ))),
    }
}

/// The Nullable form of an existing column type for the changelog rebuild, or
/// `None` when the column must be left exactly as it is.
///
/// A changelog's rows are partial by nature — a delete carries only the key, a
/// truncate carries nothing — so every data column has to accept NULL. Three
/// cases the naive `Nullable({ty})` gets wrong:
/// * already nullable, in either spelling — wrapping twice is an error;
/// * `LowCardinality(T)` — ClickHouse spells it `LowCardinality(Nullable(T))`,
///   with the Nullable INSIDE; the other order is rejected;
/// * containers (Array/Map/Tuple/Nested) and aggregate states cannot be
///   Nullable at all. Left alone: the log still works for I/U/D, and a
///   TRUNCATE against such a table fails loudly instead of silently.
fn cl_nullable(ty: &str) -> Option<String> {
    let t = ty.trim();
    let mut inner = t;
    let mut lc = 0usize;
    while let Some(r) = inner.strip_prefix("LowCardinality(").and_then(|r| r.strip_suffix(')')) {
        inner = r.trim();
        lc += 1;
    }
    if inner.starts_with("Nullable(") {
        return None;
    }
    for c in ["Array(", "Map(", "Tuple(", "Nested(", "AggregateFunction(", "SimpleAggregateFunction("] {
        if inner.starts_with(c) {
            return None;
        }
    }
    let mut w = format!("Nullable({inner})");
    for _ in 0..lc {
        w = format!("LowCardinality({w})");
    }
    Some(w)
}

/// One changelog row's DATA columns, TabSeparated, WITHOUT the trailing newline
/// — the meta columns are appended after it. Padded to `ncols` with `\N` so a
/// delete's key-only old image still lines up with the table's column list.
fn render_ch_row_trim(
    row: &crate::wire::pgoutput::Tuple,
    oids: &[u32],
    ncols: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    for i in 0..ncols {
        if i > 0 {
            out.push(b'\t');
        }
        match row.get(i) {
            Some(crate::wire::pgoutput::Cellv::Text(t)) => render_ch_value(t, oids[i], out)?,
            // Missing (short old image) and NULL render the same: absent.
            Some(crate::wire::pgoutput::Cellv::Null) | None => out.extend_from_slice(b"\\N"),
            // An unchanged-TOAST cell in a changelog is honest information:
            // "this update did not carry that column". It lands NULL, and the
            // `U` record's presence tells the reader the row changed.
            Some(crate::wire::pgoutput::Cellv::UnchangedToast) => out.extend_from_slice(b"\\N"),
        }
    }
    Ok(())
}

/// `col = lit AND …` for one replica-identity key, typed by OID.
fn key_pred(pk_cols: &[String], key: &[Vec<u8>], pk_oids: &[u32]) -> Result<String> {
    Ok(pk_cols
        .iter()
        .zip(key.iter())
        .zip(pk_oids.iter())
        .map(|((c, v), &oid)| Ok(format!("{} = {}", ch_ident(c), ch_key_literal(v, oid)?)))
        .collect::<Result<Vec<_>>>()?
        .join(" AND "))
}

/// Render a residue row where the cells at `verbatim` indices came back from
/// the destination readback (already destination-dialect — escape only, no
/// OID translation); everything else is WAL text and translates as usual.
fn render_residue_row(
    row: &[Cell],
    oids: &[u32],
    verbatim: &[usize],
    out: &mut Vec<u8>,
) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) if verbatim.contains(&i) => {
                crate::logbased::rowtext::copy_escape(t, out)
            }
            Cell::Text(t) => render_ch_value(t, oids[i], out)?,
            Cell::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell survived the readback — bug".into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ch_engine_ok, ch_partition_expr, cl_nullable};
    use super::store::{insert_owned_sql, owner_pred, pinned_pred, repin, structure};
    use super::*;
    use crate::lease::{owned_margin_secs, ttl_secs, Fence, LeaseStore};
    use crate::logbased::window::Layout;
    use crate::wire::pgoutput::Tuple;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn owner_pred_requires_a_row_and_margin() {
        let p = owner_pred("default.t", "_tok", 150);
        assert!(p.contains("count() > 0"), "{p}");
        assert!(p.contains("argMax(collected, seq) = 0"), "{p}");
        assert!(p.contains("argMax(expires_at, seq) > now64(6) + INTERVAL 150 SECOND"), "{p}");
        assert!(p.contains("dest_key = 'default.t' AND token = '_tok'"), "{p}");
    }

    #[test]
    fn input_structure_escapes_quotes() {
        let cols = vec!["ts".to_string(), "e".to_string()];
        let types: HashMap<String, String> = [
            ("ts".to_string(), "DateTime64(6, 'UTC')".to_string()),
            ("e".to_string(), "Enum8('a' = 1)".to_string()),
        ]
        .into_iter()
        .collect();
        let sql = insert_owned_sql("t", &cols, &structure(&cols, &types).unwrap(), "1");
        assert!(sql.contains("\\'UTC\\'"), "{sql}");
        assert!(sql.contains("\\'a\\'"), "{sql}");
        assert!(sql.starts_with("INSERT INTO `t` (`ts`, `e`) SELECT `ts`, `e` FROM input('"), "{sql}");
        assert!(structure(&["nope".to_string()], &types).is_err());
    }

    type Answer = Arc<dyn Fn(&str) -> (String, u64) + Send + Sync>;

    /// A ClickHouse stand-in on a local port. `answer` sees each request's SQL
    /// — the POST body, or the `query` URL parameter of an INSERT whose body
    /// is its rows — and returns the response body and the `written_rows` the
    /// summary header reports.
    async fn mock_with(answer: Answer) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else { return };
                let answer = answer.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut tmp = [0u8; 8192];
                    loop {
                        let end = loop {
                            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break p + 4;
                            }
                            match s.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                            }
                        };
                        let head_raw = String::from_utf8_lossy(&buf[..end]).to_string();
                        let head = head_raw.to_ascii_lowercase();
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        while buf.len() < end + len {
                            match s.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                            }
                        }
                        let body = String::from_utf8_lossy(&buf[end..end + len]).to_string();
                        buf.drain(..end + len);
                        let path = head_raw.split_whitespace().nth(1).unwrap_or("/").to_string();
                        let url_query = reqwest::Url::parse(&format!("http://x{path}"))
                            .ok()
                            .and_then(|u| u.query_pairs().find(|(k, _)| k == "query").map(|(_, v)| v.into_owned()))
                            .unwrap_or_default();
                        let sql = if url_query.is_empty() { body } else { url_query };
                        let (out, written) = answer(&sql);
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\
                             X-ClickHouse-Summary: {{\"written_rows\":\"{written}\"}}\r\n\r\n{out}",
                            out.len()
                        );
                        if s.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("clickhouse://default:x@127.0.0.1:{port}/default")
    }

    const T0: u64 = 1_000_000_000_000_000;

    /// Answers the reads a changelog window makes, reports one written row
    /// for every write, and a lease with a whole TTL of life.
    async fn mock_ch() -> String {
        mock_with(Arc::new(|sql: &str| {
            let out = if sql.contains("toUnixTimestamp64Micro(min(e))") {
                format!("{T0}\t{}\t1\n", T0 + ttl_secs() * 1_000_000)
            } else if sql.contains("SELECT engine FROM system.tables") {
                "MergeTree\n".to_string()
            } else if sql.contains("SELECT name, type FROM system.columns") {
                "id\tInt32\nv\tNullable(String)\n_apitap_op\tString\n_apitap_lsn\tUInt64\n\
                 _apitap_seq\tUInt32\n_apitap_at\tDateTime64(3)\n"
                    .to_string()
            } else {
                String::new()
            };
            (out, 1)
        }))
        .await
    }

    /// A changelog window: read (is a previous attempt pending?), mark the
    /// attempt, insert, and only then the watermark. The mark before the
    /// insert is what lets a replay count what already landed.
    #[test]
    fn unit_order_mark_insert_state() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let s = ChStore::connect(&mock_ch().await).unwrap();
            let mut ch = Changes::new(Layout::for_test(&["id", "v"], &[23, 25], &["id"]));
            ch.insert(Tuple::from_cells(&[
                Cell::Text(bytes::Bytes::from_static(b"1")),
                Cell::Text(bytes::Bytes::from_static(b"a")),
            ]));
            let w = ch.seal("public.t").unwrap();
            let keys = vec![s.lease_key("t")];
            let mut u = s.open_unit(&keys, "_tok").await.unwrap();
            let (n, mark) =
                apply_changelog_unit(&mut u, "t", Some(&w), &WindowId::new(10, 20), "sid").await.unwrap();
            assert_eq!(n, 1);
            s.close_unit(u, "_tok", vec![mark]).await.unwrap();
            let _ = Arc::new(Mutex::new(()));
            assert_eq!(*s.ops.lock().unwrap(), ["read", "mark_pending", "insert", "state"]);
        });
    }

    /// A re-pin extends the deadline only across no gap: a read that itself
    /// fails the OLD deadline cannot prove the statements before it passed,
    /// however much life the renewed row has now.
    #[test]
    fn repin_extends_only_a_continuous_deadline() {
        let m = 150_000_000;
        let (e0, far) = (T0 + 300_000_000, T0 + 900_000_000);
        // Open: owned with more than the margin; not owned; too little life.
        assert_eq!(repin(T0, e0, 1, 1, m, None), Some(e0));
        assert_eq!(repin(T0, e0, 0, 1, m, None), None);
        assert_eq!(repin(T0 + 200_000_000, e0, 1, 1, m, None), None);
        // Inside the old deadline, a renewed row moves it forward.
        assert_eq!(repin(T0 + 100_000_000, far, 1, 1, m, Some(e0)), Some(far));
        // Past it, the renewal does not help: something may have been skipped.
        assert_eq!(repin(T0 + 200_000_000, far, 1, 1, m, Some(e0)), None);
        // One key of two claimed.
        assert_eq!(repin(T0, far, 1, 2, m, Some(e0)), None);
        assert!(pinned_pred(e0, m).contains(&format!("+ {m} < {e0}")));
    }

    /// The fence closes for good inside a unit. A statement sent while the
    /// lease has less than the margin left writes nothing — and says nothing,
    /// an INSERT whose WHERE is false succeeds — and a renewal then restores
    /// the margin. The watermark INSERT must still write no row: against the
    /// live margin alone it would, recording a window whose rows never landed.
    #[test]
    fn a_fenced_statement_fails_the_close() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        fn num_after(sql: &str, pat: &str) -> Option<u64> {
            let at = sql.find(pat)? + pat.len();
            let rest = &sql[at..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            rest[..end].parse().ok()
        }
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Arc::new(AtomicU64::new(T0));
            let live = Arc::new(AtomicU64::new(T0 + ttl_secs() * 1_000_000));
            let written = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
            let (n2, l2, w2) = (now.clone(), live.clone(), written.clone());
            let url = mock_with(Arc::new(move |sql: &str| {
                let (now, live) = (n2.load(SeqCst), l2.load(SeqCst));
                if sql.contains("toUnixTimestamp64Micro(min(e))") {
                    return (format!("{now}\t{live}\t1\n"), 0);
                }
                if sql.contains("SELECT name, type FROM system.columns") {
                    return ("id\tInt32\nv\tNullable(String)\n".into(), 0);
                }
                if sql.contains("SELECT engine FROM system.tables") {
                    return ("MergeTree\n".into(), 0);
                }
                if !sql.contains("argMax(collected, seq) = 0") {
                    return (String::new(), 0);
                }
                // The fence as the server evaluates it, at `now`: the live
                // margin, and the pinned deadline where the statement has one.
                let live_ok = num_after(sql, "INTERVAL ").map_or(true, |m| live > now + m * 1_000_000);
                let pinned_ok = match (
                    num_after(sql, "toUnixTimestamp64Micro(now64(6)) + "),
                    sql.find("toUnixTimestamp64Micro(now64(6)) + ").and_then(|i| num_after(&sql[i..], " < ")),
                ) {
                    (Some(x), Some(e0)) => now + x < e0,
                    _ => true,
                };
                let w = u64::from(live_ok && pinned_ok);
                w2.lock().unwrap().push((sql.split_whitespace().take(3).collect::<Vec<_>>().join(" "), w));
                (String::new(), w)
            }))
            .await;
            let s = ChStore::connect(&url).unwrap();
            let keys = vec![s.lease_key("t")];
            let mut u = s.open_unit(&keys, "_tok").await.unwrap();
            // Stopped for longer than the margin allows, short of the TTL: no
            // claim is possible, but this statement is fenced out.
            now.store(T0 + (ttl_secs() - owned_margin_secs() / 2) * 1_000_000, SeqCst);
            u.insert_owned("t", &["id".to_string(), "v".to_string()], b"1\ta\n".to_vec()).await.unwrap();
            // The keeper's tick lands on resume: a whole TTL again.
            live.store(now.load(SeqCst) + ttl_secs() * 1_000_000, SeqCst);
            let mark = Watermark::Set { table: "t".into(), source_id: "sid".into(), lsn: 20, rows: 1 };
            let r = s.close_unit(u, "_tok", vec![mark]).await;
            let w = written.lock().unwrap().clone();
            assert_eq!(w.first().map(|x| x.1), Some(0), "the insert was not fenced out: {w:?}");
            assert!(matches!(r, Err(Error::Locked(_))), "a window whose insert wrote nothing was recorded: {r:?} {w:?}");
        });
    }

    /// The changelog rebuild's CTAS copies the whole table into this run's
    /// temp, unfenced and unbounded. A copy longer than the pin's budget —
    /// three minutes here, against 150 s at TTL 300 — while the keeper renews
    /// the lease must still swap: nothing owned ran before it, so nothing can
    /// have been fenced out. A claim taken meanwhile must still refuse it.
    #[test]
    fn a_slow_rebuild_keeps_a_live_claim() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Arc::new(AtomicU64::new(T0));
            let live = Arc::new(AtomicU64::new(T0 + ttl_secs() * 1_000_000));
            let (claimed, claim_in_copy) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
            let sent = Arc::new(Mutex::new(Vec::<String>::new()));
            let (n2, l2, c2, cc2, s2) = (now.clone(), live.clone(), claimed.clone(), claim_in_copy.clone(), sent.clone());
            let url = mock_with(Arc::new(move |sql: &str| {
                if sql.contains("toUnixTimestamp64Micro(min(e))") {
                    let owned = u64::from(!c2.load(SeqCst));
                    return (format!("{}\t{}\t{owned}\n", n2.load(SeqCst), l2.load(SeqCst)), 0);
                }
                s2.lock().unwrap().push(sql.split_whitespace().take(2).collect::<Vec<_>>().join(" "));
                if sql.starts_with("CREATE TABLE") && sql.contains(" AS SELECT ") {
                    // The copy takes three minutes; the keeper renews under
                    // it — unless a collector claims the row meanwhile.
                    let t = n2.fetch_add(180_000_000, SeqCst) + 180_000_000;
                    l2.store(t + ttl_secs() * 1_000_000, SeqCst);
                    c2.store(cc2.load(SeqCst), SeqCst);
                }
                (String::new(), 0)
            }))
            .await;
            let s = ChStore::connect(&url).unwrap();
            let keys = vec![s.lease_key("t")];

            let mut u = s.open_unit(&keys, "_tok").await.unwrap();
            let r = u.changelog_rebuild("t", "`id`", "tuple()", "`id`").await;
            let log = sent.lock().unwrap().clone();
            assert!(r.is_ok(), "a live claim failed after a slow copy: {r:?} {log:?}");
            assert!(log.iter().any(|l| l == "EXCHANGE TABLES"), "{log:?}");

            // The same copy, with the claim collected while it ran: no swap.
            sent.lock().unwrap().clear();
            let mut u = s.open_unit(&keys, "_tok").await.unwrap();
            claim_in_copy.store(true, SeqCst);
            let r = u.changelog_rebuild("t", "`id`", "tuple()", "`id`").await;
            let log = sent.lock().unwrap().clone();
            assert!(matches!(r, Err(Error::Locked(_))), "a collected claim swapped: {r:?} {log:?}");
            assert!(!log.iter().any(|l| l == "EXCHANGE TABLES"), "{log:?}");
        });
    }

    /// A unit re-reads its pin early — an eighth of the budget — so an owned
    /// statement starts with most of the budget ahead of it; short windows
    /// never re-read at all.
    #[test]
    fn a_pin_is_renewed_early() {
        use super::store::repin_due;
        use std::time::Duration;
        let b = Duration::from_secs(120);
        assert!(!repin_due(Duration::from_secs(1), b, false), "a short window re-read its pin");
        assert!(repin_due(Duration::from_secs(1), b, true));
        assert!(repin_due(Duration::from_secs(15), b, false));
        assert!(repin_due(Duration::from_secs(40), b, false), "a statement could start with a third of the budget gone");
    }

    #[test]
    fn a_replicated_destination_is_refused_not_silently_diverged() {
        // The bulk sink only grants on_cluster to Replicated* engines, so a
        // Replicated destination IS the cluster case — and the apply's
        // node-local sidecars may not silently land on one node of it.
        assert!(ch_engine_ok("orders", "ReplicatedMergeTree").is_err());
        assert!(ch_engine_ok("orders", "ReplicatedReplacingMergeTree").is_err());
        // The near-miss that a sloppier prefix would also refuse: Replacing
        // is not Replicated.
        assert!(ch_engine_ok("orders", "ReplacingMergeTree").is_ok());
        assert!(ch_engine_ok("orders", "MergeTree").is_ok());
        // ClickHouse Cloud shares ONE catalog across the whole service, so a
        // node-local CREATE is visible everywhere — Shared* stays allowed.
        assert!(ch_engine_ok("orders", "SharedMergeTree").is_ok());
        // No table yet: nothing to diverge from, the bootstrap decides.
        assert!(ch_engine_ok("orders", "").is_ok());
    }

    #[test]
    fn a_bare_column_name_means_monthly_on_clickhouse_too() {
        // The whole point: this is the spelling BigQuery already takes, and it
        // must mean the same thing here — a MONTH, never one partition per
        // timestamp, which is what the literal reading used to produce.
        assert_eq!(ch_partition_expr(Some("created_at")), "toYYYYMM(`created_at`)");
        assert_eq!(ch_partition_expr(Some("  occurred_at  ")), "toYYYYMM(`occurred_at`)");
        assert_eq!(ch_partition_expr(Some("`logged_at`")), "toYYYYMM(`logged_at`)");
        assert_eq!(ch_partition_expr(Some("_apitap_at")), "toYYYYMM(`_apitap_at`)");
        // Nothing given, or blank: the monthly default.
        assert_eq!(ch_partition_expr(None), "toYYYYMM(_apitap_at)");
        assert_eq!(ch_partition_expr(Some("   ")), "toYYYYMM(_apitap_at)");
        // Anything that is not a plain identifier stays the caller's own
        // expression — the escape hatch survives.
        assert_eq!(ch_partition_expr(Some("toYYYYMM(ts)")), "toYYYYMM(ts)");
        assert_eq!(ch_partition_expr(Some("toStartOfWeek(ts)")), "toStartOfWeek(ts)");
        assert_eq!(ch_partition_expr(Some("(toYYYYMM(ts), region)")), "(toYYYYMM(ts), region)");
        assert_eq!(ch_partition_expr(Some("tuple()")), "tuple()");
    }

    #[test]
    fn nullable_wrap_handles_every_shape() {
        // The bug this file shipped with: a tz-aware timestamp column.
        assert_eq!(
            cl_nullable("DateTime64(6, 'UTC')").as_deref(),
            Some("Nullable(DateTime64(6, 'UTC'))")
        );
        assert_eq!(cl_nullable("Int64").as_deref(), Some("Nullable(Int64)"));
        assert_eq!(
            cl_nullable("Decimal(12, 2)").as_deref(),
            Some("Nullable(Decimal(12, 2))")
        );
        // Nullable INSIDE LowCardinality — the other order is rejected by CH.
        assert_eq!(
            cl_nullable("LowCardinality(String)").as_deref(),
            Some("LowCardinality(Nullable(String))")
        );
        // Already nullable, in either spelling: leave it.
        assert_eq!(cl_nullable("Nullable(String)"), None);
        assert_eq!(cl_nullable("LowCardinality(Nullable(String))"), None);
        // Containers cannot be Nullable at all.
        assert_eq!(cl_nullable("Array(String)"), None);
        assert_eq!(cl_nullable("Map(String, UInt64)"), None);
        assert_eq!(cl_nullable("Tuple(UInt8, String)"), None);
    }
}
