//! log_based apply into ClickHouse.
//!
//! ClickHouse has no multi-statement transactions, so the WINDOW is the
//! atom instead: the apply order (truncate → clear keys → plain insert →
//! residue → state row LAST) makes replaying the same window idempotent —
//! the delete phase clears every key the insert phase lands, so a crash
//! between insert and state write converges on the re-run, exactly like the
//! slot re-drain does. Deletes are lightweight `DELETE FROM` joined against
//! a key table (synchronous on the issuing replica by default).

use crate::error::{Error, Result};
use crate::logbased::collapse::ResidueOp;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::rowtext::{
    ch_key_literal, pk_indices, render_ch_key, render_ch_row, render_ch_row_cells,
    render_ch_value, row_key_refs, row_key_refs_cells, tsv_unescape,
};
use crate::sink::clickhouse::{ch_ident, ch_str, ChConn};
use crate::wire::pgoutput::Cell;

const STATE_CURSOR: &str = "_lsn";

// ── changelog mode (`changelog=true`) ───────────────────────────────────────
// The destination stops being a replica and becomes an append-only audit trail:
// every captured operation is INSERTed with the meta columns below, nothing is
// ever updated or deleted, and `<table>__current` derives the current state.
// ClickHouse is built for exactly this shape — no mutations, no part rewrites.
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
    ch: ChConn,
    /// DDL this connection has already issued. `CREATE TABLE IF NOT EXISTS` is
    /// idempotent but not free: it is a full HTTP round trip against a window
    /// that only has ~7 of them, repeated for every window of every table. The
    /// first window creates; the rest remember. A dropped-out-from-under-us
    /// table would resurface as a loud error on the next statement, which is
    /// the same failure the unconditional CREATE would have hidden.
    ensured: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Once-per-run verdict of `patch_ok` (None = not probed yet).
    patch: std::sync::Mutex<Option<bool>>,
}

impl ChDest {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            ch: ChConn::parse(url)?,
            ensured: std::sync::Mutex::new(std::collections::HashSet::new()),
            patch: std::sync::Mutex::new(None),
        })
    }

    /// True the FIRST time this connection is asked about `key`.
    fn first_time(&self, key: &str) -> bool {
        self.ensured.lock().unwrap().insert(key.to_string())
    }

    /// Patch-part deletes (`lightweight_delete_mode='lightweight_update'`)
    /// turn the per-window DELETE from a part REWRITE into a patch-part
    /// write. Probed once per run: server >= 25.7 required (the setting does
    /// not exist below), and correctness of OUR predicate shape — including
    /// parts born before the ALTER — was verified against 25.8.29 (see the
    /// r3 ledger; the #87265 shape returns exact counts and MutatePart=0).
    /// 24.8 LTS destinations keep today's rewrite path untouched.
    /// `APITAP_PATCH_DELETE=0` is the kill switch (and the A/B lever).
    async fn patch_ok(&self) -> bool {
        if std::env::var("APITAP_PATCH_DELETE").as_deref() == Ok("0") {
            return false;
        }
        {
            let g = self.patch.lock().unwrap();
            if let Some(v) = *g {
                return v;
            }
        }
        let ok = match self.ch.exec("SELECT version()").await {
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

    /// Default the created table's ORDER BY to the PK so the per-window
    /// key-join delete probes the sorting key instead of scanning.
    pub(crate) fn tweak_bootstrap_opts(
        &self,
        o2: &mut crate::TransferOptions,
        pk_cols: &[String],
    ) {
        if o2.order_by.is_none() {
            o2.order_by = Some(pk_cols.join(", "));
        }
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

    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<u64>> {
        let sql = format!(
            "SELECT watermark, cursor_col, mode FROM `_apitap_state` FINAL \
             WHERE dest_table = '{}' AND source_id = '{}' \
             FORMAT TabSeparated",
            ch_str(dest_table),
            ch_str(source_id)
        );
        let body = match self.ch.exec(&sql).await {
            Ok(b) => b,
            // No state table at all = fresh destination.
            Err(Error::Transfer(m))
                if m.contains("UNKNOWN_TABLE") || m.contains("doesn't exist") =>
            {
                return Ok(None)
            }
            Err(e) => return Err(e),
        };
        let Some(line) = body.lines().next() else { return Ok(None) };
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() != 3 {
            return Err(Error::Transfer(format!(
                "log_based: malformed state row from ClickHouse: {line:?}"
            )));
        }
        if f[2] != "log_based" || f[1] != STATE_CURSOR {
            return Err(Error::InvalidInput(format!(
                "log_based: state row for this table tracks cursor '{}' in mode \
                 '{}', not an LSN — it was written by another mode. Use a \
                 different dest_table or delete the state row",
                f[1], f[2]
            )));
        }
        f[0].parse::<u64>()
            .map(Some)
            .map_err(|_| Error::Transfer(format!("log_based: bad LSN state '{}'", f[0])))
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
        let ft = ch_ident(dest_table);
        // Already a changelog (a re-bootstrap of a table we own)? Leave it.
        //
        // "Already" means ALL FOUR meta columns, never just `_apitap_op`: a
        // source table that legitimately owns a column by that name would
        // otherwise skip the rebuild here and then fail on every window
        // forever, with the slot pinning WAL the whole time.
        let has = self
            .ch
            .exec(&format!(
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
            "4" => return self.write_state(dest_table, source_id, lsn, rows).await,
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
        let desc = self
            .ch
            .exec(&format!(
                // TabSeparatedRaw, not TabSeparated: TSV escapes single quotes,
                // so a `DateTime64(6, 'UTC')` column comes back as
                // `DateTime64(6, \'UTC\')` and lands verbatim inside the CAST
                // below — a syntax error on every table with a tz-aware column.
                "SELECT name, type FROM system.columns WHERE database = currentDatabase() \
                 AND table = '{t}' ORDER BY position FORMAT TabSeparatedRaw",
                t = ch_str(dest_table),
            ))
            .await?;
        let mut cols: Vec<(String, String)> = Vec::new();
        for line in desc.lines().filter(|l| !l.is_empty()) {
            let mut it = line.splitn(2, '\t');
            let (Some(n), Some(ty)) = (it.next(), it.next()) else { continue };
            cols.push((n.to_string(), ty.to_string()));
        }
        if cols.is_empty() {
            return Err(Error::Transfer(format!(
                "log_based changelog: ClickHouse table {dest_table} has no columns — \
                 the bootstrap must run first"
            )));
        }

        let part = partition_by.map(str::to_string).unwrap_or_else(|| format!("toYYYYMM({CL_AT})"));
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
        let tmp = ch_ident(&format!("{dest_table}__apitap_cl"));
        self.ch.exec(&format!("DROP TABLE IF EXISTS {tmp}")).await?;
        self.ch
            .exec(&format!(
                // allow_nullable_key: a changelog's rows are partial by nature —
                // a TRUNCATE record carries no row at all, so even the key
                // columns are Nullable. Without this ClickHouse refuses the
                // sorting key outright (ILLEGAL_COLUMN 44).
                "CREATE TABLE {tmp} ENGINE = MergeTree PARTITION BY {part} ORDER BY ({order}) \
                 SETTINGS allow_nullable_key = 1 AS \
                 SELECT {sel}, \
                 CAST('{op}' AS String) AS {CL_OP}, \
                 CAST({lsn} AS UInt64) AS {CL_LSN}, \
                 CAST(0 AS UInt32) AS {CL_SEQ}, \
                 now64(3) AS {CL_AT} \
                 FROM {ft}",
                op = ch_str(CL_BASELINE),
            ))
            .await?;
        self.ch.exec(&format!("DROP TABLE {ft}")).await?;
        self.ch
            .exec(&format!("RENAME TABLE {tmp} TO {}", ch_ident(dest_table)))
            .await?;
        self.ensure_current_view(dest_table, pk_cols).await?;
        self.write_state(dest_table, source_id, lsn, rows).await
    }

    /// `<table>__current`: the current state derived from the log.
    ///
    /// Three things it has to get right, in this order:
    /// 1. **TRUNCATE.** A `T` record means everything logged before it is gone,
    ///    so the view first drops every row at or below the newest `T`.
    /// 2. **Latest version per key.** Ordering is the PAIR `(lsn, seq)`, never
    ///    `lsn` alone: one window stamps its end-LSN on every row it lands, so
    ///    `seq` is what orders events inside a window.
    /// 3. **Deletes.** A key whose newest record is `D` is gone — filtered AFTER
    ///    the pick, not before, or the delete would be skipped and the previous
    ///    version would resurrect.
    ///
    /// Baseline (`B`) rows carry the slot's consistent-point LSN, so any later
    /// change outranks them.
    async fn ensure_current_view(&self, dest_table: &str, pk_cols: &[String]) -> Result<()> {
        let keys = pk_cols.iter().map(|c| ch_ident(c)).collect::<Vec<_>>().join(", ");
        let view = ch_ident(&format!("{dest_table}__current"));
        let t = ch_ident(dest_table);
        self.ch
            .exec(&format!(
                "CREATE OR REPLACE VIEW {view} AS SELECT * FROM ( \
                   SELECT * FROM {t} \
                   WHERE ({CL_LSN}, {CL_SEQ}) > ( \
                     SELECT ifNull(max(({CL_LSN}, {CL_SEQ})), (toUInt64(0), toUInt32(0))) \
                     FROM {t} WHERE {CL_OP} = '{tr}' \
                   ) \
                   ORDER BY {CL_LSN} DESC, {CL_SEQ} DESC \
                   LIMIT 1 BY {keys} \
                 ) WHERE {CL_OP} != '{del}'",
                tr = ch_str("T"),
                del = ch_str("D"),
            ))
            .await?;
        Ok(())
    }

    /// The destination's SHAPE must match the mode, checked ONCE at run start
    /// on a table that already has state (a fresh bootstrap builds the right
    /// shape by construction).
    ///
    /// Both directions are damage: a replica window landing on a changelog
    /// deletes the window's keys and inserts current images ON TOP of the
    /// history, destroying the log it found; a changelog window landing on a
    /// replica has nowhere to put its meta columns. Neither can be caught in
    /// the apply path — an empty drain never calls apply at all, and by the
    /// time a non-empty one does, the run has already committed to the mode.
    pub(crate) async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
        let has = self
            .ch
            .exec(&format!(
                "SELECT count() FROM system.columns WHERE database = currentDatabase() \
                 AND table = '{t}' AND name = '{c}'",
                t = ch_str(dest_table),
                c = ch_str(CL_OP),
            ))
            .await?;
        is_shape_ok(has.trim() != "0", changelog, "ClickHouse", dest_table)
    }

    pub(crate) async fn write_state(
        &self,
        dest_table: &str,
        source_id: &str,
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        self.ensure_state_table().await?;
        self.ch
            .exec(&format!(
                "INSERT INTO `_apitap_state` \
                 (dest_table, source_id, cursor_col, watermark, mode, last_rows) \
                 VALUES ('{}', '{}', '{STATE_CURSOR}', '{lsn}', 'log_based', {rows})",
                ch_str(dest_table),
                ch_str(source_id),
            ))
            .await?;
        Ok(())
    }

    /// Apply one collapsed window. State is written LAST — a re-run of the
    /// same window is idempotent (see module docs).
    /// changelog=true apply: ONE plain INSERT of every captured operation.
    ///
    /// No delete-set, no key table, no DELETE, no TRUNCATE — ClickHouse never
    /// writes a mutation, so the destination never rewrites parts. Replay is
    /// safe because a re-drained window re-appends rows carrying the SAME
    /// `(lsn, seq)`, and `__current` picks one of them; the duplicate is inert.
    /// One readback per window: the current value of every masked column, for
    /// every key that needs one, from `<table>__current`. The view filters the
    /// base table by key first, so this probes the sorting key rather than
    /// scanning the log.
    async fn read_current(
        &self,
        dest_table: &str,
        pk_cols: &[String],
        pk_oids: &[u32],
        keys: &[crate::logbased::changelog::CKey],
        cols: &[usize],
        wal_cols: &[String],
    ) -> Result<std::collections::HashMap<crate::logbased::changelog::CKey, Vec<Option<bytes::Bytes>>>>
    {
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
        let body = self
            .ch
            .exec(&format!(
                "SELECT {sel} FROM {view} WHERE {} FORMAT TabSeparated",
                preds.join(" OR ")
            ))
            .await?;
        let np = pk_cols.len();
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        for line in body.lines().filter(|l| !l.is_empty()) {
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() != np + cols.len() {
                return Err(Error::Transfer(
                    "log_based changelog: masked readback column count mismatch".into(),
                ));
            }
            let key: crate::logbased::changelog::CKey = f[..np]
                .iter()
                .map(|x| tsv_unescape(x).unwrap_or_default())
                .collect();
            let vals = f[np..]
                .iter()
                .map(|x| tsv_unescape(x).map(bytes::Bytes::from))
                .collect();
            out.insert(key, vals);
        }
        Ok(out)
    }

    pub(crate) async fn apply_changelog(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let Some(c) = outcome.changes.get(qualified_src) else {
            self.write_state(dest_table, source_id, outcome.end_lsn, 0).await?;
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
        for name in wal_cols {
            if matches!(name.as_str(), CL_OP | CL_LSN | CL_SEQ | CL_AT) {
                return Err(Error::InvalidInput(format!(
                    "log_based changelog: source column '{name}' collides with a reserved \
                     changelog column — rename it at the source or alias it in a view"
                )));
            }
        }
        if c.events.is_empty() {
            self.write_state(dest_table, source_id, outcome.end_lsn, 0).await?;
            return Ok(0);
        }

        // Unchanged-TOAST cells must be rebuilt before anything is written —
        // writing them as NULL would silently blank the column for every reader
        // of `__current`. Costs one extra query per window, and only when the
        // window actually carries a masked cell.
        let patched = if c.masked {
            let pk_idx = pk_indices(pk_cols, wal_cols)?;
            let (keys, cols) = c.mask_plan(&pk_idx);
            let base = if keys.is_empty() || cols.is_empty() {
                std::collections::HashMap::new()
            } else {
                let pk_oids: Vec<u32> = pk_idx.iter().map(|&i| oids[i]).collect();
                self.read_current(dest_table, pk_cols, &pk_oids, &keys, &cols, wal_cols).await?
            };
            c.resolve_masked(&pk_idx, &cols, &base, wal_cols)?
        } else {
            std::collections::HashMap::new()
        };

        let ft = ch_ident(dest_table);
        let collist = wal_cols
            .iter()
            .map(|k| ch_ident(k))
            .chain([CL_OP.to_string(), CL_LSN.to_string(), CL_SEQ.to_string(), CL_AT.to_string()])
            .collect::<Vec<_>>()
            .join(", ");
        let lsn = outcome.end_lsn;
        // ONE stamp for the window. It is the PARTITION/retention key, never an
        // ordering key — `(lsn, seq)` orders. Sent explicitly rather than left
        // to a default: the rebuild materialised `_apitap_at` as a plain column,
        // so a NULL would land as the epoch and pile the whole log into a 1970
        // partition.
        let at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let mut buf = Vec::with_capacity(4 << 20);
        for (seq, ev) in c.events.iter().enumerate() {
            match patched.get(&seq).or(ev.row.as_ref()) {
                Some(row) => {
                    // A delete's old image carries the key and NULLs elsewhere —
                    // that IS the delete record, so it renders like any row.
                    render_ch_row_trim(row, oids, wal_cols.len(), &mut buf)?;
                }
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
        self.ch
            .insert_stream(
                &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                reqwest::Body::from(buf),
            )
            .await?;
        self.write_state(dest_table, source_id, outcome.end_lsn, c.count).await?;
        Ok(c.count)
    }

    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            self.write_state(dest_table, source_id, outcome.end_lsn, 0).await?;
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
        let ft = ch_ident(dest_table);
        let pk_idx = pk_indices(pk_cols, wal_cols)?;
        let pk_oids: Vec<u32> = pk_idx.iter().map(|&i| oids[i]).collect();
        let pklist = pk_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");
        let collist = wal_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");

        if c.truncate {
            self.ch.exec(&format!("TRUNCATE TABLE {ft}")).await?;
        }

        // Clear the delete-set ∪ every upsert key first, so the insert phase
        // is a plain bulk INSERT (same move as the pg apply).
        if !c.deletes.is_empty() || !c.upserts.is_empty() {
            let del = ch_ident(&format!("{dest_table}__apitap_cdc_del"));
            // The key table is built ONCE per run and truncated per window. The
            // old DROP → CREATE → … → DROP cycle spent three HTTP round trips of
            // pure ceremony on every window of every table, and at ~20k events
            // per window that ceremony is a real slice of the apply: the whole
            // window is only ~7 round trips. TRUNCATE leaves the same empty
            // table the CREATE did, so a replayed window still sees exactly the
            // state it expects.
            //
            // The first-time path DROPs before creating rather than relying on
            // IF NOT EXISTS. `AS SELECT {pklist} FROM {ft} WHERE 0` freezes the
            // key table's COLUMN SET and TYPES at creation, and the per-window
            // insert names its columns explicitly — so if the source primary key
            // gains a column (or a PK column changes type), an inherited table
            // from an earlier run would make every window fail forever with a
            // ClickHouse error naming an internal table. The old per-window DROP
            // healed that on the next window; memoizing the DDL took the healing
            // away with it. One DROP per run restores it and still costs one
            // round trip per run instead of three per window.
            let patch = self.patch_ok().await;
            if self.first_time(&del) {
                self.ch.exec(&format!("DROP TABLE IF EXISTS {del}")).await?;
                self.ch
                    .exec(&format!(
                        "CREATE TABLE {del} ENGINE = MergeTree \
                         ORDER BY tuple() AS SELECT {pklist} FROM {ft} WHERE 0"
                    ))
                    .await?;
                if patch {
                    // Materializes block columns for parts written from here
                    // on; the probe showed pre-ALTER parts patch correctly too.
                    self.ch
                        .exec(&format!(
                            "ALTER TABLE {ft} MODIFY SETTING \
                             enable_block_number_column=1, enable_block_offset_column=1"
                        ))
                        .await?;
                }
            }
            self.ch.exec(&format!("TRUNCATE TABLE {del}")).await?;
            let mut buf = Vec::with_capacity(1 << 20);
            for key in &c.deletes {
                let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
                render_ch_key(&refs, &pk_oids, &mut buf)?;
            }
            for row in &c.upserts {
                render_ch_key(&row_key_refs(row, &pk_idx), &pk_oids, &mut buf)?;
            }
            self.ch
                .insert_stream(
                    &format!("INSERT INTO {del} ({pklist}) FORMAT TabSeparated"),
                    reqwest::Body::from(buf),
                )
                .await?;
            let pred = if pk_cols.len() == 1 {
                format!("{pklist} IN (SELECT {pklist} FROM {del})")
            } else {
                format!("({pklist}) IN (SELECT {pklist} FROM {del})")
            };
            let mode = if patch {
                " SETTINGS lightweight_delete_mode='lightweight_update'"
            } else {
                ""
            };
            self.ch
                .exec(&format!("DELETE FROM {ft} WHERE {pred}{mode}"))
                .await?;
        }

        if !c.upserts.is_empty() {
            let mut buf = Vec::with_capacity(4 << 20);
            for row in &c.upserts {
                render_ch_row(row, oids, &mut buf)?;
            }
            self.ch
                .insert_stream(
                    &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                    reqwest::Body::from(buf),
                )
                .await?;
        }

        // Residue tail: serial, ordered. Masked TOAST updates read the
        // missing columns back from the destination, then delete + reinsert
        // the patched row (ClickHouse has no cheap row UPDATE).
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
                        let sel = missing
                            .iter()
                            .map(|&i| ch_ident(&wal_cols[i]))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let body = self
                            .ch
                            .exec(&format!(
                                "SELECT {sel} FROM {ft} WHERE {pred} FORMAT TabSeparated"
                            ))
                            .await?;
                        let Some(line) = body.lines().next() else {
                            return Err(Error::Transfer(
                                "log_based: masked update for a row missing at the \
                                 destination — window replay out of order?"
                                    .into(),
                            ));
                        };
                        let fields: Vec<&str> = line.split('\t').collect();
                        if fields.len() != missing.len() {
                            return Err(Error::Transfer(
                                "log_based: masked-update readback column count \
                                 mismatch"
                                    .into(),
                            ));
                        }
                        for (&i, f) in missing.iter().zip(fields.iter()) {
                            // Readback is already destination-dialect: escape it
                            // straight back out, no OID translation.
                            full[i] = match tsv_unescape(f) {
                                None => Cell::Null,
                                Some(v) => Cell::Text(bytes::Bytes::from(v)),
                            };
                            // Mark: this cell must NOT be re-translated.
                        }
                    }
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                    let mut buf = Vec::new();
                    render_residue_row(&full, oids, &missing, &mut buf)?;
                    self.ch
                        .insert_stream(
                            &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                            reqwest::Body::from(buf),
                        )
                        .await?;
                }
                ResidueOp::Upsert { row } => {
                    let key: Vec<Vec<u8>> = row_key_refs_cells(row, &pk_idx)
                        .into_iter()
                        .map(|k| k.to_vec())
                        .collect();
                    let pred = key_pred(pk_cols, &key, &pk_oids)?;
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                    let mut buf = Vec::new();
                    render_ch_row_cells(row, oids, &mut buf)?;
                    self.ch
                        .insert_stream(
                            &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                            reqwest::Body::from(buf),
                        )
                        .await?;
                }
                ResidueOp::Delete { key } => {
                    let pred = key_pred(pk_cols, key, &pk_oids)?;
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                }
            }
        }

        self.write_state(dest_table, source_id, outcome.end_lsn, c.events).await?;
        Ok(c.events)
    }
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
    use super::cl_nullable;

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
