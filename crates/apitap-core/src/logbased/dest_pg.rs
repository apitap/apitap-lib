//! log_based apply into Postgres — the reference destination: one
//! transaction carries fence → truncate → deletes → plain bulk insert →
//! residue → watermark → renewal, and the slot is only confirmed after that
//! commit.
//!
//! Every statement that reaches a connection is in `mod store`. The apply body
//! below writes through the `PgUnit` it is handed — a transaction whose first
//! statement took this run's lease row `FOR UPDATE` — and cannot reach the
//! pool at all, so a write outside a fence is not expressible here.

use crate::error::{Error, Result};
use crate::lease::Watermark;
use crate::logbased::collapse::{Collapsed, ResidueOp};
use crate::logbased::replay::WindowId;
use crate::logbased::rowtext::{copy_escape, render_copy_row, row_key_refs};
use crate::logbased::window::TableWindow;
use crate::wire::pgoutput::Cell;
use sqlx::Executor;

pub(crate) use store::{PgStore, PgUnit};

use crate::naming::STATE_CURSOR_LSN as STATE_CURSOR;

pub(crate) struct PgDest {
    store: PgStore,
}

impl PgDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        Ok(Self { store: PgStore::connect(url).await? })
    }

    /// The store: the lease, the guard and the units a run's tenure takes.
    pub(crate) fn store(&self) -> &PgStore {
        &self.store
    }

    /// Resolve every member's schema once, before a lease key is taken —
    /// then refuse a group this lane cannot keep safe (P4).
    pub(crate) async fn resolve_names(&self, tables: &[String]) -> Result<()> {
        self.store.resolve_names(tables).await?;
        self.precheck_group_fks(tables).await
    }

    /// A group member that is the REFERENCED side of a foreign key is a data
    /// hazard on this lane: the drain applies a changed key as
    /// delete-then-insert, and deleting a parent row fires the reference —
    /// CASCADE / SET NULL / SET DEFAULT silently rewrites child rows that
    /// were never part of the change (children outside the group included),
    /// and NO ACTION / RESTRICT aborts the apply mid-run. Refused at
    /// admission, naming the constraint's two tables (system review
    /// 2026-10-07, P4).
    async fn precheck_group_fks(&self, tables: &[String]) -> Result<()> {
        let qualified: Vec<String> = tables
            .iter()
            .map(|t| {
                let p = self.store.parts_of(t);
                format!("{}.{}", p.schema, p.bare)
            })
            .collect();
        let rows: Vec<(String, String)> = sqlx::query_as::<_, (String, String)>(
            "SELECT pn.nspname || '.' || pcl.relname, n.nspname || '.' || cl.relname \
             FROM pg_constraint c \
             JOIN pg_class pcl ON pcl.oid = c.confrelid \
             JOIN pg_namespace pn ON pn.oid = pcl.relnamespace \
             JOIN pg_class cl ON cl.oid = c.conrelid \
             JOIN pg_namespace n ON n.oid = cl.relnamespace \
             WHERE c.contype = 'f' \
               AND (pn.nspname || '.' || pcl.relname) = ANY($1)",
        )
        .bind(&qualified)
        .fetch_all(self.store.pool())
        .await
        .map_err(|e| Error::Transfer(format!("log_based: foreign-key precheck: {e}")))?;
        if let Some((parent, child)) = rows.first() {
            return Err(Error::InvalidInput(format!(
                "log_based: destination table {parent} is a member of this group and is \
                 REFERENCED by a foreign key from {child}. The drain applies a changed \
                 key as delete-then-insert, so deleting a parent row fires the \
                 reference: ON DELETE CASCADE / SET NULL / SET DEFAULT silently \
                 rewrites or removes child rows — rows outside this group included — \
                 and NO ACTION / RESTRICT aborts the apply mid-run. Drop the foreign \
                 key on the destination (source commits already carry referential \
                 order) or keep one of the two tables out of this group."
            )));
        }
        Ok(())
    }

    /// The drain's watermark, through the one verdict both lanes share.
    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<crate::naming::CdcWatermark>> {
        crate::naming::cdc_watermark(dest_table, self.store.read_state(dest_table, source_id).await?)
    }

    /// The bootstrap's full load lands data without constraints; the drain's
    /// apply needs the identity — add it, inside the unit whose close writes
    /// the state row.
    pub(crate) async fn bootstrap_finish(&self, u: &mut PgUnit, dest_table: &str, pk_cols: &[String]) -> Result<()> {
        add_primary_key(u, dest_table, pk_cols).await
    }

    /// Apply one collapsed window for one table inside the unit — one
    /// destination transaction with the watermark its close writes.
    pub(crate) async fn apply(
        &self,
        u: &mut PgUnit,
        dest_table: &str,
        w: Option<&TableWindow<Collapsed>>,
        id: &WindowId,
        source_id: &str,
    ) -> Result<(u64, Watermark)> {
        apply_unit(u, dest_table, w, id, source_id).await
    }
}

/// The bootstrap's full load lands into a PK-less table on purpose (the
/// constraint would slow the COPY), so the identity is added here. But "here"
/// is not always a PK-less table: a destination previously built by a
/// user-run replace carries the PK its DDL gave it, and a blind ADD PRIMARY
/// KEY then fails with "multiple primary keys" AFTER the full load — a wasted
/// bootstrap for a constraint that was already right. Ask the catalog first:
/// an equal PK is the job already done; a DIFFERENT one is a real conflict the
/// user has to resolve, refused with both spellings on the table.
async fn add_primary_key(u: &mut PgUnit, dest_table: &str, pk_cols: &[String]) -> Result<()> {
    let ft = u.table(0).qualified();
    let existing: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = $1::regclass AND i.indisprimary \
           AND array_position(i.indkey, a.attnum) < i.indnkeyatts \
         ORDER BY array_position(i.indkey, a.attnum)",
    )
    .bind(&ft)
    .fetch_all(&mut **u.tx())
    .await
    .map_err(db_err)?;
    if existing.is_empty() {
        let pklist = pk_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        u.tx().execute(format!("ALTER TABLE {ft} ADD PRIMARY KEY ({pklist})").as_str()).await.map_err(db_err)?;
    } else if existing != pk_cols {
        return Err(Error::InvalidInput(format!(
            "log_based: {dest_table} already has PRIMARY KEY ({}) but the \
             source's key is ({}) — the drain would apply updates against \
             the wrong identity. Drop the destination table (or its \
             constraint) and re-run.",
            existing.join(", "),
            pk_cols.join(", "),
        )));
    }
    Ok(())
}

/// Apply one collapsed window for one table (truncate → deletes → upserts →
/// residue) inside the unit, and name the watermark its close writes. A window
/// with no traffic for this table writes nothing but that mark. Columns and
/// keys are the window's own layout: the key names are the ones the collapser
/// keyed its rows by.
async fn apply_unit(
    u: &mut PgUnit,
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
    let Some(w) = w else {
        // Foreign-table traffic only: nothing for our table, still advance.
        return Ok((0, set(0)));
    };
    let (c, l) = (w.body(), w.layout());
    let (wal_cols, pk_cols, pk_idx) = (l.cols(), l.key_cols(), l.key_idx());

    let ft = u.table(0).qualified();
    let collist = wal_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let pklist = pk_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let tx = u.tx();

    if c.truncate {
        tx.execute(format!("TRUNCATE {ft}").as_str()).await.map_err(db_err)?;
    }

    // Delete phase covers the delete-set UNION every upsert's key: clearing
    // the way first turns 450K index-probing ON CONFLICT upserts into 450K
    // plain inserts (ape-dts's rdb_merge trick — measured 5x here).
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
        for key in c.deletes.iter() {
            let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
            render_key_row(&refs, &mut buf);
            if buf.len() > 4 << 20 {
                // send(&buf) keeps the 4 MiB capacity — mem::take would
                // regrow it from zero every chunk on the timed path.
                copy.send(&buf[..]).await.map_err(db_err)?;
                buf.clear();
            }
        }
        for row in &c.upserts {
            render_key_row(&row_key_refs(row, pk_idx), &mut buf);
            if buf.len() > 4 << 20 {
                copy.send(&buf[..]).await.map_err(db_err)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            copy.send(&buf[..]).await.map_err(db_err)?;
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
                copy.send(&buf[..]).await.map_err(db_err)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            copy.send(&buf[..]).await.map_err(db_err)?;
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
    // Every value travels as a $n bind — a cell's text is embedded nowhere,
    // so no quoting rule (backslash, newline, a quote in the payload) can be
    // got wrong, which the old cell-literal rendering did for anything but a
    // lone apostrophe (system review 2026-10-07, dest_pg binds).
    for op in &c.residue {
        let mut binds: Vec<Option<String>> = Vec::new();
        let sql = match op {
            ResidueOp::MaskedUpdate { key, row } => {
                let sets = wal_cols
                    .iter()
                    .zip(row.iter())
                    .filter(|(cname, cell)| {
                        !matches!(cell, Cell::UnchangedToast) && !pk_cols.contains(cname)
                    })
                    .map(|(cname, cell)| {
                        binds.push(bind_of(cell));
                        format!("{} = ${}", quote_ident(cname), binds.len())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if sets.is_empty() {
                    continue;
                }
                let pred = key_pred_bound(pk_cols, key, &mut binds);
                format!("UPDATE {ft} SET {sets} WHERE {pred}")
            }
            ResidueOp::Upsert { row } => {
                let vals = row
                    .iter()
                    .map(|cell| {
                        binds.push(bind_of(cell));
                        format!("${}", binds.len())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
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
                let pred = key_pred_bound(pk_cols, key, &mut binds);
                format!("DELETE FROM {ft} WHERE {pred}")
            }
            ResidueOp::Rekey { old_key, row, .. } => {
                // Move the row rather than delete-and-reinsert. The columns
                // this UPDATE does not name keep their values, and the one
                // that matters here — the TOASTed cell the source did not
                // resend — is exactly such a column.
                //
                // The PK columns ARE included, unlike MaskedUpdate: moving
                // the key is the entire point.
                let sets = wal_cols
                    .iter()
                    .zip(row.iter())
                    .filter(|(_, cell)| !matches!(cell, Cell::UnchangedToast))
                    .map(|(cname, cell)| {
                        binds.push(bind_of(cell));
                        format!("{} = ${}", quote_ident(cname), binds.len())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if sets.is_empty() {
                    // Unreachable: the key changed, so at least one PK
                    // column carries a real value.
                    continue;
                }
                // Idempotent on replay by construction: once the move has
                // been applied the old key is gone, so a re-applied window
                // matches zero rows and changes nothing.
                let pred = key_pred_bound(pk_cols, old_key, &mut binds);
                format!("UPDATE {ft} SET {sets} WHERE {pred}")
            }
        };
        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(b.as_deref());
        }
        q.execute(&mut **tx).await.map_err(db_err)?;
    }

    Ok((c.events, set(c.events)))
}

/// Everything that holds a connection. See the module doc.
mod store {
    use super::{db_err, upsert_state_tx};
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{no_longer_holds, Fence, LeaseStore, Watermark};
    use crate::sink::postgres::{lease_table, PgGuard, PgParts};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool, Postgres};
    use std::collections::{BTreeMap, HashMap};

    /// The schema half of a lease key. Keys are `PgParts::label()`, always
    /// qualified, so this never guesses.
    fn key_schema(key: &str) -> String {
        PgParts::split(key).map(|p| p.schema).unwrap_or_else(|| "public".into())
    }

    /// Every key in a group shares a schema in practice, but not by
    /// construction — so group by schema rather than assume.
    fn by_schema(keys: &[String]) -> BTreeMap<String, Vec<String>> {
        let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for k in keys {
            m.entry(key_schema(k)).or_default().push(k.clone());
        }
        m
    }

    pub(crate) struct PgStore {
        /// The apply lanes' connections.
        pool: PgPool,
        /// The keeper's own connection. On the two-connection apply pool, two
        /// lanes each holding a unit left the keeper nothing to renew with,
        /// and the members waiting their turn lapsed under a live run.
        keeper_pool: PgPool,
        /// Where each destination table lives, resolved once per run by
        /// `resolve_names` — the SAME rule the bulk lane uses, so the lock, the
        /// scan, the lease and the fence all agree under any `search_path`.
        parts: std::sync::Mutex<HashMap<String, PgParts>>,
        state_ready: std::sync::atomic::AtomicBool,
    }

    /// One fenced transaction. Dropped without `close_unit`, it rolls back.
    pub(crate) struct PgUnit {
        tx: sqlx::Transaction<'static, Postgres>,
        parts: Vec<PgParts>,
    }

    impl PgUnit {
        pub(crate) fn tx(&mut self) -> &mut sqlx::Transaction<'static, Postgres> {
            &mut self.tx
        }

        /// The i-th member of the unit, as resolved: `qualified()` is the only
        /// spelling a data statement uses.
        pub(crate) fn table(&self, i: usize) -> &PgParts {
            &self.parts[i]
        }
    }

    impl PgStore {
        pub(super) fn pool(&self) -> &PgPool {
            &self.pool
        }

        pub(crate) async fn connect(url: &str) -> Result<Self> {
            let connect = |n: u32| async move {
                PgPoolOptions::new()
                    .max_connections(n)
                    .connect(url)
                    .await
                    .map_err(|e| Error::Transfer(format!("log_based: dest connect: {e}")))
            };
            Ok(Self {
                pool: connect(2).await?,
                keeper_pool: connect(1).await?,
                parts: Default::default(),
                state_ready: Default::default(),
            })
        }

        #[cfg(test)]
        pub(crate) fn lazy(url: &str) -> Self {
            let lazy = || PgPoolOptions::new().connect_lazy(url).unwrap();
            Self { pool: lazy(), keeper_pool: lazy(), parts: Default::default(), state_ready: Default::default() }
        }

        #[cfg(test)]
        pub(crate) fn pin(&self, table: &str, p: PgParts) {
            self.parts.lock().expect("parts").insert(table.to_string(), p);
        }

        pub(crate) async fn resolve_names(&self, tables: &[String]) -> Result<()> {
            for t in tables {
                let p = crate::sink::postgres::resolve_parts(&self.pool, t).await?;
                self.parts.lock().expect("parts").insert(t.clone(), p);
            }
            Ok(())
        }

        /// The resolved parts of `dest_table`. A table the run did not resolve
        /// can only be a qualified one written as such (every member is
        /// resolved before its first lease key), so the fallback is the name
        /// as written.
        pub(super) fn parts_of(&self, dest_table: &str) -> PgParts {
            if let Some(p) = self.parts.lock().expect("parts").get(dest_table) {
                return p.clone();
            }
            debug_assert!(dest_table.contains('.'), "{dest_table}: lease key before resolve_names");
            PgParts::split(dest_table)
                .unwrap_or_else(|| PgParts { schema: "public".into(), bare: dest_table.into() })
        }

        pub(crate) fn pg_guard(&self, dest_table: &str) -> (PgGuard, String) {
            let parts = self.parts_of(dest_table);
            (PgGuard::new(self.pool.clone(), parts.schema), parts.bare)
        }

        /// Created outside any unit: IF NOT EXISTS is not atomic, and the
        /// loser of a race raises inside whatever transaction it is in.
        async fn ensure_state_table(&self) -> Result<()> {
            if self.state_ready.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
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
                .map(|_| ())
                .or_else(|e| match &e {
                    // IF NOT EXISTS is not atomic: two first-runs bootstrapping
                    // into a fresh destination at once can both pass the
                    // existence check, and the loser raises 42P07
                    // (duplicate_table) or 23505 on pg_type's unique index. The
                    // table exists either way, which is the only thing this
                    // function promises.
                    sqlx::Error::Database(d)
                        if matches!(d.code().as_deref(), Some("42P07") | Some("23505")) =>
                    {
                        Ok(())
                    }
                    _ => Err(db_err(e)),
                })?;
            self.state_ready.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        /// This table's state row, whichever lane wrote it.
        pub(crate) async fn read_state(
            &self,
            dest_table: &str,
            source_id: &str,
        ) -> Result<Option<crate::naming::StateRow>> {
            // Both spellings, because the bulk lane keys the same table as
            // schema.bare where this lane keys it bare — see
            // `naming::pg_state_keys`.
            //
            // And deliberately NOT filtered on mode. A row written by the
            // cursor lane used to be simply invisible, so a table that had been
            // append-ed and was then pointed at log_based saw NO state, decided
            // it was a fresh destination, and quietly ran a full bootstrap.
            // Read the row whatever wrote it; `naming::state_verdict` refuses
            // it if it is not ours, in both lanes.
            let (bare, qualified) = crate::naming::pg_state_keys(dest_table);
            let row: Option<(Option<String>, String, String)> = sqlx::query_as(
                "SELECT watermark, cursor_col, mode FROM _apitap_state \
                 WHERE dest_table IN ($1, $2) AND source_id = $3 \
                 ORDER BY (dest_table = $1) DESC LIMIT 1",
            )
            .bind(bare)
            .bind(qualified)
            .bind(source_id)
            .fetch_optional(&self.pool)
            .await
            .or_else(|e| match &e {
                // No state table at all = fresh destination.
                sqlx::Error::Database(d) if d.code().as_deref() == Some("42P01") => Ok(None),
                _ => Err(db_err(e)),
            })?;
            Ok(row.map(|(wm, cursor, mode)| crate::naming::StateRow::new(wm, Some(cursor), Some(mode))))
        }

        /// The schemas this run's keys can live in.
        fn schemas(&self) -> Vec<String> {
            let mut v: Vec<String> =
                self.parts.lock().expect("parts").values().map(|p| p.schema.clone()).collect();
            v.sort();
            v.dedup();
            v
        }
    }

    impl LeaseStore for PgStore {
        fn lease_key(&self, dest_table: &str) -> String {
            self.parts_of(dest_table).label()
        }

        async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
            for (schema, ks) in by_schema(keys) {
                crate::sink::postgres::lease_open(&self.pool, &schema, &ks, token).await?;
            }
            Ok(())
        }

        async fn lease_renew(&self, keys: &[String], token: &str) -> Result<u64> {
            let mut n = 0;
            for (schema, ks) in by_schema(keys) {
                n += crate::sink::postgres::lease_renew(&self.keeper_pool, &schema, &ks, token).await?;
            }
            Ok(n)
        }

        async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
            let mut out = Vec::new();
            for schema in self.schemas() {
                out.extend(crate::sink::postgres::lease_unclaimed(&self.keeper_pool, &schema, token).await?);
            }
            Ok(out)
        }

        async fn close_run(&self, _token: &str) -> Result<()> {
            Ok(())
        }
    }

    impl Fence for PgStore {
        type Unit<'a> = PgUnit;

        fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
            let (g, bare) = self.pg_guard(dest_table);
            (Box::new(g), bare)
        }

        /// FENCE: the first statement of every transaction this drain writes
        /// with takes its own lease row `FOR UPDATE`, and a collector's claim
        /// is an `UPDATE … NOWAIT` of that same row. Either this transaction
        /// takes the row first — the collector gets 55P03 and refuses, and this
        /// run is the only writer — or the collector took it first, this finds
        /// the row collected, and the transaction ends having written nothing.
        /// The interleaving that would hurt (fence passes, claim succeeds, this
        /// run writes) cannot happen: fence and writes are one transaction
        /// holding one row lock throughout.
        ///
        /// No row is not an owner: 0.56.0 read "no lease" as "nothing to fence
        /// against" and wrote. Expiry is not part of it either — a lapse nobody
        /// claimed is still this run's, and the close renews it.
        async fn open_unit<'a>(&'a self, keys: &[String], token: &str) -> Result<PgUnit> {
            self.ensure_state_table().await?;
            let mut tx = self.pool.begin().await.map_err(db_err)?;
            // Sorted, so two lanes never take two rows in opposite orders.
            let mut sorted: Vec<&String> = keys.iter().collect();
            sorted.sort();
            for k in sorted {
                let held: Option<(i32,)> = match sqlx::query_as(&format!(
                    "SELECT 1 FROM {} WHERE dest_key = $1 AND token = $2 AND NOT collected FOR UPDATE",
                    lease_table(&key_schema(k))
                ))
                .bind(k)
                .bind(token)
                .fetch_optional(&mut *tx)
                .await
                {
                    Ok(r) => r,
                    Err(e) if e.as_database_error().and_then(|d| d.code()).is_some_and(|c| c == "42P01") => None,
                    Err(e) => return Err(db_err(e)),
                };
                if held.is_none() {
                    return Err(no_longer_holds(keys));
                }
            }
            let parts = keys
                .iter()
                .map(|k| PgParts::split(k).unwrap_or_else(|| PgParts { schema: "public".into(), bare: k.clone() }))
                .collect();
            Ok(PgUnit { tx, parts })
        }

        /// Every mark, then the renewal — `clock_timestamp()`, not `now()`,
        /// which is the transaction's START and would hand a long apply a
        /// lease that expires the moment it commits — then COMMIT. A renewal
        /// that touches no row means the claim is gone: roll back.
        async fn close_unit<'a>(&'a self, mut u: PgUnit, token: &str, marks: Vec<Watermark>) -> Result<()> {
            for m in &marks {
                match m {
                    Watermark::Set { table, source_id, lsn, rows } => {
                        upsert_state_tx(&mut u.tx, table, source_id, *lsn, *rows).await?
                    }
                    Watermark::Clear { table, source_id } => {
                        let (bare, qualified) = crate::naming::pg_state_keys(table);
                        sqlx::query("DELETE FROM _apitap_state WHERE dest_table IN ($1, $2) AND source_id = $3")
                            .bind(bare)
                            .bind(qualified)
                            .bind(source_id)
                            .execute(&mut *u.tx)
                            .await
                            .map_err(db_err)?;
                    }
                }
            }
            let mut keys: Vec<String> = u.parts.iter().map(|p| p.label()).collect();
            keys.sort();
            for k in &keys {
                let r = sqlx::query(&format!(
                    "UPDATE {} SET expires_at = clock_timestamp() + make_interval(secs => $3) \
                     WHERE dest_key = $1 AND token = $2 AND NOT collected",
                    lease_table(&key_schema(k))
                ))
                .bind(k)
                .bind(token)
                .bind(crate::lease::ttl_secs() as f64)
                .execute(&mut *u.tx)
                .await
                .map_err(db_err)?;
                if r.rows_affected() != 1 {
                    return Err(no_longer_holds(&keys));
                }
            }
            u.tx.commit().await.map_err(db_err)
        }
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
/// What one residue cell binds as: `None` is SQL NULL (a TOAST cell the
/// source did not resend included — the literal path rendered it NULL too),
/// `Some` is the cell's text as it arrived (system review 2026-10-07,
/// dest_pg binds).
fn bind_of(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null | Cell::UnchangedToast => None,
        Cell::Text(t) => Some(String::from_utf8_lossy(t).into_owned()),
    }
}

/// The residue WHERE clause: every key value binds as the next `$n`; the
/// clause carries column names only (system review 2026-10-07, dest_pg
/// binds).
fn key_pred_bound(
    pk_cols: &[String],
    key: &[Vec<u8>],
    binds: &mut Vec<Option<String>>,
) -> String {
    pk_cols
        .iter()
        .zip(key.iter())
        .map(|(c, v)| {
            binds.push(Some(String::from_utf8_lossy(v).into_owned()));
            format!("{} = ${}", quote_ident(c), binds.len())
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

async fn upsert_state_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dest_table: &str,
    source_id: &str,
    lsn: u64,
    rows: u64,
) -> Result<()> {
    // The spelling this lane has always written, unchanged: every "clear the
    // state row" message, every runbook and every fixture names it. The
    // dual-spelling READ above is what reaches the bulk lane's row; nothing
    // needs to move on disk for that to work.
    let (bare, _qualified) = crate::naming::pg_state_keys(dest_table);
    sqlx::query(
        "INSERT INTO _apitap_state \
           (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
         VALUES ($1, $2, $3, $4, 'log_based', $5, now()) \
         ON CONFLICT (dest_table, source_id) DO UPDATE SET \
           cursor_col = EXCLUDED.cursor_col, watermark = EXCLUDED.watermark, \
           mode = EXCLUDED.mode, last_rows = EXCLUDED.last_rows, synced_at = now()",
    )
    .bind(bare)
    .bind(source_id)
    .bind(STATE_CURSOR)
    .bind(lsn.to_string())
    .bind(rows as i64)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::GuardStore;
    use crate::lease::LeaseStore;
    use crate::sink::postgres::{PgGuard, PgParts};

    /// The drain's lease key and the guard's refusal name are ONE string. If
    /// they drift, a collector reads a lease row the drain never writes, and a
    /// live drain looks dead (or a dead one uncollectable). 0.56.0 keyed the
    /// drain on `public.<t>` whatever the `search_path`, while the bulk lane
    /// resolved the real schema.
    #[test]
    fn lease_key_is_dest_label() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let url = "postgres://u@127.0.0.1:1/db";
            let pool = sqlx::postgres::PgPoolOptions::new().connect_lazy(url).unwrap();
            let d = PgStore::lazy(url);
            let cases = [
                ("orders", PgParts { schema: "cdcdest".into(), bare: "orders".into() }),
                ("events", PgParts { schema: "public".into(), bare: "events".into() }),
                ("Mixed Case", PgParts { schema: "postgres".into(), bare: "Mixed Case".into() }),
            ];
            for (t, p) in &cases {
                d.pin(t, p.clone());
                assert_eq!(d.lease_key(t), PgGuard::new(pool.clone(), p.schema.clone()).dest_label(&p.bare),
                           "{t}");
            }
            // A qualified name nobody resolved is taken as written, on both sides.
            assert_eq!(d.lease_key("sales.orders"),
                       PgGuard::new(pool.clone(), "sales").dest_label("orders"));
        });
    }

    #[test]
    fn qualified_quotes_each_half() {
        let p = PgParts { schema: "my.schema".into(), bare: "a\"b".into() };
        assert_eq!(p.qualified(), "\"my.schema\".\"a\"\"b\"");
    }
    /// Residue values bind, never embed: a payload carrying a quote, a
    /// backslash or a newline must reach the database as a parameter — the
    /// old literal rendering escaped only the apostrophe (system review
    /// 2026-10-07, dest_pg binds).
    #[test]
    fn residue_keys_and_cells_bind_instead_of_embedding() {
        let pk = vec!["id".to_string()];
        let mut binds: Vec<Option<String>> = Vec::new();
        let pred = key_pred_bound(&pk, &[b"x'; DROP TABLE t; --".to_vec()], &mut binds);
        assert_eq!(pred, "\"id\" = $1");
        assert_eq!(binds, vec![Some("x'; DROP TABLE t; --".to_string())]);

        let poison = Cell::Text(b"a'b\\c\n".as_slice().into());
        assert_eq!(bind_of(&poison).as_deref(), Some("a'b\\c\n"));
        assert_eq!(bind_of(&Cell::Null), None);
        assert_eq!(bind_of(&Cell::UnchangedToast), None);
    }
}
