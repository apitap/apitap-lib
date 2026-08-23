//! Postgres sink: [`PgSink`] — `COPY … FROM STDIN (FORMAT binary)` into a
//! staging table, then the atomic swap (replace) / `INSERT … SELECT` (append) /
//! `ON CONFLICT` upsert (merge). Captures indexes, constraints and grants so a
//! replace re-applies what the swap would destroy.

use crate::sink::Loader;
use crate::error::{Error, Result};
use crate::plan::{Delivered, DestState, Lane, TablePlan, WireFormat};
use crate::Mode;
use sqlx::postgres::{PgPoolCopyExt, PgPoolOptions};
use sqlx::PgPool;
use crate::dialect::postgres::{quote_ident, quote_ident_path};
// ---------------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------------

pub(crate) struct PgSink {
    pool: PgPool,
    /// Quoted destination / staging idents; staging lives in the destination's schema.
    final_t: String,
    staging_t: String,
    bare: String,
    /// Unquoted schema name, for catalog lookups (`public` when unqualified).
    schema: String,
    /// This run's identity — in the staging name, and what `prepare` compares
    /// a live peer's staging against.
    run: crate::naming::RunId,
    /// DDL captured from the pre-swap table (indexes, constraints, grants) — the swap
    /// destroys them, so replace mode re-applies these after the commit.
    restore_ddl: Vec<String>,
    /// Destination primary-key columns (merge mode), introspected in `dest_state`.
    merge_keys: Vec<String>,
    /// Set when a merge run has to bootstrap (dest missing): the swap then also
    /// recreates the SOURCE's primary key so the next merge run can upsert.
    bootstrap_pk: bool,
    /// Cursor column (incremental modes), for deterministic merge dedup ordering.
    cursor_col: Option<String>,
    /// Credential-free source identity — half of the state row's key.
    source_id: Option<String>,
    /// Cursor column's source type — picks the dialect-neutral watermark rendering.
    wm_udt: Option<String>,
    /// Canonical unquoted `schema.table` — the state row's other key half.
    dest_key: String,
    /// Was dest_table schema-qualified by the caller? Unqualified names get their
    /// schema resolved from the live connection in dest_state.
    qualified: bool,
    /// Plan column names in order, stashed at `prepare` for the merge upsert.
    col_names: Vec<String>,
    copy_in_sql: String,
    /// Double-buffer sends through a small channel so the source keeps being read
    /// WHILE the previous buffer is in flight (−11% on the byte-relay route). Row
    /// sources set this false: their encode is already CPU-heavy and the overlap was
    /// MEASURED to hurt (+2-3 s at 10M) — see benchmarks/README.md.
    overlap_send: bool,
}

impl PgSink {
    pub(crate) async fn connect(
        url: &str,
        dest_table: &str,
        max_conns: usize,
        overlap_send: bool,
        run: &crate::naming::RunId,
    ) -> Result<Self> {
        Ok(Self::bind(
            Self::shared_pool(url, max_conns).await?,
            dest_table,
            overlap_send,
            run,
        ))
    }

    /// The sink-side pool alone — a multi-table run builds it once and binds one
    /// sink per table onto it.
    pub(crate) async fn shared_pool(url: &str, max_conns: usize) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(max_conns as u32)
            .connect(url)
            .await
            .map_err(|e| crate::urlerr::connect_err("postgres destination", url, e))
    }

    /// Collect dead staging objects and refuse live peers.
    ///
    /// The listing is one catalog SELECT, and it REPLACES the `DROP TABLE IF
    /// EXISTS` it stands in for — on Postgres that is strictly cheaper, because
    /// the DROP took an ACCESS EXCLUSIVE lock and this takes none.
    ///
    /// Liveness is read from the name, not the catalog: Postgres stores no
    /// table creation time (MySQL has `create_time`, ClickHouse
    /// `metadata_modification_time`, BigQuery `creationTime`, the object stores
    /// their own), so the age lives in the token. One rule on every engine
    /// beats five clever ones.
    async fn reap_and_check_peers(&self) -> Result<()> {
        use crate::naming::{parse_peer, Artifact};
        let (head, suffix) =
            crate::naming::artifact_match(&self.bare, Artifact::Staging, crate::naming::PG_IDENT_MAX);
        // `_` and `%` are LIKE wildcards and both appear in these names.
        let esc = |v: &str| v.replace('\\', "\\\\").replace('_', "\\_").replace('%', "\\%");
        let pattern = format!("{}%{}", esc(&head), esc(suffix));
        let found: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind = 'r' AND c.relname LIKE $2",
        )
        .bind(&self.schema)
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Transfer(format!("staging scan: {e}")))?;

        let mine = parse_peer(
            &crate::naming::artifact_ident_run(
                &self.bare, Artifact::Staging, crate::naming::PG_IDENT_MAX, &self.run),
            Artifact::Staging,
        )
        .expect("a name this process minted parses");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let horizon = crate::naming::reap_horizon_secs();
        // The one name an older apitap would have used. It carries no token and
        // therefore no age, so it cannot be aged out — but leaving it forever
        // would break the cleanup this sink has always promised, and a
        // concurrent old-version run is the very bug being fixed. Collect it.
        let legacy =
            crate::naming::artifact_ident(&self.bare, Artifact::Staging, crate::naming::PG_IDENT_MAX);

        for name in found {
            let Some(peer) = parse_peer(&name, Artifact::Staging) else {
                if name == legacy {
                    self.drop_staging(&name).await?;
                }
                continue;
            };
            let age = now.saturating_sub(peer.started_unix);
            if horizon != 0 && age > horizon {
                self.drop_staging(&name).await?;
                continue;
            }
            if crate::naming::peer_blocks(&mine, &peer) {
                return Err(Error::Locked(format!(
                    "{}.{}: another apitap run is already loading this table — it \
                     started {age}s ago and lands rows by {}. Two of them cannot \
                     share one destination: {}. Run them one at a time (a \
                     scheduler's own concurrency setting is the usual answer: \
                     Airflow max_active_runs=1, a cron flock). If that run is \
                     dead rather than slow, its staging table {name} is reaped \
                     automatically after {}s, or you can DROP it now.",
                    self.schema,
                    self.bare,
                    match peer.kind {
                        crate::naming::LandKind::Swap => "replacing the whole table",
                        crate::naming::LandKind::Incremental => "appending to it",
                        crate::naming::LandKind::Cdc => "draining changes into it",
                    },
                    match mine.kind {
                        crate::naming::LandKind::Swap =>
                            "a replace swaps the whole table, so whichever finishes \
                             second throws the other's work away",
                        crate::naming::LandKind::Cdc =>
                            "a CDC drain owns the watermark and the replication slot",
                        crate::naming::LandKind::Incremental =>
                            "both would read the same watermark and land the same \
                             rows twice",
                    },
                    horizon,
                )));
            }
        }
        Ok(())
    }

    async fn drop_staging(&self, name: &str) -> Result<()> {
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {}",
            quote_ident_path(&format!("{}.{}", self.schema, name))
        ))
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| Error::Transfer(format!("reap staging {name}: {e}")))
    }

    /// Bind one destination table onto an existing pool. All per-table state
    /// (staging name, swap keys) lives in the sink; the pool carries none.
    pub(crate) fn bind(
        pool: PgPool,
        dest_table: &str,
        overlap_send: bool,
        run: &crate::naming::RunId,
    ) -> Self {
        let qualified = dest_table.contains('.');
        let (schema_pfx, bare) = match dest_table.rsplit_once('.') {
            Some((s, t)) => (format!("{s}."), t.to_string()),
            None => (String::new(), dest_table.to_string()),
        };
        // The run's token is IN the name, which is what makes it impossible for
        // one run to publish another's work: finalize renames a name only this
        // run could have minted. Before this, two runs shared one staging
        // object — A streamed its rows in, B's prepare dropped it and made a
        // fresh one, and A's finalize published B's empty table over the
        // destination while returning A's row count.
        let staging_bare = crate::naming::artifact_ident_run(
            &bare,
            crate::naming::Artifact::Staging,
            crate::naming::PG_IDENT_MAX,
            run,
        );
        let staging_t = quote_ident_path(&format!("{schema_pfx}{staging_bare}"));
        let schema = schema_pfx.trim_end_matches('.').to_string();
        let schema = if schema.is_empty() {
            "public".into()
        } else {
            schema
        };
        Self {
            pool,
            final_t: quote_ident_path(dest_table),
            copy_in_sql: format!("COPY {staging_t} FROM STDIN (FORMAT binary)"),
            staging_t,
            dest_key: format!("{schema}.{bare}"),
            bare,
            schema,
            qualified,
            restore_ddl: Vec::new(),
            merge_keys: Vec::new(),
            bootstrap_pk: false,
            cursor_col: None,
            source_id: None,
            wm_udt: None,
            col_names: Vec::new(),
            overlap_send,
            run: run.clone(),
        }
    }
}

impl PgSink {
    async fn final_exists(&self) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1::text) IS NOT NULL")
            .bind(&self.final_t)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("dest lookup: {e}")))
    }

    /// Destination column names in ordinal order.
    async fn final_columns(&self) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            "SELECT a.attname FROM pg_attribute a \
             WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&self.final_t)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Transfer(format!("dest columns: {e}")))
    }
}

impl PgSink {
    /// Views on the destination make a replace fail at the very END: the swap
    /// `DROP`s the old table and Postgres refuses while a view depends on it.
    /// The data was already copied by then. Refusing at probe time costs the
    /// user nothing they had before — the run failed either way — and saves the
    /// entire transfer's work.
    async fn refuse_dependent_views(&self) -> Result<()> {
        let views: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT quote_ident(n.nspname) || '.' || quote_ident(c.relname) \
             FROM pg_depend d \
             JOIN pg_rewrite r ON r.oid = d.objid \
             JOIN pg_class c ON c.oid = r.ev_class \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE d.refobjid = to_regclass($1::text) \
               AND d.classid = 'pg_rewrite'::regclass \
               AND c.relkind IN ('v', 'm') \
               AND c.oid <> to_regclass($1::text)",
        )
        .bind(&self.final_t)
        .fetch_all(&self.pool)
        .await
        // NOT unwrap_or_default: swallowing this query's error would turn
        // "I could not check" into "there is nothing to worry about", and the
        // user would meet the problem at the end of the load instead — which
        // is exactly the failure this check exists to move earlier.
        .map_err(|e| Error::Transfer(format!("probe dependent views: {e}")))?;
        if views.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = views.into_iter().map(|(v,)| v).collect();
        Err(Error::InvalidInput(format!(
            "mode='replace' into {} cannot swap the table: {} depend{} on it, and \
             Postgres refuses to drop a table a view needs. Nothing has been \
             copied yet — this is checked before the load, not after it. Drop and \
             recreate the view(s) around the transfer, or use mode='append'/'merge', \
             which never drop the table.",
            self.final_t,
            names.join(", "),
            if names.len() == 1 { "s" } else { "" }
        )))
    }


    fn state_table(&self) -> String {
        format!("{}.\"_apitap_state\"", quote_ident(&self.schema))
    }

    /// Dialect-neutral watermark SELECT for a cursor of type `udt` against `table`.
    fn wm_expr(udt: &str, qcur: &str, table: &str) -> String {
        match udt {
            "timestamptz" => format!(
                "SELECT to_char(max({qcur}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM {table}"
            ),
            "timestamp" | "datetime" => format!(
                "SELECT to_char(max({qcur}), 'YYYY-MM-DD HH24:MI:SS.US') FROM {table}"
            ),
            _ => format!("SELECT max({qcur})::text FROM {table}"),
        }
    }

    async fn ensure_state_table(&self) -> Result<()> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                dest_table  text NOT NULL, \
                source_id   text NOT NULL, \
                cursor_col  text NOT NULL, \
                watermark   text NOT NULL, \
                mode        text NOT NULL, \
                last_rows   bigint NOT NULL, \
                synced_at   timestamptz NOT NULL DEFAULT now(), \
                PRIMARY KEY (dest_table, source_id))",
            self.state_table()
        ))
        .execute(&self.pool)
        .await
        .map(|_| ())
        .or_else(|e| {
            // Two concurrent first-runs can both pass IF NOT EXISTS — the loser's
            // error is harmless, the table exists either way.
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("duplicate key") {
                Ok(())
            } else {
                Err(Error::Transfer(format!("state table: {e}")))
            }
        })
    }

    /// Upsert the state row inside `tx` — same transaction as the data landing, so
    /// state and data can never drift apart.
    async fn write_state(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        watermark: &str,
        mode: Mode,
        rows: u64,
    ) -> Result<()> {
        let (Some(cursor), Some(source_id)) = (&self.cursor_col, &self.source_id) else {
            return Ok(()); // plain replace without incremental context: no state
        };
        sqlx::query(&format!(
            "INSERT INTO {} \
             (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ($1, $2, $3, $4, $5, $6, now()) \
             ON CONFLICT (dest_table, source_id) DO UPDATE SET \
               cursor_col = EXCLUDED.cursor_col, watermark = EXCLUDED.watermark, \
               mode = EXCLUDED.mode, last_rows = EXCLUDED.last_rows, synced_at = now()",
            self.state_table()
        ))
        .bind(&self.dest_key)
        .bind(source_id)
        .bind(cursor)
        .bind(watermark)
        .bind(match mode {
            Mode::Replace => "replace",
            Mode::Append => "append",
            Mode::Merge => "merge",
            Mode::LogBased => "log_based",
        })
        .bind(rows as i64)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Transfer(format!("state write: {e}")))?;
        Ok(())
    }

    /// Watermark of what a table currently holds, rendered dialect-neutrally.
    async fn table_watermark(&self, table: &str) -> Result<Option<String>> {
        let (Some(cursor), Some(udt)) = (&self.cursor_col, &self.wm_udt) else {
            return Ok(None);
        };
        sqlx::query_scalar(&Self::wm_expr(udt, &quote_ident(cursor), table))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("watermark: {e}")))
    }
}

/// Postgres column type for a delivered value from a non-Postgres source.
fn pg_type_of(d: &Delivered) -> String {
    match d {
        // Unsigned deliveries widen (Postgres has no unsigned ints) so the value
        // always FITS — a u16 crammed into smallint would reinterpret silently.
        Delivered::Int {
            bytes: 8,
            unsigned: true,
        } => "numeric(20,0)".into(),
        Delivered::Int {
            bytes: 4,
            unsigned: true,
        } => "bigint".into(),
        Delivered::Int {
            bytes: 2,
            unsigned: true,
        } => "integer".into(),
        Delivered::Int { bytes: 1 | 2, .. } => "smallint".into(),
        Delivered::Int { bytes: 4, .. } => "integer".into(),
        Delivered::Int { .. } => "bigint".into(),
        Delivered::Float32 => "real".into(),
        Delivered::Float64 => "double precision".into(),
        Delivered::Decimal { p: 0, .. } => "numeric".into(),
        Delivered::Decimal { p, s } => format!("numeric({p},{s})"),
        Delivered::Bool => "boolean".into(),
        Delivered::Date => "date".into(),
        Delivered::DateTime { utc: false } => "timestamp(6)".into(),
        Delivered::DateTime { utc: true } => "timestamptz(6)".into(),
        Delivered::Uuid => "uuid".into(),
        Delivered::Json => "jsonb".into(),
        Delivered::Text => "text".into(),
        Delivered::Bytes => "bytea".into(),
    }
}

impl crate::sink::Sink for PgSink {
    type Loader = PgCopyLoader;

    fn accepts(&self) -> &[WireFormat] {
        &[WireFormat::PgCopyBinary]
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        durable: bool,
        mode: Mode,
    ) -> Result<()> {
        self.col_names = plan.cols.iter().map(|c| c.name.clone()).collect();

        // Replace destroys the old table's indexes, constraints and grants with it —
        // capture them now so finalize can re-apply after the swap. (Column DEFAULTs
        // and identity/serial ownership are NOT preserved; documented.)
        if mode == Mode::Replace && self.final_exists().await? {
            // Before anything is copied: a view on this table makes the final
            // swap impossible, and finding that out after the load is a whole
            // transfer wasted. dest_state would have been the natural home for
            // this check, except dest_state only runs for the incremental
            // modes — replace never reaches it.
            self.refuse_dependent_views().await?;
            // Resolve the table's ACTUAL namespace — an unqualified dest name follows
            // search_path everywhere else; hardcoding 'public' here would silently
            // skip the index/grant capture for such tables.
            let schema: String = sqlx::query_scalar(
                "SELECT n.nspname FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = to_regclass($1::text)",
            )
            .bind(&self.final_t)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?;
            let mut ddl: Vec<String> = Vec::new();
            let cons: Vec<(String, String)> = sqlx::query_as(
                "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conrelid = $1::regclass AND contype IN ('p','u','f','c','x') \
                 ORDER BY contype = 'f', conname",
            )
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture constraints: {e}")))?;
            for (name, def) in &cons {
                ddl.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    self.final_t,
                    quote_ident(name),
                    def
                ));
            }
            // Plain indexes (constraint-backed ones come back via ADD CONSTRAINT).
            let idx: Vec<String> = sqlx::query_scalar(
                "SELECT indexdef FROM pg_indexes \
                 WHERE schemaname = $1 AND tablename = $2 AND indexname NOT IN \
                 (SELECT conname FROM pg_constraint WHERE conrelid = $3::regclass)",
            )
            .bind(&schema)
            .bind(&self.bare)
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture indexes: {e}")))?;
            ddl.extend(idx); // indexdef names the same table the swap re-creates
            let grants: Vec<(String, String)> = sqlx::query_as(
                "SELECT grantee, string_agg(DISTINCT privilege_type, ', ') \
                 FROM information_schema.role_table_grants \
                 WHERE table_schema = $1 AND table_name = $2 AND grantee <> current_user \
                 GROUP BY grantee",
            )
            .bind(&schema)
            .bind(&self.bare)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture grants: {e}")))?;
            for (grantee, privs) in &grants {
                let who = if grantee == "PUBLIC" {
                    "PUBLIC".to_string()
                } else {
                    quote_ident(grantee)
                };
                ddl.push(format!("GRANT {privs} ON {} TO {who}", self.final_t));
            }
            self.restore_ddl = ddl;
        }
        if self.bootstrap_pk {
            let keys = plan
                .pk_cols
                .iter()
                .map(|k| quote_ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            self.restore_ddl.push(format!(
                "ALTER TABLE {} ADD PRIMARY KEY ({keys})",
                self.final_t
            ));
        }

        // Binary COPY is positional + type-strict. Same-engine transfers mirror the
        // source's exact type spellings; anything else maps the lane's deliveries.
        let cols_ddl = plan
            .cols
            .iter()
            .zip(lane.cols.iter())
            .map(|(c, lc)| {
                let ty = match (&c.native_ddl, plan.engine) {
                    (Some(native), "postgres") => native.clone(),
                    _ => pg_type_of(&lc.delivered),
                };
                format!("{} {}", quote_ident(&c.name), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        // No blind DROP any more. That statement WAS the defect: it destroyed
        // whatever staging object it found, including a concurrent run's, and
        // this run's finalize then published the empty replacement over the
        // destination while reporting its own row count.
        //
        // Instead: look at what is there, reap what is dead, refuse what is
        // alive and cannot coexist. Then create a name only this run could have
        // minted — so nothing has to be dropped first, and the CREATE failing
        // would mean a token collision, which is loud rather than destructive.
        self.reap_and_check_peers().await?;
        // Incremental staging never becomes the final table — always skip its WAL.
        let unlogged = if durable && mode == Mode::Replace {
            ""
        } else {
            "UNLOGGED "
        };
        let st = &self.staging_t;
        sqlx::query(&format!("CREATE {unlogged}TABLE {st} ({cols_ddl})"))
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("create staging: {e}")))?;
        Ok(())
    }

    async fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> Result<DestState> {
        self.cursor_col = Some(cursor.to_string());
        self.source_id = Some(source_id.to_string());
        self.wm_udt = plan
            .cols
            .iter()
            .find(|c| c.name == cursor)
            .map(|c| c.udt.clone());
        let exists = self.final_exists().await?;
        // An unqualified dest name follows search_path — resolve the REAL schema so
        // the state table and its keys land next to the actual data.
        if !self.qualified {
            let resolved: String = if exists {
                sqlx::query_scalar(
                    "SELECT n.nspname FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = to_regclass($1::text)",
                )
                .bind(&self.final_t)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?
            } else {
                sqlx::query_scalar("SELECT current_schema()")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?
            };
            self.dest_key = format!("{resolved}.{}", self.bare);
            self.schema = resolved;
        }
        self.ensure_state_table().await?;
        if !exists {
            if mode == Mode::Merge {
                if plan.pk_cols.is_empty() {
                    return Err(Error::InvalidInput(
                        "merge bootstrap needs a primary key on the SOURCE table \
                         (the destination is created with the same key)"
                            .into(),
                    ));
                }
                self.bootstrap_pk = true; // prepare() adds ADD PRIMARY KEY post-swap
            }
            return Ok(DestState {
                exists: false,
                watermark: None,
            });
        }
        // Schema drift is an error, never a silent mis-append: binary COPY is
        // positional, so the column lists must match exactly.
        let dest_cols = self.final_columns().await?;
        let src_cols: Vec<String> = plan.cols.iter().map(|c| c.name.clone()).collect();
        if dest_cols != src_cols {
            return Err(Error::InvalidInput(format!(
                "destination columns {dest_cols:?} don't match the source {src_cols:?} — \
                 run once with mode='replace' to realign the schema"
            )));
        }
        if mode == Mode::Merge {
            self.merge_keys = sqlx::query_scalar(
                "SELECT a.attname FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = $1::regclass AND i.indisprimary \
                   AND array_position(i.indkey, a.attnum) < i.indnkeyatts \
                 ORDER BY array_position(i.indkey, a.attnum)",
            )
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("dest pk: {e}")))?;
            if self.merge_keys.is_empty() {
                return Err(Error::InvalidInput(
                    "merge needs a PRIMARY KEY on the destination table".into(),
                ));
            }
        }
        // An EMPTY table cannot carry a watermark, whatever the state row says —
        // TRUNCATE-to-resync must work.
        let has_rows: bool =
            sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {})", self.final_t))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("dest emptiness: {e}")))?;
        if !has_rows {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        // The state row is authoritative: it survives destination-side writes,
        // precision differences, and enables per-source watermarks (fan-in). Only a
        // missing row (pre-state-table destinations, or a fresh dest built by plain
        // replace) falls back to deriving the watermark from the data itself.
        // Read the row's whole vocabulary, not just its value. A watermark is
        // only meaningful in the terms it was written in: a `log_based` row
        // holds an LSN, and a cursor lane that adopts an LSN as a cursor value
        // starts from a position that means nothing — skipping or repeating
        // rows while reporting success. That exact switch (CDC table later run
        // with mode="append") used to do exactly that, silently.
        //
        // Both key spellings are consulted — the CDC lane historically keyed
        // by the bare name where this lane keys by schema.bare, and a guard
        // that cannot SEE the other lane's row guards nothing. See
        // `naming::pg_state_keys`.
        let (key_bare, key_qualified) = crate::naming::pg_state_keys(&self.dest_key);
        let row: Option<(Option<String>, String, String)> = sqlx::query_as(&format!(
            "SELECT watermark, cursor_col, mode FROM {} \
             WHERE dest_table IN ($1, $2) AND source_id = $3 \
             ORDER BY (dest_table = $1) DESC LIMIT 1",
            self.state_table()
        ))
        .bind(&key_qualified)
        .bind(&key_bare)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Transfer(format!("state read: {e}")))?;
        let from_state: Option<String> = match row {
            None => None,
            Some((wm, row_cursor, row_mode)) => {
                if row_mode == "log_based" {
                    return Err(Error::InvalidInput(format!(
                        "{}: this destination table is CDC-managed — its state \
                         watermark is an LSN, which a mode=\"{}\" run cannot \
                         resume from. Keep using mode=\"log_based\", or clear \
                         this table's _apitap_state rows to hand it to the \
                         cursor lane (the next run then re-bootstraps).",
                        self.dest_key,
                        match mode {
                            Mode::Merge => "merge",
                            _ => "append",
                        },
                    )));
                }
                if row_cursor != cursor {
                    return Err(Error::InvalidInput(format!(
                        "{}: the state row tracks cursor '{row_cursor}' but this \
                         run uses cursor '{cursor}' — a watermark in one \
                         column's terms cannot resume another's. Re-run with \
                         cursor=\"{row_cursor}\", or clear this table's \
                         _apitap_state rows to restart from a full load.",
                        self.dest_key,
                    )));
                }
                wm
            }
        };
        let (siblings, data_max) = match &from_state {
            Some(_) => (false, None), // authoritative: the data max is never consulted
            None => {
                let sib: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM {} WHERE dest_table = $1",
                    self.state_table()
                ))
                .bind(&self.dest_key)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("state read: {e}")))?;
                let data = if sib == 0 {
                    self.table_watermark(&self.final_t).await?
                } else {
                    None
                };
                (sib > 0, data)
            }
        };
        let watermark = crate::plan::resolve_watermark(
            from_state,
            data_max,
            siblings,
            crate::plan::WmArbitration::StateAuthoritative,
            &self.dest_key,
            source_id,
        )?;
        Ok(DestState {
            exists: true,
            watermark,
        })
    }

    async fn loader(&self) -> Result<PgCopyLoader> {
        PgCopyLoader::open(
            self.pool.clone(),
            self.copy_in_sql.clone(),
            self.overlap_send,
        )
        .await
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        // COPY … FROM STDIN reports rows itself — no count query needed.
        Ok(loaded)
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        // 0-row guard, every mode: an empty load never touches the destination.
        if rows == 0 {
            let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
                .execute(&self.pool)
                .await;
            return Ok(());
        }
        // Watermark of what THIS run staged — computed before staging is consumed,
        // recorded in the state row alongside the data it describes.
        let staged_wm = self.table_watermark(&self.staging_t).await?;
        match mode {
            Mode::Replace => {
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("swap begin: {e}")))?;
                sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.final_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop dest: {e}")))?;
                sqlx::query(&format!(
                    "ALTER TABLE {} RENAME TO {}",
                    self.staging_t,
                    quote_ident(&self.bare)
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("rename staging: {e}")))?;
                // Replace destroyed every source's rows — stale state rows would
                // make the next incremental run skip data. Clear them all; the
                // bootstrap (if any) re-inserts its own row below.
                let has_state: bool = sqlx::query_scalar("SELECT to_regclass($1::text) IS NOT NULL")
                    .bind(self.state_table())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("state lookup: {e}")))?;
                if has_state {
                    // Both spellings: the CDC lane historically keyed rows by
                    // the bare name, and a replace that deletes only its own
                    // spelling leaves the CDC watermark alive — the next
                    // log_based run then resumes from a position that predates
                    // this replace and re-applies changes the new table never
                    // saw. See `naming::pg_state_keys`.
                    let (bare, qualified) = crate::naming::pg_state_keys(&self.dest_key);
                    sqlx::query(&format!(
                        "DELETE FROM {} WHERE dest_table IN ($1, $2)",
                        self.state_table()
                    ))
                    .bind(bare)
                    .bind(qualified)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("state clear: {e}")))?;
                }
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("swap commit: {e}")))?;
                // Re-apply what the swap destroyed. Runs AFTER the commit so index
                // builds don't extend the exclusive-lock window; the data is already
                // live. A failure here is reported with the exact statement — the
                // rows are correct, only the post-DDL needs attention.
                for ddl in &self.restore_ddl {
                    sqlx::query(ddl).execute(&self.pool).await.map_err(|e| {
                        Error::Transfer(format!(
                            "rows landed, but restoring destination DDL failed: `{ddl}`: {e}"
                        ))
                    })?;
                }
                Ok(())
            }
            Mode::Append => {
                // Stage → one INSERT..SELECT in a transaction: readers see the whole
                // delta or none of it.
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("append begin: {e}")))?;
                sqlx::query(&format!(
                    "INSERT INTO {} SELECT * FROM {}",
                    self.final_t, self.staging_t
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("append insert: {e}")))?;
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                sqlx::query(&format!("DROP TABLE {}", self.staging_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop staging: {e}")))?;
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("append commit: {e}")))
            }
            // A CDC delta lands exactly like a merge for the UPSERT half; the
            // delete half and the masked-update residue are applied by the
            // log_based engine inside this same transaction (see logbased::apply).
            Mode::Merge | Mode::LogBased => {
                let keys = self
                    .merge_keys
                    .iter()
                    .map(|k| quote_ident(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let cols = self
                    .col_names
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let updates = self
                    .col_names
                    .iter()
                    .filter(|c| !self.merge_keys.contains(c))
                    .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let action = if updates.is_empty() {
                    "DO NOTHING".to_string()
                } else {
                    format!("DO UPDATE SET {updates}")
                };
                // Parallel spans share no snapshot: a row UPDATEd mid-run can land
                // in staging twice (old and new version) and ON CONFLICT refuses to
                // touch the same row twice. Dedupe deterministically — keep the
                // highest cursor value per key.
                let order = match &self.cursor_col {
                    Some(c) => format!("{keys}, {} DESC", quote_ident(c)),
                    None => keys.clone(),
                };
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("merge begin: {e}")))?;
                sqlx::query(&format!(
                    "INSERT INTO {} ({cols}) \
                     SELECT DISTINCT ON ({keys}) {cols} FROM {} ORDER BY {order} \
                     ON CONFLICT ({keys}) {action}",
                    self.final_t, self.staging_t
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("merge upsert: {e}")))?;
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                sqlx::query(&format!("DROP TABLE {}", self.staging_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop staging: {e}")))?;
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("merge commit: {e}")))
            }
        }
    }
}

type CopyIn = sqlx::postgres::PgCopyIn<sqlx::pool::PoolConnection<sqlx::Postgres>>;

/// One `COPY … FROM STDIN` stream. Serial mode owns the copier directly; overlap mode
/// puts a 2-slot channel and a sender task in front of it.
pub(crate) enum PgCopyLoader {
    Serial(CopyIn),
    Overlap {
        /// `Err(())` is the abort marker: the sender task then ABORTS the COPY so
        /// Postgres discards the partial stream (a clean close would commit it into
        /// staging — wasted WAL for data the swap will never use).
        tx: futures::channel::mpsc::Sender<std::result::Result<Vec<u8>, ()>>,
        /// Emptied chunk buffers coming back from the sender task for reuse.
        back: futures::channel::mpsc::Receiver<Vec<u8>>,
        join: tokio::task::JoinHandle<Result<u64>>,
    },
}

impl PgCopyLoader {
    async fn open(pool: PgPool, copy_in_sql: String, overlap: bool) -> Result<Self> {
        if !overlap {
            let copier = pool
                .copy_in_raw(&copy_in_sql)
                .await
                .map_err(|e| Error::Transfer(format!("COPY IN: {e}")))?;
            return Ok(Self::Serial(copier));
        }
        use futures::StreamExt;
        let (tx, mut rx) = futures::channel::mpsc::channel::<std::result::Result<Vec<u8>, ()>>(2);
        // Sized to the buffers that can be in flight (2 channel slots + 1 in the
        // worker + 1 here); a full channel just drops the buffer instead of blocking.
        let (mut back_tx, back) = futures::channel::mpsc::channel::<Vec<u8>>(4);
        let join = tokio::spawn(async move {
            let mut copier = pool
                .copy_in_raw(&copy_in_sql)
                .await
                .map_err(|e| Error::Transfer(format!("COPY IN: {e}")))?;
            while let Some(item) = rx.next().await {
                match item {
                    Ok(mut buf) => {
                        // Borrow, don't consume: sqlx memcpys into its write buffer
                        // either way, and keeping ownership lets the emptied Vec go
                        // back to the worker for reuse.
                        copier
                            .send(&buf[..])
                            .await
                            .map(|_| ())
                            .map_err(|e| Error::Transfer(format!("pg send: {e}")))?;
                        buf.clear();
                        let _ = back_tx.try_send(buf);
                    }
                    Err(()) => {
                        let _ = copier.abort("apitap: source failed").await;
                        return Err(Error::Transfer("copy aborted".into()));
                    }
                }
            }
            copier
                .finish()
                .await
                .map_err(|e| Error::Transfer(format!("pg finish: {e}")))
        });
        Ok(Self::Overlap { tx, back, join })
    }
}

impl Loader for PgCopyLoader {
    fn reclaim(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Serial(_) => None,
            Self::Overlap { back, .. } => back.try_recv().ok(),
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        match self {
            Self::Serial(copier) => copier
                .send(buf)
                .await
                .map(|_| ())
                .map_err(|e| Error::Transfer(format!("pg send: {e}"))),
            Self::Overlap { tx, join, .. } => {
                use futures::SinkExt;
                if tx.send(Ok(buf)).await.is_err() {
                    // Sender died — surface ITS error, not the closed channel.
                    return Err(match join.await {
                        Ok(Ok(_)) => Error::Transfer("pg copy closed early".into()),
                        Ok(Err(e)) => e,
                        Err(e) => Error::Transfer(format!("join: {e}")),
                    });
                }
                Ok(())
            }
        }
    }

    async fn finish(self) -> Result<u64> {
        match self {
            Self::Serial(copier) => copier
                .finish()
                .await
                .map_err(|e| Error::Transfer(format!("pg finish: {e}"))),
            Self::Overlap { tx, join, .. } => {
                drop(tx); // close the channel; the sender task then finishes the COPY
                match join.await {
                    Ok(r) => r,
                    Err(e) => Err(Error::Transfer(format!("join: {e}"))),
                }
            }
        }
    }

    async fn abort(self, cause: Error) -> Error {
        match self {
            // Dropping the copier aborts the COPY server-side; rows never persist.
            Self::Serial(copier) => {
                let _ = copier.abort("apitap: source failed").await;
            }
            Self::Overlap { mut tx, join, .. } => {
                use futures::SinkExt;
                let _ = tx.send(Err(())).await;
                drop(tx);
                let _ = join.await;
            }
        }
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_types_for_deliveries_match_the_lossless_map() {
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 2,
                unsigned: false
            }),
            "smallint"
        );
        // Unsigned widens so the value always fits (Postgres has no unsigned ints).
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 2,
                unsigned: true
            }),
            "integer"
        );
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 4,
                unsigned: true
            }),
            "bigint"
        );
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: true
            }),
            "numeric(20,0)"
        );
        assert_eq!(
            pg_type_of(&Delivered::Decimal { p: 20, s: 0 }),
            "numeric(20,0)"
        );
        assert_eq!(pg_type_of(&Delivered::Decimal { p: 0, s: 0 }), "numeric");
        assert_eq!(pg_type_of(&Delivered::Json), "jsonb");
        assert_eq!(
            pg_type_of(&Delivered::DateTime { utc: true }),
            "timestamptz(6)"
        );
        assert_eq!(pg_type_of(&Delivered::Bytes), "bytea");
    }
}
