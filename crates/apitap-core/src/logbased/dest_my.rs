//! log_based apply into MySQL — one InnoDB transaction carries the key
//! delete-join, the plain bulk insert (LOAD DATA into a TEMPORARY twin), the
//! residue, the watermark and the lease renewal, so the guarantee matches the
//! Postgres apply.
//!
//! A WAL TRUNCATE is applied O(1) under the fence: the lease row is renewed
//! while held, and the TRUNCATE's implicit commit publishes that renewal as
//! the statement begins. The temporary twins are created BEFORE the
//! transaction: with GTID consistency enforced, CREATE TEMPORARY TABLE inside
//! one is error 1787.
//!
//! Every statement that reaches a connection is in `mod store`; the apply body
//! writes through the `MyTx` it is handed.

use crate::dialect::mysql::my_ident;
use crate::error::{Error, Result};
use crate::lease::Watermark;
use crate::logbased::collapse::{Collapsed, ResidueOp};
use crate::logbased::replay::WindowId;
use crate::logbased::rowtext::{
    bytea_hex, render_my_key, render_my_row, row_key_refs, strip_utc_offset,
    BOOL_OID, BYTEA_OID, TIMESTAMPTZ_OID, TIMETZ_OID,
};
use crate::logbased::window::TableWindow;
use crate::sink::mysql::sql_lit;
use crate::wire::pgoutput::Cell;
use std::collections::HashMap;

pub(crate) use store::{MyStore, MyTx};

use crate::naming::STATE_CURSOR_LSN as STATE_CURSOR;

/// `dest_table` may arrive schema-qualified; the MySQL database comes from the
/// URL, so only the bare name addresses the table (same trim as the sink).
fn bare(dest_table: &str) -> &str {
    dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t)
}

pub(crate) struct MyDest {
    store: MyStore,
}

impl MyDest {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        Ok(Self { store: MyStore::connect(url)? })
    }

    /// The store: the lease, the guard and the units a run's tenure takes.
    pub(crate) fn store(&self) -> &MyStore {
        &self.store
    }

    /// The drain's watermark, through the one verdict both lanes share.
    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<crate::naming::CdcWatermark>> {
        crate::naming::cdc_watermark(dest_table, self.store.read_state(dest_table, source_id).await?)
    }

    /// The bootstrap's replace path may or may not have carried the PK into
    /// the created table — ensure it, inside the unit whose close writes the
    /// state row.
    pub(crate) async fn bootstrap_finish(&self, u: &mut MyTx, dest_table: &str, pk_cols: &[String]) -> Result<()> {
        let has_pk: Option<u64> = u
            .catalog_first(
                "SELECT COUNT(*) FROM information_schema.table_constraints \
                 WHERE table_schema = ? AND table_name = ? \
                   AND constraint_type = 'PRIMARY KEY'",
                bare(dest_table),
            )
            .await?;
        if has_pk.unwrap_or(0) == 0 {
            let pklist = pk_cols.iter().map(|c| my_ident(c)).collect::<Vec<_>>().join(", ");
            let ft = u.fq(bare(dest_table));
            u.owned_ddl(&format!("ALTER TABLE {ft} ADD PRIMARY KEY ({pklist})")).await?;
        }
        Ok(())
    }

    /// Apply one collapsed window through the unit (see module docs for the
    /// TRUNCATE); its close writes the watermark named here.
    pub(crate) async fn apply(
        &self,
        u: &mut MyTx,
        dest_table: &str,
        w: Option<&TableWindow<Collapsed>>,
        id: &WindowId,
        source_id: &str,
    ) -> Result<(u64, Watermark)> {
        apply_unit(u, dest_table, w, id, source_id).await
    }
}

/// Apply one collapsed window for one table through the unit, and name the
/// watermark its close writes. A window with no traffic for this table writes
/// nothing but that mark. Columns, types and keys are the window's own layout.
async fn apply_unit(
    u: &mut MyTx,
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
    let (wal_cols, oids, pk_cols, pk_idx) = (l.cols(), l.oids(), l.key_cols(), l.key_idx());
    let table = bare(dest_table);
    let ft = u.fq(table);
    let pk_oids = l.key_oids();

    // O(1) under the fence — see the module doc and `MyTx::owned_ddl`.
    if c.truncate {
        u.owned_ddl(&format!("TRUNCATE TABLE {ft}")).await?;
    }

    let clear = !c.deletes.is_empty() || !c.upserts.is_empty();
    if clear {
        // Key twin with the destination's own column types.
        let col_types: Vec<(String, String)> = u
            .catalog(
                "SELECT column_name, column_type FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ?",
                table,
            )
            .await?;
        let type_of: HashMap<&str, &str> = col_types.iter().map(|(n, t)| (n.as_str(), t.as_str())).collect();
        let ddl = pk_cols
            .iter()
            .map(|k| {
                let ty = type_of.get(k.as_str()).ok_or_else(|| {
                    Error::Transfer(format!("log_based: PK column '{k}' missing at the MySQL destination"))
                })?;
                Ok(format!("{} {ty}", my_ident(k)))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        u.temp_ddl("DROP TEMPORARY TABLE IF EXISTS _ap_del").await?;
        u.temp_ddl(&format!("CREATE TEMPORARY TABLE _ap_del ({ddl}) ENGINE=InnoDB")).await?;
    }
    if !c.upserts.is_empty() {
        u.temp_ddl("DROP TEMPORARY TABLE IF EXISTS _ap_up").await?;
        u.temp_ddl(&format!("CREATE TEMPORARY TABLE _ap_up LIKE {ft}")).await?;
    }

    if clear {
        let mut body = Vec::with_capacity(1 << 20);
        for key in c.deletes.iter() {
            let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
            render_my_key(&refs, &pk_oids, &mut body)?;
        }
        for row in &c.upserts {
            render_my_key(&row_key_refs(row, pk_idx), &pk_oids, &mut body)?;
        }
        u.load(body, |id| load_sql(id, "_ap_del", pk_cols, &pk_oids), "load keys").await?;
        u.no_warnings("key load").await?;
        let join = pk_cols
            .iter()
            .map(|k| format!("t.{k} = d.{k}", k = my_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        u.exec(&format!("DELETE t FROM {ft} t JOIN _ap_del d ON {join}"), "delete join").await?;
    }

    if !c.upserts.is_empty() {
        let mut body = Vec::with_capacity(4 << 20);
        for row in &c.upserts {
            render_my_row(row, oids, &mut body)?;
        }
        u.load(body, |id| load_sql(id, "_ap_up", wal_cols, oids), "load rows").await?;
        u.no_warnings("row load").await?;
        let collist = wal_cols.iter().map(|c| my_ident(c)).collect::<Vec<_>>().join(", ");
        // No ON DUPLICATE KEY: the delete phase already cleared every key.
        u.exec(&format!("INSERT INTO {ft} ({collist}) SELECT {collist} FROM _ap_up"), "bulk insert")
            .await?;
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
            ResidueOp::Rekey { old_key, row, .. } => {
                // Move the row rather than delete-and-reinsert: the columns
                // this UPDATE does not name keep their values, and the
                // TOASTed cell the source did not resend is one of them.
                // The PK columns ARE included, unlike MaskedUpdate — moving
                // the key is the point.
                let sets = wal_cols
                    .iter()
                    .zip(row.iter().zip(oids.iter()))
                    .filter(|(_, (cell, _))| !matches!(cell, Cell::UnchangedToast))
                    .map(|(cname, (cell, &oid))| {
                        Ok(format!("{} = {}", my_ident(cname), my_literal(cell, oid)?))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                if sets.is_empty() {
                    // Unreachable: the key changed, so a PK column carries
                    // a real value.
                    continue;
                }
                // Idempotent on replay: after the move the old key is gone,
                // so a re-applied window matches nothing.
                format!(
                    "UPDATE {ft} SET {sets} WHERE {}",
                    key_pred(pk_cols, old_key, &pk_oids)?
                )
            }
        };
        u.exec(&sql, "residue").await?;
    }

    Ok((c.events, set(c.events)))
}

/// Everything that holds a connection. See the module doc.
mod store {
    use super::{bare, state_upsert_sql};
    use crate::dialect::mysql::my_ident;
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{
        no_longer_holds, owned_margin_secs, owner_verdict, ttl_secs, Fence, Lease, LeaseStore, Watermark,
    };
    use crate::sink::mysql::{lease_t, sql_lit, MyGuard, MySqlShared, MySqlSink};
    use mysql_async::prelude::Queryable;

    pub(crate) struct MyStore {
        shared: MySqlShared,
        state_ready: std::sync::atomic::AtomicBool,
    }

    /// One fenced unit on one connection. No transaction until the first
    /// write (`begin`); what runs before it is catalog reads, the temporary
    /// twins and the owned DDL. Dropped without `close_unit`, it rolls back.
    pub(crate) struct MyTx {
        conn: Option<mysql_async::Conn>,
        shared: MySqlShared,
        keys: Vec<String>,
        token: String,
        in_tx: bool,
        twins: bool,
    }

    fn my_err(what: &'static str) -> impl Fn(mysql_async::Error) -> Error {
        move |e| Error::Transfer(format!("log_based: mysql {what}: {e}"))
    }

    /// One step of `MyTx::owned_ddl`. The plan is pure so its order — which
    /// IS the argument for why the DDL is safe — is testable.
    #[derive(Debug)]
    pub(super) enum Step {
        /// Run it.
        Run(String),
        /// Run it; the key's row must come back (it is the owner's).
        Lock(String),
        /// Run it; the row must come back with `collected = 0`.
        Check(String),
        /// The DDL itself. Its implicit commit ends the transaction.
        Ddl(String),
    }

    // What the order test reads; the executor matches on the variant itself,
    // so outside the tests this accessor would be dead code.
    #[cfg(test)]
    impl Step {
        pub(super) fn sql(&self) -> &str {
            match self {
                Step::Run(s) | Step::Lock(s) | Step::Check(s) | Step::Ddl(s) => s,
            }
        }
    }

    pub(super) fn my_owned_ddl_plan(
        lease: &str,
        keys: &[String],
        token: &str,
        ttl: u64,
        margin: u64,
        sql: &str,
    ) -> Vec<Step> {
        let who = |k: &str| format!("dest_key = '{}' AND token = '{}'", sql_lit(k), sql_lit(token));
        let mut sorted: Vec<&String> = keys.iter().collect();
        sorted.sort();
        let mut v = vec![Step::Run("START TRANSACTION".into())];
        for k in &sorted {
            v.push(Step::Lock(format!("SELECT 1 FROM {lease} WHERE {} AND collected = 0 FOR UPDATE", who(k))));
        }
        for k in &sorted {
            v.push(Step::Run(format!(
                "UPDATE {lease} SET expires_at = UTC_TIMESTAMP(6) + INTERVAL {ttl} SECOND \
                 WHERE {} AND collected = 0",
                who(k)
            )));
            v.push(Step::Check(format!("SELECT collected FROM {lease} WHERE {}", who(k))));
        }
        v.push(Step::Run(format!("SET SESSION lock_wait_timeout = {margin}")));
        v.push(Step::Ddl(sql.to_string()));
        v.push(Step::Run("SET SESSION lock_wait_timeout = DEFAULT".into()));
        v
    }

    impl MyTx {
        fn conn(&mut self) -> &mut mysql_async::Conn {
            self.conn.as_mut().expect("unit connection")
        }

        pub(crate) fn fq(&self, table: &str) -> String {
            format!("{}.{}", my_ident(self.shared.db()), my_ident(table))
        }

        /// A catalog read about `table` in this database, outside any
        /// transaction.
        pub(crate) async fn catalog<T>(&mut self, sql: &str, table: &str) -> Result<Vec<T>>
        where
            T: mysql_async::prelude::FromRow + Send + 'static,
        {
            let db = self.shared.db().to_string();
            self.conn().exec(sql, (db, table)).await.map_err(my_err("catalog"))
        }

        pub(crate) async fn catalog_first<T>(&mut self, sql: &str, table: &str) -> Result<Option<T>>
        where
            T: mysql_async::prelude::FromRow + Send + 'static,
        {
            let db = self.shared.db().to_string();
            self.conn().exec_first(sql, (db, table)).await.map_err(my_err("catalog"))
        }

        /// `CREATE/DROP TEMPORARY TABLE` only, and only before the
        /// transaction: inside one, GTID consistency refuses it (1787).
        pub(crate) async fn temp_ddl(&mut self, sql: &str) -> Result<()> {
            let s = sql.trim_start().to_ascii_uppercase();
            if !(s.starts_with("CREATE TEMPORARY TABLE") || s.starts_with("DROP TEMPORARY TABLE")) {
                return Err(Error::Transfer(format!("internal: temp_ddl given {sql}")));
            }
            if self.in_tx {
                return Err(Error::Transfer("internal: temporary DDL inside the unit's transaction".into()));
            }
            self.twins = true;
            self.conn().query_drop(sql).await.map_err(my_err("temp ddl"))
        }

        /// Open the transaction and take every lease row `FOR UPDATE` as its
        /// first statements. Idempotent. No row, or a collected one, is not
        /// an owner: 0.56.0 read "no lease" as "nothing to fence" and wrote.
        async fn begin(&mut self) -> Result<()> {
            if self.in_tx {
                return Ok(());
            }
            let lease = lease_t(self.shared.db());
            let (keys, token) = (self.keys.clone(), self.token.clone());
            self.conn().query_drop("START TRANSACTION").await.map_err(my_err("begin"))?;
            self.in_tx = true;
            let mut sorted = keys.clone();
            sorted.sort();
            for k in &sorted {
                let held: Option<(i32,)> = match self
                    .conn()
                    .exec_first(
                        format!("SELECT 1 FROM {lease} WHERE dest_key = ? AND token = ? AND collected = 0 FOR UPDATE"),
                        (k, &token),
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(mysql_async::Error::Server(e)) if e.code == 1146 => None,
                    Err(e) => return Err(Error::Transfer(format!("log_based: fence: {e}"))),
                };
                if held.is_none() {
                    return Err(no_longer_holds(&keys));
                }
            }
            Ok(())
        }

        /// A write inside the fenced transaction.
        pub(crate) async fn exec(&mut self, sql: &str, what: &'static str) -> Result<()> {
            self.begin().await?;
            self.conn().query_drop(sql).await.map_err(my_err(what))
        }

        /// LOAD DATA LOCAL of `body` inside the fenced transaction; `sql_of`
        /// spells the statement for the registered infile id.
        pub(crate) async fn load(
            &mut self,
            body: Vec<u8>,
            sql_of: impl FnOnce(u64) -> String,
            what: &'static str,
        ) -> Result<()> {
            self.begin().await?;
            let id = self.shared.register_infile(body);
            let sql = sql_of(id);
            if let Err(e) = self.conn().query_drop(&sql).await {
                self.shared.forget_infile(id);
                return Err(my_err(what)(e));
            }
            Ok(())
        }

        /// Fail if the statement just run raised warnings.
        ///
        /// `LOAD DATA LOCAL INFILE` is the exception to strict mode, by design
        /// and by documentation: with LOCAL the server cannot stop the client
        /// mid-file, so it behaves "as if IGNORE were specified" and downgrades
        /// every data error to a warning — including "Data too long for
        /// column". So a CDC apply into a destination column narrower than the
        /// value can TRUNCATE and still report success. Reading the warning
        /// count costs one round-trip per load and turns that back into an
        /// error, quoting what the server said.
        pub(crate) async fn no_warnings(&mut self, what: &str) -> Result<()> {
            let n: Option<u32> = self
                .conn()
                .query_first("SELECT @@warning_count")
                .await
                .map_err(|e| Error::Transfer(format!("log_based: mysql warning probe: {e}")))?;
            if n.unwrap_or(0) == 0 {
                return Ok(());
            }
            let rows: Vec<(String, u32, String)> = self
                .conn()
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

        /// DDL that writes the user's table — `TRUNCATE`, `ADD PRIMARY KEY` —
        /// under the fence, and still O(1).
        ///
        /// DDL implicitly commits, so it cannot ride the transaction. Instead:
        /// take every lease row, RENEW it, read it back uncollected, then run
        /// the DDL, whose implicit commit publishes the renewal the instant
        /// the statement begins. A collector needs the row lapsed, and it was
        /// just given a full TTL; the DDL's own wait for its metadata lock is
        /// bounded to half of that. 0.56.0 ran the TRUNCATE before the fence,
        /// so an evicted drain could empty a table its collector had already
        /// refilled; a `DELETE FROM` instead would be one huge transaction
        /// (1206 on a small buffer pool, a wedge on Group Replication and
        /// Galera).
        pub(crate) async fn owned_ddl(&mut self, sql: &str) -> Result<()> {
            if self.in_tx {
                return Err(Error::Transfer("internal: owned DDL inside the unit's transaction".into()));
            }
            let plan = my_owned_ddl_plan(
                &lease_t(self.shared.db()),
                &self.keys,
                &self.token,
                ttl_secs(),
                owned_margin_secs(),
                sql,
            );
            for step in &plan {
                match step {
                    Step::Run(s) => {
                        self.conn().query_drop(s.as_str()).await.map_err(my_err("owned ddl"))?;
                        if s == "START TRANSACTION" {
                            self.in_tx = true;
                        }
                    }
                    Step::Lock(s) | Step::Check(s) => {
                        let v: Option<(i32,)> = match self.conn().query_first(s.as_str()).await {
                            Ok(r) => r,
                            Err(mysql_async::Error::Server(e)) if e.code == 1146 => None,
                            Err(e) => return Err(Error::Transfer(format!("log_based: fence: {e}"))),
                        };
                        let owner = match (step, v) {
                            (Step::Lock(_), Some(_)) => true,
                            (Step::Check(_), Some((c,))) => c == 0,
                            _ => false,
                        };
                        if !owner {
                            return Err(no_longer_holds(&self.keys));
                        }
                    }
                    Step::Ddl(s) => {
                        // The implicit commit happens here, whatever the outcome.
                        self.in_tx = false;
                        self.conn().query_drop(s.as_str()).await.map_err(my_err("owned ddl"))?;
                    }
                }
            }
            Ok(())
        }
    }

    impl Drop for MyTx {
        fn drop(&mut self) {
            let Some(mut conn) = self.conn.take() else { return };
            let (in_tx, twins) = (self.in_tx, self.twins);
            if !(in_tx || twins) {
                return;
            }
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                h.spawn(async move {
                    if in_tx {
                        let _ = conn.query_drop("ROLLBACK").await;
                    }
                    if twins {
                        let _ = conn.query_drop("DROP TEMPORARY TABLE IF EXISTS _ap_del, _ap_up").await;
                    }
                });
            }
        }
    }

    impl MyStore {
        pub(crate) fn connect(url: &str) -> Result<Self> {
            Ok(Self { shared: MySqlSink::shared_pool(url)?, state_ready: Default::default() })
        }

        pub(crate) fn my_guard(&self) -> MyGuard {
            MyGuard::new(self.shared.clone())
        }

        fn fq(&self, table: &str) -> String {
            format!("{}.{}", my_ident(self.shared.db()), my_ident(table))
        }

        /// Created outside any unit: CREATE TABLE implicitly commits.
        async fn ensure_state_table(&self, conn: &mut mysql_async::Conn) -> Result<()> {
            if self.state_ready.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
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
            .map_err(|e| Error::Transfer(format!("log_based: mysql state ddl: {e}")))?;
            self.state_ready.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        /// This table's state row, whichever lane wrote it.
        ///
        /// Both spellings: the bulk lane keys the table bare (the database is
        /// the URL's), this lane keys it as it was handed, which may be
        /// `db.table`; the drain's own spelling ranks first. And NO predicate
        /// on mode: until 0.57.0 this read said `AND mode = 'log_based'`, so a
        /// table an `append` had built showed no row, looked fresh, and was
        /// re-bootstrapped over — its rows and its cursor state with it.
        pub(crate) async fn read_state(
            &self,
            dest_table: &str,
            source_id: &str,
        ) -> Result<Option<crate::naming::StateRow>> {
            let mut conn = self.shared.conn().await?;
            let row: Option<(Option<String>, Option<String>, Option<String>)> = match conn
                .exec_first(
                    format!(
                        "SELECT watermark, cursor_col, mode FROM {} \
                         WHERE dest_table IN (?, ?) AND source_id = ? \
                         ORDER BY (dest_table = ?) DESC LIMIT 1",
                        self.fq("_apitap_state")
                    ),
                    (bare(dest_table), dest_table, source_id, dest_table),
                )
                .await
            {
                Ok(r) => r,
                // No state table at all = fresh destination.
                Err(mysql_async::Error::Server(e)) if e.code == 1146 => return Ok(None),
                Err(e) => return Err(Error::Transfer(format!("log_based: mysql state: {e}"))),
            };
            Ok(row.map(|(wm, cursor, mode)| crate::naming::StateRow::new(wm, cursor, mode)))
        }
    }

    impl LeaseStore for MyStore {
        fn lease_key(&self, dest_table: &str) -> String {
            format!("{}.{}", self.shared.db(), bare(dest_table))
        }

        async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
            crate::sink::mysql::lease_open(self.shared.pool(), self.shared.db(), keys, token).await
        }

        async fn lease_renew(&self, keys: &[String], token: &str) -> Result<u64> {
            crate::sink::mysql::lease_renew(self.shared.pool(), self.shared.db(), keys, token).await
        }

        async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
            crate::sink::mysql::lease_unclaimed(self.shared.pool(), self.shared.db(), token).await
        }

        async fn close_run(&self, _token: &str) {}
    }

    impl Fence for MyStore {
        type Unit<'a> = MyTx;

        fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
            (Box::new(self.my_guard()), bare(dest_table).to_string())
        }

        /// A connection with the apply session, and the state table. No
        /// transaction yet — see `MyTx`.
        async fn open_unit<'a>(&'a self, keys: &[String], token: &str) -> Result<MyTx> {
            let mut conn = self.shared.conn().await?;
            self.ensure_state_table(&mut conn).await?;
            // The bulk loader writes into a staging table it created itself,
            // so it can afford a relaxed session. This one writes into the
            // USER's table: `sql_mode=''` would TRUNCATE an over-long value
            // with a warning nobody reads, and `unique_checks=0` would let a
            // duplicate land on a table with unique keys. STRICT alone does
            // not add NO_ZERO_DATE, so a MySQL source's '0000-00-00' still
            // applies. `foreign_key_checks=0` stays: a window applies each
            // table independently, and enforcing FKs would refuse an order
            // that is valid once the whole window has landed. The lock wait is
            // stated, not inherited: the lease calls on this pool set it to 1.
            conn.query_drop(
                "SET time_zone='+00:00', foreign_key_checks=0, sql_mode='STRICT_ALL_TABLES', \
                 innodb_lock_wait_timeout = 50",
            )
            .await
            .map_err(my_err("session"))?;
            Ok(MyTx {
                conn: Some(conn),
                shared: self.shared.clone(),
                keys: keys.to_vec(),
                token: token.to_string(),
                in_tx: false,
                twins: false,
            })
        }

        /// Every mark, then per key the renewal and a READ of the row, judged
        /// by `owner_verdict` — never by an affected-row count — then COMMIT,
        /// then the twins go.
        async fn close_unit<'a>(&'a self, mut u: MyTx, _token: &str, marks: Vec<Watermark>) -> Result<()> {
            u.begin().await?;
            let state = self.fq("_apitap_state");
            for m in &marks {
                let sql = match m {
                    Watermark::Set { table, source_id, lsn, rows } => {
                        state_upsert_sql(&state, table, source_id, *lsn, *rows)
                    }
                    Watermark::Clear { table, source_id } => format!(
                        "DELETE FROM {state} WHERE dest_table = '{}' AND source_id = '{}'",
                        sql_lit(table),
                        sql_lit(source_id)
                    ),
                };
                u.conn().query_drop(sql).await.map_err(my_err("state write"))?;
            }
            let lease = lease_t(self.shared.db());
            let (keys, token) = (u.keys.clone(), u.token.clone());
            for k in &keys {
                u.conn()
                    .exec_drop(
                        format!(
                            "UPDATE {lease} SET expires_at = UTC_TIMESTAMP(6) + INTERVAL ? SECOND \
                             WHERE dest_key = ? AND token = ? AND collected = 0"
                        ),
                        (ttl_secs(), k, &token),
                    )
                    .await
                    .map_err(my_err("lease renew"))?;
                let row: Option<(i32, i64)> = u
                    .conn()
                    .exec_first(
                        format!(
                            "SELECT collected, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), expires_at) \
                             FROM {lease} WHERE dest_key = ? AND token = ?"
                        ),
                        (k, &token),
                    )
                    .await
                    .map_err(my_err("lease read"))?;
                owner_verdict(row.map(|(c, e)| Lease { expires_in: e, collected: c != 0 }).as_ref(), &keys)?;
            }
            u.conn().query_drop("COMMIT").await.map_err(my_err("commit"))?;
            u.in_tx = false;
            if u.twins {
                // The pooled connection outlives this unit — don't leak the twins.
                u.conn()
                    .query_drop("DROP TEMPORARY TABLE IF EXISTS _ap_del, _ap_up")
                    .await
                    .map_err(my_err("temp cleanup"))?;
                u.twins = false;
            }
            Ok(())
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::GuardStore;

    /// The drain's lease key and the guard's refusal name are ONE string, or a
    /// collector reads a lease row the drain never writes.
    #[test]
    fn lease_key_is_dest_label() {
        use crate::lease::LeaseStore;
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let d = MyStore::connect("mysql://root:x@127.0.0.1:1/bench").unwrap();
            let g = d.my_guard();
            for t in ["orders", "bench.orders", "Mixed Case"] {
                assert_eq!(d.lease_key(t), g.dest_label(bare(t)), "{t}");
            }
        });
    }

    /// The order IS the argument: the row is locked, renewed and read back
    /// before the DDL's implicit commit publishes the renewal, and the DDL's
    /// metadata-lock wait is bounded before it runs.
    #[test]
    fn owned_ddl_renews_before_ddl() {
        use store::Step;
        let keys = vec!["bench.t".to_string()];
        let plan = store::my_owned_ddl_plan("`bench`.`_apitap_lease`", &keys, "_tok", 30, 15, "TRUNCATE TABLE `bench`.`t`");
        let at = |p: &str| plan.iter().position(|s| s.sql().starts_with(p)).unwrap_or_else(|| panic!("{p}: {plan:?}"));
        assert_eq!(at("START TRANSACTION"), 0);
        assert!(matches!(&plan[1], Step::Lock(s) if s.contains("FOR UPDATE")), "{plan:?}");
        assert!(at("SELECT 1") < at("UPDATE"), "{plan:?}");
        assert!(at("UPDATE") < at("SELECT collected"), "{plan:?}");
        assert!(plan[at("UPDATE")].sql().contains("expires_at") && plan[at("UPDATE")].sql().contains("collected = 0"));
        assert!(matches!(&plan[at("SELECT collected")], Step::Check(_)));
        assert!(at("SELECT collected") < at("SET SESSION lock_wait_timeout = 15"), "{plan:?}");
        assert!(at("SET SESSION lock_wait_timeout = 15") < at("TRUNCATE"), "{plan:?}");
        assert!(matches!(&plan[at("TRUNCATE")], Step::Ddl(_)));
        assert_eq!(plan.last().unwrap().sql(), "SET SESSION lock_wait_timeout = DEFAULT");
    }
}
