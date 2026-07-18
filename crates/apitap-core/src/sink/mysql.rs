//! MySQL destination: bulk-load via `LOAD DATA LOCAL INFILE`, staged + swapped.
//!
//! MySQL has no COPY, and `LOAD DATA LOCAL INFILE` is its only true bulk path —
//! an order of magnitude past row `INSERT`. sqlx can't drive it (it errors on the
//! server's infile request), so the sink uses `mysql_async`, whose LOCAL INFILE
//! handler takes a `Stream<io::Result<Bytes>>`: each worker feeds one streaming
//! `LOAD DATA` off a channel, exactly like the Postgres COPY loader.
//!
//! The lane is the MySQL text lane (see [`crate::source::mysql`] `MyEnc::Tsv`): the source
//! renders every value server-side (`CAST … AS CHAR`, `HEX` for binary), so the
//! bytes round-trip exactly on reload (same engine) and the sink only has to
//! `UNHEX` the binary columns back. Fields are tab-separated, `\N` is NULL,
//! escaping is MySQL's `FIELDS ESCAPED BY '\\'` dialect.
//!
//! Staging mirrors the source column types (`native_ddl`) + primary key; finalize
//! swaps it in atomically (`RENAME TABLE`) for replace, or `INSERT … SELECT` for
//! append. Incremental state lives in `_apitap_state`, upserted in the same
//! statement family as the other sinks.

use crate::sink::Loader;
use crate::dialect::mysql::{is_binary_udt, my_ident};
use crate::error::{Error, Result};
use crate::plan::{Delivered, DestState, Lane, TablePlan, WireFormat};
use crate::Mode;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::SinkExt;
use mysql_async::prelude::Queryable;
use mysql_async::InfileData;
use mysql_async::{Opts, OptsBuilder, Pool};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type Registry = Arc<Mutex<HashMap<u64, mpsc::Receiver<std::io::Result<Bytes>>>>>;


/// Escape a string as a MySQL SQL single-quoted literal body (no surrounding
/// quotes). Only used for the tiny state-row writes, never the bulk path.
fn sql_lit(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

pub(crate) struct MySqlSink {
    pool: Pool,
    registry: Registry,
    /// Shared with every sink on this pool: LOAD DATA stream ids must be unique
    /// POOL-wide — two sinks with their own counters would both hand out id 0 and
    /// collide in the registry.
    next_id: Arc<AtomicU64>,
    db: String,
    bare: String,
    staging: String,
    /// (column name, is_binary) for the LOAD DATA column list.
    cols: Vec<(String, bool)>,
    /// Incremental context, set in dest_state.
    source_id: Option<String>,
    cursor_col: Option<String>,
    mode_str: &'static str,
}

/// The per-destination resources a multi-table run builds ONCE: the pool (whose
/// global INFILE handler owns the registry), the registry itself, the pool-wide
/// stream-id counter, and the URL's database.
#[derive(Clone)]
pub(crate) struct MySqlShared {
    pool: Pool,
    registry: Registry,
    next_id: Arc<AtomicU64>,
    db: String,
}

impl MySqlSink {
    pub(crate) async fn connect(url: &str, dest_table: &str) -> Result<Self> {
        Ok(Self::bind(Self::shared_pool(url)?, dest_table))
    }

    /// Pool + INFILE registry + id counter, built once per destination.
    pub(crate) fn shared_pool(url: &str) -> Result<MySqlShared> {
        let opts =
            Opts::from_url(url).map_err(|e| Error::InvalidInput(format!("mysql url: {e}")))?;
        let db = opts
            .db_name()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidInput("mysql url needs a database".into()))?
            .to_string();

        // One global LOCAL INFILE handler for the pool: each loader registers its
        // receiver under a unique id and names it in the LOAD DATA filename, so the
        // handler can hand the right stream back. (A per-connection handler would
        // need the future itself to be Sync, which an mpsc receiver isn't.)
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let reg = registry.clone();
        let builder = OptsBuilder::from_opts(opts)
            // Every pooled conn runs UTC: the DATETIME text the source emits is
            // wall-as-UTC, and the incremental watermark MAX(ts) must be read in
            // the same frame or a TIMESTAMP cursor skips/duplicates rows.
            .setup(vec!["SET time_zone = '+00:00'".to_string()])
            .local_infile_handler(Some(
                move |file_name: &[u8]| -> futures::future::BoxFuture<
                    'static,
                    std::result::Result<InfileData, mysql_async::LocalInfileError>,
                > {
                    let id: u64 = std::str::from_utf8(file_name)
                        .ok()
                        .and_then(|s| s.strip_prefix("apitap:"))
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(u64::MAX);
                    let rx = reg.lock().expect("infile registry").remove(&id);
                    Box::pin(async move {
                        match rx {
                            Some(rx) => Ok(Box::pin(rx) as InfileData),
                            None => Err(mysql_async::LocalInfileError::OtherError(
                                format!("apitap: no infile stream for id {id}").into(),
                            )),
                        }
                    })
                },
            ));
        Ok(MySqlShared {
            pool: Pool::new(builder),
            registry,
            next_id: Arc::new(AtomicU64::new(0)),
            db,
        })
    }

    /// Bind one destination table onto the shared resources.
    pub(crate) fn bind(shared: MySqlShared, dest_table: &str) -> Self {
        let bare = dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t);
        Self {
            pool: shared.pool,
            registry: shared.registry,
            next_id: shared.next_id,
            db: shared.db,
            staging: format!("{bare}__apitap_staging"),
            bare: bare.to_string(),
            cols: Vec::new(),
            source_id: None,
            cursor_col: None,
            mode_str: "replace",
        }
    }

    fn fq(&self, table: &str) -> String {
        format!("{}.{}", my_ident(&self.db), my_ident(table))
    }

    async fn exec(&self, sql: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| Error::Transfer(format!("mysql conn: {e}")))?;
        conn.query_drop(sql)
            .await
            .map_err(|e| Error::Transfer(format!("mysql exec [{sql}]: {e}")))
    }

    async fn scalar(&self, sql: &str) -> Result<Option<String>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| Error::Transfer(format!("mysql conn: {e}")))?;
        let v: Option<Option<String>> = conn
            .query_first(sql)
            .await
            .map_err(|e| Error::Transfer(format!("mysql query [{sql}]: {e}")))?;
        Ok(v.flatten())
    }

    async fn ensure_state_table(&self) -> Result<()> {
        self.exec(&format!(
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
    }

    async fn write_state(&self, watermark: &str, rows: u64) -> Result<()> {
        let (Some(source_id), Some(cursor)) = (&self.source_id, &self.cursor_col) else {
            return Ok(());
        };
        self.ensure_state_table().await?;
        self.exec(&format!(
            "INSERT INTO {state} \
               (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ('{dt}','{sid}','{cur}','{wm}','{md}',{rows},UTC_TIMESTAMP(6)) \
             ON DUPLICATE KEY UPDATE cursor_col=VALUES(cursor_col), \
               watermark=VALUES(watermark), mode=VALUES(mode), \
               last_rows=VALUES(last_rows), synced_at=VALUES(synced_at)",
            state = self.fq("_apitap_state"),
            dt = sql_lit(&self.bare),
            sid = sql_lit(source_id),
            cur = sql_lit(cursor),
            wm = sql_lit(watermark),
            md = self.mode_str,
        ))
        .await
    }
}

/// MySQL column type for a lane delivery when the source has no MySQL DDL to
/// mirror (a non-MySQL source feeding this sink). Only the deliveries a wired
/// route actually produces are mapped; anything new fails loudly here rather
/// than guessing a lossy type.
fn my_type_of(d: &Delivered, col: &str) -> Result<String> {
    Ok(match d {
        Delivered::Int { bytes, unsigned } => {
            let base = match bytes {
                1 => "TINYINT",
                2 => "SMALLINT",
                4 => "INT",
                _ => "BIGINT",
            };
            if *unsigned {
                format!("{base} UNSIGNED")
            } else {
                base.to_string()
            }
        }
        Delivered::Bool => "TINYINT(1)".into(),
        Delivered::Float32 => "FLOAT".into(),
        Delivered::Float64 => "DOUBLE".into(),
        Delivered::Decimal { p, s } if *p > 0 => format!("DECIMAL({p},{s})"),
        Delivered::Date => "DATE".into(),
        // Sessions run UTC (pool setup) — wall-as-UTC round-trips exactly.
        Delivered::DateTime { .. } => "DATETIME(6)".into(),
        Delivered::Json => "JSON".into(),
        Delivered::Uuid => "CHAR(36)".into(),
        // MEDIUMTEXT, not TEXT: TEXT caps at 65,535 BYTES and the loader session
        // runs with sql_mode='' where an over-long value TRUNCATES with only a
        // warning. MEDIUMTEXT (16MB) costs the same storage.
        Delivered::Text => "MEDIUMTEXT".into(),
        other => {
            return Err(Error::Transfer(format!(
                "mysql sink: no type mapping for column {col} delivered as {other:?} \
                 from a non-mysql source — open an issue"
            )))
        }
    })
}

impl crate::sink::Sink for MySqlSink {
    type Loader = MySqlLoader;

    fn accepts(&self) -> &[WireFormat] {
        &[WireFormat::MyTsv]
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        _mode: Mode,
    ) -> Result<()> {
        // Same-engine transfers mirror the source column types verbatim
        // (`native_ddl`) so a swap yields a table identical to the source's;
        // anything else maps the lane's deliveries — same rule as the Postgres
        // sink.
        let mut ddl = Vec::new();
        for (c, lc) in plan.cols.iter().zip(lane.cols.iter()) {
            let ty = match (&c.native_ddl, plan.engine) {
                (Some(native), "mysql") => native.clone(),
                _ => my_type_of(&lc.delivered, &c.name)?,
            };
            // A PK column must be NOT NULL — MySQL refuses a nullable key
            // part outright (Postgres implies NOT NULL; here it's explicit).
            let null = if c.nullable && !plan.pk_cols.contains(&c.name) {
                "NULL"
            } else {
                "NOT NULL"
            };
            ddl.push(format!("{} {ty} {null}", my_ident(&c.name)));
        }
        if !plan.pk_cols.is_empty() {
            let pk: Vec<String> = plan.pk_cols.iter().map(|c| my_ident(c)).collect();
            ddl.push(format!("PRIMARY KEY ({})", pk.join(", ")));
        }
        self.exec(&format!("DROP TABLE IF EXISTS {}", self.fq(&self.staging)))
            .await?;
        // A crash between the two RENAMEs in finalize can strand `<bare>__apitap_old`;
        // clear it so the next replace's RENAME can't collide (error 1050).
        self.exec(&format!(
            "DROP TABLE IF EXISTS {}",
            self.fq(&format!("{}__apitap_old", self.bare))
        ))
        .await?;
        self.exec(&format!(
            "CREATE TABLE {} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            self.fq(&self.staging),
            ddl.join(", ")
        ))
        .await?;
        self.cols = plan
            .cols
            .iter()
            .map(|c| (c.name.clone(), is_binary_udt(&c.udt)))
            .collect();
        Ok(())
    }

    async fn loader(&self) -> Result<MySqlLoader> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // 2-slot channel: a full channel backpressures the source worker while the
        // server digests the load — memory stays bounded.
        let (tx, rx) = mpsc::channel::<std::io::Result<Bytes>>(2);
        self.registry
            .lock()
            .expect("infile registry")
            .insert(id, rx);

        // Column list: binary columns arrive HEX-encoded and are UNHEXed back.
        let mut names = Vec::new();
        let mut sets = Vec::new();
        for (name, is_bin) in &self.cols {
            if *is_bin {
                let var = format!("@{name}");
                names.push(var.clone());
                sets.push(format!("{} = UNHEX({var})", my_ident(name)));
            } else {
                names.push(my_ident(name));
            }
        }
        let set_clause = if sets.is_empty() {
            String::new()
        } else {
            format!(" SET {}", sets.join(", "))
        };
        let load_sql = format!(
            "LOAD DATA LOCAL INFILE 'apitap:{id}' INTO TABLE {staging} \
             CHARACTER SET utf8mb4 \
             FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' \
             LINES TERMINATED BY '\\n' ({cols}){set_clause}",
            staging = self.fq(&self.staging),
            cols = names.join(", "),
        );

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| Error::Transfer(format!("mysql loader conn: {e}")))?;
        // UTC + relaxed integrity for the bulk load (staging carries no FKs anyway).
        conn.query_drop(
            "SET time_zone='+00:00', unique_checks=0, foreign_key_checks=0, sql_mode=''",
        )
        .await
        .map_err(|e| Error::Transfer(format!("mysql loader session: {e}")))?;

        let join = tokio::spawn(async move {
            conn.query_drop(&load_sql)
                .await
                .map_err(|e| Error::Transfer(format!("LOAD DATA: {e}")))?;
            Ok(conn.affected_rows())
        });
        Ok(MySqlLoader {
            tx,
            join,
            id,
            registry: self.registry.clone(),
        })
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        // LOAD DATA's affected_rows is the loaded count, summed across workers.
        Ok(loaded)
    }

    async fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> Result<DestState> {
        if mode == Mode::Merge {
            return Err(Error::InvalidInput(
                "merge is not supported for MySQL destinations yet — use append".into(),
            ));
        }
        self.source_id = Some(source_id.to_string());
        self.cursor_col = Some(cursor.to_string());
        self.mode_str = "append";
        let exists = self
            .scalar(&format!(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema='{}' AND table_name='{}'",
                sql_lit(&self.db),
                sql_lit(&self.bare)
            ))
            .await?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
            > 0;
        if !exists {
            return Ok(DestState {
                exists: false,
                watermark: None,
            });
        }
        // Column drift: names AND types must match. A narrowed dest column
        // (e.g. VARCHAR(500)→VARCHAR(20)) would silently truncate under sql_mode=''.
        let dest_cols: Vec<(String, String)> = {
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| Error::Transfer(format!("mysql conn: {e}")))?;
            conn.query_map(
                format!(
                    "SELECT column_name, column_type FROM information_schema.columns \
                     WHERE table_schema='{}' AND table_name='{}' ORDER BY ordinal_position",
                    sql_lit(&self.db),
                    sql_lit(&self.bare)
                ),
                |(n, t): (String, String)| (n, t),
            )
            .await
            .map_err(|e| Error::Transfer(format!("mysql columns: {e}")))?
        };
        // Compare against the source's own COLUMN_TYPE (native_ddl minus any folded
        // CHARACTER SET/COLLATE suffix — the dest reports its effective type).
        let src_cols: Vec<(String, String)> = plan
            .cols
            .iter()
            .map(|c| {
                let ty = c
                    .native_ddl
                    .as_deref()
                    .unwrap_or(&c.udt)
                    .split(" CHARACTER SET ")
                    .next()
                    .unwrap_or(&c.udt)
                    .to_string();
                (c.name.clone(), ty)
            })
            .collect();
        let dest_names: Vec<&String> = dest_cols.iter().map(|(n, _)| n).collect();
        let src_names: Vec<&String> = src_cols.iter().map(|(n, _)| n).collect();
        if dest_names != src_names {
            return Err(Error::InvalidInput(format!(
                "destination columns {dest_names:?} don't match the source \
                 {src_names:?} — run once with mode='replace' to realign"
            )));
        }
        for ((dn, dt), (_, st)) in dest_cols.iter().zip(src_cols.iter()) {
            if !dt.eq_ignore_ascii_case(st) {
                return Err(Error::InvalidInput(format!(
                    "destination column {dn} is {dt} but the source delivers {st} — \
                     run once with mode='replace' to realign the schema"
                )));
            }
        }
        // Empty table carries no watermark (TRUNCATE-to-resync must work).
        let n = self
            .scalar(&format!("SELECT COUNT(*) FROM {}", self.fq(&self.bare)))
            .await?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        if n == 0 {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        let data_wm = self
            .scalar(&format!(
                "SELECT CAST(MAX({}) AS CHAR) FROM {}",
                my_ident(cursor),
                self.fq(&self.bare)
            ))
            .await?;
        // The state table legitimately doesn't exist on the first incremental run —
        // and then no sibling can hold a row either. Every OTHER error must surface:
        // swallowing a query failure here silently degrades to the data max, which
        // is exactly the fan-in skip the guard below refuses.
        let has_state_table = self
            .scalar(&format!(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = '{}' AND table_name = '_apitap_state'",
                sql_lit(&self.db)
            ))
            .await?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
            > 0;
        let state_wm = if has_state_table {
            self.scalar(&format!(
                "SELECT watermark FROM {} WHERE dest_table='{}' AND source_id='{}'",
                self.fq("_apitap_state"),
                sql_lit(&self.bare),
                sql_lit(source_id)
            ))
            .await?
        } else {
            None
        };
        let siblings = if state_wm.is_none() && has_state_table {
            self.scalar(&format!(
                "SELECT COUNT(*) FROM {} WHERE dest_table='{}'",
                self.fq("_apitap_state"),
                sql_lit(&self.bare)
            ))
            .await?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
                > 0
        } else {
            false
        };
        // The state write lands after the RENAME swap — arbitration is Greatest
        // (see plan::WmArbitration): a crash between them costs a bounded
        // re-read, never a skip.
        let numeric = !crate::dialect::mysql::cursor_quoted(
            &plan
                .cols
                .iter()
                .find(|c| c.name == cursor)
                .map(|c| c.udt.clone())
                .unwrap_or_default(),
        )
        .unwrap_or(true);
        let watermark = crate::plan::resolve_watermark(
            state_wm,
            data_wm,
            siblings,
            crate::plan::WmArbitration::Greatest { numeric },
            &self.bare,
            source_id,
        )?;
        Ok(DestState {
            exists: true,
            watermark,
        })
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        if rows == 0 {
            return self
                .exec(&format!("DROP TABLE IF EXISTS {}", self.fq(&self.staging)))
                .await;
        }
        let staged_wm = match &self.cursor_col {
            Some(c) => {
                self.scalar(&format!(
                    "SELECT CAST(MAX({}) AS CHAR) FROM {}",
                    my_ident(c),
                    self.fq(&self.staging)
                ))
                .await?
            }
            None => None,
        };
        match mode {
            Mode::Replace => {
                let exists = self
                    .scalar(&format!(
                        "SELECT COUNT(*) FROM information_schema.tables \
                         WHERE table_schema='{}' AND table_name='{}'",
                        sql_lit(&self.db),
                        sql_lit(&self.bare)
                    ))
                    .await?
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0)
                    > 0;
                if exists {
                    // Atomic across both tables: readers never see it missing.
                    let old = format!("{}__apitap_old", self.bare);
                    self.exec(&format!(
                        "RENAME TABLE {final} TO {old}, {staging} TO {final}",
                        final = self.fq(&self.bare),
                        old = self.fq(&old),
                        staging = self.fq(&self.staging),
                    ))
                    .await?;
                    self.exec(&format!("DROP TABLE {}", self.fq(&old))).await?;
                } else {
                    self.exec(&format!(
                        "RENAME TABLE {} TO {}",
                        self.fq(&self.staging),
                        self.fq(&self.bare)
                    ))
                    .await?;
                }
                // Replace destroyed every source's rows — clear stale state.
                self.ensure_state_table().await?;
                self.exec(&format!(
                    "DELETE FROM {} WHERE dest_table='{}'",
                    self.fq("_apitap_state"),
                    sql_lit(&self.bare)
                ))
                .await?;
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Append => {
                self.exec(&format!(
                    "INSERT INTO {} SELECT * FROM {}",
                    self.fq(&self.bare),
                    self.fq(&self.staging)
                ))
                .await?;
                self.exec(&format!("DROP TABLE {}", self.fq(&self.staging)))
                    .await?;
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Merge => Err(Error::InvalidInput(
                "merge is not supported for MySQL destinations yet".into(),
            )),
        }
    }
}


pub(crate) struct MySqlLoader {
    tx: mpsc::Sender<std::io::Result<Bytes>>,
    join: tokio::task::JoinHandle<Result<u64>>,
    id: u64,
    registry: Registry,
}

impl Loader for MySqlLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        self.tx
            .send(Ok(Bytes::from(buf)))
            .await
            .map_err(|_| Error::Transfer("mysql LOAD DATA stream closed early".into()))
    }

    async fn finish(mut self) -> Result<u64> {
        // Close the stream → the server sees EOF → LOAD DATA commits.
        self.tx.close_channel();
        self.join
            .await
            .map_err(|e| Error::Transfer(format!("mysql loader join: {e}")))?
    }

    async fn abort(mut self, cause: Error) -> Error {
        // Push an error into the stream so the server ABORTS the load instead of
        // committing a partial file, then drop everything.
        let _ = self
            .tx
            .send(Err(std::io::Error::other("apitap: source failed")))
            .await;
        self.tx.close_channel();
        self.registry
            .lock()
            .expect("infile registry")
            .remove(&self.id);
        let _ = self.join.await;
        cause
    }
}
