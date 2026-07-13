//! ClickHouse connector: [`ChSink`] — streaming HTTP `INSERT … FORMAT RowBinary`
//! (or TabSeparated for the text fallback lane) into a staging MergeTree, swapped in
//! atomically with `EXCHANGE TABLES`.

use crate::driver::Loader;
use crate::error::{Error, Result};
use crate::plan::{Delivered, DestState, Lane, TablePlan, WireFormat};
use crate::Mode;

/// A ClickHouse HTTP endpoint parsed from a `clickhouse://user:pass@host:port/db` URL
/// (`clickhouse+https://` or port 8443 → TLS; port defaults to 8123).
#[derive(Clone)]
pub(crate) struct ChConn {
    base: String,
    user: String,
    password: String,
    database: String,
    client: reqwest::Client,
}

impl ChConn {
    pub(crate) fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("clickhouse url: {e}")))?;
        let https = matches!(u.scheme(), "clickhouse+https" | "https") || u.port() == Some(8443);
        let host = u
            .host_str()
            .ok_or_else(|| Error::InvalidInput("clickhouse url: missing host".into()))?;
        let port = u.port().unwrap_or(if https { 8443 } else { 8123 });
        let database = u.path().trim_start_matches('/').to_string();
        Ok(Self {
            base: format!("{}://{host}:{port}/", if https { "https" } else { "http" }),
            user: if u.username().is_empty() {
                "default".into()
            } else {
                u.username().into()
            },
            password: u.password().unwrap_or("").to_string(),
            database: if database.is_empty() {
                "default".into()
            } else {
                database
            },
            client: reqwest::Client::new(),
        })
    }

    /// Common query params. `wait_end_of_query=1` buffers the response until the
    /// statement fully completed, so the HTTP status is trustworthy (otherwise an
    /// insert can fail after a 200 was already sent).
    fn params<'a>(&'a self, extra: &'a str) -> [(&'a str, &'a str); 5] {
        [
            ("database", self.database.as_str()),
            ("wait_end_of_query", "1"),
            ("date_time_input_format", "best_effort"),
            // Naive datetimes travel as-if-UTC on the binary lanes; pinning the
            // session makes the text lane parse AND `toString(max(cursor))` render in
            // the same frame — otherwise a non-UTC ClickHouse server shifts the
            // incremental watermark by its offset (silent loss or duplicates).
            // Requires ClickHouse ≥ 23.6.
            ("session_timezone", "UTC"),
            ("query", extra),
        ]
    }

    /// Run a statement with no input data (DDL, small SELECTs); returns the body.
    /// The SQL travels as the POST body — a body-less POST has no Content-Length and
    /// ClickHouse rejects it with 411.
    pub(crate) async fn exec(&self, query: &str) -> Result<String> {
        let resp = self
            .client
            .post(&self.base)
            .basic_auth(&self.user, Some(&self.password))
            .query(&self.params("")[..4])
            .body(query.to_string())
            .send()
            .await
            .map_err(|e| Error::Connect(format!("clickhouse: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "clickhouse {status}: {}",
                body.trim()
            )));
        }
        Ok(body)
    }

    /// Stream `body` into `query` (an `INSERT … FORMAT …`): query in the URL, data as
    /// a chunked body. Squash settings force ~1M-row blocks so each worker writes a few
    /// big MergeTree parts instead of hundreds of small ones (the probe showed
    /// OpenFileForWrite climbing into the hundreds — parts churn).
    pub(crate) async fn insert_stream(&self, query: &str, body: reqwest::Body) -> Result<()> {
        let resp = self
            .client
            .post(&self.base)
            .basic_auth(&self.user, Some(&self.password))
            .query(&self.params(query))
            .query(&[
                ("min_insert_block_size_rows", "1048576"),
                ("min_insert_block_size_bytes", "536870912"),
                // The text lane must fail on \\N into a non-Nullable column exactly
                // like the RowBinary lane does — never coerce NULL to 0/''.
                ("input_format_null_as_default", "0"),
            ])
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("clickhouse insert: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!(
                "clickhouse {status}: {}",
                body.trim()
            )));
        }
        Ok(())
    }
}

/// `` ` ``-quote a ClickHouse identifier.
pub(crate) fn ch_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))
}

/// Strip wrappers/metadata that don't change the wire encoding so an existing
/// destination column can be compared against what this source delivers:
/// Nullable/LowCardinality are transparent in RowBinary, and timezone on
/// DateTime/DateTime64 is display metadata (values are epoch-based; text
/// parsing is pinned by session_timezone=UTC).
fn ch_strip_wrappers(t: &str) -> &str {
    let mut t = t.trim();
    loop {
        let mut stripped = false;
        for w in ["Nullable(", "LowCardinality("] {
            if let Some(rest) = t.strip_prefix(w).and_then(|r| r.strip_suffix(')')) {
                t = rest;
                stripped = true;
            }
        }
        if !stripped {
            break;
        }
    }
    t
}

/// Column-level nullability regardless of wrapper order — ClickHouse spells the
/// idiomatic form `LowCardinality(Nullable(T))` with Nullable INSIDE, and the
/// RowBinary null-flag byte follows the Nullable, not the outermost wrapper.
fn ch_wrapped_nullable(t: &str) -> bool {
    let mut t = t.trim();
    while let Some(rest) = t
        .strip_prefix("LowCardinality(")
        .and_then(|r| r.strip_suffix(')'))
    {
        t = rest;
    }
    t.starts_with("Nullable(")
}

/// The explicit timezone argument of a DateTime/DateTime64 column, if any.
fn ch_explicit_tz(t: &str) -> Option<String> {
    let t = ch_strip_wrappers(t);
    let args = t
        .strip_prefix("DateTime64(")
        .or_else(|| t.strip_prefix("DateTime("))
        .and_then(|r| r.strip_suffix(')'))?;
    args.split(',')
        .map(str::trim)
        .find_map(|a| a.strip_prefix('\'')?.strip_suffix('\'').map(str::to_string))
}

fn ch_base_type(t: &str) -> String {
    let t = ch_strip_wrappers(t);
    if let Some(args) = t
        .strip_prefix("DateTime64(")
        .and_then(|r| r.strip_suffix(')'))
    {
        let precision = args.split(',').next().unwrap_or(args).trim();
        return format!("DateTime64({precision})");
    }
    if t.starts_with("DateTime") && !t.starts_with("DateTime64") {
        return "DateTime".to_string();
    }
    // Bool and UInt8 share the RowBinary wire format — don't reject one for the other.
    if t == "Bool" {
        return "UInt8".to_string();
    }
    t.to_string()
}

/// Pick the higher of two watermarks. For numeric cursors the compare is numeric;
/// `None < Some` in Option's ordering means an UNPARSEABLE state watermark loses to
/// the data — the safe direction (worst case is a bounded re-read, never a skip).
fn wm_pick(numeric: bool, state: String, data: String) -> String {
    let state_wins = if numeric {
        state.parse::<i128>().ok() >= data.parse::<i128>().ok() && state.parse::<i128>().is_ok()
    } else {
        state >= data // ISO datetime text compares correctly lexicographically
    };
    if state_wins {
        state
    } else {
        data
    }
}

/// Escape a string for a single-quoted ClickHouse literal.
fn ch_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

impl ChSink {
    async fn ensure_state_table(&self) -> Result<()> {
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

    async fn write_state(&self, watermark: &str, rows: u64) -> Result<()> {
        let (Some(cursor), Some(source_id)) = (&self.cursor_col, &self.source_id) else {
            return Ok(());
        };
        self.ch
            .exec(&format!(
                "INSERT INTO `_apitap_state` \
                 (dest_table, source_id, cursor_col, watermark, mode, last_rows) \
                 VALUES ('{}', '{}', '{}', '{}', '{}', {rows})",
                ch_str(&self.final_bare),
                ch_str(source_id),
                ch_str(cursor),
                ch_str(watermark),
                self.mode_str,
            ))
            .await?;
        Ok(())
    }
}

/// ClickHouse column type for a delivered value.
fn ch_type_of(d: &Delivered) -> String {
    match d {
        Delivered::Int { bytes, unsigned } => {
            let width = match bytes {
                1 => "8",
                2 => "16",
                4 => "32",
                _ => "64",
            };
            format!("{}Int{width}", if *unsigned { "U" } else { "" })
        }
        Delivered::Float32 => "Float32".into(),
        Delivered::Float64 => "Float64".into(),
        Delivered::Decimal { p: 0, .. } => "Float64".into(), // defensive; planners avoid it
        Delivered::Decimal { p, s } => format!("Decimal({p}, {s})"),
        Delivered::Bool => "UInt8".into(),
        Delivered::Date => "Date32".into(),
        Delivered::DateTime { utc: false } => "DateTime64(6)".into(),
        Delivered::DateTime { utc: true } => "DateTime64(6, 'UTC')".into(),
        Delivered::Uuid => "UUID".into(),
        Delivered::Json | Delivered::Text => "String".into(),
        Delivered::Bytes => "String".into(),
    }
}

pub(crate) struct ChSink {
    ch: ChConn,
    final_t: String,
    staging_t: String,
    /// Unquoted final table name, for system.tables/system.columns lookups.
    final_bare: String,
    /// Incremental context for the state row (set in dest_state).
    source_id: Option<String>,
    cursor_col: Option<String>,
    mode_str: &'static str,
    /// (name, type) of the EXISTING destination — staging mirrors it verbatim so
    /// the append ATTACH can never hit INCOMPATIBLE_COLUMNS.
    dest_structure: Vec<(String, String)>,
    /// Sorting/primary keys of the existing destination (ATTACH requires them equal).
    dest_sorting_key: Option<String>,
    dest_primary_key: Option<String>,
    /// `INSERT INTO staging FORMAT …`, fixed at prepare time.
    insert_sql: String,
}

impl ChSink {
    pub(crate) fn connect(url: &str, dest_table: &str) -> Result<Self> {
        // ClickHouse table names aren't schema-qualified the Postgres way — take the
        // bare name (the URL's /database picks the namespace).
        let dest_bare = dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t);
        Ok(Self {
            ch: ChConn::parse(url)?,
            final_t: ch_ident(dest_bare),
            staging_t: ch_ident(&format!("{dest_bare}__apitap_staging")),
            final_bare: dest_bare.to_string(),
            source_id: None,
            cursor_col: None,
            mode_str: "replace",
            dest_structure: Vec::new(),
            dest_sorting_key: None,
            dest_primary_key: None,
            insert_sql: String::new(),
        })
    }
}

impl crate::driver::Sink for ChSink {
    type Loader = ChLoader;

    fn accepts(&self) -> &'static [WireFormat] {
        // Best first: binary when the source can transcode every column, else text.
        &[WireFormat::RowBinary, WireFormat::TabSeparated]
    }

    fn adjust_plan(&self, plan: &mut TablePlan) {
        // The ORDER BY column must be non-nullable in ClickHouse; the encoders read
        // the same flag, so DDL and wire stay in agreement.
        if let Some(cursor) = plan.cursor.clone() {
            for c in &mut plan.cols {
                if c.name == cursor {
                    c.nullable = false;
                }
            }
        }
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        _mode: Mode,
    ) -> Result<()> {
        let ddl_list = if self.dest_structure.is_empty() {
            plan.cols
                .iter()
                .zip(lane.cols.iter())
                .map(|(c, lc)| {
                    let ty = ch_type_of(&lc.delivered);
                    let ty = if c.nullable {
                        format!("Nullable({ty})")
                    } else {
                        ty
                    };
                    format!("{} {}", ch_ident(&c.name), ty)
                })
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            // Appending into an existing table: staging mirrors the destination
            // verbatim so ATTACH PARTITION sees an identical structure. The bytes
            // we send must still parse as those types — check base types up front
            // instead of corrupting data or hitting INCOMPATIBLE_COLUMNS later.
            for (lc, (name, dest_ty)) in lane.cols.iter().zip(self.dest_structure.iter()) {
                let want = ch_type_of(&lc.delivered);
                if ch_base_type(dest_ty) != ch_base_type(&want) {
                    return Err(Error::InvalidInput(format!(
                        "destination column {name} is {dest_ty} but this source delivers \
                         {want} — align the destination type or run once with mode='replace'"
                    )));
                }
                // The text lane parses offset-less timestamps in the COLUMN's
                // timezone (the session pin only covers tz-less columns) — every
                // stored instant would silently shift.
                if lane.format == WireFormat::TabSeparated
                    && lc.delivered == (Delivered::DateTime { utc: false })
                    && ch_explicit_tz(dest_ty).is_some_and(|tz| tz != "UTC")
                {
                    return Err(Error::InvalidInput(format!(
                        "destination column {name} is {dest_ty}: this transfer uses \
                         the text lane, which would parse naive timestamps in that \
                         timezone and shift every value — declare the column \
                         DateTime64(6) or DateTime64(6, 'UTC')"
                    )));
                }
            }
            self.dest_structure
                .iter()
                .map(|(n, t)| format!("{} {}", ch_ident(n), t))
                .collect::<Vec<_>>()
                .join(", ")
        };
        // ATTACH PARTITION FROM requires equal sorting/primary keys, so a mirrored
        // staging table must copy the destination's keys, not guess from the cursor.
        let order_by = match self.dest_sorting_key.as_deref() {
            Some("") => "tuple()".to_string(),
            Some(keys) => format!("({keys})"),
            None => plan
                .cursor
                .as_deref()
                .map_or("tuple()".to_string(), ch_ident),
        };
        let primary_key = self
            .dest_primary_key
            .as_deref()
            .filter(|pk| Some(*pk) != self.dest_sorting_key.as_deref())
            .map(|pk| format!(" PRIMARY KEY ({pk})"))
            .unwrap_or_default();
        self.ch
            .exec(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
            .await?;
        self.ch
            .exec(&format!(
                "CREATE TABLE {} ({ddl_list}) ENGINE = MergeTree{primary_key} \
                 ORDER BY {order_by}",
                self.staging_t
            ))
            .await?;
        let fmt = match lane.format {
            WireFormat::TabSeparated => "TabSeparated",
            WireFormat::RowBinary => "RowBinary",
            // accepts() never offers it — negotiation can't get here.
            WireFormat::PgCopyBinary => unreachable!("guarded by accepts()"),
        };
        self.insert_sql = format!("INSERT INTO {} FORMAT {fmt}", self.staging_t);
        Ok(())
    }

    async fn loader(&self) -> Result<ChLoader> {
        Ok(ChLoader::open(self.ch.clone(), self.insert_sql.clone()))
    }

    async fn rows_staged(&self, _loaded: u64) -> Result<u64> {
        Ok(self
            .ch
            .exec(&format!("SELECT count() FROM {}", self.staging_t))
            .await?
            .trim()
            .parse()
            .unwrap_or(0))
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
                "merge is not supported for ClickHouse destinations yet — use append, \
                 or replace with a ReplacingMergeTree downstream"
                    .into(),
            ));
        }
        self.source_id = Some(source_id.to_string());
        self.cursor_col = Some(cursor.to_string());
        self.mode_str = if mode == Mode::Append {
            "append"
        } else {
            "merge"
        };
        // Numeric vs temporal comparison for greatest(state, data) below.
        let numeric_cursor = plan
            .cols
            .iter()
            .find(|c| c.name == cursor)
            .map(|c| {
                !matches!(
                    c.udt.as_str(),
                    "date" | "timestamp" | "timestamptz" | "datetime"
                )
            })
            .unwrap_or(false);
        self.ensure_state_table().await?;
        let lit = self.final_bare.replace('\\', "\\\\").replace('\'', "\\'");
        let exists = self
            .ch
            .exec(&format!(
                "SELECT count() FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{lit}'"
            ))
            .await?
            .trim()
            .parse::<u64>()
            .map_err(|e| Error::Transfer(format!("dest lookup parse: {e}")))?
            > 0;
        if !exists {
            return Ok(DestState {
                exists: false,
                watermark: None,
            });
        }
        let dest_cols: Vec<(String, String)> = self
            .ch
            .exec(&format!(
                // TabSeparatedRaw: type strings contain quotes ('UTC') that
                // plain TSV output would backslash-escape.
                "SELECT name, type FROM system.columns \
                 WHERE database = currentDatabase() AND table = '{lit}' \
                 ORDER BY position FORMAT TabSeparatedRaw"
            ))
            .await?
            .lines()
            .filter_map(|l| {
                l.split_once('\t')
                    .map(|(n, t)| (n.to_string(), t.to_string()))
            })
            .collect();
        let dest_names: Vec<&str> = dest_cols.iter().map(|(n, _)| n.as_str()).collect();
        let src_cols: Vec<String> = plan.cols.iter().map(|c| c.name.clone()).collect();
        if dest_names != src_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>() {
            return Err(Error::InvalidInput(format!(
                "destination columns {dest_names:?} don't match the source {src_cols:?} — \
                 run once with mode='replace' to realign the schema"
            )));
        }
        // The EXISTING destination is the structural authority: mirror its
        // nullability into the plan (so the encoders agree — a view-sourced plan
        // reports every column nullable, the pre-created destination knows better)
        // and remember its full structure; prepare() builds staging from it verbatim
        // so ATTACH PARTITION always sees an identical structure.
        for (pc, (_, dest_ty)) in plan.cols.iter_mut().zip(dest_cols.iter()) {
            pc.nullable = ch_wrapped_nullable(dest_ty);
        }
        // The watermark round-trips through toString(max(cursor)), which renders in
        // the COLUMN's timezone — session_timezone only pins tz-less columns. A
        // non-UTC cursor would poison every delta with local-time text.
        if let Some((name, ty)) = dest_cols.iter().find(|(n, _)| n == cursor) {
            if let Some(tz) = ch_explicit_tz(ty) {
                if tz != "UTC" {
                    return Err(Error::InvalidInput(format!(
                        "destination cursor column {name} is {ty} — a non-UTC column \
                         timezone renders the watermark as local time and silently \
                         skips or duplicates rows; declare it DateTime64(6) or \
                         DateTime64(6, 'UTC')"
                    )));
                }
            }
        }
        self.dest_structure = dest_cols;
        // ATTACH PARTITION FROM also requires matching keys, so mirror them too —
        // and fail fast on a partitioned destination (we attach one unpartitioned
        // part) instead of streaming everything and dying at the ATTACH.
        let keys = self
            .ch
            .exec(&format!(
                "SELECT partition_key, sorting_key, primary_key FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{lit}' \
                 FORMAT TabSeparatedRaw"
            ))
            .await?;
        let mut keys = keys.trim_end_matches('\n').split('\t');
        let (part_key, sort_key, prim_key) = (
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
        );
        if !part_key.is_empty() {
            return Err(Error::InvalidInput(format!(
                "destination {} has PARTITION BY ({part_key}) — append attaches a \
                 single unpartitioned part; drop the partition key or run with \
                 mode='replace'",
                self.final_t
            )));
        }
        self.dest_sorting_key = Some(sort_key);
        self.dest_primary_key = if prim_key.is_empty() {
            None
        } else {
            Some(prim_key)
        };
        let n: u64 = self
            .ch
            .exec(&format!("SELECT count() FROM {}", self.final_t))
            .await?
            .trim()
            .parse()
            .map_err(|e| Error::Transfer(format!("dest count parse: {e}")))?;
        // An EMPTY table cannot carry a watermark, whatever the state row says —
        // TRUNCATE-to-resync must work.
        if n == 0 {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        let data_wm = {
            Some(
                self.ch
                    .exec(&format!(
                        "SELECT toString(max({})) FROM {}",
                        ch_ident(cursor),
                        self.final_t
                    ))
                    .await?
                    .trim()
                    .to_string(),
            )
        };
        // ClickHouse can't update the state row atomically with the ATTACH, so a
        // crash can leave the state one run behind. The effective watermark is the
        // GREATEST of state and data: a stale-low state row then merely re-reads a
        // delta the data already shows (loud, bounded), never skips ahead.
        let state_wm: Option<String> = {
            let out = self
                .ch
                .exec(&format!(
                    "SELECT watermark FROM `_apitap_state` FINAL \
                     WHERE dest_table = '{}' AND source_id = '{}'",
                    ch_str(&self.final_bare),
                    ch_str(source_id)
                ))
                .await?;
            let t = out.trim();
            (!t.is_empty()).then(|| t.to_string())
        };
        let watermark = match (state_wm, data_wm) {
            (Some(a), Some(b)) => Some(wm_pick(numeric_cursor, a, b)),
            (None, data) => {
                // Fan-in guard: other sources' state rows mean the global data-max
                // is not ours — a fallback would skip this source's backlog.
                let siblings: u64 = self
                    .ch
                    .exec(&format!(
                        "SELECT count() FROM `_apitap_state` FINAL WHERE dest_table = '{}'",
                        ch_str(&self.final_bare)
                    ))
                    .await?
                    .trim()
                    .parse()
                    .map_err(|e| Error::Transfer(format!("state count parse: {e}")))?;
                if siblings > 0 {
                    return Err(Error::InvalidInput(format!(
                        "destination {} has state rows from other sources but none for \
                         '{source_id}' — run mode='replace' to rebuild, or seed a state \
                         row manually",
                        self.final_bare
                    )));
                }
                data
            }
            (state, None) => state,
        };
        Ok(DestState {
            exists: true,
            watermark,
        })
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        // 0-row guard, every mode.
        if rows == 0 {
            let _ = self
                .ch
                .exec(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
                .await;
            return Ok(());
        }
        // Watermark of what THIS run staged (session is UTC-pinned).
        let staged_wm = match &self.cursor_col {
            Some(c) => Some(
                self.ch
                    .exec(&format!(
                        "SELECT toString(max({})) FROM {}",
                        ch_ident(c),
                        self.staging_t
                    ))
                    .await?
                    .trim()
                    .to_string(),
            ),
            None => None,
        };
        match mode {
            Mode::Replace => {
                self.ch
                    .exec(&format!(
                        "CREATE TABLE IF NOT EXISTS {} AS {}",
                        self.final_t, self.staging_t
                    ))
                    .await?;
                self.ch
                    .exec(&format!(
                        "EXCHANGE TABLES {} AND {}",
                        self.staging_t, self.final_t
                    ))
                    .await?;
                self.ch
                    .exec(&format!("DROP TABLE {}", self.staging_t))
                    .await?;
                // Replace destroyed every source's rows — clear ALL stale state rows
                // for this destination before the bootstrap (if any) re-inserts its own.
                self.ensure_state_table().await?;
                self.ch
                    .exec(&format!(
                        "DELETE FROM `_apitap_state` WHERE dest_table = '{}'",
                        ch_str(&self.final_bare)
                    ))
                    .await?;
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Append => {
                // Metadata-only part attach — the append-mode sibling of EXCHANGE:
                // near-instant and atomic per partition (our tables are unpartitioned,
                // so 'all' is the single partition). Requires identical structure and
                // ORDER BY, which staging shares with any table this engine created;
                // a hand-made destination that differs fails here loudly.
                self.ch
                    .exec(&format!(
                        "ALTER TABLE {} ATTACH PARTITION ID 'all' FROM {}",
                        self.final_t, self.staging_t
                    ))
                    .await?;
                self.ch
                    .exec(&format!("DROP TABLE {}", self.staging_t))
                    .await?;
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Merge => Err(Error::InvalidInput(
                "merge is not supported for ClickHouse destinations yet".into(),
            )),
        }
    }
}

/// One streaming HTTP insert. The worker's buffers go through a 2-slot channel into
/// the request body, so encoding overlaps the HTTP flush; the request itself runs in a
/// spawned task whose result carries the REAL failure (reqwest reduces a mid-body
/// error to an opaque "error sending request" on the body side).
pub(crate) struct ChLoader {
    tx: futures::channel::mpsc::Sender<std::io::Result<bytes::Bytes>>,
    join: tokio::task::JoinHandle<Result<()>>,
}

impl ChLoader {
    fn open(ch: ChConn, insert_sql: String) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel::<std::io::Result<bytes::Bytes>>(2);
        let join = tokio::spawn(async move {
            let body = reqwest::Body::wrap_stream(rx);
            ch.insert_stream(&insert_sql, body).await
        });
        Self { tx, join }
    }

    async fn real_error(join: &mut tokio::task::JoinHandle<Result<()>>) -> Error {
        match join.await {
            Ok(Ok(())) => Error::Transfer("clickhouse insert closed early".into()),
            Ok(Err(e)) => e,
            Err(e) => Error::Transfer(format!("join: {e}")),
        }
    }
}

impl Loader for ChLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        use futures::SinkExt;
        if self.tx.send(Ok(bytes::Bytes::from(buf))).await.is_err() {
            // The insert died — its task holds the real error.
            return Err(Self::real_error(&mut self.join).await);
        }
        Ok(())
    }

    async fn finish(self) -> Result<u64> {
        let Self { tx, mut join } = self;
        drop(tx); // clean end-of-body: ClickHouse commits the insert
        match (&mut join).await {
            Ok(r) => r.map(|_| 0), // rows counted server-side by the sink
            Err(e) => Err(Error::Transfer(format!("join: {e}"))),
        }
    }

    async fn abort(self, cause: Error) -> Error {
        use futures::SinkExt;
        let Self { mut tx, join } = self;
        // Erroring the body aborts the HTTP request, so ClickHouse DISCARDS the
        // partial stream instead of committing it.
        let _ = tx
            .send(Err(std::io::Error::other("apitap: source failed")))
            .await;
        drop(tx);
        let _ = join.await;
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ch_url_parses_scheme_port_and_database() {
        let c = ChConn::parse("clickhouse://alice:secret@ch.example:8123/bench").unwrap();
        assert_eq!(c.base, "http://ch.example:8123/");
        assert_eq!(c.user, "alice");
        assert_eq!(c.database, "bench");
        // Defaults: user `default`, db `default`, port 8123 http.
        let c = ChConn::parse("clickhouse://ch.example").unwrap();
        assert_eq!(c.base, "http://ch.example:8123/");
        assert_eq!(c.user, "default");
        assert_eq!(c.database, "default");
        // TLS via scheme or port.
        assert!(ChConn::parse("clickhouse+https://ch.example/d")
            .unwrap()
            .base
            .starts_with("https://ch.example:8443"));
        assert!(ChConn::parse("clickhouse://ch.example:8443/d")
            .unwrap()
            .base
            .starts_with("https://"));
    }

    #[test]
    fn base_type_strips_wrappers_and_tz() {
        assert_eq!(ch_base_type("Nullable(Int64)"), "Int64");
        assert_eq!(ch_base_type("LowCardinality(String)"), "String");
        assert_eq!(ch_base_type("LowCardinality(Nullable(String))"), "String");
        assert_eq!(ch_base_type("Bool"), ch_base_type("UInt8"));
        assert_eq!(ch_base_type("DateTime64(6, 'UTC')"), "DateTime64(6)");
        assert_eq!(ch_base_type("Nullable(DateTime64(6))"), "DateTime64(6)");
        assert_eq!(ch_base_type("DateTime('Europe/Vilnius')"), "DateTime");
        assert_eq!(ch_base_type("Decimal(38, 9)"), "Decimal(38, 9)");
        // precision differences must NOT be smoothed over
        assert_ne!(ch_base_type("DateTime64(3)"), ch_base_type("DateTime64(6)"));
        assert_ne!(ch_base_type("Int32"), ch_base_type("Int64"));
    }

    #[test]
    fn nullability_follows_the_inner_nullable() {
        assert!(ch_wrapped_nullable("Nullable(String)"));
        assert!(ch_wrapped_nullable("LowCardinality(Nullable(String))"));
        assert!(!ch_wrapped_nullable("LowCardinality(String)"));
        assert!(!ch_wrapped_nullable("Int64"));
    }

    #[test]
    fn explicit_tz_is_extracted() {
        assert_eq!(
            ch_explicit_tz("DateTime64(6, 'Europe/Vilnius')").as_deref(),
            Some("Europe/Vilnius")
        );
        assert_eq!(
            ch_explicit_tz("DateTime64(6, 'UTC')").as_deref(),
            Some("UTC")
        );
        assert_eq!(
            ch_explicit_tz("Nullable(DateTime('America/New_York'))").as_deref(),
            Some("America/New_York")
        );
        assert_eq!(ch_explicit_tz("DateTime64(6)"), None);
        assert_eq!(ch_explicit_tz("Int64"), None);
    }

    #[test]
    fn wm_pick_prefers_data_when_state_is_garbage() {
        // numeric: plain compares
        assert_eq!(wm_pick(true, "1000".into(), "500".into()), "1000");
        assert_eq!(wm_pick(true, "500".into(), "1000".into()), "1000");
        // unparseable STATE must lose to data (bounded re-read, never a skip)
        assert_eq!(wm_pick(true, "garbage".into(), "500".into()), "500");
        // unparseable DATA: state wins (numeric state parses, data doesn't)
        assert_eq!(wm_pick(true, "500".into(), "garbage".into()), "500");
        // temporal: ISO text compare
        assert_eq!(
            wm_pick(
                false,
                "2026-02-01 00:00:00".into(),
                "2026-01-01 00:00:00".into()
            ),
            "2026-02-01 00:00:00"
        );
    }

    #[test]
    fn ch_types_for_deliveries_match_the_old_maps() {
        assert_eq!(
            ch_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: true
            }),
            "UInt64"
        );
        assert_eq!(ch_type_of(&Delivered::Bool), "UInt8");
        assert_eq!(
            ch_type_of(&Delivered::Decimal { p: 18, s: 4 }),
            "Decimal(18, 4)"
        );
        assert_eq!(
            ch_type_of(&Delivered::DateTime { utc: true }),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(
            ch_type_of(&Delivered::DateTime { utc: false }),
            "DateTime64(6)"
        );
        assert_eq!(ch_type_of(&Delivered::Date), "Date32");
        assert_eq!(ch_type_of(&Delivered::Uuid), "UUID");
        assert_eq!(ch_type_of(&Delivered::Json), "String");
    }
}
