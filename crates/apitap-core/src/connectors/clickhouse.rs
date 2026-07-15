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

/// User-chosen DDL for the table apitap CREATES (engine standard: any
/// MergeTree-family engine, Replicated included). All optional; `None` = today's
/// defaults. Ignored when the destination already exists — the existing table is
/// the structural authority.
#[derive(Clone, Debug, Default)]
pub struct ChDdl {
    pub engine: Option<String>,
    pub order_by: Option<String>,
    pub on_cluster: Option<String>,
}

/// The MergeTree family — the only engines whose parts survive our staging→final
/// ATTACH/EXCHANGE choreography.
const MERGETREE_FAMILY: [&str; 14] = [
    "MergeTree",
    "ReplacingMergeTree",
    "SummingMergeTree",
    "AggregatingMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "GraphiteMergeTree",
    "ReplicatedMergeTree",
    "ReplicatedReplacingMergeTree",
    "ReplicatedSummingMergeTree",
    "ReplicatedAggregatingMergeTree",
    "ReplicatedCollapsingMergeTree",
    "ReplicatedVersionedCollapsingMergeTree",
    "ReplicatedGraphiteMergeTree",
];

impl ChDdl {
    fn validate(&self) -> Result<()> {
        if let Some(engine) = &self.engine {
            let family = engine.split('(').next().unwrap_or("").trim();
            if !MERGETREE_FAMILY.contains(&family) {
                return Err(Error::InvalidInput(format!(
                    "engine '{family}' is not a MergeTree-family engine — apitap \
                     loads through a staging table and attaches its parts, which \
                     only the MergeTree family supports (e.g. MergeTree, \
                     ReplacingMergeTree(v), ReplicatedReplacingMergeTree(v))"
                )));
            }
            if let Some(bad) = engine
                .chars()
                .find(|c| !c.is_ascii_alphanumeric() && !"_(),'{}/. +-".contains(*c))
            {
                return Err(Error::InvalidInput(format!(
                    "engine contains unexpected character '{bad}'"
                )));
            }
            // Exactly `Family` or `Family(args)` — a trailing PARTITION BY / TTL /
            // SETTINGS smuggled after the args would silently change storage.
            let shape_ok = match engine.split_once('(') {
                None => engine.trim() == family,
                Some((head, rest)) => {
                    let mut depth = 1u32;
                    let mut in_quote = false;
                    let mut end = None;
                    for (i, c) in rest.char_indices() {
                        match c {
                            '\'' => in_quote = !in_quote,
                            '(' if !in_quote => depth += 1,
                            ')' if !in_quote => {
                                depth -= 1;
                                if depth == 0 {
                                    end = Some(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    head.trim() == family && end.is_some_and(|i| rest[i + 1..].trim().is_empty())
                }
            };
            if !shape_ok {
                return Err(Error::InvalidInput(format!(
                    "engine must be exactly Family or Family(args) — put PARTITION \
                     BY/TTL/SETTINGS on a pre-created table instead (got '{engine}')"
                )));
            }
        }
        if let Some(ob) = &self.order_by {
            if ob.trim().is_empty() {
                return Err(Error::InvalidInput("order_by is empty".into()));
            }
            if let Some(bad) = ob
                .chars()
                .find(|c| !c.is_ascii_alphanumeric() && !"_,() ".contains(*c))
            {
                return Err(Error::InvalidInput(format!(
                    "order_by contains unexpected character '{bad}' — pass a \
                     column list like \"client_id, id\""
                )));
            }
            // Balanced parens, or "id)" escapes the (order_by) wrapper and smuggles
            // arbitrary clauses into the CREATE.
            let mut depth: i64 = 0;
            for c in ob.chars() {
                if c == '(' {
                    depth += 1;
                }
                if c == ')' {
                    depth -= 1;
                }
                if depth < 0 {
                    break;
                }
            }
            if depth != 0 {
                return Err(Error::InvalidInput(format!(
                    "order_by has unbalanced parentheses (got '{ob}')"
                )));
            }
        }
        if let Some(cl) = &self.on_cluster {
            if cl.is_empty()
                || cl
                    .chars()
                    .any(|c| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            {
                return Err(Error::InvalidInput(format!(
                    "on_cluster '{cl}' is not a plain cluster name"
                )));
            }
            if !self
                .engine
                .as_deref()
                .is_some_and(|e| e.trim_start().starts_with("Replicated"))
            {
                return Err(Error::InvalidInput(
                    "on_cluster requires a Replicated* engine — with a local engine \
                     the other replicas would get the table but never the data"
                        .into(),
                ));
            }
        }
        if self.is_replicated() && !self.has_explicit_zk_path() && self.on_cluster.is_none() {
            return Err(Error::InvalidInput(
                "a Replicated* engine without explicit ZooKeeper path arguments \
                 needs on_cluster=... (ClickHouse derives the unique {uuid} path \
                 only for ON CLUSTER DDL on Atomic databases) — pass on_cluster, \
                 or spell the path: ReplicatedReplacingMergeTree('/clickhouse/\
                 tables/{shard}/db/table', '{replica}', version)"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Identifier-shaped tokens of `txt` (quoted spans — zookeeper paths, macros —
    /// and pure numbers dropped).
    fn ident_tokens(txt: &str) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        let mut in_quote = false;
        let mut cur = String::new();
        let mut push = |cur: &mut String, out: &mut std::collections::HashSet<String>| {
            if !cur.is_empty() && !cur.chars().all(|c| c.is_ascii_digit()) {
                out.insert(std::mem::take(cur));
            }
            cur.clear();
        };
        for c in txt.chars() {
            if c == '\'' {
                in_quote = !in_quote;
                cur.clear();
                continue;
            }
            if in_quote {
                continue;
            }
            if c.is_ascii_alphanumeric() || c == '_' {
                cur.push(c);
            } else {
                push(&mut cur, &mut out);
            }
        }
        push(&mut cur, &mut out);
        out
    }

    /// Column names in the engine ARGUMENTS (the Replacing version column, the
    /// Collapsing sign column, …) — these must exist and be non-nullable.
    fn engine_arg_idents(&self) -> std::collections::HashSet<String> {
        self.engine
            .as_deref()
            .and_then(|e| e.split_once('('))
            .map(|(_, args)| Self::ident_tokens(args))
            .unwrap_or_default()
    }

    /// Columns named in the engine args or ORDER BY: the columns these name must
    /// be non-nullable (version/sorting columns), whatever the source claims.
    fn referenced_idents(&self) -> std::collections::HashSet<String> {
        let mut out = self.engine_arg_idents();
        if let Some(ob) = &self.order_by {
            out.extend(Self::ident_tokens(ob));
        }
        out
    }

    fn is_replicated(&self) -> bool {
        self.engine
            .as_deref()
            .is_some_and(|e| e.trim_start().starts_with("Replicated"))
    }

    /// Whether the engine spelling carries explicit ZooKeeper path arguments
    /// (first argument is a quoted string).
    fn has_explicit_zk_path(&self) -> bool {
        self.engine
            .as_deref()
            .and_then(|e| e.split_once('('))
            .is_some_and(|(_, args)| args.trim_start().starts_with('\''))
    }

    fn on_cluster_clause(&self) -> String {
        self.on_cluster
            .as_deref()
            .map(|c| format!(" ON CLUSTER `{c}`"))
            .unwrap_or_default()
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
    /// User-chosen DDL for tables apitap creates.
    ddl: ChDdl,
    /// Column DDL + ORDER BY of the staging table, kept for finalize so the final
    /// table it creates (with the user's engine) is ATTACH-identical to staging.
    plan_ddl: String,
    staging_order_by: String,
}

impl ChSink {
    pub(crate) fn connect(url: &str, dest_table: &str, ddl: ChDdl) -> Result<Self> {
        ddl.validate()?;
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
            ddl,
            plan_ddl: String::new(),
            staging_order_by: String::new(),
        })
    }
}

impl crate::driver::Sink for ChSink {
    type Loader = ChLoader;

    fn accepts(&self) -> &[WireFormat] {
        // Best first: binary when the source can transcode every column, else text.
        &[WireFormat::RowBinary, WireFormat::TabSeparated]
    }

    fn adjust_plan(&self, plan: &mut TablePlan) {
        // ORDER BY and Replacing-version columns must be non-nullable in ClickHouse;
        // the encoders read the same flag, so DDL and wire stay in agreement (an
        // actual NULL then fails loudly instead of corrupting the stream). This
        // covers the cursor plus anything the user named in engine/order_by —
        // view-sourced plans claim everything nullable.
        let named = self.ddl.referenced_idents();
        for c in &mut plan.cols {
            if plan.cursor.as_deref() == Some(&c.name) || named.contains(&c.name) {
                c.nullable = false;
            }
        }
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        mode: Mode,
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
        // staging table must copy the destination's keys, not guess from the cursor;
        // for tables apitap creates, the user's order_by wins over the cursor.
        let order_by = match self.dest_sorting_key.as_deref() {
            Some("") => "tuple()".to_string(),
            Some(keys) => format!("({keys})"),
            None => match self.ddl.order_by.as_deref() {
                Some(ob) => format!("({ob})"),
                None => plan
                    .cursor
                    .as_deref()
                    .map_or("tuple()".to_string(), ch_ident),
            },
        };
        let primary_key = self
            .dest_primary_key
            .as_deref()
            .filter(|pk| Some(*pk) != self.dest_sorting_key.as_deref())
            .map(|pk| format!(" PRIMARY KEY ({pk})"))
            .unwrap_or_default();
        if self.dest_structure.is_empty() {
            // apitap is about to CREATE the table — catch config typos now, not
            // after the whole source has streamed.
            if let Some(cl) = self.ddl.on_cluster.as_deref() {
                let shards: u64 = self
                    .ch
                    .exec(&format!(
                        "SELECT uniqExact(shard_num) FROM system.clusters \
                         WHERE cluster = '{}'",
                        ch_str(cl)
                    ))
                    .await?
                    .trim()
                    .parse()
                    .unwrap_or(0);
                if shards == 0 {
                    return Err(Error::InvalidInput(format!(
                        "cluster '{cl}' is not defined on this server — check \
                         on_cluster against system.clusters"
                    )));
                }
                if shards > 1 {
                    return Err(Error::InvalidInput(format!(
                        "cluster '{cl}' has {shards} shards — apitap loads one \
                         replication group (its state lives on the connected host); \
                         use a single-shard cluster or manage sharding yourself"
                    )));
                }
            }
            if mode == Mode::Replace && self.ddl.is_replicated() && self.ddl.has_explicit_zk_path()
            {
                let exists: u64 = self
                    .ch
                    .exec(&format!(
                        "SELECT count() FROM system.tables WHERE \
                         database = currentDatabase() AND name = '{}'",
                        ch_str(&self.final_bare)
                    ))
                    .await?
                    .trim()
                    .parse()
                    .unwrap_or(0);
                if exists > 0 {
                    return Err(Error::InvalidInput(format!(
                        "mode='replace' into the existing Replicated table {} \
                         would need a second ZooKeeper path for the shadow copy — \
                         use mode='append', drop the table first, or use \
                         on_cluster with a path-less engine spelling",
                        self.final_t
                    )));
                }
            }
            let cols: std::collections::HashSet<&str> =
                plan.cols.iter().map(|c| c.name.as_str()).collect();
            for ident in self.ddl.engine_arg_idents() {
                if !cols.contains(ident.as_str()) {
                    return Err(Error::InvalidInput(format!(
                        "engine references column '{ident}', which the source \
                         doesn't deliver — available: {:?}",
                        plan.cols.iter().map(|c| &c.name).collect::<Vec<_>>()
                    )));
                }
            }
        }
        self.plan_ddl = ddl_list.clone();
        self.staging_order_by = order_by.clone();
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
                "SELECT partition_key, sorting_key, primary_key, engine, engine_full \
                 FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{lit}' \
                 FORMAT TabSeparatedRaw"
            ))
            .await?;
        let mut keys = keys.trim_end_matches('\n').split('\t');
        let (part_key, sort_key, prim_key, dest_engine, dest_engine_full) = (
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
            keys.next().unwrap_or("").to_string(),
        );
        let normalize_keys = |k: &str| -> Vec<String> {
            k.split(',')
                .map(|t| t.replace(['`', ' '], ""))
                .filter(|t| !t.is_empty())
                .collect()
        };
        if let Some(engine) = &self.ddl.engine {
            let family = engine.split('(').next().unwrap_or("").trim();
            if family != dest_engine {
                return Err(Error::InvalidInput(format!(
                    "destination {} already exists with engine {dest_engine}, but \
                     engine='{family}' was requested — drop the table, or omit \
                     engine to append into it as-is",
                    self.final_t
                )));
            }
            // Family agreement isn't enough: a changed version/sign column would
            // silently change dedup semantics. engine_full's args (first balanced
            // paren group) hold the existing table's columns; quoted zk paths and
            // macros drop out of the token scan on both sides.
            let dest_args = dest_engine_full
                .split_once('(')
                .map(|(_, rest)| {
                    let mut depth = 1u32;
                    let mut in_quote = false;
                    let mut end = rest.len();
                    for (i, c) in rest.char_indices() {
                        match c {
                            '\'' => in_quote = !in_quote,
                            '(' if !in_quote => depth += 1,
                            ')' if !in_quote => {
                                depth -= 1;
                                if depth == 0 {
                                    end = i;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    ChDdl::ident_tokens(&rest[..end])
                })
                .unwrap_or_default();
            let want_args = self.ddl.engine_arg_idents();
            if !want_args.is_empty() && want_args != dest_args {
                return Err(Error::InvalidInput(format!(
                    "destination {} exists with engine {dest_engine_full}, whose \
                     column arguments {dest_args:?} differ from the requested \
                     {want_args:?} — drop the table, or omit engine to append \
                     into it as-is",
                    self.final_t
                )));
            }
        }
        if let Some(ob) = self.ddl.order_by.as_deref() {
            if normalize_keys(ob) != normalize_keys(&sort_key) {
                return Err(Error::InvalidInput(format!(
                    "destination {} exists with ORDER BY ({sort_key}), but \
                     order_by='{ob}' was requested — the sorting key of an \
                     existing table can't be changed; drop the table, or omit \
                     order_by to append into it as-is",
                    self.final_t
                )));
            }
        }
        // Engines whose merges rewrite or drop rows (Summing/Aggregating/
        // Collapsing/…) can move max(cursor) between runs; unless the cursor is
        // part of the sorting key its watermark is meaningless — silent skips.
        let merge_rewrites = [
            "Summing",
            "Aggregating",
            "Collapsing",
            "VersionedCollapsing",
            "Graphite",
        ]
        .iter()
        .any(|f| dest_engine.trim_start_matches("Replicated").starts_with(f));
        if merge_rewrites && !normalize_keys(&sort_key).contains(&cursor.replace('`', "")) {
            return Err(Error::InvalidInput(format!(
                "append into {} (engine {dest_engine}): the cursor column {cursor} \
                 is not part of ORDER BY ({sort_key}), so background merges can \
                 rewrite it and poison the incremental watermark — add it to the \
                 sorting key or use mode='replace'",
                self.final_t
            )));
        }
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
                if self.ddl.engine.is_some() || self.ddl.on_cluster.is_some() {
                    // The final table needs the USER's engine, so a name swap with
                    // the MergeTree staging won't do: create the final with that
                    // engine and move the parts in with a metadata-only ATTACH (a
                    // Replicated engine fans them out to the other replicas).
                    let engine = self.ddl.engine.as_deref().unwrap_or("MergeTree");
                    let oc = self.ddl.on_cluster_clause();
                    let create_cols = &self.plan_ddl;
                    let order_by = &self.staging_order_by;
                    let exists = self
                        .ch
                        .exec(&format!(
                            "SELECT count() FROM system.tables WHERE \
                             database = currentDatabase() AND name = '{}'",
                            ch_str(&self.final_bare)
                        ))
                        .await?
                        .trim()
                        .parse::<u64>()
                        .unwrap_or(0)
                        > 0;
                    if !exists {
                        // Bootstrap: the destination gets the (possibly explicit)
                        // ZooKeeper path directly — no shadow table needed.
                        // IF NOT EXISTS: a partially-applied earlier ON CLUSTER
                        // create may have left the table on some hosts.
                        self.ch
                            .exec(&format!(
                                "CREATE TABLE IF NOT EXISTS {}{oc} ({create_cols}) \
                                 ENGINE = {engine} ORDER BY {order_by}",
                                self.final_t
                            ))
                            .await?;
                        self.ch
                            .exec(&format!(
                                "ALTER TABLE {} ATTACH PARTITION ID 'all' FROM {}",
                                self.final_t, self.staging_t
                            ))
                            .await?;
                        self.ch
                            .exec(&format!("DROP TABLE {}", self.staging_t))
                            .await?;
                    } else {
                        // Replacing an EXISTING table goes through a shadow copy,
                        // which needs its own ZooKeeper path — only the ON CLUSTER
                        // {uuid} default can mint one per generation.
                        if self.ddl.is_replicated() && self.ddl.has_explicit_zk_path() {
                            // Backstop for a table created between prepare's check
                            // and now — don't leave the streamed staging behind.
                            let _ = self
                                .ch
                                .exec(&format!("DROP TABLE {}", self.staging_t))
                                .await;
                            return Err(Error::InvalidInput(format!(
                                "mode='replace' into the existing Replicated table \
                                 {} would need a second ZooKeeper path for the \
                                 shadow copy — use mode='append', drop the table \
                                 first, or use on_cluster with a path-less engine \
                                 spelling",
                                self.final_t
                            )));
                        }
                        let tmp = ch_ident(&format!("{}__apitap_new", self.final_bare));
                        self.ch
                            .exec(&format!("DROP TABLE IF EXISTS {tmp}{oc}"))
                            .await?;
                        self.ch
                            .exec(&format!(
                                "CREATE TABLE {tmp}{oc} ({create_cols}) \
                                 ENGINE = {engine} ORDER BY {order_by}"
                            ))
                            .await?;
                        self.ch
                            .exec(&format!(
                                "ALTER TABLE {tmp} ATTACH PARTITION ID 'all' FROM {}",
                                self.staging_t
                            ))
                            .await?;
                        self.ch
                            .exec(&format!("DROP TABLE {}", self.staging_t))
                            .await?;
                        // The exists check above ran on the CONNECTED host only;
                        // an ON CLUSTER EXCHANGE needs both names on EVERY host.
                        // The shell is a no-op where final already exists and gets
                        // swapped out and dropped where it didn't.
                        self.ch
                            .exec(&format!(
                                "CREATE TABLE IF NOT EXISTS {}{oc} ({create_cols}) \
                                 ENGINE = {engine} ORDER BY {order_by}",
                                self.final_t
                            ))
                            .await?;
                        self.ch
                            .exec(&format!("EXCHANGE TABLES {tmp} AND {}{oc}", self.final_t))
                            .await?;
                        // Best-effort: a leftover shadow of old data never blocks
                        // the next run (it starts with DROP IF EXISTS), while
                        // failing HERE would abort before the state rows below
                        // are cleared — a stale-high watermark, silent skips.
                        let _ = self.ch.exec(&format!("DROP TABLE {tmp}{oc}")).await;
                    }
                } else {
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
                }
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
                // ORDER BY — guaranteed because staging mirrors the destination's
                // structure and keys (dest_state/prepare).
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
    fn ddl_validates_engine_family_and_shape() {
        let ok = |e: &str| ChDdl {
            engine: Some(e.into()),
            ..Default::default()
        };
        assert!(ok("MergeTree").validate().is_ok());
        assert!(ok("ReplacingMergeTree(ins_dt)").validate().is_ok());
        assert!(ok(
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}', v)"
        )
        .validate()
        .is_ok());
        assert!(ok("Log").validate().is_err());
        assert!(ok("MergeTree; DROP TABLE x").validate().is_err());
        let bad_ob = ChDdl {
            order_by: Some("id; DROP".into()),
            ..Default::default()
        };
        assert!(bad_ob.validate().is_err());
        // on_cluster without a Replicated engine = table everywhere, data nowhere
        let lonely = ChDdl {
            engine: Some("ReplacingMergeTree(v)".into()),
            on_cluster: Some("prod".into()),
            ..Default::default()
        };
        assert!(lonely.validate().is_err());
        let clustered = ChDdl {
            engine: Some("ReplicatedReplacingMergeTree(v)".into()),
            on_cluster: Some("prod".into()),
            ..Default::default()
        };
        assert!(clustered.validate().is_ok());
        // path-less Replicated without on_cluster can't get a unique zk path
        let pathless = ChDdl {
            engine: Some("ReplicatedReplacingMergeTree(v)".into()),
            ..Default::default()
        };
        assert!(pathless.validate().is_err());
        let explicit = ChDdl {
            engine: Some(
                "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}', v)"
                    .into(),
            ),
            ..Default::default()
        };
        assert!(explicit.validate().is_ok());
        assert!(explicit.has_explicit_zk_path());
        assert!(!pathless.has_explicit_zk_path());
        // clause smuggling: escape the (order_by) wrapper / trail the engine args
        let smuggle_ob = ChDdl {
            order_by: Some("id) ENGINE = Memory --".into()),
            ..Default::default()
        };
        assert!(smuggle_ob.validate().is_err()); // char whitelist kills '-', '='
        let unbalanced = ChDdl {
            order_by: Some("id) PARTITION BY (d".into()),
            ..Default::default()
        };
        assert!(unbalanced.validate().is_err());
        let trailing = ChDdl {
            engine: Some("ReplacingMergeTree(v) TTL d + 1".into()),
            ..Default::default()
        };
        assert!(trailing.validate().is_err());
        let bare_trailing = ChDdl {
            engine: Some("MergeTree PARTITION BY d".into()),
            ..Default::default()
        };
        assert!(bare_trailing.validate().is_err());
        let nested_ok = ChDdl {
            engine: Some("SummingMergeTree((a, b))".into()),
            ..Default::default()
        };
        assert!(nested_ok.validate().is_ok());
    }

    #[test]
    fn ddl_referenced_idents_skip_quoted_spans() {
        let ddl = ChDdl {
            engine: Some(
                "ReplicatedReplacingMergeTree('/clickhouse/tables/raw_account', '{replica}', ins_dt)"
                    .into(),
            ),
            order_by: Some("client_id, id".into()),
            ..Default::default()
        };
        let ids = ddl.referenced_idents();
        assert!(ids.contains("ins_dt"));
        assert!(ids.contains("client_id"));
        assert!(ids.contains("id"));
        // the zk path and macro live in quotes — never treated as columns
        assert!(!ids.contains("raw_account"));
        assert!(!ids.contains("replica"));
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
