//! # apitap-core
//!
//! The transfer engine behind [apitap](https://apitap.dev): move whole tables between
//! databases at wire speed, in bounded memory, from a laptop or a 256 MB container.
//!
//! Architecture: one generic [`driver`] runs every route's lifecycle (probe → wire-
//! format negotiation → staging → parallel span workers → count → atomic swap) over
//! per-database [`connectors`] that implement `Source` and/or `Sink`. Encoders stay
//! monomorphized — the fast lanes (raw binary COPY passthrough, binary→RowBinary
//! transcode, wire-decode→binary encode) compile to the same hot loops as the
//! hand-written routes they replaced.
//!
//! ```no_run
//! # async fn demo() -> apitap_core::Result<()> {
//! let report = apitap_core::transfer(
//!     "postgres://user:pass@src-host/db",
//!     "postgres://user:pass@dst-host/db",
//!     "public.events",
//!     &apitap_core::TransferOptions::default(),
//! )
//! .await?;
//! println!("{} rows in {} ms", report.rows, report.elapsed_ms);
//! # Ok(()) }
//! ```

mod bqparquet;
mod connectors;
mod driver;
mod error;
mod pgcopy;
mod plan;
mod rowbinary;

pub use error::{Error, Result};

/// How rows land in the destination table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Full refresh: load into staging, atomically swap the whole table (default).
    #[default]
    Replace,
    /// Incremental append: load only rows with `cursor >` the destination's current
    /// `max(cursor)` and add them to the existing table. Stateless — the watermark
    /// lives in the destination data itself. If the destination table doesn't exist
    /// yet, the run bootstraps as a full `Replace`.
    Append,
    /// Incremental upsert (Postgres destinations): rows with `cursor >=` the
    /// watermark are merged by the destination's primary key
    /// (`INSERT … ON CONFLICT DO UPDATE`). Bootstraps like `Append`.
    Merge,
}

impl std::str::FromStr for Mode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "replace" => Ok(Mode::Replace),
            "append" => Ok(Mode::Append),
            "merge" => Ok(Mode::Merge),
            other => Err(Error::InvalidInput(format!(
                "mode must be 'replace', 'append' or 'merge' (got '{other}')"
            ))),
        }
    }
}

/// Tuning for [`transfer`]. `Default` = auto: parallelism from the CPU count and the
/// cgroup memory budget, 4 MiB send coalescing, cursor auto-detected from the table's
/// integer primary key.
#[derive(Debug, Clone)]
pub struct TransferOptions {
    /// Concurrent range pipes. `None` = auto (route-specific CPU heuristic, capped by
    /// the container's memory). Each pipe holds one connection on both sides.
    pub parallel: Option<usize>,
    /// Numeric column used to split the table into ranges. `None` = auto-detect the
    /// single-column integer primary key; if there is none, Postgres sources fall back
    /// to TID ranges and other sources to a single stream.
    pub cursor: Option<String>,
    /// Destination table. `None` = same name as the source table.
    pub dest_table: Option<String>,
    /// Bytes to coalesce per send (floor 64 KiB).
    pub chunk_bytes: usize,
    /// Postgres destinations only. `false` loads into an UNLOGGED table — skipping WAL
    /// roughly halves the destination's write cost — and the swapped-in table REMAINS
    /// unlogged: Postgres truncates it during crash recovery until you run
    /// `ALTER TABLE … SET LOGGED`. Default `true` (fully durable). Other destinations
    /// ignore the flag. Incremental delta runs always stage UNLOGGED (their staging
    /// never becomes the final table) and never change the final table's durability;
    /// a bootstrap run is an effective replace and honors this flag.
    pub durable: bool,
    /// Replace (default), incremental append, or incremental merge — see [`Mode`].
    pub mode: Mode,
    /// ClickHouse destinations only: engine of the table apitap creates, any
    /// MergeTree-family spelling incl. Replicated, e.g.
    /// `"ReplacingMergeTree(ins_dt)"` or
    /// `"ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}', v)"`.
    /// `None` = plain `MergeTree`. Ignored when the destination table already exists
    /// (the existing table is the structural authority).
    pub engine: Option<String>,
    /// ClickHouse destinations only: ORDER BY clause of the created table
    /// (e.g. `"id"` or `"client_id, id"`). `None` = the cursor column, else `tuple()`.
    pub order_by: Option<String>,
    /// ClickHouse destinations only: run the final table's DDL `ON CLUSTER` this
    /// cluster. Requires a `Replicated*` engine (data reaches other replicas through
    /// replication, not through the insert).
    pub on_cluster: Option<String>,
}

impl Default for TransferOptions {
    fn default() -> Self {
        Self {
            parallel: None,
            cursor: None,
            dest_table: None,
            chunk_bytes: 4 * 1024 * 1024,
            durable: true,
            mode: Mode::Replace,
            engine: None,
            order_by: None,
            on_cluster: None,
        }
    }
}

/// The container/cgroup memory limit, if one is set (v2 `memory.max`, v1 fallback).
pub(crate) fn mem_limit_bytes() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let s = s.trim();
        if s != "max" {
            if let Ok(n) = s.parse::<u64>() {
                return Some(n);
            }
        }
    }
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(n) = s.trim().parse::<u64>() {
            if n < (1 << 60) {
                return Some(n);
            }
        }
    }
    None
}

/// Cap an AUTO-derived pipe count by the memory budget: each pipe holds a few
/// `chunk`-sized buffers in flight (encode buffer, channel slots, HTTP/COPY write
/// buffers — ~8× chunk measured worst-case), plus ~96 MiB of process overhead.
/// A 256 MB container at the CPU-derived 8 pipes was OOM-killed on the transcoding
/// route; this brings the default back inside the box. An EXPLICIT `parallel` from the
/// caller is never overridden.
pub(crate) fn mem_capped_parallel(requested: usize, chunk: usize) -> usize {
    match mem_limit_bytes() {
        Some(mem) => {
            let reserve: u64 = 96 * 1024 * 1024;
            let per_pipe = (chunk as u64) * 8;
            let allowed = mem.saturating_sub(reserve) / per_pipe.max(1);
            requested.min((allowed.max(1)) as usize)
        }
        None => requested,
    }
}

/// What a [`transfer`] did.
#[derive(Debug, Clone)]
pub struct TransferReport {
    /// Rows landed in the destination.
    pub rows: u64,
    /// Wall-clock duration of the whole transfer.
    pub elapsed_ms: u64,
    /// Concurrent pipes actually used (0 = empty source, 1 = single stream).
    pub parallel: usize,
}

/// One table's outcome inside a [`transfer_many`]/[`transfer_schema`] run.
#[derive(Debug, Clone)]
pub struct TableResult {
    /// Source table, as given (list) or as the catalog listed it (schema).
    /// The destination table always has the same name in a multi-table run.
    pub table: String,
    /// Rows landed (0 on error — a failed table commits nothing).
    pub rows: u64,
    /// Wall-clock for THIS table, from the moment it got its pipes.
    pub elapsed_ms: u64,
    /// Pipes this table ran with (its slice of the shared budget).
    pub parallel: usize,
    /// `None` = success. A failed table never poisons its siblings: each table
    /// keeps the single-table atomicity, so the destination holds either the
    /// previous table or the complete new one — never a partial.
    pub error: Option<String>,
}

/// What a multi-table transfer did. Per-table detail in `tables`; a table-level
/// failure lands there (not as an `Err`), so one bad table doesn't hide the
/// results of the ones that landed.
#[derive(Debug, Clone)]
pub struct MultiReport {
    /// Rows landed across all SUCCESSFUL tables.
    pub rows: u64,
    /// Wall-clock duration of the whole run.
    pub elapsed_ms: u64,
    /// The shared pipe budget — the same number a single-table run would get, so
    /// peak memory stays at the single-table ceiling no matter the table count.
    pub budget: usize,
    /// Per-table outcomes, in completion order.
    pub tables: Vec<TableResult>,
}

// Per-route pipe heuristics (all measured — see benchmarks/README.md):
// byte-relay pipes into a Postgres COPY are writer-bound (more pipes contend on the
// destination), ClickHouse pipes are mostly I/O with a light transcode (~0.1 core
// each), MySQL feeders pay real decode CPU.
fn pg_pg_parallel(cores: usize) -> usize {
    cores.clamp(1, 8)
}
fn to_ch_parallel(cores: usize) -> usize {
    (cores * 8).clamp(2, 32)
}
fn my_pg_parallel(cores: usize) -> usize {
    (cores * 4).clamp(2, 16)
}
// MySQL→MySQL: source decode is real CPU (like MySQL→PG) and the dest LOAD DATA
// is server-bound; scale with cores, capped for the small tiers.
fn my_my_parallel(cores: usize) -> usize {
    (cores * 4).clamp(2, 16)
}
// BigQuery pipes: PG read + gzip (~fast level, light CPU) + upload; each is one
// parallel load job. Upload bandwidth saturates quickly — more pipes past 8 just
// shard the same uplink.
fn to_bq_parallel(cores: usize) -> usize {
    (cores * 2).clamp(2, 8)
}

/// Copy `table` from the source database to the destination, atomically replacing the
/// destination table. The route is picked by the URL schemes; each pair negotiates the
/// fastest wire format both sides speak:
///
/// - `postgres://` → `postgres://` — raw binary `COPY` passthrough (no row decode).
/// - `postgres://` → `clickhouse://` — binary COPY transcoded in-flight to RowBinary
///   (text fallback for exotic types), swapped in with `EXCHANGE TABLES`.
/// - `mysql://` → `clickhouse://` — wire decode → RowBinary.
/// - `mysql://` → `postgres://` — wire decode → binary COPY.
///
/// Guarantees, on every route:
///
/// - **Atomic**: readers of the destination table never see a partial load; a mid-run
///   failure leaves the previous table exactly as it was.
/// - **0-row guard**: an empty source never wipes an existing destination table.
/// - **Bounded memory**: bytes stream with TCP backpressure; memory use is
///   `parallel × chunk_bytes` plus socket buffers, regardless of table size.
pub async fn transfer(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
) -> Result<TransferReport> {
    use connectors::{
        clickhouse::ChSink, mysql::MySqlSource, postgres::PgSink, postgres::PgSource,
    };
    use driver::Profile;

    let started = std::time::Instant::now();
    let src_scheme = src_url.split("://").next().unwrap_or("");
    let dst_scheme = dst_url.split("://").next().unwrap_or("");
    let dest_table = opts.dest_table.as_deref().unwrap_or(table);
    let source_id = driver::source_identity(src_url, table);
    if !matches!(dst_scheme, "clickhouse" | "clickhouse+https")
        && (opts.engine.is_some() || opts.order_by.is_some() || opts.on_cluster.is_some())
    {
        return Err(Error::InvalidInput(
            "engine/order_by/on_cluster only apply to ClickHouse destinations".into(),
        ));
    }
    let ch_ddl = connectors::clickhouse::ChDdl {
        engine: opts.engine.clone(),
        order_by: opts.order_by.clone(),
        on_cluster: opts.on_cluster.clone(),
    };

    match (src_scheme, dst_scheme) {
        ("postgres" | "postgresql", "postgres" | "postgresql") => {
            let profile = Profile {
                auto_parallel: pg_pg_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = PgSink::connect(dst_url, dest_table, parallel + 1, true).await?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        ("postgres" | "postgresql", "clickhouse" | "clickhouse+https") => {
            let profile = Profile {
                auto_parallel: to_ch_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = ChSink::connect(dst_url, dest_table, ch_ddl.clone())?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        ("postgres" | "postgresql", "bigquery") => {
            let profile = Profile {
                auto_parallel: to_bq_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = connectors::bigquery::BqSink::connect(dst_url, dest_table, parallel).await?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        ("mysql", "clickhouse" | "clickhouse+https") => {
            let profile = Profile {
                auto_parallel: to_ch_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            let sink = ChSink::connect(dst_url, dest_table, ch_ddl.clone())?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        ("mysql", "mysql") => {
            let profile = Profile {
                auto_parallel: my_my_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            let sink = connectors::mysql_sink::MySqlSink::connect(dst_url, dest_table).await?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        ("mysql", "postgres" | "postgresql") => {
            let profile = Profile {
                auto_parallel: my_pg_parallel,
                span_mult: 6,
            };
            let (chunk, parallel) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            // Serial sends: this feeder's per-row encode is CPU-heavy and overlapping
            // it with the send was measured slower (see PgSink::overlap_send).
            let sink = PgSink::connect(dst_url, dest_table, parallel + 1, false).await?;
            driver::run(
                &src, sink, table, opts, &profile, chunk, parallel, |n| parallel.min(n).max(1), started, &source_id,
            )
            .await
        }
        (s, d) => Err(Error::InvalidInput(format!(
            "unsupported route {s}:// → {d}:// (supported: postgres→postgres, \
             postgres→clickhouse, postgres→bigquery, mysql→clickhouse, \
             mysql→postgres, mysql→mysql)"
        ))),
    }
}

/// Which tables a multi-table run moves.
enum TableSel<'a> {
    /// An explicit list, every name verified against the source catalog up front.
    List(&'a [String]),
    /// Every base table (+ Postgres matviews) in one schema. `None` = the MySQL
    /// URL's database; Postgres has no such default and requires the name.
    Schema(Option<&'a str>),
}

/// Copy MANY tables in one call, through ONE resource budget.
///
/// The budget is exactly what a single-table [`transfer`] would get (route CPU
/// heuristic capped by the cgroup memory model — or an explicit `opts.parallel`),
/// shared across every table: big tables take many pipes, small ones take one, and
/// peak memory stays at the single-table ceiling regardless of table count. Tables
/// run largest-first over shared connection pools, so N small tables cost far less
/// than N separate `transfer` calls.
///
/// Destination tables keep their source names (`opts.dest_table` is rejected).
/// A failing table records its error in [`MultiReport::tables`] and releases its
/// pipes; the others keep going. `Err` is reserved for setup-level failures
/// (unknown table, bad URL, colliding destination names).
pub async fn transfer_many(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
) -> Result<MultiReport> {
    multi(src_url, dst_url, TableSel::List(tables), opts).await
}

/// Copy EVERY table of a schema (MySQL: a database) in one call — same budget,
/// scheduling and guarantees as [`transfer_many`]. apitap's own artifacts
/// (`*__apitap_staging`, `_apitap_state`) are excluded; Postgres also brings
/// materialized views and skips partition children (the parent covers them).
pub async fn transfer_schema(
    src_url: &str,
    dst_url: &str,
    schema: Option<&str>,
    opts: &TransferOptions,
) -> Result<MultiReport> {
    multi(src_url, dst_url, TableSel::Schema(schema), opts).await
}

/// Catalog + collision check → the job list, largest table first (LPT — the big
/// grabs happen while the budget is still full, which is the right greedy).
async fn jobs_for<S: driver::Source>(
    src: &S,
    sel: &TableSel<'_>,
    bare_dest: bool,
) -> Result<Vec<driver::TableJob>> {
    let cat = match sel {
        TableSel::List(ts) => {
            if ts.is_empty() {
                return Err(Error::InvalidInput("tables list is empty".into()));
            }
            src.catalog(None, Some(ts)).await?
        }
        TableSel::Schema(s) => src.catalog(*s, None).await?,
    };
    if cat.is_empty() {
        return Err(Error::InvalidInput(
            "no tables to transfer (empty schema?)".into(),
        ));
    }
    // Multi-table keeps source names, and ClickHouse/MySQL/BigQuery sinks drop the
    // schema qualifier — `a.events` and `b.events` would silently overwrite each
    // other there. Postgres keeps qualifiers, but `events` and `public.events` are
    // ONE relation under the default search_path (the same normalization
    // `source_identity` applies), so unqualified names normalize before the check.
    // Refuse up front instead of racing two loads into one staging table.
    let mut seen = std::collections::HashSet::new();
    for (name, _) in &cat {
        let key = if bare_dest {
            name.rsplit_once('.')
                .map_or(name.as_str(), |(_, t)| t)
                .to_string()
        } else if name.contains('.') {
            name.clone()
        } else {
            format!("public.{name}")
        };
        if !seen.insert(key.clone()) {
            return Err(Error::InvalidInput(format!(
                "two source tables land on the same destination table '{key}' — \
                 transfer them in separate calls"
            )));
        }
    }
    // A sibling named like another table's staging artifact would get DROPped by
    // that table's prepare mid-run — reserve the `__apitap_*` namespace up front.
    for key in &seen {
        for suffix in ["__apitap_staging", "__apitap_old"] {
            if let Some(base) = key.strip_suffix(suffix) {
                if seen.contains(base) {
                    return Err(Error::InvalidInput(format!(
                        "table '{key}' collides with the staging artifacts of \
                         '{base}' — transfer them in separate calls"
                    )));
                }
            }
        }
    }
    Ok(cat
        .into_iter()
        .map(|(table, est_rows)| driver::TableJob { table, est_rows })
        .collect())
}

async fn multi(
    src_url: &str,
    dst_url: &str,
    sel: TableSel<'_>,
    opts: &TransferOptions,
) -> Result<MultiReport> {
    use connectors::{
        clickhouse::ChSink, mysql::MySqlSource, mysql_sink::MySqlSink, postgres::PgSink,
        postgres::PgSource,
    };
    use driver::Profile;

    let started = std::time::Instant::now();
    let src_scheme = src_url.split("://").next().unwrap_or("");
    let dst_scheme = dst_url.split("://").next().unwrap_or("");
    if opts.dest_table.is_some() {
        return Err(Error::InvalidInput(
            "dest_table applies to single-table transfers — multi-table runs keep \
             the source names"
                .into(),
        ));
    }
    if !matches!(dst_scheme, "clickhouse" | "clickhouse+https")
        && (opts.engine.is_some() || opts.order_by.is_some() || opts.on_cluster.is_some())
    {
        return Err(Error::InvalidInput(
            "engine/order_by/on_cluster only apply to ClickHouse destinations".into(),
        ));
    }
    let ch_ddl = connectors::clickhouse::ChDdl {
        engine: opts.engine.clone(),
        order_by: opts.order_by.clone(),
        on_cluster: opts.on_cluster.clone(),
    };

    // Each arm: budget once, source pool once, sink resources once — then every
    // table runs the unchanged single-table lifecycle inside the shared budget.
    // Pools get a little headroom over the budget: pipes take most connections,
    // the +8 covers the short-lived control work (probe/DDL/finalize) of tables
    // waiting in flight.
    let (budget, results) = match (src_scheme, dst_scheme) {
        ("postgres" | "postgresql", "postgres" | "postgresql") => {
            let profile = Profile {
                auto_parallel: pg_pg_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, false).await?;
            let pool = PgSink::shared_pool(dst_url, budget + 8).await?;
            let mk = |t: String| {
                let pool = pool.clone();
                async move { Ok(PgSink::bind(pool, &t, true)) }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("postgres" | "postgresql", "clickhouse" | "clickhouse+https") => {
            let profile = Profile {
                auto_parallel: to_ch_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Parse once: the reqwest client inside ChConn is shared by every table.
            let ch = connectors::clickhouse::ChConn::parse(dst_url)?;
            let mk = |t: String| {
                let (ch, ddl) = (ch.clone(), ch_ddl.clone());
                async move { ChSink::bind(ch, &t, ddl) }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("postgres" | "postgresql", "bigquery") => {
            let profile = Profile {
                auto_parallel: to_bq_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Authenticate once (JWT sign + OAuth round-trip live in parse) — a
            // 200-table schema must not hit the token endpoint 200 times.
            let bq = connectors::bigquery::BqConn::parse(dst_url).await?;
            let mk = |t: String| {
                let bq = bq.clone();
                async move {
                    // BigQuery has no schema qualifiers — land `public.events` as `events`.
                    let bare = t.rsplit_once('.').map_or(t.as_str(), |(_, b)| b);
                    connectors::bigquery::BqSink::bind(bq, bare, budget)
                }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "clickhouse" | "clickhouse+https") => {
            let profile = Profile {
                auto_parallel: to_ch_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Parse once: the reqwest client inside ChConn is shared by every table.
            let ch = connectors::clickhouse::ChConn::parse(dst_url)?;
            let mk = |t: String| {
                let (ch, ddl) = (ch.clone(), ch_ddl.clone());
                async move { ChSink::bind(ch, &t, ddl) }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "mysql") => {
            let profile = Profile {
                auto_parallel: my_my_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            let shared = MySqlSink::shared_pool(dst_url)?;
            let mk = |t: String| {
                let shared = shared.clone();
                async move { Ok(MySqlSink::bind(shared, &t)) }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "postgres" | "postgresql") => {
            let profile = Profile {
                auto_parallel: my_pg_parallel,
                span_mult: 6,
            };
            let (chunk, budget) = driver::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, false).await?;
            let pool = PgSink::shared_pool(dst_url, budget + 8).await?;
            // overlap_send=false: same reasoning as the single-table arm.
            let mk = |t: String| {
                let pool = pool.clone();
                async move { Ok(PgSink::bind(pool, &t, false)) }
            };
            let r =
                driver::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        (s, d) => {
            return Err(Error::InvalidInput(format!(
                "unsupported route {s}:// → {d}:// (supported: postgres→postgres, \
                 postgres→clickhouse, postgres→bigquery, mysql→clickhouse, \
                 mysql→postgres, mysql→mysql)"
            )))
        }
    };

    Ok(MultiReport {
        rows: results
            .iter()
            .filter(|r| r.error.is_none())
            .map(|r| r.rows)
            .sum(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        budget,
        tables: results,
    })
}
