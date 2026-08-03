//! # apitap-core
//!
//! The transfer engine behind [apitap](https://apitap.dev): move whole tables between
//! databases at wire speed, in bounded memory, from a laptop or a 256 MB container.
//!
//! Architecture: one generic [`driver`] runs every route's lifecycle (probe → wire-
//! format negotiation → staging → parallel span workers → count → atomic swap) over
//! per-database [`source`]/[`sink`] implementations that implement `Source` and/or `Sink`. Encoders stay
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

mod dialect;
mod read;
mod read_impl;
mod error;
mod aws;
mod logbased;
mod gcp;
mod pipeline;
mod plan;
mod sink;
mod source;
mod wire;

pub use error::{Error, Result};
pub use read::{read_schema, read_start, ArrowField, ReadHandle, ReadOptions};
pub use wire::arrowcol::{ArrowBatch, ArrowKind, FinishedCol};

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
    /// Batch CDC from a Postgres source's logical replication slot: every
    /// operation the WAL saw — inserts, updates (PK changes included),
    /// deletes, truncates — drained since the last run and applied set-based.
    /// The first run creates the slot and bootstraps with a full load pinned
    /// to the slot's exported snapshot (gap-free, duplicate-free); the LSN
    /// watermark commits in the same destination transaction as the data,
    /// and the slot is only advanced afterwards. See docs/design/log_based.md.
    LogBased,
}

impl std::str::FromStr for Mode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "replace" => Ok(Mode::Replace),
            "append" => Ok(Mode::Append),
            "merge" => Ok(Mode::Merge),
            "log_based" => Ok(Mode::LogBased),
            other => Err(Error::InvalidInput(format!(
                "mode must be 'replace', 'append', 'merge' or 'log_based' (got '{other}')"
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
    /// Bytes to coalesce per send (floor 64 KiB). `None` = auto: 4 MiB, and when
    /// the memory budget caps the pipe count below the CPU ask, the engine may
    /// thin the chunk to 2 MiB to buy pipes instead — measured 1.3-1.8× faster
    /// on memory-starved boxes (see `pipeline::knobs`). An explicit value is
    /// honored verbatim and never thinned.
    pub chunk_bytes: Option<usize>,
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
            chunk_bytes: None,
            durable: true,
            mode: Mode::Replace,
            engine: None,
            order_by: None,
            on_cluster: None,
        }
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
    if opts.mode == Mode::LogBased {
        return logbased::run::run_task(src_url, dst_url, table, opts).await;
    }
    pipeline::dispatch::single(src_url, dst_url, table, opts).await
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
    if opts.mode == Mode::LogBased {
        // The whole list is ONE slot group: one publication, one drain pass,
        // one shared watermark — never the bulk pipeline (which would run a
        // cursor merge and silently drop deletes).
        return logbased::run::run_many(src_url, dst_url, tables, opts).await;
    }
    pipeline::dispatch::multi(src_url, dst_url, pipeline::dispatch::TableSel::List(tables), opts).await
}

/// Copy MANY tables where EACH table names its own [`Mode`]. The call is
/// partitioned by mode: every bulk mode (replace/append/merge) runs through
/// the shared-budget pipeline as its own [`transfer_many`]-equivalent group,
/// and all `log_based` tables form ONE replication-slot group. Groups run
/// sequentially; per-table outcomes come back in ONE report (bulk tables
/// keep their per-table failure isolation; a CDC group fails as a unit —
/// its watermark is shared, so nothing is ever half-confirmed).
pub async fn transfer_tables(
    src_url: &str,
    dst_url: &str,
    specs: &[(String, Mode)],
    opts: &TransferOptions,
) -> Result<MultiReport> {
    let started = std::time::Instant::now();
    if specs.is_empty() {
        return Err(Error::InvalidInput("tables list is empty".into()));
    }
    {
        let mut names: Vec<&str> = specs.iter().map(|(t, _)| t.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        if names.len() != specs.len() {
            return Err(Error::InvalidInput(
                "the same table appears twice in the per-table mode map".into(),
            ));
        }
    }
    // Partition preserving the caller's order within each mode group.
    let group = |m: Mode| -> Vec<String> {
        specs.iter().filter(|(_, mm)| *mm == m).map(|(t, _)| t.clone()).collect()
    };
    let mut results: Vec<TableResult> = Vec::with_capacity(specs.len());
    let mut budget = 0usize;
    for mode in [Mode::Replace, Mode::Append, Mode::Merge, Mode::LogBased] {
        let tables = group(mode);
        if tables.is_empty() {
            continue;
        }
        let mut o = opts.clone();
        o.mode = mode;
        let r = if mode == Mode::LogBased {
            logbased::run::run_many(src_url, dst_url, &tables, &o).await?
        } else {
            pipeline::dispatch::multi(
                src_url,
                dst_url,
                pipeline::dispatch::TableSel::List(&tables),
                &o,
            )
            .await?
        };
        budget = budget.max(r.budget);
        results.extend(r.tables);
    }
    Ok(MultiReport {
        rows: results.iter().filter(|t| t.error.is_none()).map(|t| t.rows).sum(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        budget,
        tables: results,
    })
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
    if opts.mode == Mode::LogBased {
        return Err(Error::InvalidInput(
            "log_based over a whole schema isn't supported yet — pass the \
             tables explicitly (tables=[…] shares ONE replication slot)"
                .into(),
        ));
    }
    pipeline::dispatch::multi(src_url, dst_url, pipeline::dispatch::TableSel::Schema(schema), opts).await
}
