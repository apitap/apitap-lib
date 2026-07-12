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
                src, sink, table, opts, &profile, chunk, parallel, started, &source_id,
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
            let sink = ChSink::connect(dst_url, dest_table)?;
            driver::run(
                src, sink, table, opts, &profile, chunk, parallel, started, &source_id,
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
            let sink = ChSink::connect(dst_url, dest_table)?;
            driver::run(
                src, sink, table, opts, &profile, chunk, parallel, started, &source_id,
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
                src, sink, table, opts, &profile, chunk, parallel, started, &source_id,
            )
            .await
        }
        (s, d) => Err(Error::InvalidInput(format!(
            "unsupported route {s}:// → {d}:// (supported: postgres→postgres, \
             postgres→clickhouse, mysql→clickhouse, mysql→postgres)"
        ))),
    }
}
