//! The `mode="log_based"` task runner (docs/design/log_based.md).
//!
//! Runs a GROUP of tables over ONE replication slot (a single table is a
//! group of one — its slot/state naming is unchanged from the single-table
//! era). First run (no state anywhere): create the slot with
//! EXPORT_SNAPSHOT, full-load every table pinned to that one snapshot
//! (gap-free AND duplicate-free for the whole group), store the slot's
//! consistent_point as each table's LSN watermark. Every later run: drain
//! the slot ONCE from the group's minimum watermark to a stop-line in
//! memory-bounded windows; each window applies per table together with the
//! watermark — and only after every member committed is Postgres told the
//! WAL may go. Recovery from a crash between two tables' commits is the
//! min-watermark re-drain: the apply paths are idempotent, so the tables
//! that were already ahead converge.

use crate::error::{Error, Result};
use crate::logbased::dest_bq::BqDest;
use crate::logbased::dest_ch::ChDest;
use crate::logbased::dest_ice::IceDest;
use crate::logbased::dest_my::MyDest;
use crate::logbased::dest_pg::{quote_ident, quote_table, PgDest};
use crate::logbased::drain::{drain, DrainOutcome, DrainSession};
use crate::logbased::mysource;
use crate::wire::pgoutput::lsn_from_string;
use crate::wire::walsender::Walsender;
use crate::{Mode, MultiReport, TableResult, TransferOptions, TransferReport};
use md5::{Digest as _, Md5};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

/// `partition_by`/`order_by` describe the CHANGELOG, not the table the
/// bootstrap's bulk load creates — and the changelog's meta columns
/// (`_apitap_lsn`, `_apitap_seq`, `_apitap_at`) do not exist until the rebuild
/// adds them. Passing the user's clauses through to the bulk DDL makes the
/// bootstrap fail on its own future schema ("Missing columns: '_apitap_seq'").
/// The bootstrap table is rebuilt seconds later anyway, so its own ORDER BY and
/// PARTITION BY are throwaway: strip them here and let the rebuild apply the
/// real ones.
fn strip_changelog_ddl(o2: &mut TransferOptions) {
    if o2.changelog {
        o2.order_by = None;
        o2.partition_by = None;
    }
}

/// The `partition_by` / `order_by` that apply to ONE table of a group.
///
/// Looked up by the argument the caller used, then by the resolved
/// `schema.table`, then by the bare name — so a group written as
/// `tables=["orders"]` and one written as `tables=["public.orders"]` accept the
/// same keys. Falls back to the run-wide value, then to the engine default.
fn ddl_for<'a>(
    opts: &'a TransferOptions,
    table_arg: &str,
    qualified: &str,
) -> (Option<&'a str>, Option<&'a str>) {
    let bare = qualified.rsplit_once('.').map_or(qualified, |(_, t)| t);
    let pick = |m: &'a std::collections::HashMap<String, String>, fallback: Option<&'a String>| {
        m.get(table_arg)
            .or_else(|| m.get(qualified))
            .or_else(|| m.get(bare))
            .or(fallback)
            .map(String::as_str)
    };
    (
        pick(&opts.partition_by_per_table, opts.partition_by.as_ref()),
        pick(&opts.order_by_per_table, opts.order_by.as_ref()),
    )
}

/// changelog=True needs a destination that is happy to be append-only and can
/// partition the log by time. The row-store replicas can technically hold one,
/// but a table that only ever grows is the wrong shape for them — they'd have
/// no partition to drop and no cheap latest-per-key.
const CHANGELOG_DEST_MSG: &str =
    "log_based: changelog=True lands in ClickHouse and BigQuery — Postgres, MySQL \
     and Iceberg destinations stay replicas (changelog=False)";

/// Everything about `changelog=True` that can be judged BEFORE any work: the
/// destination engine, and the DDL options the rebuild cannot carry.
///
/// It has to be here rather than in `bootstrap_finish`, which is where the
/// destination refusal used to live — that runs only after every table's full
/// load has already completed, so "refused loudly" meant "refused loudly, an
/// hour and a full table copy later".
pub(crate) fn precheck_changelog(dst_url: &str, opts: &TransferOptions) -> Result<()> {
    if !opts.changelog {
        return Ok(());
    }
    let engine = crate::pipeline::norm(scheme(dst_url));
    if !matches!(engine, "clickhouse" | "bigquery") {
        return Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into()));
    }
    // The ClickHouse rebuild issues its own CREATE/DROP/RENAME and cannot yet
    // reproduce a Replicated engine or an ON CLUSTER DDL. Silently demoting a
    // replicated table to a local MergeTree is the kind of thing nobody
    // notices until a replica is missing data, so refuse instead.
    if engine == "clickhouse" && (opts.engine.is_some() || opts.on_cluster.is_some()) {
        return Err(Error::InvalidInput(
            "log_based: changelog=True rebuilds the ClickHouse table itself and cannot \
             carry engine= or on_cluster= through that rebuild yet — a Replicated table \
             would come back as a local MergeTree. Drop those options, or use \
             changelog=False"
                .into(),
        ));
    }
    Ok(())
}

/// One destination engine for the log_based apply path.
enum Dest {
    Pg(PgDest),
    Ch(ChDest),
    My(MyDest),
    Ice(IceDest),
    Bq(BqDest),
}

impl Dest {
    async fn connect(dst_url: &str) -> Result<Dest> {
        match crate::pipeline::norm(scheme(dst_url)) {
            "postgres" => Ok(Dest::Pg(PgDest::connect(dst_url).await?)),
            "clickhouse" => Ok(Dest::Ch(ChDest::connect(dst_url)?)),
            "mysql" => Ok(Dest::My(MyDest::connect(dst_url)?)),
            "iceberg" => Ok(Dest::Ice(IceDest::connect(dst_url).await?)),
            "bigquery" => Ok(Dest::Bq(BqDest::connect(dst_url).await?)),
            other => Err(Error::InvalidInput(format!(
                "log_based: unsupported destination scheme '{other}' — use \
                 postgres, clickhouse, mysql, bigquery or iceberg"
            ))),
        }
    }

    async fn read_state(&self, dest_table: &str, source_id: &str) -> Result<Option<u64>> {
        match self {
            Dest::Pg(d) => d.read_state(dest_table, source_id).await,
            Dest::Ch(d) => d.read_state(dest_table, source_id).await,
            Dest::My(d) => d.read_state(dest_table, source_id).await,
            Dest::Ice(d) => d.read_state(dest_table, source_id).await,
            Dest::Bq(d) => d.read_state(dest_table, source_id).await,
        }
    }

    /// Once per table at run start, on a table that ALREADY has state: the
    /// destination's shape must match `changelog`. A fresh bootstrap builds the
    /// right shape by construction, and the apply path is too late — an empty
    /// drain never calls it. Only the analytical destinations have two shapes.
    async fn precheck_mode(&self, dest_table: &str, changelog: bool) -> Result<()> {
        match self {
            Dest::Ch(d) => d.precheck_mode(dest_table, changelog).await,
            Dest::Bq(d) => d.precheck_mode(dest_table, changelog).await,
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) => Ok(()),
        }
    }

    /// Can this table's changelog DDL actually be built? Asked for EVERY member
    /// of a group before ANY of them is rebuilt, so a bad expression costs
    /// nothing instead of tearing the group.
    async fn validate_changelog_ddl(
        &self,
        dest_table: &str,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        match self {
            Dest::Ch(d) => d.validate_changelog_ddl(dest_table, partition_by, order_by).await,
            Dest::Bq(d) => d.validate_changelog_ddl(dest_table, partition_by, order_by).await,
            // The row stores refuse changelog=True outright, upstream of this.
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) => Ok(()),
        }
    }

    /// Record which SOURCE SERVER a table's watermark belongs to. Stored as
    /// an ordinary state row under a reserved `source_id`, so no destination's
    /// state table has to grow a column and no deployment has to migrate.
    async fn write_marker(&self, dest_table: &str, source_id: &str, value: u64) -> Result<()> {
        match self {
            Dest::Pg(d) => d.write_marker(dest_table, source_id, value).await,
            Dest::Ch(d) => d.write_marker(dest_table, source_id, value).await,
            Dest::My(d) => d.write_marker(dest_table, source_id, value).await,
            Dest::Ice(d) => d.write_marker(dest_table, source_id, value).await,
            Dest::Bq(d) => d.write_marker(dest_table, source_id, value).await,
        }
    }

    /// Remove this table's watermark, so a failed group bootstrap really does
    /// leave "no state" the way its error message says it does.
    async fn clear_state(&self, dest_table: &str, source_id: &str) -> Result<()> {
        match self {
            Dest::Pg(d) => d.clear_state(dest_table, source_id).await,
            Dest::Ch(d) => d.clear_state(dest_table, source_id).await,
            Dest::My(d) => d.clear_state(dest_table, source_id).await,
            Dest::Bq(d) => d.clear_state(dest_table, source_id).await,
            Dest::Ice(d) => d.clear_state(dest_table, source_id).await,
        }
    }

    /// Destination-specific knobs for the bootstrap's full load.
    fn tweak_bootstrap_opts(&self, o2: &mut TransferOptions, pk_cols: &[String]) {
        match self {
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) | Dest::Bq(_) => {}
            Dest::Ch(d) => d.tweak_bootstrap_opts(o2, pk_cols),
        }
    }

    /// After the bootstrap's full load landed: add identity where the engine
    /// needs one, then write the state row.
    async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
        changelog: bool,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<()> {
        if changelog {
            return match self {
                Dest::Ch(d) => {
                    d.changelog_bootstrap_finish(
                        dest_table, source_id, pk_cols, lsn, rows, partition_by, order_by,
                    )
                    .await
                }
                Dest::Bq(d) => {
                    d.changelog_bootstrap_finish(
                        dest_table, source_id, pk_cols, lsn, rows, partition_by, order_by,
                    )
                    .await
                }
                _ => Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into())),
            };
        }
        match self {
            Dest::Pg(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
            Dest::Ch(d) => d.write_state(dest_table, source_id, lsn, rows).await,
            Dest::My(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
            Dest::Ice(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
            Dest::Bq(d) => d.bootstrap_finish(dest_table, source_id, pk_cols, lsn, rows).await,
        }
    }

    /// Apply for sources that carry no Postgres pool (the MySQL binlog
    /// path). Iceberg is refused at the gate, so the three engines that
    /// need nothing from the source are all that reach this.
    async fn apply_no_src(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
        changelog: bool,
    ) -> Result<u64> {
        if changelog {
            return match self {
                Dest::Ch(d) => {
                    d.apply_changelog(dest_table, qualified_src, pk_cols, outcome, source_id)
                        .await
                }
                Dest::Bq(d) => {
                    d.apply_changelog(dest_table, qualified_src, pk_cols, outcome, source_id)
                        .await
                }
                _ => Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into())),
            };
        }
        match self {
            Dest::Pg(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ch(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::My(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Bq(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ice(_) => Err(Error::InvalidInput(
                "log_based: iceberg needs a Postgres source in this release".into(),
            )),
        }
    }

    async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
        src: &PgPool,
        changelog: bool,
    ) -> Result<u64> {
        if changelog {
            return match self {
                Dest::Ch(d) => {
                    d.apply_changelog(dest_table, qualified_src, pk_cols, outcome, source_id)
                        .await
                }
                Dest::Bq(d) => {
                    d.apply_changelog(dest_table, qualified_src, pk_cols, outcome, source_id)
                        .await
                }
                _ => Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into())),
            };
        }
        match self {
            Dest::Pg(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ch(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::My(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Bq(d) => d.apply(dest_table, qualified_src, pk_cols, outcome, source_id).await,
            Dest::Ice(d) => {
                d.apply(dest_table, qualified_src, pk_cols, outcome, source_id, src).await
            }
        }
    }

    /// Per-window buffered-bytes budget for THIS destination. BigQuery is
    /// latency-bound (one job round-trip per window) so it fills a bigger
    /// window; the CPU-bound paths stay on the small default.
    fn cdc_window_bytes(&self) -> usize {
        match self {
            Dest::Bq(_) => cdc_bq_window_budget(),
            _ => cdc_window_budget(),
        }
    }

    /// How many of a group's tables may be applied AT ONCE within one window.
    /// `1` = the serial loop.
    ///
    /// Only BigQuery goes above 1: its per-table apply is a job round-trip that
    /// spends almost no local CPU, so a serial group pays the round-trip once
    /// per table. It is a bounded pool, not an unbounded fan-out — a 100-table
    /// group firing 100 load jobs and 100 MERGE transactions at once would trip
    /// BigQuery's concurrent-job limits and make every transaction contend on
    /// the shared `_apitap_state` row set. The SQL/CH/MySQL paths each buffer a
    /// staging body locally and are CPU-bound anyway, so they stay serial.
    fn apply_lanes(&self) -> usize {
        // One lever for every destination: the per-table bodies are slices of
        // the SAME window (each event belongs to one table), so N concurrent
        // applies materialize ~one window's worth of bodies in total — not N
        // windows. Safe concurrently: PgDest runs each apply in its own pooled
        // connection/tx, ChDest is an HTTP client with Mutex'd memo state, and
        // MySQL TEMPORARY tables are per-connection.
        if let Ok(n) =
            std::env::var("APITAP_CDC_APPLY_LANES").unwrap_or_default().parse::<usize>()
        {
            return n.clamp(1, 64);
        }
        match self {
            Dest::Bq(_) => bq_apply_lanes(),
            // CPU-bound paths default to serial until a measured win says
            // otherwise — at 0.5 core, concurrent CPU work shares the same
            // quota; only the round-trip waits can overlap.
            _ => 1,
        }
    }
}

/// Concurrent BigQuery applies per window (also the group bootstrap's fan-out).
/// Bounded by memory — each in-flight apply materializes its slice of the window
/// as an NDJSON body — and capped at 8, past which the wall is BigQuery's own
/// job scheduling and the transactions start contending. Lever:
/// `APITAP_BQ_APPLY_LANES`.
fn bq_apply_lanes() -> usize {
    const CAP: usize = 8;
    if let Ok(n) = std::env::var("APITAP_BQ_APPLY_LANES").unwrap_or_default().parse::<usize>() {
        return n.clamp(1, 64);
    }
    match crate::pipeline::mem_limit_bytes() {
        // ~12 MiB of materialized body per lane over a ~96 MiB working base.
        Some(m) => ((m.saturating_sub(96 << 20) / (12 << 20)) as usize).clamp(1, CAP),
        None => CAP,
    }
}

/// One member of the slot group.
struct TableCtx {
    /// The table argument as the caller gave it (drives the bootstrap's
    /// recursive `transfer`).
    table_arg: String,
    /// Resolved "schema.table" on the source (matches pgoutput's Relation).
    qualified: String,
    dest_table: String,
    pk_cols: Vec<String>,
    /// Per-table state key — the same identity a single-table run would use,
    /// so state stays discoverable regardless of grouping.
    source_id: String,
}

/// Bytes of row data one drain window may buffer before it must apply.
/// Derived from the cgroup memory limit so CDC fits the same containers the
/// bulk paths fit (44 MB is the measured single-pipe floor). The runtime
/// baseline (interpreter + tokio + connection buffers) is reserved first;
/// the collapsed window's REAL footprint runs ~3× the byte counter (hash
/// map + Vec overhead) and the apply renders one body copy — hence /8 of
/// what remains. No cgroup limit = 256 MiB, still bounded on big boxes.
/// A single transaction always buffers whole regardless of the budget
/// (pgoutput v1 only ships a transaction after its commit).
fn window_budget() -> usize {
    const DEFAULT: usize = 256 << 20;
    const BASELINE: u64 = 24 << 20;
    match crate::pipeline::mem_limit_bytes() {
        Some(m) => ((m.saturating_sub(BASELINE) / 8) as usize).clamp(2 << 20, DEFAULT),
        None => DEFAULT,
    }
}

/// `APITAP_CDC_WINDOW_BYTES`, if set, forces the per-window buffered-bytes
/// budget for every CDC drain (min 1 MiB).
fn env_window_override() -> Option<usize> {
    std::env::var("APITAP_CDC_WINDOW_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|n| n.max(1 << 20))
}

/// Per-window buffered-bytes budget for the CPU-bound apply paths (SQL / CH /
/// MySQL): keeps two overlapped windows resident under the memory cap, and a
/// bigger window buys them nothing (their apply cost is per-row CPU, not a fixed
/// per-window round-trip). 24 MiB ceiling.
fn cdc_window_budget() -> usize {
    env_window_override().unwrap_or_else(|| (window_budget() / 2).clamp(1 << 20, 24 << 20))
}

/// Per-window budget for the LATENCY-bound BigQuery apply: each window is one
/// load + MERGE job round-trip, so a bigger window amortizes that fixed cost.
/// Use the full per-window budget (no /2, no 24 MiB clamp) — measured to roughly
/// halve wall time vs the CPU-path default, while staying inside the 256 MiB cap.
fn cdc_bq_window_budget() -> usize {
    env_window_override().unwrap_or_else(window_budget)
}

pub(crate) async fn run_task(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
) -> Result<TransferReport> {
    let started = std::time::Instant::now();
    let (rows, parallel) =
        run_group(src_url, dst_url, std::slice::from_ref(&table.to_string()), opts, 1).await
            .map(|mut v| v.pop().expect("one result per table"))?;
    Ok(TransferReport {
        rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel,
    })
}

pub(crate) async fn run_many(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
) -> Result<MultiReport> {
    let started = std::time::Instant::now();
    if opts.dest_table.is_some() {
        return Err(Error::InvalidInput(
            "dest_table applies to single-table transfers — multi-table runs \
             keep the source names"
                .into(),
        ));
    }
    match opts.slots.unwrap_or(1) {
        0 => {
            return Err(Error::InvalidInput(
                "slots must be at least 1 (or omitted for the single-slot default)".into(),
            ))
        }
        1 => {}
        n => return run_sloted(src_url, dst_url, tables, opts, n, started).await,
    }
    let per_table_started = std::time::Instant::now();
    let results = run_group(src_url, dst_url, tables, opts, 1).await?;
    let elapsed = per_table_started.elapsed().as_millis() as u64;
    let budget = results.iter().map(|(_, p)| *p).max().unwrap_or(1);
    Ok(MultiReport {
        rows: results.iter().map(|(r, _)| *r).sum(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        budget,
        tables: tables
            .iter()
            .zip(results)
            .map(|(t, (rows, parallel))| TableResult {
                table: t.clone(),
                rows,
                elapsed_ms: elapsed,
                parallel,
                error: None,
            })
            .collect(),
    })
}

/// `slots=N`: split the tables into N groups, each with its OWN replication
/// slot, and run the N pipelines CONCURRENTLY — one OS thread per group, each
/// with its own current_thread runtime.
///
/// The thread-per-group shape is not incidental: it reproduces the measured
/// receipt (4 separate 1-slot processes → 278,947 changes/s where 1 slot did
/// 121,789) and keeps each pipeline's Bytes refcounts on one thread — the
/// uncontended-atomics design the CDC path already relies on (see the shim's
/// `run_cdc` rationale). What it buys is the SOURCE's parallelism: Postgres
/// decodes each slot in one walsender process, so N slots put N cores of
/// decoding to work where one slot pegs a single core.
///
/// Group assignment is a pure function of (sorted table list, N), so re-runs
/// resume the same slots. A failed group does not roll back the others: every
/// group owns an independent slot and watermark, its committed progress is
/// durable, and the retry resumes all groups from their own state.
async fn run_sloted(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
    slots: usize,
    started: std::time::Instant,
) -> Result<MultiReport> {
    if scheme(src_url) == "mysql" {
        return Err(Error::InvalidInput(
            "slots applies to Postgres sources — MySQL has ONE binlog stream, \
             so N groups would each decode the full binlog for no shared gain"
                .into(),
        ));
    }
    if !matches!(scheme(src_url), "postgres" | "postgresql") {
        return Err(Error::InvalidInput(format!(
            "log_based needs a Postgres source (logical replication) or a \
             MySQL source (binlog) — got '{}'",
            scheme(src_url)
        )));
    }
    // Duplicates across groups would give one table two slots that both
    // decode and both apply it; the single-group path catches duplicates via
    // colliding destination names, so mirror that here on the raw list.
    {
        let mut seen = tables.to_vec();
        seen.sort_unstable();
        seen.dedup();
        if seen.len() != tables.len() {
            return Err(Error::InvalidInput(
                "log_based: the tables list has duplicates".into(),
            ));
        }
    }
    let n = slots.min(tables.len());

    // Deterministic membership: sort, then cut into N contiguous chunks. Same
    // list + same `slots` → same groups → same hashed slot names → resume.
    let mut sorted = tables.to_vec();
    sorted.sort_unstable();
    let per = sorted.len().div_ceil(n);
    let groups: Vec<Vec<String>> = sorted.chunks(per).map(|c| c.to_vec()).collect();
    let n = groups.len(); // chunking can produce fewer groups than asked

    let mut handles = Vec::with_capacity(n);
    let mut rxs = Vec::with_capacity(n);
    for g in &groups {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (su, du, o2, g) = (src_url.to_string(), dst_url.to_string(), opts.clone(), g.clone());
        handles.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio current_thread runtime");
            let _ = tx.send(rt.block_on(run_group(&su, &du, &g, &o2, n)));
        }));
        rxs.push(rx);
    }
    // Collect every group before judging any: the others keep draining to
    // their own watermarks even when one fails, so their work is never wasted.
    let mut outcomes: Vec<Result<Vec<(u64, usize)>>> = Vec::with_capacity(n);
    for rx in rxs {
        outcomes.push(match rx.await {
            Ok(res) => res,
            Err(_) => Err(Error::Transfer(
                "slot group thread died before reporting".into(),
            )),
        });
    }
    for h in handles {
        let _ = h.join();
    }
    let elapsed_all = started.elapsed().as_millis() as u64;

    let mut by_table = std::collections::HashMap::new();
    let mut first_err = None;
    for (gi, out) in outcomes.into_iter().enumerate() {
        match out {
            Ok(v) => {
                for (t, r) in groups[gi].iter().zip(v) {
                    by_table.insert(t.clone(), r);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(Error::Transfer(format!(
                        "slot group {}/{n} ({} tables) failed: {e} — the other \
                         groups own independent slots and watermarks, their \
                         committed progress is durable, and a retry resumes \
                         every group from its own state",
                        gi + 1,
                        groups[gi].len(),
                    )));
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(MultiReport {
        rows: by_table.values().map(|(r, _)| *r).sum(),
        elapsed_ms: elapsed_all,
        budget: by_table.values().map(|(_, p)| *p).max().unwrap_or(1),
        tables: tables
            .iter()
            .map(|t| {
                let (rows, parallel) = by_table[t];
                TableResult {
                    table: t.clone(),
                    rows,
                    elapsed_ms: elapsed_all,
                    parallel,
                    error: None,
                }
            })
            .collect(),
    })
}

/// Run one slot group. Returns (rows, parallel) per table, caller order.
/// A failure anywhere fails the WHOLE group — the slot is only confirmed
/// past windows every member committed, so nothing is ever lost.
/// `budget_denom` divides the drain's per-window byte budget: with `slots=N`
/// there are N of these pipelines in ONE process, and the recorded memory
/// ceiling (bounded by the largest transaction) must hold for their SUM.
async fn run_group(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
    budget_denom: usize,
) -> Result<Vec<(u64, usize)>> {
    if tables.is_empty() {
        return Err(Error::InvalidInput("tables list is empty".into()));
    }
    precheck_changelog(dst_url, opts)?;
    if scheme(src_url) == "mysql" {
        return run_group_mysql(src_url, dst_url, tables, opts).await;
    }
    if !matches!(scheme(src_url), "postgres" | "postgresql") {
        return Err(Error::InvalidInput(format!(
            "log_based needs a Postgres source (logical replication) or a \
             MySQL source (binlog) — got '{}'",
            scheme(src_url)
        )));
    }
    let dest = Dest::connect(dst_url).await?;

    let src = PgPoolOptions::new()
        .max_connections(2)
        .connect(src_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: source connect: {e}")))?;

    // Resolve every member: real (schema, name) + PK on the source.
    let single = tables.len() == 1;
    let mut ctxs = Vec::with_capacity(tables.len());
    for t in tables {
        let (schema_name, bare) = resolve_table(&src, t).await?;
        let qualified = format!("{schema_name}.{bare}");
        let pk_cols = pk_columns(&src, &qualified).await?;
        if pk_cols.is_empty() {
            return Err(Error::InvalidInput(format!(
                "log_based: {qualified} has no primary key — updates/deletes \
                 need an identity. Add a PK (or ask for REPLICA IDENTITY FULL \
                 support)"
            )));
        }
        // REPLICA IDENTITY NOTHING is refused here rather than when the
        // first Relation message arrives. By then the bootstrap has already
        // run a full load and written a watermark, so the run fails AFTER
        // moving data — and on a group, after some members committed. The
        // catalog answers this question before anything is touched.
        let ident: Option<String> = sqlx::query_scalar(
            "SELECT relreplident::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(&schema_name)
        .bind(&bare)
        .fetch_optional(&src)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: replica identity probe: {e}")))?;
        if ident.as_deref() == Some("n") {
            return Err(Error::InvalidInput(format!(
                "log_based: {qualified} has REPLICA IDENTITY NOTHING — its updates \
                 and deletes reach the WAL with no key, so there is no way to say \
                 WHICH row changed. Run: ALTER TABLE {qualified} REPLICA IDENTITY \
                 DEFAULT (or FULL), then re-run."
            )));
        }
        let dest_table = if single {
            opts.dest_table.clone().unwrap_or_else(|| bare.clone())
        } else {
            bare.clone()
        };
        ctxs.push(TableCtx {
            table_arg: t.clone(),
            qualified,
            dest_table,
            source_id: crate::pipeline::source_identity(src_url, t),
            pk_cols,
        });
    }
    {
        let mut dests: Vec<&str> = ctxs.iter().map(|c| c.dest_table.as_str()).collect();
        dests.sort_unstable();
        dests.dedup();
        if dests.len() != ctxs.len() {
            return Err(Error::InvalidInput(
                "log_based: two tables resolve to the same destination name".into(),
            ));
        }
    }

    // Stable slot/publication names. A group of ONE keeps the historical
    // single-table naming so existing slots stay owned; bigger groups hash
    // their sorted membership — changing membership means a NEW slot (and a
    // loud partial-state error below until the old state is cleared).
    let slot = if single {
        let c = &ctxs[0];
        format!("apitap_{}", hex_prefix(&format!("{}\u{1f}{}", c.source_id, c.dest_table), 12))
    } else {
        let mut pairs: Vec<String> = ctxs
            .iter()
            .map(|c| format!("{}\u{1f}{}", c.source_id, c.dest_table))
            .collect();
        pairs.sort_unstable();
        format!("apitap_g{}", hex_prefix(&pairs.join("\u{1e}"), 11))
    };
    let publication = format!("{slot}_pub");

    let qualified_all: Vec<&str> = ctxs.iter().map(|c| c.qualified.as_str()).collect();
    ensure_publication(&src, &publication, &qualified_all).await?;

    // Per-table watermarks: all absent = fresh bootstrap; all present = drain
    // from the group minimum; a mix is a torn group — refuse loudly.
    let mut wms = Vec::with_capacity(ctxs.len());
    for c in &ctxs {
        let wm = dest.read_state(&c.dest_table, &c.source_id).await?;
        if wm.is_some() {
            dest.precheck_mode(&c.dest_table, opts.changelog).await?;
        }
        wms.push(wm);
    }
    let have: Vec<&TableCtx> =
        ctxs.iter().zip(&wms).filter(|(_, w)| w.is_some()).map(|(c, _)| c).collect();
    if !have.is_empty() && have.len() != ctxs.len() {
        let missing: Vec<&str> = ctxs
            .iter()
            .zip(&wms)
            .filter(|(_, w)| w.is_none())
            .map(|(c, _)| c.dest_table.as_str())
            .collect();
        return Err(Error::InvalidInput(format!(
            "log_based: the group has state for {} of {} tables (missing: {}) — \
             group membership changed, or a bootstrap was interrupted. Clear \
             the group's state rows (and drop slot {slot}) to re-bootstrap",
            have.len(),
            ctxs.len(),
            missing.join(", ")
        )));
    }

    if have.is_empty() {
        bootstrap_group(src_url, dst_url, opts, &dest, &src, &slot, &ctxs).await
    } else {
        let wm = wms.iter().map(|w| w.expect("all present")).min().expect("nonempty");
        drain_group(
            src_url,
            &src,
            dest,
            &slot,
            &publication,
            &ctxs,
            wm,
            opts.changelog,
            budget_denom,
        )
        .await
    }
}

// ── first run: one slot, every table pinned to its snapshot ─────────────────

// ── MySQL source: same shape, binlog instead of a slot ─────────────────────

/// `mode="log_based"` from MySQL. Bootstrap = coordinate-then-full-load
/// (the overlap is safe because every apply is idempotent by PK); later
/// runs stream the binlog in windows and apply each one with its watermark.
async fn run_group_mysql(
    src_url: &str,
    dst_url: &str,
    tables: &[String],
    opts: &TransferOptions,
) -> Result<Vec<(u64, usize)>> {
    use crate::logbased::myrun;

    let dest = Dest::connect(dst_url).await?;
    if matches!(dest, Dest::Ice(_)) {
        return Err(Error::InvalidInput(
            "log_based: mysql → iceberg needs a Postgres source — postgres, \
             clickhouse, mysql and bigquery destinations work from MySQL today"
                .into(),
        ));
    }
    let pool = myrun::control_pool(src_url).await?;
    let default_db = src_url
        .rsplit('/')
        .next()
        .and_then(|s| s.split('?').next())
        .unwrap_or("")
        .to_string();
    let single = tables.len() == 1;
    let ctxs = myrun::resolve(&pool, &default_db, tables, single, opts).await?;

    // Which server do the stored coordinates belong to?
    //
    // A binlog position means nothing outside the server that issued it, and
    // nothing in a connection URL says which server answered it: a promoted
    // replica, a restored backup, or a DNS record moved during a failover all
    // answer the same address. The "position is AHEAD of the server" guard
    // catches a new server that happens to be BEHIND the stored mark; this
    // catches the other half, where the new server is ahead and the resume
    // looks perfectly ordinary while reading someone else's changes.
    //
    // A table with no marker adopts the server it is reading now — that is
    // every table bootstrapped before this check existed, and pretending to
    // know what they were reading last month would be a lie. From the first
    // run onward, a switch is refused.
    //
    // The CHECK is here — before anything moves — but the WRITE is at the end
    // of a successful run. A bootstrap's full load runs in Replace mode, and
    // Replace clears every state row for its destination table (it has to:
    // the rows those watermarks described are gone). A marker written up here
    // would be deleted by the bootstrap that follows it, and the next run
    // would adopt whatever server it found — measured exactly that way.
    let server = mysource::server_identity(&pool).await?;
    let mut adopt: Vec<(String, String)> = Vec::new();
    for c in &ctxs {
        let marker_id = format!("server-identity:{}", c.source_id);
        match dest.read_state(&c.dest_table, &marker_id).await? {
            Some(prev) if prev != server => {
                return Err(Error::InvalidInput(format!(
                    "log_based: {} was last drained from a DIFFERENT MySQL server than \
                     the one this URL now reaches. A binlog (file, position) is only \
                     meaningful on the server that wrote it, so resuming here would \
                     read unrelated changes at the stored coordinate and report \
                     success. If the source really did move — a promoted replica, a \
                     restored backup, a failover — clear this table's apitap state on \
                     the destination and re-run, which bootstraps it against the new \
                     server. If it did not, check that the URL still points where you \
                     think it does.",
                    c.qualified
                )));
            }
            Some(_) => {}
            None => adopt.push((c.dest_table.clone(), marker_id)),
        }
    }

    /// Record the source server for every table that did not have one yet.
    /// Called only after the run's own state is durable, so nothing that
    /// clears state rows can run after it.
    async fn stamp(dest: &Dest, adopt: &[(String, String)], server: u64) -> Result<()> {
        for (table, marker) in adopt {
            dest.write_marker(table, marker, server).await?;
        }
        Ok(())
    }

    // State arbitration mirrors the Postgres path: all-absent bootstraps,
    // all-present drains, a mix is a torn group.
    let mut marks = Vec::with_capacity(ctxs.len());
    for c in &ctxs {
        let m = dest.read_state(&c.dest_table, &c.source_id).await?;
        if m.is_some() {
            dest.precheck_mode(&c.dest_table, opts.changelog).await?;
        }
        marks.push(m);
    }
    let present = marks.iter().filter(|m| m.is_some()).count();
    if present != 0 && present != marks.len() {
        let missing: Vec<&str> = ctxs
            .iter()
            .zip(&marks)
            .filter(|(_, m)| m.is_none())
            .map(|(c, _)| c.qualified.as_str())
            .collect();
        return Err(Error::Transfer(format!(
            "log_based: torn group — these members have no watermark: {}. \
             Clear the group's state rows to re-bootstrap all of them",
            missing.join(", ")
        )));
    }

    if present == 0 {
        let su = src_url.to_string();
        let du = dst_url.to_string();
        let (mark, out) = myrun::bootstrap(&pool, &ctxs, opts, |table_arg, o2| {
            let su = su.clone();
            let du = du.clone();
            async move {
                let r = Box::pin(crate::transfer(&su, &du, &table_arg, &o2)).await?;
                Ok((r.rows, r.parallel))
            }
        })
        .await?;
        if opts.changelog {
            for c in ctxs.iter() {
                let (pb, ob) = ddl_for(opts, &c.table_arg, &c.qualified);
                dest.validate_changelog_ddl(&c.dest_table, pb, ob).await?;
            }
        }
        for (c, (rows, _)) in ctxs.iter().zip(&out) {
            let (pb, ob) = ddl_for(opts, &c.table_arg, &c.qualified);
            if let Err(e) = dest
                .bootstrap_finish(&c.dest_table, &c.source_id, &c.pk_cols, mark, *rows,
                    opts.changelog, pb, ob)
                .await
            {
                // Same rollback as the Postgres group: a half-written group is
                // worse than no group, because the next run refuses it.
                for c in ctxs.iter() {
                    let _ = dest.clear_state(&c.dest_table, &c.source_id).await;
                }
                return Err(e);
            }
        }
        stamp(&dest, &adopt, server).await?;
        return Ok(out);
    }

    // Drain from the group minimum — members ahead converge idempotently.
    let wm = marks.iter().flatten().copied().min().unwrap_or(0);
    let seed = ctxs
        .iter()
        .map(|c| c.source_id.as_str())
        .collect::<Vec<_>>()
        .join("\x1e");
    let budget = dest.cdc_window_bytes();
    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    // One counter PER TABLE. A single group-wide counter handed the same
    // total to every member, so a 10-table group reported 10× the changes it
    // actually applied (the data was right; the number was not).
    let rows_applied: Vec<std::cell::Cell<u64>> =
        ctxs.iter().map(|_| std::cell::Cell::new(0u64)).collect();

    myrun::drain_windows(
        src_url,
        &pool,
        &ctxs,
        wm,
        &seed,
        30,
        budget,
        opts.changelog,
        |outcome| {
            let dest = &dest;
            let ctxs = &ctxs;
            let rows_applied = &rows_applied;
            async move {
                let end = outcome.end_lsn;
                for (c, acc) in ctxs.iter().zip(rows_applied.iter()) {
                    // Every member applies — a table with no traffic in this
                    // window still advances its watermark.
                    let n = dest
                        .apply_no_src(
                            &c.dest_table, &c.qualified, &c.pk_cols, &outcome, &c.source_id,
                            opts.changelog,
                        )
                        .await?;
                    acc.set(acc.get() + n);
                    crate::progress::add_rows(n);
                }
                // A long catch-up drains window after window; the number says
                // which one is running, so a stalled run is distinguishable
                // from a slow one.
                crate::progress::next_window();
                if dbg {
                    eprintln!("[my cdc] window applied → watermark {end}");
                }
                Ok(end)
            }
        },
    )
    .await?;

    stamp(&dest, &adopt, server).await?;
    Ok(rows_applied.iter().map(|a| (a.get(), 1)).collect())
}

async fn bootstrap_group(
    src_url: &str,
    dst_url: &str,
    opts: &TransferOptions,
    dest: &Dest,
    src: &PgPool,
    slot: &str,
    ctxs: &[TableCtx],
) -> Result<Vec<(u64, usize)>> {
    // A slot with no matching state is a leftover from an aborted bootstrap —
    // start fresh (refuse if something is actively draining it).
    let stale: Option<(bool,)> =
        sqlx::query_as("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot)
            .fetch_optional(src)
            .await
            .map_err(db_err)?;
    if let Some((active,)) = stale {
        if active {
            return Err(Error::Transfer(format!(
                "log_based: slot {slot} is ACTIVE but the destination has no \
                 state — another process is using it; stop it first"
            )));
        }
        sqlx::query("SELECT pg_drop_replication_slot($1)")
            .bind(slot)
            .execute(src)
            .await
            .map_err(db_err)?;
    }

    let mut ws = Walsender::connect(src_url).await?;
    let rows = ws
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT \"{slot}\" LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let consistent_point = rows
        .first()
        .and_then(|r| r.get(1).cloned().flatten())
        .ok_or_else(|| Error::Transfer("log_based: slot creation returned no LSN".into()))?;
    let snapshot = rows
        .first()
        .and_then(|r| r.get(2).cloned().flatten())
        .ok_or_else(|| Error::Transfer("log_based: slot creation exported no snapshot".into()))?;
    let lsn = lsn_from_string(&consistent_point)?;

    // Full loads pinned to the ONE exported snapshot — every member sees the
    // same instant, so the whole group hands off gap-free. Sequential: each
    // table still gets the full pipe budget, and per-table destination knobs
    // (ClickHouse ORDER BY = that table's PK) stay per-table. The walsender
    // session stays open for the whole load (idle; the slot retains WAL).
    let sep = if src_url.contains('?') { '&' } else { '?' };
    let pinned_url = format!("{src_url}{sep}__apitap_snapshot={snapshot}");
    // BigQuery's per-table bootstrap is a chain of load/copy JOBS (I/O, ~0 local
    // CPU) — running a group's tables serially made a 10-table load pay 10× the
    // job latency. Fan them out (bounded, so N concurrent source COPYs stay
    // inside the memory cap); the CPU-bound SQL/CH/MySQL loads stay serial. Each
    // load is an independent replace from the ONE pinned snapshot, so gap-free
    // and duplicate-free are unchanged.
    use futures::stream::StreamExt as _;
    let concurrency = dest.apply_lanes();
    let drop_slot = || async {
        let _ = sqlx::query("SELECT pg_drop_replication_slot($1)").bind(slot).execute(src).await;
    };
    let loaded: Vec<Result<(u64, usize)>> = futures::stream::iter(ctxs.iter().map(|c| {
        let pinned_url = &pinned_url;
        let dest = &dest;
        async move {
            let mut o2 = opts.clone();
            o2.mode = Mode::Replace;
            o2.dest_table = Some(c.dest_table.clone());
            // `slots` belongs to the CDC group that spawned this bootstrap;
            // the per-table full-load leg is a bulk transfer, which rejects
            // it loudly if it leaks through.
            o2.slots = None;
            strip_changelog_ddl(&mut o2);
            dest.tweak_bootstrap_opts(&mut o2, &c.pk_cols);
            let r = Box::pin(crate::transfer(pinned_url, dst_url, &c.table_arg, &o2)).await?;
            Ok((r.rows, r.parallel))
        }
    }))
    .buffered(concurrency)
    .collect()
    .await;
    let mut out = Vec::with_capacity(ctxs.len());
    for (c, r) in ctxs.iter().zip(loaded) {
        match r {
            Ok(v) => out.push(v),
            Err(e) => {
                // Failed bootstrap leaves no STATE behind: drop the slot;
                // already-loaded members are plain tables the re-run replaces.
                drop_slot().await;
                return Err(Error::Transfer(format!(
                    "log_based: bootstrap of {} failed (group rolled back to \
                     no-state; re-run re-bootstraps all): {e}",
                    c.qualified
                )));
            }
        }
    }
    ws.stop_replication().await.ok();

    // Finish (cluster large targets, write the watermark) — also job-bound on
    // BigQuery, so the same bounded fan-out.
    // Validate EVERY member's changelog DDL before rebuilding ANY of them. The
    // rebuild writes a state row, so a member whose expression is wrong used to
    // fail after its siblings had already committed theirs — a torn group the
    // next run refuses, contradicting the promise made above. Validation is a
    // parse against the real, just-loaded columns, so a typo or a column only
    // some members own is refused with nothing written.
    if opts.changelog {
        for c in ctxs.iter() {
            let (pb, ob) = ddl_for(opts, &c.table_arg, &c.qualified);
            if let Err(e) = dest.validate_changelog_ddl(&c.dest_table, pb, ob).await {
                drop_slot().await;
                return Err(e);
            }
        }
    }

    let fins: Vec<Result<()>> = futures::stream::iter(ctxs.iter().zip(&out).map(|(c, (rows, _))| {
        let dest = &dest;
        async move {
            let (pb, ob) = ddl_for(opts, &c.table_arg, &c.qualified);
            dest.bootstrap_finish(&c.dest_table, &c.source_id, &c.pk_cols, lsn, *rows,
                opts.changelog, pb, ob).await
        }
    }))
    .buffered(concurrency)
    .collect()
    .await;
    if let Some(e) = fins.into_iter().find_map(Result::err) {
        // Make the rollback the message promises real: a member that DID write
        // its watermark must lose it, or the group is torn.
        for c in ctxs.iter() {
            let _ = dest.clear_state(&c.dest_table, &c.source_id).await;
        }
        drop_slot().await;
        return Err(e);
    }
    Ok(out)
}

// ── every later run: ONE windowed drain, applies fan out per table ──────────

#[allow(clippy::too_many_arguments)]
async fn drain_group(
    src_url: &str,
    src: &PgPool,
    dest: Dest,
    slot: &str,
    publication: &str,
    ctxs: &[TableCtx],
    wm: u64,
    changelog: bool,
    budget_denom: usize,
) -> Result<Vec<(u64, usize)>> {
    // How much WAL is this slot holding on the SOURCE, and is that safe?
    //
    // A logical slot keeps every WAL segment its consumer has not confirmed.
    // That is the guarantee CDC is built on, and also the way apitap can hurt
    // a production database: if the schedule stops — paused DAG, disabled cron,
    // a table nobody drains any more — the slot keeps holding WAL until the
    // source's disk is full. Postgres will not choose your availability over
    // the slot's promise unless you tell it to (`max_slot_wal_keep_size`).
    //
    // So every run reports the number, and says something when it is large.
    // Refusing would be wrong: a big backlog is exactly when the drain MUST
    // run. Silence would also be wrong, which is what this used to be.
    slot_wal_report(src, slot).await;

    // Reconcile against the slot before doing anything.
    let slot_row: Option<(Option<String>, bool)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text, active FROM pg_replication_slots \
         WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_optional(src)
    .await
    .map_err(db_err)?;
    let Some((confirmed, active)) = slot_row else {
        return Err(Error::Transfer(format!(
            "log_based: destination has a watermark but slot {slot} is GONE on \
             the source — WAL continuity is lost. Re-run after clearing the \
             state row (the next run re-bootstraps with a full load)"
        )));
    };
    if active {
        return Err(Error::Transfer(format!(
            "log_based: slot {slot} is already active — another drain is running"
        )));
    }
    if let Some(c) = confirmed {
        let c = lsn_from_string(&c)?;
        if wm < c {
            return Err(Error::Transfer(format!(
                "log_based: destination watermark {wm} is BEHIND the slot's \
                 confirmed LSN {c} — that WAL is gone; state was tampered with \
                 or restored from backup. Clear the state row to re-bootstrap"
            )));
        }
    }

    let stop_line: (String,) = sqlx::query_as("SELECT pg_current_wal_lsn()::text")
        .fetch_one(src)
        .await
        .map_err(db_err)?;
    let stop_line = lsn_from_string(&stop_line.0)?;

    let mut key_cols = HashMap::new();
    for c in ctxs {
        key_cols.insert(c.qualified.clone(), c.pk_cols.clone());
    }

    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    // Two windows are resident under overlap (one applying, one draining) —
    // the budget halves so peak memory stays at the single-window ceiling.
    // The cap matters on BIG boxes too: overlap only pays while windows
    // rotate, so past ~24 MiB of buffered rows the marginal collapse-dedup
    // is worth less than hiding the apply under the next drain.
    // With `slots=N` this pipeline is one of N in the SAME process/cgroup, so
    // the per-window budget shards by N (floor 1 MiB) to keep the process's
    // recorded memory ceiling intact.
    let budget = (dest.cdc_window_bytes() / budget_denom.max(1)).max(1 << 20);
    let mut ws = Walsender::connect(src_url).await?;
    ws.start_replication(slot, wm, publication).await?;

    // Overlapped windows (ape-dts's daemon trick, batch-shaped): the drain
    // task keeps the walsender and decodes window N+1 WHILE a spawned apply
    // task lands window N. The slot is confirmed only after the apply task
    // reports a window fully committed (watch channel carries the last
    // committed end_lsn back) — never past unapplied WAL, exactly like the
    // serial loop, just off the clock.
    let (win_tx, mut win_rx) = tokio::sync::mpsc::channel::<DrainOutcome>(1);
    let (applied_tx, mut applied_rx) = tokio::sync::watch::channel::<u64>(wm);
    let actxs: Vec<(String, String, Vec<String>, String)> = ctxs
        .iter()
        .map(|c| {
            (c.dest_table.clone(), c.qualified.clone(), c.pk_cols.clone(), c.source_id.clone())
        })
        .collect();
    let apool = src.clone();
    let clog = changelog;
    let apply_task: tokio::task::JoinHandle<Result<Vec<u64>>> = tokio::spawn(async move {
        let mut rows_per = vec![0u64; actxs.len()];
        while let Some(o) = win_rx.recv().await {
            let t_apply = std::time::Instant::now();
            let lanes = dest.apply_lanes();
            if let Dest::Bq(d) = &dest {
                // BigQuery: stage every table concurrently (one load job each),
                // then commit the whole group's MERGEs + watermarks in as few
                // script jobs as possible. A MERGE carries ~7.3 s of fixed job
                // overhead, so paying it once per GROUP instead of once per
                // TABLE is the biggest lever the profile found.
                let applied = if clog {
                    d.apply_group_changelog(&actxs, &o, lanes).await?
                } else {
                    d.apply_group(&actxs, &o, lanes).await?
                };
                for (i, n) in applied.into_iter().enumerate() {
                    rows_per[i] += n;
                }
            } else if lanes > 1 && actxs.len() > 1 {
                // BigQuery apply is one job round-trip per table (I/O, ~0 local
                // CPU) — applying a group's tables SERIALLY made a 10-table
                // window pay 10× the round-trip. Run them through a BOUNDED
                // pool: `lanes` applies in flight, the rest queued, each
                // completion pulling the next. Unbounded would melt a 100-table
                // group against BigQuery's job limits and make every
                // transaction contend on the shared state rows. The targets are
                // distinct tables; the shared `_apitap_state` INSERT is covered
                // by cdc_script's concurrent-update retry.
                use futures::stream::{StreamExt as _, TryStreamExt as _};
                // Indices, not references: a closure taking `&(..)` and
                // returning an async block trips higher-ranked lifetime
                // inference ("FnOnce is not general enough").
                let (dref, aref, oref, cref) = (&dest, &apool, &o, &actxs);
                let applied: Vec<(usize, u64)> = futures::stream::iter(0..cref.len())
                    .map(|i| async move {
                        let (dt, q, pk, sid) = &cref[i];
                        dref.apply(dt, q, pk, oref, sid, aref, clog).await.map(|n| (i, n))
                    })
                    .buffer_unordered(lanes)
                    .try_collect()
                    .await?;
                for (i, n) in applied {
                    rows_per[i] += n;
                }
            } else {
                for (i, (dest_table, qualified, pk_cols, source_id)) in actxs.iter().enumerate() {
                    rows_per[i] +=
                        dest.apply(dest_table, qualified, pk_cols, &o, source_id, &apool, clog).await?;
                }
            }
            if std::env::var("APITAP_DEBUG").is_ok() {
                let events: u64 = o.tables.values().map(|c| c.events).sum();
                eprintln!(
                    "[log_based] applied lsn={} events={events} in {:.1}s",
                    o.end_lsn,
                    t_apply.elapsed().as_secs_f64(),
                );
            }
            // Receiver may be gone on a drain-side abort — nothing to do.
            let _ = applied_tx.send(o.end_lsn);
        }
        Ok(rows_per)
    });

    let mut sess = DrainSession::default();
    let mut cur = wm;
    let mut windows = 0u32;
    // The previous window's end_lsn: sent to the applier, not yet confirmed.
    let mut pending: Option<u64> = None;
    let mut drain_err: Option<Error> = None;
    loop {
        let t_drain = std::time::Instant::now();
        let outcome = match drain(
            &mut ws, &mut sess, cur, stop_line, &key_cols, 3600, budget, &applied_rx, changelog,
        )
        .await
        {
            Ok(o) => o,
            Err(e) => {
                drain_err = Some(e);
                break;
            }
        };
        windows += 1;
        if dbg {
            let events: u64 = outcome.tables.values().map(|c| c.events).sum();
            eprintln!(
                "[log_based] window={windows} tables={} drain={:.1}s events={events} \
                 budget_hit={}",
                outcome.tables.len(),
                t_drain.elapsed().as_secs_f64(),
                outcome.hit_budget,
            );
        }
        let end = outcome.end_lsn;
        let hit = outcome.hit_budget;
        if end > cur {
            if win_tx.send(outcome).await.is_err() {
                // Apply task died — its JoinHandle carries the real error.
                break;
            }
        }
        // Confirm the PREVIOUS window once applied (bounds resident windows
        // to two and keeps the slot's confirmed LSN strictly behind commits).
        if let Some(p) = pending.take() {
            if applied_rx.wait_for(|&a| a >= p).await.is_err() {
                break;
            }
            ws.standby_status(p, false).await?;
        }
        if end > cur {
            pending = Some(end);
            cur = end;
        }
        if !hit {
            break;
        }
    }
    drop(win_tx);
    // Wait for the final in-flight window, confirm, then collect the applier.
    if drain_err.is_none() {
        if let Some(p) = pending {
            if applied_rx.wait_for(|&a| a >= p).await.is_ok() {
                ws.standby_status(p, false).await?;
            }
        }
        // A caught-up drain now reports its end_lsn AT the caught-up point, so
        // the window above already carried it to the destination and confirmed
        // it. Nothing extra to send here — see the note in `drain`, and the
        // seven gate legs that went red when this was a bare confirmation.
    }
    let joined = apply_task.await;
    ws.stop_replication().await.ok();
    if let Some(e) = drain_err {
        return Err(e);
    }
    let rows_per = match joined {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err(e),
        Err(j) => {
            return Err(Error::Transfer(format!("log_based: apply task panicked: {j}")))
        }
    };

    Ok(rows_per.into_iter().map(|r| (r, 1)).collect())
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn scheme(url: &str) -> &str {
    url.split("://").next().unwrap_or("")
}

fn hex_prefix(s: &str, n: usize) -> String {
    hex::encode(Md5::digest(s))[..n].to_string()
}

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based: {e}"))
}

async fn resolve_table(src: &PgPool, table: &str) -> Result<(String, String)> {
    let row: (String, String) = sqlx::query_as(
        "SELECT n.nspname, c.relname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::regclass",
    )
    .bind(table)
    .fetch_one(src)
    .await
    .map_err(|e| Error::InvalidInput(format!("log_based: table '{table}': {e}")))?;
    Ok(row)
}

async fn pk_columns(src: &PgPool, qualified: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT a.attname FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = $1::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey, a.attnum)",
    )
    .bind(qualified)
    .fetch_all(src)
    .await
    .map_err(db_err)?;
    Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
}

/// Create the publication carrying EVERY member, or — when it already
/// exists — verify each member's MEMBERSHIP and re-add the missing (a
/// dropped-and-recreated source table silently leaves its publication).
async fn ensure_publication(
    src: &PgPool,
    publication: &str,
    qualified_tables: &[&str],
) -> Result<()> {
    let exists: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM pg_publication WHERE pubname = $1")
            .bind(publication)
            .fetch_optional(src)
            .await
            .map_err(db_err)?;
    if exists.is_none() {
        let list = qualified_tables
            .iter()
            .map(|q| format!("ONLY {}", quote_table(q)))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!(
            "CREATE PUBLICATION {} FOR TABLE {list}",
            quote_ident(publication)
        ))
        .execute(src)
        .await
        .map_err(|e| {
            Error::Transfer(format!(
                "log_based: CREATE PUBLICATION failed (needs table ownership or \
                 superuser): {e}"
            ))
        })?;
        return Ok(());
    }
    for qualified in qualified_tables {
        let (schema, bare) = qualified.split_once('.').unwrap_or(("public", qualified));
        let member: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
        )
        .bind(publication)
        .bind(schema)
        .bind(bare)
        .fetch_optional(src)
        .await
        .map_err(db_err)?;
        if member.is_none() {
            sqlx::query(&format!(
                "ALTER PUBLICATION {} ADD TABLE ONLY {}",
                quote_ident(publication),
                quote_table(qualified)
            ))
            .execute(src)
            .await
            .map_err(|e| {
                Error::Transfer(format!(
                    "log_based: publication {publication} exists but no longer \
                     carries {qualified} (source table dropped and recreated?) — \
                     re-adding it failed: {e}"
                ))
            })?;
        }
    }
    Ok(())
}

/// Print (and warn about) the WAL a slot is retaining. Best-effort: a source
/// without permission to read `pg_replication_slots` must not fail a transfer
/// over a diagnostic.
async fn slot_wal_report(src: &sqlx::PgPool, slot: &str) {
    let row: Option<(Option<i64>, bool)> = sqlx::query_as(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, active \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_optional(src)
    .await
    .ok()
    .flatten();
    let Some((Some(bytes), _active)) = row else {
        return;
    };
    let bytes = bytes.max(0) as u64;
    let warn_at: u64 = std::env::var("APITAP_SLOT_WAL_WARN")
        .ok()
        .and_then(|v| parse_size(&v))
        .unwrap_or(4 << 30); // 4 GiB
    if bytes >= warn_at {
        // The cap is what turns "the disk filled up" into "the slot was
        // invalidated" — a bounded, recoverable failure. Name it, because most
        // servers ship with it unset.
        let cap: Option<(String, String)> =
            sqlx::query_as("SHOW max_slot_wal_keep_size")
                .fetch_optional(src)
                .await
                .ok()
                .flatten();
        let cap = cap
            .map(|(v, _)| v)
            .or(Some("-1".into()))
            .unwrap_or_default();
        let unbounded = cap.trim() == "-1" || cap.trim().is_empty();
        eprintln!(
            "apitap ▸ WARNING slot {slot} is holding {} of WAL on the source{}. \
             That WAL cannot be freed until this drain confirms it, so a schedule \
             that stops holds the source's disk hostage.{}",
            human_bytes(bytes),
            if bytes >= warn_at * 4 { " and growing past four times the warning threshold" } else { "" },
            if unbounded {
                " max_slot_wal_keep_size is unlimited on this server: set it to \
                 bound the damage (an over-cap slot is invalidated instead, which \
                 apitap reports as slot-is-GONE and recovers from with a fresh \
                 bootstrap)."
            } else {
                ""
            }
        );
    } else {
        crate::progress::note(&format!(
            "slot {slot} retains {} of WAL",
            human_bytes(bytes)
        ));
    }
}

fn parse_size(v: &str) -> Option<u64> {
    let v = v.trim();
    let (num, mult) = match v.chars().last() {
        Some('K') | Some('k') => (&v[..v.len() - 1], 1u64 << 10),
        Some('M') | Some('m') => (&v[..v.len() - 1], 1u64 << 20),
        Some('G') | Some('g') => (&v[..v.len() - 1], 1u64 << 30),
        _ => (v, 1),
    };
    num.trim().parse::<u64>().ok().map(|n| n.saturating_mul(mult))
}

fn human_bytes(n: u64) -> String {
    const K: f64 = 1024.0;
    let f = n as f64;
    if f < K * K {
        format!("{:.0} KB", f / K)
    } else if f < K * K * K {
        format!("{:.0} MB", f / (K * K))
    } else {
        format!("{:.1} GB", f / (K * K * K))
    }
}
