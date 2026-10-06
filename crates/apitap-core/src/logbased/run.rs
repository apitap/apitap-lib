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
use crate::guard::GuardStore;
use crate::lease::{Fence, LeaseStore, Tenure, Watermark};
use crate::logbased::dest_bq::{BqDest, BqUnit};
use crate::logbased::dest_ch::{ChDest, ChUnit};
use crate::logbased::dest_ice::{IceDest, IceUnit};
use crate::logbased::dest_my::{MyDest, MyTx};
use crate::logbased::dest_pg::{quote_ident, quote_table, PgDest, PgUnit};
use crate::logbased::drain::{drain, DrainSession};
use crate::logbased::window::{DrainOutcome, Slice};
use crate::logbased::mysource;
use crate::logbased::resolve::Source;
use crate::naming::CdcWatermark;
use std::sync::Arc;
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
///
/// It is also the run's `Fence` (below): every write the lane makes goes
/// through a `Unit` a `Tenure` opened over it, and the parent modules
/// (`dest_pg` … `dest_ice`) write only through the unit they are handed.
pub(crate) enum Dest {
    Pg(PgDest),
    Ch(ChDest),
    My(MyDest),
    Ice(IceDest),
    Bq(BqDest),
}

/// One open unit of writes, whichever store fenced it.
pub(crate) enum Unit<'a> {
    Pg(PgUnit),
    My(MyTx),
    Ch(ChUnit<'a>),
    Bq(BqUnit<'a>),
    Ice(IceUnit<'a>),
}

fn mismatch() -> Error {
    Error::Transfer("internal: unit/dest mismatch".into())
}

impl LeaseStore for Dest {
    /// The lease key for one destination table — the SAME string the peer
    /// scan and the refusal already use, schema-qualified.
    fn lease_key(&self, dest_table: &str) -> String {
        match self {
            Dest::Pg(d) => d.store().lease_key(dest_table),
            Dest::My(d) => d.store().lease_key(dest_table),
            Dest::Ch(d) => d.store().lease_key(dest_table),
            Dest::Bq(d) => d.store().lease_key(dest_table),
            Dest::Ice(d) => d.store().lease_key(dest_table),
        }
    }

    async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
        match self {
            Dest::Pg(d) => d.store().lease_open(keys, token).await,
            Dest::My(d) => d.store().lease_open(keys, token).await,
            Dest::Ch(d) => d.store().lease_open(keys, token).await,
            Dest::Bq(d) => d.store().lease_open(keys, token).await,
            Dest::Ice(d) => d.store().lease_open(keys, token).await,
        }
    }

    async fn lease_renew(&self, keys: &[String], token: &str) -> Result<u64> {
        match self {
            Dest::Pg(d) => d.store().lease_renew(keys, token).await,
            Dest::My(d) => d.store().lease_renew(keys, token).await,
            Dest::Ch(d) => d.store().lease_renew(keys, token).await,
            Dest::Bq(d) => d.store().lease_renew(keys, token).await,
            Dest::Ice(d) => d.store().lease_renew(keys, token).await,
        }
    }

    async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
        match self {
            Dest::Pg(d) => d.store().lease_unclaimed(token).await,
            Dest::My(d) => d.store().lease_unclaimed(token).await,
            Dest::Ch(d) => d.store().lease_unclaimed(token).await,
            Dest::Bq(d) => d.store().lease_unclaimed(token).await,
            Dest::Ice(d) => d.store().lease_unclaimed(token).await,
        }
    }

    /// Drop what the run holds that is not tied to one table — BigQuery's
    /// per-run fence table, Iceberg's note of the keys it opened. Nothing
    /// elsewhere.
    async fn close_run(&self, token: &str) -> Result<()> {
        match self {
            Dest::Pg(d) => d.store().close_run(token).await,
            Dest::My(d) => d.store().close_run(token).await,
            Dest::Ch(d) => d.store().close_run(token).await,
            Dest::Bq(d) => d.store().close_run(token).await,
            Dest::Ice(d) => d.store().close_run(token).await,
        }
    }
}

impl Fence for Dest {
    type Unit<'a> = Unit<'a>;

    /// This destination as the guard sees it for one member, and the bare name
    /// the guard spells that member with — the SAME adapter the bulk sink
    /// uses, which is the only way a drain and a bulk run can see each other.
    ///
    /// Iceberg is the exception and is not guarded: its claims live in object
    /// storage under the table's location, which only the bulk sink resolves.
    /// An Iceberg CDC bootstrap still rides the bulk sink, so the expensive half
    /// is covered; its incremental windows are not (stated in usage.md).
    fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
        match self {
            Dest::Pg(d) => d.store().guard(dest_table),
            Dest::My(d) => d.store().guard(dest_table),
            Dest::Ch(d) => d.store().guard(dest_table),
            Dest::Bq(d) => d.store().guard(dest_table),
            Dest::Ice(d) => d.store().guard(dest_table),
        }
    }

    fn serial_commit(&self) -> bool {
        match self {
            Dest::Pg(d) => d.store().serial_commit(),
            Dest::My(d) => d.store().serial_commit(),
            Dest::Ch(d) => d.store().serial_commit(),
            Dest::Bq(d) => d.store().serial_commit(),
            Dest::Ice(d) => d.store().serial_commit(),
        }
    }

    async fn open_unit<'a>(&'a self, keys: &[String], token: &str) -> Result<Unit<'a>> {
        Ok(match self {
            Dest::Pg(d) => Unit::Pg(d.store().open_unit(keys, token).await?),
            Dest::My(d) => Unit::My(d.store().open_unit(keys, token).await?),
            Dest::Ch(d) => Unit::Ch(d.store().open_unit(keys, token).await?),
            Dest::Bq(d) => Unit::Bq(d.store().open_unit(keys, token).await?),
            Dest::Ice(d) => Unit::Ice(d.store().open_unit(keys, token).await?),
        })
    }

    async fn close_unit<'a>(&'a self, u: Unit<'a>, token: &str, marks: Vec<Watermark>) -> Result<()> {
        match (self, u) {
            (Dest::Pg(d), Unit::Pg(u)) => d.store().close_unit(u, token, marks).await,
            (Dest::My(d), Unit::My(u)) => d.store().close_unit(u, token, marks).await,
            (Dest::Ch(d), Unit::Ch(u)) => d.store().close_unit(u, token, marks).await,
            (Dest::Bq(d), Unit::Bq(u)) => d.store().close_unit(u, token, marks).await,
            (Dest::Ice(d), Unit::Ice(u)) => d.store().close_unit(u, token, marks).await,
            _ => Err(mismatch()),
        }
    }
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

    /// Pin where each member lives before any lease key is taken. Postgres
    /// resolves an unqualified name against the live `search_path` exactly as
    /// the bulk lane does (`sink::postgres::resolve_parts`); the others name
    /// their tables in full already.
    async fn resolve_names(&self, tables: &[String]) -> Result<()> {
        match self {
            Dest::Pg(d) => d.resolve_names(tables).await,
            Dest::My(_) | Dest::Ch(_) | Dest::Bq(_) | Dest::Ice(_) => Ok(()),
        }
    }

    /// A member's watermark. Every destination reads its state row whole and
    /// hands it to `naming::cdc_watermark`, so a row the cursor lane wrote is
    /// refused here, at admission, before anything moves — never read as "no
    /// state" and bootstrapped over.
    async fn read_state(&self, dest_table: &str, source_id: &str) -> Result<Option<CdcWatermark>> {
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

    /// Destination-specific knobs for the bootstrap's full load.
    fn tweak_bootstrap_opts(&self, o2: &mut TransferOptions, pk_cols: &[String]) {
        match self {
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) | Dest::Bq(_) => {}
            Dest::Ch(d) => d.tweak_bootstrap_opts(o2, pk_cols),
        }
    }

    /// After the bootstrap's full load landed, inside `u`: add identity where
    /// the engine needs one (or rebuild the table as a changelog). Returns the
    /// watermark the unit's close writes — the slot's LSN.
    #[allow(clippy::too_many_arguments)]
    async fn bootstrap_finish(
        &self,
        u: &mut Unit<'_>,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        rows: u64,
        changelog: bool,
        partition_by: Option<&str>,
        order_by: Option<&str>,
    ) -> Result<Watermark> {
        match (self, u, changelog) {
            (Dest::Ch(d), Unit::Ch(u), true) => {
                d.changelog_bootstrap_finish(u, dest_table, source_id, pk_cols, lsn, partition_by, order_by).await?
            }
            (Dest::Bq(d), Unit::Bq(u), true) => {
                d.changelog_bootstrap_finish(u, dest_table, source_id, pk_cols, lsn, partition_by, order_by).await?
            }
            (_, _, true) => return Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into())),
            (Dest::Pg(d), Unit::Pg(u), false) => d.bootstrap_finish(u, dest_table, pk_cols).await?,
            (Dest::Ch(d), Unit::Ch(u), false) => d.bootstrap_finish(u, dest_table).await?,
            (Dest::My(d), Unit::My(u), false) => d.bootstrap_finish(u, dest_table, pk_cols).await?,
            (Dest::Ice(d), Unit::Ice(_), false) => d.bootstrap_finish(pk_cols)?,
            (Dest::Bq(d), Unit::Bq(u), false) => d.bootstrap_finish(u, dest_table, pk_cols, rows).await?,
            _ => return Err(mismatch()),
        }
        Ok(Watermark::Set { table: dest_table.into(), source_id: source_id.into(), lsn, rows })
    }

    /// Apply one table's window inside `u`, and name the watermark the unit's
    /// close writes. The window's lane decides the path: a changelog body goes
    /// to a changelog apply, a replica body to a replica apply, and the table's
    /// columns and keys are the ones its window carries. `src` is the Postgres
    /// source, for the one destination that reads it back (Iceberg's TOAST
    /// refetch); the MySQL binlog path has none, and Iceberg is refused before
    /// it.
    async fn apply(
        &self,
        u: &mut Unit<'_>,
        dest_table: &str,
        qualified: &str,
        source_id: &str,
        o: &DrainOutcome,
        src: Option<&PgPool>,
    ) -> Result<(u64, Watermark)> {
        match (self, u, o.slice(qualified)) {
            (Dest::Ch(d), Unit::Ch(u), Slice::Changelog(w)) => {
                d.apply_changelog(u, dest_table, w, &o.id, source_id).await
            }
            // A group of one, in either lane: the per-member path (the MySQL
            // source's windows).
            (Dest::Bq(_), Unit::Bq(u), _) => {
                let one = [(dest_table.to_string(), qualified.to_string(), source_id.to_string())];
                let mut v = self.apply_group(u, &one, o, 1).await?;
                v.pop().ok_or_else(mismatch)
            }
            (_, _, Slice::Changelog(_)) => Err(Error::InvalidInput(CHANGELOG_DEST_MSG.into())),
            (Dest::Pg(d), Unit::Pg(u), Slice::Replica(w)) => d.apply(u, dest_table, w, &o.id, source_id).await,
            (Dest::Ch(d), Unit::Ch(u), Slice::Replica(w)) => d.apply(u, dest_table, w, &o.id, source_id).await,
            (Dest::My(d), Unit::My(u), Slice::Replica(w)) => d.apply(u, dest_table, w, &o.id, source_id).await,
            (Dest::Ice(d), Unit::Ice(u), Slice::Replica(w)) => {
                let src = src.ok_or_else(|| {
                    Error::InvalidInput("log_based: iceberg needs a Postgres source in this release".into())
                })?;
                d.apply(u, dest_table, qualified, w, &o.id, source_id, &Source(src)).await
            }
            _ => Err(mismatch()),
        }
    }

    /// The unit that held `dest_table`'s window is closed, `ok` when it
    /// committed: a changelog destination's replay memo learns the marker the
    /// window wrote only now, and forgets the table on any failure (brief §0
    /// L14).
    fn settle(&self, dest_table: &str, source_id: &str, ok: bool) {
        match self {
            Dest::Ch(d) => d.settle(dest_table, source_id, ok),
            Dest::Bq(d) => d.settle(dest_table, source_id, ok),
            Dest::Pg(_) | Dest::My(_) | Dest::Ice(_) => {}
        }
    }

    /// BigQuery: a whole group's window in ONE unit. A MERGE carries ~7.3 s of
    /// fixed job overhead, so paying it once per GROUP instead of once per
    /// TABLE is the biggest lever the profile found; the unit's close commits
    /// every member's statements with its own watermark.
    async fn apply_group(
        &self,
        u: &mut BqUnit<'_>,
        members: &[crate::logbased::dest_bq::Member],
        outcome: &DrainOutcome,
        lanes: usize,
    ) -> Result<Vec<(u64, Watermark)>> {
        match self {
            Dest::Bq(d) => d.apply_group(u, members, outcome, lanes).await,
            _ => Err(mismatch()),
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
            Dest::Ch(_) => ch_apply_lanes(
                crate::pipeline::mem_limit_bytes(),
                crate::pipeline::cpu_limit_cores(),
            ),
            // CPU-bound paths default to serial until a measured win says
            // otherwise — at 0.5 core, concurrent CPU work shares the same
            // quota; only the round-trip waits can overlap.
            _ => 1,
        }
    }
}

/// Concurrent ClickHouse applies per window, from BOTH the memory and the CPU
/// budget.
///
/// The measured 30-table steady profile at 0.5 CPU / 256 MB applied 10,072
/// changes/s with the client at 13 % of its quota: each member's apply is a
/// handful of statements that WAIT on the destination, and the serial loop
/// pays that wait once per member. The members of one window are independent
/// — each opens its own unit over its own lease key — so their statements may
/// overlap without changing a single predicate, watermark or fence. Bounded
/// by memory the way BigQuery's pool is: each in-flight apply renders only its
/// table's slice of the window, and the slices partition one window, so the
/// pool does not multiply the window's residency. Lever: `APITAP_CDC_APPLY_LANES`.
///
/// Memory: ~96 MiB of working base, then one lane per 20 MiB of headroom,
/// floored at 1 and capped at 16. The 20 MiB is the measured WORST-CASE
/// marginal lane cost on the 30-table shape, not the 2-CPU average: with
/// 64 MiB windows, 16 lanes at 2 CPU peaked 250.6 MB in the 256 MB cage, and
/// the same jump at 4 CPU — where the drain overlaps a second full window —
/// OOM-killed the run. 20 MiB/lane resolves a 256 MB cage to 8 lanes whatever
/// the quota (the bound the B1 campaign measured safe), and a 512 MB cage to
/// 16.
///
/// CPU: destination-latency bound, so more quota may hold more statements in
/// flight — but only while the memory bound holds them. `round(16*c)` with a
/// floor of 8 (the measured best at 0.5 core) is the latency-side ask; the
/// SMALLER of the two bounds wins. On the 256 MB target the memory bound wins
/// at every quota; the quota only buys lanes from ~416 MB up, where 1 CPU
/// reaches 16.
fn ch_apply_lanes(mem: Option<u64>, cpu: Option<f64>) -> usize {
    const CAP: usize = 16;
    let mem_bound = match mem {
        Some(m) => ((m.saturating_sub(96 << 20) / (20 << 20)) as usize).clamp(1, CAP),
        None => CAP,
    };
    let cpu_bound = match cpu {
        Some(c) => ((c * 16.0).round() as usize).clamp(8, CAP),
        None => CAP,
    };
    mem_bound.min(cpu_bound)
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

    // A failed group fails ITS tables, each named in the report, and nothing
    // else: every group holds its own tenure over its own members, so one
    // group evicted, refused or broken leaves the others' windows and
    // watermarks standing. The caller sees the partial failure the way every
    // multi-table run reports one (`MultiTransferError`, whose report lists
    // each table's outcome) instead of one error that hid what succeeded.
    let mut by_table: HashMap<String, std::result::Result<(u64, usize), String>> = HashMap::new();
    for (gi, out) in outcomes.into_iter().enumerate() {
        match out {
            Ok(v) => {
                for (t, r) in groups[gi].iter().zip(v) {
                    by_table.insert(t.clone(), Ok(r));
                }
            }
            Err(e) => {
                let why = format!(
                    "slot group {}/{n} ({}) failed: {e} — the other groups own \
                     independent slots and watermarks, their committed progress is \
                     durable, and a retry resumes every group from its own state",
                    gi + 1,
                    groups[gi].join(", "),
                );
                for t in &groups[gi] {
                    by_table.insert(t.clone(), Err(why.clone()));
                }
            }
        }
    }
    let ok = || by_table.values().filter_map(|r| r.as_ref().ok());
    Ok(MultiReport {
        rows: ok().map(|(r, _)| *r).sum(),
        elapsed_ms: elapsed_all,
        budget: ok().map(|(_, p)| *p).max().unwrap_or(1),
        tables: tables
            .iter()
            .map(|t| {
                let (rows, parallel, error) = match &by_table[t] {
                    Ok((r, p)) => (*r, *p, None),
                    Err(e) => (0, 1, Some(e.clone())),
                };
                TableResult { table: t.clone(), rows, elapsed_ms: elapsed_all, parallel, error }
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
    // Vet the URL's ssl mode BEFORE anything opens a socket. The control pool
    // below is sqlx's, and sqlx supports a mode this client does not
    // (`verify-ca`) — without this, a URL asking for it would open a pool
    // fine and then fail deep in the replication connection, or worse, fail
    // with sqlx's certificate error and leave the user reading about
    // certificates when the real answer is "that mode is not implemented
    // here". One parse, up front, so the message is the right one.
    crate::wire::walsender::check_ssl_mode(src_url)?;
    let dest = std::sync::Arc::new(Dest::connect(dst_url).await?);

    let src = PgPoolOptions::new()
        .max_connections(2)
        .connect(src_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: source connect: {e}")))?;

    // Resolve every member: real (schema, name) + PK on the source.
    let single = tables.len() == 1;
    let mut ctxs = Vec::with_capacity(tables.len());
    // publish_via_partition_root exists from PostgreSQL 13. Before that a
    // partitioned parent cannot be published usefully at all, so it is refused
    // up front — by us, with the reason, rather than later by the server with
    // a message about publications.
    let server_num: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&src)
        .await
        .map_err(db_err)?;
    for t in tables {
        let (schema_name, bare, partitioned) = resolve_table(&src, t).await?;
        let qualified = format!("{schema_name}.{bare}");
        if partitioned && server_num < 130_000 {
            return Err(Error::InvalidInput(format!(
                "log_based: {qualified} is a partitioned table and this server is \
                 PostgreSQL {} — replicating a partitioned parent needs \
                 publish_via_partition_root, which arrived in PostgreSQL 13. \
                 Upgrade the server, or CDC the leaf partitions individually.",
                server_num / 10_000
            )));
        }
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

    // THE TENURE — before the bootstrap decision, before a watermark is read,
    // before a row moves. Lease first, then every member announced, then every
    // member checked, with the same artifact, minting call and verdict a bulk
    // run uses: the matrix's `log_based | anything | refused` row is only true
    // if the two lanes can actually see each other. 0.55.0 asserted that row
    // while a drain wrote and read nothing at all.
    //
    // From here every write is a unit the tenure opens, and nothing else can
    // open one; its keeper renews the lease off the window path and stops the
    // run the moment a peer claims it.
    let run = crate::naming::RunId::mint_drain(&crate::pipeline::source_origin(src_url));
    let members: Vec<String> = ctxs.iter().map(|c| c.dest_table.clone()).collect();
    dest.resolve_names(&members).await?;
    let tenure = Arc::new(Tenure::acquire(dest.clone(), &members, run).await?);

    // Everything from here is inside one arm, so the tenure is given back on
    // EVERY exit. Without it, a run refused between the scan and the drain — a
    // torn group, a mode mismatch, an unreadable watermark — left its own lock
    // behind and every later run of the table was refused over a drain that
    // never started. `e2e_state_contract.py` caught exactly that.
    let out = async {
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
            // The full load is a nested run (`transfer_within`): its own Swap
            // token names this run as parent, so its guard classifies our lock
            // and marker as `Found::Parent` and proceeds. The tenure — lock,
            // lease and keeper — stays held through the load and every
            // `bootstrap_finish`; the release below is the only one. (0.56.0
            // released them here and let the bulk lock cover the load, which
            // left the table free between the two and after the load, while
            // the drain still owned its slot and its watermark.)
            bootstrap_group(src_url, dst_url, opts, &tenure, &src, &slot, &ctxs).await
        } else {
            let wm = wms.iter().map(|w| w.expect("all present")).min().expect("nonempty").get();
            drain_group(
                src_url,
                &src,
                tenure.clone(),
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
    .await;
    // Waits for every open unit, stops AND JOINS the keeper (on ClickHouse a
    // renewal in flight would resurrect a closed row), then per member its
    // scratch, its markers, and its lease only once its markers are observed
    // gone — dropping a lease while its lock survives would wedge the table
    // for ever.
    tenure.release().await;
    out
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

    let dest = std::sync::Arc::new(Dest::connect(dst_url).await?);
    if matches!(*dest, Dest::Ice(_)) {
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
            Some(prev) if prev.get() != server => {
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

    /// Record the source server for every table that did not have one yet —
    /// a unit per table, whose close writes the marker as an ordinary state
    /// row under a reserved `source_id`. Called only after the run's own state
    /// is durable, so nothing that clears state rows can run after it.
    async fn stamp(tenure: &Tenure<Dest>, adopt: &[(String, String)], server: u64) -> Result<()> {
        for (table, marker) in adopt {
            let h = tenure.open(&[table.as_str()]).await?;
            let mark = Watermark::Set { table: table.clone(), source_id: marker.clone(), lsn: server, rows: 0 };
            tenure.close(h, vec![mark]).await?;
        }
        Ok(())
    }

    // THE TENURE — the MySQL twin of the Postgres path above, for the same
    // reason and with the same artifact.
    let run = crate::naming::RunId::mint_drain(&crate::pipeline::source_origin(src_url));
    let members: Vec<String> = ctxs.iter().map(|c| c.dest_table.clone()).collect();
    dest.resolve_names(&members).await?;
    let tenure = Tenure::acquire(dest.clone(), &members, run).await?;

    // Everything from here is inside one arm so the tenure is given back on
    // EVERY exit — a drain that fails for any reason must not leave its lock
    // behind for the next run to refuse over.
    let out = async {
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
            // The full load is a nested run, exactly as on the Postgres path:
            // the tenure is held through the load and every finish, and the
            // release below is the only one.
            let su = src_url.to_string();
            let du = dst_url.to_string();
            let run_c = tenure.run().clone();
            let (mark, out) = myrun::bootstrap(&pool, &ctxs, opts, |table_arg, o2| {
                let (su, du, r) = (su.clone(), du.clone(), run_c.clone());
                async move {
                    let rep = Box::pin(crate::transfer_within(&r, &su, &du, &table_arg, &o2)).await?;
                    Ok((rep.rows, rep.parallel))
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
                let finished = async {
                    let mut h = tenure.open(&[c.dest_table.as_str()]).await?;
                    let m = dest
                        .bootstrap_finish(&mut h.unit, &c.dest_table, &c.source_id, &c.pk_cols, mark, *rows,
                            opts.changelog, pb, ob)
                        .await?;
                    tenure.close(h, vec![m]).await
                }
                .await;
                if let Err(e) = finished {
                    // Same rollback as the Postgres group: a half-written group is
                    // worse than no group, because the next run refuses it.
                    clear_group(&tenure, ctxs.iter().map(|c| (c.dest_table.as_str(), c.source_id.as_str()))).await;
                    return Err(e);
                }
            }
            stamp(&tenure, &adopt, server).await?;
            return Ok(out);
        }

        // Drain from the group minimum — members ahead converge idempotently.
        let wm = marks.iter().flatten().copied().min().map_or(0, CdcWatermark::get);
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

        // Armed for the incremental drain only — the bootstrap branch above returns
        // before reaching here. See `crate::shutdown` and the note in `drain_group`.
        let _stop = crate::shutdown::Guard::install();

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
                let (tenure, ctxs, rows_applied) = (&tenure, &ctxs, &rows_applied);
                async move {
                    let end = outcome.id.end();
                    for (c, acc) in ctxs.iter().zip(rows_applied.iter()) {
                        // Every member applies — a table with no traffic in this
                        // window still advances its watermark. One unit each.
                        let n = apply_member(tenure, &c.dest_table, &c.qualified, &c.source_id, &outcome, None)
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

        stamp(&tenure, &adopt, server).await?;
        Ok::<Vec<(u64, usize)>, Error>(rows_applied.iter().map(|a| (a.get(), 1)).collect())
    }
    .await;
    // Waits for every open unit, stops and joins the keeper, then per member
    // its scratch, its markers and — only once they are gone — its lease.
    tenure.release().await;
    out
}

/// Remove every member's watermark, so a failed group bootstrap really does
/// leave "no state" the way its error message says it does: a member that DID
/// write its watermark must lose it, or the group is torn. Best-effort, each a
/// unit of its own; a member the tenure can no longer open (evicted) is left to
/// the run that collected it.
async fn clear_group<'a>(tenure: &Tenure<Dest>, members: impl Iterator<Item = (&'a str, &'a str)>) {
    for (table, source_id) in members {
        if let Ok(h) = tenure.open(&[table]).await {
            let mark = Watermark::Clear { table: table.into(), source_id: source_id.into() };
            let _ = tenure.close(h, vec![mark]).await;
        }
    }
}

async fn bootstrap_group(
    src_url: &str,
    dst_url: &str,
    opts: &TransferOptions,
    tenure: &Tenure<Dest>,
    src: &PgPool,
    slot: &str,
    ctxs: &[TableCtx],
) -> Result<Vec<(u64, usize)>> {
    let (dest, run) = (tenure.dest(), tenure.run());
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
            let r = Box::pin(crate::transfer_within(run, pinned_url, dst_url, &c.table_arg, &o2))
                .await?;
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

    // Each finish is a unit of its own: its DDL and its watermark, fenced, and
    // the watermark written only by the unit's close. BigQuery's closes queue
    // on the tenure's commit gate (every one updates the run's fence row).
    let fins: Vec<Result<()>> = futures::stream::iter(ctxs.iter().zip(&out).map(|(c, (rows, _))| {
        async move {
            let (pb, ob) = ddl_for(opts, &c.table_arg, &c.qualified);
            let mut h = tenure.open(&[c.dest_table.as_str()]).await?;
            let m = dest
                .bootstrap_finish(&mut h.unit, &c.dest_table, &c.source_id, &c.pk_cols, lsn, *rows,
                    opts.changelog, pb, ob)
                .await?;
            tenure.close(h, vec![m]).await
        }
    }))
    .buffered(concurrency)
    .collect()
    .await;
    if let Some(e) = fins.into_iter().find_map(Result::err) {
        // Make the rollback the message promises real.
        clear_group(tenure, ctxs.iter().map(|c| (c.dest_table.as_str(), c.source_id.as_str()))).await;
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
    // Shared, not owned: the apply task runs off the drain's clock and opens
    // its units through a handle of its own, while the caller keeps one to
    // release the tenure when the drain is over.
    tenure: Arc<Tenure<Dest>>,
    slot: &str,
    publication: &str,
    ctxs: &[TableCtx],
    wm: u64,
    changelog: bool,
    budget_denom: usize,
) -> Result<Vec<(u64, usize)>> {
    // A SIGTERM from here on lands the window in flight instead of throwing it
    // away — see `crate::shutdown`. It is armed HERE and not at the entry point
    // on purpose: the sibling branch is `bootstrap_group`, a bulk load that
    // publishes at the swap, so a signal absorbed there would buy nothing and
    // cost the process its clean stop while the load ran on. Restored when this
    // returns.
    let _stop = crate::shutdown::Guard::install();

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

    // Two windows are resident under overlap (one applying, one draining) —
    // the budget halves so peak memory stays at the single-window ceiling.
    // The cap matters on BIG boxes too: overlap only pays while windows
    // rotate, so past ~24 MiB of buffered rows the marginal collapse-dedup
    // is worth less than hiding the apply under the next drain.
    // With `slots=N` this pipeline is one of N in the SAME process/cgroup, so
    // the per-window budget shards by N (floor 1 MiB) to keep the process's
    // recorded memory ceiling intact.
    let budget = (tenure.dest().cdc_window_bytes() / budget_denom.max(1)).max(1 << 20);
    let mut ws = Walsender::connect(src_url).await?;
    ws.start_replication(slot, wm, publication).await?;

    // Overlapped windows (ape-dts's daemon trick, batch-shaped): the drain
    // loop keeps the walsender and decodes window N+1 WHILE a spawned apply
    // task lands window N. The slot is confirmed only after the apply task
    // reports a window fully committed (watch channel carries the last
    // committed end_lsn back) — never past unapplied WAL, exactly like the
    // serial loop, just off the clock.
    let (win_tx, win_rx) = tokio::sync::mpsc::channel::<DrainOutcome>(1);
    let (applied_tx, applied_rx) = tokio::sync::watch::channel::<u64>(wm);
    let members: Vec<crate::logbased::dest_bq::Member> = ctxs
        .iter()
        .map(|c| (c.dest_table.clone(), c.qualified.clone(), c.source_id.clone()))
        .collect();
    let apply = AbortOnDrop::spawn(apply_windows(tenure, src.clone(), members, win_rx, applied_tx));
    let drained = run_overlapped(
        drain_loop(&mut ws, win_tx, applied_rx, wm, stop_line, &key_cols, budget, changelog),
        apply,
    )
    .await;
    ws.stop_replication().await.ok();
    Ok(drained?.into_iter().map(|r| (r, 1)).collect())
}

/// A spawned task that cannot outlive its owner: dropped before it was
/// joined — the owner's future cancelled, or a panic unwinding past it — it
/// is aborted. A detached apply task is exactly what let 0.56.0 commit windows
/// after its run had raised and released the table.
struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T: Send + 'static> AbortOnDrop<T> {
    fn spawn<F: std::future::Future<Output = T> + Send + 'static>(f: F) -> Self {
        AbortOnDrop(Some(tokio::spawn(f)))
    }

    /// Wait for the task. The handle stays inside until it finishes, so a
    /// join that is itself cancelled still aborts the task on drop.
    async fn join(mut self) -> std::result::Result<T, tokio::task::JoinError> {
        self.0.as_mut().expect("joined once").await
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort(); // a finished task: a no-op
        }
    }
}

/// Run a drain loop beside the apply task it feeds, and JOIN that task on
/// every path before returning — the drain's own error included.
///
/// `drain` owns the window sender, so its end (whatever the outcome) closes
/// the channel, and the apply task lands what it already has and returns.
/// Only then does this return, so whatever releases the table afterwards
/// (`Tenure::release`, which also waits for every open unit) comes after the
/// last write. 0.56.0 had a `?` between spawn and join: a failed standby
/// status returned past the join, the run released its lock and raised, and
/// the apply task went on committing windows beside the next owner.
async fn run_overlapped<D>(drain: D, apply: AbortOnDrop<Result<Vec<u64>>>) -> Result<Vec<u64>>
where
    D: std::future::Future<Output = Result<()>>,
{
    let drained = drain.await;
    let joined = apply.join().await;
    drained?;
    match joined {
        Ok(r) => r,
        Err(j) => Err(Error::Transfer(format!("log_based: apply task panicked: {j}"))),
    }
}

/// The drain side: decode windows off the walsender and hand each to the
/// apply task, confirming a window to the slot only once the apply reported
/// it committed. Owns `win_tx`: returning — Ok or Err — closes the channel.
#[allow(clippy::too_many_arguments)]
async fn drain_loop(
    ws: &mut Walsender,
    win_tx: tokio::sync::mpsc::Sender<DrainOutcome>,
    mut applied_rx: tokio::sync::watch::Receiver<u64>,
    wm: u64,
    stop_line: u64,
    key_cols: &HashMap<String, Vec<String>>,
    budget: usize,
    changelog: bool,
) -> Result<()> {
    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    let mut sess = DrainSession::default();
    let mut cur = wm;
    let mut windows = 0u32;
    // The previous window's end_lsn: sent to the applier, not yet confirmed.
    let mut pending: Option<u64> = None;
    loop {
        let t_drain = std::time::Instant::now();
        let outcome =
            drain(ws, &mut sess, cur, stop_line, key_cols, 3600, budget, &applied_rx, changelog).await?;
        windows += 1;
        if dbg {
            eprintln!(
                "[log_based] window={windows} tables={} drain={:.1}s events={} \
                 budget_hit={}",
                outcome.tables(),
                t_drain.elapsed().as_secs_f64(),
                outcome.events(),
                outcome.hit_budget,
            );
        }
        let end = outcome.id.end();
        let hit = outcome.hit_budget;
        if end > cur && win_tx.send(outcome).await.is_err() {
            // Apply task died — its JoinHandle carries the real error.
            break;
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
    // Wait for the final in-flight window, then confirm it. A caught-up drain
    // reports its end_lsn AT the caught-up point, so that window already
    // carried it to the destination; nothing extra to send — see the note in
    // `drain`, and the seven gate legs that went red when this was a bare
    // confirmation.
    if let Some(p) = pending {
        if applied_rx.wait_for(|&a| a >= p).await.is_ok() {
            ws.standby_status(p, false).await?;
        }
    }
    Ok(())
}

/// The apply side: every window lands in units the tenure opens, so an
/// evicted or winding-down run writes nothing more, and `Tenure::release`
/// cannot pass a unit still open here.
async fn apply_windows(
    tenure: Arc<Tenure<Dest>>,
    src: PgPool,
    members: Vec<crate::logbased::dest_bq::Member>,
    mut win_rx: tokio::sync::mpsc::Receiver<DrainOutcome>,
    applied_tx: tokio::sync::watch::Sender<u64>,
) -> Result<Vec<u64>> {
    let t = &*tenure;
    let dest = t.dest();
    let mut rows_per = vec![0u64; members.len()];
    while let Some(o) = win_rx.recv().await {
        let t_apply = std::time::Instant::now();
        let lanes = dest.apply_lanes();
        if let Dest::Bq(_) = dest {
            // BigQuery: stage every table concurrently (one load job each),
            // then commit the whole group's MERGEs + watermarks in as few
            // script jobs as possible — one unit over the group. Every member
            // learns how its unit ended (`Dest::settle`), on every path.
            let tables: Vec<&str> = members.iter().map(|m| m.0.as_str()).collect();
            let r = async {
                let mut h = t.open(&tables).await?;
                let Unit::Bq(u) = &mut h.unit else { return Err(mismatch()) };
                let applied = dest.apply_group(u, &members, &o, lanes).await?;
                let (rows, marks): (Vec<u64>, Vec<Watermark>) = applied.into_iter().unzip();
                t.close(h, marks).await?;
                Ok(rows)
            }
            .await;
            for (dt, _, sid) in &members {
                dest.settle(dt, sid, r.is_ok());
            }
            for (i, n) in r?.into_iter().enumerate() {
                rows_per[i] += n;
            }
        } else if matches!(dest, Dest::Ch(_)) && members.len() > 1 {
            // A ClickHouse window's watermarks are ONE statement: the members'
            // applies still overlap in lanes, each in a unit of its own, but
            // their marks are closed together in a single unit over the whole
            // group, where `ChStore::close_unit` writes one `_apitap_state`
            // INSERT for the window. The fence, every predicate and every
            // watermark value are unchanged — only the statement count drops.
            let (sref, oref, mref) = (&src, &o, &members);
            let fs: Vec<_> = (0..mref.len())
                .map(|i| async move {
                    let (dt, q, sid) = &mref[i];
                    let (n, m) = apply_member_pending(t, dt, q, sid, oref, Some(sref)).await?;
                    Ok::<_, Error>((i, n, m))
                })
                .collect();
            let mut done = Vec::with_capacity(fs.len());
            if lanes > 1 {
                match settle_all(lanes, fs).await {
                    Ok(v) => done = v,
                    Err(e) => {
                        for (dt, _, sid) in &members {
                            dest.settle(dt, sid, false);
                        }
                        return Err(e);
                    }
                }
            } else {
                for f in fs {
                    match f.await {
                        Ok(v) => done.push(v),
                        Err(e) => {
                            for (dt, _, sid) in &members {
                                dest.settle(dt, sid, false);
                            }
                            return Err(e);
                        }
                    }
                }
            }
            done.sort_by_key(|(i, _, _)| *i);
            let tables: Vec<&str> = members.iter().map(|m| m.0.as_str()).collect();
            let marks: Vec<Watermark> = done.iter().map(|(_, _, m)| m.clone()).collect();
            let r = async {
                let h = t.open(&tables).await?;
                t.close(h, marks).await
            }
            .await;
            for (dt, _, sid) in &members {
                dest.settle(dt, sid, r.is_ok());
            }
            r?;
            for (i, n, _) in done {
                rows_per[i] += n;
            }
        } else if lanes > 1 && members.len() > 1 {
            // A BOUNDED pool of per-table units: `lanes` in flight, the rest
            // queued, each completion pulling the next. Unbounded would melt a
            // 100-table group against the destination's limits and make every
            // transaction contend on the shared state rows. Indices, not
            // references: a closure taking `&(..)` and returning an async
            // block trips higher-ranked lifetime inference ("FnOnce is not
            // general enough").
            let (sref, oref, mref) = (&src, &o, &members);
            let fs: Vec<_> = (0..mref.len())
                .map(|i| async move {
                    let (dt, q, sid) = &mref[i];
                    let n = apply_member(t, dt, q, sid, oref, Some(sref)).await?;
                    Ok::<_, Error>((i, n))
                })
                .collect();
            for (i, n) in settle_all(lanes, fs).await? {
                rows_per[i] += n;
            }
        } else {
            for (i, (dt, q, sid)) in members.iter().enumerate() {
                rows_per[i] += apply_member(t, dt, q, sid, &o, Some(&src)).await?;
            }
        }
        if std::env::var("APITAP_DEBUG").is_ok() {
            eprintln!(
                "[log_based] applied lsn={} events={} in {:.1}s",
                o.id.end(),
                o.events(),
                t_apply.elapsed().as_secs_f64(),
            );
        }
        // Receiver may be gone on a drain-side abort — nothing to do.
        let _ = applied_tx.send(o.id.end());
    }
    Ok(rows_per)
}

/// Drive a bounded pool of per-member futures to completion, then report the
/// first error in completion order.
///
/// Unlike `try_collect`, an error does NOT cancel the siblings already in
/// flight: each member's unit settles on its own path (a committed unit's
/// watermark stands; a dropped pg/my transaction rolls back; ClickHouse
/// statements already ran and the window replays), which is the
/// partial-landing shape the recovery model documents and `e2e_toast_rekey`
/// pins — a group's first member closing while a later one fails. Cancelling
/// the sibling also wasted its work and made that shape unreproducible.
async fn settle_all<F, T>(lanes: usize, fs: Vec<F>) -> Result<Vec<T>>
where
    F: std::future::Future<Output = Result<T>>,
{
    use futures::stream::StreamExt as _;
    let done: Vec<Result<T>> =
        futures::stream::iter(fs).buffer_unordered(lanes).collect().await;
    done.into_iter().collect()
}

/// One member's window in a unit of its own: open, apply, close — and then
/// tell the destination whether that unit committed (`Dest::settle`), on
/// every path, so a changelog's replay memo never holds a marker whose unit
/// failed.
async fn apply_member(
    t: &Tenure<Dest>,
    dest_table: &str,
    qualified: &str,
    source_id: &str,
    o: &DrainOutcome,
    src: Option<&PgPool>,
) -> Result<u64> {
    let dest = t.dest();
    let r = async {
        let mut h = t.open(&[dest_table]).await?;
        let (n, m) = dest.apply(&mut h.unit, dest_table, qualified, source_id, o, src).await?;
        t.close(h, vec![m]).await?;
        Ok(n)
    }
    .await;
    dest.settle(dest_table, source_id, r.is_ok());
    r
}

/// One member's window in a unit of its own, closed with NO mark: the caller
/// batches every member's mark into one group close, where the watermark
/// statements are written. Only the ClickHouse replica/changelog group path
/// calls this — a unit there is a predicate, not a transaction, so the data
/// statements were already fenced when the empty close runs, and the eventual
/// group close carries the same pinned-deadline proof for every mark.
async fn apply_member_pending(
    t: &Tenure<Dest>,
    dest_table: &str,
    qualified: &str,
    source_id: &str,
    o: &DrainOutcome,
    src: Option<&PgPool>,
) -> Result<(u64, Watermark)> {
    let mut h = t.open(&[dest_table]).await?;
    let (n, m) = t.dest().apply(&mut h.unit, dest_table, qualified, source_id, o, src).await?;
    t.close(h, Vec::new()).await?;
    Ok((n, m))
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

async fn resolve_table(src: &PgPool, table: &str) -> Result<(String, String, bool)> {
    let row: (String, String, String) = sqlx::query_as(
        "SELECT n.nspname, c.relname, c.relkind::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::regclass",
    )
    .bind(table)
    .fetch_one(src)
    .await
    .map_err(|e| Error::InvalidInput(format!("log_based: table '{table}': {e}")))?;
    // relkind 'p' is a declaratively partitioned PARENT. It matters because a
    // parent emits no WAL of its own: without `publish_via_partition_root` the
    // Relation messages name the LEAF partitions, the drain's tracking map
    // (keyed by the name the user asked for) discards every change, and the
    // watermark advances past them — the bootstrap is correct, so the
    // destination looks right on day one and then freezes forever.
    Ok((row.0, row.1, row.2 == "p"))
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
    // publish_via_partition_root=true wherever the server knows the option
    // (PostgreSQL 13+). For a partitioned parent this is the difference between
    // working and silently discarding every change: without it the Relation
    // messages carry each LEAF's name, which the drain does not track. For a
    // plain table it changes nothing. It also fixes the catalog view this very
    // function reads — with the option off, pg_publication_tables lists the
    // LEAVES, so the membership check below concluded the parent had vanished
    // and died re-adding it ("is already member of publication").
    let viaroot: bool = sqlx::query_scalar::<_, i32>(
        "SELECT current_setting('server_version_num')::int",
    )
    .fetch_one(src)
    .await
    .map(|v| v >= 130_000)
    .unwrap_or(false);
    if exists.is_none() {
        let list = qualified_tables
            .iter()
            .map(|q| format!("ONLY {}", quote_table(q)))
            .collect::<Vec<_>>()
            .join(", ");
        let with = if viaroot {
            " WITH (publish_via_partition_root = true)"
        } else {
            ""
        };
        sqlx::query(&format!(
            "CREATE PUBLICATION {} FOR TABLE {list}{with}",
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
    if viaroot {
        // Heal a publication made before this option was set (or by an older
        // apitap): one idempotent catalog update, and the next drain's
        // Relation messages carry the ROOT's name. The changes a broken
        // pipeline already confirmed past are gone either way — that needs a
        // re-bootstrap and the docs say so — but from here on it tracks.
        sqlx::query(&format!(
            "ALTER PUBLICATION {} SET (publish_via_partition_root = true)",
            quote_ident(publication)
        ))
        .execute(src)
        .await
        .map_err(db_err)?;
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
    // The number itself, always, in the machine's shape — before deciding
    // whether a human also needs a sentence about it. This is the gauge an
    // unattended pipeline alerts on: WAL a slot is holding on the SOURCE is
    // the difference between "a schedule paused" and "the source's disk
    // filled up".
    crate::progress::gauge(
        "slot.wal",
        &[
            ("slot", slot.to_string()),
            ("retained_bytes", bytes.to_string()),
            ("warn_at_bytes", warn_at.to_string()),
            ("over_threshold", (bytes >= warn_at).to_string()),
        ],
    );
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
        // Through the progress channel, not `eprintln!` — that bypassed it
        // entirely, so a JSON consumer got one stray unstructured line and it
        // was the most important one.
        crate::progress::warn(&format!(
            "slot {slot} is holding {} of WAL on the source{}. \
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
        ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    /// The drain fails after handing over two windows while the apply task is
    /// still landing them. `run_overlapped` must not return — and so must not
    /// let the run release its table — before that task finished: a detached
    /// apply commits beside the next owner.
    #[test]
    fn overlapped_loop_joins_apply_on_error() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            let finished = Arc::new(Mutex::new(None::<Instant>));
            let f2 = finished.clone();
            let apply = AbortOnDrop::spawn(async move {
                let mut n = 0u64;
                while rx.recv().await.is_some() {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    n += 1;
                }
                *f2.lock().unwrap() = Some(Instant::now());
                Ok(vec![n])
            });
            let drain = async move {
                for w in 0..2 {
                    tx.send(w).await.map_err(|_| Error::Transfer("apply gone".into()))?;
                }
                Err(Error::Transfer("the walsender went away".into()))
            };
            let r = run_overlapped(drain, apply).await;
            let returned = Instant::now();
            assert!(matches!(&r, Err(Error::Transfer(m)) if m.contains("walsender")), "{r:?}");
            let done = finished.lock().unwrap().expect("the apply task was left running past the return");
            assert!(returned >= done, "returned before the apply task finished");
        });
    }

    /// Dropping the owner aborts the task instead of detaching it.
    #[test]
    fn a_dropped_apply_is_aborted() {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let ran = Arc::new(Mutex::new(false));
            let r2 = ran.clone();
            let apply = AbortOnDrop::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                *r2.lock().unwrap() = true;
            });
            drop(apply);
            tokio::time::sleep(Duration::from_millis(120)).await;
            assert!(!*ran.lock().unwrap(), "a dropped apply task ran on");
        });
    }

    /// A pool error waits for every sibling already in flight: a member that
    /// started is not cancelled by another member's failure. The serial-era
    /// shape — an earlier member closes, a later one fails — must stay
    /// reproducible (e2e_toast_rekey's replay constructs itself from it).
    /// `try_collect` dropped the slow sibling here, and the e2e leg caught it.
    #[test]
    fn a_pool_error_does_not_cancel_in_flight_siblings() {
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let sibling_ran = Arc::new(AtomicBool::new(false));
            let s2 = sibling_ran.clone();
            let fs: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Result<u8>> + Send>>> = vec![
                Box::pin(async move { Err(Error::Transfer("boom".into())) }),
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    s2.store(true, SeqCst);
                    Ok(1)
                }),
            ];
            let r = settle_all(2, fs).await;
            assert!(matches!(&r, Err(Error::Transfer(m)) if m == "boom"), "{r:?}");
            assert!(sibling_ran.load(SeqCst), "the in-flight sibling was cancelled");
        });
    }

    /// A ClickHouse group overlaps its members' applies by default, and the
    /// lane count comes from BOTH budgets: the measured 0.5-core/256-MB target
    /// shape keeps its 8; a 256 MB cage holds 8 at every quota (the 16-lane
    /// 4-CPU point OOM-killed the run); a 512 MB cage lets the quota add lanes
    /// up to the cap; a tiny cage still runs one at a time. The pool shrinks
    /// to serial when the memory headroom cannot hold one body.
    #[test]
    fn clickhouse_lanes_follow_cpu_and_memory_together() {
        assert_eq!(ch_apply_lanes(None, None), 16, "no limits: the cap, not serial");
        assert_eq!(ch_apply_lanes(Some(256 << 20), Some(0.5)), 8, "the target shape");
        assert_eq!(ch_apply_lanes(Some(256 << 20), Some(1.0)), 8, "8 lanes fit the 256 MB cage");
        assert_eq!(ch_apply_lanes(Some(256 << 20), Some(4.0)), 8, "4 CPU does not buy memory");
        assert_eq!(ch_apply_lanes(Some(512 << 20), Some(1.0)), 16, "headroom: the quota adds lanes");
        assert_eq!(ch_apply_lanes(Some(512 << 20), Some(0.5)), 8, "0.5 core stays at 8");
        assert_eq!(ch_apply_lanes(Some(128 << 20), Some(4.0)), 1, "the memory bound wins");
        assert_eq!(ch_apply_lanes(Some(96 << 20), Some(1.0)), 1, "no headroom: one at a time");
        assert_eq!(ch_apply_lanes(Some(32 << 20), Some(4.0)), 1);
    }

    /// And the dispatch itself: a constructed ClickHouse destination answers
    /// more than one lane with no env override, where 0.57.0 answered one.
    /// The global override and the row stores' serial default are unchanged.
    #[test]
    fn row_stores_stay_serial_and_clickhouse_does_not() {
        std::env::remove_var("APITAP_CDC_APPLY_LANES");
        let d = Dest::Ch(ChDest::connect("clickhouse://default:@127.0.0.1:8123/default").unwrap());
        assert!(d.apply_lanes() > 1, "ClickHouse must overlap its members by default");
    }
}
