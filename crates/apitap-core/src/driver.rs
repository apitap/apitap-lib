//! The one generic transfer driver.
//!
//! Every route runs the same six-phase lifecycle — knobs, probe, lane negotiation,
//! staging, parallel workers over a span queue, count + atomic swap — so it lives here
//! exactly once, generic over [`Source`] and [`Sink`]. Each instantiation is fully
//! monomorphized: the hot loops (the sources' workers and the sinks' loaders) compile
//! to the same code as the hand-written routes they replaced, no dynamic dispatch.
//!
//! Adding a connector = implement `Source` and/or `Sink` in `connectors/<name>.rs` and
//! register the scheme in [`crate::transfer`]'s dispatch — nothing here changes.

use crate::error::{Error, Result};
use crate::plan::{Delta, DestState, Lane, TablePlan, WireFormat};
use crate::{Mode, TableResult, TransferOptions, TransferReport};
use std::future::Future;

/// Work-stealing statement queue: many small spans, N workers pull until it drains.
/// Static one-span-per-worker left a straggler tail (a probe caught only 7 of 12 pipes
/// still alive at ~80% wall time).
pub(crate) type WorkQueue = std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<String>>>;

pub(crate) fn work_queue(stmts: Vec<String>) -> WorkQueue {
    std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
        stmts,
    )))
}

pub(crate) fn pop(queue: &WorkQueue) -> Option<String> {
    queue.lock().unwrap().pop_front()
}

/// Split `[lo, hi]` into at most `n` contiguous, non-overlapping, covering spans.
pub(crate) fn spans(lo: i64, hi: i64, n: usize) -> Vec<(i64, i64)> {
    let n = n.max(1) as i64;
    let step = (hi - lo + 1 + n - 1) / n; // ceil
    let mut out = Vec::new();
    for k in 0..n {
        let rlo = lo + k * step;
        if rlo > hi {
            break;
        }
        out.push((rlo, std::cmp::min(lo + (k + 1) * step - 1, hi)));
    }
    out
}

/// Per-worker stream consumer on the sink side. One loader = one physical ingest
/// stream (one `COPY … FROM STDIN`, one ClickHouse `INSERT` body).
pub(crate) trait Loader: Send + 'static {
    /// Ship one coalesced buffer. The worker owns coalescing (~chunk bytes per send —
    /// tiny sends are syscall/protocol overhead, huge ones just buffer memory).
    ///
    /// Framing contract: for row-oriented formats (RowBinary, TabSeparated) buffers
    /// end on a RECORD boundary — a loader that splits its input into files/batches
    /// (object-store staging, multi-row INSERT) may rely on that. Byte-relay formats
    /// (PgCopyBinary) give NO alignment guarantee; such loaders must treat the stream
    /// as opaque bytes.
    fn send(&mut self, buf: Vec<u8>) -> impl Future<Output = Result<()>> + Send;
    /// Close the stream cleanly. Returns rows ingested if this sink reports them
    /// (Postgres COPY does; ClickHouse counts via [`Sink::rows_staged`] instead).
    fn finish(self) -> impl Future<Output = Result<u64>> + Send;
    /// Source-side failure: make the sink DISCARD the partial stream (a clean close
    /// could commit it), then hand the cause back for propagation.
    fn abort(self, cause: Error) -> impl Future<Output = Error> + Send;
}

pub(crate) trait Source: Sized + Send + Sync {
    /// Probe the table: columns, native types, PK facts. Must work on empty tables.
    fn probe(&self, table: &str) -> impl Future<Output = Result<TablePlan>> + Send;
    /// One catalog query for a multi-table run: `(name, estimated rows)` for either
    /// an explicit `tables` list (names echoed back exactly as given, every name
    /// verified to exist — loud-fail, never a silent skip) or a whole `schema`
    /// (apitap's own `__apitap_staging`/`_apitap_state` artifacts excluded).
    /// Estimates come from the planner's statistics (`reltuples`, `TABLE_ROWS`) —
    /// they only order/size the work, never gate correctness; `-1` = unknown
    /// (treated as large).
    fn catalog(
        &self,
        schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> impl Future<Output = Result<Vec<(String, i64)>>> + Send;
    /// Can this source produce `format` for this plan? (e.g. Postgres → RowBinary only
    /// when every column has a binary transcoding.)
    fn can_produce(&self, plan: &TablePlan, format: WireFormat) -> bool;
    /// Per-column delivery + SELECT expressions for a producible format.
    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane;
    /// Read statements covering the table: cursor ranges, then any source-specific
    /// PK-less fallback, then a single full statement. `want` is the target span count.
    /// `delta` (incremental modes) must be pushed into EVERY statement, including the
    /// min/max probe and the fallbacks — a span that forgets it re-reads the world.
    fn span_stmts(
        &self,
        table: &str,
        plan: &TablePlan,
        lane: &Lane,
        want: usize,
        delta: Option<&Delta>,
    ) -> impl Future<Output = Result<Vec<String>>> + Send;
    /// Spawn one worker per loader over a shared span queue; return rows reported by
    /// the loaders (0 when the sink counts server-side). Implementations own the hot
    /// loop: pull span → read → encode → coalesce → `loader.send`.
    fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> impl Future<Output = Result<u64>> + Send;
}

pub(crate) trait Sink: Sized + Send + Sync {
    type Loader: Loader;
    /// Ingest formats this sink accepts, best first. Negotiation picks the first one
    /// the source can produce. Non-static so a sink may ORDER lanes per
    /// connection (BigQuery prefers Parquet when CPU is plentiful, CSV when
    /// starved — measured, not guessed).
    fn accepts(&self) -> &[WireFormat];
    /// Can this sink take `format` for THIS plan? Default yes; a sink whose
    /// fast lane can't represent some column (BigQuery's Parquet lane vs
    /// unconstrained NUMERIC, bytea, exotic udts) declines and negotiation
    /// falls through to its next lane instead of hard-failing.
    fn lane_ok(&self, _plan: &TablePlan, _format: WireFormat) -> bool {
        true
    }
    /// Sink-specific plan constraints, applied before lane planning so the DDL and the
    /// encoders agree (e.g. ClickHouse: the ORDER BY column must be non-nullable).
    fn adjust_plan(&self, plan: &mut TablePlan);
    /// Create the staging table for this lane. `mode` is the effective mode: replace
    /// honors `durable`; incremental modes always stage UNLOGGED (staging never
    /// becomes the final table). Replace implementations should also capture whatever
    /// the swap would destroy (indexes, constraints, grants) for re-application.
    fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        durable: bool,
        mode: Mode,
    ) -> impl Future<Output = Result<()>> + Send;
    /// One ingest stream into staging (called once per worker).
    fn loader(&self) -> impl Future<Output = Result<Self::Loader>> + Send;
    /// Rows now in staging. `loaded` is the loaders' own count — sinks whose protocol
    /// reports rows return it as-is; others count server-side.
    fn rows_staged(&self, loaded: u64) -> impl Future<Output = Result<u64>> + Send;
    /// Incremental modes only: inspect the destination BEFORE staging. Returns whether
    /// the final table exists and its current `max(cursor)` as text. Implementations
    /// must also (a) verify the destination's columns match the plan (schema drift →
    /// a clear error, never a silent mis-append), (b) reject unsupported modes early
    /// (e.g. merge on ClickHouse), and (c) stash whatever finalize will need (merge
    /// keys). Never called for `Mode::Replace`.
    /// May also CONFORM the plan to the existing destination (e.g. ClickHouse
    /// mirrors the dest's column nullability so staging's structure matches for
    /// ATTACH — a view-sourced plan reports everything nullable, but the dest is
    /// the structural authority once it exists).
    fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> impl Future<Output = Result<DestState>> + Send;
    /// Land the staged rows: `Replace` = atomic swap; `Append` = move staged rows into
    /// the existing table; `Merge` = upsert them by primary key. When `rows == 0`,
    /// drop staging and leave the destination untouched (the 0-row guard) in every
    /// mode. `mode` here is the EFFECTIVE mode (a bootstrapped incremental run gets
    /// `Replace`).
    fn finalize(&self, rows: u64, mode: Mode) -> impl Future<Output = Result<()>> + Send;
}

/// Credential-free source identity for the destination's state table:
/// `scheme://host:port/db::table`. NEVER includes userinfo.
pub(crate) fn source_identity(src_url: &str, table: &str) -> String {
    match reqwest::Url::parse(src_url) {
        Ok(u) => {
            // Normalize so equivalent URLs yield ONE identity — a scheme alias or an
            // elided default port must not silently fork the watermark.
            let scheme = match u.scheme() {
                "postgresql" => "postgres",
                other => other,
            };
            let host = u.host_str().unwrap_or("unknown");
            let port = u.port().unwrap_or(match scheme {
                "postgres" => 5432,
                "mysql" => 3306,
                _ => 0,
            });
            let db = u.path().trim_matches('/');
            // Qualify the table half so 'events' and 'public.events' (the same
            // Postgres table) share one identity.
            let table = if scheme == "postgres" && !table.contains('.') {
                format!("public.{table}")
            } else {
                table.to_string()
            };
            format!("{scheme}://{host}:{port}/{db}::{table}")
        }
        // Defensive: strip anything before '@' so credentials can never leak.
        Err(_) => format!(
            "{}::{table}",
            src_url.rsplit('@').next().unwrap_or("unknown")
        ),
    }
}

/// Is this source type usable as an incremental cursor, and does its SQL literal
/// need quoting? Integers embed raw; date/time types embed as quoted text (both
/// Postgres `::text` and ClickHouse `toString` round-trip them losslessly at
/// microsecond precision). Anything else is rejected up front.
pub(crate) fn cursor_literal_quoted(udt: &str) -> Result<bool> {
    match udt {
        // integers (postgres udt names + mysql DATA_TYPE names)
        "int2" | "int4" | "int8" | "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            Ok(false)
        }
        // date/time
        "date" | "timestamp" | "timestamptz" | "datetime" => Ok(true),
        other => Err(Error::InvalidInput(format!(
            "cursor type '{other}' is not usable for append/merge — use an integer or \
             timestamp column"
        ))),
    }
}

/// Per-route tuning that is legitimately different between routes (measured, not
/// guessed — see benchmarks/README.md).
pub(crate) struct Profile {
    /// CPU-count → auto pipe count (before the memory cap).
    pub auto_parallel: fn(usize) -> usize,
    /// Spans per pipe in the work queue. >1 = tail balancing; 1 = one span per pipe.
    pub span_mult: usize,
}

/// Resolve the knobs every route shares. An explicit `parallel` is never overridden;
/// the auto value is capped by the cgroup memory budget (a 256 MB container at the
/// CPU-derived pipe count was OOM-killed before this).
pub(crate) fn knobs(opts: &TransferOptions, profile: &Profile) -> Result<(usize, usize)> {
    if opts.parallel == Some(0) {
        return Err(Error::InvalidInput("parallel must be at least 1".into()));
    }
    let chunk = opts.chunk_bytes.max(64 * 1024);
    let parallel = opts.parallel.unwrap_or_else(|| {
        crate::mem_capped_parallel((profile.auto_parallel)(num_cpus::get()), chunk)
    });
    Ok((chunk, parallel))
}

/// The whole transfer, once: probe → negotiate → stage → fan out → count → swap.
/// `started` is taken by the caller BEFORE connecting, so `elapsed_ms` includes
/// connection time (as the report always has). Borrows the source so a multi-table
/// run can drive many tables through one source (and its one pool).
///
/// `resolve` picks the FINAL pipe count once the real span count is known:
/// a single-table run passes `|n| parallel.min(n).max(1)` (the old behavior);
/// a multi-table run re-fits its budget grant there (see [`Grant::resize`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<S: Source, K: Sink, R: FnOnce(usize) -> usize>(
    src: &S,
    mut sink: K,
    table: &str,
    opts: &TransferOptions,
    profile: &Profile,
    chunk: usize,
    parallel: usize,
    resolve: R,
    started: std::time::Instant,
    source_id: &str,
) -> Result<TransferReport> {
    let mut plan = src.probe(table).await?;
    plan.cursor = opts.cursor.clone().or_else(|| plan.single_int_pk());
    sink.adjust_plan(&mut plan);

    // Incremental resolution: find the watermark in the DESTINATION (stateless — no
    // side files, no meta tables), or bootstrap as a full replace when the table
    // doesn't exist yet.
    let mut mode = opts.mode;
    let mut delta: Option<Delta> = None;
    if mode != Mode::Replace {
        let cursor = plan.cursor.clone().ok_or_else(|| {
            Error::InvalidInput(
                "append/merge needs a cursor column: pass cursor=... (integer or \
                 timestamp) or give the table a single-column integer primary key"
                    .into(),
            )
        })?;
        let col = plan.cols.iter().find(|c| c.name == cursor).ok_or_else(|| {
            Error::InvalidInput(format!("cursor column '{cursor}' not in source table"))
        })?;
        let quoted = cursor_literal_quoted(&col.udt)?;
        let st = sink.dest_state(&mut plan, mode, &cursor, source_id).await?;
        if !st.exists {
            mode = Mode::Replace; // first run: bootstrap the table with a full load
        } else if let Some(wm) = st.watermark {
            let literal = if quoted {
                format!("'{}'", wm.replace('\'', "''"))
            } else {
                // The watermark comes from destination DATA — never embed it raw
                // without proving it is the number it claims to be.
                wm.parse::<i128>().map_err(|_| {
                    Error::InvalidInput(format!(
                        "destination watermark '{wm}' is not an integer — the cursor \
                         column's types have drifted; run once with mode='replace'"
                    ))
                })?;
                wm
            };
            delta = Some(Delta {
                col: cursor,
                op: if mode == Mode::Merge { ">=" } else { ">" },
                literal,
            });
        } // exists but empty: full read, incremental landing
    }

    let format = sink
        .accepts()
        .iter()
        .copied()
        .find(|f| sink.lane_ok(&plan, *f) && src.can_produce(&plan, *f))
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "no common wire format for {} → this destination",
                plan.engine
            ))
        })?;
    let lane = src.plan_lane(&plan, format);

    sink.prepare(&plan, &lane, opts.durable, mode).await?;

    let want = if parallel > 1 {
        parallel * profile.span_mult
    } else {
        1
    };
    let stmts = src
        .span_stmts(table, &plan, &lane, want, delta.as_ref())
        .await?;
    let used = resolve(stmts.len()).min(stmts.len()).max(1);
    let mut loaders = Vec::with_capacity(used);
    for _ in 0..used {
        loaders.push(sink.loader().await?);
    }

    let loaded = src.run_workers(&plan, &lane, stmts, loaders, chunk).await?;
    let rows = sink.rows_staged(loaded).await?;
    sink.finalize(rows, mode).await?;

    Ok(TransferReport {
        rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: if rows == 0 { 0 } else { used },
    })
}

/// One table of a multi-table run: source name (as the user gave it / the catalog
/// listed it) and the planner's row estimate (`-1` = unknown).
pub(crate) struct TableJob {
    pub table: String,
    pub est_rows: i64,
}

/// A pipe is only worth having above roughly this many rows — below it, span setup
/// and the extra connection cost more than the parallel read wins (the 1M-row
/// benchmarks ran best at ~32–64k rows/pipe).
const ROWS_PER_PIPE: i64 = 32 * 1024;

/// How many pipes a table WANTS from the shared budget. Unknown sizes ask for
/// everything (a never-analyzed big table throttled to one pipe is the worse
/// failure mode); tiny tables ask for one and leave the rest to their siblings.
fn desired_pipes(est_rows: i64, budget: usize) -> usize {
    if est_rows < 0 {
        budget
    } else {
        usize::try_from(est_rows / ROWS_PER_PIPE)
            .unwrap_or(budget)
            .clamp(1, budget)
    }
}

/// The pipes one table holds from the shared budget, acquired ATOMICALLY
/// (`acquire_many`): tokio's fair semaphore hands released permits to the front
/// waiter first, so a queued table wakes holding its WHOLE ask — never 1 permit
/// with a dead top-up (released permits go to queued waiters, so a
/// wake-then-`try_acquire` loop would find nothing free and pin a 10M-row table
/// to a single pipe while the budget idles).
struct Grant<'a> {
    sem: &'a tokio::sync::Semaphore,
    held: Vec<tokio::sync::SemaphorePermit<'a>>,
    count: usize,
}

impl<'a> Grant<'a> {
    async fn acquire(sem: &'a tokio::sync::Semaphore, want: usize) -> Self {
        let permit = sem
            .acquire_many(want as u32)
            .await
            .expect("budget semaphore is never closed");
        Self {
            sem,
            held: vec![permit],
            count: want,
        }
    }

    /// Re-fit the grant once the REAL span count is known: excess goes back to the
    /// siblings immediately (a 1-span table — PK-less MySQL, say — must not sit on
    /// 32 permits for its whole stream), a shortfall tops up best-effort (planner
    /// stats under-asked; whatever is free now beats streaming under-parallel).
    /// Returns what is now held (≥ 1).
    fn resize(&mut self, target: usize) -> usize {
        let target = target.max(1);
        while self.count > target {
            let take = self.count - target;
            let last = self.held.last_mut().expect("grant holds at least one permit");
            let n = last.num_permits();
            if n <= take {
                self.held.pop();
                self.count -= n;
            } else {
                drop(last.split(take));
                self.count -= take;
            }
        }
        if self.count < target {
            if let Ok(extra) = self.sem.try_acquire_many((target - self.count) as u32) {
                self.count += extra.num_permits();
                self.held.push(extra);
            }
        }
        self.count
    }
}

/// Many tables through ONE pipe budget. The budget is the same number a
/// single-table run gets (CPU heuristic capped by the cgroup memory model), so
/// peak memory is the single-table ceiling — `budget × ~8×chunk + reserve` —
/// no matter how many tables are in flight.
///
/// Scheduling: largest-first (LPT). Each table atomically acquires its desired
/// pipe count (see [`Grant`]), then RESIZES to the real span count the moment the
/// spans are known — so a table that can't split frees its pipes for the
/// siblings, and one whose stats under-estimated tops back up from whatever is
/// free. Every table holds ≥ 1 permit while it runs, which alone bounds
/// tables-in-flight at `budget`.
///
/// Failure isolation: one table's error lands in ITS result and releases its
/// permits; the siblings keep running. Every table keeps the single-table
/// guarantees (atomic swap, 0-row guard) because each runs the unchanged [`run`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_many<S, K, F, Fut>(
    src: &S,
    mut jobs: Vec<TableJob>,
    opts: &TransferOptions,
    profile: &Profile,
    chunk: usize,
    budget: usize,
    src_url: &str,
    make_sink: F,
) -> Result<Vec<TableResult>>
where
    S: Source,
    K: Sink,
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<K>>,
{
    use futures::stream::{FuturesUnordered, StreamExt};

    // Largest first; unknown (-1) sorts largest of all.
    jobs.sort_by_key(|j| std::cmp::Reverse(if j.est_rows < 0 { i64::MAX } else { j.est_rows }));

    let sem = tokio::sync::Semaphore::new(budget);
    let mut futs = FuturesUnordered::new();
    for job in jobs {
        let sem = &sem;
        let make_sink = &make_sink;
        futs.push(async move {
            // Atomic ask (FIFO fair — a queued table wakes with ALL of it), refit
            // to the real span count inside run(). Permits release when the grant
            // drops — success, error, either way.
            let desired = desired_pipes(job.est_rows, budget);
            let mut grant = Grant::acquire(sem, desired).await;
            let granted = grant.count;

            let started = std::time::Instant::now();
            let source_id = source_identity(src_url, &job.table);
            let out = async {
                let sink = make_sink(job.table.clone()).await?;
                run(
                    src,
                    sink,
                    &job.table,
                    opts,
                    profile,
                    chunk,
                    granted,
                    |spans| grant.resize(spans.min(budget)),
                    started,
                    &source_id,
                )
                .await
            }
            .await;

            match out {
                Ok(r) => TableResult {
                    table: job.table,
                    rows: r.rows,
                    elapsed_ms: r.elapsed_ms,
                    parallel: r.parallel,
                    error: None,
                },
                Err(e) => TableResult {
                    table: job.table,
                    rows: 0,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    parallel: granted,
                    error: Some(e.to_string()),
                },
            }
        });
    }

    let mut results = Vec::new();
    while let Some(r) = futs.next().await {
        results.push(r);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_identity_is_normalized_and_credential_free() {
        let a = source_identity("postgres://user:s3cret@db.example:5432/prod", "public.t");
        let b = source_identity("postgresql://other:pw@db.example/prod", "public.t");
        assert_eq!(a, b); // scheme alias + elided default port normalize away
        assert_eq!(a, "postgres://db.example:5432/prod::public.t");
        // unqualified and public-qualified Postgres tables share one identity
        assert_eq!(
            source_identity("postgres://x:y@h/db", "t"),
            source_identity("postgres://h/db", "public.t")
        );
        assert!(!a.contains("s3cret") && !a.contains("user"));
        let m = source_identity("mysql://root:pw@127.0.0.1/bench", "events");
        assert_eq!(m, "mysql://127.0.0.1:3306/bench::events");
    }

    #[test]
    fn spans_cover_the_range_without_overlap() {
        let s = spans(1, 10, 4);
        assert_eq!(s, vec![(1, 3), (4, 6), (7, 9), (10, 10)]);
        // Full coverage, no gaps/overlap.
        assert_eq!(s.first().unwrap().0, 1);
        assert_eq!(s.last().unwrap().1, 10);
        for w in s.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0);
        }
        // More splits than values: never produces an empty/inverted span.
        let s = spans(5, 6, 8);
        assert_eq!(s, vec![(5, 5), (6, 6)]);
        // Single value.
        assert_eq!(spans(7, 7, 4), vec![(7, 7)]);
        // n=0 clamps to 1.
        assert_eq!(spans(1, 3, 0), vec![(1, 3)]);
    }
}
