//! The one generic transfer driver.
//!
//! Every route runs the same six-phase lifecycle — knobs, probe, lane negotiation,
//! staging, parallel workers over a span queue, count + atomic swap — so it lives here
//! exactly once, generic over [`Source`] and [`Sink`]. Each instantiation is fully
//! monomorphized: the hot loops (the sources' workers and the sinks' loaders) compile
//! to the same code as the hand-written routes they replaced, no dynamic dispatch.
//!
//! Adding a database = implement [`crate::source::Source`] in `source/<name>.rs`
//! and/or [`crate::sink::Sink`] in `sink/<name>.rs`, then add the scheme arms in
//! [`dispatch`] — nothing here changes.

pub(crate) mod dispatch;

use crate::error::{Error, Result};
use crate::plan::Delta;
use crate::{Mode, TableResult, TransferOptions, TransferReport};
use crate::sink::Sink;
use crate::source::Source;
use std::future::Future;


/// Credential-free source identity for the destination's state table:
/// `scheme://host:port/db::table`. NEVER includes userinfo.
/// Collapse URL-scheme aliases to one canonical spelling per engine. THE single
/// alias authority: route dispatch and `source_identity` both call it, so an
/// alias spelling can never route one way while forking the watermark identity
/// the other way. Only the routing/identity string is normalized — sinks still
/// parse the original URL (`clickhouse+https` keeps its TLS meaning there).
pub(crate) fn norm(scheme: &str) -> &str {
    match scheme {
        "postgresql" => "postgres",
        "clickhouse+https" => "clickhouse",
        other => other,
    }
}

pub(crate) fn source_identity(src_url: &str, table: &str) -> String {
    match reqwest::Url::parse(src_url) {
        Ok(u) => {
            // Normalize so equivalent URLs yield ONE identity — a scheme alias or an
            // elided default port must not silently fork the watermark.
            let scheme = norm(u.scheme());
            let host = u.host_str().unwrap_or("unknown");
            let port = u.port().unwrap_or(match scheme {
                "postgres" => crate::dialect::postgres::DEFAULT_PORT,
                "mysql" => crate::dialect::mysql::DEFAULT_PORT,
                _ => 0,
            });
            let db = u.path().trim_matches('/');
            // Qualify the table half so 'events' and 'public.events' (the same
            // Postgres table) share one identity.
            let table = if scheme == "postgres" {
                crate::dialect::postgres::canonical_table(table)
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
        mem_capped_parallel((profile.auto_parallel)(num_cpus::get()), chunk)
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
        let quoted = src.cursor_quoted(&col.udt)?;
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
#[derive(Debug)]
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
        // Grow one permit at a time: try_acquire_many is all-or-nothing, and a
        // partial top-up (3 free when 5 were wanted) is still worth taking.
        while self.count < target {
            match self.sem.try_acquire() {
                Ok(p) => {
                    self.held.push(p);
                    self.count += 1;
                }
                Err(_) => break,
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
    fn desired_pipes_sizes_the_ask() {
        // Unknown (-1) asks for the whole budget: a never-analyzed big table
        // throttled to one pipe is the worse failure mode.
        assert_eq!(desired_pipes(-1, 8), 8);
        // Tiny and empty tables ask for exactly one.
        assert_eq!(desired_pipes(0, 8), 1);
        assert_eq!(desired_pipes(100, 8), 1);
        assert_eq!(desired_pipes(ROWS_PER_PIPE - 1, 8), 1);
        // Scales by ROWS_PER_PIPE, clamped to the budget.
        assert_eq!(desired_pipes(ROWS_PER_PIPE * 3, 8), 3);
        assert_eq!(desired_pipes(i64::MAX, 8), 8);
    }

    #[tokio::test]
    async fn grant_acquires_atomically_and_resizes_both_ways() {
        let sem = tokio::sync::Semaphore::new(8);
        let mut g = Grant::acquire(&sem, 5).await;
        assert_eq!(g.count, 5);
        assert_eq!(sem.available_permits(), 3);

        // Shrink releases the excess back to the siblings immediately.
        assert_eq!(g.resize(2), 2);
        assert_eq!(sem.available_permits(), 6);

        // Grow is BEST-EFFORT PARTIAL: wanting 100 with 6 free takes the 6
        // (try_acquire_many alone would be all-or-nothing and take zero).
        assert_eq!(g.resize(100), 8);
        assert_eq!(sem.available_permits(), 0);

        // Floor is one pipe — a grant never resizes to nothing.
        assert_eq!(g.resize(0), 1);
        assert_eq!(sem.available_permits(), 7);

        // Dropping the grant returns everything.
        drop(g);
        assert_eq!(sem.available_permits(), 8);
    }

    #[tokio::test]
    async fn grant_queued_waiter_wakes_with_its_whole_ask() {
        // The bug this design exists to prevent: a queued table waking with one
        // permit and a dead top-up. acquire_many is atomic — the front waiter
        // gets its FULL ask when permits free up.
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
        let first = Grant::acquire(&sem, 4).await; // drains the budget
        let sem2 = sem.clone();
        let waiter = tokio::spawn(async move {
            let g = Grant::acquire(&sem2, 3).await;
            g.count
        });
        tokio::task::yield_now().await; // waiter is queued behind the drain
        drop(first); // release all 4
        assert_eq!(waiter.await.unwrap(), 3); // woke with 3, not 1
    }

}
