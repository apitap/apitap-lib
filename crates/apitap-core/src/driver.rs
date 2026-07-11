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
use crate::plan::{Lane, TablePlan, WireFormat};
use crate::{TransferOptions, TransferReport};
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
    /// Can this source produce `format` for this plan? (e.g. Postgres → RowBinary only
    /// when every column has a binary transcoding.)
    fn can_produce(&self, plan: &TablePlan, format: WireFormat) -> bool;
    /// Per-column delivery + SELECT expressions for a producible format.
    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane;
    /// Read statements covering the table: cursor ranges, then any source-specific
    /// PK-less fallback, then a single full statement. `want` is the target span count.
    fn span_stmts(
        &self,
        table: &str,
        plan: &TablePlan,
        lane: &Lane,
        want: usize,
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
    /// the source can produce.
    fn accepts(&self) -> &'static [WireFormat];
    /// Sink-specific plan constraints, applied before lane planning so the DDL and the
    /// encoders agree (e.g. ClickHouse: the ORDER BY column must be non-nullable).
    fn adjust_plan(&self, plan: &mut TablePlan);
    /// Create the staging table for this lane.
    fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        durable: bool,
    ) -> impl Future<Output = Result<()>> + Send;
    /// One ingest stream into staging (called once per worker).
    fn loader(&self) -> impl Future<Output = Result<Self::Loader>> + Send;
    /// Rows now in staging. `loaded` is the loaders' own count — sinks whose protocol
    /// reports rows return it as-is; others count server-side.
    fn rows_staged(&self, loaded: u64) -> impl Future<Output = Result<u64>> + Send;
    /// Atomic swap-in — or, when `rows == 0`, drop staging and leave the existing
    /// destination untouched (the 0-row guard).
    fn finalize(&self, rows: u64) -> impl Future<Output = Result<()>> + Send;
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
/// connection time (as the report always has).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<S: Source, K: Sink>(
    src: S,
    mut sink: K,
    table: &str,
    opts: &TransferOptions,
    profile: &Profile,
    chunk: usize,
    parallel: usize,
    started: std::time::Instant,
) -> Result<TransferReport> {
    let mut plan = src.probe(table).await?;
    plan.cursor = opts.cursor.clone().or_else(|| plan.single_int_pk());
    sink.adjust_plan(&mut plan);

    let format = sink
        .accepts()
        .iter()
        .copied()
        .find(|f| src.can_produce(&plan, *f))
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "no common wire format for {} → this destination",
                plan.engine
            ))
        })?;
    let lane = src.plan_lane(&plan, format);

    sink.prepare(&plan, &lane, opts.durable).await?;

    let want = if parallel > 1 {
        parallel * profile.span_mult
    } else {
        1
    };
    let stmts = src.span_stmts(table, &plan, &lane, want).await?;
    let used = parallel.min(stmts.len()).max(1);
    let mut loaders = Vec::with_capacity(used);
    for _ in 0..used {
        loaders.push(sink.loader().await?);
    }

    let loaded = src.run_workers(&plan, &lane, stmts, loaders, chunk).await?;
    let rows = sink.rows_staged(loaded).await?;
    sink.finalize(rows).await?;

    Ok(TransferReport {
        rows,
        elapsed_ms: started.elapsed().as_millis() as u64,
        parallel: if rows == 0 { 0 } else { used },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
