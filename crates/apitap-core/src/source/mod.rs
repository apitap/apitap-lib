//! The SOURCE side of a transfer: probe a table, list a catalog, split reads
//! into spans, and run the workers that stream bytes into the sink's loaders.
//!
//! Adding a source = one file here implementing [`Source`], plus its URL scheme
//! in [`crate::pipeline::dispatch`]. Database-specific SQL vocabulary shared
//! with the same database's sink lives in [`crate::dialect`].

use crate::error::Result;
use crate::plan::{Delta, Lane, TablePlan, WireFormat};
use crate::sink::Loader;
use std::future::Future;

pub(crate) mod csvfile;
pub(crate) mod github;
pub(crate) mod gsheets;
pub(crate) mod mysql;
pub(crate) mod postgres;

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
    /// Cursor-type vocabulary of THIS source's database: is `udt` usable as an
    /// incremental cursor, and does its literal need quoting? (Delegates to the
    /// dialect — the per-DB lists live in `crate::dialect`.)
    fn cursor_quoted(&self, udt: &str) -> Result<bool>;
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
