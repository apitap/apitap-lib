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
