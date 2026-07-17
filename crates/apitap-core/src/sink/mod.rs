//! The SINK side of a transfer: stage rows through per-worker [`Loader`]
//! streams, then land them atomically (swap / append / merge).
//!
//! Adding a sink = one file here implementing [`Sink`], plus its URL scheme in
//! [`crate::pipeline::dispatch`]. Database-specific SQL vocabulary shared with
//! the same database's source lives in [`crate::dialect`].

use crate::error::{Error, Result};
use crate::plan::{DestState, Lane, TablePlan, WireFormat};
use crate::Mode;
use std::future::Future;

pub(crate) mod bigquery;
pub(crate) mod clickhouse;
pub(crate) mod mysql;
pub(crate) mod postgres;

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
    /// Incremental modes only: inspect the destination BEFORE staging.
    ///
    /// The watermark DECISION is shared — fetch your inputs (own state row,
    /// data max, sibling-row presence) however your database wants, then call
    /// [`crate::plan::resolve_watermark`] with your [`crate::plan::WmArbitration`].
    /// Its invariants (fan-in guard, empty-dest, no-state fallback) are the
    /// contract; hand-rolling them is how the MySQL sink drifted once.
    ///
    /// Returns whether
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
