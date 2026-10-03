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
pub(crate) mod gcs;
pub(crate) mod iceberg;
pub(crate) mod mysql;
pub(crate) mod s3;
pub(crate) mod postgres;

/// What ONE pipe of this sink's loader keeps resident at its worst instant,
/// BEYOND the pipeline's measured chunk-scale term (`pipeline::PIPE_CHUNKS ×
/// chunk`), for `mode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PipeResidency {
    /// Chunk- and row-group-independent bytes.
    pub fixed: u64,
    /// Multiplier on the planner-chosen `RowGroup`; 0 = the sink has none.
    pub per_row_group: u64,
    /// Chunk-sized copies the loader holds on top of the pipeline term.
    pub chunks: u64,
}

impl PipeResidency {
    /// The loader holds only chunk-scale buffers already inside the measured
    /// 10×chunk term.
    pub const STREAMING: Self = Self {
        fixed: 0,
        per_row_group: 0,
        chunks: 0,
    };
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
    /// Hand back an emptied buffer from an already-shipped send, if this sink has one
    /// ready. Workers use it to recycle chunk buffers instead of allocating fresh —
    /// the fresh 4 MiB chunk Vec was 99.9% of steady-state allocator traffic
    /// (benchmarks/profiling.md). Default: none; sinks that consume the buffer
    /// zero-copy (ClickHouse HTTP body) simply never return one.
    fn reclaim(&mut self) -> Option<Vec<u8>> {
        None
    }
    /// Frame-aware fast lane: this loader can consume a RAW wire window
    /// (CopyData headers embedded) in place. Only the Arrow read loader
    /// says yes; everyone else keeps the copying planes.
    fn framed_capable(&self) -> bool {
        false
    }
    /// Consume one raw window ([`Loader::framed_capable`] loaders only).
    /// Returns bytes consumed and why the scan stopped.
    fn send_framed(
        &mut self,
        _win: &[u8],
    ) -> impl Future<Output = Result<(usize, crate::wire::arrowcol::FramedPush)>> + Send {
        async move {
            Err(Error::Transfer(
                "send_framed on a loader that is not framed_capable".into(),
            ))
        }
    }
    /// Close the stream cleanly. Returns rows ingested if this sink reports them
    /// (Postgres COPY does; ClickHouse counts via [`Sink::rows_staged`] instead).
    ///
    /// The channel-backed loaders commit on a clean end only: their input is a
    /// [`Msg`] stream, and a sender dropped without [`Msg::Finish`] surfaces the
    /// [`DROPPED`] error to the consumer, which aborts server-side instead of
    /// committing the partial stream.
    ///
    /// `finish` and `abort` are called only by `pipe::Pipe::drive`. Sources hold
    /// `&mut pipe::Pipe<L>`, which has neither.
    ///
    /// A loader that runs its ingest in a spawned task holds that task in a
    /// [`JoinOnce`](crate::pipe::JoinOnce) and joins it AT MOST ONCE. `send`
    /// joins it to report the REAL failure behind a closed channel — a proxy's
    /// 413, a COPY the server aborted — and the abort the engine runs next
    /// must not await the same handle again: awaiting a finished task handle
    /// twice is a panic, and it costs the run the error it was carrying.
    fn finish(self) -> impl Future<Output = Result<u64>> + Send;
    /// Source-side failure: make the sink DISCARD the partial stream (a clean close
    /// could commit it), then hand the cause back for propagation.
    fn abort(self, cause: Error) -> impl Future<Output = Error> + Send;
}

/// One item of a channel-backed loader's input: buffers, then — at most once,
/// at the very end — the proof that the producer meant to finish.
pub(crate) enum Msg<B> {
    Buf(B),
    Finish,
}

pub(crate) const DROPPED: &str =
    "apitap: stream closed without Finish — discarded, never committed";

/// Adapter from a [`Msg`] channel to the `Stream<io::Result<B>>` shape the
/// pg-overlap sender task, mysql_async's LOCAL INFILE handler and
/// `reqwest::Body::wrap_stream` all want.
///
/// `Buf` → `Ok(b)`; `Finish` → clean end; the channel closing WITHOUT `Finish`
/// → one `Err`, then end. That last arm is the point: a channel-backed loader
/// commits on a clean end, so "closed without Finish" — a dropped sender, a
/// panicking worker, the crew dropped from above on the read path — must abort
/// server-side, never silently commit.
///
/// Hand-written (not `stream::unfold`) so it is `Send + Sync + Unpin` whatever
/// bounds those three consumers carry.
pub(crate) struct FinishMarked<B> {
    rx: Option<futures::channel::mpsc::Receiver<Msg<B>>>,
}

impl<B> FinishMarked<B> {
    pub(crate) fn new(rx: futures::channel::mpsc::Receiver<Msg<B>>) -> Self {
        Self { rx: Some(rx) }
    }
}

impl<B> futures::Stream for FinishMarked<B> {
    type Item = std::io::Result<B>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt as _;
        let Some(rx) = self.rx.as_mut() else {
            return std::task::Poll::Ready(None);
        };
        match futures::ready!(rx.poll_next_unpin(cx)) {
            Some(Msg::Buf(b)) => std::task::Poll::Ready(Some(Ok(b))),
            Some(Msg::Finish) => {
                self.rx = None;
                std::task::Poll::Ready(None)
            }
            None => {
                self.rx = None;
                std::task::Poll::Ready(Some(Err(std::io::Error::other(DROPPED))))
            }
        }
    }
}

pub(crate) trait Sink: Sized + Send + Sync {
    type Loader: Loader;
    /// Ingest formats this sink accepts, best first. Negotiation picks the first one
    /// the source can produce. Non-static so a sink may ORDER lanes per
    /// connection (BigQuery prefers Parquet when CPU is plentiful, CSV when
    /// starved — measured, not guessed).
    fn accepts(&self) -> &[WireFormat];
    /// REQUIRED. A sink that says nothing is a sink the planner cannot see:
    /// 0.56.0 planned four parquet pipes into 128 MiB as "4 × 20 MiB" while
    /// each held ~85 MiB (audit §3.15). The `10 × chunk` term is the
    /// pipeline's; this declares what the LOADER holds on top of it.
    fn pipe_residency(mode: Mode) -> PipeResidency;
    /// Can this sink take `format` for THIS plan? Default yes; a sink whose
    /// fast lane can't represent some column (BigQuery's Parquet lane vs
    /// unconstrained NUMERIC, bytea, exotic udts) declines and negotiation
    /// falls through to its next lane instead of hard-failing.
    fn lane_ok(&self, _plan: &TablePlan, _format: WireFormat) -> bool {
        true
    }
    /// Sink-specific plan constraints, applied before lane planning so the DDL and the
    /// encoders agree (e.g. ClickHouse: the ORDER BY column must be non-nullable).
    /// Default: none — a first-cut sink doesn't have to think about this.
    fn adjust_plan(&self, _plan: &mut TablePlan) {}
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
    ) -> impl Future<Output = Result<DestState>> + Send {
        // Default for replace-only first-cut sinks: incremental modes refuse
        // loudly instead of forcing every new sink to implement state handling
        // before it can ship a full-refresh path.
        let _ = (plan, mode, cursor, source_id);
        async move {
            Err(Error::InvalidInput(
                "append/merge are not supported by this destination yet — use \
                 mode='replace'"
                    .into(),
            ))
        }
    }
    /// Land the staged rows: `Replace` = atomic swap; `Append` = move staged rows into
    /// the existing table; `Merge` = upsert them by primary key. When `rows == 0`,
    /// drop staging and leave the destination untouched (the 0-row guard) in every
    /// mode. `mode` here is the EFFECTIVE mode (a bootstrapped incremental run gets
    /// `Replace`).
    fn finalize(&self, rows: u64, mode: Mode) -> impl Future<Output = Result<()>> + Send;

    /// Undo what [`prepare`](Sink::prepare) created, after the run has failed.
    ///
    /// Drop THIS RUN's artifacts and nothing else — the name carries the run
    /// token, so "mine" is decidable without a scan. Never touch a peer's.
    ///
    /// Why this exists: until 0.55.1 the driver's happy path was a straight `?`
    /// chain with no error arm, and the trait had no hook it could have called.
    /// A source connection dropped mid-COPY, a statement timeout, a destination
    /// DDL error — any of them left this run's staging behind. Before 0.55.0
    /// that cost disk until the next run blindly dropped it. Since 0.55.0 the
    /// next run classifies a foreign token as `Found::Live` and REFUSES, so the
    /// leftover turns every later run of that table into a `locked:` error
    /// naming a run that is not running. `docs/failure-modes.md` promised
    /// "every ordinary error path still drops its own staging"; this is the
    /// method that makes the sentence true.
    ///
    /// Best-effort by contract: the driver logs a failure here and returns the
    /// ORIGINAL error, because the reason the run failed is more useful to the
    /// operator than the reason the cleanup did. A sink that leaves nothing
    /// behind (or whose finalize already released it) can keep the default.
    fn discard(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Drop this run's announcement — the tokenized `__apitap_lock` `prepare`
    /// writes before it scans for peers.
    ///
    /// Called by `pipeline::run` when `prepare` FAILS, and only then: the
    /// success paths release it themselves, each at the point where the run
    /// becomes visible some other way (staging exists, or the run is over).
    ///
    /// It has to be the driver's job because `prepare` is the one step outside
    /// the error arm that covers everything else, and a run refused by its own
    /// scan has already announced itself. Without this it leaves that
    /// announcement behind, and the NEXT run reads it as a live peer and refuses
    /// too — forever, over a run that never started. Measured in
    /// `e2e_failure_modes.py` leg 1, where it poisoned the retry after the
    /// operator had already cleaned up. Same class of leak the 0.55.1 error arm
    /// removed for staging.
    ///
    /// Best-effort by contract, like `discard`: the original error is what the
    /// operator needs to read.
    fn release_lock(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

#[cfg(test)]
mod tests {
    use super::{PipeResidency, Sink};
    use crate::sink::{bigquery::BqSink, clickhouse::ChSink, gcs::GcsSink};
    use crate::sink::{iceberg::IcebergSink, mysql::MySqlSink, postgres::PgSink, s3::S3Sink};
    use crate::wire::bqparquet::parquet_residency;
    use crate::Mode;

    /// The finish marker IS the commit decision for every channel-backed
    /// loader: a clean EOF means "the producer finished on purpose" and the
    /// server commits, so a channel that closes without `Finish` must yield an
    /// error item instead of looking like a clean end.
    ///
    /// RED (mutation: the `None` arm returns `Poll::Ready(None)` like a plain
    /// receiver): `Buf, Buf, <drop tx>` collects `[Ok, Ok]` — the 0.56.0
    /// commit-on-drop.
    #[tokio::test]
    async fn finish_marked_errors_when_dropped_without_finish() {
        use super::{FinishMarked, Msg, DROPPED};
        use futures::StreamExt as _;

        async fn collect(items: Vec<Msg<Vec<u8>>>) -> Vec<std::io::Result<Vec<u8>>> {
            let (mut tx, rx) = futures::channel::mpsc::channel::<Msg<Vec<u8>>>(8);
            for i in items {
                tx.try_send(i).expect("fresh channel has capacity");
            }
            drop(tx);
            FinishMarked::new(rx).collect().await
        }

        // Dropped without Finish: two buffers, then exactly one DROPPED error.
        let got = collect(vec![Msg::Buf(b"a".to_vec()), Msg::Buf(b"b".to_vec())]).await;
        assert_eq!(got.len(), 3, "a drop without Finish is an abort item");
        assert_eq!(got[0].as_deref().unwrap(), &b"a"[..]);
        assert_eq!(got[1].as_deref().unwrap(), &b"b"[..]);
        let err = got[2].as_ref().expect_err("drop without Finish must error");
        assert_eq!(err.to_string(), DROPPED);

        // Finish is a clean end: everything before it, then None.
        let got = collect(vec![Msg::Buf(b"a".to_vec()), Msg::Finish]).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].as_deref().unwrap(), &b"a"[..]);

        // Finish ends the stream even with buffers still queued behind it.
        let got = collect(vec![Msg::Finish, Msg::Buf(b"a".to_vec())]).await;
        assert!(got.is_empty());
    }

    /// I7: every sink declares its loader's residency, pinned as NUMBERS so a
    /// drift in `parquet_residency` or in one impl shows up here instead of a
    /// cgroup OOM at 4 pipes. The STREAMING routes stay at zero: they are the
    /// bit-identity fence with 0.56.0's plan.
    #[test]
    fn every_sink_declares_its_pipe_residency() {
        use Mode::{Append, Merge, Replace};
        let streaming = PipeResidency {
            fixed: 0,
            per_row_group: 0,
            chunks: 0,
        };
        let parquet = PipeResidency {
            fixed: 11 << 20,
            per_row_group: 3,
            chunks: 2,
        };
        let merge = PipeResidency {
            fixed: 21 << 20,
            per_row_group: 3,
            chunks: 2,
        };
        assert_eq!(PipeResidency::STREAMING, streaming);
        assert_eq!(parquet_residency(false), parquet);
        assert_eq!(parquet_residency(true), merge);
        for m in [Replace, Append, Merge] {
            assert_eq!(PgSink::pipe_residency(m), streaming, "pg {m:?}");
            assert_eq!(ChSink::pipe_residency(m), streaming, "ch {m:?}");
            assert_eq!(MySqlSink::pipe_residency(m), streaming, "my {m:?}");
            assert_eq!(S3Sink::pipe_residency(m), parquet, "s3 {m:?}");
            assert_eq!(GcsSink::pipe_residency(m), parquet, "gcs {m:?}");
            assert_eq!(BqSink::pipe_residency(m), parquet, "bq {m:?}");
            let want = if m == Merge { merge } else { parquet };
            assert_eq!(IcebergSink::pipe_residency(m), want, "ice {m:?}");
        }
    }
}
