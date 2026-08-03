//! `apitap.read()` — parallel COPY-binary range scans decoded straight into
//! Arrow columnar batches, PULLED by the consumer (memory = the batches in
//! flight, never the table). The span machinery is the same one every bulk
//! route uses; the only new parts are the Arrow builders
//! ([`crate::wire::arrowcol`]) and this orchestration.

use crate::error::{Error, Result};
use crate::wire::arrowcol::{arrow_kind, ArrowBatch, ArrowKind, BatchBuilder};

/// Tuning for [`read_start`]. Defaults are auto everywhere — the simple
/// call needs nothing but a URL and a table.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Concurrent range pipes; `None` = auto (CPU heuristic capped by the
    /// cgroup memory budget). NOTE: batches from parallel pipes arrive in
    /// nondeterministic order — pass `Some(1)` for source order.
    pub parallel: Option<usize>,
    /// Numeric column to range-split on; `None` = the integer PK, else TID
    /// ranges, else a single stream.
    pub cursor: Option<String>,
    /// Raw SQL instead of a table (single stream; `table` ignored).
    pub query: Option<String>,
    /// Seal threshold override. `None` = auto from the cgroup budget;
    /// a huge value = the materialize fast path (each worker builds ONE
    /// giant batch — fewer FFI crossings, no consumer-side rechunk).
    pub batch_bytes: Option<usize>,
}

/// One column of the result schema.
#[derive(Debug, Clone)]
pub struct ArrowField {
    pub name: String,
    pub kind: ArrowKind,
    pub nullable: bool,
}

/// The running read: a schema and a pull side. `next_batch` blocks the
/// calling (Python) thread until a batch, an error, or end-of-stream.
pub struct ReadHandle {
    pub schema: Vec<ArrowField>,
    rx: tokio::sync::mpsc::Receiver<Result<ArrowBatch>>,
    supervisor: tokio::task::JoinHandle<()>,
    completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ReadHandle {
    pub(crate) fn new(
        schema: Vec<ArrowField>,
        rx: tokio::sync::mpsc::Receiver<Result<ArrowBatch>>,
        supervisor: tokio::task::JoinHandle<()>,
        completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self { schema, rx, supervisor, completed }
    }

    /// Blocking pull (called from an arbitrary consumer thread, GIL
    /// released). `Ok(None)` = clean end of stream.
    pub fn next_batch(&mut self) -> Result<Option<ArrowBatch>> {
        match self.rx.blocking_recv() {
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => {
                self.supervisor.abort();
                Err(e)
            }
            None => {
                if self.completed.load(std::sync::atomic::Ordering::Acquire) {
                    Ok(None)
                } else {
                    Err(Error::Transfer(
                        "read: stream ended before the scan completed".into(),
                    ))
                }
            }
        }
    }

    /// Consumer went away: stop the producers.
    pub fn cancel(&mut self) {
        self.supervisor.abort();
        self.rx.close();
    }
}

impl Drop for ReadHandle {
    fn drop(&mut self) {
        self.supervisor.abort();
    }
}

/// Probe, plan spans, spawn the scan and return the pull handle. Setup
/// errors (bad URL, missing table, unsupported types) surface HERE, before
/// any stream object exists on the Python side.
pub async fn read_start(src_url: &str, table: &str, opts: &ReadOptions) -> Result<ReadHandle> {
    crate::read_impl::start(src_url, table, opts).await
}
