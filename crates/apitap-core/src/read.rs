//! `apitap.read()` — parallel COPY-binary range scans decoded straight into
//! Arrow columnar batches, PULLED by the consumer (memory = the batches in
//! flight, never the table). The span machinery is the same one every bulk
//! route uses; the only new parts are the Arrow builders
//! ([`crate::wire::arrowcol`]) and this orchestration.

use crate::error::{Error, Result};
use crate::wire::arrowcol::{ArrowBatch, ArrowKind};

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
    /// Read ONLY these columns (source order preserved). `None` = all.
    /// The polars lazy plugin pushes its projection through here — a query
    /// that touches 2 of 15 columns then decodes 2 of 15 columns.
    pub columns: Option<Vec<String>>,
    /// Seal threshold override. `None` = auto from the cgroup budget;
    /// a huge value = the materialize fast path (each worker builds ONE
    /// giant batch — fewer FFI crossings, no consumer-side rechunk).
    pub batch_bytes: Option<usize>,
    /// SQL predicate ANDed into every span statement — the lazy plugin's
    /// FILTER pushdown. The server filters, so the wire and the decoders
    /// only carry survivors. Rendered by the trusted dialect-aware
    /// translator, never raw user text; the client-side filter still runs,
    /// so this is bandwidth, never correctness.
    pub push_where: Option<String>,
    /// The caller's own row predicate (`read(where=...)`), in the source's
    /// SQL. Unlike `push_where` it decides the result, so it is never dropped:
    /// it is parsed as exactly one expression in the source dialect, rendered
    /// back in canonical form and ANDed into every span statement. Text that
    /// is not one expression (`1=1) OR (1=1`, a second statement) is refused,
    /// because it would escape the span's own range and repeat rows.
    pub where_sql: Option<String>,
    /// Computed columns, Postgres only: (name, SQL expression yielding an
    /// `int8`). Each becomes a column of the read, selectable by name in
    /// `columns`. apitap.sql() ships an aggregate's argument this way, scaled
    /// to an exact integer, instead of the numeric columns it is made of.
    pub computed: Vec<(String, String)>,
    /// At most this many span statements (default: 6 per pipe, for skew).
    /// apitap.sql()'s key filter rides every statement, so it asks for one
    /// statement per pipe.
    pub max_spans: Option<usize>,
    /// Row gates the decoder applies (apitap.sql()'s join keys): a row
    /// whose gate column misses its set is dropped before its other columns
    /// decode. Column places are the read's. The consumer still checks every
    /// row, so a gate is work saved, never the answer; lanes that do not
    /// decode COPY binary ignore it.
    pub gates: Vec<std::sync::Arc<crate::wire::arrowcol::KeyGate>>,
}

/// One column of the result schema.
#[derive(Debug, Clone)]
pub struct ArrowField {
    pub name: String,
    pub kind: ArrowKind,
    pub nullable: bool,
    /// Read as TEXT although the source column is not text (json, arrays,
    /// enums, time, unconstrained numeric, …): a predicate on it compares
    /// the server's native value, not the string the reader sees, so the
    /// lazy pushdown must not cross the wire with it (MASTER M-46).
    pub rewritten: bool,
    /// Fixed-width text (Postgres `bpchar`, MySQL `CHAR`): its compares
    /// ignore trailing spaces, as the server's do. VARCHAR and TEXT keep them.
    pub padded: bool,
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
        Self {
            schema,
            rx,
            supervisor,
            completed,
        }
    }

    /// Blocking pull (called from an arbitrary consumer thread, GIL
    /// released). `Ok(None)` = clean end of stream.
    pub fn next_batch(&mut self) -> Result<Option<ArrowBatch>> {
        // NOT `rx.blocking_recv()`: tokio refuses to block from inside a
        // runtime context, and consumers like deltalake/datafusion pull from
        // one — the panic (pre-1.81) or abort would kill the process (review
        // round 2 R2-10). `futures::executor::block_on` polls from any
        // thread; the producer side keeps running on the runtime's workers.
        match futures::executor::block_on(self.rx.recv()) {
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

    /// Async pull, for consumers already on the runtime (apitap.sql()).
    pub(crate) async fn recv(&mut self) -> Result<Option<ArrowBatch>> {
        match self.rx.recv().await {
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

    /// Non-blocking pull: `None` = nothing ready yet, `Some(None)` = clean end.
    pub(crate) fn try_recv(&mut self) -> Result<Option<Option<ArrowBatch>>> {
        use tokio::sync::mpsc::error::TryRecvError;
        match self.rx.try_recv() {
            Ok(Ok(b)) => Ok(Some(Some(b))),
            Ok(Err(e)) => {
                self.supervisor.abort();
                Err(e)
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                if self.completed.load(std::sync::atomic::Ordering::Acquire) {
                    Ok(Some(None))
                } else {
                    Err(Error::Transfer("read: stream ended before the scan completed".into()))
                }
            }
        }
    }

    /// A finished result as a stream: the batches are already here, so the
    /// supervisor has nothing to watch. Must be called on the runtime.
    pub fn ready(schema: Vec<ArrowField>, batches: Vec<ArrowBatch>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(batches.len().max(1));
        for b in batches {
            tx.try_send(Ok(b)).expect("channel sized to the batches");
        }
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        Self {
            schema,
            rx,
            supervisor: tokio::spawn(async {}),
            completed,
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
/// Schema-only probe (no workers started): the fields `read_start` would
/// emit for this table. The polars lazy plugin registers this up front and
/// starts the real read later with the query's projection pushed down.
pub async fn read_schema(src_url: &str, table: &str) -> Result<Vec<ArrowField>> {
    crate::read_impl::schema(src_url, table).await
}

pub async fn read_start(src_url: &str, table: &str, opts: &ReadOptions) -> Result<ReadHandle> {
    crate::read_impl::start(src_url, table, opts).await
}
