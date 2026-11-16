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
    /// Text whose compares ignore trailing spaces, as the server's do:
    /// fixed-width (Postgres `bpchar`, MySQL `CHAR`) or under a MySQL PAD
    /// SPACE collation (all but the 0900 and `nopad` ones). Postgres VARCHAR
    /// and TEXT keep them.
    pub padded: bool,
    /// Text under a case-insensitive collation (MySQL `_ci`): its compares
    /// fold case, as the server's do.
    pub ci: bool,
    /// And accent-insensitive (every MySQL `_ci` but `_as_ci`): `'é' = 'e'`.
    pub ai: bool,
    /// Text whose source orders it otherwise than by its bytes: the
    /// collation, as a refusal names it (a MySQL collation but `_bin`/
    /// `binary`; a Postgres locale but C/POSIX/C.UTF-8, set by apitap.sql()'s
    /// probe). apitap.sql() checks an ORDER BY on it (`sql::order`).
    pub locale: Option<String>,
    /// NULL sorts lowest at the column's source (MySQL: first ascending,
    /// last descending), not highest (Postgres): an ORDER BY on it without
    /// NULLS FIRST/LAST places NULLs as its source does (W1 round P).
    pub nulls_low: bool,
    /// apitap.sql(): the source server a stage table's text column was read
    /// from unchanged (a pass-through of a source column, not computed), so
    /// an ORDER BY on it can ask that server how its collation orders a pair
    /// (`sql::order::verify`). `None` for a source table's own columns.
    pub origin: Option<String>,
}

/// The running read: a schema and a pull side. `next_batch` blocks the
/// calling (Python) thread until a batch, an error, or end-of-stream.
pub struct ReadHandle {
    pub schema: Vec<ArrowField>,
    rx: tokio::sync::mpsc::Receiver<Result<ArrowBatch>>,
    supervisor: tokio::task::JoinHandle<()>,
    completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The read's byte gate ([`BatchTx`]) and the units of the batch the
    /// consumer holds now: released when it asks for the next one.
    gate: Option<(std::sync::Arc<tokio::sync::Semaphore>, u32)>,
    held: u32,
    /// The read's claim on the pipes the open reads planned (given back
    /// when the handle drops).
    _planned: Option<crate::read_impl::ReadPlanned>,
}

/// The sending end of a read: a sealed batch takes its bytes from the read's
/// gate before it is queued, and gives them back when the consumer asks for
/// the batch after it (the fold has consumed it by then). The gate holds
/// what the read's pipes reserved for their batches, so sealed, queued and
/// in-hand batches stay inside it whatever the batch count; one batch larger
/// than the whole gate passes alone.
#[derive(Clone)]
pub(crate) struct BatchTx {
    tx: tokio::sync::mpsc::Sender<Result<ArrowBatch>>,
    gate: Option<(std::sync::Arc<tokio::sync::Semaphore>, u32)>,
}

/// A batch's bytes in gate units (KiB, rounded up), at most the whole gate.
fn gate_units(b: &ArrowBatch, cap: u32) -> u32 {
    u32::try_from(b.bytes().div_ceil(1024)).unwrap_or(u32::MAX).min(cap)
}

impl BatchTx {
    /// A gate of `bytes` (None: ungated, the count-bounded channel alone).
    pub(crate) fn new(tx: tokio::sync::mpsc::Sender<Result<ArrowBatch>>, bytes: Option<u64>) -> Self {
        let gate = bytes.map(|b| {
            let cap = u32::try_from(b.div_ceil(1024)).unwrap_or(u32::MAX >> 4).max(1);
            (std::sync::Arc::new(tokio::sync::Semaphore::new(cap as usize)), cap)
        });
        Self { tx, gate }
    }

    pub(crate) fn gate(&self) -> Option<(std::sync::Arc<tokio::sync::Semaphore>, u32)> {
        self.gate.clone()
    }

    pub(crate) async fn send(&self, item: Result<ArrowBatch>) -> std::result::Result<(), ()> {
        if let (Ok(b), Some((g, cap))) = (&item, &self.gate) {
            g.acquire_many(gate_units(b, *cap)).await.map_err(|_| ())?.forget();
        }
        self.tx.send(item).await.map_err(|_| ())
    }
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
            gate: None,
            held: 0,
            _planned: None,
        }
    }

    /// The handle of a read whose pipes are claimed in the read plan.
    pub(crate) fn with_planned(mut self, p: Option<crate::read_impl::ReadPlanned>) -> Self {
        self._planned = p;
        self
    }

    /// The handle of a gated read ([`BatchTx::gate`]).
    pub(crate) fn with_gate(mut self, gate: Option<(std::sync::Arc<tokio::sync::Semaphore>, u32)>) -> Self {
        self.gate = gate;
        self
    }

    /// The batch in hand is consumed: its bytes go back to the gate, and
    /// `next` (the one handed out now) takes its place.
    fn settle(&mut self, next: Option<&ArrowBatch>) {
        if let Some((g, cap)) = &self.gate {
            if self.held > 0 {
                g.add_permits(self.held as usize);
            }
            self.held = next.map_or(0, |b| gate_units(b, *cap));
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
        self.settle(None);
        match futures::executor::block_on(self.rx.recv()) {
            Some(Ok(b)) => {
                self.settle(Some(&b));
                Ok(Some(b))
            }
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
        self.settle(None);
        match self.rx.recv().await {
            Some(Ok(b)) => {
                self.settle(Some(&b));
                Ok(Some(b))
            }
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
        // Asking again means the batch in hand is consumed, whether or not
        // the next one is ready: holding its bytes past here could leave
        // the producers waiting on the gate behind an empty channel.
        self.settle(None);
        match self.rx.try_recv() {
            Ok(Ok(b)) => {
                self.settle(Some(&b));
                Ok(Some(Some(b)))
            }
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
            gate: None,
            held: 0,
            _planned: None,
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
    if crate::sql::mem::is_mem(src_url) {
        return crate::sql::mem::schema(src_url, table);
    }
    crate::read_impl::schema(src_url, table).await
}

pub async fn read_start(src_url: &str, table: &str, opts: &ReadOptions) -> Result<ReadHandle> {
    if crate::sql::mem::is_mem(src_url) {
        return crate::sql::mem::start(src_url, table, opts);
    }
    crate::read_impl::start(src_url, table, opts).await
}

#[cfg(test)]
mod gate_tests {
    use super::{BatchTx, ReadHandle};
    use crate::wire::arrowcol::{ArrowBatch, FinishedCol};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    fn batch(kib: usize) -> ArrowBatch {
        ArrowBatch { rows: kib * 128, cols: vec![FinishedCol::I64 { validity: None, data: vec![0; kib * 128] }] }
    }

    /// The gate holds sealed, queued and in-hand batches to its bytes, not to
    /// the channel's count; a batch past the whole gate passes alone; the
    /// consumer's batch is given back when it asks for the next.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_gate_bounds_the_bytes_a_read_holds() {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let tx = BatchTx::new(tx, Some(100 << 10));
        let gate = tx.gate();
        let sent = Arc::new(AtomicUsize::new(0));
        let (s2, t2) = (sent.clone(), tx.clone());
        let producer = tokio::spawn(async move {
            for kib in [40, 40, 40, 300, 10] {
                t2.send(Ok(batch(kib))).await.unwrap();
                s2.fetch_add(1, Ordering::SeqCst);
            }
        });
        drop(tx);
        let mut h = ReadHandle::new(Vec::new(), rx, tokio::spawn(async {}), Arc::new(AtomicBool::new(true))).with_gate(gate);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // 40 + 40 fit in 100 KiB; the third waits although the channel has room.
        assert_eq!(sent.load(Ordering::SeqCst), 2);
        assert_eq!(h.recv().await.unwrap().unwrap().rows, 40 * 128);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // In hand (40) + queued (40) still fill the gate: the third waits on.
        assert_eq!(sent.load(Ordering::SeqCst), 2);
        h.recv().await.unwrap().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(sent.load(Ordering::SeqCst), 3, "the first batch was given back");
        // The oversized batch waits until the gate is whole, then passes alone.
        h.recv().await.unwrap().unwrap();
        let big = h.recv().await.unwrap().unwrap();
        assert_eq!(big.rows, 300 * 128);
        assert_eq!(h.recv().await.unwrap().unwrap().rows, 10 * 128);
        producer.await.unwrap();
        assert!(h.recv().await.unwrap().is_none());
    }
}
