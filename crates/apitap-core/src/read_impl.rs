//! The wiring behind [`crate::read_start`]: probe → arrow lane →
//! span statements → the SAME parallel COPY workers every bulk route uses,
//! with an [`ArrowLoader`] per worker feeding sealed batches into one
//! bounded channel the consumer pulls from. Batch size and channel depth
//! come from the cgroup budget — the 256 MB (and 44 MB) tiers hold by
//! arithmetic, not hope.

use crate::error::{Error, Result};
use crate::pipeline::{self, Profile};
use crate::plan::{Delivered, WireFormat};
use crate::read::{ArrowField, ReadHandle, ReadOptions};
use crate::sink::Loader;
use crate::source::{postgres::PgSource, Source};
use crate::wire::arrowcol::{arrow_kind, ArrowBatch, ArrowKind, BatchBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn start(
    src_url: &str,
    table: &str,
    opts: &ReadOptions,
) -> Result<ReadHandle> {
    if opts.query.is_some() {
        return Err(Error::InvalidInput(
            "read: query= lands next — pass table= (and optionally cursor=) today".into(),
        ));
    }
    let scheme = src_url.split("://").next().unwrap_or("");
    if !matches!(scheme, "postgres" | "postgresql") {
        return Err(Error::InvalidInput(format!(
            "read: unsupported source scheme '{scheme}' — Postgres first \
             (mysql lands next)"
        )));
    }

    // Read-side profile: pipes mostly WAIT (network + server-side COPY), so
    // fractional-CPU boxes still want several — the 0.5-core sweep measured
    // 1 pipe 46.7s vs 5 pipes 26.7s on the same box. Floor at 4; the cgroup
    // MEMORY cap still shrinks it on tiny-RAM tiers.
    let profile = Profile {
        auto_parallel: |c| (c / 2).max(4).min(8),
        span_mult: 6,
        table_pipe_cap: usize::MAX,
    };
    let topts = crate::TransferOptions {
        parallel: opts.parallel,
        cursor: opts.cursor.clone(),
        ..Default::default()
    };
    let (chunk, parallel) = pipeline::knobs(&topts, &profile)?;

    let src = PgSource::connect(src_url, parallel + 1).await?;
    let mut plan = src.probe(table).await?;
    plan.cursor = opts.cursor.clone().or_else(|| plan.single_int_pk());
    let mut lane = src.plan_lane(&plan, WireFormat::PgCopyBinary);

    // Arrow vocabulary per column; anything outside it (arrays, intervals,
    // enums, huge NUMERIC, json/jsonb, uuid) rides `::text` as Utf8 — the
    // simple contract: every table reads, exotic columns read as strings.
    let mut kinds = Vec::with_capacity(lane.cols.len());
    let mut schema = Vec::with_capacity(lane.cols.len());
    for (lc, pc) in lane.cols.iter_mut().zip(plan.cols.iter()) {
        let kind = match arrow_kind(&lc.delivered) {
            Some(k) => k,
            None => {
                lc.select = format!("({})::text", lc.select);
                lc.delivered = Delivered::Text;
                ArrowKind::Utf8
            }
        };
        kinds.push(kind);
        schema.push(ArrowField { name: pc.name.clone(), kind, nullable: pc.nullable });
    }

    let want = if parallel > 1 { parallel * profile.span_mult } else { 1 };
    let stmts = src.span_stmts(table, &plan, &lane, want, None).await?;
    let used = parallel.min(stmts.len()).max(1);

    // Residency model: N partial builders + C sealed batches in the channel
    // + 1 batch held by the consumer + N chunk buffers.
    let mem = pipeline::mem_limit_bytes();
    let mut cap = match mem {
        Some(m) if m < 128 << 20 => 1,
        _ => 2,
    };
    let batch_bytes = match opts.batch_bytes {
        Some(b) => {
            // Materialize fast path: one giant batch per worker.
            cap = 1;
            b
        }
        None => match mem {
            Some(m) => ((m.saturating_sub(40 << 20) as usize) / (4 * (used + cap + 1)))
                .clamp(1 << 20, 32 << 20),
            None => 16 << 20,
        },
    };

    let (tx, rx) = mpsc::channel::<Result<ArrowBatch>>(cap);
    let completed = Arc::new(AtomicBool::new(false));
    let loaders: Vec<ArrowLoader> = (0..used)
        .map(|_| ArrowLoader {
            b: BatchBuilder::new(kinds.clone(), batch_bytes),
            tx: tx.clone(),
            spare: None,
        })
        .collect();

    let done = completed.clone();
    let supervisor = tokio::spawn(async move {
        if let Err(e) = src.run_workers(&plan, &lane, stmts, loaders, chunk).await {
            let _ = tx.send(Err(e)).await;
        }
        // Ordering: mark complete BEFORE the last sender drops, so a `None`
        // recv on the consumer side always sees the truth.
        done.store(true, Ordering::Release);
        drop(tx);
    });

    Ok(ReadHandle::new(schema, rx, supervisor, completed))
}

/// One per worker: parses the worker's COPY stream into columnar builders
/// and ships sealed batches. Chunk buffers recycle through the worker loop
/// (the same zero-steady-state-allocation contract every sink honors).
struct ArrowLoader {
    b: BatchBuilder,
    tx: mpsc::Sender<Result<ArrowBatch>>,
    spare: Option<Vec<u8>>,
}

impl Loader for ArrowLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        self.b.push(&buf)?;
        self.spare = Some(buf);
        if let Some(batch) = self.b.take_ready() {
            self.tx
                .send(Ok(batch))
                .await
                .map_err(|_| Error::Transfer("read cancelled by consumer".into()))?;
        }
        Ok(())
    }

    fn reclaim(&mut self) -> Option<Vec<u8>> {
        self.spare.take().map(|mut v| {
            v.clear();
            v
        })
    }

    async fn finish(mut self) -> Result<u64> {
        let last = self.b.finish()?;
        let rows = self.b.rows_total();
        if let Some(batch) = last {
            self.tx
                .send(Ok(batch))
                .await
                .map_err(|_| Error::Transfer("read cancelled by consumer".into()))?;
        }
        Ok(rows)
    }

    async fn abort(self, cause: Error) -> Error {
        cause
    }
}
