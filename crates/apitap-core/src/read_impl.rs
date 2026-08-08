//! The wiring behind [`crate::read_start`]: probe → arrow lane →
//! span statements → the SAME parallel workers every bulk route uses,
//! with an [`ArrowLoader`] per worker feeding sealed batches into one
//! bounded channel the consumer pulls from. Batch size and channel depth
//! come from the cgroup budget — the 256 MB (and 44 MB) tiers hold by
//! arithmetic, not hope.
//!
//! Two sources speak this path: Postgres rides its raw COPY plane
//! (framed windows straight off the walsender socket), MySQL rides the
//! transfer route's binary-protocol workers, which already emit the same
//! PG binary-COPY stream the batch builder decodes — one wire vocabulary,
//! two databases.

use crate::error::{Error, Result};
use crate::pipeline::{self, Profile};
use crate::plan::{Delivered, WireFormat};
use crate::read::{ArrowField, ReadHandle, ReadOptions};
use crate::sink::Loader;
use crate::source::{mysql::MySqlSource, postgres::PgSource, Source};
use crate::wire::arrowcol::{arrow_kind, ArrowBatch, ArrowKind, BatchBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// One-time allocator tuning for the batch cycle: column buffers run 1–32 MiB
/// and churn every batch — above glibc's default M_MMAP_THRESHOLD (128 KiB)
/// each one is a fresh mmap the kernel zeroes page by page (clear_page_erms
/// profiled ~13% of a 0.5-core run) and a munmap on free. Raising the
/// threshold keeps them in the arena, where freed blocks come back warm.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn tune_allocator() {
    // Only above the tight tiers: arena reuse holds freed blocks, and on a
    // 256 MB box that retention measured +20-30 MB peak — there, glibc's
    // default mmap/munmap behavior IS the memory ceiling working as intended.
    if pipeline::mem_limit_bytes().is_some_and(|m| m < 512 << 20) {
        return;
    }
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, 64 << 20);
    });
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn tune_allocator() {}

/// The per-source seams of the read path. Everything else — spans, workers,
/// the batch builder, the memory model — is the shared [`Source`] machinery.
#[derive(Clone, Copy, PartialEq)]
enum ReadDialect {
    Pg,
    My,
}

fn read_dialect(src_url: &str) -> Result<ReadDialect> {
    let scheme = src_url.split("://").next().unwrap_or("");
    match scheme {
        "postgres" | "postgresql" => Ok(ReadDialect::Pg),
        "mysql" => Ok(ReadDialect::My),
        _ => Err(Error::InvalidInput(format!(
            "read: unsupported source scheme '{scheme}' — postgres:// and \
             mysql:// today"
        ))),
    }
}

/// Schema-only probe: what [`start`] would emit, WITHOUT starting workers.
/// The lazy plugin registers this schema up front, then starts the real
/// read later with the query's projection pushed down.
pub(crate) async fn schema(src_url: &str, table: &str) -> Result<Vec<ArrowField>> {
    match read_dialect(src_url)? {
        ReadDialect::Pg => schema_with(PgSource::connect(src_url, 1).await?, table).await,
        ReadDialect::My => schema_with(MySqlSource::connect(src_url, 1).await?, table).await,
    }
}

async fn schema_with<S: Source>(src: S, table: &str) -> Result<Vec<ArrowField>> {
    let plan = src.probe(table).await?;
    let lane = src.plan_lane(&plan, WireFormat::PgCopyBinary);
    Ok(lane
        .cols
        .iter()
        .zip(plan.cols.iter())
        .map(|(lc, pc)| ArrowField {
            name: pc.name.clone(),
            kind: arrow_kind(&lc.delivered).unwrap_or(ArrowKind::Utf8),
            nullable: pc.nullable,
        })
        .collect())
}

pub(crate) async fn start(
    src_url: &str,
    table: &str,
    opts: &ReadOptions,
) -> Result<ReadHandle> {
    tune_allocator();
    if opts.query.is_some() {
        return Err(Error::InvalidInput(
            "read: query= lands next — pass table= (and optionally cursor=) today".into(),
        ));
    }
    let dialect = read_dialect(src_url)?;

    // Read-side profile: pipes mostly WAIT (network + server-side reads), so
    // fractional-CPU boxes still want several — the 0.5-core sweep measured
    // 1 pipe 46.7s vs 5 pipes 26.7s on the same box. Floor at 4; the cgroup
    // MEMORY cap still shrinks it on tiny-RAM tiers. MySQL workers pay real
    // client-side decode CPU on top — same profile for now, its own sweep
    // owns the number once profiled.
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
    let (chunk, mut parallel) = pipeline::knobs(&topts, &profile)?;
    if opts.parallel.is_none() {
        if let Some(m) = pipeline::mem_limit_bytes() {
            // Read pipes hold no sink-side buffers, so the transfer model's
            // 10×chunk-per-pipe cap overshoots here. Measured on the 256 MB
            // tier (10M rows, 0.5 core): 5 pipes 26.7s/194MB peak, 6 pipes
            // 24.8s/211MB, 7 pipes 25.6s/238MB — 6 is the knee. ~36 MB per
            // pipe reproduces that knee and scales down with the budget.
            parallel = ((m.saturating_sub(40 << 20)) / (36 << 20)).clamp(1, 8) as usize;
        }
    }

    match dialect {
        ReadDialect::Pg => {
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let rp = plan_read(&src, dialect, table, opts, parallel).await?;
            Ok(launch_loaders(src, rp, chunk))
        }
        ReadDialect::My => {
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            let rp = plan_read(&src, dialect, table, opts, parallel).await?;
            // Direct-Arrow lane by default: cells decode straight into the
            // column builders (one dispatch, one copy). APITAP_MY_ARROW=0
            // keeps the COPY lane — the same A/B-and-escape-hatch pattern
            // as the pg raw plane's APITAP_RAW_COPY.
            if std::env::var("APITAP_MY_ARROW").is_ok_and(|v| v == "0") {
                Ok(launch_loaders(src, rp, chunk))
            } else {
                Ok(launch_my_arrow(src, rp))
            }
        }
    }
}

/// Everything [`start`] resolves before workers move: the probed plan, the
/// lane, per-column Arrow kinds/schema, span statements, and the residency
/// numbers (worker count, channel depth, batch seal size).
struct ReadPlan {
    plan: crate::plan::TablePlan,
    lane: crate::plan::Lane,
    kinds: Vec<ArrowKind>,
    schema: Vec<ArrowField>,
    stmts: Vec<String>,
    used: usize,
    cap: usize,
    batch_bytes: usize,
}

async fn plan_read<S: Source>(
    src: &S,
    dialect: ReadDialect,
    table: &str,
    opts: &ReadOptions,
    parallel: usize,
) -> Result<ReadPlan> {
    let mut plan = src.probe(table).await?;
    plan.cursor = opts.cursor.clone().or_else(|| plan.single_int_pk());
    let mut lane = src.plan_lane(&plan, WireFormat::PgCopyBinary);
    if dialect == ReadDialect::Pg {
        // Spans arrive verbatim (FrameRaw): the batch builder walks each
        // span's own header/trailer, so the strip-and-recopy stage — one full
        // memcpy of the stream plus a 4 MiB accumulator per pipe —
        // disappears. PG-only: the framed windows are raw walsender wire;
        // MySQL workers synthesize their COPY stream client-side and feed the
        // plain (chunk-boundary-safe) push path.
        lane.raw_frames = true;
    }
    // Predicate pushdown (the lazy plugin's filter): every span statement
    // gains an AND, so the server filters and the decoders only see
    // survivors. The client-side filter still runs — bandwidth, never
    // correctness.
    lane.push_where = opts.push_where.clone();

    // Column projection (the lazy plugin pushes polars' with_columns here):
    // the SELECT list narrows, so the server serializes and this side decodes
    // ONLY what the query touches — requested order preserved. plan.cols
    // narrows in lockstep: MySQL derives its per-column encoders from there.
    if let Some(want) = &opts.columns {
        let idx: Vec<usize> = want
            .iter()
            .map(|w| {
                plan.cols.iter().position(|c| &c.name == w).ok_or_else(|| {
                    Error::InvalidInput(format!(
                        "read: column '{w}' is not in {table} (columns: {})",
                        plan.cols
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })
            })
            .collect::<Result<_>>()?;
        lane.cols = idx.iter().map(|&i| lane.cols[i].clone()).collect();
        plan.cols = idx.iter().map(|&i| plan.cols[i].clone()).collect();
    }

    // Arrow vocabulary per column; anything outside it (arrays, intervals,
    // enums, huge NUMERIC, json/jsonb, uuid) rides as Utf8 — the simple
    // contract: every table reads, exotic columns read as strings. HOW the
    // text arrives is per-dialect: Postgres rewrites the SELECT (`::text`,
    // the server serializes text); MySQL rewrites the column's udt so the
    // client-side encoder (derived from plan.cols, not the lane) emits a
    // plain text field — json and oversized NEWDECIMAL already arrive as
    // utf8 bytes on the binary protocol, no server cast needed.
    let mut kinds = Vec::with_capacity(lane.cols.len());
    let mut schema = Vec::with_capacity(lane.cols.len());
    for (lc, pc) in lane.cols.iter_mut().zip(plan.cols.iter_mut()) {
        let kind = match arrow_kind(&lc.delivered) {
            Some(k) => k,
            None => {
                match dialect {
                    ReadDialect::Pg => {
                        lc.select = format!("({})::text", lc.select);
                    }
                    ReadDialect::My => {
                        pc.udt = "longtext".into();
                        pc.precision = None;
                        pc.scale = None;
                    }
                }
                lc.delivered = Delivered::Text;
                ArrowKind::Utf8
            }
        };
        kinds.push(kind);
        schema.push(ArrowField { name: pc.name.clone(), kind, nullable: pc.nullable });
    }

    let want = if parallel > 1 { parallel * 6 } else { 1 };
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
            // 56 MiB reserve (was 40): the raw COPY plane carries ~8 MB of
            // per-worker read buffers, and 250 MB peaks in a 256 MB cgroup
            // were two pages from the OOM killer. Headroom is a feature.
            Some(m) => ((m.saturating_sub(56 << 20) as usize) / (4 * (used + cap + 1)))
                .clamp(1 << 20, 32 << 20),
            None => 16 << 20,
        },
    };

    Ok(ReadPlan { plan, lane, kinds, schema, stmts, used, cap, batch_bytes })
}

/// The COPY-stream lane: one [`ArrowLoader`] per worker behind the source's
/// own `run_workers` (Postgres always; MySQL under `APITAP_MY_ARROW=0`).
fn launch_loaders<S: Source + 'static>(src: S, rp: ReadPlan, chunk: usize) -> ReadHandle {
    let ReadPlan { plan, lane, kinds, schema, stmts, used, cap, batch_bytes } = rp;
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

    ReadHandle::new(schema, rx, supervisor, completed)
}

/// The MySQL direct-Arrow lane: workers append decoded cells straight into
/// their builders and ship sealed batches — no Loader, no byte stream.
fn launch_my_arrow(src: MySqlSource, rp: ReadPlan) -> ReadHandle {
    let ReadPlan { plan, kinds, schema, stmts, used, cap, batch_bytes, .. } = rp;
    let (tx, rx) = mpsc::channel::<Result<ArrowBatch>>(cap);
    let completed = Arc::new(AtomicBool::new(false));

    let done = completed.clone();
    let supervisor = tokio::spawn(async move {
        if let Err(e) = src
            .run_arrow_read(&plan, stmts, kinds, batch_bytes, tx.clone(), used)
            .await
        {
            let _ = tx.send(Err(e)).await;
        }
        // Same ordering contract as the loader lane.
        done.store(true, Ordering::Release);
        drop(tx);
    });

    ReadHandle::new(schema, rx, supervisor, completed)
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        self.b.push(&buf)?;
        self.spare = Some(buf);
        if let Some(batch) = self.b.take_ready()? {
            self.tx
                .send(Ok(batch))
                .await
                .map_err(|_| Error::Transfer("read cancelled by consumer".into()))?;
        }
        Ok(())
    }

    fn framed_capable(&self) -> bool {
        true
    }

    async fn send_framed(
        &mut self,
        win: &[u8],
    ) -> Result<(usize, crate::wire::arrowcol::FramedPush)> {
        let r = self.b.push_framed(win)?;
        // One seal check per window fill — cheap at this cadence.
        if let Some(batch) = self.b.take_ready()? {
            self.tx
                .send(Ok(batch))
                .await
                .map_err(|_| Error::Transfer("read cancelled by consumer".into()))?;
        }
        Ok(r)
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
