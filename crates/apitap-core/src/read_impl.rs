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
use crate::source::{
    github::GithubSource, github_api::GithubApiSource, gsheets::GsheetsSource, mysql::MySqlSource,
    postgres::PgSource, Source,
};
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
    /// A non-database source (`github://` CSV, `github+api://`, `gsheets://`):
    /// one stream, no server to filter, columns as the source types them.
    Gh,
    GhApi,
    Gs,
}

fn read_dialect(src_url: &str) -> Result<ReadDialect> {
    let scheme = src_url.split("://").next().unwrap_or("");
    match scheme {
        "postgres" | "postgresql" => Ok(ReadDialect::Pg),
        "mysql" => Ok(ReadDialect::My),
        "github" => Ok(ReadDialect::Gh),
        "github+api" => Ok(ReadDialect::GhApi),
        "gsheets" => Ok(ReadDialect::Gs),
        _ => Err(Error::InvalidInput(format!(
            "read: unsupported source scheme '{scheme}' — postgres://, mysql://, \
             github://, github+api:// and gsheets:// today"
        ))),
    }
}

/// Schema-only probe: what [`start`] would emit, WITHOUT starting workers.
/// The lazy plugin registers this schema up front, then starts the real
/// read later with the query's projection pushed down.
pub(crate) async fn schema(src_url: &str, table: &str) -> Result<Vec<ArrowField>> {
    let dialect = read_dialect(src_url)?;
    match dialect {
        ReadDialect::Pg => schema_with(PgSource::connect(src_url, 1).await?, table, dialect).await,
        ReadDialect::My => {
            schema_with(MySqlSource::connect(src_url, 1).await?, table, dialect).await
        }
        ReadDialect::Gh => schema_with(GithubSource::connect(src_url).await?, table, dialect).await,
        ReadDialect::GhApi => {
            schema_with(GithubApiSource::connect(src_url).await?, table, dialect).await
        }
        ReadDialect::Gs => schema_with(GsheetsSource::connect(src_url).await?, table, dialect).await,
    }
}

/// The schema the read will deliver — decided by `column_kind`, the same
/// rule `start` applies. This probe used to take `arrow_kind` alone, so a
/// Postgres column the reader casts to text (time, an unconstrained
/// NUMERIC, …) was announced as its native type to the lazy frame.
async fn schema_with<S: Source>(
    src: S,
    table: &str,
    dialect: ReadDialect,
) -> Result<Vec<ArrowField>> {
    let plan = src.probe(table).await?;
    let lane = src.plan_lane(&plan, WireFormat::PgCopyBinary);
    Ok(lane
        .cols
        .iter()
        .zip(plan.cols.iter())
        .map(|(lc, pc)| {
            let kind = column_kind(dialect, lc, pc);
            ArrowField {
                name: pc.name.clone(),
                kind: kind.unwrap_or(ArrowKind::Utf8),
                nullable: pc.nullable,
                rewritten: read_as_text(kind, &pc.udt),
                padded: padded_text(kind, &pc.udt, pc.native_ddl.as_deref()),
                ci: ci_text(kind, pc.native_ddl.as_deref()),
            }
        })
        .collect())
}

/// The reader hands this column over as a string that is NOT the column's
/// own value: cast to text (`kind` None), or a Utf8 column whose source type
/// is not plain text (MySQL json arrives as UTF-8 bytes but compares as
/// JSON). A pushed-down predicate on it would be judged by the server on the
/// native value (MASTER M-46).
/// The collation a MySQL column's DDL names (`… COLLATE utf8mb4_0900_ai_ci`).
fn collation_of(native: Option<&str>) -> Option<String> {
    native?.rsplit_once(" COLLATE ").map(|(_, c)| c.trim().to_ascii_lowercase())
}

/// Text whose compares ignore trailing spaces ([`ArrowField::padded`]).
fn padded_text(kind: Option<ArrowKind>, udt: &str, native: Option<&str>) -> bool {
    if !matches!(kind, Some(ArrowKind::Utf8)) {
        return false;
    }
    matches!(udt.to_ascii_lowercase().as_str(), "bpchar" | "char")
        || collation_of(native).is_some_and(|c| !c.contains("0900") && !c.contains("nopad") && c != "binary")
}

/// Text under a case-insensitive collation ([`ArrowField::ci`]).
fn ci_text(kind: Option<ArrowKind>, native: Option<&str>) -> bool {
    matches!(kind, Some(ArrowKind::Utf8)) && collation_of(native).is_some_and(|c| c.ends_with("_ci"))
}

fn read_as_text(kind: Option<ArrowKind>, udt: &str) -> bool {
    match kind {
        None => true,
        Some(ArrowKind::Utf8) => !matches!(
            udt.to_ascii_lowercase().as_str(),
            "text"
                | "varchar"
                | "bpchar"
                | "name"
                | "char"
                | "tinytext"
                | "mediumtext"
                | "longtext"
        ),
        Some(_) => false,
    }
}

/// A column's Arrow kind, or `None` when the reader delivers it as text
/// instead (the caller rewrites the SELECT or the udt). Postgres ships raw
/// COPY BINARY, so only the udts the Arrow builder decodes from send format
/// keep their kind; everything else (time, interval, inet, arrays, domains,
/// enums, unconstrained NUMERIC, …) would put send-format bytes into a Utf8
/// column (MASTER review M-8 / M-45).
fn column_kind(
    dialect: ReadDialect,
    lc: &crate::plan::LaneCol,
    pc: &crate::plan::ColumnPlan,
) -> Option<ArrowKind> {
    let native = match dialect {
        // A NUMERIC past Decimal's 76 digits is delivered as Float64, which
        // its send form is not: a zero or a NaN (8 bytes) decoded as a tiny
        // float, anything else failed the read (R32). It reads as text.
        ReadDialect::Pg => {
            pg_send_decodable(&pc.udt, pc.precision)
                && (pc.udt != "numeric" || matches!(lc.delivered, Delivered::Decimal { .. }))
        }
        ReadDialect::My | ReadDialect::Gh | ReadDialect::GhApi | ReadDialect::Gs => true,
    };
    arrow_kind(&lc.delivered).filter(|_| native)
}

pub(crate) async fn start(src_url: &str, table: &str, opts: &ReadOptions) -> Result<ReadHandle> {
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
    // Read pipes hold no sink-side buffers, so the transfer model's
    // 10×chunk-per-pipe cap overshoots here; the swept read calibration
    // (pipeline::READ_RESERVE / READ_PIPE, receipts there) is the price. The
    // chunk still comes from the transfer model, as in 0.56.0 (bit-identity;
    // §8 lists the oddity).
    let shape = pipeline::knobs(&topts, &profile, crate::sink::PipeResidency::STREAMING)?;
    let (chunk, mut parallel) = (shape.chunk, shape.pipes);
    if let Some(m) = pipeline::mem_limit_bytes() {
        match opts.parallel {
            None => {
                parallel = pipeline::fit(
                    pipeline::READ_PIPE_CAP,
                    Some(chunk),
                    Some(m),
                    &pipeline::PipeCost::READ,
                )
                .pipes;
            }
            Some(p) => {
                let (fits, note) = clamp_explicit_read(p, chunk, m);
                if let Some(n) = note {
                    crate::progress::note(&n);
                }
                parallel = fits;
            }
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
        // One stream each: the API or the file serves a table front to back.
        ReadDialect::Gh => {
            let src = GithubSource::connect(src_url).await?;
            let rp = plan_read(&src, dialect, table, opts, 1).await?;
            Ok(launch_loaders(src, rp, chunk))
        }
        ReadDialect::GhApi => {
            let src = GithubApiSource::connect(src_url).await?;
            let rp = plan_read(&src, dialect, table, opts, 1).await?;
            Ok(launch_loaders(src, rp, chunk))
        }
        ReadDialect::Gs => {
            let src = GsheetsSource::connect(src_url).await?;
            let rp = plan_read(&src, dialect, table, opts, 1).await?;
            Ok(launch_loaders(src, rp, chunk))
        }
    }
}

/// `read(where=...)`: exactly one SQL expression in the source's dialect,
/// returned in the parser's canonical rendering. It is spliced as `AND (w)`
/// into every span statement, so text that closes that parenthesis
/// (`1=1) OR (1=1`), a second statement, or a trailing token would widen
/// every span to the whole table and repeat rows; all of it is refused here.
fn user_where(dialect: ReadDialect, w: &str) -> Result<String> {
    use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect};
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::Token;
    let (pg, my) = (PostgreSqlDialect {}, MySqlDialect {});
    let d: &dyn Dialect = match dialect {
        ReadDialect::Pg => &pg,
        ReadDialect::My => &my,
        ReadDialect::Gh | ReadDialect::GhApi | ReadDialect::Gs => {
            return Err(Error::InvalidInput(
                "read: where= needs a source with a SQL server (postgres://, mysql://); \
                 filter github/gsheets rows with .lazy() or apitap.sql()"
                    .into(),
            ))
        }
    };
    let bad = |why: String| {
        Error::InvalidInput(format!(
            "read: where= must be one SQL expression in the source's dialect ({why}): {w}"
        ))
    };
    let mut p = Parser::new(d).try_with_sql(w).map_err(|e| bad(e.to_string()))?;
    let expr = p.parse_expr().map_err(|e| bad(e.to_string()))?;
    let next = p.peek_token().token;
    if next != Token::EOF {
        return Err(bad(format!("unexpected `{next}` after the expression")));
    }
    Ok(expr.to_string())
}

/// The read pipes `mem` bytes hold, by the same swept price the automatic pick
/// uses: apitap.sql() fits its probe scan to what its build structure leaves.
pub(crate) fn read_pipes_for(mem: u64) -> usize {
    pipeline::fit(pipeline::READ_PIPE_CAP, Some(4 << 20), Some(mem), &pipeline::PipeCost::READ).pipes
}

/// An explicit `parallel=` past what the memory limit holds is clamped, with a
/// note. It was honoured as asked, and the OOM killer ended the process
/// instead: a 64 MiB cage read with `parallel=6` or `8` died on the spot,
/// while the planner's own pick for that cage (2 pipes) ran. The price is the
/// same swept read calibration the automatic pick uses.
fn clamp_explicit_read(asked: usize, chunk: usize, mem: u64) -> (usize, Option<String>) {
    let fits = pipeline::fit(asked, Some(chunk), Some(mem), &pipeline::PipeCost::READ).pipes;
    if fits >= asked {
        return (asked, None);
    }
    (
        fits,
        Some(format!(
            "read: parallel={asked} does not fit the {} MiB memory limit; reading with {fits} \
             pipe(s) (a larger limit, or APITAP_MEM_BUDGET in a shared container, allows more)",
            mem >> 20
        )),
    )
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
    gates: Vec<std::sync::Arc<crate::wire::arrowcol::KeyGate>>,
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
    let user = opts
        .where_sql
        .as_deref()
        .map(|w| user_where(dialect, w))
        .transpose()?;
    lane.push_where = match (user, opts.push_where.clone()) {
        (Some(u), Some(p)) => Some(format!("({u}) AND ({p})")),
        (u, p) => u.or(p),
    };

    // github:// and gsheets:// frame a row's cells by position, so a narrowed
    // plan would shift values into the wrong columns with no error.
    if opts.columns.is_some() && matches!(dialect, ReadDialect::Gh | ReadDialect::Gs) {
        return Err(Error::InvalidInput(
            "read: columns= is not supported for github:// and gsheets:// sources yet; \
             read every column"
                .into(),
        ));
    }
    if !opts.computed.is_empty() {
        if dialect != ReadDialect::Pg {
            return Err(Error::InvalidInput("read: computed columns are Postgres-only".into()));
        }
        for (name, sql) in &opts.computed {
            plan.cols.push(crate::plan::ColumnPlan {
                name: name.clone(),
                nullable: true,
                int_pk: false,
                native_ddl: None,
                udt: "int8".into(),
                precision: None,
                scale: None,
            });
            lane.cols.push(crate::plan::LaneCol {
                delivered: Delivered::Int { bytes: 8, unsigned: false },
                select: format!("({sql})::int8"),
            });
        }
    }
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
        plan.cursor_col = plan.cursor_plan().cloned();
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
        let native_kind = column_kind(dialect, lc, pc);
        let rewritten = read_as_text(native_kind, &pc.udt);
        let kind = match native_kind {
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
                    // json/uuid cells of these sources arrive in their binary
                    // send form, which a text column would carry corrupted.
                    ReadDialect::Gh | ReadDialect::GhApi | ReadDialect::Gs => {
                        return Err(Error::InvalidInput(format!(
                            "read: column {} ({}) of {table} cannot be read as Arrow yet; \
                             leave it out of columns=",
                            pc.name, pc.udt
                        )));
                    }
                }
                lc.delivered = Delivered::Text;
                ArrowKind::Utf8
            }
        };
        kinds.push(kind);
        schema.push(ArrowField {
            name: pc.name.clone(),
            kind,
            nullable: pc.nullable,
            rewritten,
            padded: padded_text(Some(kind), &pc.udt, pc.native_ddl.as_deref()),
            ci: ci_text(Some(kind), pc.native_ddl.as_deref()),
        });
    }

    let want = if parallel > 1 { parallel * 6 } else { 1 };
    let want = opts.max_spans.map_or(want, |m| want.min(m.max(1)));
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
        // `APITAP_BATCH_BYTES` pins the batch size, bypassing the residency
        // model — the A/B lever that separates "more workers" from "smaller
        // batches", which the derivation below otherwise moves TOGETHER (it
        // divides the budget by the worker count). Same escape-hatch pattern
        // as APITAP_RAW_COPY / APITAP_MY_ARROW.
        None => match std::env::var("APITAP_BATCH_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|b| *b > 0)
        {
            Some(b) => b,
            None => match mem {
                // 56 MiB reserve (was 40): the raw COPY plane carries ~8 MB of
                // per-worker read buffers, and 250 MB peaks in a 256 MB cgroup
                // were two pages from the OOM killer. Headroom is a feature.
                Some(m) => ((m.saturating_sub(56 << 20) as usize) / (4 * (used + cap + 1)))
                    .clamp(1 << 20, 32 << 20),
                None => 16 << 20,
            },
        },
    };

    Ok(ReadPlan {
        plan,
        lane,
        kinds,
        schema,
        stmts,
        used,
        cap,
        batch_bytes,
        gates: opts.gates.clone(),
    })
}

/// The COPY-stream lane: one [`ArrowLoader`] per worker behind the source's
/// own `run_workers` (Postgres always; MySQL under `APITAP_MY_ARROW=0`).
fn launch_loaders<S: Source + 'static>(src: S, rp: ReadPlan, chunk: usize) -> ReadHandle {
    let ReadPlan {
        plan,
        lane,
        kinds,
        schema,
        stmts,
        used,
        cap,
        batch_bytes,
        gates,
    } = rp;
    let (tx, rx) = mpsc::channel::<Result<ArrowBatch>>(cap);
    let completed = Arc::new(AtomicBool::new(false));
    let loaders: Vec<ArrowLoader> = (0..used)
        .map(|_| {
            let mut b = BatchBuilder::new(kinds.clone(), batch_bytes);
            b.set_gates(&gates);
            ArrowLoader { b, tx: tx.clone(), spare: None }
        })
        .collect();

    let done = completed.clone();
    let supervisor = tokio::spawn(async move {
        let pipes = crate::pipe::Pipes::for_read(loaders);
        if let Err(e) = src.run_workers(&plan, &lane, stmts, pipes, chunk).await {
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
    let ReadPlan {
        plan,
        kinds,
        schema,
        stmts,
        used,
        cap,
        batch_bytes,
        ..
    } = rp;
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

/// Postgres udts whose COPY BINARY (`send`) form the Arrow builder decodes
/// directly. NUMERIC qualifies only with a declared precision; the planner
/// maps unconstrained NUMERIC to Float64, which its send form is not.
fn pg_send_decodable(udt: &str, precision: Option<i32>) -> bool {
    match udt {
        "int2" | "int4" | "int8" | "float4" | "float8" | "bool" | "date" | "timestamp"
        | "timestamptz" | "bytea" | "varchar" | "bpchar" | "text" | "name" => true,
        "numeric" => precision.is_some_and(|p| p > 0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn collations_set_padded_and_ci() {
        use crate::wire::arrowcol::ArrowKind;
        let u = Some(ArrowKind::Utf8);
        let ddl = |c: &str| Some(format!("varchar(10) CHARACTER SET utf8mb4 COLLATE {c}"));
        assert!(!super::padded_text(u, "varchar", ddl("utf8mb4_0900_ai_ci").as_deref()), "0900 is NO PAD");
        assert!(super::ci_text(u, ddl("utf8mb4_0900_ai_ci").as_deref()));
        assert!(super::padded_text(u, "varchar", ddl("utf8mb4_general_ci").as_deref()), "general_ci is PAD SPACE");
        assert!(!super::ci_text(u, ddl("utf8mb4_bin").as_deref()), "_bin is case-sensitive");
        assert!(!super::padded_text(u, "varchar", ddl("utf8mb4_unicode_520_nopad_ci").as_deref()));
        assert!(super::padded_text(u, "bpchar", Some("character(10)")), "Postgres CHAR");
        assert!(!super::padded_text(u, "varchar", Some("character varying(10)")) && !super::ci_text(u, Some("character varying(10)")));
        assert!(!super::ci_text(Some(ArrowKind::Int64), ddl("utf8mb4_0900_ai_ci").as_deref()));
    }


    #[test]
    fn only_send_decodable_pg_udts_skip_the_text_cast() {
        for u in ["int4", "timestamptz", "text", "bytea", "bool"] {
            assert!(super::pg_send_decodable(u, None), "{u}");
        }
        assert!(super::pg_send_decodable("numeric", Some(12)));
        for u in [
            "numeric", "time", "timetz", "interval", "inet", "money", "_int4", "oid", "citext",
            "my_enum",
        ] {
            assert!(!super::pg_send_decodable(u, None), "{u}");
        }
    }
    // The read lane's pipe calibration moved to pipeline::tests (T7), where the
    // one `fit` every lane uses is tested.

    /// where=: one expression passes in canonical form; anything that could
    /// escape the `AND (w)` it is spliced into is refused.
    #[test]
    fn user_where_is_exactly_one_expression() {
        use super::{user_where, ReadDialect};
        assert_eq!(
            user_where(ReadDialect::Pg, "l_shipdate > DATE '1995-03-15'").unwrap(),
            "l_shipdate > DATE '1995-03-15'"
        );
        assert_eq!(
            user_where(ReadDialect::My, "o_orderdate < '1995-03-15' AND o_orderkey <= 30140291").unwrap(),
            "o_orderdate < '1995-03-15' AND o_orderkey <= 30140291"
        );
        assert!(user_where(ReadDialect::Pg, "a = 1 OR b = 2").is_ok());
        for bad in [
            "1=1) OR (1=1",
            "a = 1; DROP TABLE t",
            "a = 1 b",
            "",
            "a = 1 --",
            "(a = 1",
        ] {
            let e = user_where(ReadDialect::Pg, bad);
            assert!(e.is_err() || bad == "a = 1 --", "{bad:?} must be refused, got {e:?}");
        }
        // A trailing comment is no token: it parses, and the rendering drops it,
        // so nothing after the expression reaches the span statement.
        assert_eq!(user_where(ReadDialect::Pg, "a = 1 --").unwrap(), "a = 1");
    }

    /// An explicit parallel the limit holds runs as asked, silently; one past
    /// it runs at the planner's fit for that limit, with a note naming both.
    #[test]
    fn explicit_parallel_is_clamped_to_the_memory_limit() {
        const MB: u64 = 1 << 20;
        assert_eq!(super::clamp_explicit_read(2, 4 << 20, 256 * MB), (2, None));
        assert_eq!(super::clamp_explicit_read(8, 4 << 20, 256 * MB), (8, None));
        let (n, note) = super::clamp_explicit_read(8, 4 << 20, 64 * MB);
        assert_eq!(n, 2, "the swept 64 MiB fit");
        let note = note.expect("a clamp says so");
        assert!(note.contains("parallel=8") && note.contains("64 MiB") && note.contains("2 pipe"), "{note}");
        // Past the automatic cap (8) is not a clamp in itself: a big limit
        // holds 16 explicit pipes.
        assert_eq!(super::clamp_explicit_read(16, 4 << 20, 1024 * MB).0, 16);
    }
}
