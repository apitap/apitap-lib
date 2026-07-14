//! Postgres connector: [`PgSource`] (raw `COPY … TO STDOUT` byte streams) and
//! [`PgSink`] (`COPY … FROM STDIN (FORMAT binary)` with staging + atomic swap).
//!
//! The source never decodes rows. It produces three wire formats:
//!
//! - **PgCopyBinary** — the raw binary COPY stream relayed byte-for-byte (exactly what
//!   `psql src | psql dst` would move, minus the shell). Per-span framing (header /
//!   trailer) is stripped so many spans concatenate into ONE destination COPY, which
//!   lets the work-stealing span queue balance straggler tails.
//! - **RowBinary** — the binary COPY stream transcoded in-flight by
//!   [`crate::rowbinary::Transcoder`] (byte-swaps, epoch rebasing, exact NUMERIC
//!   scaling) — Postgres skips text formatting, ClickHouse skips text parsing.
//! - **TabSeparated** — text COPY relayed untouched (Postgres `text` COPY and
//!   ClickHouse TabSeparated share the tab delimiter, `\N` NULLs and `\t\n\r\\`
//!   escapes; corner case: a literal vertical tab escapes as `\v`, which ClickHouse
//!   doesn't unescape — checksum validation would catch it). This is the fallback for
//!   tables with a column the binary transcoder doesn't cover.

use crate::driver::{pop, spans, Loader, Source, WorkQueue};
use crate::error::{Error, Result};
use crate::plan::{ColumnPlan, Delivered, Delta, DestState, Lane, LaneCol, TablePlan, WireFormat};
use crate::rowbinary::{rb_type, RbType, Transcoder};
use crate::Mode;
use futures::TryStreamExt;
use sqlx::postgres::{PgPoolCopyExt, PgPoolOptions};
use sqlx::{PgPool, Row};

/// `foo` → `"foo"`, `fo"o` → `"fo""o"` — safe Postgres identifier quoting.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!(r#""{}""#, ident.replace('"', r#""""#))
}

/// `public.events` → `"public"."events"` (each path segment quoted).
pub(crate) fn quote_ident_path(path: &str) -> String {
    path.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

async fn connect_pool(url: &str, max: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // timestamptz then serializes with a "+00" offset in text mode, which
                // ClickHouse's best_effort parser reads — saves an AT TIME ZONE cast on
                // every row. Binary streams are timezone-independent, so this is safe
                // for every lane.
                sqlx::Executor::execute(conn, "SET timezone = 'UTC'").await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .map_err(|e| Error::Connect(e.to_string()))
}

// ---------------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------------

pub(crate) struct PgSource {
    pool: PgPool,
}

impl PgSource {
    pub(crate) async fn connect(url: &str, max_conns: usize) -> Result<Self> {
        Ok(Self {
            pool: connect_pool(url, max_conns as u32).await?,
        })
    }
}

impl Source for PgSource {
    async fn probe(&self, table: &str) -> Result<TablePlan> {
        // One catalog probe keyed on `$1::regclass`, so name resolution (search_path,
        // quoting) matches the COPY statements exactly — information_schema would
        // hard-default unqualified names to `public` and never lists materialized
        // views at all, even though COPY (SELECT …) reads them fine. `format_type`
        // gives the exact native spelling (`character varying(20)`, `numeric(18,4)`)
        // so a Postgres sink can mirror the schema byte-faithfully; NUMERIC
        // precision/scale unpack from atttypmod.
        let t = quote_ident_path(table);
        let rows = sqlx::query(
            "SELECT a.attname AS name, t.typname AS udt, \
                    format_type(a.atttypid, a.atttypmod) AS native, \
                    (NOT a.attnotnull) AS nullable, \
                    CASE WHEN t.typname = 'numeric' AND a.atttypmod >= 4 \
                         THEN (((a.atttypmod - 4) >> 16) & 65535) END AS p, \
                    CASE WHEN t.typname = 'numeric' AND a.atttypmod >= 4 \
                         THEN ((a.atttypmod - 4) & 65535) END AS s \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&t)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::InvalidInput(format!("probing {table}: {e}")))?;
        if rows.is_empty() {
            return Err(Error::InvalidInput(format!(
                "source table {table} not found"
            )));
        }

        // Single-column integer PK → the auto cursor.
        let pk = sqlx::query(
            "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS ty \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = $1::regclass AND i.indisprimary \
               AND array_position(i.indkey, a.attnum) < i.indnkeyatts \
             ORDER BY array_position(i.indkey, a.attnum)",
        )
        .bind(&t)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        let pk_cols: Vec<String> = pk.iter().map(|r| r.get::<String, _>("name")).collect();
        let int_pk_name: Option<String> = if pk.len() == 1 {
            let name: String = pk[0].get("name");
            let ty: String = pk[0].get("ty");
            matches!(ty.as_str(), "smallint" | "integer" | "bigint").then_some(name)
        } else {
            None
        };

        let cols = rows
            .iter()
            .map(|r| {
                let name: String = r.get("name");
                ColumnPlan {
                    int_pk: Some(&name) == int_pk_name.as_ref(),
                    native_ddl: Some(r.get::<String, _>("native")),
                    udt: r.get("udt"),
                    precision: r.get("p"),
                    scale: r.get("s"),
                    nullable: r.get("nullable"),
                    name,
                }
            })
            .collect();
        Ok(TablePlan {
            engine: "postgres",
            cols,
            cursor: None,
            pk_cols,
        })
    }

    fn can_produce(&self, plan: &TablePlan, format: WireFormat) -> bool {
        match format {
            // Raw passthrough and text relay carry ANY column type.
            WireFormat::PgCopyBinary | WireFormat::TabSeparated => true,
            // Binary transcoding needs every column covered (e.g. NUMERIC p>38 would
            // be Decimal256, which the transcoder doesn't emit → text fallback).
            WireFormat::RowBinary => plan
                .cols
                .iter()
                .all(|c| rb_type(&c.udt, c.precision, c.scale).is_some()),
        }
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        let cols = plan
            .cols
            .iter()
            .map(|c| {
                let pq = quote_ident(&c.name);
                match format {
                    // Passthrough: delivery is nominal (a same-engine sink mirrors
                    // `native_ddl` instead), select is the raw column.
                    WireFormat::PgCopyBinary => LaneCol {
                        delivered: delivered_of_udt(c),
                        select: pq,
                    },
                    WireFormat::RowBinary => LaneCol {
                        delivered: delivered_of_udt(c),
                        select: pq,
                    },
                    // Text casts anything whose CSV/TSV form the destination wouldn't
                    // parse natively; the rule is "let each database do what it's good
                    // at" — Postgres casts, this process never touches a row.
                    WireFormat::TabSeparated => match c.udt.as_str() {
                        // Postgres text booleans are t/f — cast to 0/1.
                        "bool" => LaneCol {
                            delivered: Delivered::Int {
                                bytes: 1,
                                unsigned: true,
                            },
                            select: format!("{pq}::int"),
                        },
                        "json" | "jsonb" => LaneCol {
                            delivered: Delivered::Json,
                            select: format!("{pq}::text"),
                        },
                        "int2" | "int4" | "int8" | "float4" | "float8" | "numeric" | "date"
                        | "timestamp" | "timestamptz" | "uuid" | "varchar" | "bpchar" | "text"
                        | "name" => LaneCol {
                            delivered: delivered_of_udt(c),
                            select: pq,
                        },
                        // Anything else rides as text — lossless, if not the tightest type.
                        _ => LaneCol {
                            delivered: Delivered::Text,
                            select: format!("{pq}::text"),
                        },
                    },
                }
            })
            .collect();
        Lane { format, cols }
    }

    async fn span_stmts(
        &self,
        table: &str,
        plan: &TablePlan,
        lane: &Lane,
        want: usize,
        delta: Option<&Delta>,
    ) -> Result<Vec<String>> {
        let src_t = quote_ident_path(table);
        let select_list = lane
            .cols
            .iter()
            .map(|c| c.select.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let copy_opts = match lane.format {
            WireFormat::TabSeparated => "FORMAT text",
            WireFormat::PgCopyBinary | WireFormat::RowBinary => "FORMAT binary",
        };
        // Incremental predicate — appended to EVERY statement in this fn, including
        // the min/max probe and the ctid fallback.
        let dpred = delta
            .map(|d| format!(" AND {} {} {}", quote_ident(&d.col), d.op, d.literal))
            .unwrap_or_default();

        // Span strategy, in measured order of preference: integer-cursor ranges (index
        // scan on a correlated PK beat TID ranges by ~4% at 16 pipes), then CTID page
        // ranges (TID Range Scan, PG 14+ — no index needed, so PK-LESS tables and
        // timestamp-cursor deltas still get full parallelism), then a single stream.
        let int_cursor = plan.cursor.as_deref().and_then(|c| {
            plan.cols
                .iter()
                .find(|pc| pc.name == c)
                .filter(|pc| matches!(pc.udt.as_str(), "int2" | "int4" | "int8"))
                .map(|_| c.to_string())
        });
        let mut stmts: Vec<String> = Vec::new();
        if want > 1 {
            if let Some(col) = &int_cursor {
                let qcol = quote_ident(col);
                let (lo, hi): (Option<i64>, Option<i64>) = sqlx::query_as(&format!(
                    "SELECT min({qcol})::int8, max({qcol})::int8 FROM {src_t} \
                     WHERE true{dpred}"
                ))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    Error::InvalidInput(format!(
                        "min/max of cursor {col} (must be a numeric column): {e}"
                    ))
                })?;
                if let (Some(lo), Some(hi)) = (lo, hi) {
                    for (rlo, rhi) in spans(lo, hi, want) {
                        stmts.push(format!(
                            "COPY (SELECT {select_list} FROM {src_t} \
                             WHERE {qcol} >= {rlo} AND {qcol} <= {rhi}{dpred}) \
                             TO STDOUT ({copy_opts})"
                        ));
                    }
                } else if delta.is_some() {
                    // Empty delta: no rows past the watermark. One statement that
                    // reads nothing keeps the pipeline uniform.
                    stmts.push(format!(
                        "COPY (SELECT {select_list} FROM {src_t} WHERE false) \
                         TO STDOUT ({copy_opts})"
                    ));
                }
            }
            if stmts.is_empty() {
                let (ver, npages): (i32, i64) = sqlx::query_as(
                    "SELECT current_setting('server_version_num')::int4, \
                            (pg_relation_size($1::regclass) / 8192)::int8",
                )
                .bind(&src_t)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::InvalidInput(format!("sizing {table}: {e}")))?;
                if ver >= 140_000 && npages > 0 {
                    for (plo, phi) in spans(0, npages - 1, want) {
                        stmts.push(format!(
                            "COPY (SELECT {select_list} FROM {src_t} \
                             WHERE ctid >= '({plo},0)'::tid AND ctid < '({},0)'::tid{dpred}) \
                             TO STDOUT ({copy_opts})",
                            phi + 1
                        ));
                    }
                }
            }
        }
        if stmts.is_empty() {
            stmts.push(format!(
                "COPY (SELECT {select_list} FROM {src_t} WHERE true{dpred}) \
                 TO STDOUT ({copy_opts})"
            ));
        }
        Ok(stmts)
    }

    async fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> Result<u64> {
        let queue = crate::driver::work_queue(stmts);
        let mut tasks = Vec::with_capacity(loaders.len());
        for loader in loaders {
            let mode = match lane.format {
                WireFormat::PgCopyBinary => PgReadMode::FrameStrip,
                WireFormat::RowBinary => PgReadMode::Transcode(
                    plan.cols
                        .iter()
                        .map(|c| {
                            // can_produce(RowBinary) already proved coverage.
                            (rb_type(&c.udt, c.precision, c.scale).unwrap(), c.nullable)
                        })
                        .collect(),
                ),
                WireFormat::TabSeparated => PgReadMode::Text,
            };
            tasks.push(tokio::spawn(copy_out_worker(
                self.pool.clone(),
                queue.clone(),
                mode,
                loader,
                chunk,
            )));
        }
        let mut rows = 0u64;
        for t in tasks {
            rows += t
                .await
                .map_err(|e| Error::Transfer(format!("join: {e}")))??;
        }
        Ok(rows)
    }
}

/// Nominal delivery for a Postgres type (what a NON-Postgres sink would declare when
/// the value arrives through a binary lane).
fn delivered_of_udt(c: &ColumnPlan) -> Delivered {
    match c.udt.as_str() {
        "int2" => Delivered::Int {
            bytes: 2,
            unsigned: false,
        },
        "int4" => Delivered::Int {
            bytes: 4,
            unsigned: false,
        },
        "int8" => Delivered::Int {
            bytes: 8,
            unsigned: false,
        },
        "float4" => Delivered::Float32,
        "float8" => Delivered::Float64,
        // NUMERIC with a declared precision is exact; unconstrained NUMERIC has no
        // scale to declare, so it rides as Float64 (documented lossy). p ≤ 76 is what
        // a Decimal256-capable text parser accepts; the binary lane's own p ≤ 38 gate
        // lives in `rb_type`.
        "numeric" => match (c.precision, c.scale) {
            (Some(p), Some(s)) if p <= 76 => Delivered::Decimal {
                p: p as u16,
                s: s.max(0) as u16,
            },
            _ => Delivered::Float64,
        },
        "bool" => Delivered::Bool,
        "date" => Delivered::Date,
        "timestamp" => Delivered::DateTime { utc: false },
        "timestamptz" => Delivered::DateTime { utc: true },
        "uuid" => Delivered::Uuid,
        "json" | "jsonb" => Delivered::Json,
        "varchar" | "bpchar" | "text" | "name" => Delivered::Text,
        _ => Delivered::Text,
    }
}

// ---------------------------------------------------------------------------------
// Byte-stream worker
// ---------------------------------------------------------------------------------

enum PgReadMode {
    /// Raw binary passthrough with per-span framing stripped (see [`SpanStrip`]).
    FrameStrip,
    /// Binary → RowBinary, fresh [`Transcoder`] per span (it consumes the framing).
    Transcode(Vec<(RbType, bool)>),
    /// Text relay — rows of one text format concatenate cleanly.
    Text,
}

/// One worker: keeps ONE sink stream open and feeds it the byte streams of successive
/// `COPY` statements pulled from the shared work queue. Output is coalesced to
/// ~`chunk` bytes — sqlx yields one piece per COPY message (≈ one row), and per-row
/// sends are pure protocol overhead.
async fn copy_out_worker<L: Loader>(
    pool: PgPool,
    queue: WorkQueue,
    mode: PgReadMode,
    mut loader: L,
    chunk: usize,
) -> Result<u64> {
    let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
    if matches!(mode, PgReadMode::FrameStrip) {
        // One synthetic stream header for the whole worker; each span's own header and
        // trailer are stripped so the spans concatenate into one valid COPY stream.
        crate::pgcopy::header(&mut out);
    }
    while let Some(sql) = pop(&queue) {
        let mut stream = match pool.copy_out_raw(&sql).await {
            Ok(s) => s,
            Err(e) => {
                return Err(loader
                    .abort(Error::Transfer(format!("COPY OUT: {e}")))
                    .await)
            }
        };
        let mut transcoder = match &mode {
            PgReadMode::Transcode(cols) => Some(Transcoder::new(cols.clone())),
            _ => None,
        };
        let mut strip = match &mode {
            PgReadMode::FrameStrip => Some(SpanStrip::new()),
            _ => None,
        };
        loop {
            let piece = match stream.try_next().await {
                Ok(Some(b)) => b,
                Ok(None) => break,
                Err(e) => return Err(loader.abort(Error::Transfer(format!("pg read: {e}"))).await),
            };
            let step = match (&mut transcoder, &mut strip) {
                (Some(t), _) => t.push(&piece, &mut out),
                (_, Some(s)) => s.push(&piece, &mut out),
                _ => {
                    out.extend_from_slice(&piece);
                    Ok(())
                }
            };
            if let Err(e) = step {
                return Err(loader.abort(e).await);
            }
            // mem::replace (not take): take leaves capacity 0 and the next chunk pays
            // ~1 extra full copy in geometric regrowth.
            if out.len() >= chunk {
                let full = std::mem::replace(&mut out, Vec::with_capacity(chunk + 64 * 1024));
                loader.send(full).await?;
            }
        }
        let complete = match (&transcoder, &strip) {
            (Some(t), _) => t.finished(),
            (_, Some(s)) => s.finished(),
            _ => true,
        };
        if !complete {
            return Err(loader
                .abort(Error::Transfer("pg binary COPY ended mid-stream".into()))
                .await);
        }
    }
    if matches!(mode, PgReadMode::FrameStrip) {
        crate::pgcopy::trailer(&mut out);
    }
    if !out.is_empty() {
        loader.send(out).await?;
    }
    loader.finish().await
}

/// Per-span framing stripper for the raw binary passthrough: skips the 19-byte header
/// (+ extension area) at stream start and withholds the last 2 bytes so the trailer
/// never reaches the destination mid-stream (the worker emits one synthetic header up
/// front and one trailer at the very end). The withheld tail doubles as the "did the
/// stream end cleanly" check.
struct SpanStrip {
    hdr: [u8; 19],
    hdr_len: usize,
    skip: usize,
    pending: [u8; 2],
    npending: usize,
}

impl SpanStrip {
    fn new() -> Self {
        Self {
            hdr: [0; 19],
            hdr_len: 0,
            skip: 0,
            pending: [0; 2],
            npending: 0,
        }
    }

    fn push(&mut self, mut b: &[u8], out: &mut Vec<u8>) -> Result<()> {
        if self.hdr_len < 19 {
            let take = (19 - self.hdr_len).min(b.len());
            self.hdr[self.hdr_len..self.hdr_len + take].copy_from_slice(&b[..take]);
            self.hdr_len += take;
            b = &b[take..];
            if self.hdr_len < 19 {
                return Ok(());
            }
            if &self.hdr[..11] != b"PGCOPY\n\xff\r\n\0" {
                return Err(Error::Transfer("pg binary COPY: bad header".into()));
            }
            self.skip = u32::from_be_bytes(self.hdr[15..19].try_into().unwrap()) as usize;
        }
        if self.skip > 0 {
            let take = self.skip.min(b.len());
            self.skip -= take;
            b = &b[take..];
        }
        // Body relay with a 2-byte holdback (the eventual trailer).
        match b.len() {
            0 => {}
            1 => {
                if self.npending == 2 {
                    out.push(self.pending[0]);
                    self.pending[0] = self.pending[1];
                    self.pending[1] = b[0];
                } else {
                    self.pending[self.npending] = b[0];
                    self.npending += 1;
                }
            }
            n => {
                out.extend_from_slice(&self.pending[..self.npending]);
                out.extend_from_slice(&b[..n - 2]);
                self.pending.copy_from_slice(&b[n - 2..]);
                self.npending = 2;
            }
        }
        Ok(())
    }

    /// Did the span end exactly on the 2-byte trailer?
    fn finished(&self) -> bool {
        self.hdr_len == 19 && self.skip == 0 && self.npending == 2 && self.pending == [0xFF, 0xFF]
    }
}

// ---------------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------------

pub(crate) struct PgSink {
    pool: PgPool,
    /// Quoted destination / staging idents; staging lives in the destination's schema.
    final_t: String,
    staging_t: String,
    bare: String,
    /// Unquoted schema name, for catalog lookups (`public` when unqualified).
    schema: String,
    /// DDL captured from the pre-swap table (indexes, constraints, grants) — the swap
    /// destroys them, so replace mode re-applies these after the commit.
    restore_ddl: Vec<String>,
    /// Destination primary-key columns (merge mode), introspected in `dest_state`.
    merge_keys: Vec<String>,
    /// Set when a merge run has to bootstrap (dest missing): the swap then also
    /// recreates the SOURCE's primary key so the next merge run can upsert.
    bootstrap_pk: bool,
    /// Cursor column (incremental modes), for deterministic merge dedup ordering.
    cursor_col: Option<String>,
    /// Credential-free source identity — half of the state row's key.
    source_id: Option<String>,
    /// Cursor column's source type — picks the dialect-neutral watermark rendering.
    wm_udt: Option<String>,
    /// Canonical unquoted `schema.table` — the state row's other key half.
    dest_key: String,
    /// Was dest_table schema-qualified by the caller? Unqualified names get their
    /// schema resolved from the live connection in dest_state.
    qualified: bool,
    /// Plan column names in order, stashed at `prepare` for the merge upsert.
    col_names: Vec<String>,
    copy_in_sql: String,
    /// Double-buffer sends through a small channel so the source keeps being read
    /// WHILE the previous buffer is in flight (−11% on the byte-relay route). Row
    /// sources set this false: their encode is already CPU-heavy and the overlap was
    /// MEASURED to hurt (+2-3 s at 10M) — see benchmarks/README.md.
    overlap_send: bool,
}

impl PgSink {
    pub(crate) async fn connect(
        url: &str,
        dest_table: &str,
        max_conns: usize,
        overlap_send: bool,
    ) -> Result<Self> {
        let qualified = dest_table.contains('.');
        let (schema_pfx, bare) = match dest_table.rsplit_once('.') {
            Some((s, t)) => (format!("{s}."), t.to_string()),
            None => (String::new(), dest_table.to_string()),
        };
        let staging_t = quote_ident_path(&format!("{schema_pfx}{bare}__apitap_staging"));
        let schema = schema_pfx.trim_end_matches('.').to_string();
        let schema = if schema.is_empty() {
            "public".into()
        } else {
            schema
        };
        Ok(Self {
            pool: PgPoolOptions::new()
                .max_connections(max_conns as u32)
                .connect(url)
                .await
                .map_err(|e| Error::Connect(e.to_string()))?,
            final_t: quote_ident_path(dest_table),
            copy_in_sql: format!("COPY {staging_t} FROM STDIN (FORMAT binary)"),
            staging_t,
            dest_key: format!("{schema}.{bare}"),
            bare,
            schema,
            qualified,
            restore_ddl: Vec::new(),
            merge_keys: Vec::new(),
            bootstrap_pk: false,
            cursor_col: None,
            source_id: None,
            wm_udt: None,
            col_names: Vec::new(),
            overlap_send,
        })
    }
}

impl PgSink {
    async fn final_exists(&self) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(&self.final_t)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("dest lookup: {e}")))
    }

    /// Destination column names in ordinal order.
    async fn final_columns(&self) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            "SELECT a.attname FROM pg_attribute a \
             WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&self.final_t)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Transfer(format!("dest columns: {e}")))
    }
}

impl PgSink {
    fn state_table(&self) -> String {
        format!("{}.\"_apitap_state\"", quote_ident(&self.schema))
    }

    /// Dialect-neutral watermark SELECT for a cursor of type `udt` against `table`.
    fn wm_expr(udt: &str, qcur: &str, table: &str) -> String {
        match udt {
            "timestamptz" => format!(
                "SELECT to_char(max({qcur}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM {table}"
            ),
            "timestamp" | "datetime" => format!(
                "SELECT to_char(max({qcur}), 'YYYY-MM-DD HH24:MI:SS.US') FROM {table}"
            ),
            _ => format!("SELECT max({qcur})::text FROM {table}"),
        }
    }

    async fn ensure_state_table(&self) -> Result<()> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                dest_table  text NOT NULL, \
                source_id   text NOT NULL, \
                cursor_col  text NOT NULL, \
                watermark   text NOT NULL, \
                mode        text NOT NULL, \
                last_rows   bigint NOT NULL, \
                synced_at   timestamptz NOT NULL DEFAULT now(), \
                PRIMARY KEY (dest_table, source_id))",
            self.state_table()
        ))
        .execute(&self.pool)
        .await
        .map(|_| ())
        .or_else(|e| {
            // Two concurrent first-runs can both pass IF NOT EXISTS — the loser's
            // error is harmless, the table exists either way.
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("duplicate key") {
                Ok(())
            } else {
                Err(Error::Transfer(format!("state table: {e}")))
            }
        })
    }

    /// Upsert the state row inside `tx` — same transaction as the data landing, so
    /// state and data can never drift apart.
    async fn write_state(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        watermark: &str,
        mode: Mode,
        rows: u64,
    ) -> Result<()> {
        let (Some(cursor), Some(source_id)) = (&self.cursor_col, &self.source_id) else {
            return Ok(()); // plain replace without incremental context: no state
        };
        sqlx::query(&format!(
            "INSERT INTO {} \
             (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ($1, $2, $3, $4, $5, $6, now()) \
             ON CONFLICT (dest_table, source_id) DO UPDATE SET \
               cursor_col = EXCLUDED.cursor_col, watermark = EXCLUDED.watermark, \
               mode = EXCLUDED.mode, last_rows = EXCLUDED.last_rows, synced_at = now()",
            self.state_table()
        ))
        .bind(&self.dest_key)
        .bind(source_id)
        .bind(cursor)
        .bind(watermark)
        .bind(match mode {
            Mode::Replace => "replace",
            Mode::Append => "append",
            Mode::Merge => "merge",
        })
        .bind(rows as i64)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Transfer(format!("state write: {e}")))?;
        Ok(())
    }

    /// Watermark of what a table currently holds, rendered dialect-neutrally.
    async fn table_watermark(&self, table: &str) -> Result<Option<String>> {
        let (Some(cursor), Some(udt)) = (&self.cursor_col, &self.wm_udt) else {
            return Ok(None);
        };
        sqlx::query_scalar(&Self::wm_expr(udt, &quote_ident(cursor), table))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("watermark: {e}")))
    }
}

/// Postgres column type for a delivered value from a non-Postgres source.
fn pg_type_of(d: &Delivered) -> String {
    match d {
        // Unsigned deliveries widen (Postgres has no unsigned ints) so the value
        // always FITS — a u16 crammed into smallint would reinterpret silently.
        Delivered::Int {
            bytes: 8,
            unsigned: true,
        } => "numeric(20,0)".into(),
        Delivered::Int {
            bytes: 4,
            unsigned: true,
        } => "bigint".into(),
        Delivered::Int {
            bytes: 2,
            unsigned: true,
        } => "integer".into(),
        Delivered::Int { bytes: 1 | 2, .. } => "smallint".into(),
        Delivered::Int { bytes: 4, .. } => "integer".into(),
        Delivered::Int { .. } => "bigint".into(),
        Delivered::Float32 => "real".into(),
        Delivered::Float64 => "double precision".into(),
        Delivered::Decimal { p: 0, .. } => "numeric".into(),
        Delivered::Decimal { p, s } => format!("numeric({p},{s})"),
        Delivered::Bool => "boolean".into(),
        Delivered::Date => "date".into(),
        Delivered::DateTime { utc: false } => "timestamp(6)".into(),
        Delivered::DateTime { utc: true } => "timestamptz(6)".into(),
        Delivered::Uuid => "uuid".into(),
        Delivered::Json => "jsonb".into(),
        Delivered::Text => "text".into(),
        Delivered::Bytes => "bytea".into(),
    }
}

impl crate::driver::Sink for PgSink {
    type Loader = PgCopyLoader;

    fn accepts(&self) -> &'static [WireFormat] {
        &[WireFormat::PgCopyBinary]
    }

    fn adjust_plan(&self, _plan: &mut TablePlan) {}

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        durable: bool,
        mode: Mode,
    ) -> Result<()> {
        self.col_names = plan.cols.iter().map(|c| c.name.clone()).collect();

        // Replace destroys the old table's indexes, constraints and grants with it —
        // capture them now so finalize can re-apply after the swap. (Column DEFAULTs
        // and identity/serial ownership are NOT preserved; documented.)
        if mode == Mode::Replace && self.final_exists().await? {
            // Resolve the table's ACTUAL namespace — an unqualified dest name follows
            // search_path everywhere else; hardcoding 'public' here would silently
            // skip the index/grant capture for such tables.
            let schema: String = sqlx::query_scalar(
                "SELECT n.nspname FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = to_regclass($1)",
            )
            .bind(&self.final_t)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?;
            let mut ddl: Vec<String> = Vec::new();
            let cons: Vec<(String, String)> = sqlx::query_as(
                "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conrelid = $1::regclass AND contype IN ('p','u','f','c','x') \
                 ORDER BY contype = 'f', conname",
            )
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture constraints: {e}")))?;
            for (name, def) in &cons {
                ddl.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    self.final_t,
                    quote_ident(name),
                    def
                ));
            }
            // Plain indexes (constraint-backed ones come back via ADD CONSTRAINT).
            let idx: Vec<String> = sqlx::query_scalar(
                "SELECT indexdef FROM pg_indexes \
                 WHERE schemaname = $1 AND tablename = $2 AND indexname NOT IN \
                 (SELECT conname FROM pg_constraint WHERE conrelid = $3::regclass)",
            )
            .bind(&schema)
            .bind(&self.bare)
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture indexes: {e}")))?;
            ddl.extend(idx); // indexdef names the same table the swap re-creates
            let grants: Vec<(String, String)> = sqlx::query_as(
                "SELECT grantee, string_agg(DISTINCT privilege_type, ', ') \
                 FROM information_schema.role_table_grants \
                 WHERE table_schema = $1 AND table_name = $2 AND grantee <> current_user \
                 GROUP BY grantee",
            )
            .bind(&schema)
            .bind(&self.bare)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("capture grants: {e}")))?;
            for (grantee, privs) in &grants {
                let who = if grantee == "PUBLIC" {
                    "PUBLIC".to_string()
                } else {
                    quote_ident(grantee)
                };
                ddl.push(format!("GRANT {privs} ON {} TO {who}", self.final_t));
            }
            self.restore_ddl = ddl;
        }
        if self.bootstrap_pk {
            let keys = plan
                .pk_cols
                .iter()
                .map(|k| quote_ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            self.restore_ddl.push(format!(
                "ALTER TABLE {} ADD PRIMARY KEY ({keys})",
                self.final_t
            ));
        }

        // Binary COPY is positional + type-strict. Same-engine transfers mirror the
        // source's exact type spellings; anything else maps the lane's deliveries.
        let cols_ddl = plan
            .cols
            .iter()
            .zip(lane.cols.iter())
            .map(|(c, lc)| {
                let ty = match (&c.native_ddl, plan.engine) {
                    (Some(native), "postgres") => native.clone(),
                    _ => pg_type_of(&lc.delivered),
                };
                format!("{} {}", quote_ident(&c.name), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let st = &self.staging_t;
        sqlx::query(&format!("DROP TABLE IF EXISTS {st}"))
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("drop staging: {e}")))?;
        // Incremental staging never becomes the final table — always skip its WAL.
        let unlogged = if durable && mode == Mode::Replace {
            ""
        } else {
            "UNLOGGED "
        };
        sqlx::query(&format!("CREATE {unlogged}TABLE {st} ({cols_ddl})"))
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("create staging: {e}")))?;
        Ok(())
    }

    async fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> Result<DestState> {
        self.cursor_col = Some(cursor.to_string());
        self.source_id = Some(source_id.to_string());
        self.wm_udt = plan
            .cols
            .iter()
            .find(|c| c.name == cursor)
            .map(|c| c.udt.clone());
        let exists = self.final_exists().await?;
        // An unqualified dest name follows search_path — resolve the REAL schema so
        // the state table and its keys land next to the actual data.
        if !self.qualified {
            let resolved: String = if exists {
                sqlx::query_scalar(
                    "SELECT n.nspname FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = to_regclass($1)",
                )
                .bind(&self.final_t)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?
            } else {
                sqlx::query_scalar("SELECT current_schema()")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Error::Transfer(format!("resolve schema: {e}")))?
            };
            self.dest_key = format!("{resolved}.{}", self.bare);
            self.schema = resolved;
        }
        self.ensure_state_table().await?;
        if !exists {
            if mode == Mode::Merge {
                if plan.pk_cols.is_empty() {
                    return Err(Error::InvalidInput(
                        "merge bootstrap needs a primary key on the SOURCE table \
                         (the destination is created with the same key)"
                            .into(),
                    ));
                }
                self.bootstrap_pk = true; // prepare() adds ADD PRIMARY KEY post-swap
            }
            return Ok(DestState {
                exists: false,
                watermark: None,
            });
        }
        // Schema drift is an error, never a silent mis-append: binary COPY is
        // positional, so the column lists must match exactly.
        let dest_cols = self.final_columns().await?;
        let src_cols: Vec<String> = plan.cols.iter().map(|c| c.name.clone()).collect();
        if dest_cols != src_cols {
            return Err(Error::InvalidInput(format!(
                "destination columns {dest_cols:?} don't match the source {src_cols:?} — \
                 run once with mode='replace' to realign the schema"
            )));
        }
        if mode == Mode::Merge {
            self.merge_keys = sqlx::query_scalar(
                "SELECT a.attname FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = $1::regclass AND i.indisprimary \
                   AND array_position(i.indkey, a.attnum) < i.indnkeyatts \
                 ORDER BY array_position(i.indkey, a.attnum)",
            )
            .bind(&self.final_t)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Transfer(format!("dest pk: {e}")))?;
            if self.merge_keys.is_empty() {
                return Err(Error::InvalidInput(
                    "merge needs a PRIMARY KEY on the destination table".into(),
                ));
            }
        }
        // An EMPTY table cannot carry a watermark, whatever the state row says —
        // TRUNCATE-to-resync must work.
        let has_rows: bool =
            sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {})", self.final_t))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("dest emptiness: {e}")))?;
        if !has_rows {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        // The state row is authoritative: it survives destination-side writes,
        // precision differences, and enables per-source watermarks (fan-in). Only a
        // missing row (pre-state-table destinations, or a fresh dest built by plain
        // replace) falls back to deriving the watermark from the data itself.
        let from_state: Option<String> = sqlx::query_scalar(&format!(
            "SELECT watermark FROM {} WHERE dest_table = $1 AND source_id = $2",
            self.state_table()
        ))
        .bind(&self.dest_key)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Transfer(format!("state read: {e}")))?;
        let watermark = match from_state {
            Some(wm) => Some(wm),
            None => {
                // Fan-in guard: if OTHER sources hold state rows here, the global
                // data-max belongs to the most advanced of them — falling back would
                // silently skip THIS source's backlog. Fail loudly instead.
                let siblings: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM {} WHERE dest_table = $1",
                    self.state_table()
                ))
                .bind(&self.dest_key)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Transfer(format!("state read: {e}")))?;
                if siblings > 0 {
                    return Err(Error::InvalidInput(format!(
                        "destination {} has state rows from other sources but none for \
                         '{source_id}' — a data-derived watermark would be wrong here. \
                         Run mode='replace' to rebuild, or seed a state row manually",
                        self.dest_key
                    )));
                }
                self.table_watermark(&self.final_t).await?
            }
        };
        Ok(DestState {
            exists: true,
            watermark,
        })
    }

    async fn loader(&self) -> Result<PgCopyLoader> {
        PgCopyLoader::open(
            self.pool.clone(),
            self.copy_in_sql.clone(),
            self.overlap_send,
        )
        .await
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        // COPY … FROM STDIN reports rows itself — no count query needed.
        Ok(loaded)
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        // 0-row guard, every mode: an empty load never touches the destination.
        if rows == 0 {
            let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
                .execute(&self.pool)
                .await;
            return Ok(());
        }
        // Watermark of what THIS run staged — computed before staging is consumed,
        // recorded in the state row alongside the data it describes.
        let staged_wm = self.table_watermark(&self.staging_t).await?;
        match mode {
            Mode::Replace => {
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("swap begin: {e}")))?;
                sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.final_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop dest: {e}")))?;
                sqlx::query(&format!(
                    "ALTER TABLE {} RENAME TO {}",
                    self.staging_t,
                    quote_ident(&self.bare)
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("rename staging: {e}")))?;
                // Replace destroyed every source's rows — stale state rows would
                // make the next incremental run skip data. Clear them all; the
                // bootstrap (if any) re-inserts its own row below.
                let has_state: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                    .bind(self.state_table())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("state lookup: {e}")))?;
                if has_state {
                    sqlx::query(&format!(
                        "DELETE FROM {} WHERE dest_table = $1",
                        self.state_table()
                    ))
                    .bind(&self.dest_key)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("state clear: {e}")))?;
                }
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("swap commit: {e}")))?;
                // Re-apply what the swap destroyed. Runs AFTER the commit so index
                // builds don't extend the exclusive-lock window; the data is already
                // live. A failure here is reported with the exact statement — the
                // rows are correct, only the post-DDL needs attention.
                for ddl in &self.restore_ddl {
                    sqlx::query(ddl).execute(&self.pool).await.map_err(|e| {
                        Error::Transfer(format!(
                            "rows landed, but restoring destination DDL failed: `{ddl}`: {e}"
                        ))
                    })?;
                }
                Ok(())
            }
            Mode::Append => {
                // Stage → one INSERT..SELECT in a transaction: readers see the whole
                // delta or none of it.
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("append begin: {e}")))?;
                sqlx::query(&format!(
                    "INSERT INTO {} SELECT * FROM {}",
                    self.final_t, self.staging_t
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("append insert: {e}")))?;
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                sqlx::query(&format!("DROP TABLE {}", self.staging_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop staging: {e}")))?;
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("append commit: {e}")))
            }
            Mode::Merge => {
                let keys = self
                    .merge_keys
                    .iter()
                    .map(|k| quote_ident(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let cols = self
                    .col_names
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let updates = self
                    .col_names
                    .iter()
                    .filter(|c| !self.merge_keys.contains(c))
                    .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let action = if updates.is_empty() {
                    "DO NOTHING".to_string()
                } else {
                    format!("DO UPDATE SET {updates}")
                };
                // Parallel spans share no snapshot: a row UPDATEd mid-run can land
                // in staging twice (old and new version) and ON CONFLICT refuses to
                // touch the same row twice. Dedupe deterministically — keep the
                // highest cursor value per key.
                let order = match &self.cursor_col {
                    Some(c) => format!("{keys}, {} DESC", quote_ident(c)),
                    None => keys.clone(),
                };
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| Error::Transfer(format!("merge begin: {e}")))?;
                sqlx::query(&format!(
                    "INSERT INTO {} ({cols}) \
                     SELECT DISTINCT ON ({keys}) {cols} FROM {} ORDER BY {order} \
                     ON CONFLICT ({keys}) {action}",
                    self.final_t, self.staging_t
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Transfer(format!("merge upsert: {e}")))?;
                if let Some(wm) = &staged_wm {
                    self.write_state(&mut tx, wm, mode, rows).await?;
                }
                sqlx::query(&format!("DROP TABLE {}", self.staging_t))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Transfer(format!("drop staging: {e}")))?;
                tx.commit()
                    .await
                    .map_err(|e| Error::Transfer(format!("merge commit: {e}")))
            }
        }
    }
}

type CopyIn = sqlx::postgres::PgCopyIn<sqlx::pool::PoolConnection<sqlx::Postgres>>;

/// One `COPY … FROM STDIN` stream. Serial mode owns the copier directly; overlap mode
/// puts a 2-slot channel and a sender task in front of it.
pub(crate) enum PgCopyLoader {
    Serial(CopyIn),
    Overlap {
        /// `Err(())` is the abort marker: the sender task then ABORTS the COPY so
        /// Postgres discards the partial stream (a clean close would commit it into
        /// staging — wasted WAL for data the swap will never use).
        tx: futures::channel::mpsc::Sender<std::result::Result<Vec<u8>, ()>>,
        join: tokio::task::JoinHandle<Result<u64>>,
    },
}

impl PgCopyLoader {
    async fn open(pool: PgPool, copy_in_sql: String, overlap: bool) -> Result<Self> {
        if !overlap {
            let copier = pool
                .copy_in_raw(&copy_in_sql)
                .await
                .map_err(|e| Error::Transfer(format!("COPY IN: {e}")))?;
            return Ok(Self::Serial(copier));
        }
        use futures::StreamExt;
        let (tx, mut rx) = futures::channel::mpsc::channel::<std::result::Result<Vec<u8>, ()>>(2);
        let join = tokio::spawn(async move {
            let mut copier = pool
                .copy_in_raw(&copy_in_sql)
                .await
                .map_err(|e| Error::Transfer(format!("COPY IN: {e}")))?;
            while let Some(item) = rx.next().await {
                match item {
                    Ok(buf) => copier
                        .send(buf)
                        .await
                        .map(|_| ())
                        .map_err(|e| Error::Transfer(format!("pg send: {e}")))?,
                    Err(()) => {
                        let _ = copier.abort("apitap: source failed").await;
                        return Err(Error::Transfer("copy aborted".into()));
                    }
                }
            }
            copier
                .finish()
                .await
                .map_err(|e| Error::Transfer(format!("pg finish: {e}")))
        });
        Ok(Self::Overlap { tx, join })
    }
}

impl Loader for PgCopyLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        match self {
            Self::Serial(copier) => copier
                .send(buf)
                .await
                .map(|_| ())
                .map_err(|e| Error::Transfer(format!("pg send: {e}"))),
            Self::Overlap { tx, join } => {
                use futures::SinkExt;
                if tx.send(Ok(buf)).await.is_err() {
                    // Sender died — surface ITS error, not the closed channel.
                    return Err(match join.await {
                        Ok(Ok(_)) => Error::Transfer("pg copy closed early".into()),
                        Ok(Err(e)) => e,
                        Err(e) => Error::Transfer(format!("join: {e}")),
                    });
                }
                Ok(())
            }
        }
    }

    async fn finish(self) -> Result<u64> {
        match self {
            Self::Serial(copier) => copier
                .finish()
                .await
                .map_err(|e| Error::Transfer(format!("pg finish: {e}"))),
            Self::Overlap { tx, join } => {
                drop(tx); // close the channel; the sender task then finishes the COPY
                match join.await {
                    Ok(r) => r,
                    Err(e) => Err(Error::Transfer(format!("join: {e}"))),
                }
            }
        }
    }

    async fn abort(self, cause: Error) -> Error {
        match self {
            // Dropping the copier aborts the COPY server-side; rows never persist.
            Self::Serial(copier) => {
                let _ = copier.abort("apitap: source failed").await;
            }
            Self::Overlap { mut tx, join } => {
                use futures::SinkExt;
                let _ = tx.send(Err(())).await;
                drop(tx);
                let _ = join.await;
            }
        }
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_escapes_embedded_quotes_and_paths() {
        assert_eq!(quote_ident("events"), r#""events""#);
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(quote_ident_path("public.events"), r#""public"."events""#);
        assert_eq!(quote_ident_path("bare"), r#""bare""#);
    }

    /// Build one binary-COPY stream: header (+ext), tuples, trailer.
    fn copy_stream(ext: &[u8], tuples: &[u8]) -> Vec<u8> {
        let mut v = b"PGCOPY\n\xff\r\n\0".to_vec();
        v.extend(0u32.to_be_bytes());
        v.extend((ext.len() as u32).to_be_bytes());
        v.extend_from_slice(ext);
        v.extend_from_slice(tuples);
        v.extend((-1i16).to_be_bytes());
        v
    }

    #[test]
    fn span_strip_removes_framing_at_any_chunking() {
        // Two spans (the second with a header extension area), pathological chunk
        // sizes — the stripped output must be exactly the concatenated tuple bytes.
        let t1 = [0u8, 1, 0xFF, 0xFF, 7, 8]; // tuple bytes may contain FF FF
        let t2 = [9u8, 10, 11];
        for chunk_size in 1..=7 {
            let mut out = Vec::new();
            for (ext, tuples) in [(&b""[..], &t1[..]), (&b"xtra"[..], &t2[..])] {
                let stream = copy_stream(ext, tuples);
                let mut strip = SpanStrip::new();
                for c in stream.chunks(chunk_size) {
                    strip.push(c, &mut out).unwrap();
                }
                assert!(strip.finished(), "chunk_size={chunk_size}");
            }
            assert_eq!(out, [&t1[..], &t2[..]].concat(), "chunk_size={chunk_size}");
        }
        // Truncated stream (no trailer) must NOT report finished.
        let mut strip = SpanStrip::new();
        let mut out = Vec::new();
        let full = copy_stream(b"", &t1);
        strip.push(&full[..full.len() - 1], &mut out).unwrap();
        assert!(!strip.finished());
        // Bad signature is rejected.
        let mut strip = SpanStrip::new();
        assert!(strip
            .push(b"NOTPGCOPY\0\0\0\0\0\0\0\0\0\0", &mut Vec::new())
            .is_err());
    }

    #[test]
    fn pg_types_for_deliveries_match_the_lossless_map() {
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 2,
                unsigned: false
            }),
            "smallint"
        );
        // Unsigned widens so the value always fits (Postgres has no unsigned ints).
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 2,
                unsigned: true
            }),
            "integer"
        );
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 4,
                unsigned: true
            }),
            "bigint"
        );
        assert_eq!(
            pg_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: true
            }),
            "numeric(20,0)"
        );
        assert_eq!(
            pg_type_of(&Delivered::Decimal { p: 20, s: 0 }),
            "numeric(20,0)"
        );
        assert_eq!(pg_type_of(&Delivered::Decimal { p: 0, s: 0 }), "numeric");
        assert_eq!(pg_type_of(&Delivered::Json), "jsonb");
        assert_eq!(
            pg_type_of(&Delivered::DateTime { utc: true }),
            "timestamptz(6)"
        );
        assert_eq!(pg_type_of(&Delivered::Bytes), "bytea");
    }
}
