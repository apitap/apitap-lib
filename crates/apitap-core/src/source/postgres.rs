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
//!   [`crate::wire::rowbinary::Transcoder`] (byte-swaps, epoch rebasing, exact NUMERIC
//!   scaling) — Postgres skips text formatting, ClickHouse skips text parsing.
//! - **TabSeparated** — text COPY relayed untouched (Postgres `text` COPY and
//!   ClickHouse TabSeparated share the tab delimiter, `\N` NULLs and `\t\n\r\\`
//!   escapes; corner case: a literal vertical tab escapes as `\v`, which ClickHouse
//!   doesn't unescape — checksum validation would catch it). This is the fallback for
//!   tables with a column the binary transcoder doesn't cover.

use crate::pipeline::{pop, spans, WorkQueue};
use crate::sink::Loader;
use crate::source::Source;
use crate::error::{Error, Result};
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::wire::rowbinary::{rb_type, RbType, Transcoder};
use futures::TryStreamExt;
use sqlx::postgres::PgPoolCopyExt;
use sqlx::{PgPool, Row};
use crate::dialect::postgres::{connect_pool, quote_ident, quote_ident_path};
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
    async fn catalog(
        &self,
        schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        if let Some(ts) = tables {
            // Resolve through quote_ident_path + ::regclass — the SAME spelling
            // probe and COPY will use, so a mixed-case "public.MyTable" resolves
            // here exactly as it will read (raw regclass input would case-fold it
            // to a different relation). A missing table fails the whole query with
            // its name in the error — loud, and before any table has moved.
            // reltuples: -1 = never analyzed (unknown). Names echo back by
            // ordinality so the caller keeps its own spelling.
            let quoted: Vec<String> = ts.iter().map(|t| quote_ident_path(t)).collect();
            let rows = sqlx::query(
                "SELECT g.ord AS ord, \
                        (SELECT CASE WHEN c.reltuples < 0 THEN -1 \
                                     ELSE c.reltuples::bigint END \
                         FROM pg_class c WHERE c.oid = g.t::regclass) AS est \
                 FROM unnest($1::text[]) WITH ORDINALITY AS g(t, ord) \
                 ORDER BY g.ord",
            )
            .bind(&quoted)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::InvalidInput(format!("resolving tables: {e}")))?;
            return Ok(rows
                .iter()
                .map(|r| {
                    let ord = r.get::<i64, _>("ord") as usize;
                    (
                        ts[ord - 1].clone(),
                        r.get::<Option<i64>, _>("est").unwrap_or(-1),
                    )
                })
                .collect());
        }
        let schema = schema.ok_or_else(|| {
            Error::InvalidInput(
                "postgres sources need schema='…' (there is no default schema for a \
                 whole-schema transfer)"
                    .into(),
            )
        })?;
        // Base tables, partitioned parents and matviews. CHILDREN — declarative
        // partitions AND old-style INHERITS tables — are skipped when their direct
        // parent lives in this same schema: the parent's scan reads them, so
        // listing both would move every row twice. A child whose parent lives in
        // ANOTHER schema stays listed (its parent isn't in this run — dropping it
        // would silently omit its rows). apitap's own artifacts never travel.
        let rows = sqlx::query(
            "SELECT n.nspname || '.' || c.relname AS name, \
                    CASE WHEN c.reltuples < 0 THEN -1 ELSE c.reltuples::bigint END AS est \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 \
               AND c.relkind IN ('r', 'p', 'm') \
               AND NOT EXISTS (SELECT 1 FROM pg_inherits i \
                               JOIN pg_class p ON p.oid = i.inhparent \
                               JOIN pg_namespace pn ON pn.oid = p.relnamespace \
                               WHERE i.inhrelid = c.oid AND pn.nspname = $1) \
               AND c.relname NOT LIKE '%\\_\\_apitap\\_staging' \
               AND c.relname NOT LIKE '%\\_\\_apitap\\_old' \
               AND c.relname <> '_apitap_state' \
             ORDER BY c.reltuples DESC",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::InvalidInput(format!("listing schema {schema}: {e}")))?;
        Ok(rows
            .iter()
            .map(|r| (r.get::<String, _>("name"), r.get::<i64, _>("est")))
            .collect())
    }

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
        let queue = crate::pipeline::work_queue(stmts);
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
        crate::wire::pgcopy::header(&mut out);
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
        crate::wire::pgcopy::trailer(&mut out);
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


#[cfg(test)]
mod tests {
    use super::*;
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

}
