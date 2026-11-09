//! `apitap-sql://<token>`: the answer of an `apitap.sql()` query, as a source.
//!
//! `apitap.sql(..., dest=...)` computes the answer in Rust (`crate::sql`),
//! registers it here under a random token, and hands
//! `apitap-sql://<token>` to the ordinary `transfer()`. Every destination then
//! lands it the way it lands any table: typed columns, staging, the atomic
//! swap, the run lock. Nothing was written for one destination only.
//!
//! The columns are typed, so a SUM of decimals lands as numeric(38, s), a
//! COUNT as bigint and a group key in its source type. They are produced in the
//! three wire formats the sinks take between them:
//!
//! - Postgres binary COPY: Postgres, S3, Iceberg, the parquet lanes;
//! - MySQL TSV;
//! - the Postgres text dialect (TabSeparated): ClickHouse, the CSV lanes.

use crate::error::{Error, Result};
use crate::pipe::{Pipe, PipeBody, Pipes, Rows, Spans};
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::Loader;
use crate::source::Source;
use crate::wire::arrowcol::{ArrowKind, FinishedCol};
use crate::wire::pgcopy as pgc;
use crate::SqlResult;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

static REGISTRY: Mutex<Option<HashMap<String, Arc<Held>>>> = Mutex::new(None);

/// A registered answer: rows in memory, or a stage table (on disk past its
/// budget) read a chunk at a time as it lands.
pub(crate) enum Held {
    Rows(SqlResult),
    Table(crate::sql::mem::MemTable),
}

impl Held {
    fn schema(&self) -> &[crate::read::ArrowField] {
        match self {
            Held::Rows(r) => &r.schema,
            Held::Table(t) => &t.schema,
        }
    }

    fn rows(&self) -> usize {
        match self {
            Held::Rows(r) => r.batch.rows,
            Held::Table(t) => t.rows(),
        }
    }

    fn n_chunks(&self) -> usize {
        match self {
            Held::Rows(_) => 1,
            Held::Table(t) => t.n_chunks(),
        }
    }

    fn chunk(&self, i: usize) -> Result<std::borrow::Cow<'_, crate::ArrowBatch>> {
        match self {
            Held::Rows(r) => Ok(std::borrow::Cow::Borrowed(&r.batch)),
            Held::Table(t) => t.chunk(i),
        }
    }
}

/// Keeps the result reachable as `apitap-sql://<token>` while it lives.
pub(crate) struct Registered {
    pub url: String,
    token: String,
}

impl Drop for Registered {
    fn drop(&mut self) {
        if let Ok(mut g) = REGISTRY.lock() {
            if let Some(m) = g.as_mut() {
                m.remove(&self.token);
            }
        }
    }
}

pub(crate) fn register(r: SqlResult) -> Registered {
    register_held(Held::Rows(r))
}

/// [`register`] for a stage table: its chunks land one at a time.
pub(crate) fn register_table(t: crate::sql::mem::MemTable) -> Registered {
    register_held(Held::Table(t))
}

fn register_held(h: Held) -> Registered {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let token = format!(
        "{:x}{:x}{:x}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    REGISTRY
        .lock()
        .expect("sql result registry")
        .get_or_insert_with(HashMap::new)
        .insert(token.clone(), Arc::new(h));
    Registered {
        url: format!("apitap-sql://{token}"),
        token,
    }
}

pub(crate) struct SqlResultSource {
    res: Arc<Held>,
}

impl SqlResultSource {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let token = url.strip_prefix("apitap-sql://").unwrap_or(url);
        let res = REGISTRY
            .lock()
            .expect("sql result registry")
            .as_ref()
            .and_then(|m| m.get(token).cloned())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "apitap-sql:// names no live apitap.sql() result (it exists only inside \
                     apitap.sql(..., dest=...))"
                        .into(),
                )
            })?;
        Ok(Self { res })
    }
}

fn udt(k: ArrowKind) -> (&'static str, Option<i32>, Option<i32>) {
    match k {
        ArrowKind::Int16 => ("smallint", None, None),
        ArrowKind::Int32 => ("integer", None, None),
        ArrowKind::Int64 => ("bigint", None, None),
        ArrowKind::Float32 => ("real", None, None),
        ArrowKind::Float64 => ("double precision", None, None),
        ArrowKind::Decimal { p, s } => ("numeric", Some(i32::from(p)), Some(i32::from(s))),
        ArrowKind::Bool => ("boolean", None, None),
        ArrowKind::Date32 => ("date", None, None),
        ArrowKind::TimestampUtc => ("timestamptz", None, None),
        ArrowKind::TimestampNaive => ("timestamp", None, None),
        ArrowKind::Utf8 => ("text", None, None),
        ArrowKind::Binary => ("bytea", None, None),
    }
}

fn delivered(k: ArrowKind) -> Delivered {
    match k {
        ArrowKind::Int16 => Delivered::Int { bytes: 2, unsigned: false },
        ArrowKind::Int32 => Delivered::Int { bytes: 4, unsigned: false },
        ArrowKind::Int64 => Delivered::Int { bytes: 8, unsigned: false },
        ArrowKind::Float32 => Delivered::Float32,
        ArrowKind::Float64 => Delivered::Float64,
        ArrowKind::Decimal { p, s } => Delivered::Decimal {
            p: u16::from(p),
            s: s.max(0) as u16,
        },
        ArrowKind::Bool => Delivered::Bool,
        ArrowKind::Date32 => Delivered::Date,
        ArrowKind::TimestampUtc => Delivered::DateTime { utc: true },
        ArrowKind::TimestampNaive => Delivered::DateTime { utc: false },
        ArrowKind::Utf8 => Delivered::Text,
        ArrowKind::Binary => Delivered::Bytes,
    }
}

impl Source for SqlResultSource {
    fn cursor_quoted(&self, _udt: &str) -> Result<bool> {
        Err(Error::InvalidInput(
            "apitap.sql(dest=...) lands its answer with mode='replace'; it has no cursor".into(),
        ))
    }

    async fn probe(&self, _table: &str) -> Result<TablePlan> {
        if let Some(f) = self.res.schema().iter().find(|f| f.kind == ArrowKind::Binary) {
            return Err(Error::InvalidInput(format!(
                "apitap.sql(dest=...): the result column {} is binary, which v1 does not land",
                f.name
            )));
        }
        Ok(TablePlan {
            engine: "apitap.sql",
            cols: self
                .res
                .schema()
                .iter()
                .map(|f| {
                    let (u, p, s) = udt(f.kind);
                    ColumnPlan {
                        name: f.name.clone(),
                        nullable: true,
                        int_pk: false,
                        native_ddl: None,
                        udt: u.into(),
                        precision: p,
                        scale: s,
                    }
                })
                .collect(),
            cursor: None,
            pk_cols: Vec::new(),
            cursor_col: None,
        })
    }

    async fn catalog(&self, _schema: Option<&str>, tables: Option<&[String]>) -> Result<Vec<(String, i64)>> {
        let n = self.res.rows() as i64;
        Ok(match tables {
            Some(ts) => ts.iter().map(|t| (t.clone(), n)).collect(),
            None => vec![("result".to_string(), n)],
        })
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        matches!(format, WireFormat::PgCopyBinary | WireFormat::MyTsv | WireFormat::TabSeparated)
    }

    fn plan_lane(&self, _plan: &TablePlan, format: WireFormat) -> Lane {
        Lane {
            format,
            raw_frames: false,
            push_where: None,
            cols: self
                .res
                .schema()
                .iter()
                .map(|f| LaneCol {
                    delivered: delivered(f.kind),
                    select: f.name.clone(),
                })
                .collect(),
        }
    }

    async fn span_stmts(
        &self,
        table: &str,
        _plan: &TablePlan,
        _lane: &Lane,
        _want: usize,
        _delta: Option<&Delta>,
    ) -> Result<Vec<String>> {
        Ok(vec![table.to_string()])
    }

    async fn run_workers<L: Loader>(
        &self,
        _plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        pipes: Pipes<L>,
        chunk: usize,
    ) -> Result<u64> {
        pipes
            .run_inline(stmts, ResBody { res: &self.res, format: lane.format, chunk })
            .await
    }
}

struct ResBody<'a> {
    res: &'a Held,
    format: WireFormat,
    chunk: usize,
}

fn valid(v: &Option<Vec<u8>>, r: usize) -> bool {
    v.as_ref().is_none_or(|bm| bm[r / 8] & (1 << (r % 8)) != 0)
}

/// Days from 1970-01-01 to 2000-01-01, Postgres's binary epoch.
const PG_EPOCH_DAYS: i32 = 10957;
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

fn dec_text(v: i128, s: i8) -> String {
    if s <= 0 {
        return v.to_string();
    }
    let s = s as usize;
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let digits = if digits.len() <= s { format!("{}{digits}", "0".repeat(s + 1 - digits.len())) } else { digits };
    let (w, f) = digits.split_at(digits.len() - s);
    format!("{}{w}.{f}", if neg { "-" } else { "" })
}

/// (year, month, day) of a day count from 1970-01-01 (proleptic Gregorian;
/// Howard Hinnant's civil_from_days).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn date_text(days: i32) -> String {
    let (y, m, d) = civil(i64::from(days));
    format!("{y:04}-{m:02}-{d:02}")
}

fn ts_text(us: i64, utc: bool) -> String {
    let days = us.div_euclid(86_400_000_000);
    let rem = us.rem_euclid(86_400_000_000);
    let (y, mo, d) = civil(days);
    let (h, mi, sec, frac) = (rem / 3_600_000_000, rem / 60_000_000 % 60, rem / 1_000_000 % 60, rem % 1_000_000);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec:02}.{frac:06}{}", if utc { "+00" } else { "" })
}

/// Cell `r` of a column as text, `None` = NULL. Booleans are spelled for
/// `my` (1/0) or Postgres text (t/f).
fn text(c: &FinishedCol, k: ArrowKind, r: usize, my: bool) -> Option<Vec<u8>> {
    let s = match c {
        FinishedCol::I16 { validity, data } if valid(validity, r) => data[r].to_string(),
        FinishedCol::I32 { validity, data } if valid(validity, r) => match k {
            ArrowKind::Date32 => date_text(data[r]),
            _ => data[r].to_string(),
        },
        FinishedCol::I64 { validity, data } if valid(validity, r) => match k {
            ArrowKind::TimestampUtc => ts_text(data[r], true),
            ArrowKind::TimestampNaive => ts_text(data[r], false),
            _ => data[r].to_string(),
        },
        FinishedCol::F32 { validity, data } if valid(validity, r) => data[r].to_string(),
        FinishedCol::F64 { validity, data } if valid(validity, r) => data[r].to_string(),
        FinishedCol::Dec128 { validity, data } if valid(validity, r) => match k {
            ArrowKind::Decimal { s, .. } => dec_text(data[r], s),
            _ => data[r].to_string(),
        },
        FinishedCol::Bool { validity, data } if valid(validity, r) => {
            let on = data[r / 8] & (1 << (r % 8)) != 0;
            (if my { if on { "1" } else { "0" } } else if on { "t" } else { "f" }).to_string()
        }
        FinishedCol::Utf8 { validity, offsets, data } if valid(validity, r) => {
            return Some(data[offsets[r] as usize..offsets[r + 1] as usize].to_vec())
        }
        _ => return None,
    };
    Some(s.into_bytes())
}

/// Postgres text COPY escaping.
fn pg_text_escape(b: &[u8], out: &mut Vec<u8>) {
    for &c in b {
        match c {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            _ => out.push(c),
        }
    }
}

fn binary_cell(c: &FinishedCol, k: ArrowKind, r: usize, out: &mut Vec<u8>) -> Result<()> {
    match c {
        FinishedCol::I16 { validity, data } if valid(validity, r) => pgc::field(&data[r].to_be_bytes(), out),
        FinishedCol::I32 { validity, data } if valid(validity, r) => match k {
            ArrowKind::Date32 => pgc::field(&(data[r] - PG_EPOCH_DAYS).to_be_bytes(), out),
            _ => pgc::field(&data[r].to_be_bytes(), out),
        },
        FinishedCol::I64 { validity, data } if valid(validity, r) => match k {
            ArrowKind::TimestampUtc | ArrowKind::TimestampNaive => {
                pgc::field(&(data[r] - PG_EPOCH_MICROS).to_be_bytes(), out)
            }
            _ => pgc::field(&data[r].to_be_bytes(), out),
        },
        FinishedCol::F32 { validity, data } if valid(validity, r) => pgc::field(&data[r].to_bits().to_be_bytes(), out),
        FinishedCol::F64 { validity, data } if valid(validity, r) => pgc::field(&data[r].to_bits().to_be_bytes(), out),
        FinishedCol::Dec128 { validity, data } if valid(validity, r) => {
            let s = match k {
                ArrowKind::Decimal { s, .. } => s,
                _ => 0,
            };
            pgc::numeric_field_from_str(&dec_text(data[r], s), out)?
        }
        FinishedCol::Bool { validity, data } if valid(validity, r) => {
            pgc::field(&[u8::from(data[r / 8] & (1 << (r % 8)) != 0)], out)
        }
        FinishedCol::Utf8 { validity, offsets, data } if valid(validity, r) => {
            pgc::field(&data[offsets[r] as usize..offsets[r + 1] as usize], out)
        }
        _ => pgc::null_field(out),
    }
    Ok(())
}

impl<L: Loader> PipeBody<L> for ResBody<'_> {
    async fn run(self, pipe: &mut Pipe<L>, spans: Spans) -> Result<Rows> {
        let _ = spans.next()?;
        let kinds: Vec<ArrowKind> = self.res.schema().iter().map(|f| f.kind).collect();
        let mut out: Vec<u8> = Vec::with_capacity(self.chunk.min(1 << 20) + 4096);
        if self.format == WireFormat::PgCopyBinary {
            pgc::header(&mut out);
        }
        // A chunk of the answer at a time (one, for rows in memory).
        for ci in 0..self.res.n_chunks() {
        let b = self.res.chunk(ci)?;
        for r in 0..b.rows {
            match self.format {
                WireFormat::PgCopyBinary => {
                    pgc::tuple_start(b.cols.len(), &mut out);
                    for (c, &k) in b.cols.iter().zip(&kinds) {
                        binary_cell(c, k, r, &mut out)?;
                    }
                }
                WireFormat::MyTsv | WireFormat::TabSeparated => {
                    let my = self.format == WireFormat::MyTsv;
                    for (i, (c, &k)) in b.cols.iter().zip(&kinds).enumerate() {
                        if i > 0 {
                            out.push(b'\t');
                        }
                        match text(c, k, r, my) {
                            None => out.extend_from_slice(b"\\N"),
                            Some(t) if my => crate::wire::mytsv::tsv_escape(&t, &mut out),
                            Some(t) => pg_text_escape(&t, &mut out),
                        }
                    }
                    out.push(b'\n');
                }
                WireFormat::RowBinary => {
                    return Err(Error::Transfer("apitap.sql result: RowBinary was not negotiated".into()))
                }
            }
            if out.len() >= self.chunk {
                let buf = std::mem::replace(&mut out, Vec::with_capacity(self.chunk.min(1 << 20) + 4096));
                pipe.send(buf).await?;
            }
        }
        }
        if self.format == WireFormat::PgCopyBinary {
            pgc::trailer(&mut out);
        }
        if !out.is_empty() {
            pipe.send(out).await?;
        }
        Ok(Rows::LoaderElseOwn(self.res.rows() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_and_date_text() {
        assert_eq!(dec_text(54_288_091_370_089, 4), "5428809137.0089");
        assert_eq!(dec_text(-5, 4), "-0.0005");
        assert_eq!(dec_text(7, 0), "7");
        assert_eq!(date_text(0), "1970-01-01");
        assert_eq!(date_text(9204), "1995-03-15");
        assert_eq!(ts_text(0, true), "1970-01-01 00:00:00.000000+00");
    }
}
