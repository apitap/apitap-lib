//! `mem://<run>`: a staged query's intermediate answers as tables.
//!
//! A query split into stages (`stage`) keeps each stage's answer here, under
//! the run's URL and the stage's name, and the later stages read it like any
//! source: `read_schema`, `read_start` (projected) and a row count. Nothing
//! leaves the process; a run's tables go when the run ends (`Run`'s drop).

use crate::error::{Error, Result};
use crate::read::{ArrowField, ReadHandle, ReadOptions};
use crate::wire::arrowcol::{ArrowBatch, FinishedCol};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) const SCHEME: &str = "mem://";

/// One stage's answer: in memory, or past [`stage_budget`] on disk in
/// chunks a reader takes one at a time (TPC-H Q13 sf10 staged 9.9M rows).
pub(crate) struct MemTable {
    pub schema: Vec<ArrowField>,
    rows: usize,
    chunks: Chunks,
}

enum Chunks {
    Mem(ArrowBatch),
    /// The file and each chunk's (offset, length).
    Disk(std::path::PathBuf, Vec<(u64, u64)>),
}

impl Drop for MemTable {
    fn drop(&mut self) {
        if let Chunks::Disk(path, _) = &self.chunks {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Rows per chunk of a stage on disk.
const CHUNK: usize = 65536;

/// The bytes a stage may hold in memory: `APITAP_SQL_STAGE_BYTES` (a test
/// lever), else a sixteenth of the memory limit, else 256 MiB.
fn stage_budget() -> u64 {
    if let Some(b) = std::env::var("APITAP_SQL_STAGE_BYTES")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .filter(|b| *b > 0)
    {
        return b;
    }
    crate::pipeline::mem_limit_bytes().map_or(256 << 20, |m| m / 16)
}

impl MemTable {
    /// A stage of `batch`: written to disk chunk by chunk past the budget.
    pub(crate) fn new(schema: Vec<ArrowField>, batch: ArrowBatch) -> Result<MemTable> {
        let rows = batch.rows;
        if (batch.bytes() as u64) <= stage_budget() {
            return Ok(MemTable { schema, rows, chunks: Chunks::Mem(batch) });
        }
        use std::io::Write as _;
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("apitap-stage-{}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
        let io = |e: std::io::Error| Error::Transfer(format!("sql: stage file {}: {e}", path.display()));
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).map_err(io)?);
        let mut at = Vec::new();
        let mut off = 0u64;
        let mut buf = Vec::new();
        let mut r = 0;
        while r < rows {
            let n = CHUNK.min(rows - r);
            let idx: Vec<usize> = (r..r + n).collect();
            let c = take_rows(&batch, &idx);
            buf.clear();
            encode(&c, &mut buf);
            f.write_all(&buf).map_err(io)?;
            at.push((off, buf.len() as u64));
            off += buf.len() as u64;
            r += n;
        }
        f.flush().map_err(io)?;
        drop(batch);
        Ok(MemTable { schema, rows, chunks: Chunks::Disk(path, at) })
    }
}

fn put_opt(w: &mut Vec<u8>, v: &Option<Vec<u8>>) {
    match v {
        None => w.push(0),
        Some(b) => {
            w.push(1);
            w.extend_from_slice(&(b.len() as u64).to_le_bytes());
            w.extend_from_slice(b);
        }
    }
}

fn encode(b: &ArrowBatch, w: &mut Vec<u8>) {
    w.extend_from_slice(&(b.rows as u64).to_le_bytes());
    w.extend_from_slice(&(b.cols.len() as u64).to_le_bytes());
    macro_rules! fixed {
        ($t:expr, $v:ident, $d:ident) => {{
            w.push($t);
            put_opt(w, $v);
            w.extend_from_slice(&($d.len() as u64).to_le_bytes());
            for x in $d {
                w.extend_from_slice(&x.to_le_bytes());
            }
        }};
    }
    for c in &b.cols {
        match c {
            FinishedCol::I16 { validity: v, data: d } => fixed!(0, v, d),
            FinishedCol::I32 { validity: v, data: d } => fixed!(1, v, d),
            FinishedCol::I64 { validity: v, data: d } => fixed!(2, v, d),
            FinishedCol::F32 { validity: v, data: d } => fixed!(3, v, d),
            FinishedCol::F64 { validity: v, data: d } => fixed!(4, v, d),
            FinishedCol::Dec128 { validity: v, data: d } => fixed!(5, v, d),
            FinishedCol::Bool { validity: v, data: d } => fixed!(6, v, d),
            FinishedCol::Utf8 { validity: v, offsets: o, data: d } | FinishedCol::Bin { validity: v, offsets: o, data: d } => {
                w.push(if matches!(c, FinishedCol::Utf8 { .. }) { 7 } else { 8 });
                put_opt(w, v);
                w.extend_from_slice(&(o.len() as u64).to_le_bytes());
                for x in o {
                    w.extend_from_slice(&x.to_le_bytes());
                }
                w.extend_from_slice(&(d.len() as u64).to_le_bytes());
                w.extend_from_slice(d);
            }
        }
    }
}

/// [`encode`]'s chunk back, columns `want` only.
fn decode(r: &[u8], want: &[usize]) -> Result<ArrowBatch> {
    let short = || Error::Transfer("sql: a stage chunk ended early".into());
    let mut i = 0usize;
    let u64_at = |i: &mut usize| -> Result<u64> {
        let v = u64::from_le_bytes(r.get(*i..*i + 8).ok_or_else(short)?.try_into().expect("8 bytes"));
        *i += 8;
        Ok(v)
    };
    let rows = u64_at(&mut i)? as usize;
    let ncols = u64_at(&mut i)? as usize;
    let mut cols: Vec<Option<FinishedCol>> = (0..ncols).map(|_| None).collect();
    fn opt(r: &[u8], i: &mut usize) -> Result<Option<Vec<u8>>> {
        let short = || Error::Transfer("sql: a stage chunk ended early".into());
        let has = *r.get(*i).ok_or_else(short)?;
        *i += 1;
        if has == 0 {
            return Ok(None);
        }
        let n = u64::from_le_bytes(r.get(*i..*i + 8).ok_or_else(short)?.try_into().expect("8")) as usize;
        *i += 8;
        let v = r.get(*i..*i + n).ok_or_else(short)?.to_vec();
        *i += n;
        Ok(Some(v))
    }
    macro_rules! vec_of {
        ($t:ty, $w:expr, $i:ident) => {{
            let n = u64::from_le_bytes(r.get($i..$i + 8).ok_or_else(short)?.try_into().expect("8")) as usize;
            $i += 8;
            let b = r.get($i..$i + n * $w).ok_or_else(short)?;
            $i += n * $w;
            b.chunks_exact($w).map(|x| <$t>::from_le_bytes(x.try_into().expect("width"))).collect::<Vec<$t>>()
        }};
    }
    for c in 0..ncols {
        let tag = *r.get(i).ok_or_else(short)?;
        i += 1;
        let v = opt(r, &mut i)?;
        let col = match tag {
            0 => FinishedCol::I16 { validity: v, data: vec_of!(i16, 2, i) },
            1 => FinishedCol::I32 { validity: v, data: vec_of!(i32, 4, i) },
            2 => FinishedCol::I64 { validity: v, data: vec_of!(i64, 8, i) },
            3 => FinishedCol::F32 { validity: v, data: vec_of!(f32, 4, i) },
            4 => FinishedCol::F64 { validity: v, data: vec_of!(f64, 8, i) },
            5 => FinishedCol::Dec128 { validity: v, data: vec_of!(i128, 16, i) },
            6 => FinishedCol::Bool { validity: v, data: vec_of!(u8, 1, i) },
            7 | 8 => {
                let offsets = vec_of!(i32, 4, i);
                let data = vec_of!(u8, 1, i);
                if tag == 7 {
                    FinishedCol::Utf8 { validity: v, offsets, data }
                } else {
                    FinishedCol::Bin { validity: v, offsets, data }
                }
            }
            _ => return Err(Error::Transfer("sql: a stage chunk of an unknown column".into())),
        };
        if want.contains(&c) {
            cols[c] = Some(col);
        }
    }
    Ok(ArrowBatch { rows, cols: want.iter().map(|&c| cols[c].take().ok_or_else(short)).collect::<Result<_>>()? })
}

type Tables = HashMap<String, HashMap<String, Arc<MemTable>>>;
static TABLES: Mutex<Option<Tables>> = Mutex::new(None);

/// A staged run's tables: registered under `url`, dropped with it.
pub(crate) struct Run {
    pub url: String,
}

impl Run {
    pub(crate) fn new() -> Run {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        Run { url: format!("{SCHEME}{:x}-{:x}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)) }
    }

    pub(crate) fn put(&self, name: &str, t: MemTable) {
        // (`MemTable::new` decided memory or disk.)
        TABLES
            .lock()
            .expect("mem tables")
            .get_or_insert_with(HashMap::new)
            .entry(self.url.clone())
            .or_default()
            .insert(name.to_ascii_lowercase(), Arc::new(t));
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        if let Ok(mut g) = TABLES.lock() {
            if let Some(m) = g.as_mut() {
                m.remove(&self.url);
            }
        }
    }
}

pub(crate) fn is_mem(url: &str) -> bool {
    url.starts_with(SCHEME)
}

fn get(url: &str, table: &str) -> Result<Arc<MemTable>> {
    TABLES
        .lock()
        .expect("mem tables")
        .as_ref()
        .and_then(|m| m.get(url))
        .and_then(|t| t.get(&table.to_ascii_lowercase()).cloned())
        .ok_or_else(|| Error::InvalidInput(format!("sql: no stage {table} in this query")))
}

pub(crate) fn schema(url: &str, table: &str) -> Result<Vec<ArrowField>> {
    Ok(get(url, table)?.schema.clone())
}

pub(crate) fn rows(url: &str, table: &str) -> Option<f64> {
    get(url, table).ok().map(|t| t.rows as f64)
}

/// The table's rows, projected to `opts.columns` (all when `None`), as one
/// batch already here.
pub(crate) fn start(url: &str, table: &str, opts: &ReadOptions) -> Result<ReadHandle> {
    let t = get(url, table)?;
    let idx: Vec<usize> = match &opts.columns {
        None => (0..t.schema.len()).collect(),
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.schema
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(c))
                    .ok_or_else(|| Error::InvalidInput(format!("sql: stage {table} has no column {c}")))
            })
            .collect::<Result<_>>()?,
    };
    let schema: Vec<ArrowField> = idx.iter().map(|&i| t.schema[i].clone()).collect();
    // A chunk at a time through a bounded channel: the reader never holds
    // the whole stage a second time.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<ArrowBatch>>(2);
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = completed.clone();
    let sup = tokio::spawn(async move {
        match &t.chunks {
            Chunks::Mem(b) => {
                let mut r = 0;
                while r < b.rows {
                    let n = CHUNK.min(b.rows - r);
                    let rows: Vec<usize> = (r..r + n).collect();
                    let cols: Vec<FinishedCol> = idx.iter().map(|&i| slice_col(&b.cols[i], &rows)).collect();
                    if tx.send(Ok(ArrowBatch { rows: n, cols })).await.is_err() {
                        return;
                    }
                    r += n;
                }
            }
            Chunks::Disk(path, at) => {
                use std::io::{Read as _, Seek as _};
                let mut f = match std::fs::File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        let _ = tx.send(Err(Error::Transfer(format!("sql: stage file {}: {e}", path.display())))).await;
                        return;
                    }
                };
                for &(off, len) in at {
                    let mut buf = vec![0u8; len as usize];
                    let r = f
                        .seek(std::io::SeekFrom::Start(off))
                        .and_then(|_| f.read_exact(&mut buf))
                        .map_err(|e| Error::Transfer(format!("sql: stage file {}: {e}", path.display())))
                        .and_then(|_| decode(&buf, &idx));
                    let stop = r.is_err();
                    if tx.send(r).await.is_err() || stop {
                        return;
                    }
                }
            }
        }
        done.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    Ok(ReadHandle::new(schema, rx, sup, completed))
}

/// Rows `rows` (consecutive) of one column.
fn slice_col(c: &FinishedCol, rows: &[usize]) -> FinishedCol {
    let mut o = empty_like(c);
    let mut at = 0;
    for &r in rows {
        push_rep(&mut o, c, r, 1, &mut at);
    }
    o
}

/// Row `r` of column `c` repeated `n` times onto `out` (same variant).
fn push_rep(out: &mut FinishedCol, c: &FinishedCol, r: usize, n: usize, at: &mut usize) {
    fn bit(v: &Option<Vec<u8>>, r: usize) -> bool {
        v.as_ref().is_none_or(|b| b[r / 8] & (1 << (r % 8)) != 0)
    }
    fn set(v: &mut Option<Vec<u8>>, i: usize, on: bool) {
        let b = v.get_or_insert_with(Vec::new);
        if b.len() <= i / 8 {
            b.resize(i / 8 + 1, 0);
        }
        if on {
            b[i / 8] |= 1 << (i % 8);
        }
    }
    macro_rules! fixed {
        ($ov:ident, $od:ident, $v:ident, $d:ident) => {{
            for k in 0..n {
                set($ov, *at + k, bit($v, r));
                $od.push($d[r]);
            }
        }};
    }
    match (out, c) {
        (FinishedCol::I16 { validity: ov, data: od }, FinishedCol::I16 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::I32 { validity: ov, data: od }, FinishedCol::I32 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::I64 { validity: ov, data: od }, FinishedCol::I64 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::F32 { validity: ov, data: od }, FinishedCol::F32 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::F64 { validity: ov, data: od }, FinishedCol::F64 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::Dec128 { validity: ov, data: od }, FinishedCol::Dec128 { validity: v, data: d }) => fixed!(ov, od, v, d),
        (FinishedCol::Bool { validity: ov, data: od }, FinishedCol::Bool { validity: v, data: d }) => {
            for k in 0..n {
                let i = *at + k;
                set(ov, i, bit(v, r));
                if od.len() <= i / 8 {
                    od.resize(i / 8 + 1, 0);
                }
                if d[r / 8] & (1 << (r % 8)) != 0 {
                    od[i / 8] |= 1 << (i % 8);
                }
            }
        }
        (FinishedCol::Utf8 { validity: ov, offsets: oo, data: od }, FinishedCol::Utf8 { validity: v, offsets: o, data: d })
        | (FinishedCol::Bin { validity: ov, offsets: oo, data: od }, FinishedCol::Bin { validity: v, offsets: o, data: d }) => {
            let s = &d[o[r] as usize..o[r + 1] as usize];
            for k in 0..n {
                set(ov, *at + k, bit(v, r));
                od.extend_from_slice(s);
                oo.push(od.len() as i32);
            }
        }
        _ => {}
    }
    *at += n;
}

fn empty_like(c: &FinishedCol) -> FinishedCol {
    match c {
        FinishedCol::I16 { .. } => FinishedCol::I16 { validity: None, data: Vec::new() },
        FinishedCol::I32 { .. } => FinishedCol::I32 { validity: None, data: Vec::new() },
        FinishedCol::I64 { .. } => FinishedCol::I64 { validity: None, data: Vec::new() },
        FinishedCol::F32 { .. } => FinishedCol::F32 { validity: None, data: Vec::new() },
        FinishedCol::F64 { .. } => FinishedCol::F64 { validity: None, data: Vec::new() },
        FinishedCol::Bool { .. } => FinishedCol::Bool { validity: None, data: Vec::new() },
        FinishedCol::Dec128 { .. } => FinishedCol::Dec128 { validity: None, data: Vec::new() },
        FinishedCol::Utf8 { .. } => FinishedCol::Utf8 { validity: None, offsets: vec![0], data: Vec::new() },
        FinishedCol::Bin { .. } => FinishedCol::Bin { validity: None, offsets: vec![0], data: Vec::new() },
    }
}

/// Each row repeated by its count (`counts[r]`), up to `limit` rows; the
/// validity bitmaps come out sized to the rows (or `None` when all valid).
pub(crate) fn repeat(b: &ArrowBatch, counts: &[usize], limit: Option<usize>) -> ArrowBatch {
    let mut cols: Vec<FinishedCol> = b.cols.iter().map(empty_like).collect();
    let mut rows = 0usize;
    for (r, &n) in counts.iter().enumerate() {
        let n = match limit {
            Some(l) => n.min(l.saturating_sub(rows)),
            None => n,
        };
        if n == 0 {
            continue;
        }
        for (o, c) in cols.iter_mut().zip(&b.cols) {
            let mut at = rows;
            push_rep(o, c, r, n, &mut at);
        }
        rows += n;
    }
    for c in &mut cols {
        let v = match c {
            FinishedCol::I16 { validity, .. }
            | FinishedCol::I32 { validity, .. }
            | FinishedCol::I64 { validity, .. }
            | FinishedCol::F32 { validity, .. }
            | FinishedCol::F64 { validity, .. }
            | FinishedCol::Bool { validity, .. }
            | FinishedCol::Dec128 { validity, .. }
            | FinishedCol::Utf8 { validity, .. }
            | FinishedCol::Bin { validity, .. } => validity,
        };
        if let Some(bits) = v {
            bits.resize(rows.div_ceil(8), 0);
            let all = (0..rows).all(|i| bits[i / 8] & (1 << (i % 8)) != 0);
            if all {
                *v = None;
            }
        }
    }
    for c in &mut cols {
        if let FinishedCol::Bool { data, .. } = c {
            data.resize(rows.div_ceil(8), 0);
        }
    }
    ArrowBatch { rows, cols }
}

/// `n` zeros in `like`'s variant (a COUNT over no rows).
pub(crate) fn zeros_like(like: &FinishedCol, n: usize) -> FinishedCol {
    match like {
        FinishedCol::I16 { .. } => FinishedCol::I16 { validity: None, data: vec![0; n] },
        FinishedCol::I32 { .. } => FinishedCol::I32 { validity: None, data: vec![0; n] },
        FinishedCol::F32 { .. } => FinishedCol::F32 { validity: None, data: vec![0.0; n] },
        FinishedCol::F64 { .. } => FinishedCol::F64 { validity: None, data: vec![0.0; n] },
        FinishedCol::Dec128 { .. } => FinishedCol::Dec128 { validity: None, data: vec![0; n] },
        _ => FinishedCol::I64 { validity: None, data: vec![0; n] },
    }
}

/// `n` NULLs in `like`'s variant.
pub(crate) fn nulls_like(like: &FinishedCol, n: usize) -> FinishedCol {
    let v = Some(vec![0u8; n.div_ceil(8)]);
    match like {
        FinishedCol::I16 { .. } => FinishedCol::I16 { validity: v, data: vec![0; n] },
        FinishedCol::I32 { .. } => FinishedCol::I32 { validity: v, data: vec![0; n] },
        FinishedCol::I64 { .. } => FinishedCol::I64 { validity: v, data: vec![0; n] },
        FinishedCol::F32 { .. } => FinishedCol::F32 { validity: v, data: vec![0.0; n] },
        FinishedCol::F64 { .. } => FinishedCol::F64 { validity: v, data: vec![0.0; n] },
        FinishedCol::Bool { .. } => FinishedCol::Bool { validity: v, data: vec![0; n.div_ceil(8)] },
        FinishedCol::Dec128 { .. } => FinishedCol::Dec128 { validity: v, data: vec![0; n] },
        FinishedCol::Utf8 { .. } => FinishedCol::Utf8 { validity: v, offsets: vec![0; n + 1], data: Vec::new() },
        FinishedCol::Bin { .. } => FinishedCol::Bin { validity: v, offsets: vec![0; n + 1], data: Vec::new() },
    }
}

/// `a`'s rows then `b`'s, column by column (the same variants).
/// The rows `idx` of `b`, in that order.
pub(crate) fn take_rows(b: &ArrowBatch, idx: &[usize]) -> ArrowBatch {
    let mut cols: Vec<FinishedCol> = b.cols.iter().map(empty_like).collect();
    for (o, c) in cols.iter_mut().zip(&b.cols) {
        let mut at = 0;
        for &r in idx {
            push_rep(o, c, r, 1, &mut at);
        }
    }
    ArrowBatch { rows: idx.len(), cols }
}

pub(crate) fn concat(a: &ArrowBatch, b: &ArrowBatch) -> Result<ArrowBatch> {
    if a.cols.len() != b.cols.len() {
        return Err(Error::InvalidInput("sql: internal: concatenated parts differ in columns".into()));
    }
    let mut ones = vec![1usize; a.rows];
    ones.extend(std::iter::repeat_n(1usize, b.rows));
    let mut cols = Vec::with_capacity(a.cols.len());
    for (x, y) in a.cols.iter().zip(&b.cols) {
        if std::mem::discriminant(x) != std::mem::discriminant(y) {
            return Err(Error::InvalidInput("sql: internal: concatenated parts differ in column types".into()));
        }
        let mut o = empty_like(x);
        let mut at = 0usize;
        for r in 0..a.rows {
            push_rep(&mut o, x, r, 1, &mut at);
        }
        for r in 0..b.rows {
            push_rep(&mut o, y, r, 1, &mut at);
        }
        cols.push(o);
    }
    let mut out = ArrowBatch { rows: a.rows + b.rows, cols };
    // Normalise the bitmaps the way `repeat` does.
    out = repeat(&out, &ones[..out.rows], None);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chunk written to a stage file reads back the same, every column
    /// kind and NULLs included, and a projection takes only its columns.
    #[test]
    fn stage_chunks_round_trip() {
        let b = ArrowBatch {
            rows: 3,
            cols: vec![
                FinishedCol::I16 { validity: None, data: vec![1, -2, 3] },
                FinishedCol::I32 { validity: Some(vec![0b101]), data: vec![7, 0, 9] },
                FinishedCol::I64 { validity: None, data: vec![i64::MIN, 0, i64::MAX] },
                FinishedCol::F32 { validity: None, data: vec![1.5, -0.0, 3.25] },
                FinishedCol::F64 { validity: None, data: vec![f64::MIN, 0.1, 2.0] },
                FinishedCol::Dec128 { validity: Some(vec![0b011]), data: vec![i128::MAX, -5, 0] },
                FinishedCol::Bool { validity: None, data: vec![0b110] },
                FinishedCol::Utf8 { validity: Some(vec![0b111]), offsets: vec![0, 1, 1, 4], data: "aéb".as_bytes().to_vec() },
                FinishedCol::Bin { validity: None, offsets: vec![0, 0, 2, 3], data: vec![0, 255, 7] },
            ],
        };
        let mut w = Vec::new();
        encode(&b, &mut w);
        let all: Vec<usize> = (0..b.cols.len()).collect();
        assert_eq!(decode(&w, &all).unwrap(), b);
        let some = decode(&w, &[7, 2]).unwrap();
        assert_eq!(some.cols, vec![
            FinishedCol::Utf8 { validity: Some(vec![0b111]), offsets: vec![0, 1, 1, 4], data: "aéb".as_bytes().to_vec() },
            FinishedCol::I64 { validity: None, data: vec![i64::MIN, 0, i64::MAX] },
        ]);
        assert!(decode(&w[..w.len() - 1], &all).is_err(), "a torn chunk is an error");
    }
}
