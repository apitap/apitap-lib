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
/// chunks a reader takes one at a time (TPC-H Q13 10g seed staged 9.9M rows).
pub(crate) struct MemTable {
    pub schema: Vec<ArrowField>,
    rows: usize,
    chunks: Chunks,
    /// The bytes of `chunks` counted in [`STAGE_HELD`] (memory chunks).
    held: u64,
}

/// The bytes every stage table and writer of the process holds in memory:
/// the stage budget bounds them all together, not each stage alone (nine
/// CTEs of 14 MB each under a 16 MB budget held 126 MB, the run peaked at
/// 193 MB of 256).
static STAGE_HELD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn held_add(n: u64) -> u64 {
    STAGE_HELD.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n
}

fn held_sub(n: u64) {
    let _ = STAGE_HELD.fetch_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed, |v| Some(v.saturating_sub(n)));
}

enum Chunks {
    Mem(ArrowBatch),
    /// The file and each chunk's (offset, length).
    Disk(std::path::PathBuf, Vec<(u64, u64)>),
}

impl Drop for MemTable {
    fn drop(&mut self) {
        held_sub(std::mem::take(&mut self.held));
        if let Chunks::Disk(path, _) = &self.chunks {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Rows per chunk of a stage on disk.
const CHUNK: usize = 65536;

/// About the most bytes a chunk on disk (or a repeated piece) holds.
const CHUNK_BYTES: usize = 8 << 20;

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
        let mut w = Writer::new(schema);
        w.push(batch)?;
        w.finish()
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Whether the table's rows are in memory (under the stage budget).
    pub(crate) fn in_memory(&self) -> bool {
        matches!(self.chunks, Chunks::Mem(_))
    }

    /// How many chunks `chunk` reads (1 for a table in memory).
    pub(crate) fn n_chunks(&self) -> usize {
        match &self.chunks {
            Chunks::Mem(_) => 1,
            Chunks::Disk(_, at) => at.len(),
        }
    }

    /// Chunk `i`, every column: borrowed in memory, read from disk otherwise.
    pub(crate) fn chunk(&self, i: usize) -> Result<std::borrow::Cow<'_, ArrowBatch>> {
        match &self.chunks {
            Chunks::Mem(b) => Ok(std::borrow::Cow::Borrowed(b)),
            Chunks::Disk(path, at) => {
                use std::io::{Read as _, Seek as _};
                let io = |e: std::io::Error| Error::Transfer(format!("sql: stage file {}: {e}", path.display()));
                let (off, len) = at[i];
                let mut file = std::fs::File::open(path).map_err(io)?;
                let mut buf = vec![0u8; len as usize];
                file.seek(std::io::SeekFrom::Start(off)).and_then(|_| file.read_exact(&mut buf)).map_err(io)?;
                let all: Vec<usize> = (0..self.schema.len()).collect();
                let b = decode(&buf, &all)?;
                drop_cache(&file, off + len);
                Ok(std::borrow::Cow::Owned(b))
            }
        }
    }

    /// The table as a read stream, a chunk at a time: a query's answer handed
    /// on without its rows in memory at once (`sql::run_stream`).
    pub(crate) fn into_stream(self) -> Result<crate::read::ReadHandle> {
        let run = Run::new();
        run.put("answer", self);
        // The stream holds the table; the run's name goes with `run`.
        start(&run.url, "answer", &crate::read::ReadOptions::default())
    }

    /// An empty table's columns, no rows (`None` past zero rows).
    pub(crate) fn empty_batch(&self) -> Option<ArrowBatch> {
        match &self.chunks {
            Chunks::Mem(b) if b.rows == 0 => Some(ArrowBatch { rows: 0, cols: b.cols.clone() }),
            _ => None,
        }
    }

    /// Each chunk in order, every column, read one at a time.
    pub(crate) fn for_each(&self, f: &mut dyn FnMut(ArrowBatch) -> Result<()>) -> Result<()> {
        match &self.chunks {
            Chunks::Mem(b) if b.rows == 0 => Ok(()),
            Chunks::Mem(b) => f(ArrowBatch { rows: b.rows, cols: b.cols.clone() }),
            Chunks::Disk(path, at) => {
                use std::io::{Read as _, Seek as _};
                let io = |e: std::io::Error| Error::Transfer(format!("sql: stage file {}: {e}", path.display()));
                let mut file = std::fs::File::open(path).map_err(io)?;
                let all: Vec<usize> = (0..self.schema.len()).collect();
                let mut buf = Vec::new();
                let mut read = 0u64;
                for &(off, len) in at {
                    buf.resize(len as usize, 0);
                    file.seek(std::io::SeekFrom::Start(off)).and_then(|_| file.read_exact(&mut buf)).map_err(io)?;
                    f(decode(&buf, &all)?)?;
                    read += len;
                    if read >= WRITE_OUT {
                        drop_cache(&file, off + len);
                        read = 0;
                    }
                }
                if let Some(&(off, len)) = at.last() {
                    drop_cache(&file, off + len);
                }
                Ok(())
            }
        }
    }

    /// The whole stage as one batch (a final answer the caller asked for).
    pub(crate) fn into_batch(mut self) -> Result<ArrowBatch> {
        if let Chunks::Mem(b) = &mut self.chunks {
            return Ok(std::mem::replace(b, ArrowBatch { rows: 0, cols: Vec::new() }));
        }
        let mut parts = Vec::new();
        self.for_each(&mut |b| {
            parts.push(b);
            Ok(())
        })?;
        concat_all(&parts.iter().collect::<Vec<_>>())
    }
}

/// A stage written a batch at a time (a query answered in parts, a LEFT
/// JOIN's two halves): in memory up to [`stage_budget`], then chunks on
/// disk, so the whole never sits in memory (TPC-H Q13 10g seed: 9.9M groups
/// in 16 parts died concatenating them).
pub(crate) struct Writer {
    schema: Vec<ArrowField>,
    rows: usize,
    mem: Vec<ArrowBatch>,
    mem_bytes: u64,
    budget: u64,
    like: Option<Vec<FinishedCol>>,
    disk: Option<Spill>,
}

struct Spill {
    path: std::path::PathBuf,
    file: std::io::BufWriter<std::fs::File>,
    at: Vec<(u64, u64)>,
    off: u64,
    buf: Vec<u8>,
    /// Bytes written since the last [`write_out`].
    since: u64,
}

/// Bytes a stage file takes before its pages are written out and dropped.
const WRITE_OUT: u64 = 16 << 20;

/// A file's written pages to disk now, and out of the page cache: the cache
/// counts against the cgroup (TPC-H Q13 on the 10g seed spilled 69M orders:
/// cgroup 260 MB of 256, 213 MB of it page cache, 108 MB dirty, beside
/// 167 MB anonymous). A data-only range flush, no journal commit; the same
/// recipe as a view's bulk files (`view::start_writeback`).
/// A failed writeback (EIO, ENOSPC) is returned: the pages it lost would
/// read back as zeros.
fn write_out(f: &std::fs::File) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: a valid open descriptor; the calls only do I/O and advise.
        let r = unsafe {
            libc::sync_file_range(
                f.as_raw_fd(),
                0,
                0,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
        if r != 0 {
            let e = std::io::Error::last_os_error();
            // A file system without the call (EINVAL, ENOSYS, ESPIPE,
            // EOPNOTSUPP) is no failure: the pages stay to the kernel.
            if !matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS | libc::ESPIPE | libc::EOPNOTSUPP)) {
                return Err(e);
            }
        }
        // SAFETY: as above; advice only.
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
    Ok(())
}

/// A file's pages before `end` out of the page cache once read (advice
/// only): behind the read position, so the readahead ahead of it stays.
fn drop_cache(f: &std::fs::File, end: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: a valid open descriptor; advice only.
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, end as libc::off_t, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (f, end);
}

impl Drop for Writer {
    fn drop(&mut self) {
        held_sub(std::mem::take(&mut self.mem_bytes));
        if let Some(s) = &self.disk {
            let _ = std::fs::remove_file(&s.path);
        }
    }
}

impl Writer {
    pub(crate) fn new(schema: Vec<ArrowField>) -> Writer {
        Writer { schema, rows: 0, mem: Vec::new(), mem_bytes: 0, budget: stage_budget(), like: None, disk: None }
    }

    /// Straight to disk (a join's spilled probe rows, several at once).
    pub(crate) fn on_disk(schema: Vec<ArrowField>) -> Writer {
        let mut w = Writer::new(schema);
        w.budget = 0;
        w
    }

    /// Empty columns of the variants written so far (`None` before any).
    pub(crate) fn like(&self) -> Option<&[FinishedCol]> {
        self.like.as_deref()
    }

    pub(crate) fn push(&mut self, b: ArrowBatch) -> Result<()> {
        if b.cols.len() != self.schema.len() {
            return Err(Error::InvalidInput("sql: internal: a stage part differs in columns".into()));
        }
        match &self.like {
            None => self.like = Some(b.cols.iter().map(empty_like).collect()),
            // Every part of one kind per column: a chunk of another would be
            // concatenated or read back as the first's.
            Some(like) if like.iter().zip(&b.cols).any(|(l, c)| std::mem::discriminant(l) != std::mem::discriminant(c)) => {
                return Err(Error::InvalidInput("sql: internal: a stage part differs in column kinds".into()));
            }
            Some(_) if b.rows == 0 => return Ok(()),
            Some(_) => {}
        }
        self.rows += b.rows;
        if self.disk.is_none() {
            let n = b.bytes() as u64;
            self.mem_bytes += n;
            let empty = b.rows == 0;
            self.mem.push(b);
            // A part of no rows stays: on disk it would leave no chunk, and
            // the stage no columns.
            if held_add(n) <= self.budget || empty {
                return Ok(());
            }
            // A name no other process or run can hold (a container's pid 1
            // is every container's), created new: never truncating, never
            // through a link someone left at that path.
            use std::hash::{BuildHasher as _, Hasher as _};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u64(self.rows as u64);
            let path = std::env::temp_dir().join(format!("apitap-stage-{}-{:016x}", std::process::id(), h.finish()));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| Error::Transfer(format!("sql: stage file {}: {e}", path.display())))?;
            self.disk = Some(Spill { path, file: std::io::BufWriter::new(file), at: Vec::new(), off: 0, buf: Vec::new(), since: 0 });
            held_sub(std::mem::take(&mut self.mem_bytes));
            for b in std::mem::take(&mut self.mem) {
                self.write(&b)?;
            }
            return Ok(());
        }
        self.write(&b)
    }

    fn write(&mut self, b: &ArrowBatch) -> Result<()> {
        use std::io::Write as _;
        let s = self.disk.as_mut().expect("spilling");
        let io = |e: std::io::Error| Error::Transfer(format!("sql: stage file {}: {e}", s.path.display()));
        // At most CHUNK rows and about CHUNK_BYTES a chunk: a reader decodes
        // one chunk at a time, and 64K rows of 5 KB text were 320 MB.
        let per_row = (b.bytes() / b.rows.max(1)).max(1);
        let rows_cap = (CHUNK_BYTES / per_row).clamp(1, CHUNK);
        let mut r = 0;
        while r < b.rows {
            let n = rows_cap.min(b.rows - r);
            s.buf.clear();
            if r == 0 && n == b.rows {
                encode(b, &mut s.buf);
            } else {
                let idx: Vec<usize> = (r..r + n).collect();
                encode(&take_rows(b, &idx), &mut s.buf);
            }
            s.file.write_all(&s.buf).map_err(io)?;
            s.at.push((s.off, s.buf.len() as u64));
            s.off += s.buf.len() as u64;
            s.since += s.buf.len() as u64;
            if s.since >= WRITE_OUT {
                s.file.flush().map_err(io)?;
                write_out(s.file.get_ref()).map_err(io)?;
                s.since = 0;
            }
            r += n;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<MemTable> {
        let schema = std::mem::take(&mut self.schema);
        let rows = self.rows;
        if let Some(mut s) = self.disk.take() {
            use std::io::Write as _;
            if let Err(e) = s.file.flush().and_then(|()| write_out(s.file.get_ref())) {
                let _ = std::fs::remove_file(&s.path);
                return Err(Error::Transfer(format!("sql: stage file {}: {e}", s.path.display())));
            }
            return Ok(MemTable { schema, rows, chunks: Chunks::Disk(s.path, s.at), held: 0 });
        }
        let parts = std::mem::take(&mut self.mem);
        let batch = match parts.len() {
            0 => ArrowBatch { rows: 0, cols: self.like.take().unwrap_or_default() },
            1 => parts.into_iter().next().expect("one part"),
            _ => concat_all(&parts.iter().collect::<Vec<_>>())?,
        };
        let held = std::mem::take(&mut self.mem_bytes);
        Ok(MemTable { schema, rows, chunks: Chunks::Mem(batch), held })
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
    // A chunk read back wrong (pages lost, zeros) must fail here, not index
    // past its columns: every column takes a byte at least.
    if ncols > r.len() || want.iter().any(|&c| c >= ncols) {
        return Err(Error::Transfer(format!("sql: a stage chunk of {ncols} columns read for column(s) {want:?}")));
    }
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
        let v = r.get(*i..i.checked_add(n).ok_or_else(short)?).ok_or_else(short)?.to_vec();
        *i += n;
        Ok(Some(v))
    }
    macro_rules! vec_of {
        ($t:ty, $w:expr, $i:ident) => {{
            let n = u64::from_le_bytes(r.get($i..$i + 8).ok_or_else(short)?.try_into().expect("8")) as usize;
            $i += 8;
            let len = n.checked_mul($w).ok_or_else(short)?;
            let b = r.get($i..$i.checked_add(len).ok_or_else(short)?).ok_or_else(short)?;
            $i += len;
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
                // The page cache dropped behind the read every 16 MiB, as
                // `for_each` does: a large answer streamed out is read once.
                let mut read = 0u64;
                for &(off, len) in at {
                    let mut buf = vec![0u8; len as usize];
                    let r = f
                        .seek(std::io::SeekFrom::Start(off))
                        .and_then(|_| f.read_exact(&mut buf))
                        .map_err(|e| Error::Transfer(format!("sql: stage file {}: {e}", path.display())))
                        .and_then(|_| decode(&buf, &idx));
                    read += len;
                    if read >= WRITE_OUT {
                        drop_cache(&f, off + len);
                        read = 0;
                    }
                    let stop = r.is_err();
                    if tx.send(r).await.is_err() || stop {
                        return;
                    }
                }
                if let Some(&(off, len)) = at.last() {
                    drop_cache(&f, off + len);
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
/// Row `r` of `b` repeated `counts[r]` times, in order, at most `left` rows
/// in all (counted down), handed to `f` in pieces of at most [`CHUNK`]
/// rows: a plain SELECT's rows rebuilt from its groups' counts never sit in
/// memory whole (TPC-H lineitem's 60M rows from 4 (flag, status) groups at
/// 10g seed were OOM-killed in a 256 MB cage).
pub(crate) fn repeat_chunks(b: &ArrowBatch, counts: &[usize], left: &mut Option<usize>, f: &mut dyn FnMut(ArrowBatch) -> Result<()>) -> Result<()> {
    let fresh = || -> Vec<FinishedCol> { b.cols.iter().map(empty_like).collect() };
    let mut cols = fresh();
    let mut rows = 0usize;
    // The bytes already in the piece: rows of other widths before this one.
    let mut held = 0usize;
    for (r, &n) in counts.iter().enumerate() {
        let mut n = left.map_or(n, |l| n.min(l));
        if let Some(l) = left.as_mut() {
            *l -= n;
        }
        // The row's width, so a piece stays near CHUNK_BYTES (and its i32
        // text offsets far from wrapping).
        let width: usize = b
            .cols
            .iter()
            .map(|c| match c {
                FinishedCol::Utf8 { offsets, .. } | FinishedCol::Bin { offsets, .. } => (offsets[r + 1] - offsets[r]) as usize + 4,
                _ => 16,
            })
            .sum::<usize>()
            .max(1);
        while n > 0 {
            let room = (CHUNK_BYTES.saturating_sub(held) / width).max(1);
            let take = n.min(CHUNK - rows).min(room);
            for (o, c) in cols.iter_mut().zip(&b.cols) {
                let mut at = rows;
                push_rep(o, c, r, take, &mut at);
            }
            rows += take;
            held += take * width;
            n -= take;
            if rows == CHUNK || held >= CHUNK_BYTES {
                let mut full = std::mem::replace(&mut cols, fresh());
                tidy(&mut full, rows);
                f(ArrowBatch { rows, cols: full })?;
                rows = 0;
                held = 0;
            }
        }
        if *left == Some(0) {
            break;
        }
    }
    if rows > 0 {
        tidy(&mut cols, rows);
        f(ArrowBatch { rows, cols })?;
    }
    Ok(())
}

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
    tidy(&mut cols, rows);
    ArrowBatch { rows, cols }
}

/// Bitmaps sized to `rows` (validity `None` when every row is valid).
fn tidy(cols: &mut [FinishedCol], rows: usize) {
    for c in cols.iter_mut() {
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
    for c in cols.iter_mut() {
        if let FinishedCol::Bool { data, .. } = c {
            data.resize(rows.div_ceil(8), 0);
        }
    }
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

/// The batches' rows one after another (the same column variants).
pub(crate) fn concat_all(bs: &[&ArrowBatch]) -> Result<ArrowBatch> {
    let Some(first) = bs.first() else { return Err(Error::InvalidInput("sql: internal: nothing to concatenate".into())) };
    let mut cols: Vec<FinishedCol> = first.cols.iter().map(empty_like).collect();
    // Text and binary offsets are i32: past 2 GiB in one column they wrap.
    for c in 0..cols.len() {
        let total: usize = bs
            .iter()
            .map(|b| match b.cols.get(c) {
                Some(FinishedCol::Utf8 { data, .. } | FinishedCol::Bin { data, .. }) => data.len(),
                _ => 0,
            })
            .sum();
        if total > i32::MAX as usize {
            return Err(Error::InvalidInput(format!("sql: a text column of {total} bytes in one result (past 2 GiB); narrow the query")));
        }
    }
    let mut rows = 0usize;
    for b in bs {
        if b.cols.len() != cols.len() {
            return Err(Error::InvalidInput("sql: internal: concatenated parts differ in columns".into()));
        }
        for (o, c) in cols.iter_mut().zip(&b.cols) {
            if std::mem::discriminant(o) != std::mem::discriminant(c) {
                return Err(Error::InvalidInput("sql: internal: concatenated parts differ in column types".into()));
            }
            let mut at = rows;
            for r in 0..b.rows {
                push_rep(o, c, r, 1, &mut at);
            }
        }
        rows += b.rows;
    }
    tidy(&mut cols, rows);
    Ok(ArrowBatch { rows, cols })
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

    fn part(from: i64, n: usize) -> ArrowBatch {
        let keys: Vec<i64> = (from..from + n as i64).collect();
        let mut off = vec![0i32];
        let mut data = Vec::new();
        for k in &keys {
            data.extend_from_slice(format!("k{k}").as_bytes());
            off.push(data.len() as i32);
        }
        ArrowBatch { rows: n, cols: vec![FinishedCol::I64 { validity: None, data: keys }, FinishedCol::Utf8 { validity: None, offsets: off, data }] }
    }

    fn kt_schema() -> Vec<ArrowField> {
        vec![
            ArrowField { name: "k".into(), kind: crate::wire::arrowcol::ArrowKind::Int64, nullable: false, rewritten: false, padded: false, ci: false, ai: false },
            ArrowField { name: "t".into(), kind: crate::wire::arrowcol::ArrowKind::Utf8, nullable: false, rewritten: false, padded: false, ci: false, ai: false },
        ]
    }

    /// The stage budget bounds every stage held in memory together: a
    /// second stage that fits the budget alone goes to disk beside a first
    /// one still held (other tests' stages only add to the count).
    #[test]
    fn stage_budget_is_shared_by_the_stages_held() {
        let one = part(0, 1000).bytes() as u64;
        let mut w1 = Writer::new(kt_schema());
        w1.budget = one * 3 / 2;
        w1.push(part(0, 1000)).unwrap();
        let t1 = w1.finish().unwrap();
        let mut w2 = Writer::new(kt_schema());
        w2.budget = one * 3 / 2;
        w2.push(part(0, 1000)).unwrap();
        let t2 = w2.finish().unwrap();
        // Only when the first stayed in memory (another test's stages may
        // have sent it to disk, and gone before the second was written).
        if matches!(&t1.chunks, Chunks::Mem(_)) {
            assert!(matches!(&t2.chunks, Chunks::Disk(..)), "the second stage spilled: the first still holds its bytes");
        }
        drop((t1, t2));
    }

    /// A chunk on disk holds about CHUNK_BYTES, not 64K rows of any width.
    #[test]
    fn stage_chunks_are_bounded_by_bytes() {
        let wide = "x".repeat(4096);
        let n = 5000;
        let mut off = vec![0i32];
        let mut data = Vec::new();
        for _ in 0..n {
            data.extend_from_slice(wide.as_bytes());
            off.push(data.len() as i32);
        }
        let b = ArrowBatch { rows: n, cols: vec![FinishedCol::I64 { validity: None, data: (0..n as i64).collect() }, FinishedCol::Utf8 { validity: None, offsets: off, data }] };
        let mut w = Writer::on_disk(kt_schema());
        w.push(b).unwrap();
        let t = w.finish().unwrap();
        let Chunks::Disk(_, at) = &t.chunks else { panic!("on disk") };
        assert!(at.len() >= 2, "20 MB of rows in more than one chunk");
        assert!(at.iter().all(|&(_, len)| len <= (CHUNK_BYTES as u64) * 2), "each chunk near CHUNK_BYTES");
        let mut rows = 0;
        t.for_each(&mut |x| {
            rows += x.rows;
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, n);
    }

    /// A disk-backed answer streamed out: a consumer that stops after the
    /// first batch leaves no stage file and no task behind; one whose file
    /// goes away mid-stream gets an error, not a hang.
    #[tokio::test]
    async fn stream_stops_and_errors_clean_up() {
        let disk_table = || {
            let mut w = Writer::on_disk(kt_schema());
            for i in 0..4 {
                w.push(part(i * 70000, 70000)).unwrap();
            }
            w.finish().unwrap()
        };
        let path_of = |t: &MemTable| match &t.chunks {
            Chunks::Disk(p, _) => p.clone(),
            Chunks::Mem(_) => panic!("on disk"),
        };
        // Stops after the first batch.
        let t = disk_table();
        let path = path_of(&t);
        let mut h = t.into_stream().unwrap();
        assert!(h.recv().await.unwrap().is_some());
        drop(h);
        for _ in 0..50 {
            if !path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(!path.exists(), "the stage file goes when the consumer stops");
        // The file is cut short mid-stream: an error arrives, no hang.
        let t = disk_table();
        let path = path_of(&t);
        let mut h = t.into_stream().unwrap();
        assert!(h.recv().await.unwrap().is_some());
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();
        let mut errored = false;
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(5), h.recv()).await.expect("no hang") {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    errored = true;
                    break;
                }
            }
        }
        assert!(errored, "a cut stage file is an error to the consumer");
        drop(h);
        for _ in 0..50 {
            if !path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(!path.exists(), "the stage file goes after an error too");
    }

    /// A table read by `chunk(i)` gives the rows `for_each` gives, in order,
    /// from disk or from memory.
    #[test]
    fn chunks_read_back_as_written() {
        for on_disk in [true, false] {
            let mut w = if on_disk { Writer::on_disk(kt_schema()) } else { Writer::new(kt_schema()) };
            if !on_disk {
                w.budget = u64::MAX;
            }
            for p in [part(0, 70000), part(70000, 5)] {
                w.push(p).unwrap();
            }
            let t = w.finish().unwrap();
            assert_eq!(t.in_memory(), !on_disk);
            let mut a = Vec::new();
            for i in 0..t.n_chunks() {
                a.push(t.chunk(i).unwrap().into_owned());
            }
            let mut b = Vec::new();
            t.for_each(&mut |x| {
                b.push(x);
                Ok(())
            })
            .unwrap();
            let a = concat_all(&a.iter().collect::<Vec<_>>()).unwrap();
            let b = concat_all(&b.iter().collect::<Vec<_>>()).unwrap();
            assert_eq!(a, b, "on disk: {on_disk}");
            assert_eq!(a.rows, 70005);
        }
    }

    /// Repeated rows come in pieces bounded by rows and bytes, in order, and
    /// the limit counts across pieces.
    #[test]
    fn repeat_chunks_streams_in_bounded_pieces() {
        let b = part(0, 2);
        let mut pieces = Vec::new();
        let mut left = None;
        repeat_chunks(&b, &[100_000, 3], &mut left, &mut |x| {
            pieces.push(x);
            Ok(())
        })
        .unwrap();
        assert!(pieces.len() >= 2 && pieces.iter().all(|p| p.rows <= CHUNK));
        assert_eq!(pieces.iter().map(|p| p.rows).sum::<usize>(), 100_003);
        let all = concat_all(&pieces.iter().collect::<Vec<_>>()).unwrap();
        assert_eq!(all, repeat(&b, &[100_000, 3], None));
        let mut left = Some(5);
        let mut n = 0;
        repeat_chunks(&b, &[3, 3], &mut left, &mut |x| {
            n += x.rows;
            Ok(())
        })
        .unwrap();
        assert_eq!((n, left), (5, Some(0)));
        // NULLs keep their place through the pieces.
        let nb = ArrowBatch {
            rows: 2,
            cols: vec![
                FinishedCol::I64 { validity: Some(vec![0b10]), data: vec![0, 7] },
                FinishedCol::Utf8 { validity: Some(vec![0b01]), offsets: vec![0, 1, 1], data: b"a".to_vec() },
            ],
        };
        let mut pieces = Vec::new();
        let mut left = None;
        repeat_chunks(&nb, &[70_000, 2], &mut left, &mut |x| {
            pieces.push(x);
            Ok(())
        })
        .unwrap();
        let all = concat_all(&pieces.iter().collect::<Vec<_>>()).unwrap();
        assert_eq!(all, repeat(&nb, &[70_000, 2], None), "NULLs placed as one repeat places them");
    }

    /// Parts written past the budget land on disk and read back as the
    /// rows written, in order; under it they stay one batch in memory.
    #[test]
    fn stage_writer_spills_parts_in_order() {
        let schema = vec![
            ArrowField { name: "k".into(), kind: crate::wire::arrowcol::ArrowKind::Int64, nullable: false, rewritten: false, padded: false, ci: false, ai: false },
            ArrowField { name: "t".into(), kind: crate::wire::arrowcol::ArrowKind::Utf8, nullable: false, rewritten: false, padded: false, ci: false, ai: false },
        ];
        let want = concat_all(&[&part(0, 3), &part(3, 0), &part(3, 70000), &part(70003, 5)]).unwrap();
        for budget in [u64::MAX, 1024] {
            let mut w = Writer::new(schema.clone());
            w.budget = budget;
            for p in [part(0, 3), part(3, 0), part(3, 70000), part(70003, 5)] {
                w.push(p).unwrap();
            }
            assert_eq!(w.like().unwrap().len(), 2);
            let t = w.finish().unwrap();
            assert_eq!(t.rows(), 70008);
            let disk = matches!(&t.chunks, Chunks::Disk(..));
            assert_eq!(disk, budget == 1024, "budget {budget}");
            let path = match &t.chunks {
                Chunks::Disk(p, _) => Some(p.clone()),
                Chunks::Mem(_) => None,
            };
            let mut n = 0;
            t.for_each(&mut |b| {
                n += b.rows;
                Ok(())
            })
            .unwrap();
            assert_eq!(n, 70008);
            assert_eq!(t.into_batch().unwrap(), want, "budget {budget}");
            if let Some(p) = path {
                assert!(!p.exists(), "the stage file goes with its table");
            }
        }
    }

    fn kt_empty() -> ArrowBatch {
        ArrowBatch { rows: 0, cols: vec![FinishedCol::I64 { validity: None, data: vec![] }, FinishedCol::Utf8 { validity: None, offsets: vec![0], data: vec![] }] }
    }

    /// C7: a stage of no rows keeps its columns even when the stages held
    /// together are past the budget (a 0-row batch never goes to disk).
    #[test]
    fn a_stage_of_no_rows_keeps_its_columns_past_the_budget() {
        held_add(1);
        let mut w = Writer::on_disk(kt_schema());
        let r = w.push(kt_empty());
        held_sub(1);
        r.unwrap();
        let b = w.finish().unwrap().into_batch().unwrap();
        assert_eq!((b.rows, b.cols.len()), (0, 2), "the columns of the stage");
    }

    /// C8: a chunk asked for a column it does not hold is an error, not a
    /// panic.
    #[test]
    fn a_chunk_asked_for_a_column_past_its_own_is_an_error() {
        let b = ArrowBatch { rows: 1, cols: vec![FinishedCol::I64 { validity: None, data: vec![7] }] };
        let mut w = Vec::new();
        encode(&b, &mut w);
        let r = std::panic::catch_unwind(|| decode(&w, &[3]));
        assert!(matches!(r, Ok(Err(_))), "an error, not a panic or a batch");
        // A zeroed chunk (pages never written back) reads as an error too.
        let r = std::panic::catch_unwind(|| decode(&vec![0u8; 64], &[0]));
        assert!(matches!(r, Ok(Err(_))), "zeros read as an error");
    }

    /// C9: a piece of repeated rows stays near CHUNK_BYTES counting every
    /// row already in it, not the current row's width alone.
    #[test]
    fn repeat_pieces_count_the_bytes_already_in_them() {
        let wide = "w".repeat(4 << 20);
        let narrow = "n".repeat(100);
        let mut data = wide.clone().into_bytes();
        data.extend_from_slice(narrow.as_bytes());
        let b = ArrowBatch {
            rows: 2,
            cols: vec![FinishedCol::I64 { validity: None, data: vec![1, 2] }, FinishedCol::Utf8 { validity: None, offsets: vec![0, wide.len() as i32, (wide.len() + narrow.len()) as i32], data }],
        };
        let mut left = None;
        let mut sizes = Vec::new();
        // The bytes the piece's rows hold (lengths; `bytes()` counts the
        // buffers' capacities, whose growth slack is not the rows' width).
        let held = |x: &ArrowBatch| -> usize {
            x.cols
                .iter()
                .map(|c| match c {
                    FinishedCol::I64 { data, .. } => data.len() * 8,
                    FinishedCol::Utf8 { offsets, data, .. } => offsets.len() * 4 + data.len(),
                    _ => 0,
                })
                .sum()
        };
        repeat_chunks(&b, &[1, 200_000], &mut left, &mut |x| {
            sizes.push((x.rows, held(&x)));
            Ok(())
        })
        .unwrap();
        assert_eq!(sizes.iter().map(|s| s.0).sum::<usize>(), 200_001);
        for &(rows, bytes) in &sizes {
            assert!(rows == 1 || bytes <= CHUNK_BYTES + (64 << 10), "a piece of {rows} rows holds {bytes} B");
        }
    }

    /// C10: a batch whose column kinds differ from the first one's is
    /// refused at the push, in memory as on disk.
    #[test]
    fn a_batch_of_other_column_kinds_is_refused() {
        let mut w = Writer::new(kt_schema());
        w.push(ArrowBatch { rows: 1, cols: vec![FinishedCol::I64 { validity: None, data: vec![1] }, FinishedCol::Utf8 { validity: None, offsets: vec![0, 1], data: b"a".to_vec() }] }).unwrap();
        let r = w.push(ArrowBatch { rows: 1, cols: vec![FinishedCol::Utf8 { validity: None, offsets: vec![0, 1], data: b"b".to_vec() }, FinishedCol::Utf8 { validity: None, offsets: vec![0, 1], data: b"a".to_vec() }] });
        assert!(r.is_err(), "column kinds checked per push");
    }
}
