//! Incremental views (idea 7, C1 + C2): the answer of an `apitap.sql()`
//! query kept current by moving it with each source change, never by
//! recomputing it (docs/design/2026-11-05-sql-engine-and-memory.md, "C").
//!
//! The state is the eager-aggregate layout from the RisingWave research:
//!
//! - `R[g]`: the answer's accumulators per group, and the rows behind each;
//! - on a join, `G[k]`: the GROUP BY side's group per join key, and `S[k]`:
//!   the other side's contribution per join key (its rows and accumulators),
//!   kept for every key, matched or not, because a later GROUP BY-side insert
//!   may match it.
//!
//! Every change is a signed row image: an insert is `+after`, a delete is
//! `-before`, an update is `-before` then `+after`. Each side's WHERE is
//! re-checked on each image, so a row that crosses its filter leaves or
//! enters the answer. The bootstrap is the same path: every current row is
//! an insert, so a first run and a later window cannot disagree on how a row
//! counts.
//!
//! Only invertible aggregates are maintained: `SUM`, `COUNT`, `COUNT(*)` and
//! `AVG` (as sum and count). `MIN` and `MAX` are refused: a delete of the
//! minimum needs the rest of its group. Integer and decimal sums are exact;
//! a `DOUBLE` sum moves by float additions and may drift from a recompute in
//! its last bits.
//!
//! The state file carries the query's fingerprint, the source positions it
//! reflects, and a SHA-256 of its whole body as a trailer. A file whose
//! trailer, fingerprint or version does not match is refused, never trusted:
//! the caller rebuilds the state by a full first run.

use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"APTVIEW3";
const VERSION: u32 = 3;
const BUCKET_MAGIC: &[u8; 8] = b"APTVBKT4";
const OVERLAY_MAGIC: &[u8; 8] = b"APTVOVL1";
/// The bytes of records one hashed block of a bucket holds: the unit a
/// window that touches a few keys reads instead of the whole bucket.
const BLOCK: usize = 4096;
/// Overlays a bucket stacks before a window compacts it into a new base.
const MAX_OVERLAYS: usize = 32;

/// One file of a bucket: its name in the state directory, the hash the
/// manifest checks it by, and its size.
#[derive(Clone, Debug)]
struct BFile {
    name: String,
    hash: [u8; 32],
    bytes: u64,
}

/// A key's whole state as an overlay holds it: `G[k]` and `S[k]`, each
/// absent when the key has none (a tombstone over the base).
struct Entry {
    k: i64,
    g: Option<u32>,
    s: Option<(i64, Vec<IAcc>)>,
}

/// One section of a base bucket: `count` fixed-width records of `rec` bytes
/// from byte `start`, `per` to a block, each block's first key and hash.
struct Sec {
    count: u64,
    per: u64,
    rec: u64,
    /// The section's layout: `G`, the group's width; `S`, one byte per
    /// aggregate (see [`s_layout`]).
    lay: Vec<u8>,
    start: u64,
    blocks: Vec<(i64, u32)>,
}

/// The bytes one bucket of `G`/`S` may hold in memory while a window or the
/// bootstrap moves it: `APITAP_SQL_VIEW_BUCKET_BYTES` (a test lever), else a
/// fifth of the memory limit, else 256 MiB. A bucket past it is split.
pub(super) fn bucket_budget() -> u64 {
    if let Some(b) = std::env::var("APITAP_SQL_VIEW_BUCKET_BYTES")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .filter(|b| *b > 0)
    {
        return b;
    }
    crate::pipeline::mem_limit_bytes().map_or(256 << 20, |m| m / 5)
}

/// One move of the state, once a row's WHERE, key and group are known.
#[derive(Clone, Debug)]
enum Contrib {
    /// One table, no join: a row of group `g`.
    R { g: u32, vals: Vec<Num>, w: i64 },
    /// The GROUP BY side's row at `key`, in group `g`.
    G { key: i64, g: u32, w: i64 },
    /// The other side's row at `key`.
    S { key: i64, vals: Vec<Num>, w: i64 },
}

impl Contrib {
    fn key(&self) -> Result<i64> {
        match self {
            Contrib::G { key, .. } | Contrib::S { key, .. } => Ok(*key),
            Contrib::R { .. } => Err(bad("a one-table move routed to a join store")),
        }
    }

    /// What one pending change really holds: its slot in a bucket's vector,
    /// counted twice for the vector's doubling slack, plus the values' own
    /// allocation and malloc's header. A 32-byte guess let the bootstrap's
    /// pending window grow ~2.5x past the budget and the 256 MiB cage died.
    fn bytes(&self) -> u64 {
        let slot = 2 * std::mem::size_of::<Contrib>() as u64;
        match self {
            Contrib::G { .. } => slot,
            Contrib::R { vals, .. } | Contrib::S { vals, .. } => {
                slot + 16 + (vals.capacity() * std::mem::size_of::<Num>()) as u64
            }
        }
    }

    fn write(&self, w: &mut impl Write) -> std::io::Result<()> {
        match self {
            Contrib::G { key, g, w: s } => {
                w.write_all(&[1])?;
                w.write_all(&key.to_le_bytes())?;
                w.write_all(&s.to_le_bytes())?;
                w.write_all(&g.to_le_bytes())
            }
            Contrib::S { key, vals, w: s } => {
                w.write_all(&[2])?;
                w.write_all(&key.to_le_bytes())?;
                w.write_all(&s.to_le_bytes())?;
                w.write_all(&(vals.len() as u16).to_le_bytes())?;
                for v in vals {
                    match v {
                        Num::Null => w.write_all(&[0])?,
                        Num::Int(x) => {
                            w.write_all(&[1])?;
                            w.write_all(&x.to_le_bytes())?
                        }
                        Num::Dec(x) => {
                            w.write_all(&[2])?;
                            w.write_all(&x.to_le_bytes())?
                        }
                        Num::Float(x) => {
                            w.write_all(&[3])?;
                            w.write_all(&x.to_bits().to_le_bytes())?
                        }
                    }
                }
                Ok(())
            }
            Contrib::R { .. } => Err(std::io::Error::other("a one-table move is never logged")),
        }
    }

    /// The next record of a log, `None` at its end.
    fn read(r: &mut impl Read) -> std::io::Result<Option<Contrib>> {
        let mut tag = [0u8; 1];
        match r.read_exact(&mut tag) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let mut b8 = [0u8; 8];
        r.read_exact(&mut b8)?;
        let key = i64::from_le_bytes(b8);
        r.read_exact(&mut b8)?;
        let w = i64::from_le_bytes(b8);
        Ok(Some(match tag[0] {
            1 => {
                let mut b4 = [0u8; 4];
                r.read_exact(&mut b4)?;
                Contrib::G { key, g: u32::from_le_bytes(b4), w }
            }
            2 => {
                let mut b2 = [0u8; 2];
                r.read_exact(&mut b2)?;
                let n = u16::from_le_bytes(b2) as usize;
                let mut vals = Vec::with_capacity(n);
                for _ in 0..n {
                    r.read_exact(&mut tag)?;
                    vals.push(match tag[0] {
                        0 => Num::Null,
                        1 => {
                            r.read_exact(&mut b8)?;
                            Num::Int(i64::from_le_bytes(b8))
                        }
                        2 => {
                            let mut b16 = [0u8; 16];
                            r.read_exact(&mut b16)?;
                            Num::Dec(i128::from_le_bytes(b16))
                        }
                        _ => {
                            r.read_exact(&mut b8)?;
                            Num::Float(f64::from_bits(u64::from_le_bytes(b8)))
                        }
                    });
                }
                Contrib::S { key, vals, w }
            }
            _ => return Err(std::io::Error::other("a view log record of an unknown kind")),
        }))
    }
}

/// `G` and `S` on local disk, cut by key into buckets (a join view). A bucket
/// is loaded, moved by its pending changes and written back one at a time,
/// so memory holds one bucket and the pending window whatever the state's
/// size; the bootstrap's changes are logged per bucket on disk first.
struct Store {
    dir: PathBuf,
    /// Bucket `i` holds the keys in [splits[i-1], splits[i]), open at both ends.
    splits: Vec<i64>,
    /// Each bucket's base file (sorted, block-indexed), `None` while it has none.
    files: Vec<Option<BFile>>,
    /// Each bucket's overlays over its base, oldest first: the touched keys'
    /// whole state after a window that moved only a few of them.
    ovls: Vec<Vec<BFile>>,
    seq: u64,
    budget: u64,
    /// Record bytes per hashed block in the bases this store writes.
    block: usize,
    pend: Vec<Vec<Contrib>>,
    pend_bytes: u64,
    /// Each bucket's log of spilled changes, appended to and closed per
    /// spill (thousands of small buckets would hold more files open than
    /// a process may).
    logs: Vec<Option<PathBuf>>,
    /// Changes each bucket's log holds.
    logn: Vec<u64>,
    dirty: Vec<bool>,
    /// Buckets moved by an overlay and by a whole rewrite, since loaded.
    sparse: usize,
    full: usize,
    /// Files the next manifest no longer names, removed once it is written.
    stale: Vec<String>,
}

impl Store {
    fn fresh(dir: PathBuf, splits: Vec<i64>) -> Result<Store> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| Error::Transfer(format!("view state {}: {e}", dir.display())))?;
        let n = splits.len() + 1;
        Ok(Store::with(dir, splits, vec![None; n], vec![Vec::new(); n], 0))
    }

    fn with(dir: PathBuf, splits: Vec<i64>, files: Vec<Option<BFile>>, ovls: Vec<Vec<BFile>>, seq: u64) -> Store {
        let n = files.len();
        Store {
            dir,
            splits,
            files,
            ovls,
            seq,
            budget: bucket_budget(),
            block: BLOCK,
            pend: (0..n).map(|_| Vec::new()).collect(),
            pend_bytes: 0,
            logs: (0..n).map(|_| None).collect(),
            logn: vec![0; n],
            dirty: vec![false; n],
            sparse: 0,
            full: 0,
            stale: Vec::new(),
        }
    }

    fn bucket_of(&self, k: i64) -> usize {
        self.splits.partition_point(|&s| s <= k)
    }

    fn route(&mut self, c: Contrib) -> Result<()> {
        let b = self.bucket_of(c.key()?);
        self.pend_bytes += c.bytes();
        self.pend[b].push(c);
        self.dirty[b] = true;
        if self.pend_bytes > self.budget / 2 {
            self.spill()?;
        }
        Ok(())
    }

    /// Every pending change to its bucket's log on disk, in order.
    fn spill(&mut self) -> Result<()> {
        let io = |e: std::io::Error| Error::Transfer(format!("view log: {e}"));
        for b in 0..self.pend.len() {
            if self.pend[b].is_empty() {
                continue;
            }
            if self.logs[b].is_none() {
                self.seq += 1;
                self.logs[b] = Some(self.dir.join(format!("log-{b}-{}.bin", self.seq)));
            }
            let path = self.logs[b].as_ref().expect("named above");
            self.logn[b] += self.pend[b].len() as u64;
            let mut w = Vec::new();
            for c in std::mem::take(&mut self.pend[b]) {
                c.write(&mut w).map_err(io)?;
            }
            append(path, &w)?;
        }
        // Taken, not drained: a drained vector keeps its capacity, and 397
        // buckets' worth of it is the window all over again.
        self.pend_bytes = 0;
        Ok(())
    }

    fn name(&mut self) -> String {
        self.seq += 1;
        format!("b-{:010}.bkt", self.seq)
    }
}

/// An invertible accumulator. `n` counts the rows (COUNT(*)) or the non-NULL
/// arguments (every other aggregate); `i` is the exact integer or scaled
/// decimal sum, `f` the float sum.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
struct IAcc {
    n: i64,
    i: i128,
    f: f64,
}

impl IAcc {
    fn add(&mut self, f: AggFn, v: Num, w: i64) -> Result<()> {
        if f == AggFn::CountStar {
            self.n += w;
            return Ok(());
        }
        let over = || bad("an incremental SUM overflowed 38 decimal digits");
        match v {
            Num::Null => {}
            Num::Int(x) => {
                self.n += w;
                self.i = self.i.checked_add(i128::from(x) * i128::from(w)).ok_or_else(over)?;
            }
            Num::Dec(x) => {
                self.n += w;
                self.i = self.i.checked_add(x.checked_mul(i128::from(w)).ok_or_else(over)?).ok_or_else(over)?;
            }
            Num::Float(x) => {
                self.n += w;
                self.f += x * w as f64;
            }
        }
        Ok(())
    }

    fn merge(&mut self, o: &IAcc, w: i64) -> Result<()> {
        let over = || bad("an incremental SUM overflowed 38 decimal digits");
        self.n += o.n * w;
        self.i = self.i.checked_add(o.i.checked_mul(i128::from(w)).ok_or_else(over)?).ok_or_else(over)?;
        self.f += o.f * w as f64;
        Ok(())
    }

    /// The batch engine's accumulator for the same rows.
    fn out(&self, a: &Agg) -> Result<Acc> {
        Ok(match a.f {
            AggFn::Count | AggFn::CountStar => Acc::N(u64::try_from(self.n).map_err(|_| bad("a negative incremental COUNT (state out of step)"))?),
            _ if self.n == 0 => Acc::None,
            AggFn::Sum => match a.ty {
                Some(Ty::Float) => Acc::F(self.f),
                Some(Ty::Dec(_)) => Acc::D(self.i),
                _ => Acc::I(i64::try_from(self.i).map_err(|_| bad("SUM overflowed 64-bit integers"))?),
            },
            AggFn::Avg => {
                let s = match a.ty {
                    Some(Ty::Float) | None => self.f,
                    Some(t) => as_f64(Num::Dec(self.i), match t {
                        Ty::Int => Ty::Dec(0),
                        t => t,
                    })
                    .unwrap_or(0.0),
                };
                Acc::Avg(s, self.n as u64)
            }
            AggFn::Min | AggFn::Max => return Err(bad("MIN/MAX in an incremental view (refused at plan)")),
        })
    }
}

fn put_bytes(w: &mut Vec<u8>, b: &[u8]) {
    w.extend_from_slice(&(b.len() as u64).to_le_bytes());
    w.extend_from_slice(b);
}

fn put_bfile(w: &mut Vec<u8>, f: &BFile) {
    put_bytes(w, f.name.as_bytes());
    w.extend_from_slice(&f.hash);
    w.extend_from_slice(&f.bytes.to_le_bytes());
}

/// One sorted section of a base: `n` records of `rec` bytes (`put` writes
/// record `i`, whose key is `key(i)`) cut into blocks of about `block`
/// bytes; the index gets the count, records per block, and each block's
/// first key and CRC-32 (the index itself is hashed by SHA-256 in the
/// manifest; a block's CRC finds a torn or flipped block, and is a
/// SIMD pass where software SHA-256 cost seconds on a 20 GB view).
fn put_section(
    w: &mut Vec<u8>,
    foot: &mut Vec<u8>,
    n: usize,
    rec: usize,
    lay: &[u8],
    block: usize,
    key: impl Fn(usize) -> i64,
    mut put: impl FnMut(&mut Vec<u8>, usize),
) {
    let per = (block / rec).max(1);
    foot.extend_from_slice(&(n as u64).to_le_bytes());
    foot.extend_from_slice(&(per as u64).to_le_bytes());
    foot.extend_from_slice(lay);
    let mut i = 0;
    while i < n {
        let start = w.len();
        let end = (i + per).min(n);
        for j in i..end {
            put(w, j);
        }
        foot.extend_from_slice(&key(i).to_le_bytes());
        foot.extend_from_slice(&crc32fast::hash(&w[start..]).to_le_bytes());
        i = end;
    }
}

/// The width a base stores each `G` group id in.
fn g_width(max: u32) -> u8 {
    if max < 1 << 8 {
        1
    } else if max < 1 << 16 {
        2
    } else {
        4
    }
}

fn put_g_rec(w: &mut Vec<u8>, k: i64, g: u32, gw: u8) {
    w.extend_from_slice(&k.to_le_bytes());
    match gw {
        1 => w.push(g as u8),
        2 => w.extend_from_slice(&(g as u16).to_le_bytes()),
        _ => w.extend_from_slice(&g.to_le_bytes()),
    }
}

fn get_g_rec(r: &[u8], gw: u8) -> (i64, u32) {
    let k = i64::from_le_bytes(r[..8].try_into().expect("8"));
    let g = match gw {
        1 => u32::from(r[8]),
        2 => u32::from(u16::from_le_bytes(r[8..10].try_into().expect("2"))),
        _ => u32::from_le_bytes(r[8..12].try_into().expect("4")),
    };
    (k, g)
}

/// An `S` section's layout, one byte per aggregate: bit 0, its count is
/// stored (else it equals the key's rows, as COUNT(*) and a SUM of a column
/// with no NULL do); bits 1-2, its exact sum's width (0: always zero, 1: 8
/// bytes, 2: 16); bit 3, its float sum is stored. A 20 GB view's `S` went
/// from 80 bytes a key to 24.
fn s_layout<'a>(na: usize, rows: impl Iterator<Item = (i64, &'a [IAcc])>) -> Vec<u8> {
    let mut lay = vec![0u8; na];
    for (n, accs) in rows {
        for (l, a) in lay.iter_mut().zip(accs) {
            if a.n != n {
                *l |= 1;
            }
            if a.i != 0 {
                let w = if i64::try_from(a.i).is_ok() { 1 } else { 2 };
                *l = (*l & !6) | (((*l >> 1) & 3).max(w) << 1);
            }
            if a.f.to_bits() != 0 {
                *l |= 8;
            }
        }
    }
    lay
}

fn s_rec_len(lay: &[u8]) -> usize {
    16 + lay.iter().map(|&l| 8 * usize::from(l & 1) + 8 * usize::from((l >> 1) & 3) + 8 * usize::from(l >> 3 & 1)).sum::<usize>()
}

fn put_s_rec(w: &mut Vec<u8>, k: i64, n: i64, accs: &[IAcc], lay: &[u8]) {
    w.extend_from_slice(&k.to_le_bytes());
    w.extend_from_slice(&n.to_le_bytes());
    for (a, &l) in accs.iter().zip(lay) {
        if l & 1 != 0 {
            w.extend_from_slice(&a.n.to_le_bytes());
        }
        match (l >> 1) & 3 {
            1 => w.extend_from_slice(&(a.i as i64).to_le_bytes()),
            2 => w.extend_from_slice(&a.i.to_le_bytes()),
            _ => {}
        }
        if l & 8 != 0 {
            w.extend_from_slice(&a.f.to_bits().to_le_bytes());
        }
    }
}

/// An `S` record: its key and rows; its accumulators into `out`.
fn get_s_rec(r: &[u8], lay: &[u8], out: &mut [IAcc]) -> (i64, i64) {
    let k = i64::from_le_bytes(r[..8].try_into().expect("8"));
    let n = i64::from_le_bytes(r[8..16].try_into().expect("8"));
    let mut at = 16;
    let mut next = |len: usize| {
        let x = &r[at..at + len];
        at += len;
        x
    };
    for (a, &l) in out.iter_mut().zip(lay) {
        a.n = if l & 1 != 0 { i64::from_le_bytes(next(8).try_into().expect("8")) } else { n };
        a.i = match (l >> 1) & 3 {
            1 => i128::from(i64::from_le_bytes(next(8).try_into().expect("8"))),
            2 => i128::from_le_bytes(next(16).try_into().expect("16")),
            _ => 0,
        };
        a.f = if l & 8 != 0 { f64::from_bits(u64::from_le_bytes(next(8).try_into().expect("8"))) } else { 0.0 };
    }
    (k, n)
}

fn put_acc(w: &mut Vec<u8>, a: &IAcc) {
    w.extend_from_slice(&a.n.to_le_bytes());
    w.extend_from_slice(&a.i.to_le_bytes());
    w.extend_from_slice(&a.f.to_bits().to_le_bytes());
}

fn get_acc(b: &[u8]) -> IAcc {
    IAcc {
        n: i64::from_le_bytes(b[..8].try_into().expect("8")),
        i: i128::from_le_bytes(b[8..24].try_into().expect("16")),
        f: f64::from_bits(u64::from_le_bytes(b[24..32].try_into().expect("8"))),
    }
}

/// An incremental view: the plan, each side's re-checked WHERE, and the state.
pub(crate) struct View {
    p: Plan,
    /// Per side: every WHERE conjunct, compiled for per-image evaluation.
    preds: Vec<Vec<PExpr>>,
    tys: Vec<Vec<Option<Ty>>>,
    fp: [u8; 32],
    st: State,
    /// A join view's `G`/`S` on disk (`None`: held in `st`, as in the unit
    /// tests and for a view without a join).
    store: Option<Store>,
}

#[derive(Default)]
struct State {
    groups: Groups,
    /// `R`: groups × aggregates.
    r: Vec<IAcc>,
    /// Rows behind each group (matched rows on a join).
    rn: Vec<i64>,
    /// `G[k]` (join only).
    g: HashMap<i64, u32, foldhash::fast::RandomState>,
    /// `S[k]` (join only): slot per key into `s_n` / `s_acc`.
    s: HashMap<i64, u32, foldhash::fast::RandomState>,
    s_n: Vec<i64>,
    s_acc: Vec<IAcc>,
    s_free: Vec<u32>,
}

impl State {
    /// `S[k]`'s slot, a fresh zeroed one when the key has none yet.
    fn s_slot(&mut self, key: i64, na: usize) -> Result<usize> {
        if let Some(&x) = self.s.get(&key) {
            return Ok(x as usize);
        }
        let x = match self.s_free.pop() {
            Some(x) => x,
            None => {
                let x = u32::try_from(self.s_n.len()).map_err(|_| bad("more than 4 billion join keys"))?;
                self.s_n.push(0);
                self.s_acc.resize(self.s_acc.len() + na, IAcc::default());
                x
            }
        };
        self.s.insert(key, x);
        Ok(x as usize)
    }

    /// Set a key's whole state (an overlay entry): present parts replace,
    /// absent parts remove.
    fn set_entry(&mut self, e: Entry, na: usize) -> Result<()> {
        match e.g {
            Some(g) => {
                self.g.insert(e.k, g);
            }
            None => {
                self.g.remove(&e.k);
            }
        }
        match e.s {
            Some((n, accs)) => {
                let x = self.s_slot(e.k, na)?;
                self.s_n[x] = n;
                self.s_acc[x * na..(x + 1) * na].copy_from_slice(&accs);
            }
            None => {
                if let Some(x) = self.s.remove(&e.k) {
                    let x = x as usize;
                    self.s_n[x] = 0;
                    self.s_acc[x * na..(x + 1) * na].fill(IAcc::default());
                    self.s_free.push(x as u32);
                }
            }
        }
        Ok(())
    }
}

impl View {
    /// Plan `query` for incremental maintenance, refusing before any read
    /// what a change stream cannot keep exact.
    pub(crate) fn plan(
        query: &str,
        sources: &HashMap<String, String>,
        schemas: &mut dyn FnMut(&str, &str) -> Result<Vec<ArrowField>>,
    ) -> Result<View> {
        let mut p = plan(query, sources, schemas)?;
        if p.out.iter().any(|o| matches!(o.1, OutCol::Expr(_))) {
            return Err(unsupported("mode=\"log_based\" with arithmetic over aggregates in SELECT (a run without mode= takes it)"));
        }
        if !p.having.is_empty() {
            return Err(unsupported("mode=\"log_based\" with HAVING (a run without mode= takes it)"));
        }
        if !p.orx.is_empty() {
            return Err(unsupported("mode=\"log_based\" with an OR over both tables (a run without mode= takes it)"));
        }
        if p.sides.iter().any(|s| !s.flags.is_empty()) {
            return Err(unsupported("mode=\"log_based\" with a CASE over the other table (a run without mode= takes it)"));
        }
        if p.sides.iter().any(|s| !s.semis.is_empty()) {
            return Err(unsupported("mode=\"log_based\" with EXISTS or IN (subquery) (a run without mode= takes them)"));
        }
        if p.star.is_some() {
            return Err(unsupported(
                "mode=\"log_based\" over three or more tables (a run without mode= joins them; an incremental view keeps two)",
            ));
        }
        let mm: Vec<&str> = p
            .aggs
            .iter()
            .filter_map(|a| match a.f {
                AggFn::Min => Some("MIN"),
                AggFn::Max => Some("MAX"),
                _ => None,
            })
            .collect();
        if !mm.is_empty() {
            return Err(unsupported(format!(
                "{} with mode=\"log_based\": a delete of the extreme needs the rest of its group, so it \
                 cannot move by changes alone (SUM, COUNT and AVG can); run it without mode=",
                mm.join(" and ")
            )));
        }
        let mut preds = Vec::new();
        for s in 0..p.sides.len() {
            if p.sides[s].dialect == Dialect::Other {
                return Err(unsupported(format!(
                    "{} with mode=\"log_based\": it has no change stream (postgres:// and mysql:// do)",
                    p.sides[s].name
                )));
            }
            let mut ps = Vec::new();
            for e in p.sides[s].conj_src.clone() {
                ps.push(pexpr(&mut p.sides, s, &e).map_err(|err| {
                    unsupported(format!(
                        "{err} with mode=\"log_based\": every WHERE condition is re-checked on each change, \
                         so it must be one apitap evaluates itself"
                    ))
                })?);
            }
            preds.push(ps);
        }
        for side in &p.sides {
            for &c in &side.read {
                let f = &side.schema[c];
                if matches!(f.kind, ArrowKind::TimestampUtc | ArrowKind::TimestampNaive | ArrowKind::Binary) {
                    return Err(unsupported(format!(
                        "the {:?} column {}.{} in an incremental view (v1 reads integers, decimals, floats, \
                         booleans, dates and text from a change)",
                        f.kind, side.name, f.name
                    )));
                }
            }
        }
        let tys = p.sides.iter().map(side_tys).collect();
        let mut h = Sha256::new();
        h.update(query.as_bytes());
        for s in &p.sides {
            h.update([0u8]);
            h.update(s.name.as_bytes());
            h.update([0u8]);
            h.update(s.table.as_bytes());
            for &c in &s.read {
                h.update([1u8]);
                h.update(s.schema[c].name.as_bytes());
                h.update(format!("{:?}", s.schema[c].kind).as_bytes());
            }
        }
        let fp: [u8; 32] = h.finalize().into();
        let mut v = View { p, preds, tys, fp, st: State::default(), store: None };
        if v.p.groups.is_empty() {
            v.st.groups.id(&[])?;
            v.st.r = vec![IAcc::default(); v.p.aggs.len()];
            v.st.rn = vec![0];
        }
        Ok(v)
    }

    /// The plan (for the live runner).
    pub(super) fn pl(&self) -> &Plan {
        &self.p
    }

    /// The query fingerprint, hex: names the slot, publication and state file.
    pub(crate) fn fp_hex(&self) -> String {
        self.fp.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The columns side `s` needs from each change image, in the order
    /// [`View::apply`] takes them.
    pub(crate) fn columns(&self, s: usize) -> Vec<String> {
        let side = &self.p.sides[s];
        side.read.iter().map(|&c| side.schema[c].name.clone()).collect()
    }

    /// Images of side `s` as one batch, typed like a read of it: `rows` hold
    /// each image's cells in [`View::columns`] order, as the text a change
    /// stream ships (`None` = NULL).
    fn images(&self, s: usize, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> Result<ArrowBatch> {
        let side = &self.p.sides[s];
        let kinds: Vec<ArrowKind> = side.read.iter().map(|&c| side.schema[c].kind).collect();
        let mut b = crate::wire::arrowcol::BatchBuilder::new(kinds.clone(), usize::MAX >> 1);
        use crate::wire::arrowcol::CellVal as C;
        for (_, img) in rows {
            if img.len() != kinds.len() {
                return Err(bad(format!("a change image of {} has {} cells, the view reads {}", side.name, img.len(), kinds.len())));
            }
            for (c, (cell, k)) in img.iter().zip(&kinds).enumerate() {
                let Some(raw) = cell else {
                    b.append_cell(c, C::Null);
                    continue;
                };
                let t = std::str::from_utf8(raw).map_err(|_| bad("a change cell that is not UTF-8"))?;
                let fail = || bad(format!("the change value {t:?} of {}.{} is not a {k:?}", side.name, side.schema[side.read[c]].name));
                match k {
                    ArrowKind::Int16 => b.append_cell(c, C::I16(t.trim().parse().map_err(|_| fail())?)),
                    ArrowKind::Int32 => b.append_cell(c, C::I32(t.trim().parse().map_err(|_| fail())?)),
                    ArrowKind::Int64 => b.append_cell(c, C::I64(t.trim().parse().map_err(|_| fail())?)),
                    ArrowKind::Float32 => b.append_cell(c, C::F32(t.trim().parse().map_err(|_| fail())?)),
                    ArrowKind::Float64 => b.append_cell(c, C::F64(t.trim().parse().map_err(|_| fail())?)),
                    ArrowKind::Bool => b.append_cell(
                        c,
                        C::Bool(match t.trim() {
                            "t" | "true" | "1" => true,
                            "f" | "false" | "0" => false,
                            _ => return Err(fail()),
                        }),
                    ),
                    ArrowKind::Decimal { s: sc, .. } => {
                        let v = match number(t.trim()).map_err(|_| fail())? {
                            VExpr::Int(v) => rescale(i128::from(v), 0, *sc)?,
                            VExpr::Dec(v, vs) if vs <= *sc => rescale(v, vs, *sc)?,
                            _ => return Err(fail()),
                        };
                        b.append_cell(c, C::Dec128(v));
                    }
                    ArrowKind::Date32 => {
                        let d = days_from_text(t).and_then(|d| i32::try_from(d).ok()).ok_or_else(fail)?;
                        b.append_cell(c, C::DateDays(d));
                    }
                    ArrowKind::Utf8 => b.append_cell(c, C::Str(raw)),
                    _ => return Err(fail()),
                }
            }
            b.row_done();
        }
        Ok(b.finish()?.unwrap_or(ArrowBatch { rows: 0, cols: Vec::new() }))
    }

    /// Move the answer by signed images of side `s` (+1 insert, −1 delete;
    /// an update is its old image at −1 then its new one at +1), in change
    /// order. A batch read of the side may be applied the same way, every row
    /// at +1: that is the bootstrap.
    pub(crate) fn apply(&mut self, s: usize, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let b = self.images(s, rows)?;
        let w: Vec<i64> = rows.iter().map(|r| r.0).collect();
        self.apply_batch(s, &b, &w)
    }

    /// [`View::apply`] over a typed batch whose columns are side `s`'s read
    /// list; `w` is each row's sign.
    pub(crate) fn apply_batch(&mut self, s: usize, b: &ArrowBatch, w: &[i64]) -> Result<()> {
        let mut kb = Vec::with_capacity(64);
        for r in 0..b.rows {
            let w = w[r];
            if !self.preds[s].iter().all(|pe| peval(pe, b, r, &self.tys[s]) == Some(true)) {
                continue;
            }
            let c = match self.p.keys {
                None => {
                    let g = self.group_of(b, r, &mut kb)? as u32;
                    Contrib::R { g, vals: self.vals(b, r)?, w }
                }
                Some(k) => {
                    let at = k.iter().find(|x| x.0 == s).expect("both sides keyed").1;
                    let Some(key) = key_at(&b.cols[at], r)? else { continue };
                    if s == self.p.group_side {
                        let g = self.group_of(b, r, &mut kb)? as u32;
                        Contrib::G { key, g, w }
                    } else {
                        Contrib::S { key, vals: self.vals(b, r)?, w }
                    }
                }
            };
            match self.store.as_mut() {
                Some(st) if !matches!(c, Contrib::R { .. }) => st.route(c)?,
                _ => self.apply_one(&c)?,
            }
        }
        Ok(())
    }

    /// Move the in-memory state by one change.
    fn apply_one(&mut self, c: &Contrib) -> Result<()> {
        let na = self.p.aggs.len();
        match c {
            Contrib::R { g, vals, w } => self.fold_r(*g as usize, vals, *w),
            Contrib::G { key, g, w } => self.move_key(*key, *g as usize, *w),
            Contrib::S { key, vals, w } => {
                let (key, w) = (*key, *w);
                let st = &mut self.st;
                let slot = st.s_slot(key, na)?;
                st.s_n[slot] += w;
                for (i, a) in self.p.aggs.iter().enumerate() {
                    st.s_acc[slot * na + i].add(a.f, vals[i], w)?;
                }
                if st.s_n[slot] < 0 {
                    return Err(bad(format!("join key {key} was deleted more often than inserted (state out of step)")));
                }
                if st.s_n[slot] == 0 {
                    st.s.remove(&key);
                    st.s_acc[slot * na..(slot + 1) * na].fill(IAcc::default());
                    st.s_free.push(slot as u32);
                }
                if let Some(&g) = self.st.g.get(&key) {
                    self.fold_r(g as usize, vals, w)?;
                }
                Ok(())
            }
        }
    }

    fn group_of(&mut self, b: &ArrowBatch, r: usize, kb: &mut Vec<u8>) -> Result<usize> {
        kb.clear();
        for &g in &self.p.groups {
            encode_group(&b.cols[g], r, kb);
        }
        let g = self.st.groups.id(kb)? as usize;
        let na = self.p.aggs.len();
        if self.st.rn.len() <= g {
            self.st.rn.resize(g + 1, 0);
            self.st.r.resize((g + 1) * na, IAcc::default());
        }
        Ok(g)
    }

    fn vals(&self, b: &ArrowBatch, r: usize) -> Result<Vec<Num>> {
        self.p.aggs.iter().map(|a| a.arg.as_ref().map_or(Ok(Num::Null), |e| eval(e, b, r))).collect()
    }

    fn fold_r(&mut self, g: usize, vals: &[Num], w: i64) -> Result<()> {
        let na = self.p.aggs.len();
        self.st.rn[g] += w;
        if self.st.rn[g] < 0 {
            return Err(bad("a group lost more rows than it had (state out of step)"));
        }
        for (i, a) in self.p.aggs.iter().enumerate() {
            self.st.r[g * na + i].add(a.f, vals[i], w)?;
        }
        Ok(())
    }

    /// A GROUP BY-side row at `key` in group `g` arrives (+1) or leaves (−1):
    /// the other side's contribution at that key moves with it.
    fn move_key(&mut self, key: i64, g: usize, w: i64) -> Result<()> {
        let na = self.p.aggs.len();
        if w > 0 {
            if self.st.g.contains_key(&key) {
                return Err(unsupported(format!(
                    "the join key {key} twice on the GROUP BY table: mode=\"log_based\" v1 needs that \
                     table's join key unique (its primary key)"
                )));
            }
            self.st.g.insert(key, g as u32);
        } else {
            match self.st.g.get(&key) {
                Some(&g0) if g0 as usize == g => {
                    self.st.g.remove(&key);
                }
                _ => return Err(bad(format!("a delete of join key {key} the view does not hold in that group (state out of step)"))),
            }
        }
        if let Some(&slot) = self.st.s.get(&key) {
            let slot = slot as usize;
            self.st.rn[g] += self.st.s_n[slot] * w;
            for i in 0..na {
                let src = self.st.s_acc[slot * na + i];
                self.st.r[g * na + i].merge(&src, w)?;
            }
        }
        Ok(())
    }

    /// The current answer, shaped exactly as [`run`](super::run) returns it.
    pub(crate) fn answer(&self) -> Result<SqlResult> {
        let na = self.p.aggs.len();
        let accs: Vec<Acc> = self
            .st
            .r
            .chunks(na.max(1))
            .flat_map(|c| c.iter().zip(&self.p.aggs).map(|(x, a)| x.out(a)))
            .collect::<Result<_>>()?;
        if self.p.groups.is_empty() {
            return result(&self.p, &self.st.groups, &accs, None);
        }
        let keep: Vec<bool> = self.st.rn.iter().map(|&n| n > 0).collect();
        // Groups equal under a GROUP BY column's collation are one row.
        if let Some((g, a, h)) = super::collated(&self.p, &self.st.groups, &accs, Some(&keep))? {
            return result(&self.p, &g, &a, Some(&h));
        }
        result(&self.p, &self.st.groups, &accs, Some(&keep))
    }

    /// Start a fresh state at `path` (its buckets in `path` with the extension
    /// `d`): a join view's `G`/`S` go to disk, cut at `splits`.
    pub(crate) fn begin(&mut self, path: &Path, splits: Vec<i64>) -> Result<()> {
        if self.p.keys.is_some() {
            self.store = Some(Store::fresh(path.with_extension("d"), splits)?);
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_bucket_budget(&mut self, b: u64) {
        if let Some(s) = self.store.as_mut() {
            s.budget = b;
        }
    }

    #[cfg(test)]
    fn set_block(&mut self, b: usize) {
        if let Some(s) = self.store.as_mut() {
            s.block = b;
        }
    }

    /// Buckets moved by an overlay and by a whole rewrite since the state
    /// was begun or loaded.
    pub(crate) fn flush_stats(&self) -> (usize, usize) {
        self.store.as_ref().map_or((0, 0), |s| (s.sparse, s.full))
    }

    /// Buckets of a join view's disk state (0 without one).
    pub(crate) fn buckets(&self) -> usize {
        self.store.as_ref().map_or(0, |s| s.files.len())
    }

    fn clear_bucket(&mut self) {
        let st = &mut self.st;
        st.g = HashMap::default();
        st.s = HashMap::default();
        st.s_n = Vec::new();
        st.s_acc = Vec::new();
        st.s_free = Vec::new();
    }

    fn bucket_mem(&self) -> u64 {
        let na = self.p.aggs.len() as u64;
        self.st.g.len() as u64 * 24 + self.st.s.len() as u64 * (32 + 48 * na)
    }

    /// Move every bucket with pending changes. A bucket a window touches at a
    /// few keys reads just their blocks and its overlays, and stacks one more
    /// overlay with those keys' new state: the base is not rewritten. Any
    /// other (the bootstrap, a large window, a bucket with too many overlays)
    /// is loaded whole, moved and written back as one new base, split in two
    /// at its median key when it outgrew the budget. One bucket is in memory
    /// at a time.
    fn flush(&mut self) -> Result<()> {
        let Some(mut store) = self.store.take() else { return Ok(()) };
        let r = self.flush_store(&mut store);
        self.store = Some(store);
        self.clear_bucket();
        r
    }

    fn flush_store(&mut self, store: &mut Store) -> Result<()> {
        for b in (0..store.files.len()).rev() {
            if !store.dirty[b] {
                continue;
            }
            self.clear_bucket();
            let n = store.logn[b] + store.pend[b].len() as u64;
            let ovl: u64 = store.ovls[b].iter().map(|f| f.bytes).sum();
            // Each change reads at most a block of each section; the window
            // goes sparse while that stays within half the base.
            let sparse = store.files[b].as_ref().is_some_and(|f| {
                n * 4 * store.block as u64 <= f.bytes && ovl <= f.bytes && store.ovls[b].len() < MAX_OVERLAYS
            });
            if sparse {
                self.flush_sparse(store, b)?;
                store.sparse += 1;
            } else {
                self.flush_full(store, b)?;
                store.full += 1;
            }
            store.logn[b] = 0;
            store.dirty[b] = false;
        }
        store.pend_bytes = 0;
        Ok(())
    }

    /// The bucket's changes: its log (read, removed) then its pending ones.
    fn take_changes(store: &mut Store, b: usize, mut each: impl FnMut(Contrib) -> Result<()>) -> Result<()> {
        let dir = store.dir.clone();
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", dir.display()));
        // A log only lives within the run that spilled it (no manifest names
        // it), so it is read back without a sync.
        if let Some(path) = store.logs[b].take() {
            let mut r = std::io::BufReader::with_capacity(64 << 10, std::fs::File::open(&path).map_err(io)?);
            while let Some(c) = Contrib::read(&mut r).map_err(io)? {
                each(c)?;
            }
            let _ = std::fs::remove_file(&path);
        }
        for c in std::mem::take(&mut store.pend[b]) {
            each(c)?;
        }
        Ok(())
    }

    fn flush_full(&mut self, store: &mut Store, b: usize) -> Result<()> {
        let dir = store.dir.clone();
        let na = self.p.aggs.len();
        if let Some(f) = store.files[b].clone() {
            self.load_base(&dir, &f)?;
        }
        for f in store.ovls[b].clone() {
            for e in self.read_overlay(&dir, &f)? {
                self.st.set_entry(e, na)?;
            }
        }
        Self::take_changes(store, b, |c| self.apply_one(&c))?;
        for f in store.files[b].take().into_iter().chain(store.ovls[b].drain(..)) {
            store.stale.push(f.name);
        }
        let mut keys: Vec<i64> = self.st.g.keys().chain(self.st.s.keys()).copied().collect();
        if self.bucket_mem() > store.budget && keys.len() >= 2 {
            keys.sort_unstable();
            keys.dedup();
            let mid = keys[keys.len() / 2];
            let lo = self.write_bucket(&dir, store.name(), |k| k < mid, store.block)?;
            let hi = self.write_bucket(&dir, store.name(), |k| k >= mid, store.block)?;
            store.splits.insert(b, mid);
            store.files[b] = Some(lo);
            store.files.insert(b + 1, Some(hi));
            store.ovls.insert(b + 1, Vec::new());
            store.pend.insert(b + 1, Vec::new());
            store.logs.insert(b + 1, None);
            store.logn.insert(b + 1, 0);
            store.dirty.insert(b + 1, false);
        } else if !keys.is_empty() {
            store.files[b] = Some(self.write_bucket(&dir, store.name(), |_| true, store.block)?);
        }
        Ok(())
    }

    fn flush_sparse(&mut self, store: &mut Store, b: usize) -> Result<()> {
        let dir = store.dir.clone();
        let na = self.p.aggs.len();
        let mut ch = Vec::new();
        Self::take_changes(store, b, |c| {
            ch.push(c);
            Ok(())
        })?;
        let mut keys: Vec<i64> = ch.iter().map(Contrib::key).collect::<Result<_>>()?;
        keys.sort_unstable();
        keys.dedup();
        // Newest overlay last, so it wins; a key an overlay names is not read
        // from the base (the overlay holds its whole state, or its absence).
        let mut seen: std::collections::HashSet<i64, foldhash::fast::RandomState> = Default::default();
        for f in store.ovls[b].clone() {
            for e in self.read_overlay(&dir, &f)? {
                if keys.binary_search(&e.k).is_ok() {
                    seen.insert(e.k);
                    self.st.set_entry(e, na)?;
                }
            }
        }
        let rest: Vec<i64> = keys.iter().copied().filter(|k| !seen.contains(k)).collect();
        let base = store.files[b].clone().ok_or_else(|| bad("a sparse bucket move without a base"))?;
        self.lookup_base(&dir, &base, &rest)?;
        for c in &ch {
            self.apply_one(c)?;
        }
        let f = self.write_overlay(&dir, store.name(), &keys)?;
        store.ovls[b].push(f);
        Ok(())
    }

    /// Write the loaded bucket's keys `keep` holds as a base: magic, the
    /// query's fingerprint, the `G` records (key, group) and the `S` records
    /// (key, rows, accumulators), each section sorted by key and cut into
    /// blocks of about `block` bytes, then the index (per section: count,
    /// records per block, each block's first key and CRC-32) and its length.
    /// The manifest holds the hash of the fingerprint and the index, so one
    /// key is read by one block, checked by its own CRC.
    fn write_bucket(&self, dir: &Path, name: String, keep: impl Fn(i64) -> bool, block: usize) -> Result<BFile> {
        let path = dir.join(&name);
        let st = &self.st;
        let na = self.p.aggs.len();
        let mut w: Vec<u8> = Vec::new();
        w.extend_from_slice(BUCKET_MAGIC);
        w.extend_from_slice(&self.fp);
        let mut foot: Vec<u8> = Vec::new();
        let mut gk: Vec<(i64, u32)> = st.g.iter().filter(|x| keep(*x.0)).map(|(&k, &g)| (k, g)).collect();
        gk.sort_unstable();
        let gw = g_width(gk.iter().map(|x| x.1).max().unwrap_or(0));
        put_section(&mut w, &mut foot, gk.len(), 8 + gw as usize, &[gw], block, |i| gk[i].0, |w, i| put_g_rec(w, gk[i].0, gk[i].1, gw));
        let mut sk: Vec<(i64, u32)> = st.s.iter().filter(|x| keep(*x.0)).map(|(&k, &x)| (k, x)).collect();
        sk.sort_unstable();
        let slot = |i: usize| sk[i].1 as usize;
        let lay = s_layout(na, (0..sk.len()).map(|i| (st.s_n[slot(i)], &st.s_acc[slot(i) * na..(slot(i) + 1) * na])));
        put_section(&mut w, &mut foot, sk.len(), s_rec_len(&lay), &lay, block, |i| sk[i].0, |w, i| {
            let x = slot(i);
            put_s_rec(w, sk[i].0, st.s_n[x], &st.s_acc[x * na..(x + 1) * na], &lay);
        });
        self.seal_base(&path, name, w, foot, true)
    }

    /// A base from sorted arrays (the bulk first run): `g` (key, group), and
    /// `S` as keys, rows and `na` accumulators per key.
    fn write_sorted(&self, dir: &Path, name: String, block: usize, g: &[(i64, u32)], sk: &[i64], sn: &[i64], sacc: &[IAcc]) -> Result<BFile> {
        // Synced once for all at the end of the bulk run (one sync per file
        // right after its write stalled the run on each).
        let path = dir.join(&name);
        let na = self.p.aggs.len();
        let mut w: Vec<u8> = Vec::with_capacity(40 + g.len() * 12 + sk.len() * (16 + 16 * na));
        w.extend_from_slice(BUCKET_MAGIC);
        w.extend_from_slice(&self.fp);
        let mut foot: Vec<u8> = Vec::new();
        let gw = g_width(g.iter().map(|x| x.1).max().unwrap_or(0));
        put_section(&mut w, &mut foot, g.len(), 8 + gw as usize, &[gw], block, |i| g[i].0, |w, i| put_g_rec(w, g[i].0, g[i].1, gw));
        let lay = s_layout(na, (0..sk.len()).map(|i| (sn[i], &sacc[i * na..(i + 1) * na])));
        put_section(&mut w, &mut foot, sk.len(), s_rec_len(&lay), &lay, block, |i| sk[i], |w, i| {
            put_s_rec(w, sk[i], sn[i], &sacc[i * na..(i + 1) * na], &lay);
        });
        self.seal_base(&path, name, w, foot, false)
    }

    /// The index and its length after the records, written and synced.
    fn seal_base(&self, path: &Path, name: String, mut w: Vec<u8>, foot: Vec<u8>, sync: bool) -> Result<BFile> {
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let mut h = Sha256::new();
        h.update(self.fp);
        h.update(&foot);
        let hash: [u8; 32] = h.finalize().into();
        w.extend_from_slice(&foot);
        w.extend_from_slice(&(foot.len() as u64).to_le_bytes());
        let mut f = std::fs::File::create(path).map_err(io)?;
        f.write_all(&w).map_err(io)?;
        if sync {
            f.sync_all().map_err(io)?;
        } else {
            start_writeback(&f);
        }
        Ok(BFile { name, hash, bytes: w.len() as u64 })
    }

    /// Open a base: its index, checked against the manifest's hash.
    fn open_base(&self, path: &Path, f: &BFile) -> Result<(std::fs::File, [Sec; 2])> {
        use std::io::{Seek, SeekFrom};
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let stale = |why: &str| Error::Transfer(format!("view state bucket {}: {why}; delete the view's state to rebuild it", path.display()));
        let mut file = std::fs::File::open(path).map_err(io)?;
        let len = file.metadata().map_err(io)?.len();
        if len < 40 + 32 + 8 {
            return Err(stale("not a view bucket"));
        }
        let mut head = [0u8; 40];
        file.read_exact(&mut head).map_err(io)?;
        if &head[..8] != BUCKET_MAGIC {
            return Err(stale("not a view bucket of this version"));
        }
        if head[8..40] != self.fp {
            return Err(stale("it belongs to another query"));
        }
        let mut b8 = [0u8; 8];
        file.seek(SeekFrom::End(-8)).map_err(io)?;
        file.read_exact(&mut b8).map_err(io)?;
        let flen = u64::from_le_bytes(b8);
        if flen > len - 48 {
            return Err(stale("its index length is out of range (torn)"));
        }
        let fstart = len - 8 - flen;
        let mut foot = vec![0u8; flen as usize];
        file.seek(SeekFrom::Start(fstart)).map_err(io)?;
        file.read_exact(&mut foot).map_err(io)?;
        let mut h = Sha256::new();
        h.update(self.fp);
        h.update(&foot);
        let got: [u8; 32] = h.finalize().into();
        if got != f.hash {
            return Err(stale("its index hash does not match the manifest (torn or edited)"));
        }
        let mut at = 0usize;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = foot.get(at..at + n).ok_or_else(|| stale("its index ends early"))?;
            at += n;
            Ok(s)
        };
        let u64_ = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8"));
        let mut secs = Vec::with_capacity(2);
        let mut start = 40u64;
        for si in 0..2 {
            let count = u64_(take(8)?);
            let per = u64_(take(8)?);
            if per == 0 {
                return Err(stale("a section has no records per block"));
            }
            let lay = take(if si == 0 { 1 } else { self.p.aggs.len() })?.to_vec();
            let rec = if si == 0 {
                if !matches!(lay[0], 1 | 2 | 4) {
                    return Err(stale("its group width is not 1, 2 or 4"));
                }
                8 + u64::from(lay[0])
            } else {
                if lay.iter().any(|&l| l > 15 || (l >> 1) & 3 == 3) {
                    return Err(stale("its layout byte is not one this version writes"));
                }
                s_rec_len(&lay) as u64
            };
            let mut blocks = Vec::new();
            for _ in 0..count.div_ceil(per) {
                let k = i64::from_le_bytes(take(8)?.try_into().expect("8"));
                blocks.push((k, u32::from_le_bytes(take(4)?.try_into().expect("4"))));
            }
            secs.push(Sec { count, per, rec, lay, start, blocks });
            start += count * rec;
        }
        if at != foot.len() || start != fstart {
            return Err(stale("its index does not match its size"));
        }
        let s = secs.pop().expect("two");
        let g = secs.pop().expect("two");
        Ok((file, [g, s]))
    }

    /// Block `bi` of a section, checked by its hash.
    fn read_block(file: &mut std::fs::File, path: &Path, sec: &Sec, bi: usize) -> Result<Vec<u8>> {
        use std::io::{Seek, SeekFrom};
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let first = bi as u64 * sec.per;
        let n = sec.per.min(sec.count - first);
        let mut buf = vec![0u8; (n * sec.rec) as usize];
        file.seek(SeekFrom::Start(sec.start + first * sec.rec)).map_err(io)?;
        file.read_exact(&mut buf).map_err(io)?;
        if crc32fast::hash(&buf) != sec.blocks[bi].1 || i64::from_le_bytes(buf[..8].try_into().expect("8")) != sec.blocks[bi].0 {
            return Err(Error::Transfer(format!(
                "view state bucket {}: a block's hash does not match its index (torn or edited); delete the view's state to rebuild it",
                path.display()
            )));
        }
        Ok(buf)
    }

    fn put_g(&mut self, r: &[u8], sec: &Sec) {
        let (k, g) = get_g_rec(r, sec.lay[0]);
        self.st.g.insert(k, g);
    }

    fn put_s(&mut self, r: &[u8], sec: &Sec) -> Result<()> {
        let na = self.p.aggs.len();
        let k = i64::from_le_bytes(r[..8].try_into().expect("8"));
        let x = self.st.s_slot(k, na)?;
        let (_, n) = get_s_rec(r, &sec.lay, &mut self.st.s_acc[x * na..(x + 1) * na]);
        self.st.s_n[x] = n;
        Ok(())
    }

    /// Every record of a base into the loaded bucket, block by checked block.
    fn load_base(&mut self, dir: &Path, f: &BFile) -> Result<()> {
        let path = dir.join(&f.name);
        let (mut file, secs) = self.open_base(&path, f)?;
        for (si, sec) in secs.iter().enumerate() {
            for bi in 0..sec.blocks.len() {
                let blk = Self::read_block(&mut file, &path, sec, bi)?;
                for r in blk.chunks_exact(sec.rec as usize) {
                    if si == 0 {
                        self.put_g(r, sec);
                    } else {
                        self.put_s(r, sec)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The base's records at `keys` (sorted) into the loaded bucket: one
    /// block read per section for each block a key falls in.
    fn lookup_base(&mut self, dir: &Path, f: &BFile, keys: &[i64]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let path = dir.join(&f.name);
        let (mut file, secs) = self.open_base(&path, f)?;
        for (si, sec) in secs.iter().enumerate() {
            let rec = sec.rec as usize;
            let mut i = 0;
            while i < keys.len() {
                let bi = sec.blocks.partition_point(|x| x.0 <= keys[i]);
                if bi == 0 {
                    i += 1;
                    continue;
                }
                let bi = bi - 1;
                let end = sec.blocks.get(bi + 1).map(|x| x.0);
                let blk = Self::read_block(&mut file, &path, sec, bi)?;
                let n = blk.len() / rec;
                let key_of = |m: usize| i64::from_le_bytes(blk[m * rec..m * rec + 8].try_into().expect("8"));
                while i < keys.len() && end.map_or(true, |e| keys[i] < e) {
                    let k = keys[i];
                    let (mut lo, mut hi) = (0, n);
                    while lo < hi {
                        let m = (lo + hi) / 2;
                        if key_of(m) < k {
                            lo = m + 1;
                        } else {
                            hi = m;
                        }
                    }
                    if lo < n && key_of(lo) == k {
                        let r = &blk[lo * rec..(lo + 1) * rec];
                        if si == 0 {
                            self.put_g(r, sec);
                        } else {
                            self.put_s(r, sec)?;
                        }
                    }
                    i += 1;
                }
            }
        }
        Ok(())
    }

    /// Write the loaded state of `keys` as an overlay: magic, fingerprint,
    /// the entries (key, which parts it has, the group, the rows and
    /// accumulators), then a SHA-256 of all of it, which the manifest holds.
    fn write_overlay(&self, dir: &Path, name: String, keys: &[i64]) -> Result<BFile> {
        let path = dir.join(&name);
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let st = &self.st;
        let na = self.p.aggs.len();
        let mut w: Vec<u8> = Vec::new();
        w.extend_from_slice(OVERLAY_MAGIC);
        w.extend_from_slice(&self.fp);
        w.extend_from_slice(&(keys.len() as u64).to_le_bytes());
        for &k in keys {
            let g = st.g.get(&k).copied();
            let s = st.s.get(&k).map(|&x| x as usize);
            w.extend_from_slice(&k.to_le_bytes());
            w.push(u8::from(g.is_some()) | (u8::from(s.is_some()) << 1));
            if let Some(g) = g {
                w.extend_from_slice(&g.to_le_bytes());
            }
            if let Some(x) = s {
                w.extend_from_slice(&st.s_n[x].to_le_bytes());
                for a in &st.s_acc[x * na..(x + 1) * na] {
                    put_acc(&mut w, a);
                }
            }
        }
        let hash: [u8; 32] = Sha256::digest(&w).into();
        w.extend_from_slice(&hash);
        let mut f = std::fs::File::create(&path).map_err(io)?;
        f.write_all(&w).map_err(io)?;
        f.sync_all().map_err(io)?;
        Ok(BFile { name, hash, bytes: w.len() as u64 })
    }

    fn read_overlay(&self, dir: &Path, f: &BFile) -> Result<Vec<Entry>> {
        let path = dir.join(&f.name);
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let mut buf = Vec::new();
        std::fs::File::open(&path).map_err(io)?.read_to_end(&mut buf).map_err(io)?;
        let stale = |why: &str| Error::Transfer(format!("view state overlay {}: {why}; delete the view's state to rebuild it", path.display()));
        if buf.len() < 8 + 32 + 8 + 32 || &buf[..8] != OVERLAY_MAGIC {
            return Err(stale("not a view overlay"));
        }
        let (body, trailer) = buf.split_at(buf.len() - 32);
        let got: [u8; 32] = Sha256::digest(body).into();
        if got != trailer || got != f.hash {
            return Err(stale("its content hash does not match the manifest (torn or edited)"));
        }
        if body[8..40] != self.fp {
            return Err(stale("it belongs to another query"));
        }
        let na = self.p.aggs.len();
        let mut at = 40;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = body.get(at..at + n).ok_or_else(|| stale("it ends early"))?;
            at += n;
            Ok(s)
        };
        let i64_ = |b: &[u8]| i64::from_le_bytes(b.try_into().expect("8"));
        let n = u64::from_le_bytes(take(8)?.try_into().expect("8"));
        let mut out = Vec::new();
        for _ in 0..n {
            let k = i64_(take(8)?);
            let flags = take(1)?[0];
            let g = if flags & 1 != 0 { Some(u32::from_le_bytes(take(4)?.try_into().expect("4"))) } else { None };
            let s = if flags & 2 != 0 {
                let rows = i64_(take(8)?);
                let mut accs = Vec::with_capacity(na);
                for _ in 0..na {
                    accs.push(get_acc(take(32)?));
                }
                Some((rows, accs))
            } else {
                None
            };
            out.push(Entry { k, g, s });
        }
        if at != body.len() {
            return Err(stale("it has trailing bytes"));
        }
        Ok(out)
    }

    /// Write the state with `positions` (one per source, opaque) to `path`:
    /// first every bucket with pending changes, then the manifest (the
    /// answer's accumulators, the positions, the buckets' files and hashes)
    /// atomically (temp file, fsync, rename), then the files it no longer
    /// names are removed. A crash before the rename leaves the old manifest
    /// naming the old files, all still there. Returns the manifest's hash.
    pub(crate) fn save(&mut self, path: &Path, positions: &[String]) -> Result<[u8; 32]> {
        if self.p.keys.is_some() && self.store.is_none() {
            // A join state held in memory (tests): one bucket of everything.
            let mut store = Store::fresh(path.with_extension("d"), Vec::new())?;
            if !self.st.g.is_empty() || !self.st.s.is_empty() {
                let dir = store.dir.clone();
                store.files[0] = Some(self.write_bucket(&dir, store.name(), |_| true, store.block)?);
            }
            self.store = Some(store);
            self.clear_bucket();
        }
        self.flush()?;
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let st = &self.st;
        let na = self.p.aggs.len();
        let mut w: Vec<u8> = Vec::new();
        w.extend_from_slice(MAGIC);
        w.extend_from_slice(&VERSION.to_le_bytes());
        w.extend_from_slice(&self.fp);
        w.extend_from_slice(&(positions.len() as u64).to_le_bytes());
        for p in positions {
            put_bytes(&mut w, p.as_bytes());
        }
        w.extend_from_slice(&(na as u64).to_le_bytes());
        w.extend_from_slice(&(st.groups.keys.len() as u64).to_le_bytes());
        for (g, k) in st.groups.keys.iter().enumerate() {
            put_bytes(&mut w, k);
            w.extend_from_slice(&st.rn[g].to_le_bytes());
            for a in &st.r[g * na..(g + 1) * na] {
                put_acc(&mut w, a);
            }
        }
        match &self.store {
            None => w.push(0),
            Some(store) => {
                w.push(1);
                w.extend_from_slice(&store.seq.to_le_bytes());
                w.extend_from_slice(&(store.splits.len() as u64).to_le_bytes());
                for x in &store.splits {
                    w.extend_from_slice(&x.to_le_bytes());
                }
                for (f, ovls) in store.files.iter().zip(&store.ovls) {
                    match f {
                        None => w.push(0),
                        Some(f) => {
                            w.push(1);
                            put_bfile(&mut w, f);
                        }
                    }
                    w.extend_from_slice(&(ovls.len() as u64).to_le_bytes());
                    for f in ovls {
                        put_bfile(&mut w, f);
                    }
                }
            }
        }
        let hash: [u8; 32] = Sha256::digest(&w).into();
        w.extend_from_slice(&hash);
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            f.write_all(&w).map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        std::fs::rename(&tmp, path).map_err(io)?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        if let Some(store) = self.store.as_mut() {
            for name in store.stale.drain(..) {
                let _ = std::fs::remove_file(store.dir.join(name));
            }
        }
        Ok(hash)
    }

    /// Load the state saved at `path` for this same query. `want` is the
    /// hash the destination recorded, when it recorded one. Any mismatch is
    /// an error naming it: the caller rebuilds by a full first run. A join
    /// view's buckets are read when a change reaches them, each checked
    /// against the hash the manifest holds for it.
    pub(crate) fn load(&mut self, path: &Path, want: Option<[u8; 32]>) -> Result<Vec<String>> {
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", path.display()));
        let mut buf = Vec::new();
        std::fs::File::open(path).map_err(io)?.read_to_end(&mut buf).map_err(io)?;
        let stale = |why: &str| Error::Transfer(format!("view state {}: {why}; rebuild it with a full run", path.display()));
        if buf.len() < 8 + 4 + 32 + 32 || &buf[..8] != MAGIC {
            return Err(stale("not an apitap view state file of this version"));
        }
        let (body, trailer) = buf.split_at(buf.len() - 32);
        let got: [u8; 32] = Sha256::digest(body).into();
        if got != trailer {
            return Err(stale("its content hash does not match (torn or edited)"));
        }
        if want.is_some_and(|h| h != got) {
            return Err(stale("it is not the state the destination recorded"));
        }
        let mut at = 8;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = body.get(at..at + n).ok_or_else(|| stale("it ends early"))?;
            at += n;
            Ok(s)
        };
        let u32_ = |b: &[u8]| u32::from_le_bytes(b.try_into().expect("4"));
        let u64_ = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8"));
        let i64_ = |b: &[u8]| i64::from_le_bytes(b.try_into().expect("8"));
        if u32_(take(4)?) != VERSION {
            return Err(stale("it was written by another state version"));
        }
        if take(32)? != self.fp {
            return Err(stale("it was built for another query or source schema"));
        }
        let mut positions = Vec::new();
        for _ in 0..u64_(take(8)?) {
            let n = u64_(take(8)?) as usize;
            positions.push(String::from_utf8(take(n)?.to_vec()).map_err(|_| stale("a position is not UTF-8"))?);
        }
        let na = u64_(take(8)?) as usize;
        if na != self.p.aggs.len() {
            return Err(stale("its aggregate count differs"));
        }
        let mut st = State::default();
        for _ in 0..u64_(take(8)?) {
            let n = u64_(take(8)?) as usize;
            let k = take(n)?.to_vec();
            st.groups.id(&k)?;
            st.rn.push(i64_(take(8)?));
            for _ in 0..na {
                st.r.push(get_acc(take(32)?));
            }
        }
        let store = match take(1)?[0] {
            0 => None,
            _ => {
                let seq = u64_(take(8)?);
                let mut splits = Vec::new();
                for _ in 0..u64_(take(8)?) {
                    splits.push(i64_(take(8)?));
                }
                let mut files = Vec::new();
                let mut ovls = Vec::new();
                let bfile = |take: &mut dyn FnMut(usize) -> Result<Vec<u8>>| -> Result<BFile> {
                    let n = u64_(&take(8)?) as usize;
                    let name = String::from_utf8(take(n)?).map_err(|_| stale("a bucket file name is not UTF-8"))?;
                    let hash: [u8; 32] = take(32)?.try_into().expect("32");
                    Ok(BFile { name, hash, bytes: u64_(&take(8)?) })
                };
                let mut tk = |n: usize| take(n).map(<[u8]>::to_vec);
                for _ in 0..=splits.len() {
                    files.push(match tk(1)?[0] {
                        0 => None,
                        _ => Some(bfile(&mut tk)?),
                    });
                    let mut o = Vec::new();
                    for _ in 0..u64_(&tk(8)?) {
                        o.push(bfile(&mut tk)?);
                    }
                    ovls.push(o);
                }
                Some(Store::with(path.with_extension("d"), splits, files, ovls, seq))
            }
        };
        if at != body.len() {
            return Err(stale("it has trailing bytes"));
        }
        if self.p.keys.is_some() != store.is_some() {
            return Err(stale("its shape does not match the query"));
        }
        self.st = st;
        self.store = store;
        Ok(positions)
    }

}

/// A bulk first run's staging. Every bootstrap row is an insert, so instead
/// of moving the state change by change (a hash map per bucket, a log
/// replayed through it), the rows go out as fixed records, `G` as (key,
/// group) and `S` as (key, rows, accumulators), into per-bucket files; then
/// each bucket is sorted, its `S` duplicates summed, `G` and `S` merge-joined
/// into `R`, and written as bases. `S` records may arrive already summed per
/// key (a server-side GROUP BY) or one per row.
pub(crate) struct Bulk {
    dir: PathBuf,
    splits: Vec<i64>,
    g: Vec<Vec<u8>>,
    s: Vec<Vec<u8>>,
    gf: Vec<bool>,
    sf: Vec<bool>,
    staged: u64,
    budget: u64,
    /// The staged `S` records' layout ([`s_layout`]) and length.
    slay: Vec<u8>,
    srec: usize,
    /// Groups seen so far (up to 64), encoded key and id: a row in one of
    /// them skips the hash lookup and its allocation.
    seen: Vec<(Vec<u8>, u32)>,
    /// The same for one text group column, by its raw bytes.
    seen_raw: Vec<(Vec<u8>, u32)>,
    /// One `S` record being built, reused key to key.
    rec: Vec<u8>,
    /// The `S` key being summed from rows (key, rows, accumulators): rows
    /// come in their table's order, most often a key's rows together, so a
    /// key is one record instead of one per row.
    cur: Option<(i64, i64, Vec<IAcc>)>,
    /// Per bucket: the `G` chunk being staged's last key and rows, and the
    /// bucket's rows in all. `G` is staged as chunks of delta-varint
    /// records (key change, group), a pipe's keys being in order: ~2 bytes
    /// a row, not 12 (a 34M-row side staged 70 MB, not 408).
    gprev: Vec<i64>,
    gchunk: Vec<u64>,
    grows: Vec<u64>,
    /// The last bucket a key fell in, as `[lo, hi)` and its index: a pipe
    /// reads its key range in order, so the next key is most often there.
    last: (i64, i64, usize),
    /// `G` rows and `S` records staged.
    pub(crate) rows: [u64; 2],
}

impl Bulk {
    pub(crate) fn srec(&self) -> usize {
        self.srec
    }

    #[inline]
    fn bucket_of(&mut self, k: i64) -> usize {
        let (lo, hi, b) = self.last;
        if lo <= k && k < hi {
            return b;
        }
        let b = self.splits.partition_point(|&s| s <= k);
        let lo = if b == 0 { i64::MIN } else { self.splits[b - 1] };
        self.last = (lo, self.splits.get(b).copied().unwrap_or(i64::MAX), b);
        b
    }

    fn path(&self, side: char, b: usize) -> PathBuf {
        self.dir.join(format!("bulk-{side}-{b}.bin"))
    }

    fn put_g(&mut self, key: i64, g: u32) {
        let b = self.bucket_of(key);
        let d = key.wrapping_sub(self.gprev[b]);
        self.gprev[b] = key;
        let v = &mut self.g[b];
        let before = v.len();
        put_varint(v, ((d << 1) ^ (d >> 63)) as u64);
        put_varint(v, u64::from(g));
        self.staged += (v.len() - before) as u64;
        self.gchunk[b] += 1;
        self.grows[b] += 1;
        self.rows[0] += 1;
    }

    fn put_s(&mut self, rec: &[u8]) {
        let key = i64::from_le_bytes(rec[..8].try_into().expect("8"));
        let b = self.bucket_of(key);
        self.s[b].extend_from_slice(rec);
        self.staged += rec.len() as u64;
        self.rows[1] += 1;
    }

    /// Staged bytes past a quarter of the budget (a vector's doubling slack
    /// makes that up to half) go to the buckets' files.
    fn maybe_spill(&mut self) -> Result<()> {
        if self.staged > self.budget / 4 {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        for b in 0..self.g.len() {
            if !self.g[b].is_empty() {
                // A chunk: its rows and bytes, then its records.
                let body = std::mem::take(&mut self.g[b]);
                let mut w = Vec::with_capacity(16 + body.len());
                w.extend_from_slice(&self.gchunk[b].to_le_bytes());
                w.extend_from_slice(&(body.len() as u64).to_le_bytes());
                w.extend_from_slice(&body);
                append(&self.path('g', b), &w)?;
                self.gf[b] = true;
                self.gprev[b] = 0;
                self.gchunk[b] = 0;
            }
            if !self.s[b].is_empty() {
                append(&self.path('s', b), &std::mem::take(&mut self.s[b]))?;
                self.sf[b] = true;
            }
        }
        self.staged = 0;
        Ok(())
    }
}

fn append(path: &Path, data: &[u8]) -> Result<()> {
    let io = |e: std::io::Error| Error::Transfer(format!("view bulk {}: {e}", path.display()));
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).map_err(io)?;
    f.write_all(data).map_err(io)?;
    start_writeback(&f);
    Ok(())
}

/// Write a file's pages to disk now and drop them from the page cache.
/// The cache counts against the cgroup's memory: left dirty, a gigabyte of
/// bases filled a 256 MiB cage and every allocation then waited on reclaim
/// (45 s of writing that a disk does in 3); left clean it still sat at the
/// limit. A data-only flush (no journal commit, unlike fsync) of a few MiB
/// is a few milliseconds. The final fdatasync still makes it durable.
fn start_writeback(f: &std::fs::File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: a valid open descriptor; the calls only do I/O and advise.
        unsafe {
            libc::sync_file_range(
                f.as_raw_fd(),
                0,
                0,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            );
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
}

/// A synced file's pages out of the cache (they count against the cgroup).
fn drop_cache(f: &std::fs::File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: a valid open descriptor; advice only.
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
}

fn put_varint(w: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        w.push(v as u8 | 0x80);
        v >>= 7;
    }
    w.push(v as u8);
}

fn get_varint(b: &[u8], at: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    let mut sh = 0;
    loop {
        let x = *b.get(*at).ok_or_else(|| bad("a truncated bulk G record"))?;
        *at += 1;
        v |= u64::from(x & 0x7f) << sh;
        if x < 0x80 {
            return Ok(v);
        }
        sh += 7;
        if sh > 63 {
            return Err(bad("a bulk G varint past 64 bits"));
        }
    }
}

/// A bucket's staged `G` chunks as (key, group) pairs, in staging order.
fn read_g_chunks(path: &Path, rows: u64) -> Result<Vec<(i64, u32)>> {
    let b = read_all(path)?;
    let mut out = Vec::with_capacity(rows as usize);
    let mut at = 0usize;
    while at < b.len() {
        let head = b.get(at..at + 16).ok_or_else(|| bad("a truncated bulk G chunk"))?;
        let n = u64::from_le_bytes(head[..8].try_into().expect("8"));
        let len = u64::from_le_bytes(head[8..].try_into().expect("8")) as usize;
        at += 16;
        let end = at.checked_add(len).filter(|&e| e <= b.len()).ok_or_else(|| bad("a truncated bulk G chunk"))?;
        let mut prev = 0i64;
        for _ in 0..n {
            let z = get_varint(&b, &mut at)?;
            let d = ((z >> 1) as i64) ^ -((z & 1) as i64);
            prev = prev.wrapping_add(d);
            let g = u32::try_from(get_varint(&b, &mut at)?).map_err(|_| bad("a bulk G group past u32"))?;
            out.push((prev, g));
        }
        if at != end {
            return Err(bad("a bulk G chunk of the wrong length"));
        }
    }
    if out.len() as u64 != rows {
        return Err(bad("a bulk G file missing rows"));
    }
    Ok(out)
}

fn read_all(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| Error::Transfer(format!("view bulk {}: {e}", path.display())))
}

/// The bases a bulk first run writes, in key order.
struct BulkOut {
    dir: PathBuf,
    splits: Vec<i64>,
    files: Vec<Option<BFile>>,
    seq: u64,
    block: usize,
    budget: u64,
    /// Seconds reading record files, sorting and joining, writing bases.
    t: [f64; 3],
}

impl BulkOut {
    fn name(&mut self) -> String {
        self.seq += 1;
        format!("b-{:010}.bkt", self.seq)
    }
}

impl View {
    /// The staged `S` layout of a bulk run: per aggregate, a server-side
    /// GROUP BY ships a count and an int8 sum (`sums`) or nothing (COUNT(*),
    /// whose count is the rows); a row read here keeps everything.
    pub(crate) fn bulk_layout(&self, sums: Option<&[bool]>) -> Vec<u8> {
        match sums {
            Some(s) => s.iter().map(|&x| if x { 1 | (1 << 1) } else { 0 }).collect(),
            None => vec![1 | (2 << 1) | 8; self.p.aggs.len()],
        }
    }

    /// The GROUP BY side's columns a bulk run reads: its join key, then its
    /// group columns (its WHERE runs on the server).
    pub(crate) fn bulk_g_columns(&self) -> Vec<String> {
        let s = self.p.group_side;
        let side = &self.p.sides[s];
        let at = self.p.keys.expect("a join").iter().find(|x| x.0 == s).expect("keyed").1;
        std::iter::once(at).chain(self.p.groups.iter().copied()).map(|c| side.schema[side.read[c]].name.clone()).collect()
    }

    /// The other side's columns a bulk run reads to sum its rows here: its
    /// join key, then the columns its aggregates' arguments name (its WHERE
    /// runs on the server, so the columns only that names are not shipped).
    pub(crate) fn bulk_s_columns(&self) -> Vec<String> {
        let side = &self.p.sides[1 - self.p.group_side];
        self.bulk_s_read().iter().map(|&c| side.schema[side.read[c]].name.clone()).collect()
    }

    /// [`Self::bulk_s_columns`] as positions in the side's read list.
    fn bulk_s_read(&self) -> Vec<usize> {
        let s = 1 - self.p.group_side;
        let at = self.p.keys.expect("a join").iter().find(|x| x.0 == s).expect("keyed").1;
        let mut out = vec![at];
        for a in &self.p.aggs {
            if let Some(e) = &a.arg {
                e.cols(&mut out);
            }
        }
        out
    }

    /// The aggregates' arguments re-pointed at [`Self::bulk_s_columns`].
    fn bulk_s_args(&self) -> Vec<Option<VExpr>> {
        let at = self.bulk_s_read();
        self.p.aggs.iter().map(|a| a.arg.as_ref().map(|e| e.remap(&at))).collect()
    }

    /// A bulk first run needs a join view on disk whose every WHERE
    /// condition runs on its server (bulk rows are not re-checked here).
    /// `APITAP_SQL_VIEW_BULK=0` turns it off.
    pub(crate) fn bulk_ok(&self) -> bool {
        self.store.is_some()
            && self.p.keys.is_some()
            && self.p.sides.iter().enumerate().all(|(i, s)| s.local.is_empty() && s.conj.len() == self.preds[i].len())
            && !std::env::var("APITAP_SQL_VIEW_BULK").is_ok_and(|v| v == "0")
    }

    pub(crate) fn bulk_begin(&self, slay: Vec<u8>) -> Result<Bulk> {
        let st = self.store.as_ref().ok_or_else(|| bad("a bulk first run without a disk state"))?;
        let n = st.splits.len() + 1;
        Ok(Bulk {
            dir: st.dir.clone(),
            splits: st.splits.clone(),
            g: vec![Vec::new(); n],
            s: vec![Vec::new(); n],
            gf: vec![false; n],
            sf: vec![false; n],
            staged: 0,
            budget: st.budget,
            srec: s_rec_len(&slay),
            slay,
            seen: Vec::new(),
            seen_raw: Vec::new(),
            rec: Vec::new(),
            cur: None,
            gprev: vec![0; n],
            gchunk: vec![0; n],
            grows: vec![0; n],
            last: (0, 0, 0),
            rows: [0, 0],
        })
    }

    /// The GROUP BY side's rows, WHERE pushed: read as
    /// [`View::bulk_g_columns`] (`narrow`), or as [`View::columns`].
    pub(crate) fn bulk_g(&mut self, bk: &mut Bulk, b: &ArrowBatch, narrow: bool) -> Result<()> {
        let s = self.p.group_side;
        let (at, gcols): (usize, Vec<usize>) = if narrow {
            (0, (1..=self.p.groups.len()).collect())
        } else {
            (self.p.keys.expect("a join").iter().find(|x| x.0 == s).expect("keyed").1, self.p.groups.clone())
        };
        let mut kb = Vec::with_capacity(64);
        let mut seen = std::mem::take(&mut bk.seen);
        // An integer key column read straight, not through a cell match
        // per row (the G side is tens of millions of rows).
        let (i64s, i32s, valid): (&[i64], &[i32], Option<&[u8]>) = match &b.cols[at] {
            FinishedCol::I64 { validity, data } => (data, &[], validity.as_deref()),
            FinishedCol::I32 { validity, data } => (&[], data, validity.as_deref()),
            _ => (&[], &[], None),
        };
        let typed = !i64s.is_empty() || !i32s.is_empty();
        // One text group column: a row's raw bytes are matched against the
        // groups seen so far, and only a new one is encoded and looked up
        // (the same encoding and id as below, so the state is the same).
        let text: Option<(Option<&[u8]>, &[i32], &[u8])> = match gcols.as_slice() {
            [c] => match &b.cols[*c] {
                FinishedCol::Utf8 { validity, offsets, data } | FinishedCol::Bin { validity, offsets, data } => {
                    Some((validity.as_deref(), offsets, data))
                }
                _ => None,
            },
            _ => None,
        };
        let mut seen_raw = std::mem::take(&mut bk.seen_raw);
        for r in 0..b.rows {
            let key = if typed {
                if valid.is_some_and(|bm| bm[r / 8] & (1 << (r % 8)) == 0) {
                    continue;
                }
                if i64s.is_empty() { i64::from(i32s[r]) } else { i64s[r] }
            } else {
                match key_at(&b.cols[at], r)? {
                    Some(k) => k,
                    None => continue,
                }
            };
            let raw = text.and_then(|(v, o, d)| {
                v.is_none_or(|bm| bm[r / 8] & (1 << (r % 8)) != 0).then(|| &d[o[r] as usize..o[r + 1] as usize])
            });
            if let Some(raw) = raw {
                if let Some(x) = seen_raw.iter().find(|x| x.0 == raw) {
                    bk.put_g(key, x.1);
                    continue;
                }
            }
            kb.clear();
            for &g in &gcols {
                encode_group(&b.cols[g], r, &mut kb);
            }
            let g = match seen.iter().find(|x| x.0 == kb) {
                Some(x) => x.1,
                None => {
                    let g = self.st.groups.id(&kb)? as usize;
                    let na = self.p.aggs.len();
                    if self.st.rn.len() <= g {
                        self.st.rn.resize(g + 1, 0);
                        self.st.r.resize((g + 1) * na, IAcc::default());
                    }
                    if seen.len() < 64 {
                        seen.push((kb.clone(), g as u32));
                    }
                    g as u32
                }
            };
            if let Some(raw) = raw {
                if seen_raw.len() < 64 {
                    seen_raw.push((raw.to_vec(), g));
                }
            }
            bk.put_g(key, g);
        }
        bk.seen = seen;
        bk.seen_raw = seen_raw;
        bk.maybe_spill()
    }

    /// The other side's rows, one `S` record each.
    /// The other side's rows summed per key: `narrow` when the batch holds
    /// [`Self::bulk_s_columns`] only, else the side's whole read list.
    pub(crate) fn bulk_s_rows(&mut self, bk: &mut Bulk, b: &ArrowBatch, narrow: bool) -> Result<()> {
        let s = 1 - self.p.group_side;
        let at = if narrow { 0 } else { self.p.keys.expect("a join").iter().find(|x| x.0 == s).expect("keyed").1 };
        let na = self.p.aggs.len();
        let nargs = if narrow { Some(self.bulk_s_args()) } else { None };
        let args: Vec<Option<&VExpr>> = match &nargs {
            Some(v) => v.iter().map(Option::as_ref).collect(),
            None => self.p.aggs.iter().map(|a| a.arg.as_ref()).collect(),
        };
        // Each exact argument evaluated over the batch at once; the rest row
        // by row.
        let cols: Vec<Option<(Vec<i128>, Vec<bool>)>> = self
            .p
            .aggs
            .iter()
            .zip(&args)
            .map(|(a, e)| match (e, a.f) {
                (Some(e), f) if f != AggFn::CountStar => eval_exact(e, b),
                _ => Ok(None),
            })
            .collect::<Result<_>>()?;
        let (i64s, i32s, valid): (&[i64], &[i32], Option<&[u8]>) = match &b.cols[at] {
            FinishedCol::I64 { validity, data } => (data, &[], validity.as_deref()),
            FinishedCol::I32 { validity, data } => (&[], data, validity.as_deref()),
            _ => (&[], &[], None),
        };
        let typed = !i64s.is_empty() || !i32s.is_empty();
        let over = || bad("an incremental SUM overflowed 38 decimal digits");
        let mut cur = bk.cur.take();
        for r in 0..b.rows {
            let key = if typed {
                if valid.is_some_and(|bm| bm[r / 8] & (1 << (r % 8)) == 0) {
                    continue;
                }
                if i64s.is_empty() { i64::from(i32s[r]) } else { i64s[r] }
            } else {
                match key_at(&b.cols[at], r)? {
                    Some(k) => k,
                    None => continue,
                }
            };
            // A new key: the last one's sum out, its accumulators reused.
            if let Some(c) = cur.as_mut().filter(|c| c.0 != key) {
                Self::put_s_sum(bk, c.0, c.1, &c.2)?;
                c.0 = key;
                c.1 = 0;
                c.2.fill(IAcc::default());
            }
            let c = cur.get_or_insert_with(|| (key, 0, vec![IAcc::default(); na]));
            c.1 += 1;
            for (i, a) in self.p.aggs.iter().enumerate() {
                match &cols[i] {
                    // IAcc::add of an Int or a Dec, inline.
                    Some((v, ok)) => {
                        if ok[r] {
                            c.2[i].n += 1;
                            c.2[i].i = c.2[i].i.checked_add(v[r]).ok_or_else(over)?;
                        }
                    }
                    None => {
                        let v = match args[i] {
                            Some(e) => eval(e, b, r)?,
                            None => Num::Null,
                        };
                        c.2[i].add(a.f, v, 1)?;
                    }
                }
            }
        }
        bk.cur = cur;
        bk.maybe_spill()
    }

    /// One key's summed rows as an `S` record in the staged layout. A sum
    /// the narrow layout cannot hold (past int8, or a float) fails the run,
    /// which the caller reruns with the wide layout.
    fn put_s_sum(bk: &mut Bulk, k: i64, n: i64, accs: &[IAcc]) -> Result<()> {
        for (a, &l) in accs.iter().zip(&bk.slay) {
            if ((l >> 1) & 3 == 1 && i64::try_from(a.i).is_err()) || (l & 8 == 0 && a.f != 0.0) || (l & 1 == 0 && a.n != n) {
                return Err(bad("a per-key sum out of range of the narrow bulk layout"));
            }
        }
        let mut rec = std::mem::take(&mut bk.rec);
        rec.clear();
        put_s_rec(&mut rec, k, n, accs, &bk.slay);
        bk.put_s(&rec);
        bk.rec = rec;
        Ok(())
    }

    /// `S` records already built (a server-side GROUP BY per key; a key may
    /// still come in several, one per part).
    pub(crate) fn bulk_s_recs(&mut self, bk: &mut Bulk, recs: &[u8]) -> Result<()> {
        if recs.len() % bk.srec != 0 {
            return Err(bad("a bulk S record batch of a ragged length"));
        }
        for r in recs.chunks_exact(bk.srec) {
            bk.put_s(r);
        }
        bk.maybe_spill()
    }

    /// Settle every bucket into bases and `R`; the state is then what a
    /// change-by-change first run leaves, ready to save.
    pub(crate) fn bulk_finish(&mut self, mut bk: Bulk) -> Result<()> {
        if let Some((k, n, accs)) = bk.cur.take() {
            Self::put_s_sum(&mut bk, k, n, &accs)?;
        }
        bk.spill()?;
        let store = self.store.take().ok_or_else(|| bad("a bulk first run without a disk state"))?;
        let mut out = BulkOut { dir: store.dir.clone(), splits: Vec::new(), files: Vec::new(), seq: store.seq, block: store.block, budget: store.budget, t: [0.0; 3] };
        for b in 0..bk.g.len() {
            let s = bk.sf[b].then(|| bk.path('s', b));
            let ssize = s.as_ref().and_then(|p| std::fs::metadata(p).ok()).map_or(0, |m| m.len());
            if !bk.gf[b] {
                self.settle(&mut out, &bk.slay, None, None, s, 0)?;
                continue;
            }
            let gp = bk.path('g', b);
            let t = std::time::Instant::now();
            let gv = read_g_chunks(&gp, bk.grows[b])?;
            let _ = std::fs::remove_file(&gp);
            if bk.grows[b] * 12 + ssize > 2 * out.budget {
                // Past what a bucket may hold: as fixed records, for the
                // settle to cut in two by key.
                let mut w = Vec::with_capacity(gv.len() * 12);
                for (k, g) in &gv {
                    w.extend_from_slice(&k.to_le_bytes());
                    w.extend_from_slice(&g.to_le_bytes());
                }
                drop(gv);
                let fx = gp.with_extension("fx");
                append(&fx, &w)?;
                out.t[0] += t.elapsed().as_secs_f64();
                self.settle(&mut out, &bk.slay, Some(fx), None, s, 0)?;
            } else {
                out.t[0] += t.elapsed().as_secs_f64();
                self.settle(&mut out, &bk.slay, None, Some(gv), s, 0)?;
            }
        }
        let t = std::time::Instant::now();
        for f in out.files.iter().flatten() {
            let p = out.dir.join(&f.name);
            let f = std::fs::File::open(&p).map_err(|e| Error::Transfer(format!("view state {}: {e}", p.display())))?;
            f.sync_data().map_err(|e| Error::Transfer(format!("view state {}: {e}", p.display())))?;
            drop_cache(&f);
        }
        if std::env::var_os("APITAP_DEBUG").is_some() {
            eprintln!(
                "[sql] view bulk: {} base(s): read {:.1}s, sort+join {:.1}s, write {:.1}s, sync {:.1}s",
                out.files.len(),
                out.t[0],
                out.t[1],
                out.t[2],
                t.elapsed().as_secs_f64()
            );
        }
        if out.files.is_empty() {
            out.files.push(None);
        }
        let n = out.files.len();
        let mut st = Store::with(out.dir, out.splits, out.files, vec![Vec::new(); n], out.seq);
        st.budget = store.budget;
        st.block = store.block;
        st.full = n;
        self.store = Some(st);
        Ok(())
    }

    /// One bucket's record files into bases. Past twice the budget it is
    /// cut in two by key first (streamed, so memory holds one half at most).
    /// `G` comes as a file of fixed (key, group) records, or already read
    /// (`gmem`, a bucket that fits).
    fn settle(&mut self, out: &mut BulkOut, slay: &[u8], g: Option<PathBuf>, gmem: Option<Vec<(i64, u32)>>, s: Option<PathBuf>, depth: u32) -> Result<()> {
        let sl = s_rec_len(slay);
        let size = |p: &Option<PathBuf>| p.as_ref().and_then(|p| std::fs::metadata(p).ok()).map_or(0, |m| m.len());
        if gmem.is_none() && size(&g) + size(&s) > 2 * out.budget && depth < 48 {
            let (mut lo, mut hi) = (i64::MAX, i64::MIN);
            for (p, rl) in [(&g, 12), (&s, sl)] {
                if let Some(p) = p {
                    scan(p, rl, |r| {
                        let k = i64::from_le_bytes(r[..8].try_into().expect("8"));
                        lo = lo.min(k);
                        hi = hi.max(k);
                    })?;
                }
            }
            if lo < hi {
                let mid = ((lo as i128 + hi as i128).div_euclid(2)) as i64;
                let mut halves = Vec::new();
                for (p, rl) in [(&g, 12), (&s, sl)] {
                    let Some(p) = p else {
                        halves.push((None, None));
                        continue;
                    };
                    let (pl, ph) = (p.with_extension(format!("{depth}l")), p.with_extension(format!("{depth}h")));
                    let (mut wl, mut wh) = (Vec::new(), Vec::new());
                    let (mut nl, mut nh) = (false, false);
                    let mut io_err = None;
                    scan(p, rl, |r| {
                        let k = i64::from_le_bytes(r[..8].try_into().expect("8"));
                        let (w, path, n) = if k <= mid { (&mut wl, &pl, &mut nl) } else { (&mut wh, &ph, &mut nh) };
                        w.extend_from_slice(r);
                        if w.len() >= 1 << 20 {
                            if let Err(e) = append(path, w) {
                                io_err.get_or_insert(e);
                            }
                            w.clear();
                            *n = true;
                        }
                        if !w.is_empty() {
                            *n = true;
                        }
                    })?;
                    if let Some(e) = io_err {
                        return Err(e);
                    }
                    if !wl.is_empty() {
                        append(&pl, &wl)?;
                    }
                    if !wh.is_empty() {
                        append(&ph, &wh)?;
                    }
                    let _ = std::fs::remove_file(p);
                    halves.push((nl.then_some(pl), nh.then_some(ph)));
                }
                let (gl, gh) = halves.remove(0);
                let (sl_, sh) = halves.remove(0);
                self.settle(out, slay, gl, None, sl_, depth + 1)?;
                return self.settle(out, slay, gh, None, sh, depth + 1);
            }
        }
        let na = self.p.aggs.len();
        let t = std::time::Instant::now();
        let mut gv: Vec<(i64, u32)> = match (gmem, &g) {
            (Some(v), _) => v,
            (None, Some(p)) => read_all(p)?
                .chunks_exact(12)
                .map(|r| (i64::from_le_bytes(r[..8].try_into().expect("8")), u32::from_le_bytes(r[8..12].try_into().expect("4"))))
                .collect(),
            (None, None) => Vec::new(),
        };
        let sb = match &s {
            Some(p) => read_all(p)?,
            None => Vec::new(),
        };
        out.t[0] += t.elapsed().as_secs_f64();
        let t = std::time::Instant::now();
        gv.sort_unstable_by_key(|x| x.0);
        if let Some(w) = gv.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(unsupported(format!(
                "the join key {} twice on the GROUP BY table: mode=\"log_based\" v1 needs that \
                 table's join key unique (its primary key)",
                w[0].0
            )));
        }
        for p in g.iter().chain(s.iter()) {
            let _ = std::fs::remove_file(p);
        }
        // (key, record) pairs sorted in place: no reach back into the
        // records for every comparison.
        let mut order: Vec<(i64, u32)> =
            sb.chunks_exact(sl).enumerate().map(|(i, r)| (i64::from_le_bytes(r[..8].try_into().expect("8")), i as u32)).collect();
        order.sort_unstable();
        let (mut sk, mut sn, mut sacc): (Vec<i64>, Vec<i64>, Vec<IAcc>) = (Vec::new(), Vec::new(), Vec::new());
        sk.reserve(order.len());
        sn.reserve(order.len());
        sacc.reserve(order.len() * na);
        let mut one = vec![IAcc::default(); na];
        for &(_, i) in &order {
            let r = &sb[i as usize * sl..(i as usize + 1) * sl];
            let (k, n) = get_s_rec(r, slay, &mut one);
            if sk.last() == Some(&k) {
                *sn.last_mut().expect("a key") += n;
                let at = sacc.len() - na;
                for a in 0..na {
                    sacc[at + a].merge(&one[a], 1)?;
                }
            } else {
                sk.push(k);
                sn.push(n);
                sacc.extend_from_slice(&one);
            }
        }
        drop(order);
        drop(sb);
        // R: every key both sides hold.
        let (mut i, mut j) = (0, 0);
        while i < gv.len() && j < sk.len() {
            match gv[i].0.cmp(&sk[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    let g = gv[i].1 as usize;
                    self.st.rn[g] += sn[j];
                    for a in 0..na {
                        let src = sacc[j * na + a];
                        self.st.r[g * na + a].merge(&src, 1)?;
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        out.t[1] += t.elapsed().as_secs_f64();
        let t = std::time::Instant::now();
        // Bases of at most half the budget once loaded, cut between keys.
        let target = (out.budget / 2).max(1);
        let per_s = 32 + 48 * na as u64;
        let (mut gs, mut ss, mut gi, mut si, mut mem) = (0, 0, 0, 0, 0u64);
        loop {
            let next = match (gv.get(gi), sk.get(si)) {
                (None, None) => break,
                (Some(a), None) => a.0,
                (None, Some(&b)) => b,
                (Some(a), Some(&b)) => a.0.min(b),
            };
            if mem > target && (gi > gs || si > ss) {
                self.emit(out, &gv[gs..gi], &sk[ss..si], &sn[ss..si], &sacc[ss * na..si * na])?;
                (gs, ss, mem) = (gi, si, 0);
            }
            if gv.get(gi).is_some_and(|x| x.0 == next) {
                gi += 1;
                mem += 24;
            }
            if sk.get(si) == Some(&next) {
                si += 1;
                mem += per_s;
            }
        }
        if gi > gs || si > ss {
            self.emit(out, &gv[gs..gi], &sk[ss..si], &sn[ss..si], &sacc[ss * na..si * na])?;
        }
        out.t[2] += t.elapsed().as_secs_f64();
        Ok(())
    }

    fn emit(&self, out: &mut BulkOut, g: &[(i64, u32)], sk: &[i64], sn: &[i64], sacc: &[IAcc]) -> Result<()> {
        let first = match (g.first(), sk.first()) {
            (Some(a), Some(&b)) => a.0.min(b),
            (Some(a), None) => a.0,
            (None, Some(&b)) => b,
            (None, None) => return Ok(()),
        };
        let name = out.name();
        let f = self.write_sorted(&out.dir, name, out.block, g, sk, sn, sacc)?;
        if !out.files.is_empty() {
            out.splits.push(first);
        }
        out.files.push(Some(f));
        Ok(())
    }
}

/// Every `rl`-byte record of a file, in order.
fn scan(path: &Path, rl: usize, mut each: impl FnMut(&[u8])) -> Result<()> {
    let io = |e: std::io::Error| Error::Transfer(format!("view bulk {}: {e}", path.display()));
    let mut f = std::io::BufReader::with_capacity(1 << 20, std::fs::File::open(path).map_err(io)?);
    let mut buf = vec![0u8; rl * 4096];
    let mut have = 0;
    loop {
        let n = f.read(&mut buf[have..]).map_err(io)?;
        if n == 0 {
            break;
        }
        have += n;
        let whole = have / rl * rl;
        for r in buf[..whole].chunks_exact(rl) {
            each(r);
        }
        buf.copy_within(whole..have, 0);
        have -= whole;
    }
    if have != 0 {
        return Err(bad("a bulk record file of a ragged length"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(name: &str, kind: ArrowKind) -> ArrowField {
        ArrowField { name: name.into(), kind, nullable: true, rewritten: false, padded: false, ci: false, ai: false }
    }

    fn schemas() -> impl FnMut(&str, &str) -> Result<Vec<ArrowField>> {
        |_u: &str, t: &str| {
            Ok(match t {
                "li" => vec![
                    f("id", ArrowKind::Int64),
                    f("k", ArrowKind::Int64),
                    f("price", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("d", ArrowKind::Int32),
                ],
                "ord" => vec![
                    f("k", ArrowKind::Int64),
                    f("prio", ArrowKind::Utf8),
                    f("flag", ArrowKind::Int32),
                    f("od", ArrowKind::Date32),
                    f("ts", ArrowKind::TimestampUtc),
                ],
                _ => return Err(bad("no such table")),
            })
        }
    }

    fn srcs() -> HashMap<String, String> {
        HashMap::from([
            ("pg".to_string(), "postgres://u@h/db".to_string()),
            ("my".to_string(), "mysql://u@h/db".to_string()),
            ("gh".to_string(), "github+api://o/r".to_string()),
        ])
    }

    const QJ: &str = "SELECT o.prio, SUM(l.price), COUNT(*), COUNT(l.price), AVG(l.price) \
        FROM pg.li l JOIN my.ord o ON l.k = o.k \
        WHERE l.d > 3 AND o.flag = 1 AND o.od < DATE '2020-01-10' GROUP BY 1 ORDER BY 1";

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
    }

    #[derive(Clone)]
    struct Li {
        k: Option<i64>,
        cents: Option<i64>,
        d: i32,
    }
    #[derive(Clone)]
    struct Or {
        prio: String,
        flag: i32,
        day: u32,
    }

    fn txt(v: impl std::fmt::Display) -> Option<Vec<u8>> {
        Some(v.to_string().into_bytes())
    }

    /// Cells of an image in the view's column order for side `s`.
    fn img(v: &View, s: usize, by: &dyn Fn(&str) -> Option<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        v.columns(s).iter().map(|c| by(c)).collect()
    }

    fn li_img(v: &View, id: i64, r: &Li) -> Vec<Option<Vec<u8>>> {
        img(v, 0, &|c| match c {
            "id" => txt(id),
            "k" => r.k.and_then(txt),
            "price" => r.cents.and_then(|c| txt(format!("{}{}.{:02}", if c < 0 { "-" } else { "" }, c.abs() / 100, c.abs() % 100))),
            "d" => txt(r.d),
            _ => None,
        })
    }

    fn or_img(v: &View, k: i64, r: &Or) -> Vec<Option<Vec<u8>>> {
        img(v, 1, &|c| match c {
            "k" => txt(k),
            "prio" => txt(&r.prio),
            "flag" => txt(r.flag),
            "od" => txt(format!("2020-01-{:02}", r.day)),
            _ => None,
        })
    }

    type Row = (String, Option<i128>, i64, i64, Option<f64>);

    fn rows_of(res: &SqlResult) -> Vec<Row> {
        let c = &res.batch.cols;
        (0..res.batch.rows)
            .map(|r| {
                let prio = String::from_utf8(cell_text(&c[0], r).unwrap().to_vec()).unwrap();
                let sum = match cell_num(&c[1], r) {
                    Num::Dec(x) => Some(x),
                    _ => None,
                };
                let n = |i: usize| match cell_num(&c[i], r) {
                    Num::Int(x) => x,
                    _ => panic!("count"),
                };
                let avg = match cell_num(&c[4], r) {
                    Num::Float(x) => Some(x),
                    _ => None,
                };
                (prio, sum, n(2), n(3), avg)
            })
            .collect()
    }

    fn naive(li: &HashMap<i64, Li>, or: &HashMap<i64, Or>) -> Vec<Row> {
        let mut m: std::collections::BTreeMap<String, (i128, i64, i64)> = Default::default();
        for r in li.values() {
            let Some(k) = r.k else { continue };
            if r.d <= 3 {
                continue;
            }
            let Some(o) = or.get(&k) else { continue };
            if o.flag != 1 || o.day >= 10 {
                continue;
            }
            let e = m.entry(o.prio.clone()).or_default();
            e.1 += 1;
            if let Some(c) = r.cents {
                e.0 += i128::from(c);
                e.2 += 1;
            }
        }
        m.into_iter()
            .map(|(p, (s, n, nn))| {
                (p, (nn > 0).then_some(s), n, nn, (nn > 0).then(|| s as f64 / 100.0 / nn as f64))
            })
            .collect()
    }

    fn same(a: &[Row], b: &[Row]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| {
                x.0 == y.0
                    && x.1 == y.1
                    && x.2 == y.2
                    && x.3 == y.3
                    && match (x.4, y.4) {
                        (Some(p), Some(q)) => (p - q).abs() < 1e-9 * p.abs().max(1.0),
                        (None, None) => true,
                        _ => false,
                    }
            })
    }

    /// Random inserts, deletes, updates, filter crossings and key moves on
    /// both sides; after every step the view equals a recompute by a naive
    /// model that shares no code with it.
    #[test]
    fn a_join_view_follows_random_changes_exactly() {
        for seed in 1..=6u64 {
            let mut rng = Rng(seed);
            let mut v = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
            let (mut li, mut or): (HashMap<i64, Li>, HashMap<i64, Or>) = Default::default();
            let mut next_id = 0i64;
            let prios = ["a", "b", "c", "d"];
            let rand_li = |rng: &mut Rng| Li {
                k: (rng.next(20) != 0).then(|| rng.next(40) as i64),
                cents: (rng.next(10) != 0).then(|| rng.next(100_000) as i64 - 20_000),
                d: rng.next(8) as i32,
            };
            let rand_or = |rng: &mut Rng| Or {
                prio: prios[rng.next(4) as usize].to_string(),
                flag: rng.next(3) as i32,
                day: 1 + rng.next(15) as u32,
            };
            for step in 0..600 {
                let mut ch: Vec<(usize, i64, Vec<Option<Vec<u8>>>)> = Vec::new();
                match rng.next(7) {
                    0 | 1 => {
                        let r = rand_li(&mut rng);
                        ch.push((0, 1, li_img(&v, next_id, &r)));
                        li.insert(next_id, r);
                        next_id += 1;
                    }
                    2 if !li.is_empty() => {
                        let id = *li.keys().nth(rng.next(li.len() as u64) as usize).unwrap();
                        let old = li.remove(&id).unwrap();
                        ch.push((0, -1, li_img(&v, id, &old)));
                    }
                    3 if !li.is_empty() => {
                        let id = *li.keys().nth(rng.next(li.len() as u64) as usize).unwrap();
                        let new = rand_li(&mut rng);
                        let old = li.insert(id, new.clone()).unwrap();
                        ch.push((0, -1, li_img(&v, id, &old)));
                        ch.push((0, 1, li_img(&v, id, &new)));
                    }
                    4 => {
                        let k = rng.next(45) as i64;
                        if let std::collections::hash_map::Entry::Vacant(e) = or.entry(k) {
                            let r = rand_or(&mut rng);
                            ch.push((1, 1, or_img(&v, k, &r)));
                            e.insert(r);
                        }
                    }
                    5 if !or.is_empty() => {
                        let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                        let old = or.remove(&k).unwrap();
                        ch.push((1, -1, or_img(&v, k, &old)));
                    }
                    6 if !or.is_empty() => {
                        // An update; a third of them move the key.
                        let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                        let new = rand_or(&mut rng);
                        let k2 = if rng.next(3) == 0 { rng.next(45) as i64 } else { k };
                        if k2 == k || !or.contains_key(&k2) {
                            let old = or.remove(&k).unwrap();
                            ch.push((1, -1, or_img(&v, k, &old)));
                            ch.push((1, 1, or_img(&v, k2, &new)));
                            or.insert(k2, new);
                        }
                    }
                    _ => {}
                }
                for (s, w, im) in ch {
                    v.apply(s, &[(w, im)]).unwrap();
                }
                let got = rows_of(&v.answer().unwrap());
                let want = naive(&li, &or);
                assert!(same(&got, &want), "seed {seed} step {step}:\n got {got:?}\nwant {want:?}");
            }
            // The state survives a save and a load, and the answer with it.
            let dir = std::env::temp_dir().join(format!("apitap-view-test-{}-{seed}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("v.state");
            let h = v.save(&path, &["0/16B3748".into(), "binlog.000003:4".into()]).unwrap();
            let mut w = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
            let pos = w.load(&path, Some(h)).unwrap();
            assert_eq!(pos, ["0/16B3748", "binlog.000003:4"]);
            assert!(same(&rows_of(&w.answer().unwrap()), &naive(&li, &or)));
            // A torn byte, another recorded hash, or another query is refused.
            let mut bytes = std::fs::read(&path).unwrap();
            let mid = bytes.len() / 2;
            bytes[mid] ^= 1;
            std::fs::write(&path, &bytes).unwrap();
            let e = w.load(&path, None).unwrap_err().to_string();
            assert!(e.contains("content hash"), "{e}");
            v.save(&path, &[]).unwrap();
            let e = w.load(&path, Some(h)).unwrap_err().to_string();
            assert!(e.contains("not the state the destination recorded"), "{e}");
            let mut other = View::plan(&QJ.replace("l.d > 3", "l.d > 4"), &srcs(), &mut schemas()).unwrap();
            let e = other.load(&path, None).unwrap_err().to_string();
            assert!(e.contains("another query"), "{e}");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// The same random traffic with `G`/`S` on disk in buckets far smaller
    /// than the state, so buckets split again and again; every step saves
    /// (flushes) and the state is reloaded into a fresh view twice midway.
    #[test]
    fn a_bucketed_join_view_follows_random_changes_and_reloads() {
        let dir = std::env::temp_dir().join(format!("apitap-view-bkt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.state");
        let mut rng = Rng(42);
        let fresh = |path: &Path| {
            let mut v = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
            v.begin(path, vec![10, 30]).unwrap();
            v.set_bucket_budget(600);
            v.set_block(24);
            v
        };
        // Buckets moved by an overlay and by a rewrite, over every view.
        let (mut sparse, mut full) = (0, 0);
        let mut v = fresh(&path);
        let (mut li, mut or): (HashMap<i64, Li>, HashMap<i64, Or>) = Default::default();
        let mut next_id = 0i64;
        let prios = ["a", "b", "c", "d"];
        for step in 0..400 {
            let mut ch: Vec<(usize, i64, Vec<Option<Vec<u8>>>)> = Vec::new();
            match rng.next(6) {
                0 | 1 => {
                    let r = Li { k: Some(rng.next(40) as i64), cents: Some(rng.next(9000) as i64), d: rng.next(8) as i32 };
                    ch.push((0, 1, li_img(&v, next_id, &r)));
                    li.insert(next_id, r);
                    next_id += 1;
                }
                2 if !li.is_empty() => {
                    let id = *li.keys().nth(rng.next(li.len() as u64) as usize).unwrap();
                    let old = li.remove(&id).unwrap();
                    ch.push((0, -1, li_img(&v, id, &old)));
                }
                3 => {
                    let k = rng.next(45) as i64;
                    if let std::collections::hash_map::Entry::Vacant(e) = or.entry(k) {
                        let r = Or { prio: prios[rng.next(4) as usize].into(), flag: rng.next(2) as i32, day: 1 + rng.next(15) as u32 };
                        ch.push((1, 1, or_img(&v, k, &r)));
                        e.insert(r);
                    }
                }
                4 if !or.is_empty() => {
                    let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                    let old = or.remove(&k).unwrap();
                    ch.push((1, -1, or_img(&v, k, &old)));
                }
                5 if !or.is_empty() => {
                    let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                    let new = Or { prio: prios[rng.next(4) as usize].into(), flag: rng.next(2) as i32, day: 1 + rng.next(15) as u32 };
                    let old = or.insert(k, new.clone()).unwrap();
                    ch.push((1, -1, or_img(&v, k, &old)));
                    ch.push((1, 1, or_img(&v, k, &new)));
                }
                _ => {}
            }
            for (s, w, im) in ch {
                v.apply(s, &[(w, im)]).unwrap();
            }
            v.save(&path, &[format!("p{step}")]).unwrap();
            let got = rows_of(&v.answer().unwrap());
            assert!(same(&got, &naive(&li, &or)), "step {step}: got {got:?}");
            if step == 150 || step == 300 {
                let mut w = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
                assert_eq!(w.load(&path, None).unwrap(), [format!("p{step}")]);
                w.set_bucket_budget(600);
                w.set_block(24);
                let (a, b) = v.flush_stats();
                (sparse, full) = (sparse + a, full + b);
                v = w;
            }
        }
        let (a, b) = v.flush_stats();
        (sparse, full) = (sparse + a, full + b);
        assert!(v.buckets() > 6, "the buckets split: {}", v.buckets());
        // Both moves ran: a window reading a few blocks into an overlay, and
        // a bucket rewritten whole (the overlays compacted, or a split).
        assert!(sparse > 20 && full > 20, "sparse {sparse}, full {full}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The bulk first run (fixed records, sorted per bucket, merge-joined)
    /// leaves the state a change-by-change one would: the same answer, and
    /// then the same answers under random changes after a save and reload.
    /// A tiny budget cuts buckets in two by key and bases into many pieces.
    #[test]
    fn a_bulk_first_run_equals_the_change_by_change_one() {
        for (seed, budget) in [(3u64, 300u64), (4, 5000), (5, 120), (6, 300)] {
            let dir = std::env::temp_dir().join(format!("apitap-view-bulk-{}-{seed}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("v.state");
            let mut rng = Rng(seed);
            let prios = ["a", "b", "c", "d"];
            let (mut li, mut or): (HashMap<i64, Li>, HashMap<i64, Or>) = Default::default();
            for id in 0..400i64 {
                li.insert(id, Li {
                    k: (rng.next(20) != 0).then(|| rng.next(90) as i64 - 10),
                    cents: (rng.next(10) != 0).then(|| rng.next(100_000) as i64 - 20_000),
                    d: rng.next(8) as i32,
                });
            }
            for k in -5..80i64 {
                if rng.next(4) != 0 {
                    or.insert(k, Or { prio: prios[rng.next(4) as usize].into(), flag: rng.next(3) as i32, day: 1 + rng.next(15) as u32 });
                }
            }
            let mut v = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
            v.begin(&path, vec![10, 30, 50]).unwrap();
            v.set_bucket_budget(budget);
            v.set_block(40);
            assert!(v.bulk_ok(), "every WHERE condition of QJ runs on its server");
            // Seed 4 ships `S` as a server-side GROUP BY would: one record
            // per key (count and int8 sum per summed aggregate, nothing for
            // COUNT(*)), split across two "parts" so a key comes twice.
            let server = seed == 4;
            // Seed 6 sums the rows here into that narrow layout, a key's
            // rows mostly together (sorted by key, as a heap often is).
            let narrow = seed == 6;
            let sums = [true, false, true, true];
            let mut bk = v.bulk_begin(v.bulk_layout((server || narrow).then_some(&sums[..]))).unwrap();
            // Only rows the pushed WHERE keeps reach a bulk run.
            let keep = |v: &View, s: usize, im: &Vec<Option<Vec<u8>>>| {
                let b = v.images(s, &[(1, im.clone())]).unwrap();
                v.preds[s].iter().all(|pe| peval(pe, &b, 0, &v.tys[s]) == Some(true))
            };
            let ors: Vec<_> = or.iter().map(|(&k, r)| or_img(&v, k, r)).filter(|im| keep(&v, 1, im)).collect();
            let mut lrows: Vec<_> = li.iter().collect();
            if narrow {
                lrows.sort_by_key(|(id, r)| (r.k, **id));
            }
            let lis: Vec<_> = lrows.iter().map(|&(&id, r)| li_img(&v, id, r)).filter(|im| keep(&v, 0, im)).collect();
            for c in ors.chunks(7) {
                let b = v.images(1, &c.iter().map(|im| (1, im.clone())).collect::<Vec<_>>()).unwrap();
                v.bulk_g(&mut bk, &b, false).unwrap();
            }
            if server {
                for part in 0..2 {
                    let mut per: std::collections::BTreeMap<i64, (i64, i64, i64)> = Default::default();
                    for (id, r) in &li {
                        if id % 2 != part || !keep(&v, 0, &li_img(&v, *id, r)) {
                            continue;
                        }
                        let Some(k) = r.k else { continue };
                        let e = per.entry(k).or_default();
                        e.0 += 1;
                        if let Some(c) = r.cents {
                            e.1 += 1;
                            e.2 += c;
                        }
                    }
                    let mut recs = Vec::new();
                    for (k, (rows, n, sum)) in per {
                        recs.extend_from_slice(&k.to_le_bytes());
                        recs.extend_from_slice(&rows.to_le_bytes());
                        for _ in 0..3 {
                            recs.extend_from_slice(&n.to_le_bytes());
                            recs.extend_from_slice(&sum.to_le_bytes());
                        }
                    }
                    v.bulk_s_recs(&mut bk, &recs).unwrap();
                }
            } else {
                for c in lis.chunks(11) {
                    let b = v.images(0, &c.iter().map(|im| (1, im.clone())).collect::<Vec<_>>()).unwrap();
                    if narrow {
                        // As a bulk read ships it: the key and the
                        // arguments' columns only.
                        let nb = ArrowBatch { rows: b.rows, cols: v.bulk_s_read().iter().map(|&c| b.cols[c].clone()).collect() };
                        v.bulk_s_rows(&mut bk, &nb, true).unwrap();
                    } else {
                        v.bulk_s_rows(&mut bk, &b, false).unwrap();
                    }
                }
            }
            v.bulk_finish(bk).unwrap();
            let got = rows_of(&v.answer().unwrap());
            assert!(same(&got, &naive(&li, &or)), "seed {seed}: bulk got {got:?}");
            v.save(&path, &["p".into()]).unwrap();
            if budget < 1000 {
                assert!(v.buckets() > 4, "seed {seed}: pieces {}", v.buckets());
            }
            let mut w = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
            w.load(&path, None).unwrap();
            w.set_bucket_budget(budget);
            w.set_block(40);
            let mut v = w;
            let mut next_id = 400i64;
            for step in 0..150 {
                let mut ch: Vec<(usize, i64, Vec<Option<Vec<u8>>>)> = Vec::new();
                match rng.next(5) {
                    0 => {
                        let r = Li { k: Some(rng.next(90) as i64 - 10), cents: Some(rng.next(9000) as i64), d: rng.next(8) as i32 };
                        ch.push((0, 1, li_img(&v, next_id, &r)));
                        li.insert(next_id, r);
                        next_id += 1;
                    }
                    1 if !li.is_empty() => {
                        let id = *li.keys().nth(rng.next(li.len() as u64) as usize).unwrap();
                        let old = li.remove(&id).unwrap();
                        ch.push((0, -1, li_img(&v, id, &old)));
                    }
                    2 if !or.is_empty() => {
                        let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                        let new = Or { prio: prios[rng.next(4) as usize].into(), flag: rng.next(3) as i32, day: 1 + rng.next(15) as u32 };
                        let old = or.insert(k, new.clone()).unwrap();
                        ch.push((1, -1, or_img(&v, k, &old)));
                        ch.push((1, 1, or_img(&v, k, &new)));
                    }
                    3 if !or.is_empty() => {
                        let k = *or.keys().nth(rng.next(or.len() as u64) as usize).unwrap();
                        let old = or.remove(&k).unwrap();
                        ch.push((1, -1, or_img(&v, k, &old)));
                    }
                    _ => {
                        let k = rng.next(95) as i64 - 10;
                        if let std::collections::hash_map::Entry::Vacant(e) = or.entry(k) {
                            let r = Or { prio: prios[rng.next(4) as usize].into(), flag: rng.next(3) as i32, day: 1 + rng.next(15) as u32 };
                            ch.push((1, 1, or_img(&v, k, &r)));
                            e.insert(r);
                        }
                    }
                }
                for (s, w, im) in ch {
                    v.apply(s, &[(w, im)]).unwrap();
                }
                v.save(&path, &[format!("p{step}")]).unwrap();
                let got = rows_of(&v.answer().unwrap());
                assert!(same(&got, &naive(&li, &or)), "seed {seed} step {step}: got {got:?}");
            }
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// A base's compact records read back exactly: counts that differ from
    /// the rows, sums past i64, float sums, and the narrow forms beside them.
    #[test]
    fn compact_base_records_round_trip() {
        let a = |n: i64, i: i128, f: f64| IAcc { n, i, f };
        let rows: Vec<(i64, i64, Vec<IAcc>)> = vec![
            (-7, 3, vec![a(3, 0, 0.0), a(2, 5, 0.0), a(3, i128::from(i64::MAX) * 4, 0.0), a(3, 0, -0.0)]),
            (11, 1, vec![a(1, 0, 0.0), a(1, -9, 0.0), a(1, 1, 2.5), a(1, 0, 0.0)]),
            (i64::MAX, 2, vec![a(2, 0, 0.0), a(0, 0, 0.0), a(2, -1, f64::NAN), a(2, 0, 0.0)]),
        ];
        let lay = s_layout(4, rows.iter().map(|r| (r.1, &r.2[..])));
        assert_eq!(lay, vec![0, 1 | (1 << 1), (2 << 1) | 8, 8]);
        let mut w = Vec::new();
        for (k, n, accs) in &rows {
            put_s_rec(&mut w, *k, *n, accs, &lay);
        }
        assert_eq!(w.len(), rows.len() * s_rec_len(&lay));
        for (r, (k, n, accs)) in w.chunks_exact(s_rec_len(&lay)).zip(&rows) {
            let mut out = vec![IAcc::default(); 4];
            assert_eq!(get_s_rec(r, &lay, &mut out), (*k, *n));
            for (x, y) in out.iter().zip(accs) {
                assert_eq!((x.n, x.i, x.f.to_bits()), (y.n, y.i, y.f.to_bits()));
            }
        }
        for (gmax, gw) in [(0u32, 1u8), (255, 1), (256, 2), (65_535, 2), (65_536, 4), (u32::MAX, 4)] {
            assert_eq!(g_width(gmax), gw);
            let mut w = Vec::new();
            put_g_rec(&mut w, -3, gmax, gw);
            assert_eq!(get_g_rec(&w, gw), (-3, gmax));
        }
    }

    /// One table, no join: a group's rows move it, and a group with no rows
    /// left is gone from the answer. A global aggregate is always one row.
    #[test]
    fn a_single_table_view_and_a_global_aggregate() {
        let q = "SELECT o.prio, COUNT(*), SUM(o.flag) FROM my.ord o WHERE o.flag > 0 GROUP BY 1 ORDER BY 1";
        let mut v = View::plan(q, &srcs(), &mut schemas()).unwrap();
        let im = |v: &View, prio: &str, flag: i32| {
            v.columns(0)
                .iter()
                .map(|c| match c.as_str() {
                    "prio" => txt(prio),
                    "flag" => txt(flag),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let a = im(&v, "a", 2);
        let b = im(&v, "b", 5);
        let z = im(&v, "a", 0);
        v.apply(0, &[(1, a.clone()), (1, b.clone()), (1, z.clone()), (1, a.clone())]).unwrap();
        let r = v.answer().unwrap();
        assert_eq!(r.batch.rows, 2);
        assert!(matches!(cell_num(&r.batch.cols[1], 0), Num::Int(2)));
        assert!(matches!(cell_num(&r.batch.cols[2], 0), Num::Int(4)));
        v.apply(0, &[(-1, b.clone())]).unwrap();
        assert_eq!(v.answer().unwrap().batch.rows, 1, "group b has no rows left");

        let g = "SELECT COUNT(*), SUM(o.flag) FROM my.ord o";
        let mut v = View::plan(g, &srcs(), &mut schemas()).unwrap();
        let r = v.answer().unwrap();
        assert_eq!(r.batch.rows, 1);
        assert!(matches!(cell_num(&r.batch.cols[0], 0), Num::Int(0)));
        assert!(matches!(cell_num(&r.batch.cols[1], 0), Num::Null));
        let one = v.columns(0).iter().map(|c| if c == "flag" { txt(7) } else { None }).collect::<Vec<_>>();
        v.apply(0, &[(1, one)]).unwrap();
        assert!(matches!(cell_num(&v.answer().unwrap().batch.cols[1], 0), Num::Int(7)));
    }

    /// What a change stream cannot keep exact is refused at plan, naming it.
    #[test]
    fn what_cannot_be_maintained_is_refused_by_name() {
        for (q, want) in [
            ("SELECT o.prio, MIN(l.price) FROM pg.li l JOIN my.ord o ON l.k = o.k GROUP BY 1", "MIN"),
            ("SELECT o.prio, MAX(o.flag) FROM my.ord o GROUP BY 1", "MAX"),
            ("SELECT COUNT(*) FROM gh.issues", "no change stream"),
            ("SELECT o.prio, COUNT(*) FROM my.ord o WHERE o.flag + 1 > 2 GROUP BY 1", "re-checked on each change"),
            ("SELECT o.prio, COUNT(*) FROM my.ord o WHERE o.ts IS NOT NULL GROUP BY 1", "TimestampUtc"),
        ] {
            let mut sc = schemas();
            let mut s2 = |u: &str, t: &str| -> Result<Vec<ArrowField>> {
                if t == "issues" {
                    return Ok(vec![f("number", ArrowKind::Int64)]);
                }
                sc(u, t)
            };
            let e = View::plan(q, &srcs(), &mut s2).err().map(|e| e.to_string()).unwrap_or_default();
            assert!(e.contains(want), "{q}: {e}");
        }
        // A repeated GROUP BY-side key is refused when it arrives.
        let mut v = View::plan(QJ, &srcs(), &mut schemas()).unwrap();
        let r = Or { prio: "a".into(), flag: 1, day: 2 };
        let im = or_img(&v, 5, &r);
        v.apply(1, &[(1, im.clone())]).unwrap();
        let e = v.apply(1, &[(1, im)]).unwrap_err().to_string();
        assert!(e.contains("twice on the GROUP BY table"), "{e}");
    }
}
