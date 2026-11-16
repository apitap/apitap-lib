//! `EXISTS` / `IN (subquery)` and their `NOT` forms: the subquery's table is
//! read once (its own WHERE on its own server) into a set of its keys, and
//! the outer table's rows are kept by their key's membership. The semi-join
//! is apitap's; no server is sent another's keys.

use super::*;
use std::sync::{Arc, OnceLock};

/// The keys a subquery gave (and whether a NULL was among them).
#[derive(Clone, Default)]
/// Shared, not copied, by the subqueries that read the same keys (`sig`).
pub(super) struct KeySet {
    lo: i64,
    bits: Arc<Vec<u64>>,
    hash: Option<Arc<std::collections::HashSet<i64, foldhash::fast::RandomState>>>,
    null: bool,
    /// Per key, the least and greatest value of the compared column (a
    /// correlation `inner.c <> outer.c`, `Semi::cmp`).
    mm: Option<Arc<HashMap<i64, (i64, i64), foldhash::fast::RandomState>>>,
    /// The bitmap widens to the keys as they come (no range asked of the
    /// server first), a hash set past [`BITS_MAX`].
    grow: bool,
    /// Past [`flat_budget`] the keys are held by count, not by range
    /// ([`Sparse`]); `bits` is then empty.
    sparse: Option<Arc<Sparse>>,
    /// The flat bitmap's budget, taken when the set is made (0: none taken
    /// yet, [`flat_budget`]).
    budget: u64,
}

/// Key ranges this wide are a bitmap (64 MiB at the cap).
const BITS_MAX: u64 = 512 << 20;

/// The bytes a key set's flat bitmap (one bit per key of its range) may
/// take before the set holds its keys by count instead ([`Sparse`]): a
/// thirty-second of the memory limit (8 MiB at 256 MiB: every TPC-H set on
/// the 10g seed, 7.5 MiB at most, stays the bitmap it was). TPC-H Q21's
/// sets over `lineitem_w`'s 280M order keys were 35 MiB bitmaps each for
/// ~700K keys (W1 phase A: `keys` 62.9 → 164.8 MiB).
fn flat_budget() -> u64 {
    crate::pipeline::mem_limit_bytes().map_or(8 << 20, |m| m / 32).max(1 << 20)
}

/// Keys per chunk of a [`Sparse`] set (the low 16 bits address within one).
const CHUNK_KEYS: i64 = 1 << 16;

/// The most offsets a chunk's list holds (8 KiB, a bitmap's size): a list
/// that fills it with more than half unique turns into the bitmap.
const CHUNK_LIST_MAX: usize = 4096;

/// The heap a hash set of `keys` takes: buckets of 8 B and a control byte,
/// at most 7/8 full, a power of two of them (4M keys: 72 MiB measured).
fn hash_price(keys: u64) -> u64 {
    (keys.saturating_mul(8) / 7).max(1).next_power_of_two() * 9
}

/// Heap bytes an occupied list chunk costs: its slot in `chunks` and the
/// list's allocation, which glibc rounds up to 32 B (the first 8 offsets
/// ride in it). 4M keys over 2^36 (~4 a chunk) measured 60 MiB of heap for
/// 1.05M chunks: 57 B a chunk.
const CHUNK_OVERHEAD: u64 = std::mem::size_of::<Chunk>() as u64 + 32;

/// Keys a [`Sparse`] set takes before its price is held to a hash set's.
const SPARSE_PRICE_FROM: u64 = 1 << 16;

#[cfg(test)]
static PRICE_OFF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
thread_local! {
    /// List compactions on this thread (tests run on threads of their own).
    static LIST_SORTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The widest directory, in chunks (2^22 chunks = 2^38 keys, 16 MiB of
/// directory); past it the set is a hash set.
const DIR_MAX: i64 = 1 << 22;

/// One 64K-key chunk of a [`Sparse`] set.
#[derive(Clone)]
enum Chunk {
    /// Offsets in the chunk: appended while the set loads (duplicates
    /// compacted when the list fills), sorted and unique once sealed.
    List(Vec<u16>),
    Bits(Box<[u64; 1024]>),
}

/// Keys held by count: a directory of 64K-key chunks over the keys' span
/// (4 B a chunk), each chunk a list (2 B a key) or a bitmap (8 KiB), so
/// the set is about min(2 B × keys, span / 8) plus the directory.
#[derive(Clone, Default)]
pub(super) struct Sparse {
    /// The chunk number of `dir[0]`.
    base: i64,
    /// Per chunk of the span: its index in `chunks` plus one (0: empty).
    dir: Vec<u32>,
    chunks: Vec<Chunk>,
    /// The last chunk inserted into (keys arrive in runs).
    last: (i64, u32),
    /// Bitmap chunks, the offsets the lists hold now (duplicates a list has
    /// not compacted yet included), and a lower bound on the distinct keys:
    /// exact for a list of at most 8 (an insert there checks its offsets) and
    /// for a bitmap, the count at its last compaction for a longer list.
    /// Never the insert count: `IN (SELECT fk ...)` repeats its keys, and a
    /// set priced by its inserts stayed sparse past a hash set's size (W1
    /// review 2: 4M keys one a chunk, each 4 times, ~240 MiB against 72).
    bits: u64,
    entries: u64,
    distinct: u64,
    /// Per chunk, its keys counted into `distinct`.
    counted: Vec<u32>,
}

impl Sparse {
    /// The heap the set takes, as glibc rounds it.
    fn price(&self) -> u64 {
        let lists = self.chunks.len() as u64 - self.bits;
        let past_first = (self.entries * 2).saturating_sub(lists * 16);
        lists * CHUNK_OVERHEAD + self.bits * 8192 + self.dir.len() as u64 * 4 + self.counted.len() as u64 * 4 + past_first
    }

    /// `false`: the span would pass [`DIR_MAX`], or the set would cost more
    /// than a hash set of its keys (a key or so a chunk: 4M keys one a chunk
    /// over 2^38 took ~240 MiB as lists against 72 as a hash set, W1 review
    /// R2); the caller turns to a hash set.
    fn insert(&mut self, k: i64) -> bool {
        let c = k.div_euclid(CHUNK_KEYS);
        let off = k.rem_euclid(CHUNK_KEYS) as u16;
        let idx = if self.last.1 != 0 && self.last.0 == c {
            self.last.1
        } else {
            if self.dir.is_empty() {
                self.base = c;
                self.dir.push(0);
            }
            let lo = self.base;
            let hi = lo + self.dir.len() as i64;
            if c < lo || c >= hi {
                // Widen toward the key, at least doubling.
                let grow = (self.dir.len() as i64).max(64);
                let (need_lo, need_hi) = (c.min(lo), (c + 1).max(hi));
                if need_hi - need_lo > DIR_MAX {
                    return false;
                }
                let (nlo, nhi) = if c < lo { (c.min(lo - grow), hi) } else { (lo, (c + 1).max(hi + grow)) };
                let (nlo, nhi) = (nlo.max(need_hi - DIR_MAX), nhi.min(need_lo + DIR_MAX));
                let mut d = vec![0u32; (nhi - nlo) as usize];
                let at = (lo - nlo) as usize;
                d[at..at + self.dir.len()].copy_from_slice(&self.dir);
                self.dir = d;
                self.base = nlo;
            }
            let slot = (c - self.base) as usize;
            if self.dir[slot] == 0 {
                // A new chunk is where the price grows: held to a hash set's
                // here, once per chunk, never per key.
                #[cfg(test)]
                let priced = !PRICE_OFF.load(std::sync::atomic::Ordering::Relaxed);
                #[cfg(not(test))]
                let priced = true;
                // While the keys come, past the most a hash set of them could
                // take (a power of two just doubled: 16/7 × 9 B a key); at
                // `seal` against what it takes (`KeySet::seal`).
                if priced && self.distinct >= SPARSE_PRICE_FROM && self.price() > self.distinct * 9 * 16 / 7 {
                    return false;
                }
                self.chunks.push(Chunk::List(Vec::with_capacity(8)));
                self.counted.push(0);
                self.dir[slot] = self.chunks.len() as u32;
            }
            self.last = (c, self.dir[slot]);
            self.dir[slot]
        };
        let i = idx as usize - 1;
        let ch = &mut self.chunks[i];
        match ch {
            Chunk::Bits(b) => {
                let w = &mut b[off as usize >> 6];
                if *w & (1 << (off & 63)) == 0 {
                    *w |= 1 << (off & 63);
                    self.distinct += 1;
                    self.counted[i] += 1;
                }
            }
            Chunk::List(v) => {
                // At most 8 offsets (one allocation's worth): a repeat is
                // found here, so a key or so a chunk is counted exactly.
                if v.len() < 8 {
                    if !v.contains(&off) {
                        v.push(off);
                        self.entries += 1;
                        self.distinct += 1;
                        self.counted[i] += 1;
                    }
                    return true;
                }
                // Full: compact. Unless that frees half, the list doubles,
                // or at its most turns into the bitmap (the same 8 KiB), so
                // every sort is paid for by as many inserts as it sorted: a
                // duplicate never sorts the list again on its own.
                if v.len() == v.capacity() {
                    #[cfg(test)]
                    LIST_SORTS.with(|c| c.set(c.get() + 1));
                    let before = v.len() as u64;
                    v.sort_unstable();
                    v.dedup();
                    let n = v.len() as u32;
                    self.entries -= before - n as u64;
                    self.distinct += u64::from(n - self.counted[i]);
                    self.counted[i] = n;
                    if v.len() * 2 > v.capacity() {
                        if v.capacity() >= CHUNK_LIST_MAX {
                            let mut b = Box::new([0u64; 1024]);
                            for &o in v.iter() {
                                b[o as usize >> 6] |= 1 << (o & 63);
                            }
                            let fresh = b[off as usize >> 6] & (1 << (off & 63)) == 0;
                            b[off as usize >> 6] |= 1 << (off & 63);
                            self.entries -= n as u64;
                            if fresh {
                                self.distinct += 1;
                                self.counted[i] += 1;
                            }
                            *ch = Chunk::Bits(b);
                            self.bits += 1;
                            return true;
                        }
                        v.reserve(v.capacity());
                    }
                }
                v.push(off);
                self.entries += 1;
            }
        }
        true
    }

    /// The distinct keys, exact (a sealed set's lists are unique).
    fn sealed_count(&self) -> u64 {
        self.chunks
            .iter()
            .map(|c| match c {
                Chunk::List(v) => v.len() as u64,
                Chunk::Bits(b) => b.iter().map(|w| u64::from(w.count_ones())).sum(),
            })
            .sum()
    }

    /// Lists sorted and unique, so a lookup can search them; the counts
    /// exact after.
    fn seal(&mut self) {
        for ch in &mut self.chunks {
            if let Chunk::List(v) = ch {
                v.sort_unstable();
                v.dedup();
                v.shrink_to_fit();
            }
        }
        self.distinct = self.sealed_count();
        self.entries = self.chunks.iter().map(|c| if let Chunk::List(v) = c { v.len() as u64 } else { 0 }).sum();
    }

    #[inline]
    fn has(&self, k: i64) -> bool {
        let c = k.div_euclid(CHUNK_KEYS);
        let Some(slot) = c.checked_sub(self.base).and_then(|i| usize::try_from(i).ok()) else { return false };
        let Some(&i) = self.dir.get(slot) else { return false };
        if i == 0 {
            return false;
        }
        let off = k.rem_euclid(CHUNK_KEYS) as u16;
        match &self.chunks[i as usize - 1] {
            Chunk::Bits(b) => b[off as usize >> 6] & (1 << (off & 63)) != 0,
            Chunk::List(v) => v.binary_search(&off).is_ok(),
        }
    }

    fn for_each_key(&self, mut f: impl FnMut(i64)) {
        for (slot, &i) in self.dir.iter().enumerate() {
            if i == 0 {
                continue;
            }
            let base = (self.base + slot as i64) * CHUNK_KEYS;
            match &self.chunks[i as usize - 1] {
                Chunk::Bits(b) => {
                    for (w, &bits) in b.iter().enumerate() {
                        let mut x = bits;
                        while x != 0 {
                            f(base + (w * 64) as i64 + x.trailing_zeros() as i64);
                            x &= x - 1;
                        }
                    }
                }
                Chunk::List(v) => {
                    let mut prev = None;
                    for &o in v {
                        // Unsealed lists may repeat an offset.
                        if prev != Some(o) {
                            f(base + o as i64);
                        }
                        prev = Some(o);
                    }
                }
            }
        }
    }

    fn bytes(&self) -> u64 {
        let lists: u64 = self
            .chunks
            .iter()
            .map(|c| match c {
                Chunk::List(v) => v.capacity() as u64 * 2,
                Chunk::Bits(_) => 8192,
            })
            .sum();
        self.dir.capacity() as u64 * 4 + self.chunks.capacity() as u64 * std::mem::size_of::<Chunk>() as u64 + lists
    }
}

impl KeySet {
    /// The bytes the set holds (bitmap words, hash set buckets, the
    /// per-key least/greatest map), for the ledger's `Keys` class. A set
    /// shared by two subqueries is counted by each.
    pub(super) fn bytes(&self) -> u64 {
        let hash = self.hash.as_ref().map_or(0, |h| h.capacity() as u64 * 9);
        let mm = self.mm.as_ref().map_or(0, |m| m.capacity() as u64 * 25);
        let sparse = self.sparse.as_ref().map_or(0, |s| s.bytes());
        self.bits.len() as u64 * 8 + hash + mm + sparse
    }

    /// The flat bitmap's budget for this set.
    fn budget(&self) -> u64 {
        if self.budget == 0 {
            flat_budget()
        } else {
            self.budget
        }
    }

    fn over(kr: Kr) -> KeySet {
        let budget = flat_budget();
        match kr {
            // A range whose bitmap fits the budget: that bitmap, as before.
            Kr::Known(lo, hi) if u64::try_from(hi as i128 - lo as i128 + 1).is_ok_and(|n| n <= BITS_MAX && n.div_ceil(8) <= budget) => {
                let n = (hi as i128 - lo as i128 + 1) as usize;
                KeySet { lo, bits: Arc::new(vec![0; n.div_ceil(64)]), budget, ..KeySet::default() }
            }
            // Wider: the keys by count (a HAVING keeps few of its groups).
            Kr::Known(..) => KeySet { sparse: Some(Arc::new(Sparse::default())), budget, ..KeySet::default() },
            _ => KeySet { hash: Some(Default::default()), budget, ..KeySet::default() },
        }
    }

    /// A set whose bitmap covers the keys it is given: a subquery's key
    /// range asked of its server is a full scan when no index leads the key
    /// (TPC-H Q13's o_custkey: MIN/MAX over 69M MySQL rows, 30 s, twice).
    fn growing() -> KeySet {
        KeySet::growing_within(flat_budget())
    }

    /// [`growing`](Self::growing) with the flat bitmap's budget given.
    fn growing_within(budget: u64) -> KeySet {
        KeySet { grow: true, budget: budget.max(1), ..KeySet::default() }
    }

    /// The bitmap's keys into a [`Sparse`] set (the bitmap would pass its budget).
    fn make_sparse(&mut self) {
        let mut sp = Sparse::default();
        let mut ok = true;
        self.for_each_key(|k| ok &= sp.insert(k));
        if !ok {
            self.make_hash();
            return;
        }
        self.sparse = Some(Arc::new(sp));
        self.bits = Arc::new(Vec::new());
        self.lo = 0;
    }

    /// Every key so far into a hash set (a span past every other form),
    /// sized for them up front; a sparse set's chunks are freed as they are
    /// read, so the two forms are not both whole at once.
    fn make_hash(&mut self) {
        let n = match &self.sparse {
            Some(sp) => sp.distinct.max(sp.sealed_count()),
            None => self.bits.iter().map(|w| u64::from(w.count_ones())).sum(),
        };
        let mut h: std::collections::HashSet<i64, foldhash::fast::RandomState> =
            std::collections::HashSet::with_capacity_and_hasher(usize::try_from(n).unwrap_or(0), Default::default());
        match self.sparse.take() {
            Some(sp) => {
                let mut sp = Arc::try_unwrap(sp).unwrap_or_else(|a| (*a).clone());
                let dir = std::mem::take(&mut sp.dir);
                for (slot, &i) in dir.iter().enumerate() {
                    if i == 0 {
                        continue;
                    }
                    let base = (sp.base + slot as i64) * CHUNK_KEYS;
                    match std::mem::replace(&mut sp.chunks[i as usize - 1], Chunk::List(Vec::new())) {
                        Chunk::Bits(b) => {
                            for (w, &bits) in b.iter().enumerate() {
                                let mut x = bits;
                                while x != 0 {
                                    h.insert(base + (w * 64) as i64 + x.trailing_zeros() as i64);
                                    x &= x - 1;
                                }
                            }
                        }
                        Chunk::List(v) => {
                            for o in v {
                                h.insert(base + o as i64);
                            }
                        }
                    }
                }
            }
            None => self.for_each_key(|x| {
                h.insert(x);
            }),
        }
        self.hash = Some(Arc::new(h));
        self.bits = Arc::new(Vec::new());
        self.lo = 0;
    }

    /// The load is over: a sparse set's lists sorted for lookups, then the
    /// form that costs less for its distinct keys (the sparse set's price is
    /// held to a hash set's most while the keys come).
    pub(super) fn seal(&mut self) {
        if let Some(sp) = &mut self.sparse {
            Arc::make_mut(sp).seal();
        }
        if self.sparse.as_ref().is_some_and(|sp| sp.price() > hash_price(sp.distinct)) {
            self.make_hash();
        }
    }

    /// Widen the bitmap to hold `k`: at least double, toward `k`, on 64-key
    /// words; past [`BITS_MAX`] keys wide, a hash set of the keys so far.
    fn widen(&mut self, k: i64) {
        let words = self.bits.len() as i128;
        let kw = (k as i128).div_euclid(64);
        let (lo_w, hi_w) = if words == 0 { (kw, kw + 1) } else { ((self.lo as i128).div_euclid(64), (self.lo as i128).div_euclid(64) + words) };
        // At least double, but never past the cap while the keys themselves
        // fit under it: doubling alone jumped from 268M bits to past 512M
        // and fell to a hash set (75M keys: ~1.2 GB against a 37 MB bitmap).
        // The doubling stops at the flat budget too: a bitmap whose keys fit
        // it must not flip to the sparse form because a doubling overshot
        // (TPC-H Q4 on the 10g seed, 60M keys = 7.4 MB under 8 MiB, flipped in
        // 3 of 4 runs as six pipes' keys arrived out of order: +20% CPU).
        let budget = self.budget();
        let cap = ((BITS_MAX / 64) as i128).min((budget / 8) as i128).max(1);
        let grow = words.max(1024);
        let (nlo, nhi) = if kw < lo_w {
            ((kw).min(lo_w - grow).max(hi_w - cap).min(kw), hi_w)
        } else {
            (lo_w, (kw + 1).max(hi_w + grow).min(lo_w + cap).max(kw + 1))
        };
        let (mut nlo, mut nhi) = (nlo, nhi);
        // The old words carried over: all of them, or only those the keys
        // occupy when the bitmap is fitted to them.
        let mut keep = 0..words as usize;
        if (nhi - nlo) * 8 > budget as i128 {
            // The doubling ran ahead of the keys: fit the words the keys
            // occupy and the new one, with what the cap leaves on the side it
            // grows to. Pipes deliver their spans out of order, so a bitmap
            // grown up from a middle key met a low one past the cap (TPC-H
            // Q4 on the 10g seed, 60M keys in 7.5 MiB: still sparse in 6 of
            // 10 runs with the cap alone, +1 s of CPU in the key set's read).
            // Once per widen, never per key.
            let first = self.bits.iter().position(|w| *w != 0);
            let last = self.bits.iter().rposition(|w| *w != 0);
            if let (Some(f), Some(l)) = (first, last) {
                let (klo, khi) = ((lo_w + f as i128).min(kw), (lo_w + l as i128 + 1).max(kw + 1));
                if khi - klo <= cap {
                    keep = f..l + 1;
                    if kw < lo_w {
                        (nlo, nhi) = ((khi - cap).max(klo - grow), khi);
                    } else {
                        (nlo, nhi) = (klo, (klo + cap).min(khi + grow));
                    }
                }
            }
        }
        let fits = (nhi - nlo) * 64 <= BITS_MAX as i128 && nlo * 64 >= i64::MIN as i128 && nhi * 64 <= i64::MAX as i128;
        // Past its budget the bitmap gives way to the keys held by count.
        if (nhi - nlo) * 8 > budget as i128 {
            self.make_sparse();
            return;
        }
        // Past BITS_MAX keys wide (a limit whose flat budget passes that
        // bitmap): held by count too, a hash set only if that costs less or
        // the span passes the directory (scale with the limit, W1 review 2).
        if !fits {
            self.make_sparse();
            return;
        }
        let mut v = vec![0u64; (nhi - nlo) as usize];
        let at = (lo_w + keep.start as i128 - nlo) as usize;
        v[at..at + keep.len()].copy_from_slice(&self.bits[keep]);
        self.bits = Arc::new(v);
        self.lo = (nlo * 64) as i64;
    }

    fn slot(&self, k: i64) -> Option<usize> {
        let i = usize::try_from(k.checked_sub(self.lo)?).ok()?;
        (i < self.bits.len() * 64).then_some(i)
    }

    fn insert(&mut self, k: i64) -> Result<()> {
        // Unshared while it is read (`keys` owns it until it is done).
        if let Some(sp) = &mut self.sparse {
            if !Arc::make_mut(sp).insert(k) {
                self.make_hash();
                return self.insert(k);
            }
            return Ok(());
        }
        match &mut self.hash {
            Some(h) => {
                Arc::make_mut(h).insert(k);
            }
            None => {
                let i = match self.slot(k) {
                    Some(i) => i,
                    None if self.grow => {
                        self.widen(k);
                        return self.insert(k);
                    }
                    None => return Err(bad("a subquery key past the range its server gave")),
                };
                Arc::make_mut(&mut self.bits)[i / 64] |= 1 << (i % 64);
            }
        }
        Ok(())
    }

    /// Every key (not the NULL), in no order.
    fn for_each_key(&self, mut f: impl FnMut(i64)) {
        if let Some(sp) = &self.sparse {
            sp.for_each_key(f);
            return;
        }
        match &self.hash {
            Some(h) => h.iter().for_each(|&k| f(k)),
            None => {
                for (w, &bits) in self.bits.iter().enumerate() {
                    let mut b = bits;
                    while b != 0 {
                        let i = b.trailing_zeros() as usize;
                        f(self.lo + (w * 64 + i) as i64);
                        b &= b - 1;
                    }
                }
            }
        }
    }

    #[inline]
    pub(super) fn has(&self, k: i64) -> bool {
        if let Some(sp) = &self.sparse {
            return sp.has(k);
        }
        match &self.hash {
            Some(h) => h.contains(&k),
            None => self.slot(k).is_some_and(|i| self.bits[i / 64] & (1 << (i % 64)) != 0),
        }
    }
}

/// `GROUP BY key HAVING agg op literal` of a subquery.
#[derive(Clone)]
pub(super) struct Having {
    pub agg: Agg,
    pub op: BinaryOperator,
    pub lit: VExpr,
}

/// The subquery: its table (its WHERE pushed to its server), its key's read
/// position, and a HAVING over its key's groups.
#[derive(Clone)]
pub(super) struct Sub {
    pub side: Side,
    pub key: usize,
    pub having: Option<Having>,
}

/// One table's rows filtered by a subquery's keys.
#[derive(Clone)]
pub(super) struct Semi {
    /// Read position of the outer key.
    pub col: usize,
    /// `NOT EXISTS` / `NOT IN`.
    pub anti: bool,
    /// `NOT IN`: a NULL among the keys keeps no row, and a NULL outer key is
    /// unknown (not kept), as SQL has it.
    pub not_in: bool,
    pub sub: Sub,
    pub set: Arc<OnceLock<KeySet>>,
    /// A second correlation that is not an equality (TPC-H Q21's
    /// `l2.l_suppkey <> l1.l_suppkey`): (read position of the outer column,
    /// read position of the inner one, the comparison as `inner op outer`).
    /// Each key keeps its inner rows' least and greatest value, which
    /// decide it: a row with `c <> v` exists unless every c is v.
    pub cmp: Option<(usize, usize, star::Op)>,
}

impl Semi {
    /// Whether row `r` passes.
    #[inline]
    pub(super) fn pass(&self, b: &ArrowBatch, r: usize) -> bool {
        let set = self.set.get().expect("a subquery's keys are read before the rows");
        match key_at(&b.cols[self.col], r) {
            Ok(Some(k)) => {
                let h = match (&self.cmp, &set.mm) {
                    (Some((oc, _, op)), Some(mm)) => match (mm.get(&k), key_at(&b.cols[*oc], r)) {
                        (Some(&(lo, hi)), Ok(Some(v))) => match op {
                            star::Op::Ne => !(lo == v && hi == v),
                            star::Op::Lt => lo < v,
                            star::Op::Le => lo <= v,
                            star::Op::Gt => hi > v,
                            star::Op::Ge => hi >= v,
                            // Equalities are the key's, never here.
                            star::Op::Eq => false,
                        },
                        _ => false,
                    },
                    _ => set.has(k),
                };
                if self.not_in && !h && set.null {
                    false
                } else {
                    h != self.anti
                }
            }
            // A NULL outer key equals no key: EXISTS and IN drop it, NOT
            // EXISTS keeps it, NOT IN does not know (drops it).
            _ => self.anti && !self.not_in,
        }
    }
}

/// An aggregate's state as a value.
fn acc_num(a: Acc) -> Num {
    match a {
        Acc::None => Num::Null,
        Acc::I(x) => Num::Int(x),
        Acc::D(x) => Num::Dec(x),
        Acc::F(x) => Num::Float(x),
        Acc::Avg(s, n) => Num::Float(s / n as f64),
        Acc::N(n) => Num::Int(n as i64),
        // A key set's HAVING takes no MIN/MAX (refused at plan).
        Acc::T(_) => Num::Null,
    }
}

/// `agg op lit`, exact for integers and decimals.
fn having_ok(h: &Having, a: Acc) -> Result<bool> {
    use std::cmp::Ordering as O;
    let ty = match h.agg.f {
        AggFn::Count | AggFn::CountStar => Ty::Int,
        AggFn::Avg => Ty::Float,
        _ => h.agg.ty.unwrap_or(Ty::Int),
    };
    let v = acc_num(a);
    let lt = type_of(&h.lit)?;
    let lv = eval(&h.lit, &ArrowBatch { rows: 1, cols: Vec::new() }, 0)?;
    let o = match (v, lv) {
        (Num::Null, _) | (_, Num::Null) => return Ok(false),
        (Num::Float(_), _) | (_, Num::Float(_)) => as_f64(v, ty).unwrap_or(0.0).partial_cmp(&as_f64(lv, lt).unwrap_or(0.0)),
        _ => {
            let s = scale(ty).max(scale(lt));
            let x = match v {
                Num::Int(x) => rescale(i128::from(x), 0, s)?,
                Num::Dec(x) => rescale(x, scale(ty), s)?,
                _ => unreachable!("matched above"),
            };
            let y = match lv {
                Num::Int(y) => rescale(i128::from(y), 0, s)?,
                Num::Dec(y) => rescale(y, scale(lt), s)?,
                _ => unreachable!("matched above"),
            };
            Some(x.cmp(&y))
        }
    };
    let Some(o) = o else { return Ok(false) };
    Ok(match h.op {
        BinaryOperator::Gt => o == O::Greater,
        BinaryOperator::GtEq => o != O::Less,
        BinaryOperator::Lt => o == O::Less,
        BinaryOperator::LtEq => o != O::Greater,
        BinaryOperator::Eq => o == O::Equal,
        _ => o != O::Equal,
    })
}

/// A HAVING range's sums: i64 while every sum fits, i128 from the first
/// that does not (the values already summed carried over exactly).
enum Sums {
    N(Vec<i64>),
    W(Vec<i128>),
}

impl Sums {
    #[inline]
    fn add(&mut self, i: usize, v: i128) -> Result<()> {
        if let Sums::N(a) = self {
            if let Some(x) = i64::try_from(v).ok().and_then(|v| a[i].checked_add(v)) {
                a[i] = x;
                return Ok(());
            }
            *self = Sums::W(a.iter().map(|&x| i128::from(x)).collect());
        }
        match self {
            Sums::W(a) => {
                a[i] = a[i].checked_add(v).ok_or_else(|| bad("a HAVING SUM overflowed 38 decimal digits"))?;
                Ok(())
            }
            Sums::N(_) => unreachable!("widened above"),
        }
    }

    fn get(&self, i: usize) -> i128 {
        match self {
            Sums::N(a) => i128::from(a[i]),
            Sums::W(a) => a[i],
        }
    }
}

/// A subquery's keys. With a HAVING the key's groups are summed a key range
/// at a time (each range one statement, `key BETWEEN a AND b`, the server's
/// own scan): a dense array per range, ~12 bytes a key, the range sized to
/// the build budget, so the answer is exact whatever order the table is
/// stored in and memory stays what the budget gives.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn keys(sub: &Sub, not_in: bool, cmp: Option<usize>) -> Result<KeySet> {
    let kname = sub.side.schema[sub.side.read[sub.key]].name.clone();
    // Only a HAVING's groups need the key range up front (its passes split
    // it); a plain set widens to its keys as they come.
    let (kr, parts) = if sub.having.is_some() {
        key_range_of(sub.side.dialect, sub.side.name.clone(), sub.side.url.clone(), sub.side.table.clone(), kname.clone()).await?
    } else {
        (Kr::Unknown, Vec::new())
    };
    let mut set = if sub.having.is_some() { KeySet::over(kr) } else { KeySet::growing() };
    let tys = side_tys(&sub.side);
    // Pipes for what the memory leaves beside what the process holds now
    // (the sets read before this one among it): an automatic pick prices
    // the whole cgroup as free, and TPC-H Q21 10g seed was OOM-killed reading
    // l3's least and greatest l_suppkey per order beside l2's.
    // The decoder's gate: the keys of the subqueries narrowing this one
    // (TPC-H Q21's l2 and l3 by the 700K orders of l1's magic set, of 15M):
    // a row they drop is one `local_ok` would drop, before it decodes.
    let gates = narrowing_gates(&sub.side);
    let density = gate_density(&gates);
    let free_pipes = || -> Option<usize> {
        let m = crate::pipeline::mem_limit_bytes()?;
        let held = held_bytes().unwrap_or(m / 4);
        let mut n = crate::read_impl::read_pipes_for(m.saturating_sub(held + super::ledger::probe_headroom()));
        // As a probe's: no more pipes than the cores decode (Q21 10g seed at
        // four beside 93 MiB was OOM-killed, the sockets' kernel memory
        // charged to the cage too).
        if let Some(c) = crate::pipeline::cpu_limit_cores() {
            n = n.min(probe_pipe_cap(c, &sub.side, held, density));
        }
        let n = n.clamp(1, SERVER_SCAN_PIPES);
        if dbg() {
            eprintln!(
                "[sql] subquery over {}: {n} pipe(s), {} MiB held ({} anon; glibc {}) of {} MiB, gate density {density:.3}",
                sub.side.name,
                held >> 20,
                anon_bytes().unwrap_or(0) >> 20,
                super::ledger::malloc_split().map_or("split unknown".into(), |(u, f, mm)| format!("in use {}, free {}, mmapped {} MiB", u >> 20, f >> 20, mm >> 20)),
                m >> 20
            );
        }
        Some(n)
    };
    if let Some(ic) = cmp {
        let mut mm: HashMap<i64, (i64, i64), foldhash::fast::RandomState> = HashMap::default();
        let mut rd = open_gated(&sub.side, None, free_pipes(), None, gates.clone()).await?;
        while let Some(b) = rd.recv().await? {
            for r in 0..b.rows {
                if !local_ok(&sub.side, &tys, &b, r) {
                    continue;
                }
                let (Some(k), Some(v)) = (key_at(&b.cols[sub.key], r)?, key_at(&b.cols[ic], r)?) else { continue };
                mm.entry(k).and_modify(|e| *e = (e.0.min(v), e.1.max(v))).or_insert((v, v));
            }
        }
        if dbg() {
            eprintln!("[sql] subquery over {}: {} keys with their least and greatest {}", sub.side.name, mm.len(), sub.side.schema[sub.side.read[ic]].name);
        }
        set.mm = Some(Arc::new(mm));
        return Ok(set);
    }
    let Some(h) = &sub.having else {
        let mut rd = open_gated(&sub.side, None, free_pipes(), None, gates.clone()).await?;
        while let Some(b) = rd.recv().await? {
            for r in 0..b.rows {
                if !local_ok(&sub.side, &tys, &b, r) {
                    continue;
                }
                match key_at(&b.cols[sub.key], r)? {
                    Some(k) => set.insert(k)?,
                    None => set.null = true,
                }
            }
        }
        set.seal();
        return Ok(set);
    };
    if matches!(h.agg.f, AggFn::Min | AggFn::Max) || matches!(h.agg.ty, Some(Ty::Float)) {
        return Err(unsupported("HAVING MIN/MAX or over a float in a subquery (SUM, COUNT, AVG of integers and decimals)"));
    }
    let (lo, hi) = match kr {
        Kr::Known(a, b) => (a, b),
        Kr::Empty => return Ok(set),
        Kr::Unknown => return Err(unsupported(format!("IN (SELECT … HAVING …) over {}, a source with no SQL server", sub.side.name))),
    };
    let star = h.agg.f == AggFn::CountStar;
    // The group's state: its sum and its count (non-NULL arguments, or rows
    // for COUNT(*)).
    let state = |sum: i128, n: u32| -> Acc {
        match h.agg.f {
            AggFn::Count | AggFn::CountStar => Acc::N(u64::from(n)),
            _ if n == 0 => Acc::None,
            AggFn::Avg => Acc::Avg(as_f64(Num::Dec(sum), h.agg.ty.unwrap_or(Ty::Int)).unwrap_or(0.0), u64::from(n)),
            _ => match h.agg.ty {
                Some(Ty::Int) => i64::try_from(sum).map(Acc::I).unwrap_or(Acc::D(sum)),
                _ => Acc::D(sum),
            },
        }
    };
    // 12 bytes a key (an i64 sum, widened to i128 the moment one does not
    // fit, and a u32 count): TPC-H Q18's 60M keys took twelve passes at 20.
    let per = (build_budget() / 12).clamp(1 << 16, 1 << 30) as i64;
    // One pass per range, each a read of the subquery's table: a key span
    // past this many (a key near 9e18 beside small ones) is refused before
    // any range is listed, instead of a Vec of 1e12 of them.
    let span = hi as i128 - lo as i128 + 1;
    if span > i128::from(per) * 4096 {
        return Err(unsupported(format!(
            "IN (SELECT … GROUP BY {} HAVING …) over a key span of {span}: past {} passes of {per} keys",
            sub.side.schema[sub.side.read[sub.key]].name, 4096
        )));
    }
    let mut ranges = Vec::new();
    let mut a = lo;
    while a <= hi {
        let b = a.saturating_add(per - 1).min(hi);
        ranges.push((a, b));
        a = match b.checked_add(1) {
            Some(x) => x,
            None => break,
        };
    }
    // Each range one read, its pipes what the memory leaves beside the
    // range's arrays (an automatic pick prices the whole cgroup as free:
    // with the next range's read started early beside them, TPC-H Q18 was
    // OOM-killed). The read waits on its server's range scan more than on
    // the decode (Q18: 13.5 s of CPU in 51 s at one pipe a range), so up to
    // the scan pipes a filtered probe takes, and never fewer than two.
    let pipes = |n: usize| -> Option<usize> {
        let held = 12 * n as u64;
        crate::pipeline::mem_limit_bytes().map(|m| crate::read_impl::read_pipes_for(m.saturating_sub(held + super::ledger::having_room())).clamp(2, SERVER_SCAN_PIPES))
    };
    let passes = ranges.len();
    for &(a, b) in &ranges {
        let n = (b as i128 - a as i128 + 1) as usize;
        let mut sum = Sums::N(vec![0i64; n]);
        let mut cnt = vec![0u32; n];
        let mut between = format!("{} BETWEEN {a} AND {b}", quote(sub.side.dialect, &kname));
        // The pages the range lies in (a table stored in key order).
        if sub.side.dialect == Dialect::Pg {
            if let Some(n) = ctid_span(&parts, a, b) {
                between = format!("{between} AND {n}");
            }
        }
        let mut rd = open(&sub.side, Some(between), pipes(n)).await?;
        while let Some(bt) = rd.recv().await? {
            let col = match &h.agg.arg {
                Some(e) => eval_exact(e, &bt).ok().flatten(),
                None => None,
            };
            for r in 0..bt.rows {
                if !local_ok(&sub.side, &tys, &bt, r) {
                    continue;
                }
                let Some(k) = key_at(&bt.cols[sub.key], r)? else { continue };
                let Some(i) = usize::try_from(k as i128 - a as i128).ok().filter(|&i| i < n) else { continue };
                if star {
                    cnt[i] += 1;
                    continue;
                }
                let v = match (&col, &h.agg.arg) {
                    (Some((v, ok)), _) => ok[r].then_some(v[r]),
                    (None, Some(e)) => match eval(e, &bt, r)? {
                        Num::Int(x) => Some(i128::from(x)),
                        Num::Dec(x) => Some(x),
                        _ => None,
                    },
                    (None, None) => None,
                };
                if let Some(v) = v {
                    sum.add(i, v)?;
                    cnt[i] += 1;
                }
            }
        }
        for j in 0..n {
            if cnt[j] > 0 && having_ok(h, state(sum.get(j), cnt[j]))? {
                set.insert(a + j as i64)?;
            }
        }
        if dbg() {
            super::ledger::mark("having range", &[("having arrays", 12 * n as u64), ("keys", set.bytes())]);
        }
    }
    // NOT IN: a NULL key's group, when its HAVING holds, is a NULL in the list.
    if not_in {
        let mut rd = open(&sub.side, Some(format!("{} IS NULL", quote(sub.side.dialect, &kname))), Some(1)).await?;
        let (mut s, mut c) = (0i128, 0u32);
        while let Some(bt) = rd.recv().await? {
            for r in 0..bt.rows {
                if !local_ok(&sub.side, &tys, &bt, r) {
                    continue;
                }
                if star {
                    c += 1;
                } else if let Some(e) = &h.agg.arg {
                    match eval(e, &bt, r)? {
                        Num::Int(x) => {
                            s += i128::from(x);
                            c += 1;
                        }
                        Num::Dec(x) => {
                            s += x;
                            c += 1;
                        }
                        _ => {}
                    }
                }
            }
        }
        set.null = c > 0 && having_ok(h, state(s, c))?;
    }
    if dbg() {
        eprintln!("[sql] subquery over {}: GROUP BY {kname} HAVING in {passes} key range(s)", sub.side.name);
    }
    set.seal();
    Ok(set)
}

/// The bytes of every key set the plan's subqueries hold (loaded ones).
pub(super) fn plan_bytes(p: &Plan) -> u64 {
    fn side(s: &Side) -> u64 {
        s.semis.iter().map(|n| n.set.get().map_or(0, |k| k.bytes()) + side(&n.sub.side)).sum()
    }
    let mut b: u64 = p.sides.iter().map(side).sum();
    if let Some(st) = &p.star {
        b += st.dims.iter().map(|d| side(&d.side)).sum::<u64>();
    }
    b
}

/// Gates over `side`'s key columns from its loaded `IN` / `EXISTS` key
/// sets (not NOT, not a comparison's: those keep rows a gate would drop).
fn narrowing_gates(side: &Side) -> Vec<Arc<KeyGate>> {
    let mut out = Vec::new();
    for n in &side.semis {
        if n.anti || n.not_in || n.cmp.is_some() {
            continue;
        }
        let Some(set) = n.set.get() else { continue };
        let (mut lo, mut hi, mut cnt) = (i64::MAX, i64::MIN, 0u64);
        set.for_each_key(|k| {
            lo = lo.min(k);
            hi = hi.max(k);
            cnt += 1;
        });
        if cnt == 0 {
            continue;
        }
        out.extend(gate_of(side, n.col, lo, hi, cnt, |g| set.for_each_key(|k| g.set(k))));
    }
    out
}

/// The process's resident bytes (`/proc/self/statm`), where there is one.
pub(super) fn rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
    // The page size of this machine (64 KiB on some arm64 kernels).
    #[cfg(unix)]
    // SAFETY: sysconf reads a constant; it takes no pointers.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    #[cfg(not(unix))]
    let page = 4096i64;
    Some(pages * if page > 0 { page as u64 } else { 4096 })
}

/// What the process holds for a plan to price beside: its resident bytes
/// less the heap glibc keeps free (`mallinfo2` `fordblks`, at most the
/// anonymous bytes past malloc's live ones, so pages a trim already gave
/// back are not taken off twice); without `mallinfo2` (glibc < 2.33), the
/// resident bytes. Freed heap left resident counted as held: TPC-H Q13's
/// second statement read its 69M-row subquery with 2 pipes instead of 6
/// (59 MiB held, 38 anon, of which 3 live, even right after a trim), +40 s
/// of wall at the same CPU (W1 review R4: RSS after a trim measured Q13 at
/// 193.7 s against the released wheel's 153.9). The principled price is the
/// ledger's reservations (B9, next round).
pub(super) fn held_bytes() -> Option<u64> {
    let rss = rss_bytes()?;
    let retained = match (super::ledger::malloc_split(), anon_bytes()) {
        (Some(m), Some(anon)) => super::ledger::retained(anon, m),
        _ => 0,
    };
    Some(rss.saturating_sub(retained))
}

/// The anonymous part of [`rss_bytes`] (heap, stacks), for the debug lines.
pub(super) fn anon_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = s.lines().find_map(|l| l.strip_prefix("RssAnon:"))?.trim().trim_end_matches("kB").trim().parse().ok()?;
    Some(kb << 10)
}

/// Every subquery of the plan read into its key set, before any row of the
/// tables they filter.
pub(super) async fn load(p: &Plan) -> Result<()> {
    let mut all: Vec<&Semi> = p.sides.iter().flat_map(|s| s.semis.iter()).collect();
    if let Some(st) = &p.star {
        all.extend(st.dims.iter().flat_map(|d| d.side.semis.iter()));
    }
    let go = async {
        for s in all {
            load_one(s).await?;
        }
        Ok(())
    };
    // The same keys read once per plan (TPC-H Q21: l2's and l3's narrowing
    // set), or once per query answered in parts, whose scope is outside.
    if KEY_CACHE.try_with(|_| ()).is_ok() {
        go.await
    } else {
        KEY_CACHE.scope(std::cell::RefCell::new(HashMap::new()), go).await
    }
}

/// One subquery's keys, its own subqueries' (which narrow it) first.
fn load_one(s: &Semi) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        for n in &s.sub.side.semis {
            load_one(n).await?;
        }
        if s.set.get().is_none() {
            let sig = sig(s);
            if let Some(set) = sig.as_ref().and_then(|k| KEY_CACHE.try_with(|c| c.borrow().get(k).cloned()).ok().flatten()) {
                if dbg() {
                    eprintln!("[sql] subquery over {}: keys from an earlier part", s.sub.side.name);
                }
                let _ = s.set.set(set);
                return Ok(());
            }
            let set = keys(&s.sub, s.not_in, s.cmp.map(|c| c.1)).await?;
            if dbg() {
                super::ledger::mark(&format!("subquery over {}", s.sub.side.name), &[("keys", set.bytes())]);
            }
            // The read's buffers back to the system before the next read
            // prices its pipes by what the process holds.
            crate::trim_heap();
            if dbg() {
                eprintln!("[sql] subquery over {}: keys read", s.sub.side.name);
            }
            if let Some(k) = sig {
                let _ = KEY_CACHE.try_with(|c| c.borrow_mut().insert(k, set.clone()));
            }
            let _ = s.set.set(set);
        }
        Ok(())
    })
}

tokio::task_local! {
    /// Under one statement (`run_one_out`), its runs and its parts
    /// (`partitioned`): the key sets read once, kept for a run again or the
    /// next part, whose subqueries are the same (TPC-H Q13 10g seed: 8 parts
    /// each read 69M orders' keys again, 45 s each).
    pub(super) static KEY_CACHE: std::cell::RefCell<HashMap<String, KeySet>>;
}

/// What makes two subqueries' key sets the same: the table, what is read,
/// the conditions, the key, the comparison and the subqueries narrowing it.
/// `None` (never shared) for a HAVING.
pub(super) fn sig(s: &Semi) -> Option<String> {
    let sub = &s.sub;
    if sub.having.is_some() {
        return None;
    }
    let side = &sub.side;
    let cols: Vec<&str> = side.read.iter().map(|&c| side.schema[c].name.as_str()).collect();
    // `local` and `virt` too: a side with no SQL server (a stage table,
    // github://, gsheets://) keeps its WHERE there, with `conj` empty, and
    // two IN over one CTE with different filters shared a key set.
    let mut out = format!(
        "{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{}|{:?}|{}",
        super::url_sig(&side.url),
        side.table,
        cols,
        side.conj,
        side.conj_src.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        side.local,
        side.virt,
        sub.key,
        s.cmp.map(|c| c.1),
        s.not_in
    );
    for n in &side.semis {
        out.push('[');
        out.push_str(&sig(n)?);
        out.push(']');
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A set read with no key range first widens its bitmap both ways as
    /// keys come in any order, and turns into a hash set past the cap,
    /// every key kept and no other.
    /// Ascending keys over a span past half the cap (300M): the doubling
    /// stops at the cap instead of jumping past it, and past the flat budget
    /// the keys are held by count, in the cheaper form.
    #[test]
    fn growing_key_set_stays_a_bitmap_up_to_the_cap() {
        let mut s = KeySet::growing_within(8 << 20);
        let mut k = 0i64;
        while k < 300_000_000 {
            s.insert(k).unwrap();
            k += 1_000_003;
        }
        s.insert(300_000_000).unwrap();
        s.seal();
        // Past the flat budget the 301 keys are held by count, in the form
        // that costs less once loaded (here a hash set of 301 keys: 4.5 KiB
        // against 35 KiB of one-key chunks), never a bitmap of the span.
        assert!(s.bits.is_empty() && s.bytes() <= 64 << 10, "{} B", s.bytes());
        assert!(s.has(300_000_000) && s.has(0) && !s.has(1));
    }

    #[test]
    fn growing_key_set_holds_every_key() {
        let keys: Vec<i64> = vec![5_000_000, 3, -70_000, 4_999_999, 64, 63, -1, 0, 1 << 20, 12_345_678, -64, 2_000_001];
        let mut s = KeySet::growing_within(8 << 20);
        for &k in &keys {
            s.insert(k).unwrap();
        }
        assert!(s.hash.is_none(), "a narrow span stays a bitmap");
        for &k in &keys {
            assert!(s.has(k), "key {k}");
        }
        for k in [2, 65, -2, -65, 5_000_001, 12_345_677, i64::MAX, i64::MIN] {
            assert!(!s.has(k), "not a key: {k}");
        }
        let mut n = 0;
        s.for_each_key(|_| n += 1);
        assert_eq!(n, keys.len());
        // Past the cap: a hash set of the same keys.
        s.insert(i64::MAX - 1).unwrap();
        s.insert(i64::MIN + 1).unwrap();
        assert!(s.hash.is_some(), "a span past BITS_MAX is a hash set");
        for &k in keys.iter().chain(&[i64::MAX - 1, i64::MIN + 1]) {
            assert!(s.has(k), "key {k} after the switch");
        }
        assert!(!s.has(2));
    }

    /// W1 B2: a set holds its keys by count once its range's bitmap passes
    /// the budget: 700K keys spread over a 280M range (TPC-H Q21's order
    /// keys on lineitem_w) in at most 4 MiB, where the bitmap was 35 MiB;
    /// every key kept and no other, in any insertion order.
    #[test]
    fn a_sparse_key_set_is_sized_by_its_keys_not_its_range() {
        let mut s = KeySet::growing_within(8 << 20);
        let keys: Vec<i64> = (0..700_000i64).map(|i| 1 + i * 400 + (i % 7)).collect();
        for (n, &k) in keys.iter().enumerate() {
            // Out of order: odd positions from the end.
            let k = if n % 2 == 1 { keys[keys.len() - n] } else { k };
            s.insert(k).unwrap();
        }
        for &k in &keys {
            s.insert(k).unwrap();
        }
        s.seal();
        assert!(s.bytes() <= 4 << 20, "{} B for 700K keys over 280M", s.bytes());
        assert!(s.hash.is_none(), "a sparse set, not a hash set");
        for &k in keys.iter().step_by(97) {
            assert!(s.has(k), "key {k}");
            assert!(!s.has(k + 1), "not a key: {}", k + 1);
        }
        assert!(!s.has(0) && !s.has(-5) && !s.has(i64::MAX));
        let mut n = 0u64;
        s.for_each_key(|_| n += 1);
        assert_eq!(n, keys.len() as u64);
    }

    /// Keys whose whole range fits the flat budget stay one bitmap however
    /// they arrive: a doubling toward a far key stops at the budget instead
    /// of overshooting it into the sparse form (TPC-H Q4, 60M keys).
    #[test]
    fn a_key_set_that_fits_its_budget_stays_flat_whatever_the_order() {
        const B: u64 = 8 << 20;
        let budget_keys = (B * 8) as i64;
        let mut s = KeySet::growing_within(B);
        s.insert(0).unwrap();
        s.insert(budget_keys * 6 / 10).unwrap();
        s.insert(budget_keys * 95 / 100).unwrap();
        s.insert(budget_keys / 3).unwrap();
        s.seal();
        assert!(s.sparse.is_none() && s.hash.is_none(), "{} keys of range fit {B} B: a flat bitmap", budget_keys * 95 / 100);
        assert!(s.bytes() <= B);
        for k in [0, budget_keys * 6 / 10, budget_keys * 95 / 100, budget_keys / 3] {
            assert!(s.has(k));
        }
        assert!(!s.has(1));
    }

    /// A bitmap grown up from a middle key, its doubling past the keys it
    /// holds, then given a low key: still one flat bitmap when every key fits
    /// the budget (pipes deliver spans out of order).
    #[test]
    fn a_key_set_grown_from_the_middle_stays_flat() {
        const B: u64 = 8 << 20;
        let budget_keys = (B * 8) as i64;
        let mid = budget_keys / 2;
        let top = budget_keys * 9 / 10;
        let mut s = KeySet::growing_within(B);
        let mut k = mid;
        while k < top {
            s.insert(k).unwrap();
            k += 1 << 16;
        }
        s.insert(top).unwrap();
        s.insert(0).unwrap();
        s.seal();
        assert!(s.sparse.is_none() && s.hash.is_none(), "keys 0..{top} fit {B} B: a flat bitmap");
        assert!(s.bytes() <= B);
        assert!(s.has(0) && s.has(mid) && s.has(top) && !s.has(1));
    }

    /// Heap measured by glibc (`mallinfo2` in use) for 4M keys over 2^36,
    /// as a sparse set and as a hash set: the price model's check. Run
    /// alone (`--ignored --nocapture`): other tests allocate meanwhile.
    #[test]
    #[ignore]
    fn measure_sparse_against_hash_heap() {
        let used = || super::super::ledger::malloc_split().map_or(0, |m| m.0 + m.2);
        let n = 4_000_000i64;
        for span in [36u32, 38] {
        let stride = (1i64 << span) / n;
        let key = |i: i64| i * stride + (i * 7919) % stride;
        eprintln!("4M keys over 2^{span}:");
        for (name, off) in [("sparse (unpriced)", true), ("sparse (priced)", false)] {
            PRICE_OFF.store(off, std::sync::atomic::Ordering::Relaxed);
            let before = used();
            let mut s = Sparse::default();
            let mut fell = false;
            for i in 0..n {
                if !s.insert(key(i)) {
                    fell = true;
                    break;
                }
            }
            let after = used();
            eprintln!("{name}: heap {} MiB, price {} MiB, chunks {}, fell to hash: {fell}", (after - before) >> 20, s.price() >> 20, s.chunks.len());
            drop(s);
        }
        PRICE_OFF.store(false, std::sync::atomic::Ordering::Relaxed);
        let before = used();
        let mut h: std::collections::HashSet<i64, foldhash::fast::RandomState> = Default::default();
        for i in 0..n {
            h.insert(key(i));
        }
        let after = used();
        eprintln!("hash: heap {} MiB, price {} MiB", (after - before) >> 20, hash_price(n as u64) >> 20);
        }
    }

    /// 4M keys cost no more as a key set than as the hash set the set was
    /// before the sparse form: one key a chunk (over 2^38) is a hash set;
    /// four a chunk (over 2^36) stay sparse, cheaper (W1 review R2).
    #[test]
    fn a_wide_sparse_key_set_costs_no_more_than_a_hash_set() {
        let n = 4_000_000i64;
        for span in [36u32, 38] {
            let stride = (1i64 << span) / n;
            let mut s = KeySet::growing_within(8 << 20);
            for i in 0..n {
                s.insert(i * stride + (i * 7919) % stride).unwrap();
            }
            s.seal();
            let held = s.sparse.as_ref().map_or(hash_price(n as u64), |sp| sp.price());
            assert!(held <= hash_price(n as u64), "2^{span}: {} MiB as a key set against {} MiB as a hash set", held >> 20, hash_price(n as u64) >> 20);
            assert_eq!(s.sparse.is_some(), span == 36, "2^{span}: the cheaper form");
            assert!(s.has(0) && s.has(stride * 7 + (7 * 7919) % stride) && !s.has(1));
        }
    }

    /// With a flat budget past the widest bitmap (a large limit), keys wider
    /// than BITS_MAX are held by count, not dropped to a hash set: 64 runs of
    /// 8192 keys a gigakey apart are ~4.5 MiB sparse (most of it the 1M-chunk
    /// directory) against a 9 MiB hash set (W1 review 2).
    #[test]
    fn a_large_limit_holds_wide_keys_by_count() {
        let mut s = KeySet::growing_within(1 << 30);
        for c in 0..64i64 {
            for j in 0..8192i64 {
                s.insert(c * (1 << 30) + j * 8).unwrap();
            }
        }
        s.seal();
        assert!(s.sparse.is_some() && s.hash.is_none(), "held by count: {} B", s.bytes());
        assert!(s.has(63 * (1 << 30) + 8191 * 8) && !s.has(1));
    }

    /// 4M distinct keys one a chunk (over 2^38), each given four times as an
    /// `IN (SELECT fk ...)` gives them: priced by its distinct keys, not its
    /// inserts, the set ends no larger than the hash set 0.67.2 held (W1
    /// review 2: priced by inserts it stayed ~240 MiB of one-key chunks).
    #[test]
    fn repeated_keys_are_priced_by_their_distinct_count() {
        let n = 4_000_000i64;
        let stride = (1i64 << 38) / n;
        let mut s = KeySet::growing_within(8 << 20);
        // Each key's repeats together (rows of one parent side by side): a
        // set priced by its inserts never saw one key a chunk while loading.
        for i in 0..n {
            for _ in 0..4 {
                s.insert(i * stride + (i * 7919) % stride).unwrap();
            }
        }
        s.seal();
        let held = s.sparse.as_ref().map_or(hash_price(n as u64), |sp| sp.price());
        assert!(held <= hash_price(n as u64), "{} MiB as a key set against {} MiB as a hash set", held >> 20, hash_price(n as u64) >> 20);
        assert!(s.has(0) && s.has(stride * 9 + (9 * 7919) % stride) && !s.has(1));
        let mut c = 0u64;
        s.for_each_key(|_| c += 1);
        assert_eq!(c, n as u64);
    }

    /// Ten million inserts of 4096 distinct offsets into one chunk: the list
    /// is sorted a bounded number of times, not once per duplicate (W1
    /// review R3: a list at 4096 unique re-sorted 4097 offsets each insert).
    #[test]
    fn a_full_chunk_list_is_not_sorted_per_duplicate() {
        let before = LIST_SORTS.with(|c| c.get());
        let mut sp = Sparse::default();
        for i in 0..10_000_000i64 {
            assert!(sp.insert(i % 4096));
        }
        let sorts = LIST_SORTS.with(|c| c.get()) - before;
        assert!(sorts <= 64, "{sorts} sorts for 10M inserts");
        sp.seal();
        assert!((0..4096).all(|k| sp.has(k)) && !sp.has(4096));
    }

    /// A dense chunk turns into a bitmap, negative keys and a key far away
    /// (past the directory's span) are kept too.
    #[test]
    fn a_sparse_key_set_keeps_dense_chunks_negative_and_far_keys() {
        let mut sp = Sparse::default();
        for k in 0..10_000i64 {
            assert!(sp.insert(k * 3));
        }
        for k in [-1i64, -65_536, -65_537, 123_456_789] {
            assert!(sp.insert(k));
        }
        assert!(!sp.insert(i64::MAX - 1), "a span past DIR_MAX is the caller's to change");
        sp.seal();
        assert!(sp.chunks.iter().any(|c| matches!(c, Chunk::Bits(_))));
        for k in [0i64, 3, 29_997, -1, -65_536, -65_537, 123_456_789] {
            assert!(sp.has(k), "key {k}");
        }
        for k in [1i64, 2, 30_000, -2, 123_456_788] {
            assert!(!sp.has(k), "not a key: {k}");
        }
        let mut s = KeySet::growing_within(8 << 20);
        for k in [5i64, 1 << 30, 3 << 30] {
            s.insert(k).unwrap();
        }
        s.insert(i64::MAX - 1).unwrap();
        s.seal();
        for k in [5i64, 1 << 30, 3 << 30, i64::MAX - 1] {
            assert!(s.has(k), "key {k}");
        }
    }

    /// The i64 sums widen to i128 the moment one does not fit, every sum
    /// already made carried over exactly; past 38 digits is refused.
    #[test]
    fn having_sums_widen_exactly() {
        let mut s = Sums::N(vec![0; 3]);
        s.add(0, 5).unwrap();
        s.add(1, i128::from(i64::MAX)).unwrap();
        assert!(matches!(s, Sums::N(_)));
        s.add(1, 1).unwrap();
        assert!(matches!(s, Sums::W(_)), "past i64: widened");
        assert_eq!((s.get(0), s.get(1), s.get(2)), (5, i128::from(i64::MAX) + 1, 0));
        s.add(2, -7).unwrap();
        assert_eq!(s.get(2), -7);
        s.add(0, i128::MAX - 5).unwrap();
        assert!(s.add(0, 1).is_err(), "past i128: refused");
    }
}
