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
}

/// Key ranges this wide are a bitmap (64 MiB at the cap).
const BITS_MAX: u64 = 512 << 20;

impl KeySet {
    fn over(kr: Kr) -> KeySet {
        match kr {
            Kr::Known(lo, hi) if u64::try_from(hi as i128 - lo as i128 + 1).is_ok_and(|n| n <= BITS_MAX) => {
                let n = (hi as i128 - lo as i128 + 1) as usize;
                KeySet { lo, bits: Arc::new(vec![0; n.div_ceil(64)]), hash: None, null: false, mm: None, grow: false }
            }
            _ => KeySet { lo: 0, bits: Arc::new(Vec::new()), hash: Some(Default::default()), null: false, mm: None, grow: false },
        }
    }

    /// A set whose bitmap covers the keys it is given: a subquery's key
    /// range asked of its server is a full scan when no index leads the key
    /// (TPC-H Q13's o_custkey: MIN/MAX over 69M MySQL rows, 30 s, twice).
    fn growing() -> KeySet {
        KeySet { lo: 0, bits: Arc::new(Vec::new()), hash: None, null: false, mm: None, grow: true }
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
        let cap = (BITS_MAX / 64) as i128;
        let grow = words.max(1024);
        let (nlo, nhi) = if kw < lo_w {
            ((kw).min(lo_w - grow).max(hi_w - cap).min(kw), hi_w)
        } else {
            (lo_w, (kw + 1).max(hi_w + grow).min(lo_w + cap).max(kw + 1))
        };
        let fits = (nhi - nlo) * 64 <= BITS_MAX as i128 && nlo * 64 >= i64::MIN as i128 && nhi * 64 <= i64::MAX as i128;
        if !fits {
            let mut h: std::collections::HashSet<i64, foldhash::fast::RandomState> = Default::default();
            self.for_each_key(|x| {
                h.insert(x);
            });
            self.hash = Some(Arc::new(h));
            self.bits = Arc::new(Vec::new());
            self.lo = 0;
            return;
        }
        let mut v = vec![0u64; (nhi - nlo) as usize];
        let at = (lo_w - nlo) as usize;
        v[at..at + words as usize].copy_from_slice(&self.bits);
        self.bits = Arc::new(v);
        self.lo = (nlo * 64) as i64;
    }

    fn slot(&self, k: i64) -> Option<usize> {
        let i = usize::try_from(k.checked_sub(self.lo)?).ok()?;
        (i < self.bits.len() * 64).then_some(i)
    }

    fn insert(&mut self, k: i64) -> Result<()> {
        // Unshared while it is read (`keys` owns it until it is done).
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
        let held = rss_bytes().unwrap_or(m / 4);
        let mut n = crate::read_impl::read_pipes_for(m.saturating_sub(held + PROBE_HEADROOM));
        // As a probe's: no more pipes than the cores decode (Q21 10g seed at
        // four beside 93 MiB was OOM-killed, the sockets' kernel memory
        // charged to the cage too).
        if let Some(c) = crate::pipeline::cpu_limit_cores() {
            n = n.min(probe_pipe_cap(c, &sub.side, held, density));
        }
        let n = n.clamp(1, SERVER_SCAN_PIPES);
        if dbg() {
            eprintln!(
                "[sql] subquery over {}: {n} pipe(s), {} MiB held ({} anon) of {} MiB, gate density {density:.3}",
                sub.side.name,
                held >> 20,
                anon_bytes().unwrap_or(0) >> 20,
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
        crate::pipeline::mem_limit_bytes().map(|m| crate::read_impl::read_pipes_for(m.saturating_sub(held + (32 << 20))).clamp(2, SERVER_SCAN_PIPES))
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
    Ok(set)
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
    /// Ascending keys over a span past half the cap (300M) stay a bitmap:
    /// the doubling stops at the cap instead of jumping past it.
    #[test]
    fn growing_key_set_stays_a_bitmap_up_to_the_cap() {
        let mut s = KeySet::growing();
        let mut k = 0i64;
        while k < 300_000_000 {
            s.insert(k).unwrap();
            k += 1_000_003;
        }
        s.insert(300_000_000).unwrap();
        assert!(s.hash.is_none(), "a 300M span is under BITS_MAX: a bitmap");
        assert!(s.has(300_000_000) && s.has(0) && !s.has(1));
    }

    #[test]
    fn growing_key_set_holds_every_key() {
        let keys: Vec<i64> = vec![5_000_000, 3, -70_000, 4_999_999, 64, 63, -1, 0, 1 << 20, 12_345_678, -64, 2_000_001];
        let mut s = KeySet::growing();
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
