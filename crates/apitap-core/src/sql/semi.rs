//! `EXISTS` / `IN (subquery)` and their `NOT` forms: the subquery's table is
//! read once (its own WHERE on its own server) into a set of its keys, and
//! the outer table's rows are kept by their key's membership. The semi-join
//! is apitap's; no server is sent another's keys.

use super::*;
use std::sync::{Arc, OnceLock};

/// The keys a subquery gave (and whether a NULL was among them).
#[derive(Clone, Default)]
pub(super) struct KeySet {
    lo: i64,
    bits: Vec<u64>,
    hash: Option<std::collections::HashSet<i64, foldhash::fast::RandomState>>,
    null: bool,
    /// Per key, the least and greatest value of the compared column (a
    /// correlation `inner.c <> outer.c`, `Semi::cmp`).
    mm: Option<HashMap<i64, (i64, i64), foldhash::fast::RandomState>>,
}

/// Key ranges this wide are a bitmap (64 MiB at the cap).
const BITS_MAX: u64 = 512 << 20;

impl KeySet {
    fn over(kr: Kr) -> KeySet {
        match kr {
            Kr::Known(lo, hi) if u64::try_from(hi as i128 - lo as i128 + 1).is_ok_and(|n| n <= BITS_MAX) => {
                let n = (hi as i128 - lo as i128 + 1) as usize;
                KeySet { lo, bits: vec![0; n.div_ceil(64)], hash: None, null: false, mm: None }
            }
            _ => KeySet { lo: 0, bits: Vec::new(), hash: Some(Default::default()), null: false, mm: None },
        }
    }

    fn slot(&self, k: i64) -> Option<usize> {
        let i = usize::try_from(k.checked_sub(self.lo)?).ok()?;
        (i < self.bits.len() * 64).then_some(i)
    }

    fn insert(&mut self, k: i64) -> Result<()> {
        match &mut self.hash {
            Some(h) => {
                h.insert(k);
            }
            None => {
                let i = self.slot(k).ok_or_else(|| bad("a subquery key past the range its server gave"))?;
                self.bits[i / 64] |= 1 << (i % 64);
            }
        }
        Ok(())
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
    let (kr, parts) = key_range_of(sub.side.dialect, sub.side.name.clone(), sub.side.url.clone(), sub.side.table.clone(), kname.clone()).await?;
    let mut set = KeySet::over(kr);
    let tys = side_tys(&sub.side);
    if let Some(ic) = cmp {
        let mut mm: HashMap<i64, (i64, i64), foldhash::fast::RandomState> = HashMap::default();
        let mut rd = open(&sub.side, None, None).await?;
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
        set.mm = Some(mm);
        return Ok(set);
    }
    let Some(h) = &sub.having else {
        let mut rd = open(&sub.side, None, None).await?;
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

/// Every subquery of the plan read into its key set, before any row of the
/// tables they filter.
pub(super) async fn load(p: &Plan) -> Result<()> {
    let mut all: Vec<&Semi> = p.sides.iter().flat_map(|s| s.semis.iter()).collect();
    if let Some(st) = &p.star {
        all.extend(st.dims.iter().flat_map(|d| d.side.semis.iter()));
    }
    for s in all {
        load_one(s).await?;
    }
    Ok(())
}

/// One subquery's keys, its own subqueries' (which narrow it) first.
fn load_one(s: &Semi) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        for n in &s.sub.side.semis {
            load_one(n).await?;
        }
        if s.set.get().is_none() {
            let set = keys(&s.sub, s.not_in, s.cmp.map(|c| c.1)).await?;
            if dbg() {
                eprintln!("[sql] subquery over {}: keys read", s.sub.side.name);
            }
            let _ = s.set.set(set);
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
