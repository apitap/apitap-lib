//! Joins of three and more tables (docs/design/2026-11-07-sql-multiway-join.md).
//!
//! One table streams (the fact, `P`: the one the aggregates name), one is
//! built into the key map as a two-table join builds its GROUP BY side (`B`),
//! and every other table is a dimension: joined by its unique key, loaded
//! once into a map key → payload, and looked up from the row of the table it
//! hangs under. A dimension under `B`'s part filters the build row and hands
//! its fields to the group key; one under `P`'s part filters the probe row and
//! hands values to the checks. The semi-joins all happen here: each server is
//! asked for its own table's rows only.

use super::*;

/// A comparison between two fields of a joined row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Op {
    pub(super) fn of(op: &BinaryOperator) -> Option<Op> {
        Some(match op {
            BinaryOperator::Eq => Op::Eq,
            BinaryOperator::NotEq => Op::Ne,
            BinaryOperator::Lt => Op::Lt,
            BinaryOperator::LtEq => Op::Le,
            BinaryOperator::Gt => Op::Gt,
            BinaryOperator::GtEq => Op::Ge,
            _ => return None,
        })
    }

    /// The same comparison with its operands swapped.
    pub(super) fn flip(self) -> Op {
        match self {
            Op::Lt => Op::Gt,
            Op::Le => Op::Ge,
            Op::Gt => Op::Lt,
            Op::Ge => Op::Le,
            o => o,
        }
    }
}

/// A check of two fields: the operator, and each field's decimal scale (0
/// when not a decimal), so `l_quantity < 0.2 * AVG(l_quantity)` compares
/// numbers, not bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Cmp {
    pub op: Op,
    pub sa: i8,
    pub sb: i8,
}

impl Cmp {
    const EQ: Cmp = Cmp { op: Op::Eq, sa: 0, sb: 0 };
}

/// Two encoded fields compared by their values (`None`: a NULL, or kinds
/// that do not compare).
fn cmp_fields(a: (u8, &[u8]), sa: i8, b: (u8, &[u8]), sb: i8) -> Option<std::cmp::Ordering> {
    let int = |x: &[u8]| x.try_into().ok().map(i64::from_le_bytes);
    let dec = |x: &[u8]| x.try_into().ok().map(i128::from_le_bytes);
    let flt = |x: &[u8]| x.try_into().ok().map(f64::from_le_bytes);
    // Exact numbers as (mantissa, scale); floats as floats.
    let exact = |t: u8, x: &[u8], s: i8| -> Option<(i128, i8)> {
        match t {
            1 => int(x).map(|v| (i128::from(v), 0)),
            3 => dec(x).map(|v| (v, s)),
            _ => None,
        }
    };
    let float = |t: u8, x: &[u8], s: i8| -> Option<f64> {
        match t {
            1 => int(x).map(|v| v as f64),
            2 => flt(x),
            3 => dec(x).map(|v| v as f64 / pow10f(s)),
            _ => None,
        }
    };
    match (a.0, b.0) {
        (0, _) | (_, 0) => None,
        (5, 5) | (4, 4) => Some(a.1.cmp(b.1)),
        _ => match (exact(a.0, a.1, sa), exact(b.0, b.1, sb)) {
            (Some((x, xs)), Some((y, ys))) => dec_cmp(x, xs, y, ys),
            _ => float(a.0, a.1, sa)?.partial_cmp(&float(b.0, b.1, sb)?),
        },
    }
}

/// Whether two fields pass `c`.
#[inline]
fn check(a: (u8, &[u8]), b: (u8, &[u8]), c: Cmp) -> bool {
    if c.op == Op::Eq && c.sa == c.sb && a.0 == b.0 {
        return same(a, b);
    }
    use std::cmp::Ordering as O;
    match cmp_fields(a, c.sa, b, c.sb) {
        None => false,
        Some(o) => match c.op {
            Op::Eq => o == O::Equal,
            Op::Ne => o != O::Equal,
            Op::Lt => o == O::Less,
            Op::Le => o != O::Greater,
            Op::Gt => o == O::Greater,
            Op::Ge => o != O::Less,
        },
    }
}

/// What a dimension hangs under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Up {
    /// A main side: 0 = `B` (the build), 1 = `P` (the probe).
    Main(usize),
    Dim(usize),
}

/// A dimension table.
pub(super) struct Dim {
    pub side: Side,
    /// Read position of its unique key.
    pub key: usize,
    /// Read by the plan tests; the runtime walks `kids` instead.
    #[cfg_attr(not(test), allow(dead_code))]
    pub up: Up,
    /// Read position of the key's partner in the table above.
    pub fk: usize,
    /// Read positions of its own fields that flow up (GROUP BY columns and
    /// check values), in payload order; its children's payloads follow.
    pub fields: Vec<usize>,
    pub kids: Vec<usize>,
    /// The key itself is a GROUP BY column: it comes from the FK of the row
    /// above (first in this table's place), not from the payload, so the
    /// payloads stay as few as its other fields make them.
    pub key_first: bool,
    /// Its CASE conditions' results (`side.flags`) as one more payload
    /// field after its own (a probe-part dimension's, TPC-H Q8's
    /// `nation = 'BRAZIL'`).
    pub flag: bool,
    /// A two-column key: (its second key's read position, the parent's
    /// partner's read position).
    pub key2: Option<(usize, usize)>,
    /// (read position of one of its columns, another dimension): a row whose
    /// column that one's map lacks is dropped at load.
    pub filters: Vec<(usize, usize)>,
}

/// GROUP BY columns of a dimension whose key is grouped too (so they are
/// its key's, one value per key): read after the aggregation, for the
/// answer's rows only, instead of carried in every payload (TPC-H Q10's
/// customer name, address, comment over millions of customers).
pub(super) struct Late {
    pub dim: usize,
    /// The GROUP BY index of the dimension's key.
    pub key_g: usize,
    /// The table read for them: its key first, then the columns.
    pub side: Side,
    /// (GROUP BY index, read position in `side`).
    pub cols: Vec<(usize, usize)>,
}

/// The plan of a join of three or more tables, beside the two-table plan of
/// its `B` and `P`.
pub(super) struct Star {
    pub dims: Vec<Dim>,
    /// `B`'s and `P`'s own fields (read positions).
    pub bfields: Vec<usize>,
    pub pfields: Vec<usize>,
    /// Dimensions directly under `B` and under `P`, in payload order.
    pub bkids: Vec<usize>,
    pub pkids: Vec<usize>,
    /// Per build field (flattened): kept in the group key (a GROUP BY column
    /// or a value a probe-side check needs).
    pub keep: Vec<bool>,
    /// Build fields compared on the build row; probe fields on the probe row.
    pub local_b: Vec<(usize, usize, Cmp)>,
    pub local_p: Vec<(usize, usize, Cmp)>,
    /// (field of the kept group key, probe field) that must pass the check.
    pub cross: Vec<(usize, usize, Cmp)>,
    /// Per GROUP BY column: its field among the group key's GROUP BY fields
    /// (the key once the check fields are merged away), and its kind.
    pub gpos: Vec<usize>,
    pub gkind: Vec<ArrowKind>,
    /// Kept build fields that are check values (merged away at the end).
    pub hidden: Vec<bool>,
    /// Per GROUP BY column: its field in the key as the passes build it
    /// (`None`: read late).
    pub gpos_rt: Vec<Option<usize>>,
    pub late: Vec<Late>,
    /// The final key's GROUP BY fields in order: `None` = next field of the
    /// key the passes built, `Some((l, c))` = `late[l]`'s column `c`.
    pub gfull: Vec<Option<(usize, usize)>>,
    /// Probe fields that are GROUP BY columns (their places in the probe
    /// row's flattened fields): the fold keys its accumulators by the build
    /// group and these.
    pub pgroup: Vec<usize>,
    /// (dimension, its flag field's place in the probe row's fields).
    pub pflags: Vec<(usize, usize)>,
    /// The fact's lookup columns: (read position in `P`, the value's place in
    /// the probe row's fields, its kind).
    pub plookups: Vec<(usize, usize, ArrowKind)>,
}

/// (table, schema column).
pub(super) type Col = (usize, usize);

/// Union-find over the columns the equalities name.
fn classes(eqs: &[(Col, Col)]) -> Vec<Vec<Col>> {
    let mut cls: Vec<Vec<Col>> = Vec::new();
    for &(a, b) in eqs {
        let ia = cls.iter().position(|c| c.contains(&a));
        let ib = cls.iter().position(|c| c.contains(&b));
        match (ia, ib) {
            (None, None) => cls.push(vec![a, b]),
            (Some(i), None) => cls[i].push(b),
            (None, Some(j)) => cls[j].push(a),
            (Some(i), Some(j)) if i != j => {
                let m = cls.remove(i.max(j));
                cls[i.min(j)].extend(m);
            }
            _ => {}
        }
    }
    cls
}

/// The tree a star plan hangs on, in the order the query named the tables.
pub(super) struct Tree {
    pub b: usize,
    pub p: usize,
    /// (B's key column, P's key column).
    pub main: (usize, usize),
    /// Per table: its parent and (its own key column, the parent's column).
    pub parent: Vec<Option<(usize, usize, usize)>>,
    /// Per table: in `B`'s part.
    pub build: Vec<bool>,
    /// Column pairs that must be equal past the tree's own edges.
    pub checks: Vec<(Col, Col)>,
    /// Column pairs of two tables the WHERE compares (`l_quantity < v`).
    pub cmps: Vec<(Col, Col, Op)>,
    /// Per table joined by two columns: (its second key column, the parent's
    /// partner) — TPC-H's partsupp on (partkey, suppkey).
    pub parent2: Vec<Option<(usize, usize)>>,
    /// (table, its column, another dimension whose key that column equals):
    /// the other's map filters this one's rows as it loads (partsupp keeps
    /// the parts Q9's `p_name LIKE '%green%'` kept).
    pub filters: Vec<(usize, usize, usize)>,
}

/// Hang every table under `B` or `P`: `B` on a column it shares with `P`;
/// each other table by its unique key (its first column, checked at load)
/// under a table already placed it shares that key's class with, `B`'s part
/// first. `None`: some table cannot be placed.
pub(super) fn tree(n: usize, p: usize, b: usize, eqs: &[(Col, Col)]) -> Option<Tree> {
    let cls = classes(eqs);
    let class_of = |c: Col| cls.iter().position(|x| x.contains(&c));
    // B on P: a class holding a column of each, B's first column first.
    let mut main = None;
    for c in &cls {
        let pc = c.iter().find(|x| x.0 == p);
        let mut bcs: Vec<&Col> = c.iter().filter(|x| x.0 == b).collect();
        bcs.sort_by_key(|x| x.1 != 0);
        if let (Some(pc), Some(bc)) = (pc, bcs.first()) {
            main = Some((bc.1, pc.1));
            break;
        }
    }
    let main = main?;
    let mut parent: Vec<Option<(usize, usize, usize)>> = vec![None; n];
    let mut parent2: Vec<Option<(usize, usize)>> = vec![None; n];
    let mut placed = vec![false; n];
    let mut build = vec![false; n];
    placed[b] = true;
    placed[p] = true;
    build[b] = true;
    let mut edges: Vec<(Col, Col)> = vec![((b, main.0), (p, main.1))];
    loop {
        let mut moved = false;
        for t in 0..n {
            if placed[t] {
                continue;
            }
            let Some(k) = class_of((t, 0)) else { continue };
            // Two key columns both joined to one placed table: the pair is
            // the key (TPC-H's partsupp); checked unique at load.
            if let Some(k1) = class_of((t, 1)) {
                let mut cand: Vec<(usize, usize, usize)> = cls[k]
                    .iter()
                    .filter(|x| x.0 != t && placed[x.0])
                    .filter_map(|&(u, uc)| cls[k1].iter().find(|y| y.0 == u).map(|y| (u, uc, y.1)))
                    .collect();
                cand.sort_by_key(|x| !build[x.0]);
                if let Some(&(u, uc, uc1)) = cand.first() {
                    parent[t] = Some((u, 0, uc));
                    parent2[t] = Some((1, uc1));
                    placed[t] = true;
                    build[t] = build[u];
                    edges.push(((t, 0), (u, uc)));
                    edges.push(((t, 1), (u, uc1)));
                    moved = true;
                    continue;
                }
            }
            // A placed partner, B's part first.
            let mut cand: Vec<&Col> = cls[k].iter().filter(|x| x.0 != t && placed[x.0]).collect();
            cand.sort_by_key(|x| !build[x.0]);
            if let Some(&&(u, uc)) = cand.first() {
                parent[t] = Some((u, 0, uc));
                placed[t] = true;
                build[t] = build[u];
                edges.push(((t, 0), (u, uc)));
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    if placed.iter().any(|x| !x) {
        return None;
    }
    // Per class: the columns the tree's edges join are one component each;
    // every other component must equal the first.
    let mut checks = Vec::new();
    for c in &cls {
        let mut comp: Vec<Vec<Col>> = c.iter().map(|&x| vec![x]).collect();
        for &(x, y) in &edges {
            let (Some(i), Some(j)) = (comp.iter().position(|v| v.contains(&x)), comp.iter().position(|v| v.contains(&y))) else { continue };
            if i != j {
                let m = comp.remove(i.max(j));
                comp[i.min(j)].extend(m);
            }
        }
        for o in comp.iter().skip(1) {
            checks.push((comp[0][0], o[0]));
        }
    }
    // A dimension's column in another single-key dimension's key class (one
    // neither under the other): that one's map filters this one at load.
    let under = |mut x: usize, a: usize| {
        while let Some((u, ..)) = parent[x] {
            if u == a {
                return true;
            }
            x = u;
        }
        false
    };
    let mut filters = Vec::new();
    for d in 0..n {
        if d == b || d == p {
            continue;
        }
        for e in 0..n {
            if e == d || e == b || e == p || parent2[e].is_some() || under(d, e) || under(e, d) {
                continue;
            }
            if let Some(ke) = class_of((e, 0)) {
                if let Some(&(_, dc)) = cls[ke].iter().find(|x| x.0 == d) {
                    // One direction only (the first found), so loads order.
                    if !filters.iter().any(|f: &(usize, usize, usize)| f.0 == e && f.2 == d) {
                        filters.push((d, dc, e));
                    }
                }
            }
        }
    }
    Some(Tree { b, p, main, parent, build, checks, cmps: Vec::new(), parent2, filters })
}

/// Where a field's value comes from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Src {
    /// The table's row (`B`, `P`) or its payload (a dimension).
    Row,
    /// A dimension's key, from the FK of the row above.
    Fk,
    /// Read after the aggregation (`Late`).
    Deferred,
    /// A dimension's CASE condition bits (computed at its load).
    Flag,
}

/// A part's fields flattened: (table, schema column, GROUP BY index or
/// `None` for a check value, source), the table's own first, then each
/// child's depth first.
type Flat = Vec<(usize, usize, Option<usize>, Src)>;

type Own = Vec<Vec<(usize, Option<usize>, Src)>>;

fn flat(r: usize, own: &Own, kids: &dyn Fn(usize) -> Vec<usize>, out: &mut Flat) {
    for &(c, g, src) in &own[r] {
        out.push((r, c, g, src));
    }
    for k in kids(r) {
        flat(k, own, kids, out);
    }
}

/// The field layout of a star plan; moves the dimension tables out of
/// `sides` (which keeps `B` and `P`). `may_defer(g)`: GROUP BY column `g`
/// can be read after the aggregation (no ORDER BY before a LIMIT needs it).
pub(super) fn layout(sides: &mut Vec<Side>, t: &Tree, group_cols: &[Col], may_defer: &dyn Fn(usize) -> bool) -> Result<Star> {
    let n = sides.len();
    let mut own: Own = vec![Vec::new(); n];
    for (gi, &(tb, c)) in group_cols.iter().enumerate() {
        own[tb].push((c, Some(gi), Src::Row));
    }
    #[derive(PartialEq)]
    enum K {
        LocalB,
        LocalP,
        Cross,
    }
    let scale_of = |x: Col| match sides[x.0].schema[x.1].kind {
        ArrowKind::Decimal { s, .. } => s,
        _ => 0,
    };
    let mut chk = Vec::new();
    for (a, b, op) in t.checks.iter().map(|&(a, b)| (a, b, Op::Eq)).chain(t.cmps.iter().copied()) {
        let (a, b, k, op) = match (t.build[a.0], t.build[b.0]) {
            (true, true) => (a, b, K::LocalB, op),
            (false, false) => (a, b, K::LocalP, op),
            (true, false) => (a, b, K::Cross, op),
            (false, true) => (b, a, K::Cross, op.flip()),
        };
        for x in [a, b] {
            if !own[x.0].iter().any(|f| f.0 == x.1 && f.1.is_none()) {
                own[x.0].push((x.1, None, Src::Row));
            }
        }
        let c = if op == Op::Eq && scale_of(a) == scale_of(b) { Cmp::EQ } else { Cmp { op, sa: scale_of(a), sb: scale_of(b) } };
        chk.push((a, b, k, c));
    }
    // A dimension whose key is grouped: the key from the FK, first; its
    // other GROUP BY columns late when they may be.
    let mut key_first = vec![false; n];
    for x in 2..n {
        let k = t.parent[x].expect("every table placed").1;
        if let Some(i) = own[x].iter().position(|f| f.0 == k && f.1.is_some()) {
            // A probe-part dimension's key from its FK as well; its other
            // GROUP BY columns stay in the payload (late reads are the
            // build part's).
            if !t.build[x] {
                let mut f = own[x].remove(i);
                f.2 = Src::Fk;
                own[x].insert(0, f);
                key_first[x] = true;
                continue;
            }
            let mut f = own[x].remove(i);
            f.2 = Src::Fk;
            own[x].insert(0, f);
            key_first[x] = true;
            // Late reads match their rows by an integer key.
            let int_key = matches!(sides[x].schema[k].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64);
            for f in own[x].iter_mut().skip(1) {
                if int_key && f.1.is_some_and(|g| may_defer(g)) && f.2 == Src::Row {
                    f.2 = Src::Deferred;
                }
            }
        }
    }
    // The fact's lookup columns: each a value field of its dimension, which
    // must hang under the fact.
    let lookups = sides[1].lookups.clone();
    for &(_, d, c) in &lookups {
        if t.build[d] {
            return Err(unsupported(format!(
                "an aggregate reading {}.{}, a table joined through the GROUP BY table",
                sides[d].name, sides[d].schema[c].name
            )));
        }
        if !own[d].iter().any(|f| f.0 == c && f.1.is_none()) {
            own[d].push((c, None, Src::Row));
        }
    }
    // A probe-part dimension's CASE conditions: one field of their bits,
    // after its own (its payload carries it).
    let mut flagged = vec![false; n];
    for x in 2..n {
        if !sides[x].flags.is_empty() {
            if t.build[x] {
                return Err(unsupported(format!(
                    "a CASE whose conditions name {}, a table joined through the GROUP BY table (the GROUP BY table's own, or a table under the aggregated one, can)",
                    sides[x].name
                )));
            }
            own[x].push((usize::MAX, None, Src::Flag));
            flagged[x] = true;
        }
    }
    let parent = t.parent.clone();
    let kids = move |x: usize| -> Vec<usize> { (0..n).filter(|&y| parent[y].is_some_and(|p| p.0 == x)).collect() };
    let (mut bfull, mut pfull) = (Flat::new(), Flat::new());
    flat(0, &own, &kids, &mut bfull);
    flat(1, &own, &kids, &mut pfull);
    // What the passes build: every field but the late ones.
    let bflat: Flat = bfull.iter().copied().filter(|f| f.3 != Src::Deferred).collect();
    let pflat: Flat = pfull.iter().copied().filter(|f| f.3 != Src::Deferred).collect();
    let pos = |fl: &Flat, x: Col| fl.iter().position(|f| f.0 == x.0 && f.1 == x.1 && f.2.is_none()).expect("a check value is a field");
    let cross_b: Vec<usize> = chk.iter().filter(|c| c.2 == K::Cross).map(|c| pos(&bflat, c.0)).collect();
    let keep: Vec<bool> = bflat.iter().enumerate().map(|(i, f)| f.2.is_some() || cross_b.contains(&i)).collect();
    let mut kept_ix = vec![usize::MAX; bflat.len()];
    let mut hidden = Vec::new();
    for (i, f) in bflat.iter().enumerate() {
        if keep[i] {
            kept_ix[i] = hidden.len();
            hidden.push(f.2.is_none());
        }
    }
    // GROUP BY index → its field in the final key and in the passes' key.
    let mut gpos = vec![0; group_cols.len()];
    let mut gpos_rt = vec![None; group_cols.len()];
    let mut gkind = vec![ArrowKind::Int64; group_cols.len()];
    let (mut j, mut jr) = (0, 0);
    for f in bfull.iter().chain(pfull.iter()) {
        if let Some(g) = f.2 {
            gpos[g] = j;
            gkind[g] = sides[f.0].schema[f.1].kind;
            j += 1;
            if f.3 != Src::Deferred {
                gpos_rt[g] = Some(jr);
                jr += 1;
            }
        }
    }
    let pgroup: Vec<usize> = pflat.iter().enumerate().filter(|(_, f)| f.2.is_some()).map(|(i, _)| i).collect();
    let pflags: Vec<(usize, usize)> = pflat.iter().enumerate().filter(|(_, f)| f.3 == Src::Flag).map(|(i, f)| (f.0 - 2, i)).collect();
    let plookups: Vec<(usize, usize, ArrowKind)> = lookups
        .iter()
        .map(|&(i, d, c)| {
            let at = sides[1].read.iter().position(|&x| x == i).expect("a lookup column is read");
            let pi = pflat.iter().position(|f| f.0 == d && f.1 == c && f.2.is_none()).expect("a lookup value is a field");
            (at, pi, sides[d].schema[c].kind)
        })
        .collect();
    let local_b = chk.iter().filter(|c| c.2 == K::LocalB).map(|c| (pos(&bflat, c.0), pos(&bflat, c.1), c.3)).collect();
    let local_p = chk.iter().filter(|c| c.2 == K::LocalP).map(|c| (pos(&pflat, c.0), pos(&pflat, c.1), c.3)).collect();
    let cross = chk.iter().filter(|c| c.2 == K::Cross).map(|c| (kept_ix[pos(&bflat, c.0)], pos(&pflat, c.1), c.3)).collect();
    // The late reads' projections, before the dimensions' own wants.
    let mut late = Vec::new();
    for x in 2..n {
        let cols: Vec<(usize, usize)> = own[x].iter().filter(|f| f.2 == Src::Deferred).map(|f| (f.1.expect("grouped"), f.0)).collect();
        if cols.is_empty() {
            continue;
        }
        let mut side = sides[x].clone();
        side.read = if side.whole { side.read.clone() } else { Vec::new() };
        let k = t.parent[x].expect("placed").1;
        side.want(k);
        let cols = cols.into_iter().map(|(g, c)| (g, side.want(c))).collect();
        let key_g = own[x][0].1.expect("the key is grouped");
        late.push(Late { dim: x - 2, key_g, side, cols });
    }
    let mut gfull = Vec::new();
    for f in bfull.iter().chain(pfull.iter()).filter(|f| f.2.is_some()) {
        gfull.push(match f.3 {
            Src::Deferred => {
                let l = late.iter().position(|l| l.dim == f.0 - 2).expect("a late read");
                let c = late[l].cols.iter().position(|c| Some(c.0) == f.2).expect("its column");
                Some((l, c))
            }
            _ => None,
        });
    }
    // Read positions, before the dimensions leave `sides`: a table's row
    // fields only (an FK-borne key and the late columns are not read here).
    let want_own = |sides: &mut Vec<Side>, x: usize| -> Vec<usize> {
        own[x].iter().filter(|f| f.2 == Src::Row).map(|f| sides[x].want(f.0)).collect()
    };
    let bfields = want_own(sides, 0);
    let pfields = want_own(sides, 1);
    let mut dims = Vec::new();
    let mut parts = Vec::new();
    for x in 2..n {
        let (u, k, uc) = t.parent[x].expect("every table placed");
        let fk = sides[u].want(uc);
        let key = sides[x].want(k);
        let key2 = t.parent2[x].map(|(k1, uc1)| (sides[x].want(k1), sides[u].want(uc1)));
        let fields = want_own(sides, x);
        let up = if u < 2 { Up::Main(u) } else { Up::Dim(u - 2) };
        let filters: Vec<(usize, usize)> = t.filters.iter().filter(|f| f.0 == x).map(|f| (sides[x].want(f.1), f.2 - 2)).collect();
        parts.push((key, up, fk, fields, kids(x).into_iter().map(|y| y - 2).collect::<Vec<_>>(), key_first[x], flagged[x], key2, filters));
    }
    for (side, (key, up, fk, fields, kids, key_first, flag, key2, filters)) in sides.drain(2..).zip(parts) {
        dims.push(Dim { side, key, up, fk, fields, kids, key_first, flag, key2, filters });
    }
    Ok(Star {
        dims,
        bfields,
        pfields,
        bkids: kids(0).into_iter().map(|y| y - 2).collect(),
        pkids: kids(1).into_iter().map(|y| y - 2).collect(),
        keep,
        local_b,
        local_p,
        cross,
        gpos,
        gkind,
        hidden,
        gpos_rt,
        late,
        gfull,
        pgroup,
        pflags,
        plookups,
    })
}

/// A dimension in memory: its key → its payload, the encoded fields that
/// flow up (its own, then its children's).
pub(super) struct DimMap {
    store: Store,
    payloads: Vec<Box<[u8]>>,
    ids: HashMap<Box<[u8]>, u32, foldhash::fast::RandomState>,
    name: String,
    key: String,
    /// A two-column key as one: `k1 * span + (k2 - lo)` over the second
    /// column's range (lo, span).
    combo: Option<(i64, i64)>,
}

enum Store {
    /// Direct-address over [lo, lo + len): `MAX` = no row at that key.
    D8(i64, Vec<u8>),
    D32(i64, Vec<u32>),
    /// (key, payload id), sorted when the load ends (`seal`): 16 bytes a
    /// key and no rehash doubling the map mid-load (TPC-H Q9's 2.3M
    /// partsupp keys measured past 256 MB as a hash map).
    Sorted(Vec<(i64, u32)>),
}

/// Key `k`'s place in sorted `v`: a guess where the keys' spread puts it,
/// then a gallop from there (a binary search took ~21 dependent loads a probe
/// row over TPC-H Q9's 2.3M partsupp keys, 17% of its CPU; their combined
/// keys spread evenly, so the guess lands within a few). Never worse than
/// twice a binary search's steps.
#[inline]
fn find_sorted(v: &[(i64, u32)], k: i64) -> Option<usize> {
    let n = v.len();
    let (lo, hi) = (v.first()?.0, v[n - 1].0);
    if k < lo || k > hi {
        return None;
    }
    if n < 64 || hi == lo {
        return v.binary_search_by_key(&k, |x| x.0).ok();
    }
    let g = ((k as i128 - lo as i128) * (n as i128 - 1) / (hi as i128 - lo as i128)) as usize;
    let search = |a: usize, b: usize| v[a..=b].binary_search_by_key(&k, |x| x.0).ok().map(|i| a + i);
    let mut step = 1usize;
    if v[g].0 == k {
        return Some(g);
    }
    if v[g].0 < k {
        let mut a = g;
        loop {
            let b = a.saturating_add(step).min(n - 1);
            if v[b].0 >= k {
                return search(a, b);
            }
            if b == n - 1 {
                return None;
            }
            a = b;
            step *= 2;
        }
    }
    let mut b = g;
    loop {
        let a = b.saturating_sub(step);
        if v[a].0 <= k {
            return search(a, b);
        }
        if a == 0 {
            return None;
        }
        b = a;
        step *= 2;
    }
}

/// Key ranges this wide are held as a direct array (a byte per key while
/// the payloads are fewer than 255).
const DIM_DIRECT_MAX: u64 = 64 << 20;

impl DimMap {
    fn new(name: &str, key: &str, kr: Kr) -> DimMap {
        let store = match kr {
            Kr::Known(lo, hi) if u64::try_from(hi as i128 - lo as i128 + 1).is_ok_and(|n| n <= DIM_DIRECT_MAX) => {
                Store::D8(lo, vec![u8::MAX; (hi as i128 - lo as i128 + 1) as usize])
            }
            _ => Store::Sorted(Vec::new()),
        };
        DimMap { store, payloads: Vec::new(), ids: HashMap::default(), name: name.to_string(), key: key.to_string(), combo: None }
    }

    /// A map keyed by two columns: the first's range `kr`, the second's `kr2`.
    fn new2(name: &str, key: &str, kr: Kr, kr2: Kr) -> DimMap {
        let combo = match kr2 {
            Kr::Known(lo, hi) => i64::try_from(hi as i128 - lo as i128 + 1).ok().map(|span| (lo, span)),
            _ => None,
        };
        let whole = match (kr, combo) {
            (Kr::Known(a, b), Some((_, span))) => match (a.checked_mul(span), b.checked_mul(span).and_then(|x| x.checked_add(span - 1))) {
                (Some(x), Some(y)) => Kr::Known(x, y),
                _ => Kr::Unknown,
            },
            _ => Kr::Unknown,
        };
        let mut m = DimMap::new(name, key, whole);
        // No range for the second column: no combined key (the load refuses).
        m.combo = combo;
        m
    }

    /// The combined key of a two-column key (`None`: outside its ranges).
    #[inline]
    pub(super) fn pair(&self, k1: i64, k2: i64) -> Option<i64> {
        let (lo, span) = self.combo?;
        let o = k2.checked_sub(lo).filter(|&o| (0..span).contains(&o))?;
        k1.checked_mul(span)?.checked_add(o)
    }

    #[inline]
    pub(super) fn get(&self, k: i64) -> Option<&[u8]> {
        let id = match &self.store {
            Store::D8(lo, a) => {
                let i = usize::try_from(k.checked_sub(*lo)?).ok()?;
                let x = *a.get(i)?;
                (x != u8::MAX).then_some(u32::from(x))?
            }
            Store::D32(lo, a) => {
                let i = usize::try_from(k.checked_sub(*lo)?).ok()?;
                let x = *a.get(i)?;
                (x != u32::MAX).then_some(x)?
            }
            Store::Sorted(v) => v[find_sorted(v, k)?].1,
        };
        Some(&self.payloads[id as usize])
    }

    fn pid(&mut self, pay: &[u8]) -> Result<u32> {
        if let Some(&i) = self.ids.get(pay) {
            return Ok(i);
        }
        let i = u32::try_from(self.payloads.len()).map_err(|_| bad("a dimension of more than 4 billion payloads"))?;
        let b: Box<[u8]> = pay.into();
        self.ids.insert(b.clone(), i);
        self.payloads.push(b);
        if i == 255 {
            // Past a byte: the array widens.
            if let Store::D8(lo, a) = &self.store {
                let w = a.iter().map(|&x| if x == u8::MAX { u32::MAX } else { u32::from(x) }).collect();
                self.store = Store::D32(*lo, w);
            }
        }
        Ok(i)
    }

    fn set(&mut self, k: i64, id: u32) -> Result<()> {
        let repeat = || {
            unsupported(format!(
                "{} repeats the key {k} on {} (a table joined to the rest must join by a unique key; v1 takes its first column)",
                self.name, self.key
            ))
        };
        let slot_of = |lo: i64, len: usize| usize::try_from(k.checked_sub(lo)?).ok().filter(|&i| i < len);
        match &mut self.store {
            Store::D8(lo, a) => match slot_of(*lo, a.len()) {
                Some(i) if a[i] == u8::MAX => a[i] = id as u8,
                Some(_) => return Err(repeat()),
                None => return Err(bad(format!("{}: a key past the range its server gave", self.name))),
            },
            Store::D32(lo, a) => match slot_of(*lo, a.len()) {
                Some(i) if a[i] == u32::MAX => a[i] = id,
                Some(_) => return Err(repeat()),
                None => return Err(bad(format!("{}: a key past the range its server gave", self.name))),
            },
            // Repeats are found when sealed.
            Store::Sorted(v) => v.push((k, id)),
        }
        Ok(())
    }

    /// The load is over: a sorted store is sorted, and a repeated key
    /// refused, naming the table.
    fn seal(&mut self) -> Result<()> {
        // The payloads' dedup index is the load's alone.
        self.ids = HashMap::default();
        if let Store::Sorted(v) = &mut self.store {
            v.sort_unstable_by_key(|x| x.0);
            v.shrink_to_fit();
            if let Some(w) = v.windows(2).find(|w| w[0].0 == w[1].0) {
                return Err(unsupported(format!(
                    "{} repeats the key {} on {} (a table joined to the rest must join by a unique key; v1 takes its first column, or its first two)",
                    self.name, w[0].0, self.key
                )));
            }
        }
        Ok(())
    }

    /// The lowest and highest key held (`None`: none, or a two-column map).
    pub(super) fn key_span(&self) -> Option<(i64, i64)> {
        if self.combo.is_some() {
            return None;
        }
        let mut lo_hi = None;
        match &self.store {
            Store::D8(lo, a) => {
                let first = a.iter().position(|&x| x != u8::MAX)?;
                let last = a.iter().rposition(|&x| x != u8::MAX)?;
                lo_hi = Some((lo + first as i64, lo + last as i64));
            }
            Store::D32(lo, a) => {
                let first = a.iter().position(|&x| x != u32::MAX)?;
                let last = a.iter().rposition(|&x| x != u32::MAX)?;
                lo_hi = Some((lo + first as i64, lo + last as i64));
            }
            Store::Sorted(v) => {
                if let (Some(a), Some(b)) = (v.first(), v.last()) {
                    lo_hi = Some((a.0, b.0));
                }
            }
        }
        lo_hi
    }

    /// Keys held.
    pub(super) fn key_count(&self) -> u64 {
        match &self.store {
            Store::D8(_, a) => a.iter().filter(|&&x| x != u8::MAX).count() as u64,
            Store::D32(_, a) => a.iter().filter(|&&x| x != u32::MAX).count() as u64,
            Store::Sorted(v) => v.len() as u64,
        }
    }

    /// Every key held.
    pub(super) fn for_each_key(&self, mut f: impl FnMut(i64)) {
        match &self.store {
            Store::D8(lo, a) => a.iter().enumerate().filter(|x| *x.1 != u8::MAX).for_each(|(i, _)| f(lo + i as i64)),
            Store::D32(lo, a) => a.iter().enumerate().filter(|x| *x.1 != u32::MAX).for_each(|(i, _)| f(lo + i as i64)),
            Store::Sorted(v) => v.iter().for_each(|x| f(x.0)),
        }
    }

    /// A direct-address map (a lookup is one load, not a search).
    #[inline]
    fn direct(&self) -> bool {
        !matches!(self.store, Store::Sorted(_))
    }

    pub(super) fn bytes(&self) -> u64 {
        let st = match &self.store {
            Store::D8(_, a) => a.len() as u64,
            Store::D32(_, a) => 4 * a.len() as u64,
            Store::Sorted(v) => 16 * v.capacity() as u64,
        };
        st + self.payloads.iter().map(|p| p.len() as u64 + 40).sum::<u64>()
    }
}

/// Every dimension read once into its map, children before their parents
/// (a row whose child misses is dropped: the semi-join, here). A dimension
/// starts the moment its children and filters are in, beside the others
/// running then: the loads wait on their servers (TPC-H Q8's six took 9.9 s
/// of wall for 1.9 s of CPU one after another).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn load(st: &Star) -> Result<Vec<DimMap>> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let n = st.dims.len();
    let deps = |d: usize| st.dims[d].kids.iter().copied().chain(st.dims[d].filters.iter().map(|f| f.1)).collect::<Vec<_>>();
    // No more pipes than the CPU decodes (twice the cores, at least 2),
    // shared by the loads that start together.
    let cap = crate::pipeline::cpu_limit_cores().map(|c| ((c.ceil() as usize) * 2).max(2));
    let mut maps: Vec<Option<Arc<DimMap>>> = (0..n).map(|_| None).collect();
    let mut started = vec![false; n];
    let mut running = FuturesUnordered::new();
    let mut done = 0;
    while done < n {
        let ready: Vec<usize> = (0..n).filter(|&d| !started[d] && deps(d).iter().all(|&e| maps[e].is_some())).collect();
        let pipes = cap.map(|c| (c / (ready.len() + running.len()).max(1)).max(1));
        for d in ready {
            started[d] = true;
            let snap = maps.clone();
            running.push(async move { load_dim(st, d, &snap, pipes).await.map(|m| (d, m)) });
        }
        match running.next().await {
            Some(r) => {
                let (d, m) = r?;
                maps[d] = Some(Arc::new(m));
                done += 1;
            }
            None => return Err(bad("a dimension that waits on itself")),
        }
    }
    drop(running);
    maps.into_iter()
        .map(|m| Arc::try_unwrap(m.expect("every dimension loaded")).map_err(|_| bad("a dimension map still shared")))
        .collect()
}

/// Dimension `d` read into its map; `maps` holds its children and filters.
async fn load_dim(st: &Star, d: usize, maps: &[Option<Arc<DimMap>>], pipes: Option<usize>) -> Result<DimMap> {
    let mut pay = Vec::new();
    let dim = &st.dims[d];
    let kname = dim.side.schema[dim.side.read[dim.key]].name.clone();
    let kr = key_range(&dim.side, &kname).await?;
    let mut m = match dim.key2 {
        None => DimMap::new(&dim.side.name, &kname, kr),
        Some((k2, _)) => {
            let k2name = dim.side.schema[dim.side.read[k2]].name.clone();
            let kr2 = key_range(&dim.side, &k2name).await?;
            let m = DimMap::new2(&dim.side.name, &format!("({kname}, {k2name})"), kr, kr2);
            if m.combo.is_none() {
                return Err(unsupported(format!("{}: a two-column key whose second column has no range", dim.side.name)));
            }
            m
        }
    };
    let tys = side_tys(&dim.side);
    // Its rows must be in the maps of its filters and its children: those
    // gate the read (a row they drop would be dropped below).
    let mut of: Vec<(usize, &DimMap)> = dim.filters.iter().map(|&(c, e)| (c, maps[e].as_deref().expect("filters first"))).collect();
    of.extend(dim.kids.iter().filter(|&&k| st.dims[k].key2.is_none()).map(|&k| (st.dims[k].fk, maps[k].as_deref().expect("children first"))));
    let gates = dim_gates(&dim.side, &of);
    if dbg() && !gates.is_empty() {
        eprintln!("[sql] dimension {}: {} gate(s), {} KiB", dim.side.name, gates.len(), gates.iter().map(|g| g.bytes()).sum::<u64>() >> 10);
    }
    // A selective gate: the load waits on its server's scan, not on the
    // decode (TPC-H Q9's partsupp kept 2.3M of 52.8M rows: 32 s of wall for
    // 8 s of CPU at two pipes), so up to the scan pipes a filtered probe
    // takes, within the memory the maps held so far leave.
    let pipes = if selective(&gates) {
        let held: u64 = maps.iter().flatten().map(|m| m.bytes()).sum::<u64>() + gates.iter().map(|g| g.bytes()).sum::<u64>();
        crate::pipeline::mem_limit_bytes()
            .map(|m| crate::read_impl::read_pipes_for(m.saturating_sub(held + PROBE_HEADROOM)).clamp(1, SERVER_SCAN_PIPES))
            .or(pipes)
    } else {
        pipes
    };
    let mut h = open_gated(&dim.side, None, pipes, None, gates).await?;
    let mut rows = 0u64;
    let dim_cap = super::build_budget();
    while let Some(b) = h.recv().await? {
        'row: for r in 0..b.rows {
            if !local_ok(&dim.side, &tys, &b, r) {
                continue;
            }
            let Some(k) = key_at(&b.cols[dim.key], r)? else { continue };
            let k = match dim.key2 {
                None => k,
                Some((k2, _)) => {
                    let Some(k2) = key_at(&b.cols[k2], r)? else { continue };
                    match m.pair(k, k2) {
                        Some(c) => c,
                        None => continue,
                    }
                }
            };
            // Another dimension's map this row's column must be in.
            let mut kept = true;
            for &(c, e) in &dim.filters {
                match key_at(&b.cols[c], r)? {
                    Some(v) if maps[e].as_deref().expect("filters first").get(v).is_some() => {}
                    _ => kept = false,
                }
            }
            if !kept {
                continue;
            }
            pay.clear();
            for &f in &dim.fields {
                encode_group(&b.cols[f], r, &mut pay);
            }
            if dim.flag {
                let mut m = 0u64;
                for (j, f) in dim.side.flags.iter().enumerate() {
                    if peval(f, &b, r, &tys) == Some(true) {
                        m |= 1 << j;
                    }
                }
                put_field(&mut pay, 1, &(m as i64).to_le_bytes());
            }
            for &kid in &dim.kids {
                let kd = &st.dims[kid];
                let Some(fk) = key_at(&b.cols[kd.fk], r)? else { continue 'row };
                let km = maps[kid].as_deref().expect("children first");
                let fk = match kd.key2 {
                    None => fk,
                    Some((_, f2)) => match key_at(&b.cols[f2], r)?.and_then(|v| km.pair(fk, v)) {
                        Some(c) => c,
                        None => continue 'row,
                    },
                };
                match km.get(fk) {
                    Some(x) => {
                        if kd.key_first {
                            encode_group(&b.cols[kd.fk], r, &mut pay);
                        }
                        pay.extend_from_slice(x)
                    }
                    None => continue 'row,
                }
            }
            let id = m.pid(&pay)?;
            m.set(k, id)?;
            rows += 1;
        }
        // A dimension past the build budget is refused here, not after it
        // has taken the memory the build and the probe need (the maps are
        // held through every pass; `ids` is priced as payloads again).
        let held = m.bytes() + m.payloads.len() as u64 * 24;
        if held > dim_cap {
            return Err(unsupported(format!(
                "the dimension {} ({} MiB held at {rows} rows) is larger than the build budget ({} MiB); raise the memory limit",
                dim.side.name,
                held >> 20,
                dim_cap >> 20
            )));
        }
    }
    m.seal()?;
    if dbg() {
        eprintln!("[sql] dimension {}: {rows} rows, {} payload(s), {} KiB", dim.side.name, m.payloads.len(), m.bytes() >> 10);
    }
    Ok(m)
}

/// One field into a group key (`group_fields` reads it back).
fn put_field(kb: &mut Vec<u8>, tag: u8, v: &[u8]) {
    kb.push(tag);
    if tag != 0 {
        kb.extend_from_slice(&(v.len() as u32).to_le_bytes());
        kb.extend_from_slice(v);
    }
}

/// Two fields equal as SQL has it (a NULL equals nothing).
#[inline]
fn same(a: (u8, &[u8]), b: (u8, &[u8])) -> bool {
    a.0 != 0 && a == b
}

/// A part's row flattened into `full`: its own fields, then each child
/// dimension's payload; `false` when a dimension holds no row for it or a
/// check within the part fails.
fn row(fields: &[usize], kids: &[usize], local: &[(usize, usize, Cmp)], st: &Star, maps: &[DimMap], b: &ArrowBatch, r: usize, full: &mut Vec<u8>) -> Result<bool> {
    // The dimensions first, the direct ones before the searched ones: a row
    // that joins no dimension row is dropped by one load, before any field
    // of it is encoded (TPC-H Q9's partsupp search ran for every lineitem
    // row while its part, one byte away, dropped 96% of them).
    const K: usize = 8;
    let mut got: [&[u8]; K] = [&[]; K];
    if kids.len() > K {
        return row_slow(fields, kids, local, st, maps, b, r, full);
    }
    for direct in [true, false] {
        for (j, &kid) in kids.iter().enumerate() {
            if maps[kid].direct() != direct {
                continue;
            }
            let kd = &st.dims[kid];
            let Some(fk) = key_at(&b.cols[kd.fk], r)? else { return Ok(false) };
            let fk = match kd.key2 {
                None => fk,
                Some((_, f2)) => match key_at(&b.cols[f2], r)?.and_then(|v| maps[kid].pair(fk, v)) {
                    Some(c) => c,
                    None => return Ok(false),
                },
            };
            match maps[kid].get(fk) {
                Some(x) => got[j] = x,
                None => return Ok(false),
            }
        }
    }
    full.clear();
    for &f in fields {
        encode_group(&b.cols[f], r, full);
    }
    for (j, &kid) in kids.iter().enumerate() {
        if st.dims[kid].key_first {
            encode_group(&b.cols[st.dims[kid].fk], r, full);
        }
        full.extend_from_slice(got[j]);
    }
    if !local.is_empty() {
        let f = group_fields(full);
        if local.iter().any(|&(x, y, c)| !check(f[x], f[y], c)) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// [`row`] for a part with more dimensions than it holds on the stack.
#[allow(clippy::too_many_arguments)]
fn row_slow(fields: &[usize], kids: &[usize], local: &[(usize, usize, Cmp)], st: &Star, maps: &[DimMap], b: &ArrowBatch, r: usize, full: &mut Vec<u8>) -> Result<bool> {
    full.clear();
    for &f in fields {
        encode_group(&b.cols[f], r, full);
    }
    for &kid in kids {
        let kd = &st.dims[kid];
        let Some(fk) = key_at(&b.cols[kd.fk], r)? else { return Ok(false) };
        let fk = match kd.key2 {
            None => fk,
            Some((_, f2)) => match key_at(&b.cols[f2], r)?.and_then(|v| maps[kid].pair(fk, v)) {
                Some(c) => c,
                None => return Ok(false),
            },
        };
        match maps[kid].get(fk) {
            Some(x) => {
                if kd.key_first {
                    encode_group(&b.cols[kd.fk], r, full);
                }
                full.extend_from_slice(x)
            }
            None => return Ok(false),
        }
    }
    if !local.is_empty() {
        let f = group_fields(full);
        if local.iter().any(|&(x, y, c)| !check(f[x], f[y], c)) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The group key of a build row: `false` when the row joins no dimension
/// row or fails a check.
pub(super) fn build_key(st: &Star, maps: &[DimMap], b: &ArrowBatch, r: usize, kb: &mut Vec<u8>, full: &mut Vec<u8>) -> Result<bool> {
    if !row(&st.bfields, &st.bkids, &st.local_b, st, maps, b, r, full)? {
        return Ok(false);
    }
    kb.clear();
    if st.keep.iter().all(|&k| k) {
        kb.extend_from_slice(full);
    } else {
        for (i, (t, v)) in group_fields(full).into_iter().enumerate() {
            if st.keep[i] {
                put_field(kb, t, v);
            }
        }
    }
    Ok(true)
}

/// The probe row's fields (`pv`): `false` when it joins no dimension row or
/// fails a check within its part.
pub(super) fn probe_row(st: &Star, maps: &[DimMap], b: &ArrowBatch, r: usize, pv: &mut Vec<u8>) -> Result<bool> {
    row(&st.pfields, &st.pkids, &st.local_p, st, maps, b, r, pv)
}

/// Each group's kept fields the probe-side checks compare against.
pub(super) fn cross_values(st: &Star, keys: &KeyArena) -> Vec<Vec<(u8, Vec<u8>)>> {
    if st.cross.is_empty() {
        return Vec::new();
    }
    keys.iter()
        .map(|k| {
            let f = group_fields(k);
            st.cross.iter().map(|&(g, _, _)| (f[g].0, f[g].1.to_vec())).collect()
        })
        .collect()
}

/// Whether group `g` passes the probe row's cross checks (`pf`: the probe
/// row's fields).
#[inline]
pub(super) fn cross_ok(st: &Star, gcross: &[Vec<(u8, Vec<u8>)>], g: u32, pf: &[(u8, &[u8])]) -> bool {
    st.cross.iter().enumerate().all(|(j, &(_, pi, c))| {
        let v = &gcross[g as usize][j];
        check((v.0, &v.1), pf[pi], c)
    })
}

/// The late GROUP BY columns of the answer's groups (`rows`, the groups as
/// the passes built them): each late dimension read once for the keys the
/// rows hold, and the rows' keys rebuilt whole, in the final layout.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn read_late(st: &Star, groups: Groups, accs: Vec<Acc>, na: usize, rows: &[usize]) -> Result<(Groups, Vec<Acc>, Vec<bool>)> {
    // The answer's rows only, before the read: the other groups (hundreds of
    // thousands under a LIMIT 20) are let go, so the read has their memory.
    let keys: Vec<Box<[u8]>> = rows.iter().map(|&r| Box::<[u8]>::from(&groups.keys[r])).collect();
    let racc: Vec<Acc> = rows.iter().flat_map(|&r| accs[r * na..(r + 1) * na].iter().copied()).collect();
    drop(groups);
    drop(accs);
    // Per late read: the keys wanted, then their columns' encoded values.
    let mut got: Vec<HashMap<i64, Vec<(u8, Vec<u8>)>>> = Vec::new();
    for l in &st.late {
        let at = st.gpos_rt[l.key_g].expect("the key is built");
        let mut want: HashMap<i64, Vec<(u8, Vec<u8>)>> = HashMap::new();
        for k in &keys {
            let f = group_fields(k);
            if let (1, b) = f[at] {
                want.insert(i64::from_le_bytes(b.try_into().map_err(|_| bad("a key field"))?), Vec::new());
            }
        }
        let tys = side_tys(&l.side);
        // The keys wanted gate the read: the other rows' columns (TPC-H
        // Q10's names, addresses and comments of 9.9M customers, for 20)
        // are never decoded.
        let gates: Vec<Arc<KeyGate>> = match (want.keys().min(), want.keys().max()) {
            (Some(&lo), Some(&hi)) => gate_of(&l.side, 0, lo, hi, want.len() as u64, |g| want.keys().for_each(|&k| g.set(k))).into_iter().collect(),
            _ => Vec::new(),
        };
        let mut h = open_gated(&l.side, None, None, None, gates).await?;
        let mut one = Vec::new();
        while let Some(b) = h.recv().await? {
            for r in 0..b.rows {
                if !local_ok(&l.side, &tys, &b, r) {
                    continue;
                }
                let Some(k) = key_at(&b.cols[0], r)? else { continue };
                if let Some(v) = want.get_mut(&k) {
                    v.clear();
                    for &(_, c) in &l.cols {
                        one.clear();
                        encode_group(&b.cols[c], r, &mut one);
                        let (t, x) = group_fields(&one)[0];
                        v.push((t, x.to_vec()));
                    }
                }
            }
        }
        got.push(want);
    }
    let mut ng = Groups::default();
    let mut nacc = Vec::with_capacity(rows.len() * na);
    let mut kb = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        let f = group_fields(k);
        let mut next = 0;
        kb.clear();
        for slot in &st.gfull {
            match slot {
                None => {
                    put_field(&mut kb, f[next].0, f[next].1);
                    next += 1;
                }
                Some((l, c)) => {
                    let at = st.gpos_rt[st.late[*l].key_g].expect("built");
                    let k = i64::from_le_bytes(f[at].1.try_into().map_err(|_| bad("a key field"))?);
                    match got[*l].get(&k).and_then(|v| v.get(*c)) {
                        Some((t, x)) => put_field(&mut kb, *t, x),
                        None => return Err(bad(format!("{}: the key {k} is gone from its table since the aggregation", st.dims[st.late[*l].dim].side.name))),
                    }
                }
            }
        }
        ng.id(&kb)?;
        nacc.extend_from_slice(&racc[i * na..(i + 1) * na]);
    }
    let n = ng.keys.len();
    Ok((ng, nacc, vec![true; n]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TPC-H Q5's tables, in its FROM order: customer 0, orders 1,
    /// lineitem 2, supplier 3, nation 4, region 5; first columns are the keys.
    /// Columns: c (0 custkey, 1 nationkey), o (0 orderkey, 1 custkey),
    /// l (0 orderkey, 1 suppkey), s (0 suppkey, 1 nationkey),
    /// n (0 nationkey, 1 regionkey), r (0 regionkey).
    #[test]
    fn q5_hangs_nation_under_customer_and_checks_the_supplier_nation() {
        let eqs = vec![
            ((0, 0), (1, 1)), // c_custkey = o_custkey
            ((2, 0), (1, 0)), // l_orderkey = o_orderkey
            ((2, 1), (3, 0)), // l_suppkey = s_suppkey
            ((0, 1), (3, 1)), // c_nationkey = s_nationkey
            ((3, 1), (4, 0)), // s_nationkey = n_nationkey
            ((4, 1), (5, 0)), // n_regionkey = r_regionkey
        ];
        let t = tree(6, 2, 1, &eqs).expect("placed");
        assert_eq!(t.main, (0, 0));
        assert_eq!(t.parent[0], Some((1, 0, 1)), "customer under orders by o_custkey");
        assert_eq!(t.parent[3], Some((2, 0, 1)), "supplier under lineitem by l_suppkey");
        assert_eq!(t.parent[4], Some((0, 0, 1)), "nation under customer (B's part first)");
        assert_eq!(t.parent[5], Some((4, 0, 1)), "region under nation");
        assert_eq!(t.build, vec![true, true, false, false, true, true]);
        // The nation class joined c and n by the tree; s is checked.
        assert_eq!(t.checks.len(), 1);
        let (x, y) = t.checks[0];
        let mut pair = [x, y];
        pair.sort();
        assert!(pair.contains(&(3, 1)), "{pair:?}");
        assert!(pair.iter().any(|c| *c == (0, 1) || *c == (4, 0)), "{pair:?}");
    }

    /// The guessed search finds exactly what a binary search finds, over
    /// even and badly skewed keys, present and absent, at both ends.
    #[test]
    fn find_sorted_matches_binary_search() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for skew in [false, true] {
            let mut keys: Vec<i64> = (0..5000).map(|_| {
                let r = (next() % 1_000_000) as i64;
                if skew { r * r / 1000 } else { r }
            }).collect();
            keys.sort_unstable();
            keys.dedup();
            let v: Vec<(i64, u32)> = keys.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect();
            for _ in 0..20_000 {
                let k = (next() % 1_100_000) as i64 - 50_000;
                let k = if skew { k.abs() * (k.abs() / 1000) } else { k };
                assert_eq!(find_sorted(&v, k), v.binary_search_by_key(&k, |e| e.0).ok(), "k={k} skew={skew}");
            }
            for &(k, _) in v.iter().step_by(37).chain([v[0], v[v.len() - 1]].iter()) {
                assert_eq!(find_sorted(&v, k).map(|i| v[i].0), Some(k));
            }
        }
        assert_eq!(find_sorted(&[], 5), None);
        assert_eq!(find_sorted(&[(3, 0)], 3), Some(0));
    }

    #[test]
    fn a_table_joined_by_no_key_is_not_placed() {
        // t2 joins t0 by its second column only.
        let eqs = vec![((0, 0), (1, 0)), ((2, 1), (0, 1))];
        assert!(tree(3, 1, 0, &eqs).is_none());
    }
}
