//! `apitap.sql()`: a join + aggregate across two databases, computed in Rust.
//!
//! v1 runs one shape and refuses everything else before any read:
//!
//! ```sql
//! SELECT g.., AGG(expr).. FROM a.t1 x [INNER] JOIN b.t2 y ON x.k = y.k
//! WHERE <conjuncts, each naming one side> GROUP BY g.. ORDER BY .. LIMIT n
//! ```
//!
//! or the same over one table without the join. The plan is the
//! small-memory one the 5 GB + 5 GB measurement asked for
//! (docs/design/2026-11-05-sql-engine-and-memory.md):
//!
//! - each side's conjuncts and projection are pushed into its own source;
//! - the join key's range is asked of both sources (unfiltered `MIN`/`MAX`,
//!   instant on a key) and the intersection is pushed into both scans;
//! - the side holding the GROUP BY columns is read first and reduced to one
//!   group id per key, in a direct-address array when the key range allows
//!   it, else a hash map;
//! - the other side streams past it, and every matching row's aggregate
//!   arguments fold straight into its group's accumulators, so no joined row
//!   ever exists.
//!
//! Decimals are exact (i128 with a tracked scale); `/` and AVG are f64.

use crate::error::{Error, Result};
use crate::read::{ArrowField, ReadHandle, ReadOptions};
use crate::wire::arrowcol::{ArrowBatch, ArrowKind, FinishedCol, KeyGate};
use std::sync::Arc;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, ObjectNamePart, OrderByKind, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value,
};
use std::collections::HashMap;

fn unsupported(what: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("sql: {what} is not supported (apitap.sql() v1)"))
}

fn bad(what: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("sql: {what}"))
}

/// The answer: one Arrow batch.
pub struct SqlResult {
    pub schema: Vec<ArrowField>,
    pub batch: ArrowBatch,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
    Pg,
    My,
    /// github://, github+api://, gsheets://: no server to filter, so the
    /// side's WHERE runs here (`Side::local`) and its key range is unknown.
    Other,
}

/// One table of the query.
#[derive(Clone)]
struct Side {
    /// The name the query refers to it by: its alias, else its table name.
    name: String,
    url: String,
    table: String,
    dialect: Dialect,
    schema: Vec<ArrowField>,
    /// Columns this side reads, in read order (indices into `schema`).
    read: Vec<usize>,
    /// WHERE conjuncts pushed to this side, rendered.
    conj: Vec<String>,
    /// The same conjuncts as parsed: an incremental view re-checks them on
    /// every change image (`view`).
    conj_src: Vec<Expr>,
    /// WHERE conjuncts evaluated here, per row (a side with no SQL server).
    local: Vec<PExpr>,
    /// Computed `int8` columns the source evaluates (`pushdown_args`).
    computed: Vec<(String, String)>,
    /// The source cannot project (github:// CSV, gsheets://): every column is
    /// read, in schema order, so a read position is the schema index.
    whole: bool,
    /// Its rows filtered by subqueries' keys (`EXISTS`, `IN (SELECT …)`).
    semis: Vec<semi::Semi>,
    /// Conditions of CASE arms over this (the build) table whose values come
    /// from the other: each row's results as a bit mask, carried in the group
    /// key (`VExpr::CaseB`).
    flags: Vec<PExpr>,
    /// Columns computed here, never asked of the server: (schema index of
    /// the virtual column, schema index of the column it is computed from,
    /// how).
    virt: Vec<(usize, usize, Virt)>,
    /// A star plan's fact table: dimensions' columns its aggregates read
    /// (TPC-H Q9's `ps_supplycost`), filled per probe row from their
    /// payloads — (schema index here, dimension's side, its column).
    lookups: Vec<(usize, usize, usize)>,
}

impl Side {
    fn col(&self, name: &str) -> Option<usize> {
        self.schema
            .iter()
            .position(|f| f.name == name)
            .or_else(|| self.schema.iter().position(|f| f.name.eq_ignore_ascii_case(name)))
    }

    /// The position of schema column `c` in this side's read list (added).
    fn want(&mut self, c: usize) -> usize {
        match self.read.iter().position(|&x| x == c) {
            Some(i) => i,
            None => {
                self.read.push(c);
                self.read.len() - 1
            }
        }
    }
}

/// How a virtual column is computed from its base column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Virt {
    /// `EXTRACT(YEAR FROM col)`.
    Year,
    /// `SUBSTRING(col FROM from FOR len)`: characters, 1-based.
    Substr(i64, Option<i64>),
}

/// A typed scalar expression over one side's read columns.
#[derive(Clone, Debug)]
enum VExpr {
    /// (position in the side's read list, kind).
    Col(usize, ArrowKind),
    Int(i64),
    Dec(i128, i8),
    Float(f64),
    Neg(Box<VExpr>),
    Bin(Box<VExpr>, char, Box<VExpr>),
    /// `CAST(x AS t)`: a text column (CSV, sheet) read as a number, or a
    /// number re-typed.
    Cast(Box<VExpr>, Ty),
    /// A decimal the source already scaled to an exact integer (an
    /// aggregate argument computed server-side, `pushdown_args`).
    Scaled(Box<VExpr>, i8),
    /// `CASE WHEN p THEN x .. [ELSE y] END` over one side: the first arm
    /// whose condition holds (a NULL condition does not), else the `ELSE`,
    /// else NULL; every branch brought to the CASE's one type. `tys` types
    /// the conditions' columns by read position.
    Case { arms: Vec<(PExpr, VExpr)>, els: Option<Box<VExpr>>, tys: Vec<Option<Ty>> },
    /// A CASE whose conditions are the build table's (its `flags`, by bit)
    /// and whose results are this table's: the arm is picked by the matched
    /// group's flag bits at the probe (TPC-H Q14's `p_type LIKE 'PROMO%'`
    /// over lineitem's revenue). Only an aggregate's whole argument.
    CaseB { arms: Vec<(u32, VExpr)>, els: Option<Box<VExpr>>, side: usize },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Ty {
    Int,
    Dec(i8),
    Float,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AggFn {
    Sum,
    Count,
    CountStar,
    Min,
    Max,
    Avg,
}

#[derive(Clone)]
struct Agg {
    f: AggFn,
    arg: Option<VExpr>,
    ty: Option<Ty>,
}

/// One output column, in SELECT order.
enum OutCol {
    /// A GROUP BY column: its index among the group columns.
    Group(usize),
    Agg(usize),
    /// Arithmetic over aggregates and numbers (TPC-H Q14's
    /// `100.00 * SUM(..) / SUM(..)`), a float as `/` is.
    Expr(OExpr),
}

/// An output expression over the aggregates.
#[derive(Clone, Debug)]
enum OExpr {
    Agg(usize),
    Lit(f64),
    Neg(Box<OExpr>),
    Bin(Box<OExpr>, char, Box<OExpr>),
}

/// An aggregate's value as a float (`None`: NULL).
fn acc_f64(a: Acc, ag: &Agg) -> Option<f64> {
    match a {
        Acc::None if matches!(ag.f, AggFn::Count | AggFn::CountStar) => Some(0.0),
        Acc::None => None,
        Acc::I(x) => Some(x as f64),
        Acc::D(x) => Some(x as f64 / 10f64.powi(i32::from(scale(ag.ty.unwrap_or(Ty::Int))))),
        Acc::F(x) => Some(x),
        Acc::Avg(s, n) => Some(s / n as f64),
        Acc::N(n) => Some(n as f64),
    }
}

/// An output expression on one group's accumulators (`None`: NULL, a
/// division by zero included).
fn oeval(o: &OExpr, accs: &[Acc], aggs: &[Agg]) -> Option<f64> {
    match o {
        OExpr::Agg(i) => acc_f64(accs[*i], &aggs[*i]),
        OExpr::Lit(x) => Some(*x),
        OExpr::Neg(x) => oeval(x, accs, aggs).map(|v| -v),
        OExpr::Bin(l, op, r) => {
            let (a, b) = (oeval(l, accs, aggs)?, oeval(r, accs, aggs)?);
            match op {
                '+' => Some(a + b),
                '-' => Some(a - b),
                '*' => Some(a * b),
                _ => (b != 0.0).then(|| a / b),
            }
        }
    }
}

/// A SELECT item as arithmetic over aggregates.
fn oexpr(sides: &mut [Side], e: &Expr, side: &mut Option<usize>, aggs: &mut Vec<Agg>) -> Result<OExpr> {
    Ok(match e {
        Expr::Nested(x) => oexpr(sides, x, side, aggs)?,
        Expr::Function(_) => {
            let a = aggregate(sides, e, side)?.ok_or_else(|| unsupported(format!("the SELECT expression {e}")))?;
            aggs.push(a);
            OExpr::Agg(aggs.len() - 1)
        }
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => OExpr::Lit(n.parse().map_err(|_| bad(format!("the number {n}")))?),
            _ => return Err(unsupported(format!("the SELECT expression {e}"))),
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => OExpr::Neg(Box::new(oexpr(sides, expr, side, aggs)?)),
        Expr::BinaryOp { left, op, right } => {
            let o = match op {
                BinaryOperator::Plus => '+',
                BinaryOperator::Minus => '-',
                BinaryOperator::Multiply => '*',
                BinaryOperator::Divide => '/',
                _ => return Err(unsupported(format!("the operator {op} over aggregates"))),
            };
            OExpr::Bin(Box::new(oexpr(sides, left, side, aggs)?), o, Box::new(oexpr(sides, right, side, aggs)?))
        }
        _ => return Err(unsupported(format!("the SELECT expression {e}"))),
    })
}

/// A scalar value of the numeric vocabulary.
#[derive(Clone, Copy, Debug)]
enum Num {
    Null,
    Int(i64),
    Dec(i128),
    Float(f64),
}

fn kind_ty(k: ArrowKind) -> Option<Ty> {
    match k {
        ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64 => Some(Ty::Int),
        ArrowKind::Decimal { s, .. } => Some(Ty::Dec(s)),
        ArrowKind::Float32 | ArrowKind::Float64 => Some(Ty::Float),
        _ => None,
    }
}

fn type_of(e: &VExpr) -> Result<Ty> {
    Ok(match e {
        VExpr::Col(_, k) => kind_ty(*k).ok_or_else(|| {
            unsupported(format!("arithmetic on a {k:?} column (CAST it, e.g. CAST(x AS DECIMAL(18,2)))"))
        })?,
        VExpr::Cast(_, t) => *t,
        VExpr::Scaled(_, s) => Ty::Dec(*s),
        VExpr::Int(_) => Ty::Int,
        VExpr::Dec(_, s) => Ty::Dec(*s),
        VExpr::Float(_) => Ty::Float,
        VExpr::Neg(x) => type_of(x)?,
        VExpr::CaseB { arms, els, .. } => {
            let mut t: Option<Ty> = None;
            for x in arms.iter().map(|a| &a.1).chain(els.as_deref()) {
                let u = type_of(x)?;
                t = Some(match (t, u) {
                    (None, u) => u,
                    (Some(Ty::Float), _) | (_, Ty::Float) => Ty::Float,
                    (Some(Ty::Int), Ty::Int) => Ty::Int,
                    (Some(a), b) => Ty::Dec(scale(a).max(scale(b))),
                });
            }
            t.unwrap_or(Ty::Int)
        }
        VExpr::Case { arms, els, .. } => {
            let mut t: Option<Ty> = None;
            for x in arms.iter().map(|a| &a.1).chain(els.as_deref()) {
                let u = type_of(x)?;
                t = Some(match (t, u) {
                    (None, u) => u,
                    (Some(Ty::Float), _) | (_, Ty::Float) => Ty::Float,
                    (Some(Ty::Int), Ty::Int) => Ty::Int,
                    (Some(a), b) => Ty::Dec(scale(a).max(scale(b))),
                });
            }
            t.unwrap_or(Ty::Int)
        }
        VExpr::Bin(l, op, r) => {
            let (a, b) = (type_of(l)?, type_of(r)?);
            match (a, b, op) {
                (_, _, '/') | (Ty::Float, _, _) | (_, Ty::Float, _) => Ty::Float,
                (Ty::Int, Ty::Int, _) => Ty::Int,
                (x, y, '*') => Ty::Dec(scale(x) + scale(y)),
                (x, y, _) => Ty::Dec(scale(x).max(scale(y))),
            }
        }
    })
}

fn scale(t: Ty) -> i8 {
    match t {
        Ty::Dec(s) => s,
        _ => 0,
    }
}

const P10: [i128; 39] = {
    let mut a = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        a[i] = a[i - 1] * 10;
        i += 1;
    }
    a
};

/// A value of type `from` as type `to` (a CASE branch brought to the CASE's
/// type: an integer or a narrower decimal widened exactly, or a float).
fn coerce(v: Num, from: Ty, to: Ty) -> Result<Num> {
    Ok(match (v, to) {
        (Num::Null, _) => Num::Null,
        (v, Ty::Float) => Num::Float(as_f64(v, from).unwrap_or(0.0)),
        (Num::Int(x), Ty::Int) => Num::Int(x),
        (Num::Int(x), Ty::Dec(s)) => Num::Dec(rescale(i128::from(x), 0, s)?),
        (Num::Dec(x), Ty::Dec(s)) => Num::Dec(rescale(x, scale(from), s)?),
        (v, t) => return Err(bad(format!("a CASE branch of {from:?} ({v:?}) as {t:?}"))),
    })
}

fn rescale(v: i128, from: i8, to: i8) -> Result<i128> {
    if to == from {
        return Ok(v);
    }
    let d = (to - from) as usize;
    v.checked_mul(*P10.get(d).ok_or_else(|| bad("decimal scale past 38"))?)
        .ok_or_else(|| bad("decimal overflow"))
}

/// 10^s as an f64, off a table for the scales a decimal has (a `powi` per
/// row and cell showed in TPC-H Q19's profile). Up to 10^22 every power is
/// exact, so the table and `powi` agree bit for bit; past it, `powi`.
#[inline]
fn pow10f(s: i8) -> f64 {
    const T: [f64; 23] = {
        let mut t = [1f64; 23];
        let mut i = 1;
        while i < 23 {
            t[i] = t[i - 1] * 10.0;
            i += 1;
        }
        t
    };
    usize::try_from(s).ok().and_then(|i| T.get(i).copied()).unwrap_or_else(|| 10f64.powi(i32::from(s)))
}

fn as_f64(n: Num, t: Ty) -> Option<f64> {
    match n {
        Num::Null => None,
        Num::Int(i) => Some(i as f64),
        Num::Dec(d) => Some(d as f64 / pow10f(scale(t))),
        Num::Float(f) => Some(f),
    }
}

/// An exact (integer or decimal) expression over a whole batch, a column at
/// a time: each row's value at the expression's scale, and whether it is
/// non-NULL. The types are worked out once, not per row and node (the row
/// way walked the tree for them at every node of every row). The values and
/// errors are the row way's; `None` when the expression is not one this
/// covers (a float, a CAST, a column of another kind), for the row way.
pub(crate) fn eval_exact(e: &VExpr, b: &ArrowBatch) -> Result<Option<(Vec<i128>, Vec<bool>)>> {
    let n = b.rows;
    let bits = |v: &Option<Vec<u8>>| -> Vec<bool> {
        match v {
            None => vec![true; n],
            Some(bm) => (0..n).map(|r| bm[r / 8] & (1 << (r % 8)) != 0).collect(),
        }
    };
    Ok(Some(match e {
        VExpr::Col(i, k) => {
            let int = matches!(kind_ty(*k), Some(Ty::Int));
            match (&b.cols[*i], int) {
                (FinishedCol::I16 { validity, data }, true) => (data.iter().map(|&x| i128::from(x)).collect(), bits(validity)),
                (FinishedCol::I32 { validity, data }, true) => (data.iter().map(|&x| i128::from(x)).collect(), bits(validity)),
                (FinishedCol::I64 { validity, data }, true) => (data.iter().map(|&x| i128::from(x)).collect(), bits(validity)),
                (FinishedCol::Dec128 { validity, data }, false) if matches!(kind_ty(*k), Some(Ty::Dec(_))) => {
                    (data.clone(), bits(validity))
                }
                _ => return Ok(None),
            }
        }
        VExpr::Int(v) => (vec![i128::from(*v); n], vec![true; n]),
        VExpr::Dec(v, _) => (vec![*v; n], vec![true; n]),
        VExpr::Scaled(x, _) => {
            if !matches!(type_of(x)?, Ty::Int) {
                return Ok(None);
            }
            match eval_exact(x, b)? {
                Some(v) => v,
                None => return Ok(None),
            }
        }
        VExpr::Neg(x) => {
            let int = matches!(type_of(x)?, Ty::Int);
            let Some((mut v, ok)) = eval_exact(x, b)? else { return Ok(None) };
            for (x, &o) in v.iter_mut().zip(&ok) {
                if o {
                    if int && *x == i128::from(i64::MIN) {
                        return Err(bad("integer overflow"));
                    }
                    *x = -*x;
                }
            }
            (v, ok)
        }
        VExpr::Bin(l, op, rt) => {
            let (tl, tr, tout) = (type_of(l)?, type_of(rt)?, type_of(e)?);
            if !matches!(op, '+' | '-' | '*') || matches!(tout, Ty::Float) {
                return Ok(None);
            }
            // A literal operand is rescaled once, not once per row.
            let lit = |e: &VExpr| match e {
                VExpr::Int(v) => Some(i128::from(*v)),
                VExpr::Dec(v, _) => Some(*v),
                _ => None,
            };
            if let (Ty::Dec(sc), Some(c)) = (tout, lit(l).or_else(|| lit(rt))) {
                let left = lit(l).is_some();
                let (other, so, sl) = if left { (rt, scale(tr), scale(tl)) } else { (l, scale(tl), scale(tr)) };
                let Some((mut v, ok)) = eval_exact(other, b)? else { return Ok(None) };
                let over = || bad("decimal overflow");
                match op {
                    '*' => {
                        for r in 0..n {
                            if ok[r] {
                                v[r] = if left { c.checked_mul(v[r]) } else { v[r].checked_mul(c) }.ok_or_else(over)?;
                            }
                        }
                    }
                    _ => {
                        let c = rescale(c, sl, sc)?;
                        let up = P10.get((sc - so) as usize).copied().ok_or_else(|| bad("decimal scale past 38"))?;
                        for r in 0..n {
                            if ok[r] {
                                let x = if so == sc { v[r] } else { v[r].checked_mul(up).ok_or_else(over)? };
                                v[r] = match (op, left) {
                                    ('+', _) => c.checked_add(x),
                                    ('-', true) => c.checked_sub(x),
                                    _ => x.checked_sub(c),
                                }
                                .ok_or_else(over)?;
                            }
                        }
                    }
                }
                return Ok(Some((v, ok)));
            }
            let Some((x, okx)) = eval_exact(l, b)? else { return Ok(None) };
            let Some((y, oky)) = eval_exact(rt, b)? else { return Ok(None) };
            let mut out = vec![0i128; n];
            let ok: Vec<bool> = okx.iter().zip(&oky).map(|(a, c)| *a && *c).collect();
            match tout {
                Ty::Int => {
                    for r in 0..n {
                        if !ok[r] {
                            continue;
                        }
                        let (a, c) = (x[r] as i64, y[r] as i64);
                        out[r] = i128::from(
                            match op {
                                '+' => a.checked_add(c),
                                '-' => a.checked_sub(c),
                                _ => a.checked_mul(c),
                            }
                            .ok_or_else(|| bad("integer overflow"))?,
                        );
                    }
                }
                Ty::Dec(sc) => {
                    let (sx, sy) = (scale(tl), scale(tr));
                    let over = || bad("decimal overflow");
                    for r in 0..n {
                        if !ok[r] {
                            continue;
                        }
                        out[r] = match op {
                            '*' => x[r].checked_mul(y[r]).ok_or_else(over)?,
                            '+' => rescale(x[r], sx, sc)?.checked_add(rescale(y[r], sy, sc)?).ok_or_else(over)?,
                            _ => rescale(x[r], sx, sc)?.checked_sub(rescale(y[r], sy, sc)?).ok_or_else(over)?,
                        };
                    }
                }
                Ty::Float => unreachable!("returned above"),
            }
            (out, ok)
        }
        VExpr::Float(_) | VExpr::Cast(..) | VExpr::Case { .. } | VExpr::CaseB { .. } => return Ok(None),
    }))
}

/// Evaluate `e` (of type `t`) on row `r` of `b`.
fn eval(e: &VExpr, b: &ArrowBatch, r: usize) -> Result<Num> {
    Ok(match e {
        VExpr::Col(i, _) => cell_num(&b.cols[*i], r),
        VExpr::Int(v) => Num::Int(*v),
        VExpr::Dec(v, _) => Num::Dec(*v),
        VExpr::Float(v) => Num::Float(*v),
        VExpr::CaseB { .. } => return Err(bad("a CASE over the build table evaluated without its group")),
        VExpr::Case { arms, els, tys } => {
            let to = type_of(e)?;
            let pick = arms.iter().find(|(p, _)| peval(p, b, r, tys) == Some(true)).map(|a| &a.1).or(els.as_deref());
            match pick {
                None => Num::Null,
                Some(x) => coerce(eval(x, b, r)?, type_of(x)?, to)?,
            }
        }
        VExpr::Cast(x, t) => {
            if let VExpr::Col(i, ArrowKind::Utf8) = x.as_ref() {
                let Some(raw) = cell_text(&b.cols[*i], r) else { return Ok(Num::Null) };
                let txt = std::str::from_utf8(raw).unwrap_or("").trim();
                if txt.is_empty() {
                    return Ok(Num::Null);
                }
                let fail = || bad(format!("CAST of {txt:?} to {t:?}"));
                return Ok(match t {
                    Ty::Int => Num::Int(txt.parse().map_err(|_| fail())?),
                    Ty::Float => Num::Float(txt.parse().map_err(|_| fail())?),
                    Ty::Dec(s) => match number(txt).map_err(|_| fail())? {
                        VExpr::Int(v) => Num::Dec(rescale(v as i128, 0, *s)?),
                        VExpr::Dec(v, vs) if vs <= *s => Num::Dec(rescale(v, vs, *s)?),
                        _ => return Err(bad(format!("CAST of {txt:?} to DECIMAL with scale {s} would round"))),
                    },
                });
            }
            let ti = type_of(x)?;
            match (eval(x, b, r)?, t) {
                (Num::Null, _) => Num::Null,
                (v, Ty::Float) => Num::Float(as_f64(v, ti).unwrap_or(0.0)),
                (Num::Int(v), Ty::Int) => Num::Int(v),
                (Num::Int(v), Ty::Dec(s)) => Num::Dec(rescale(v as i128, 0, *s)?),
                (Num::Dec(v), Ty::Dec(s)) if scale(ti) <= *s => Num::Dec(rescale(v, scale(ti), *s)?),
                _ => return Err(unsupported(format!("CAST from {ti:?} to {t:?}"))),
            }
        }
        VExpr::Scaled(x, _) => match eval(x, b, r)? {
            Num::Int(v) => Num::Dec(i128::from(v)),
            Num::Null => Num::Null,
            _ => return Err(bad("a scaled decimal that is not an integer")),
        },
        VExpr::Neg(x) => match eval(x, b, r)? {
            Num::Null => Num::Null,
            Num::Int(v) => Num::Int(v.checked_neg().ok_or_else(|| bad("integer overflow"))?),
            Num::Dec(v) => Num::Dec(-v),
            Num::Float(v) => Num::Float(-v),
        },
        VExpr::Bin(l, op, rt) => {
            let (tl, tr, tout) = (type_of(l)?, type_of(rt)?, type_of(e)?);
            let (a, c) = (eval(l, b, r)?, eval(rt, b, r)?);
            if matches!(a, Num::Null) || matches!(c, Num::Null) {
                return Ok(Num::Null);
            }
            match tout {
                Ty::Float => {
                    let (x, y) = (as_f64(a, tl).unwrap_or(0.0), as_f64(c, tr).unwrap_or(0.0));
                    Num::Float(match op {
                        '+' => x + y,
                        '-' => x - y,
                        '*' => x * y,
                        _ => {
                            if y == 0.0 {
                                return Ok(Num::Null);
                            }
                            x / y
                        }
                    })
                }
                Ty::Int => {
                    let (Num::Int(x), Num::Int(y)) = (a, c) else {
                        return Err(bad("integer expression invariant"));
                    };
                    Num::Int(
                        match op {
                            '+' => x.checked_add(y),
                            '-' => x.checked_sub(y),
                            _ => x.checked_mul(y),
                        }
                        .ok_or_else(|| bad("integer overflow"))?,
                    )
                }
                Ty::Dec(s) => {
                    let x = match a {
                        Num::Int(v) => v as i128,
                        Num::Dec(v) => v,
                        _ => return Err(bad("decimal expression invariant")),
                    };
                    let y = match c {
                        Num::Int(v) => v as i128,
                        Num::Dec(v) => v,
                        _ => return Err(bad("decimal expression invariant")),
                    };
                    let (sx, sy) = (scale(tl), scale(tr));
                    Num::Dec(match op {
                        '*' => x.checked_mul(y).ok_or_else(|| bad("decimal overflow"))?,
                        '+' => rescale(x, sx, s)?
                            .checked_add(rescale(y, sy, s)?)
                            .ok_or_else(|| bad("decimal overflow"))?,
                        _ => rescale(x, sx, s)?
                            .checked_sub(rescale(y, sy, s)?)
                            .ok_or_else(|| bad("decimal overflow"))?,
                    })
                }
            }
        }
    })
}

fn valid(v: &Option<Vec<u8>>, r: usize) -> bool {
    v.as_ref().is_none_or(|bm| bm[r / 8] & (1 << (r % 8)) != 0)
}

fn cell_num(c: &FinishedCol, r: usize) -> Num {
    match c {
        FinishedCol::I16 { validity, data } if valid(validity, r) => Num::Int(i64::from(data[r])),
        FinishedCol::I32 { validity, data } if valid(validity, r) => Num::Int(i64::from(data[r])),
        FinishedCol::I64 { validity, data } if valid(validity, r) => Num::Int(data[r]),
        FinishedCol::F32 { validity, data } if valid(validity, r) => Num::Float(f64::from(data[r])),
        FinishedCol::F64 { validity, data } if valid(validity, r) => Num::Float(data[r]),
        FinishedCol::Dec128 { validity, data } if valid(validity, r) => Num::Dec(data[r]),
        _ => Num::Null,
    }
}

/// The join key of row `r`, if it is not NULL. A text key (a CSV or a sheet)
/// is read as an integer; one that is not is refused, naming it.
fn key_at(c: &FinishedCol, r: usize) -> Result<Option<i64>> {
    if let FinishedCol::Utf8 { validity, offsets, data } = c {
        if !valid(validity, r) {
            return Ok(None);
        }
        let t = &data[offsets[r] as usize..offsets[r + 1] as usize];
        let t = std::str::from_utf8(t).unwrap_or("").trim();
        if t.is_empty() {
            return Ok(None);
        }
        return t
            .parse::<i64>()
            .map(Some)
            .map_err(|_| bad(format!("the join key value {t:?} is not an integer")));
    }
    Ok(match cell_num(c, r) {
        Num::Int(v) => Some(v),
        _ => None,
    })
}

/// A text cell's bytes, if it is a non-NULL Utf8 cell.
fn cell_text(c: &FinishedCol, r: usize) -> Option<&[u8]> {
    match c {
        FinishedCol::Utf8 { validity, offsets, data } if valid(validity, r) => {
            Some(&data[offsets[r] as usize..offsets[r + 1] as usize])
        }
        _ => None,
    }
}

/// A WHERE conjunct evaluated per row, for a side whose source cannot filter.
#[derive(Clone, Debug)]
enum PExpr {
    And(Box<PExpr>, Box<PExpr>),
    Or(Box<PExpr>, Box<PExpr>),
    Not(Box<PExpr>),
    IsNull(usize, bool),
    /// (read position, operator, literal).
    Cmp(usize, BinaryOperator, Lit),
    In(usize, Vec<Lit>, bool),
    /// (read position, pattern, negated, the column's collation): `%` any
    /// run, `_` one character; a CHAR(n) value's trailing spaces are not
    /// matched, a `_ci` column's case is folded (the pattern's at plan time).
    Like(usize, String, bool, Coll),
    /// `ABS(col % n) = i` over an integer column: the part filter a query
    /// answered in parts (`partitioned`) adds, here for a stage table.
    Part(usize, i64, i64),
}

/// SQL LIKE of `t` against `pat` (`%` any run, `_` one character), by
/// characters, without an escape.
fn like(t: &str, pat: &str) -> bool {
    // ASCII both: the same walk over bytes, nothing allocated (a per-row
    // LIKE collected two Vec<char> a row: 13% of TPC-H Q14's CPU).
    if t.is_ascii() && pat.is_ascii() {
        return like_bytes(t.as_bytes(), pat.as_bytes());
    }
    let (t, p): (Vec<char>, Vec<char>) = (t.chars().collect(), pat.chars().collect());
    let (mut i, mut j) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while i < t.len() {
        if j < p.len() && (p[j] == '_' || p[j] == t[i]) {
            i += 1;
            j += 1;
        } else if j < p.len() && p[j] == '%' {
            star = Some(j);
            mark = i;
            j += 1;
        } else if let Some(s) = star {
            j = s + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    while j < p.len() && p[j] == '%' {
        j += 1;
    }
    j == p.len()
}

/// [`like`] over ASCII bytes (a byte is a character).
fn like_bytes(t: &[u8], p: &[u8]) -> bool {
    let (mut i, mut j) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while i < t.len() {
        if j < p.len() && (p[j] == b'_' || p[j] == t[i]) {
            i += 1;
            j += 1;
        } else if j < p.len() && p[j] == b'%' {
            star = Some(j);
            mark = i;
            j += 1;
        } else if let Some(s) = star {
            j = s + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    while j < p.len() && p[j] == b'%' {
        j += 1;
    }
    j == p.len()
}

#[derive(Clone, Debug)]
enum Lit {
    Num(f64),
    /// A number written with a fraction (`1772627.2087`): mantissa and
    /// scale, so a decimal column compares with it exactly (TPC-H Q15's
    /// `total_revenue = MAX(total_revenue)`).
    Dec(i128, i8),
    Str(String),
    /// A string compared under the column's collation
    /// ([`ArrowField::padded`], [`ArrowField::ci`]).
    Coll(String, Coll),
    Bool(bool),
}

/// How a text column compares: trailing spaces ignored (`pad`), case
/// folded (`ci`), accents folded (`ai`: MySQL's `_ai_ci` and `general_ci`,
/// `'é' = 'e'`), as the column's server compares.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct Coll {
    pad: bool,
    ci: bool,
    ai: bool,
}

impl Coll {
    fn of(f: &ArrowField) -> Coll {
        Coll { pad: f.padded, ci: f.ci, ai: f.ai }
    }

    fn plain(self) -> bool {
        !self.pad && !self.ci
    }

    /// The bytes two values are equal by under this collation: padding off,
    /// case folded, accents and the letters UCA weighs as base letters
    /// (`ß` = `ss`, `æ` = `ae`, `ø` = `o`, …) folded. ASCII folds in place.
    fn fold<'a>(self, t: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
        use std::borrow::Cow;
        let t = if self.pad { t.trim_ascii_end() } else { t };
        if !self.ci {
            return Cow::Borrowed(t);
        }
        if t.is_ascii() {
            return if t.iter().any(u8::is_ascii_uppercase) { Cow::Owned(t.to_ascii_lowercase()) } else { Cow::Borrowed(t) };
        }
        let s = String::from_utf8_lossy(t);
        if !self.ai {
            return Cow::Owned(s.to_lowercase().into_bytes());
        }
        use unicode_normalization::char::is_combining_mark;
        use unicode_normalization::UnicodeNormalization as _;
        let mut out = String::with_capacity(s.len());
        for c in s.nfkd().filter(|c| !is_combining_mark(*c)) {
            match c {
                'ß' | 'ẞ' => out.push_str("ss"),
                'æ' | 'Æ' => out.push_str("ae"),
                'œ' | 'Œ' => out.push_str("oe"),
                'ø' | 'Ø' => out.push('o'),
                'đ' | 'Đ' => out.push('d'),
                'ł' | 'Ł' => out.push('l'),
                'ħ' | 'Ħ' => out.push('h'),
                'ı' => out.push('i'),
                c => out.extend(c.to_lowercase()),
            }
        }
        Cow::Owned(out.into_bytes())
    }

    /// `t` against `y` under this collation.
    fn cmp(self, t: &[u8], y: &[u8]) -> std::cmp::Ordering {
        if self.plain() {
            return t.cmp(y);
        }
        self.fold(t).cmp(&self.fold(y))
    }

    /// The value a LIKE matches (the pattern was folded the same way at
    /// plan time).
    fn like_text<'a>(self, t: &'a [u8]) -> Option<std::borrow::Cow<'a, str>> {
        use std::borrow::Cow;
        match self.fold(t) {
            Cow::Borrowed(b) => std::str::from_utf8(b).ok().map(Cow::Borrowed),
            Cow::Owned(v) => String::from_utf8(v).ok().map(Cow::Owned),
        }
    }
}

/// The cell as a comparable value: text, number or bool.
enum Cv<'a> {
    Null,
    Num(f64),
    Text(&'a [u8]),
    Bool(bool),
}

fn cv<'a>(c: &'a FinishedCol, r: usize, ty: Option<Ty>) -> Cv<'a> {
    if let Some(t) = cell_text(c, r) {
        return Cv::Text(t);
    }
    match c {
        FinishedCol::Bool { validity, data } if valid(validity, r) => Cv::Bool(data[r / 8] & (1 << (r % 8)) != 0),
        _ => match as_f64(cell_num(c, r), ty.unwrap_or(Ty::Float)) {
            Some(f) => Cv::Num(f),
            None => Cv::Null,
        },
    }
}

/// Three-valued: `None` = unknown (a NULL took part); a row passes on Some(true).
fn peval(e: &PExpr, b: &ArrowBatch, r: usize, tys: &[Option<Ty>]) -> Option<bool> {
    use std::cmp::Ordering as O;
    let cmp = |i: usize, l: &Lit| -> Option<O> {
        if let (Lit::Dec(m, ls), FinishedCol::Dec128 { validity, data }, Some(Ty::Dec(cs))) = (l, &b.cols[i], tys[i]) {
            if !valid(validity, r) {
                return None;
            }
            return dec_cmp(data[r], cs, *m, *ls);
        }
        match (cv(&b.cols[i], r, tys[i]), l) {
            (Cv::Null, _) => None,
            (Cv::Num(x), Lit::Num(y)) => x.partial_cmp(y),
            (Cv::Num(x), Lit::Dec(m, ls)) => x.partial_cmp(&(*m as f64 / pow10f(*ls))),
            (Cv::Text(t), Lit::Dec(m, ls)) => std::str::from_utf8(t).ok()?.trim().parse::<f64>().ok()?.partial_cmp(&(*m as f64 / pow10f(*ls))),
            // Trailing spaces count for VARCHAR and TEXT, not for CHAR(n)
            // (PG's bpchar): TPC-H's l_shipmode arrives padded.
            (Cv::Text(t), Lit::Str(y)) => Some(t.cmp(y.as_bytes())),
            (Cv::Text(t), Lit::Coll(y, c)) => Some(c.cmp(t, y.as_bytes())),
            (Cv::Text(t), Lit::Num(y)) => std::str::from_utf8(t).ok()?.trim().parse::<f64>().ok()?.partial_cmp(y),
            (Cv::Bool(x), Lit::Bool(y)) => Some(x.cmp(y)),
            (Cv::Text(t), Lit::Bool(y)) => Some(matches!(t, b"true" | b"t" | b"1").cmp(y)),
            _ => None,
        }
    };
    match e {
        PExpr::And(a, c) => match (peval(a, b, r, tys), peval(c, b, r, tys)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        PExpr::Or(a, c) => match (peval(a, b, r, tys), peval(c, b, r, tys)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        PExpr::Not(a) => peval(a, b, r, tys).map(|x| !x),
        PExpr::IsNull(i, neg) => {
            let null = matches!(cv(&b.cols[*i], r, tys[*i]), Cv::Null);
            Some(null != *neg)
        }
        PExpr::Cmp(i, op, l) => {
            let o = cmp(*i, l)?;
            Some(match op {
                BinaryOperator::Eq => o == O::Equal,
                BinaryOperator::NotEq => o != O::Equal,
                BinaryOperator::Lt => o == O::Less,
                BinaryOperator::LtEq => o != O::Greater,
                BinaryOperator::Gt => o == O::Greater,
                _ => o != O::Less,
            })
        }
        PExpr::Part(i, n, k) => key_at(&b.cols[*i], r).ok().flatten().map(|v| (v % *n).abs() == *k),
        PExpr::Like(i, pat, neg, c) => match cv(&b.cols[*i], r, tys[*i]) {
            Cv::Text(t) => Some(like(&c.like_text(t)?, pat) != *neg),
            _ => None,
        },
        PExpr::In(i, ls, neg) => {
            let mut unknown = false;
            for l in ls {
                match cmp(*i, l) {
                    Some(O::Equal) => return Some(!*neg),
                    None => unknown = true,
                    _ => {}
                }
            }
            if unknown {
                None
            } else {
                Some(*neg)
            }
        }
    }
}

/// `a / 10^sa` against `b / 10^sb`, exactly (`None` past i128).
fn dec_cmp(a: i128, sa: i8, b: i128, sb: i8) -> Option<std::cmp::Ordering> {
    let up = |v: i128, by: i8| -> Option<i128> { (0..by).try_fold(v, |x, _| x.checked_mul(10)) };
    let s = sa.max(sb);
    Some(up(a, s - sa)?.cmp(&up(b, s - sb)?))
}

impl Lit {
    /// The literal as a float, when it is a number.
    fn num(&self) -> Option<f64> {
        match self {
            Lit::Num(f) => Some(*f),
            Lit::Dec(m, s) => Some(*m as f64 / pow10f(*s)),
            _ => None,
        }
    }
}

/// A numeric literal's digits as (mantissa, scale), when it has a fraction
/// and fits.
fn dec_lit(n: &str) -> Option<Lit> {
    let (i, f) = n.split_once('.')?;
    if f.is_empty() || f.len() > 30 || !i.bytes().chain(f.bytes()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let m: i128 = format!("{i}{f}").parse().ok()?;
    Some(Lit::Dec(m, i8::try_from(f.len()).ok()?))
}

fn lit(e: &Expr) -> Option<Lit> {
    match e {
        // Arithmetic of numbers, folded (TPC-H Q19's `1 + 10`).
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply | BinaryOperator::Divide) => {
            let (a, b) = (lit(left)?.num()?, lit(right)?.num()?);
            Some(Lit::Num(match op {
                BinaryOperator::Plus => a + b,
                BinaryOperator::Minus => a - b,
                BinaryOperator::Multiply => a * b,
                _ => a / b,
            }))
        }
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => dec_lit(n).or_else(|| n.parse::<f64>().ok().map(Lit::Num)),
            Value::SingleQuotedString(t) => Some(Lit::Str(t.clone())),
            Value::Boolean(x) => Some(Lit::Bool(*x)),
            _ => None,
        },
        Expr::TypedString { data_type: sqlparser::ast::DataType::Date, value } => match value {
            Value::SingleQuotedString(t) => days_from_text(t).map(|d| Lit::Num(d as f64)),
            _ => None,
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match lit(expr)? {
            Lit::Num(f) => Some(Lit::Num(-f)),
            Lit::Dec(m, s) => Some(Lit::Dec(-m, s)),
            _ => None,
        },
        Expr::Nested(x) => lit(x),
        _ => None,
    }
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` text (Howard Hinnant's
/// days_from_civil), `None` when it is not one.
fn days_from_text(t: &str) -> Option<i64> {
    let t = t.trim();
    let (y, rest) = t.split_once('-')?;
    let (m, d) = rest.split_once('-')?;
    let d = d.get(..2).filter(|_| d.len() == 2 || !d.as_bytes()[2].is_ascii_digit())?;
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

/// `ABS(x % n) = k` (integers `n > 0`, `k`): x, n, k.
fn part_of<'a>(left: &'a Expr, right: &Expr) -> Option<(&'a Expr, i64, i64)> {
    let k = right.to_string().parse::<i64>().ok()?;
    let Expr::Function(f) = left else { return None };
    if !f.name.to_string().eq_ignore_ascii_case("abs") {
        return None;
    }
    let FunctionArguments::List(l) = &f.args else { return None };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(a))] = l.args.as_slice() else { return None };
    let a = match a {
        Expr::Nested(x) => x.as_ref(),
        x => x,
    };
    let Expr::BinaryOp { left: x, op: BinaryOperator::Modulo, right: n } = a else { return None };
    let n = n.to_string().parse::<i64>().ok().filter(|n| *n > 0)?;
    Some((x.as_ref(), n, k))
}

/// Compile a conjunct of side `s` for per-row evaluation.
fn pexpr(sides: &mut [Side], s: usize, e: &Expr) -> Result<PExpr> {
    let col = |sides: &mut [Side], x: &Expr| -> Result<Option<usize>> {
        Ok(match column_of(sides, x)? {
            Some((ss, c)) if ss == s => Some(sides[s].want(c)),
            _ => None,
        })
    };
    let side_name = sides[s].name.clone();
    // A string literal compares under its column's collation (`Lit::Coll`).
    let padl = |sides: &[Side], c: usize, l: Lit| match l {
        Lit::Str(t) if !Coll::of(&sides[s].schema[sides[s].read[c]]).plain() => Lit::Coll(t, Coll::of(&sides[s].schema[sides[s].read[c]])),
        l => l,
    };
    let no = || unsupported(format!("the WHERE condition {e} on {side_name} (comparisons of a column with a literal, IN, IS NULL, AND/OR/NOT)"));
    Ok(match e {
        Expr::Nested(x) => pexpr(sides, s, x)?,
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            PExpr::And(Box::new(pexpr(sides, s, left)?), Box::new(pexpr(sides, s, right)?))
        }
        Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
            PExpr::Or(Box::new(pexpr(sides, s, left)?), Box::new(pexpr(sides, s, right)?))
        }
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => PExpr::Not(Box::new(pexpr(sides, s, expr)?)),
        Expr::IsNull(x) => PExpr::IsNull(col(sides, x)?.ok_or_else(no)?, false),
        Expr::IsNotNull(x) => PExpr::IsNull(col(sides, x)?.ok_or_else(no)?, true),
        Expr::Like { negated, any: false, expr, pattern, escape_char: None } => match pattern.as_ref() {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(pt) => {
                    let c = col(sides, expr)?.ok_or_else(no)?;
                    let co = Coll::of(&sides[s].schema[sides[s].read[c]]);
                    let pt = if co.ci { String::from_utf8_lossy(&Coll { pad: false, ..co }.fold(pt.as_bytes())).into_owned() } else { pt.clone() };
                    PExpr::Like(c, pt, *negated, co)
                }
                _ => return Err(no()),
            },
            _ => return Err(no()),
        },
        Expr::Between { expr, negated, low, high } => {
            let c = col(sides, expr)?.ok_or_else(no)?;
            let (lo, hi) = (padl(sides, c, lit(low).ok_or_else(no)?), padl(sides, c, lit(high).ok_or_else(no)?));
            let both = PExpr::And(Box::new(PExpr::Cmp(c, BinaryOperator::GtEq, lo)), Box::new(PExpr::Cmp(c, BinaryOperator::LtEq, hi)));
            if *negated {
                PExpr::Not(Box::new(both))
            } else {
                both
            }
        }
        Expr::InList { expr, list, negated } => {
            let c = col(sides, expr)?.ok_or_else(no)?;
            let ls = list.iter().map(lit).collect::<Option<Vec<_>>>().ok_or_else(no)?;
            PExpr::In(c, ls.into_iter().map(|l| padl(sides, c, l)).collect(), *negated)
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } if part_of(left, right).is_some() => {
            let (x, n, k) = part_of(left, right).expect("matched");
            PExpr::Part(col(sides, x)?.ok_or_else(no)?, n, k)
        }
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq
            ) =>
        {
            if let (Some(c), Some(l)) = (col(sides, left)?, lit(right)) {
                PExpr::Cmp(c, op.clone(), padl(sides, c, l))
            } else if let (Some(l), Some(c)) = (lit(left), col(sides, right)?) {
                let flipped = match op {
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    o => o.clone(),
                };
                PExpr::Cmp(c, flipped, padl(sides, c, l))
            } else {
                return Err(no());
            }
        }
        _ => return Err(no()),
    })
}

impl PExpr {
    /// The read positions this condition names.
    fn cols(&self, out: &mut Vec<usize>) {
        match self {
            PExpr::And(a, c) | PExpr::Or(a, c) => {
                a.cols(out);
                c.cols(out);
            }
            PExpr::Not(a) => a.cols(out),
            PExpr::IsNull(i, _) | PExpr::Cmp(i, ..) | PExpr::In(i, ..) | PExpr::Like(i, ..) | PExpr::Part(i, ..) => {
                if !out.contains(i) {
                    out.push(*i);
                }
            }
        }
    }

    /// The condition over other read positions (`f` maps each).
    fn remap(&self, f: &dyn Fn(usize) -> usize) -> PExpr {
        match self {
            PExpr::And(a, c) => PExpr::And(Box::new(a.remap(f)), Box::new(c.remap(f))),
            PExpr::Or(a, c) => PExpr::Or(Box::new(a.remap(f)), Box::new(c.remap(f))),
            PExpr::Not(a) => PExpr::Not(Box::new(a.remap(f))),
            PExpr::IsNull(i, n) => PExpr::IsNull(f(*i), *n),
            PExpr::Cmp(i, o, l) => PExpr::Cmp(f(*i), o.clone(), l.clone()),
            PExpr::In(i, ls, n) => PExpr::In(f(*i), ls.clone(), *n),
            PExpr::Like(i, pt, n, c) => PExpr::Like(f(*i), pt.clone(), *n, *c),
            PExpr::Part(i, n, k) => PExpr::Part(f(*i), *n, *k),
        }
    }
}

impl VExpr {
    /// The read positions this expression names (its CASE conditions too).
    pub(crate) fn cols(&self, out: &mut Vec<usize>) {
        match self {
            VExpr::Col(i, _) => {
                if !out.contains(i) {
                    out.push(*i);
                }
            }
            VExpr::Neg(x) | VExpr::Cast(x, _) | VExpr::Scaled(x, _) => x.cols(out),
            VExpr::Bin(l, _, r) => {
                l.cols(out);
                r.cols(out);
            }
            VExpr::Case { arms, els, .. } => {
                for (p, x) in arms {
                    p.cols(out);
                    x.cols(out);
                }
                if let Some(x) = els {
                    x.cols(out);
                }
            }
            VExpr::CaseB { arms, els, .. } => {
                for (_, x) in arms {
                    x.cols(out);
                }
                if let Some(x) = els {
                    x.cols(out);
                }
            }
            VExpr::Int(_) | VExpr::Dec(..) | VExpr::Float(_) => {}
        }
    }

    /// The expression over other read positions: position `i` becomes
    /// `at`'s index of it.
    pub(crate) fn remap(&self, at: &[usize]) -> VExpr {
        let f = |i: usize| at.iter().position(|x| *x == i).expect("listed");
        match self {
            VExpr::Col(i, k) => VExpr::Col(f(*i), *k),
            VExpr::Neg(x) => VExpr::Neg(Box::new(x.remap(at))),
            VExpr::Cast(x, t) => VExpr::Cast(Box::new(x.remap(at)), *t),
            VExpr::Scaled(x, sc) => VExpr::Scaled(Box::new(x.remap(at)), *sc),
            VExpr::Bin(l, o, r) => VExpr::Bin(Box::new(l.remap(at)), *o, Box::new(r.remap(at))),
            VExpr::Case { arms, els, tys } => {
                let mut nt = vec![None; at.len()];
                for (n, &o) in at.iter().enumerate() {
                    nt[n] = tys.get(o).copied().flatten();
                }
                VExpr::Case {
                    arms: arms.iter().map(|(p, x)| (p.remap(&f), x.remap(at))).collect(),
                    els: els.as_ref().map(|x| Box::new(x.remap(at))),
                    tys: nt,
                }
            }
            VExpr::CaseB { arms, els, side } => VExpr::CaseB {
                arms: arms.iter().map(|(b, x)| (*b, x.remap(at))).collect(),
                els: els.as_ref().map(|x| Box::new(x.remap(at))),
                side: *side,
            },
            other => other.clone(),
        }
    }
}

/// Each read column's numeric type, for the local predicates.
fn side_tys(side: &Side) -> Vec<Option<Ty>> {
    side.read.iter().map(|&c| kind_ty(side.schema[c].kind)).collect()
}

/// Whether row `r` passes every local conjunct of `side`.
fn local_ok(side: &Side, tys: &[Option<Ty>], b: &ArrowBatch, r: usize) -> bool {
    side.local.iter().all(|p| peval(p, b, r, tys) == Some(true)) && side.semis.iter().all(|s| s.pass(b, r))
}

/// Append group column `c`'s row `r` to the group-key encoding `kb`.
fn encode_group(c: &FinishedCol, r: usize, kb: &mut Vec<u8>) {
    let put = |kb: &mut Vec<u8>, tag: u8, bytes: &[u8]| {
        kb.push(tag);
        kb.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        kb.extend_from_slice(bytes);
    };
    match c {
        FinishedCol::I16 { validity, data } if valid(validity, r) => put(kb, 1, &i64::from(data[r]).to_le_bytes()),
        FinishedCol::I32 { validity, data } if valid(validity, r) => put(kb, 1, &i64::from(data[r]).to_le_bytes()),
        FinishedCol::I64 { validity, data } if valid(validity, r) => put(kb, 1, &data[r].to_le_bytes()),
        FinishedCol::F32 { validity, data } if valid(validity, r) => put(kb, 2, &f64::from(data[r]).to_le_bytes()),
        FinishedCol::F64 { validity, data } if valid(validity, r) => put(kb, 2, &data[r].to_le_bytes()),
        FinishedCol::Dec128 { validity, data } if valid(validity, r) => put(kb, 3, &data[r].to_le_bytes()),
        FinishedCol::Bool { validity, data } if valid(validity, r) => {
            put(kb, 4, &[u8::from(data[r / 8] & (1 << (r % 8)) != 0)])
        }
        FinishedCol::Utf8 { validity, offsets, data } | FinishedCol::Bin { validity, offsets, data }
            if valid(validity, r) =>
        {
            put(kb, 5, &data[offsets[r] as usize..offsets[r + 1] as usize])
        }
        _ => kb.push(0),
    }
}

/// Group keys end to end in one buffer: a key costs its bytes and a 4-byte
/// offset, not a box of its own (a pointer, a length and the allocator's
/// rounding and header: ~30 bytes more per group at 2M+ groups).
#[derive(Default, Clone)]
struct KeyArena {
    data: Vec<u8>,
    /// End of each key in `data`.
    ends: Vec<u32>,
}

impl KeyArena {
    #[inline]
    fn len(&self) -> usize {
        self.ends.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    fn push(&mut self, k: &[u8]) -> Result<()> {
        self.data.extend_from_slice(k);
        self.ends.push(u32::try_from(self.data.len()).map_err(|_| bad("group keys past 4 GiB"))?);
        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(move |i| &self[i])
    }
}

impl std::ops::Index<usize> for KeyArena {
    type Output = [u8];
    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.data[a..self.ends[i] as usize]
    }
}

impl<'a> IntoIterator for &'a KeyArena {
    type Item = &'a [u8];
    type IntoIter = Box<dyn Iterator<Item = &'a [u8]> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Group ids by encoded group key, and each group's first row's key bytes.
/// The index holds group ids, not keys: each key is held once, in `keys`
/// (a map from key to id held it twice, and TPC-H Q3's 1.5M groups took
/// four passes for it).
#[derive(Default, Clone)]
struct Groups {
    ids: hashbrown::HashTable<u32>,
    hasher: foldhash::fast::RandomState,
    keys: KeyArena,
    /// What the keys hold, priced: each key's bytes, its box (pointer and
    /// allocator rounding), its list slot and index entry with their slack.
    bytes: u64,
}

/// Up to this many groups, a row's group is found by comparing its key
/// bytes with each group's: a few memcmp of short keys beat hashing every
/// row (profiled at 8% of a 7.3M-row build with 5 groups).
const GROUPS_LINEAR: usize = 16;

impl Groups {
    #[inline]
    fn id(&mut self, kb: &[u8]) -> Result<u32> {
        use std::hash::BuildHasher as _;
        let Groups { ids, hasher, keys, bytes } = self;
        if keys.len() <= GROUPS_LINEAR {
            if let Some(i) = keys.iter().position(|k| k == kb) {
                return Ok(i as u32);
            }
        }
        let h = hasher.hash_one(kb);
        if keys.len() > GROUPS_LINEAR {
            if let Some(&g) = ids.find(h, |&g| keys[g as usize] == *kb) {
                return Ok(g);
            }
        }
        let g = u32::try_from(keys.len()).map_err(|_| bad("more than 4 billion groups"))?;
        *bytes += kb.len() as u64 + 24;
        keys.push(kb)?;
        ids.insert_unique(h, g, |&x| hasher.hash_one(&keys[x as usize]));
        Ok(g)
    }
}

/// key → group id, built from the GROUP BY side.
enum KeyMap {
    /// Direct-address over [lo, lo + len): `NONE` = no row at that key.
    Direct8(i64, Vec<u8>),
    Direct16(i64, Vec<u16>),
    Direct32(i64, Vec<u32>),
    Hash(HashMap<i64, u32, foldhash::fast::RandomState>),
}

impl KeyMap {
    #[inline]
    fn get(&self, k: i64) -> Option<u32> {
        match self {
            KeyMap::Direct8(lo, a) => k.checked_sub(*lo).and_then(|i| a.get(usize::try_from(i).ok()?)).and_then(|&g| (g != u8::MAX).then_some(u32::from(g))),
            KeyMap::Direct16(lo, a) => k.checked_sub(*lo).and_then(|i| a.get(usize::try_from(i).ok()?)).and_then(|&g| (g != u16::MAX).then_some(u32::from(g))),
            KeyMap::Direct32(lo, a) => k.checked_sub(*lo).and_then(|i| a.get(usize::try_from(i).ok()?)).and_then(|&g| (g != u32::MAX).then_some(g)),
            KeyMap::Hash(m) => m.get(&k).copied(),
        }
    }

    /// Keys held (a count; the direct arrays are scanned).
    fn keys(&self) -> usize {
        match self {
            KeyMap::Direct8(_, a) => a.iter().filter(|&&g| g != u8::MAX).count(),
            KeyMap::Direct16(_, a) => a.iter().filter(|&&g| g != u16::MAX).count(),
            KeyMap::Direct32(_, a) => a.iter().filter(|&&g| g != u32::MAX).count(),
            KeyMap::Hash(m) => m.len(),
        }
    }

    fn for_each_key(&self, mut f: impl FnMut(i64)) {
        match self {
            KeyMap::Direct8(lo, a) => a.iter().enumerate().filter(|x| *x.1 != u8::MAX).for_each(|(i, _)| f(lo + i as i64)),
            KeyMap::Direct16(lo, a) => a.iter().enumerate().filter(|x| *x.1 != u16::MAX).for_each(|(i, _)| f(lo + i as i64)),
            KeyMap::Direct32(lo, a) => a.iter().enumerate().filter(|x| *x.1 != u32::MAX).for_each(|(i, _)| f(lo + i as i64)),
            KeyMap::Hash(m) => m.keys().for_each(|&k| f(k)),
        }
    }

    fn bytes(&self) -> u64 {
        match self {
            KeyMap::Direct8(_, a) => a.len() as u64,
            KeyMap::Direct16(_, a) => 2 * a.len() as u64,
            KeyMap::Direct32(_, a) => 4 * a.len() as u64,
            KeyMap::Hash(m) => 24 * m.capacity() as u64,
        }
    }
}

/// The largest direct-address array the build may allocate.
const DIRECT_MAX: u64 = 64 << 20;

/// Backends a Postgres key range without an index is asked of at once.
const KEY_RANGE_PARTS: usize = 8;

/// One page range of a Postgres table and the key range it holds: what the
/// partitioned MIN/MAX found, and the span an early probe scans.
#[derive(Clone, Debug)]
struct Part {
    pages: (i64, Option<i64>),
    keys: Option<(i64, i64)>,
}

/// The page range a key range's rows lie in, from the per-part key ranges
/// the server gave (a table stored in key order: TPC-H's lineitem), as a
/// `ctid` condition; `None` when every part holds some (no narrowing). A
/// pass over [lo, hi] then scans its pages, not the whole table — a scan
/// bound, not another table's data sent to the server.
fn ctid_span(parts: &[Part], lo: i64, hi: i64) -> Option<String> {
    // (APITAP_SQL_NARROW=0: off, for measuring against the whole-table scan.)
    if parts.is_empty() || std::env::var("APITAP_SQL_NARROW").is_ok_and(|v| v == "0") {
        return None;
    }
    // The parts whose keys meet [lo, hi]. The open-ended last part was taken
    // always (rows appended since its range was asked land there), which
    // dropped the upper page bound and had every pass of TPC-H Q3 scan to
    // the table's end. A row appended between the passes is the passes'
    // snapshot question (one snapshot for every pass, on the production
    // list), not this one's.
    let hit: Vec<&Part> = parts.iter().filter(|x| x.keys.is_some_and(|(a, b)| a <= hi && b >= lo)).collect();
    if hit.len() == parts.len() {
        return None;
    }
    let Some(start) = hit.iter().map(|x| x.pages.0).min() else {
        return Some("false".into());
    };
    let end = if hit.iter().any(|x| x.pages.1.is_none()) { None } else { hit.iter().filter_map(|x| x.pages.1).max() };
    Some(format!("ctid >= '({start},0)'::tid{}", end.map(|e| format!(" AND ctid < '({e},0)'::tid")).unwrap_or_default()))
}

/// The widest key range the runtime key filter covers (a 16 MiB bitmap;
/// `get_bit` takes an int4 bit index).
const KEY_FILTER_MAX_KEYS: u64 = 128 << 20;

/// What a probe holds past its read pipes and the build structure: the
/// batches the fold has in hand and the ones queued to it, and the host
/// process (an interpreter, its runtime). The read price per pipe was swept
/// on reads alone; a probe priced on it filled a 256 MB cage to the last
/// pipe and an unfiltered 60M x 70M probe (TPC-H lineitem x orders, five
/// aggregates) was OOM-killed at 8 pipes beside a 57 MiB key map.
const PROBE_HEADROOM: u64 = 64 << 20;

/// Fill the structure for UNIQUE keys; `None` if a key repeats.
fn key_map_unique(pairs: &[(i64, u32)], groups: usize) -> Option<KeyMap> {
    let (Some(lo), Some(hi)) = (pairs.iter().map(|p| p.0).min(), pairs.iter().map(|p| p.0).max()) else {
        return Some(KeyMap::Hash(HashMap::default()));
    };
    let span = u64::try_from(hi as i128 - lo as i128 + 1).unwrap_or(u64::MAX);
    let width: u64 = if groups < 255 {
        1
    } else if groups < 65535 {
        2
    } else {
        4
    };
    // Direct-address only where it is the smaller structure (the price
    // `build_cost` gives a hash entry): a sparse range is a hash map (TPC-H
    // Q3's 366K orders over 15M keys measured 57 MiB as a direct array).
    let direct = span.saturating_mul(width);
    if direct <= DIRECT_MAX && direct <= 24 * pairs.len() as u64 {
        let span = span as usize;
        macro_rules! fill {
            ($t:ty, $v:ident) => {{
                let mut a = vec![<$t>::MAX; span];
                for &(k, g) in pairs {
                    let slot = &mut a[(k - lo) as usize];
                    if *slot != <$t>::MAX {
                        return None;
                    }
                    *slot = g as $t;
                }
                KeyMap::$v(lo, a)
            }};
        }
        return Some(match width {
            1 => fill!(u8, Direct8),
            2 => fill!(u16, Direct16),
            _ => fill!(u32, Direct32),
        });
    }
    let mut m: HashMap<i64, u32, foldhash::fast::RandomState> =
        HashMap::with_capacity_and_hasher(pairs.len(), Default::default());
    for &(k, g) in pairs {
        if m.insert(k, g).is_some() {
            return None;
        }
    }
    Some(KeyMap::Hash(m))
}

/// The build side's key → group(s). A unique key maps straight to its group;
/// when keys repeat (issues × labels, orders × items) each key maps to a slot
/// whose groups sit in `multi` (CSR: offsets, group ids), and a probe row
/// counts once per match, as an SQL join does.
struct BuildMap {
    map: KeyMap,
    multi: Option<(Vec<u32>, Vec<u32>)>,
}

enum Look<'a> {
    Miss,
    One(u32),
    Many(&'a [u32]),
}

impl BuildMap {
    fn keys(&self) -> usize {
        self.map.keys()
    }

    fn for_each_key(&self, f: impl FnMut(i64)) {
        self.map.for_each_key(f)
    }

    #[inline]
    fn look(&self, k: i64) -> Look<'_> {
        match (self.map.get(k), &self.multi) {
            (None, _) => Look::Miss,
            (Some(g), None) => Look::One(g),
            (Some(slot), Some((offs, gids))) => {
                let s = slot as usize;
                Look::Many(&gids[offs[s] as usize..offs[s + 1] as usize])
            }
        }
    }

    fn bytes(&self) -> u64 {
        self.map.bytes() + self.multi.as_ref().map_or(0, |(o, g)| 4 * (o.len() + g.len()) as u64)
    }

    fn label(&self) -> String {
        let base = match &self.map {
            KeyMap::Direct8(..) => "direct u8",
            KeyMap::Direct16(..) => "direct u16",
            KeyMap::Direct32(..) => "direct u32",
            KeyMap::Hash(_) => "hash",
        };
        match &self.multi {
            None => base.to_string(),
            Some((o, g)) => format!("{base}, {} keys repeat into {} rows", o.len() - 1, g.len()),
        }
    }
}

/// Pick and fill the build structure from the (key, group) pairs.
fn build_map(pairs: &mut [(i64, u32)], groups: usize) -> BuildMap {
    if let Some(map) = key_map_unique(pairs, groups) {
        return BuildMap { map, multi: None };
    }
    pairs.sort_by_key(|p| p.0);
    let mut slots: Vec<(i64, u32)> = Vec::new();
    let mut offs: Vec<u32> = vec![0];
    let mut gids: Vec<u32> = Vec::with_capacity(pairs.len());
    let mut i = 0;
    while i < pairs.len() {
        let k = pairs[i].0;
        slots.push((k, slots.len() as u32));
        while i < pairs.len() && pairs[i].0 == k {
            gids.push(pairs[i].1);
            i += 1;
        }
        offs.push(gids.len() as u32);
    }
    let map = key_map_unique(&slots, slots.len()).expect("one slot per key");
    BuildMap { map, multi: Some((offs, gids)) }
}

/// One aggregate's running state for one group.
#[derive(Clone, Copy)]
enum Acc {
    None,
    I(i64),
    D(i128),
    F(f64),
    Avg(f64, u64),
    N(u64),
}

/// Two accumulators of one aggregate as one (groups merged).
fn merge_acc(a: &mut Acc, b: Acc, f: AggFn) -> Result<()> {
    *a = match (*a, b) {
        (x, Acc::None) => x,
        (Acc::None, y) => y,
        (Acc::N(x), Acc::N(y)) => Acc::N(x + y),
        (Acc::Avg(s, n), Acc::Avg(t, m)) => Acc::Avg(s + t, n + m),
        (Acc::I(x), Acc::I(y)) => Acc::I(match f {
            AggFn::Sum => x.checked_add(y).ok_or_else(|| bad("SUM overflowed 64-bit integers"))?,
            AggFn::Min => x.min(y),
            _ => x.max(y),
        }),
        (Acc::D(x), Acc::D(y)) => Acc::D(match f {
            AggFn::Sum => x.checked_add(y).ok_or_else(|| bad("SUM overflowed 38 decimal digits"))?,
            AggFn::Min => x.min(y),
            _ => x.max(y),
        }),
        (Acc::F(x), Acc::F(y)) => Acc::F(match f {
            AggFn::Sum => x + y,
            AggFn::Min => x.min(y),
            _ => x.max(y),
        }),
        _ => return Err(bad("two accumulators of one aggregate of different kinds")),
    };
    Ok(())
}

/// The matched groups re-keyed to the fields `keep(i, n)` holds (field `i`
/// of `n`), the accumulators of groups that become one merged.
fn merge_fields(groups: Groups, accs: Vec<Acc>, hit: Vec<bool>, aggs: &[Agg], keep: &dyn Fn(usize, usize) -> bool) -> Result<(Groups, Vec<Acc>, Vec<bool>)> {
    let na = aggs.len();
    let mut ng = Groups::default();
    let mut nacc: Vec<Acc> = Vec::new();
    let mut nhit: Vec<bool> = Vec::new();
    let mut kb = Vec::new();
    for (g, k) in groups.keys.iter().enumerate() {
        if !hit.get(g).copied().unwrap_or(false) {
            continue;
        }
        kb.clear();
        let f = group_fields(k);
        let n = f.len();
        for (i, (t, v)) in f.into_iter().enumerate() {
            if keep(i, n) {
                kb.push(t);
                if t != 0 {
                    kb.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    kb.extend_from_slice(v);
                }
            }
        }
        let to = ng.id(&kb)? as usize;
        if nacc.len() < (to + 1) * na {
            nacc.resize((to + 1) * na, Acc::None);
            nhit.resize(to + 1, false);
        }
        nhit[to] = true;
        for (i, a) in aggs.iter().enumerate() {
            merge_acc(&mut nacc[to * na + i], accs[g * na + i], a.f)?;
        }
    }
    Ok((ng, nacc, nhit))
}

/// The build groups with the probe's GROUP BY fields: each combined group
/// (`cg`, keyed by build group id then probe fields) as the build key then
/// those fields, in the combined ids' order (the accumulators' indices).
fn combine(groups: &Groups, cg: Groups) -> Result<Groups> {
    let mut out = Groups::default();
    for k in &cg.keys {
        let g = u32::from_le_bytes(k[..4].try_into().map_err(|_| bad("a combined group key"))?) as usize;
        let mut key = groups.keys[g].to_vec();
        key.extend_from_slice(&k[4..]);
        out.id(&key)?;
    }
    Ok(out)
}

/// The groups without the fields only the passes needed: a star plan's
/// check values, the CASE flags (the probe's GROUP BY fields after them stay).
fn strip(p: &Plan, g: Groups, a: Vec<Acc>, h: Vec<bool>) -> Result<(Groups, Vec<Acc>, Vec<bool>)> {
    let nflags = usize::from(!p.sides[p.group_side].flags.is_empty());
    match &p.star {
        Some(st) if nflags > 0 || st.hidden.iter().any(|&x| x) => {
            let nb = st.hidden.len();
            merge_fields(g, a, h, &p.aggs, &|i, _| (i < nb && !st.hidden[i]) || i >= nb + nflags)
        }
        None if nflags > 0 => merge_fields(g, a, h, &p.aggs, &|i, n| i + 1 < n),
        _ => Ok((g, a, h)),
    }
}

/// Groups whose GROUP BY text is equal under its column's collation, as
/// one: a MySQL `_ci` column's 'Brazil' and 'BRAZIL' are one group there, and
/// the answer shows the first value the group met (MySQL shows one of them).
/// Groups are few next to rows, so the fold runs here, not per row.
fn collate(p: &Plan, g: Groups, a: Vec<Acc>, h: Vec<bool>) -> Result<(Groups, Vec<Acc>, Vec<bool>)> {
    let keep = p.keys.is_some().then_some(h.as_slice());
    Ok(collated(p, &g, &a, keep)?.unwrap_or((g, a, h)))
}

/// [`collate`] over borrowed groups (`keep`: the groups that are rows;
/// `None`: all); `None` when no GROUP BY column folds.
fn collated(p: &Plan, g: &Groups, a: &[Acc], keep: Option<&[bool]>) -> Result<Option<(Groups, Vec<Acc>, Vec<bool>)>> {
    // A plain SELECT staged as GROUP BY every output (`stage`): its rows are
    // rows, not groups, and 'a' and 'A' are both returned.
    if GROUPS_EXACT.try_with(|_| ()).is_ok() {
        return Ok(None);
    }
    let folds: Vec<(usize, Coll)> = p
        .gcoll
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.plain())
        .filter_map(|(gi, &c)| Some((match &p.star { Some(st) => st.gpos_rt[gi]?, None => gi }, c)))
        .collect();
    if folds.is_empty() {
        return Ok(None);
    }
    let na = p.aggs.len();
    let mut rep: HashMap<(usize, Vec<u8>), Vec<u8>> = HashMap::new();
    let (mut ng, mut nacc, mut nhit) = (Groups::default(), Vec::<Acc>::new(), Vec::<bool>::new());
    let mut kb = Vec::new();
    for (gi, k) in g.keys.iter().enumerate() {
        if keep.is_some_and(|h| !h.get(gi).copied().unwrap_or(false)) {
            continue;
        }
        kb.clear();
        for (i, (t, v)) in group_fields(k).into_iter().enumerate() {
            kb.push(t);
            if t == 0 {
                continue;
            }
            let v: Vec<u8> = match folds.iter().find(|x| x.0 == i) {
                Some(&(_, c)) if t == 5 => rep.entry((i, c.fold(v).into_owned())).or_insert_with(|| v.to_vec()).clone(),
                _ => v.to_vec(),
            };
            kb.extend_from_slice(&(v.len() as u32).to_le_bytes());
            kb.extend_from_slice(&v);
        }
        let to = ng.id(&kb)? as usize;
        if nacc.len() < (to + 1) * na {
            nacc.resize((to + 1) * na, Acc::None);
            nhit.resize(to + 1, false);
        }
        nhit[to] = true;
        for (i, ag) in p.aggs.iter().enumerate() {
            merge_acc(&mut nacc[to * na + i], a[gi * na + i], ag.f)?;
        }
    }
    Ok(Some((ng, nacc, nhit)))
}

fn fold(a: &mut Acc, f: AggFn, ty: Option<Ty>, v: Num) -> Result<()> {
    if f == AggFn::CountStar {
        if let Acc::N(n) = a {
            *n += 1;
        } else {
            *a = Acc::N(1);
        }
        return Ok(());
    }
    if matches!(v, Num::Null) {
        if matches!(a, Acc::None) && f == AggFn::Count {
            *a = Acc::N(0);
        }
        return Ok(());
    }
    match f {
        AggFn::Count => {
            *a = Acc::N(if let Acc::N(n) = a { *n + 1 } else { 1 });
        }
        AggFn::Avg => {
            let x = as_f64(v, ty.unwrap_or(Ty::Float)).unwrap_or(0.0);
            *a = match a {
                Acc::Avg(s, n) => Acc::Avg(*s + x, *n + 1),
                _ => Acc::Avg(x, 1),
            };
        }
        AggFn::Sum | AggFn::Min | AggFn::Max => {
            *a = match (*a, v) {
                (Acc::None, Num::Int(x)) => Acc::I(x),
                (Acc::None, Num::Dec(x)) => Acc::D(x),
                (Acc::None, Num::Float(x)) => Acc::F(x),
                (Acc::I(s), Num::Int(x)) => Acc::I(match f {
                    AggFn::Sum => s.checked_add(x).ok_or_else(|| bad("SUM overflowed 64-bit integers"))?,
                    AggFn::Min => s.min(x),
                    _ => s.max(x),
                }),
                (Acc::D(s), Num::Dec(x)) => Acc::D(match f {
                    AggFn::Sum => s.checked_add(x).ok_or_else(|| bad("SUM overflowed 38 decimal digits"))?,
                    AggFn::Min => s.min(x),
                    _ => s.max(x),
                }),
                (Acc::F(s), Num::Float(x)) => Acc::F(match f {
                    AggFn::Sum => s + x,
                    AggFn::Min => s.min(x),
                    _ => s.max(x),
                }),
                _ => return Err(bad("aggregate type invariant")),
            };
        }
        AggFn::CountStar => unreachable!("handled above"),
    }
    Ok(())
}

/// Everything resolved from the statement.
struct Plan {
    sides: Vec<Side>,
    /// (side, read position) of each side's join key, when there is a join.
    keys: Option<[(usize, usize); 2]>,
    /// The GROUP BY side (the build side of a join) and its group columns'
    /// read positions; `group_side` is the only side without a join.
    group_side: usize,
    groups: Vec<usize>,
    agg_side: usize,
    aggs: Vec<Agg>,
    out: Vec<(String, OutCol)>,
    order: Vec<(usize, bool)>,
    limit: Option<usize>,
    /// Three or more tables: the dimensions and the field layout
    /// (`sides` then holds `B` at 0 and `P` at 1).
    star: Option<star::Star>,
    /// The GROUP BY holds the build side's join key: a key range's groups
    /// are its own, so each pass's answer is final when it ends (and the
    /// groups' memory is the pass's to split on).
    key_groups: bool,
    /// WHERE conditions naming several tables, all of whose columns are
    /// GROUP BY columns: a filter of the final groups.
    residual: Option<Residual>,
    /// An OR over two tables' conditions (TPC-H Q19): per branch, the build
    /// row's flag bit (`None`: no build condition) and the probe row's
    /// condition (`None`: none); a match passes when one branch holds on both.
    orx: Vec<(Option<u32>, Option<PExpr>)>,
    /// Each GROUP BY column's collation: groups equal under it are one
    /// (`collate`), and ORDER BY compares by it.
    gcoll: Vec<Coll>,
    /// HAVING: (aggregate, op, number) per conjunct, all must hold.
    having: Vec<(usize, star::Op, VExpr)>,
}

/// Conditions over the GROUP BY columns: each read position is a GROUP BY
/// index (`read`), typed by `tys`.
struct Residual {
    preds: Vec<PExpr>,
    read: Vec<usize>,
    tys: Vec<Option<Ty>>,
}

impl Residual {
    /// Whether the group whose fields are `f` passes (`fi`: GROUP BY index →
    /// field).
    fn ok(&self, f: &[(u8, &[u8])], fi: &dyn Fn(usize) -> usize) -> bool {
        let cols: Vec<FinishedCol> = self
            .read
            .iter()
            .map(|&g| {
                let (t, b) = f[fi(g)];
                let some = Some(vec![1u8]);
                match t {
                    1 => FinishedCol::I64 { validity: some, data: vec![i64::from_le_bytes(b.try_into().unwrap_or([0; 8]))] },
                    2 => FinishedCol::F64 { validity: some, data: vec![f64::from_le_bytes(b.try_into().unwrap_or([0; 8]))] },
                    3 => FinishedCol::Dec128 { validity: some, data: vec![i128::from_le_bytes(b.try_into().unwrap_or([0; 16]))] },
                    4 => FinishedCol::Bool { validity: some, data: vec![u8::from(b.first() == Some(&1))] },
                    5 => FinishedCol::Utf8 { validity: some, offsets: vec![0, b.len() as i32], data: b.to_vec() },
                    _ => FinishedCol::I64 { validity: Some(vec![0]), data: vec![0] },
                }
            })
            .collect();
        let b = ArrowBatch { rows: 1, cols };
        self.preds.iter().all(|p| peval(p, &b, 0, &self.tys) == Some(true))
    }
}

fn ident_name(i: &Ident) -> String {
    i.value.clone()
}

/// A column of each table compared on the joined row: only kinds whose
/// values compare as the servers would. The check compares the group-key
/// encoding, where a date, an integer and a timestamp are all one i64 and
/// text is raw bytes; so a date against a timestamp (days against micros),
/// a date against a number, a number against text, and text under a
/// collation (MySQL `_ci`/`_ai`, a non-C Postgres locale) would answer
/// silently wrong. Those are refused, naming both columns.
fn comparable(sides: &[Side], a: (usize, usize), b: (usize, usize)) -> Result<()> {
    use ArrowKind as K;
    let (ka, kb) = (&sides[a.0].schema[a.1].kind, &sides[b.0].schema[b.1].kind);
    let num = |k: &K| matches!(k, K::Int16 | K::Int32 | K::Int64 | K::Decimal { .. } | K::Float32 | K::Float64);
    let ok = (num(ka) && num(kb))
        || matches!((ka, kb), (K::Date32, K::Date32) | (K::TimestampUtc, K::TimestampUtc) | (K::TimestampNaive, K::TimestampNaive));
    if ok {
        return Ok(());
    }
    Err(unsupported(format!(
        "a comparison of {}.{} ({ka:?}) with {}.{} ({kb:?}) across tables (numbers, two dates or two timestamps of one kind \
         compare there; text, whose collation may differ, and mixed kinds do not)",
        sides[a.0].name, sides[a.0].schema[a.1].name, sides[b.0].name, sides[b.0].schema[b.1].name
    )))
}

/// The (side, schema column) an expression names, if it is a column.
fn column_of(sides: &[Side], e: &Expr) -> Result<Option<(usize, usize)>> {
    match e {
        Expr::Identifier(i) => {
            let n = ident_name(i);
            let hits: Vec<(usize, usize)> = sides
                .iter()
                .enumerate()
                .filter_map(|(s, side)| side.col(&n).map(|c| (s, c)))
                .collect();
            match hits.as_slice() {
                [one] => Ok(Some(*one)),
                [] => Err(bad(format!("no table has a column {n}"))),
                _ => Err(bad(format!("column {n} is in both tables; qualify it"))),
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let (t, c) = (ident_name(&parts[0]), ident_name(&parts[1]));
            let s = sides
                .iter()
                .position(|x| x.name.eq_ignore_ascii_case(&t))
                .ok_or_else(|| bad(format!("{t} is not a table of the query")))?;
            let col = sides[s]
                .col(&c)
                .ok_or_else(|| bad(format!("{t} has no column {c}")))?;
            Ok(Some((s, col)))
        }
        Expr::Nested(x) => column_of(sides, x),
        _ => Ok(None),
    }
}

/// Every side an expression's columns come from.
fn sides_of(sides: &[Side], e: &Expr, out: &mut Vec<usize>) -> Result<()> {
    let mut err = None;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            match column_of(sides, x) {
                Ok(Some((s, _))) => {
                    if !out.contains(&s) {
                        out.push(s);
                    }
                }
                Ok(None) => {}
                Err(e) => err = Some(e),
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// A conjunct for one side, rendered for its source: qualifiers dropped,
/// column names spelled as that side's schema spells them.
fn render_for(sides: &[Side], s: usize, e: &Expr) -> Result<String> {
    // A subquery left inside a condition (under OR, NOT, CASE …) would go to
    // the server as text naming tables it does not have (a stage table:
    // `relation "__s.st1_a0" does not exist`): only a top-level IN/EXISTS
    // conjunct is a subquery here.
    let mut sub = false;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if matches!(x, Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }) {
            sub = true;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    if sub {
        return Err(unsupported(format!("the WHERE condition {e}: a subquery under OR, NOT or an expression (an IN or EXISTS of its own conjunct only)")));
    }
    let mut e = e.clone();
    let mut err = None;
    let _ = sqlparser::ast::visit_expressions_mut(&mut e, |x| {
        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            match column_of(sides, x) {
                Ok(Some((_, c))) => *x = Expr::Identifier(Ident::new(sides[s].schema[c].name.clone())),
                Ok(None) => {}
                Err(e) => err = Some(e),
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    match err {
        Some(e) => Err(e),
        None => Ok(e.to_string()),
    }
}

fn conjuncts(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        // Any parenthesised conjunct: `(NOT EXISTS (…))` from a rewrite's
        // `and_of` must still be seen as the EXISTS it is.
        Expr::Nested(x) => conjuncts(x, out),
        _ => out.push(e.clone()),
    }
}

fn number(s: &str) -> Result<VExpr> {
    if let Ok(i) = s.parse::<i64>() {
        return Ok(VExpr::Int(i));
    }
    if !s.contains(['e', 'E']) {
        if let Some((w, f)) = s.split_once('.') {
            let digits = format!("{w}{f}");
            if let (Ok(v), Ok(sc)) = (digits.parse::<i128>(), i8::try_from(f.len())) {
                if sc <= 38 {
                    return Ok(VExpr::Dec(v, sc));
                }
            }
        }
    }
    s.parse::<f64>()
        .map(VExpr::Float)
        .map_err(|_| bad(format!("number {s}")))
}

/// An aggregate argument: arithmetic over one side's numeric columns.
fn vexpr(sides: &mut [Side], e: &Expr, side: &mut Option<usize>) -> Result<VExpr> {
    if let Some((s, c)) = column_of(sides, e)? {
        let k = sides[s].schema[c].kind;
        // A star dimension's column (index 2 on, B and P first): read off
        // its payload into a lookup column of the fact table (P, index 1).
        let (s, at) = if s >= 2 && side.is_none_or(|x| x == 1) {
            let name = format!("__lk_{}_{}", sides[s].name, sides[s].schema[c].name);
            let i = match sides[1].schema.iter().position(|f| f.name == name) {
                Some(i) => i,
                None => {
                    sides[1].schema.push(ArrowField { name, kind: k, nullable: true, rewritten: false, padded: false, ci: false, ai: false });
                    let i = sides[1].schema.len() - 1;
                    sides[1].lookups.push((i, s, c));
                    i
                }
            };
            (1, sides[1].want(i))
        } else {
            (s, sides[s].want(c))
        };
        if side.is_some_and(|x| x != s) {
            return Err(unsupported("an aggregate argument mixing both tables' columns"));
        }
        *side = Some(s);
        return Ok(VExpr::Col(at, k));
    }
    Ok(match e {
        Expr::Nested(x) => vexpr(sides, x, side)?,
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => number(n)?,
            other => return Err(unsupported(format!("the literal {other} in an aggregate"))),
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => VExpr::Neg(Box::new(vexpr(sides, expr, side)?)),
        Expr::UnaryOp { op: UnaryOperator::Plus, expr } => vexpr(sides, expr, side)?,
        Expr::Cast { expr, data_type, .. } => {
            let dt = data_type.to_string().to_ascii_uppercase();
            let t = if ["INT", "INTEGER", "BIGINT", "SMALLINT", "INT8", "INT4", "SIGNED"].contains(&dt.as_str()) {
                Ty::Int
            } else if ["DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8", "REAL"].contains(&dt.as_str()) {
                Ty::Float
            } else if let Some(args) = dt
                .strip_prefix("DECIMAL(")
                .or_else(|| dt.strip_prefix("NUMERIC("))
                .and_then(|r| r.strip_suffix(')'))
            {
                let sc = args.split(',').nth(1).map(str::trim).unwrap_or("0");
                Ty::Dec(sc.parse::<i8>().map_err(|_| bad(format!("CAST to {dt}")))?)
            } else {
                return Err(unsupported(format!("CAST to {dt} (INTEGER, BIGINT, DECIMAL(p,s), DOUBLE)")));
            };
            VExpr::Cast(Box::new(vexpr(sides, expr, side)?), t)
        }
        Expr::BinaryOp { left, op, right } => {
            let o = match op {
                BinaryOperator::Plus => '+',
                BinaryOperator::Minus => '-',
                BinaryOperator::Multiply => '*',
                BinaryOperator::Divide => '/',
                other => return Err(unsupported(format!("the operator {other} in an aggregate"))),
            };
            VExpr::Bin(Box::new(vexpr(sides, left, side)?), o, Box::new(vexpr(sides, right, side)?))
        }
        Expr::Case { operand, conditions, else_result } if case_other_table(sides, operand.as_deref(), conditions, else_result.as_deref(), *side)?.is_some() => {
            let (cs, rs) = case_other_table(sides, operand.as_deref(), conditions, else_result.as_deref(), *side)?.expect("checked");
            *side = Some(rs);
            let mut arms = Vec::new();
            for w in conditions {
                let cond = match operand {
                    Some(o) => Expr::BinaryOp { left: o.clone(), op: BinaryOperator::Eq, right: Box::new(w.condition.clone()) },
                    None => w.condition.clone(),
                };
                let p = pexpr(sides, cs, &cond)?;
                let bit = u32::try_from(sides[cs].flags.len()).ok().filter(|&b| b < 64).ok_or_else(|| unsupported("more than 64 CASE conditions over the build table"))?;
                sides[cs].flags.push(p);
                arms.push((bit, vexpr(sides, &w.result, side)?));
            }
            let els = match else_result {
                Some(x) if !matches!(x.as_ref(), Expr::Value(v) if matches!(v.value, Value::Null)) => Some(Box::new(vexpr(sides, x, side)?)),
                _ => None,
            };
            if *side != Some(rs) {
                return Err(unsupported("a CASE whose results name both tables"));
            }
            let e = VExpr::CaseB { arms, els, side: cs };
            type_of(&e)?;
            e
        }
        Expr::Case { operand, conditions, else_result } => {
            // The side is the one the conditions name (or the one already
            // set by the rest of the argument).
            let s = match *side {
                Some(s) => s,
                None => {
                    let mut found = None;
                    for w in conditions {
                        found = found.or(first_side(sides, &w.condition)?);
                    }
                    found.or(operand.as_deref().map(|o| first_side(sides, o)).transpose()?.flatten())
                        .ok_or_else(|| unsupported("a CASE whose conditions name no column"))?
                }
            };
            *side = Some(s);
            let mut arms = Vec::new();
            for w in conditions {
                // `CASE x WHEN v` is `CASE WHEN x = v`.
                let cond = match operand {
                    Some(o) => Expr::BinaryOp { left: o.clone(), op: BinaryOperator::Eq, right: Box::new(w.condition.clone()) },
                    None => w.condition.clone(),
                };
                let p = pexpr(sides, s, &cond)?;
                arms.push((p, vexpr(sides, &w.result, side)?));
            }
            let els = match else_result {
                Some(x) if !matches!(x.as_ref(), Expr::Value(v) if matches!(v.value, Value::Null)) => Some(Box::new(vexpr(sides, x, side)?)),
                _ => None,
            };
            if *side != Some(s) {
                return Err(unsupported("a CASE mixing both tables' columns"));
            }
            let tys = side_tys(&sides[s]);
            let e = VExpr::Case { arms, els, tys };
            type_of(&e)?;
            e
        }
        other => return Err(unsupported(format!("the expression {other} in an aggregate"))),
    })
}

/// A CASE whose conditions name one table and whose results name another:
/// (conditions' table, results' table).
fn case_other_table(sides: &[Side], operand: Option<&Expr>, conditions: &[sqlparser::ast::CaseWhen], els: Option<&Expr>, side: Option<usize>) -> Result<Option<(usize, usize)>> {
    let mut cs = None;
    for w in conditions {
        cs = cs.or(first_side(sides, &w.condition)?);
    }
    if let Some(o) = operand {
        cs = cs.or(first_side(sides, o)?);
    }
    let mut rs = side;
    for x in conditions.iter().map(|w| &w.result).chain(els) {
        rs = rs.or(first_side(sides, x)?);
    }
    Ok(match (cs, rs) {
        (Some(c), Some(r)) if c != r => Some((c, r)),
        _ => None,
    })
}

/// The tables whose columns an aggregate's value is made of, inside `e`
/// (a SELECT item, maybe arithmetic over aggregates): a CASE's results
/// count, its conditions do not (they may be another table's).
fn agg_value_sides(sides: &[Side], e: &Expr, inside: bool, out: &mut Vec<usize>) -> Result<()> {
    match e {
        Expr::Function(f) => {
            if let FunctionArguments::List(l) = &f.args {
                for a in &l.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) = a {
                        agg_value_sides(sides, x, true, out)?;
                    }
                }
            }
            Ok(())
        }
        Expr::Nested(x) | Expr::UnaryOp { expr: x, .. } => agg_value_sides(sides, x, inside, out),
        Expr::BinaryOp { left, right, .. } => {
            agg_value_sides(sides, left, inside, out)?;
            agg_value_sides(sides, right, inside, out)
        }
        Expr::Case { conditions, else_result, .. } if inside => {
            for w in conditions {
                agg_value_sides(sides, &w.result, true, out)?;
            }
            if let Some(x) = else_result {
                agg_value_sides(sides, x, true, out)?;
            }
            Ok(())
        }
        _ if inside => sides_of(sides, e, out),
        _ => Ok(()),
    }
}

/// The side the first column `e` names belongs to, if it names one.
fn first_side(sides: &[Side], e: &Expr) -> Result<Option<usize>> {
    let mut on = Vec::new();
    sides_of(sides, e, &mut on)?;
    Ok(on.first().copied())
}

fn aggregate(sides: &mut [Side], e: &Expr, side: &mut Option<usize>) -> Result<Option<Agg>> {
    let Expr::Function(f) = e else { return Ok(None) };
    let name = f.name.to_string().to_ascii_lowercase();
    let func = match name.as_str() {
        "sum" => AggFn::Sum,
        "count" => AggFn::Count,
        "min" => AggFn::Min,
        "max" => AggFn::Max,
        "avg" => AggFn::Avg,
        _ => return Err(unsupported(format!("the function {}", f.name))),
    };
    if f.filter.is_some() || f.over.is_some() || !f.within_group.is_empty() {
        return Err(unsupported(format!("{} with FILTER, OVER or WITHIN GROUP", f.name)));
    }
    let FunctionArguments::List(list) = &f.args else {
        return Err(unsupported(format!("{} without an argument list", f.name)));
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return Err(unsupported(format!("{} with DISTINCT, clauses, or not one argument", f.name)));
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if func == AggFn::Count => Ok(Some(Agg {
            f: AggFn::CountStar,
            arg: None,
            ty: None,
        })),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => {
            let v = vexpr(sides, x, side)?;
            fn nested_caseb(e: &VExpr) -> bool {
                match e {
                    VExpr::CaseB { .. } => true,
                    VExpr::Neg(x) | VExpr::Cast(x, _) | VExpr::Scaled(x, _) => nested_caseb(x),
                    VExpr::Bin(l, _, r) => nested_caseb(l) || nested_caseb(r),
                    VExpr::Case { arms, els, .. } => arms.iter().any(|a| nested_caseb(&a.1)) || els.as_deref().is_some_and(nested_caseb),
                    _ => false,
                }
            }
            let inner = match &v {
                VExpr::CaseB { arms, els, .. } => arms.iter().any(|a| nested_caseb(&a.1)) || els.as_deref().is_some_and(nested_caseb),
                other => nested_caseb(other),
            };
            if inner {
                return Err(unsupported(format!("{}: a CASE over the other table inside an expression (make the CASE the whole argument)", f.name)));
            }
            // COUNT of a column that is not a number (text, a date): counts
            // its non-NULL rows; it was refused as arithmetic on the column.
            if func == AggFn::Count {
                if let VExpr::Col(i, k) = &v {
                    if kind_ty(*k).is_none() {
                        let one = VExpr::Case { arms: vec![(PExpr::IsNull(*i, true), VExpr::Int(1))], els: None, tys: vec![None; *i + 1] };
                        return Ok(Some(Agg { f: func, arg: Some(one), ty: Some(Ty::Int) }));
                    }
                }
            }
            let ty = Some(type_of(&v)?);
            Ok(Some(Agg { f: func, arg: Some(v), ty }))
        }
        _ => Err(unsupported(format!("the argument of {}", f.name))),
    }
}

mod live;
pub(crate) mod mem;
mod semi;
mod stage;
mod star;
mod view;
pub use live::run_live;

fn plan(query: &str, sources: &HashMap<String, String>, schemas: &mut dyn FnMut(&str, &str) -> Result<Vec<ArrowField>>) -> Result<Plan> {
    plan_with(query, sources, schemas, &mut |_, _| Ok(None))
}

/// [`plan`] with each table's estimated row count (`rows(url, table)`, from
/// its server's catalog) to choose the build table of a star by.
fn plan_with(
    query: &str,
    sources: &HashMap<String, String>,
    schemas: &mut dyn FnMut(&str, &str) -> Result<Vec<ArrowField>>,
    rows: &mut dyn FnMut(&str, &str) -> Result<Option<f64>>,
) -> Result<Plan> {
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, query)
        .map_err(|e| bad(format!("parse error: {e}")))?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return Err(bad("expected exactly one SELECT statement"));
    };
    if q.with.is_some() || q.offset.is_some() || q.fetch.is_some() || !q.limit_by.is_empty() {
        return Err(unsupported("WITH, OFFSET, FETCH or LIMIT BY"));
    }
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return Err(unsupported("anything but a plain SELECT (UNION, VALUES, …)"));
    };
    // A derived table in FROM is inlined (TPC-H Q7/Q8/Q9).
    let flat;
    let sel: &sqlparser::ast::Select = match flatten(sel)? {
        Some(f) => {
            flat = f;
            &flat
        }
        None => sel,
    };
    if sel.distinct.is_some() || sel.top.is_some() || sel.prewhere.is_some() {
        return Err(unsupported("DISTINCT, TOP or PREWHERE"));
    }
    if sel.from.is_empty() {
        return Err(unsupported("a SELECT without FROM"));
    }
    let mut sides = Vec::new();
    let mut add = |tf: &TableFactor, sides: &mut Vec<Side>| -> Result<()> {
        let TableFactor::Table { name, alias, args: None, .. } = tf else {
            return Err(unsupported("a FROM item that is not a table"));
        };
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|p| match p {
                ObjectNamePart::Identifier(i) => ident_name(i),
            })
            .collect();
        if parts.len() < 2 || parts.len() > 3 {
            return Err(bad(format!(
                "table {name} must be <source>.<table> or <source>.<schema>.<table>, the source named in sources="
            )));
        }
        let url = sources
            .get(&parts[0])
            .ok_or_else(|| bad(format!("{} is not in sources= ({})", parts[0], sources.keys().cloned().collect::<Vec<_>>().join(", "))))?
            .clone();
        let dialect = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Dialect::Pg
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Dialect::My
        } else if ["github://", "github+api://", "gsheets://", mem::SCHEME].iter().any(|p| url.starts_with(p)) {
            Dialect::Other
        } else {
            return Err(unsupported(format!(
                "the source {} (postgres://, mysql://, github://, github+api://, gsheets:// today)",
                parts[0]
            )));
        };
        let table = parts[1..].join(".");
        let schema = schemas(&url, &table)?;
        let nm = alias.as_ref().map(|a| ident_name(&a.name)).unwrap_or_else(|| parts.last().cloned().unwrap_or_default());
        if sides.iter().any(|s: &Side| s.name.eq_ignore_ascii_case(&nm)) {
            return Err(bad(format!("two tables named {nm}; alias one")));
        }
        let whole = url.starts_with("github://") || url.starts_with("gsheets://");
        let read = if whole { (0..schema.len()).collect() } else { Vec::new() };
        sides.push(Side { name: nm, url, table, dialect, schema, read, conj: Vec::new(), conj_src: Vec::new(), local: Vec::new(), computed: Vec::new(), whole, semis: Vec::new(), flags: Vec::new(), virt: Vec::new(), lookups: Vec::new() });
        Ok(())
    };
    // Every table, in the order named: comma-separated FROM items and their
    // INNER JOINs; the ON conditions join the WHERE's conjuncts.
    let mut pool = Vec::new();
    for f in &sel.from {
        add(&f.relation, &mut sides)?;
        for j in &f.joins {
            add(&j.relation, &mut sides)?;
            match &j.join_operator {
                JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => conjuncts(e, &mut pool),
                _ => return Err(unsupported("a join that is not INNER JOIN … ON")),
            }
        }
    }
    if let Some(w) = &sel.selection {
        conjuncts(w, &mut pool);
    }
    // A conjunct every disjunct of an OR shares stands on its own (TPC-H
    // Q19 repeats its join `p_partkey = l_partkey` in each branch).
    let mut lifted = Vec::new();
    for c in pool.iter_mut() {
        let mut ds = Vec::new();
        disjuncts(c, &mut ds);
        if ds.len() < 2 {
            continue;
        }
        let parts: Vec<Vec<Expr>> = ds
            .iter()
            .map(|d| {
                let mut v = Vec::new();
                conjuncts(d, &mut v);
                v
            })
            .collect();
        let common: Vec<Expr> = parts[0].iter().filter(|x| parts.iter().all(|p| p.iter().any(|y| y.to_string() == x.to_string()))).cloned().collect();
        if common.is_empty() {
            continue;
        }
        let rest: Vec<Vec<Expr>> = parts.iter().map(|p| p.iter().filter(|x| !common.iter().any(|y| y.to_string() == x.to_string())).cloned().collect()).collect();
        lifted.extend(common);
        // A disjunct left with nothing is TRUE: the OR is.
        *c = if rest.iter().any(|r| r.is_empty()) {
            Expr::Value(sqlparser::ast::Value::Boolean(true).into())
        } else {
            or_of(rest.into_iter().map(and_of).collect())
        };
    }
    pool.retain(|c| !matches!(c, Expr::Value(v) if matches!(v.value, Value::Boolean(true))));
    pool.extend(lifted);
    // (Two tables: the OR's branches go to each server already, `orx`.)
    if sides.len() > 2 {
        let implied = implied_filters(&sides, &pool)?;
        pool.extend(implied);
    }
    // Equalities of two tables' columns join them; other comparisons of two
    // tables' columns are checks of the joined row; the rest filter.
    let mut eqs: Vec<(star::Col, star::Col)> = Vec::new();
    let mut cmps: Vec<(star::Col, star::Col, star::Op)> = Vec::new();
    let mut filters = Vec::new();
    for c in pool {
        if let Expr::BinaryOp { left, op, right } = &c {
            if let Some(o) = star::Op::of(op).filter(|o| *o != star::Op::Eq) {
                if let (Some(a), Some(b)) = (column_of(&sides, left)?, column_of(&sides, right)?) {
                    if a.0 != b.0 {
                        comparable(&sides, a, b)?;
                        cmps.push((a, b, o));
                        continue;
                    }
                }
            }
        }
        if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = &c {
            if let (Some(a), Some(b)) = (column_of(&sides, left)?, column_of(&sides, right)?) {
                if a.0 != b.0 {
                    let keyable = |s: usize, k: usize| matches!(sides[s].schema[k].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64 | ArrowKind::Utf8);
                    // A DECIMAL equality (TPC-H Q2's `ps_supplycost =` the
                    // staged MIN) is not a key to join by: it is checked on
                    // the row the integer keys joined, as `<` would be.
                    let dec = |s: usize, k: usize| matches!(sides[s].schema[k].kind, ArrowKind::Decimal { .. });
                    if dec(a.0, a.1) && dec(b.0, b.1) {
                        cmps.push((a, b, star::Op::Eq));
                        continue;
                    }
                    for (s, k) in [a, b] {
                        if !keyable(s, k) {
                            return Err(unsupported(format!(
                                "a join key that is not an integer ({}.{} is {:?})",
                                sides[s].name, sides[s].schema[k].name, sides[s].schema[k].kind
                            )));
                        }
                    }
                    eqs.push((a, b));
                    continue;
                }
            }
        }
        filters.push(c);
    }
    let items: Vec<(Expr, Option<String>)> = sel
        .projection
        .iter()
        .map(|i| match i {
            SelectItem::UnnamedExpr(e) => Ok((e.clone(), None)),
            SelectItem::ExprWithAlias { expr, alias } => Ok((expr.clone(), Some(ident_name(alias)))),
            _ => Err(unsupported("SELECT *")),
        })
        .collect::<Result<_>>()?;
    // EXTRACT(YEAR FROM col) in SELECT and GROUP BY: a virtual column of
    // col's table, computed here.
    let mut items = items;
    let mut gby = group_list(&sel.group_by, &items)?;
    for e in items.iter_mut().map(|x| &mut x.0).chain(gby.iter_mut()) {
        years_to_virtual(&mut sides, e)?;
    }
    let n = sides.len();
    // Two tables grouped on columns of both go the star way (its probe part
    // holds GROUP BY fields too).
    let two_sided = n == 2 && {
        let mut gt = Vec::new();
        for g in &gby {
            if let Some((s, _)) = column_of(&sides, g)? {
                if !gt.contains(&s) {
                    gt.push(s);
                }
            }
        }
        gt.len() == 2
    };
    let mut star_tree: Option<star::Tree> = None;
    match n {
        1 if !eqs.is_empty() => return Err(bad("an equality of two columns of one table")),
        1 => {}
        2 if eqs.is_empty() => return Err(unsupported("two tables with no join condition (a.k = b.k)")),
        // Two tables joined on one equality, nothing else of both compared:
        // the two-table plan. Otherwise the star plan, which checks the
        // other equalities and comparisons on the joined row.
        2 if !two_sided && eqs.len() == 1 && cmps.is_empty() => {}
        _ => {
            // The fact table: the one the aggregates name.
            let mut agg_tables = Vec::new();
            for (e, _) in &items {
                agg_value_sides(&sides, e, false, &mut agg_tables)?;
            }
            // Several (a fact's columns beside a dimension's, TPC-H Q9): the
            // one joined to the most tables is the fact.
            let degree = |t: usize| {
                let mut o: Vec<usize> = eqs.iter().filter_map(|&(a, b)| if a.0 == t { Some(b.0) } else if b.0 == t { Some(a.0) } else { None }).collect();
                o.sort_unstable();
                o.dedup();
                o.len()
            };
            let mut est = Vec::with_capacity(n);
            for sd in &sides {
                est.push(rows(&sd.url, &sd.table)?);
            }
            let p = match agg_tables.as_slice() {
                [p] => *p,
                // Only COUNT(*) (or nothing, a staged plan's hidden count):
                // the largest table streams, by the row estimates.
                [] => (0..n).max_by(|&a, &b| est[a].unwrap_or(-1.0).partial_cmp(&est[b].unwrap_or(-1.0)).unwrap_or(std::cmp::Ordering::Equal).then(b.cmp(&a))).unwrap_or(0),
                many => *many.iter().max_by_key(|&&t| (degree(t), std::cmp::Reverse(t))).expect("several"),
            };
            // The GROUP BY tables (P's join key stands for its partner's).
            let mut gt = Vec::new();
            for g in gby.clone() {
                if let Some((s, _)) = column_of(&sides, &g)? {
                    gt.push(s);
                }
            }
            // B: a table joined to P whose part holds every GROUP BY table.
            // B: a table joined to P whose part holds every GROUP BY table
            // if one does (the groups are then the build's alone), else the
            // first that places every table (the probe part groups too).
            // Among those, the one whose part holds the most tables: the
            // build gets the key-range passes, the probe part's dimensions
            // are held whole (TPC-H Q7 builds orders → customer → nation and
            // keeps supplier → nation beside the probe, not the reverse).
            // With row estimates, the largest table of those (it builds key
            // range by key range; what is held whole should be small): TPC-H
            // Q9 builds orders, not supplier, though supplier's part holds
            // more tables.
            let mut found: Option<star::Tree> = None;
            let mut any: Option<star::Tree> = None;
            let size = |t: &star::Tree| (est[t.b].unwrap_or(-1.0), t.build.iter().filter(|&&x| x).count());
            for b in (0..n).filter(|&b| b != p) {
                if let Some(t) = star::tree(n, p, b, &eqs) {
                    let slot = if gt.iter().all(|&g| g == p || t.build[g]) { &mut found } else { &mut any };
                    if slot.as_ref().is_none_or(|x| size(&t).partial_cmp(&size(x)) == Some(std::cmp::Ordering::Greater)) {
                        *slot = Some(t);
                    }
                }
            }
            let t = found.or(any).ok_or_else(|| {
                unsupported("this join: every table but the aggregated one must join by its first (key) column to a table already joined")
            })?;
            // B and P first; the dimensions after, in the order named.
            let mut order = vec![t.b, t.p];
            order.extend((0..n).filter(|&x| x != t.b && x != t.p));
            let at = |x: usize| order.iter().position(|&y| y == x).expect("every table");
            let mut taken: Vec<Option<Side>> = sides.into_iter().map(Some).collect();
            sides = order.iter().map(|&x| taken[x].take().expect("once")).collect();
            let rm = |c: star::Col| (at(c.0), c.1);
            eqs = eqs.into_iter().map(|(a, b)| (rm(a), rm(b))).collect();
            let mut parent = vec![None; n];
            let mut build = vec![false; n];
            for x in 0..n {
                parent[at(x)] = t.parent[x].map(|(u, k, uc)| (at(u), k, uc));
                build[at(x)] = t.build[x];
            }
            let mut parent2 = vec![None; n];
            for x in 0..n {
                parent2[at(x)] = t.parent2[x];
            }
            star_tree = Some(star::Tree {
                b: 0,
                p: 1,
                main: t.main,
                parent,
                build,
                checks: t.checks.into_iter().map(|(a, b)| (rm(a), rm(b))).collect(),
                cmps: cmps.iter().map(|&(a, b, o)| (rm(a), rm(b), o)).collect(),
                parent2,
                filters: t.filters.iter().map(|&(d, c, e)| (at(d), c, at(e))).collect(),
            });
        }
    }
    let mut keys = None;
    if n >= 2 {
        let (a, b) = match &star_tree {
            Some(t) => ((0, t.main.0), (1, t.main.1)),
            None => eqs[0],
        };
        let ka = sides[a.0].want(a.1);
        let kb = sides[b.0].want(b.1);
        let mut k = [(a.0, ka), (b.0, kb)];
        k.sort_by_key(|x| x.0);
        keys = Some(k);
    }

    // WHERE: each conjunct goes to the one side it names; a subquery
    // becomes a key-set filter of the table it names.
    let mut residual_exprs: Vec<Expr> = Vec::new();
    let mut orx_raw: Vec<Expr> = Vec::new();
    {
        for c in filters {
            let sub = match &c {
                Expr::Exists { subquery, negated } => Some((None, subquery, *negated)),
                Expr::InSubquery { expr, subquery, negated } => Some((Some(expr), subquery, *negated)),
                _ => None,
            };
            if let Some((outer_e, q, neg)) = sub {
                let no = |what: &str| unsupported(format!("the subquery in {c}: {what}"));
                let SetExpr::Select(ss) = q.body.as_ref() else { return Err(no("not a plain SELECT")) };
                let [f] = ss.from.as_slice() else { return Err(no("not one table")) };
                if !f.joins.is_empty() || ss.distinct.is_some() || q.limit.is_some() || q.order_by.is_some() || q.offset.is_some() || q.fetch.is_some() || !q.limit_by.is_empty() {
                    return Err(no("a join, DISTINCT, LIMIT, OFFSET, FETCH, LIMIT BY or ORDER BY"));
                }
                let mut inner = Vec::new();
                add(&f.relation, &mut inner)?;
                let mut ic = Vec::new();
                if let Some(w) = &ss.selection {
                    conjuncts(w, &mut ic);
                }
                // The correlation (EXISTS): inner.k = outer.k; the rest is
                // the inner table's own WHERE.
                let mut corr: Option<((usize, usize), usize)> = None;
                let mut ne: Option<((usize, usize), usize, star::Op)> = None;
                for x in ic {
                    // inner.c <> outer.c (or < > <= >=): decided per key by
                    // the inner rows' least and greatest c (`Semi::cmp`).
                    if let Expr::BinaryOp { left, op, right } = &x {
                        if let Some(o) = star::Op::of(op).filter(|o| *o != star::Op::Eq) {
                            let (li, ri) = (column_of(&inner, left).ok().flatten(), column_of(&inner, right).ok().flatten());
                            let (lo, ro) = (column_of(&sides, left).ok().flatten(), column_of(&sides, right).ok().flatten());
                            let hit = match (li, ri, lo, ro) {
                                (Some(i), None, _, Some(oc)) => Some((oc, i.1, o)),
                                (None, Some(i), Some(oc), _) => Some((oc, i.1, o.flip())),
                                _ => None,
                            };
                            if let Some(h) = hit {
                                if ne.replace(h).is_some() {
                                    return Err(no("more than one comparison with the outer table"));
                                }
                                continue;
                            }
                        }
                    }
                    // A subquery of the subquery's own table: its keys narrow
                    // the subquery's rows (a staged plan's narrowing).
                    if let Expr::InSubquery { expr: ie, subquery: nq, negated: nneg } = &x {
                        let SetExpr::Select(ns) = nq.body.as_ref() else { return Err(no("a nested subquery that is not a plain SELECT")) };
                        let [nf] = ns.from.as_slice() else { return Err(no("a nested subquery not over one table")) };
                        if !nf.joins.is_empty() || ns.having.is_some() || !matches!(&ns.group_by, GroupByExpr::Expressions(es, _) if es.is_empty()) {
                            return Err(no("a nested subquery with a join, GROUP BY or HAVING"));
                        }
                        let mut nsd = Vec::new();
                        add(&nf.relation, &mut nsd)?;
                        if let Some(w) = &ns.selection {
                            let mut nc = Vec::new();
                            conjuncts(w, &mut nc);
                            for y in nc {
                                if nsd[0].dialect == Dialect::Other {
                                    let pe = pexpr(&mut nsd, 0, &y)?;
                                    nsd[0].local.push(pe);
                                } else {
                                    let r = render_for(&nsd, 0, &y)?;
                                    nsd[0].conj.push(r);
                                    nsd[0].conj_src.push(y);
                                }
                            }
                        }
                        let [SelectItem::UnnamedExpr(pe) | SelectItem::ExprWithAlias { expr: pe, .. }] = ns.projection.as_slice() else {
                            return Err(no("a nested subquery not selecting one column"));
                        };
                        let nk = column_of(&nsd, pe)?.ok_or_else(|| no("a nested subquery selecting an expression"))?.1;
                        let oc = column_of(&inner, ie)?.ok_or_else(|| no("a nested IN of an expression"))?.1;
                        let key = nsd[0].want(nk);
                        let col = inner[0].want(oc);
                        let side = nsd.remove(0);
                        inner[0].semis.push(semi::Semi { col, anti: *nneg, not_in: *nneg, sub: semi::Sub { side, key, having: None }, set: Default::default(), cmp: None });
                        continue;
                    }
                    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = &x {
                        // A name the inner table lacks is the outer's.
                        let (li, ri) = (column_of(&inner, left).ok().flatten(), column_of(&inner, right).ok().flatten());
                        let (lo, ro) = (column_of(&sides, left).ok().flatten(), column_of(&sides, right).ok().flatten());
                        let hit = match (li, ri, lo, ro) {
                            (Some(i), None, _, Some(o)) => Some((o, i.1)),
                            (None, Some(i), Some(o), _) => Some((o, i.1)),
                            _ => None,
                        };
                        if let Some(h) = hit {
                            if corr.replace(h).is_some() {
                                return Err(no("more than one correlation"));
                            }
                            continue;
                        }
                    }
                    if inner[0].dialect == Dialect::Other {
                        let pe = pexpr(&mut inner, 0, &x)?;
                        inner[0].local.push(pe);
                    } else {
                        let r = render_for(&inner, 0, &x)?;
                        inner[0].conj.push(r);
                        inner[0].conj_src.push(x.clone());
                    }
                }
                let (outer, ikey) = match (outer_e, corr) {
                    (None, Some((o, i))) => (o, i),
                    (None, None) => return Err(no("EXISTS with no equality to the outer table")),
                    (Some(e), None) => {
                        let o = column_of(&sides, e)?.ok_or_else(|| no("IN of an expression"))?;
                        let [SelectItem::UnnamedExpr(ie) | SelectItem::ExprWithAlias { expr: ie, .. }] = ss.projection.as_slice() else {
                            return Err(no("IN of a subquery not selecting one column"));
                        };
                        let i = column_of(&inner, ie)?.ok_or_else(|| no("IN of a subquery selecting an expression"))?;
                        (o, i.1)
                    }
                    (Some(_), Some(_)) => return Err(no("a correlated IN")),
                };
                for (sd, k) in [(&sides[outer.0], outer.1), (&inner[0], ikey)] {
                    if !matches!(sd.schema[k].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64) {
                        return Err(no(&format!("{}.{} is not an integer key", sd.name, sd.schema[k].name)));
                    }
                }
                // GROUP BY the key HAVING agg op literal.
                let groups_by = group_list(&ss.group_by, &[])?;
                let having = match (&ss.having, groups_by.as_slice()) {
                    (None, []) => None,
                    (None, [g]) if column_of(&inner, g)?.is_some_and(|x| x.1 == ikey) => None,
                    (Some(Expr::BinaryOp { left, op, right }), [g]) if column_of(&inner, g)?.is_some_and(|x| x.1 == ikey) => {
                        let mut s0 = None;
                        let agg = aggregate(&mut inner, left, &mut s0)?.ok_or_else(|| no("HAVING not on an aggregate"))?;
                        let lit = match right.as_ref() {
                            Expr::Value(v) => match &v.value {
                                Value::Number(n, _) => number(n)?,
                                _ => return Err(no("HAVING against a non-number")),
                            },
                            _ => return Err(no("HAVING against an expression")),
                        };
                        if !matches!(op, BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Eq | BinaryOperator::NotEq) {
                            return Err(no("HAVING with that comparison"));
                        }
                        Some(semi::Having { agg, op: op.clone(), lit })
                    }
                    _ => return Err(no("a GROUP BY other than its key, or a HAVING other than aggregate-compares-number")),
                };
                let cmp = match ne {
                    None => None,
                    Some((oc, ic, op)) => {
                        if oc.0 != outer.0 || having.is_some() {
                            return Err(no("a comparison with a table other than the correlated one, or beside a HAVING"));
                        }
                        for (sd, k) in [(&sides[oc.0], oc.1), (&inner[0], ic)] {
                            if !matches!(sd.schema[k].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64) {
                                return Err(no(&format!("{}.{} compared with the outer table is not an integer", sd.name, sd.schema[k].name)));
                            }
                        }
                        Some((sides[oc.0].want(oc.1), inner[0].want(ic), op))
                    }
                };
                let key = inner[0].want(ikey);
                let col = sides[outer.0].want(outer.1);
                let side = inner.remove(0);
                sides[outer.0].semis.push(semi::Semi {
                    col,
                    anti: neg,
                    not_in: neg && outer_e.is_some(),
                    sub: semi::Sub { side, key, having },
                    set: Default::default(),
                    cmp,
                });
                continue;
            }
            let mut on = Vec::new();
            sides_of(&sides, &c, &mut on)?;
            match on.as_slice() {
                [s] if sides[*s].dialect == Dialect::Other => {
                    let pe = pexpr(&mut sides, *s, &c)?;
                    sides[*s].local.push(pe);
                }
                [s] => {
                    let r = render_for(&sides, *s, &c)?;
                    sides[*s].conj.push(r);
                    sides[*s].conj_src.push(c.clone());
                }
                [] => return Err(unsupported(format!("the WHERE condition {c}, which names no column"))),
                // An OR over the two tables of a join, each branch's
                // conditions one table's each: matched branch by branch.
                _ if n == 2 && star_tree.is_none() && {
                    let mut ds = Vec::new();
                    disjuncts(&c, &mut ds);
                    ds.len() > 1
                } => orx_raw.push(c.clone()),
                // Several tables: a filter of the groups, when every column
                // it names is grouped on (checked below).
                _ if star_tree.is_some() => residual_exprs.push(c.clone()),
                _ => return Err(unsupported(format!("the WHERE condition {c}, which names more than one table"))),
            }
        }
    }

    // GROUP BY: columns, by name or by SELECT ordinal.
    let group_exprs = gby.clone();
    let mut group_cols: Vec<(usize, usize)> = Vec::new();
    for e in &group_exprs {
        let mut c = column_of(&sides, e)?.ok_or_else(|| unsupported(format!("GROUP BY on the expression {e}")))?;
        if let Some(t) = &star_tree {
            if c.0 == 1 {
                // P's join key stands for B's (Q3's l_orderkey = o_orderkey);
                // P's other columns group at the probe.
                if c.1 == t.main.1 {
                    c = (0, t.main.0);
                }
            }
        }
        group_cols.push(c);
    }

    // P's join key stands for B's in a star plan, wherever it is named.
    let subst = |c: (usize, usize)| match &star_tree {
        Some(t) if c.0 == 1 && c.1 == t.main.1 => (0, t.main.0),
        _ => c,
    };
    // SELECT: group columns and aggregates.
    let mut agg_side = None;
    let mut aggs = Vec::new();
    let mut out = Vec::new();
    for (e, alias) in &items {
        let name = alias.clone().unwrap_or_else(|| e.to_string());
        if let Some(a) = aggregate(&mut sides, e, &mut agg_side)? {
            aggs.push(a);
            out.push((name, OutCol::Agg(aggs.len() - 1)));
            continue;
        }
        let col = column_of(&sides, e)?;
        if col.is_none() && matches!(e, Expr::BinaryOp { .. } | Expr::Nested(_) | Expr::UnaryOp { .. }) {
            let o = oexpr(&mut sides, e, &mut agg_side, &mut aggs)?;
            out.push((name, OutCol::Expr(o)));
            continue;
        }
        let c = subst(col.ok_or_else(|| unsupported(format!("the SELECT expression {e}")))?);
        let gi = group_cols
            .iter()
            .position(|g| *g == c)
            .ok_or_else(|| bad(format!("{e} is neither aggregated nor in GROUP BY")))?;
        let name = alias.clone().unwrap_or_else(|| sides[c.0].schema[c.1].name.clone());
        out.push((name, OutCol::Group(gi)));
    }
    if aggs.is_empty() {
        return Err(unsupported("a query without aggregates (v1 runs join + aggregate)"));
    }
    // HAVING: a conjunction of `aggregate op number`, filtered on the final
    // groups (`order_rows`) — a staged plan's grouping keeps only the groups
    // that pass (TPC-H Q11 10g seed: 2M groups, a few thousand kept).
    let mut having = Vec::new();
    if let Some(h) = &sel.having {
        let mut hc = Vec::new();
        conjuncts(h, &mut hc);
        for c in hc {
            let no = || unsupported(format!("the HAVING condition {c} (v1 takes aggregate op number)"));
            let Expr::BinaryOp { left, op, right } = &c else { return Err(no()) };
            let o = star::Op::of(op).ok_or_else(no)?;
            let (ae, le, o) = match (number_lit(left), number_lit(right)) {
                (None, Some(v)) => (left, v, o),
                (Some(v), None) => (right, v, o.flip()),
                _ => return Err(no()),
            };
            // The SELECT's own aggregate when it is the same (Q11's
            // `value`): one accumulator a group, not two.
            let same = items.iter().zip(&out).find_map(|((e, _), (_, oc))| match oc {
                OutCol::Agg(i) if e.to_string() == ae.to_string() => Some(*i),
                _ => None,
            });
            let i = match same {
                Some(i) => i,
                None => {
                    let a = aggregate(&mut sides, ae, &mut agg_side)?.ok_or_else(no)?;
                    aggs.push(a);
                    aggs.len() - 1
                }
            };
            having.push((i, o, le));
        }
    }
    // Each GROUP BY column's collation (before a star layout reorders sides).
    let gcoll: Vec<Coll> = group_cols.iter().map(|&(s, c)| Coll::of(&sides[s].schema[c])).collect();
    let group_side = match (group_cols.first(), &star_tree) {
        (_, Some(_)) => 0,
        (Some(g), None) => g.0,
        (None, None) => {
            // A global aggregate: the build side is the one without the
            // aggregate arguments (COUNT(*) alone reads from the first).
            match (keys, agg_side) {
                (Some(_), Some(a)) => 1 - a,
                _ => 0,
            }
        }
    };
    if star_tree.is_none() && group_cols.iter().any(|g| g.0 != group_side) {
        return Err(unsupported("GROUP BY columns from both tables"));
    }
    if star_tree.is_some() && agg_side.is_some_and(|a| a != 1) {
        return Err(unsupported("aggregates over a joined dimension table (the aggregated table must be the one the rest hang on)"));
    }
    let agg_side = agg_side.unwrap_or(if keys.is_some() { 1 - group_side } else { group_side });
    if keys.is_some() && agg_side == group_side {
        return Err(unsupported(
            "aggregating the GROUP BY table's own columns across a join (v1 aggregates the other table)",
        ));
    }

    let mut order = Vec::new();
    if let Some(ob) = &q.order_by {
        let OrderByKind::Expressions(es) = &ob.kind else {
            return Err(unsupported("ORDER BY ALL"));
        };
        for o in es {
            let i = match &o.expr {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<usize>().ok().and_then(|i| i.checked_sub(1)),
                    _ => None,
                },
                e => {
                    let s = e.to_string();
                    out.iter().position(|(n, _)| n.eq_ignore_ascii_case(&s)).or_else(|| {
                        column_of(&sides, e).ok().flatten().map(subst).and_then(|c| {
                            out.iter().position(|(_, oc)| matches!(oc, OutCol::Group(g) if group_cols[*g] == c))
                        })
                    })
                }
            }
            .filter(|&i| i < out.len())
            .ok_or_else(|| unsupported(format!("ORDER BY {} (name an output column or its position)", o.expr)))?;
            order.push((i, o.options.asc != Some(false)));
        }
    }
    let limit = match &q.limit {
        None => None,
        Some(Expr::Value(v)) => match &v.value {
            Value::Number(n, _) => Some(n.parse::<usize>().map_err(|_| bad(format!("LIMIT {n}")))?),
            _ => return Err(unsupported("a LIMIT that is not a number")),
        },
        Some(other) => return Err(unsupported(format!("LIMIT {other}"))),
    };
    // An OR over the build and probe tables: implied filters to each server
    // (the OR of its own branches' conditions, when every branch has one),
    // the build's conditions as flag bits, the probe's per row.
    let mut orx = Vec::new();
    if let Some(c) = orx_raw.first() {
        if orx_raw.len() > 1 {
            return Err(unsupported("more than one OR over both tables"));
        }
        let (bs, ps) = (group_side, agg_side);
        let mut ds = Vec::new();
        disjuncts(c, &mut ds);
        let mut per: Vec<(Vec<Expr>, Vec<Expr>)> = Vec::new();
        for d in &ds {
            let mut cs = Vec::new();
            conjuncts(d, &mut cs);
            let (mut b, mut pp) = (Vec::new(), Vec::new());
            for x in cs {
                let mut on = Vec::new();
                sides_of(&sides, &x, &mut on)?;
                match on.as_slice() {
                    [s1] if *s1 == bs => b.push(x),
                    [s1] if *s1 == ps => pp.push(x),
                    _ => return Err(unsupported(format!("the WHERE condition {c}: a branch's condition {x} names both tables"))),
                }
            }
            per.push((b, pp));
        }
        for (s0, pick) in [(bs, 0usize), (ps, 1)] {
            let all: Option<Vec<Expr>> = per.iter().map(|x| {
                let v = if pick == 0 { &x.0 } else { &x.1 };
                (!v.is_empty()).then(|| and_of(v.clone()))
            }).collect();
            if let Some(all) = all {
                let e = or_of(all);
                if sides[s0].dialect == Dialect::Other {
                    let pe = pexpr(&mut sides, s0, &e)?;
                    sides[s0].local.push(pe);
                } else {
                    let r = render_for(&sides, s0, &e)?;
                    sides[s0].conj.push(r);
                    sides[s0].conj_src.push(e);
                }
            }
        }
        for (b, pp) in per {
            let bit = if b.is_empty() {
                None
            } else {
                let pe = pexpr(&mut sides, bs, &and_of(b))?;
                let bit = u32::try_from(sides[bs].flags.len()).ok().filter(|&x| x < 64).ok_or_else(|| unsupported("more than 64 OR branches"))?;
                sides[bs].flags.push(pe);
                Some(bit)
            };
            let probe = if pp.is_empty() { None } else { Some(pexpr(&mut sides, ps, &and_of(pp))?) };
            orx.push((bit, probe));
        }
    }
    // Conditions over several tables: on the GROUP BY columns only.
    let residual = if residual_exprs.is_empty() {
        None
    } else {
        let mut gside = Side {
            name: "__groups".into(),
            url: String::new(),
            table: String::new(),
            dialect: Dialect::Other,
            schema: group_cols
                .iter()
                .enumerate()
                .map(|(i, &(s, c))| ArrowField { name: format!("__g{i}"), kind: sides[s].schema[c].kind, nullable: true, rewritten: false, padded: sides[s].schema[c].padded, ci: sides[s].schema[c].ci, ai: sides[s].schema[c].ai })
                .collect(),
            read: Vec::new(),
            conj: Vec::new(),
            conj_src: Vec::new(),
            local: Vec::new(),
            computed: Vec::new(),
            whole: false,
            semis: Vec::new(),
            flags: Vec::new(),
            virt: Vec::new(),
            lookups: Vec::new(),
        };
        let mut preds = Vec::new();
        for e in &residual_exprs {
            let mut e2 = e.clone();
            let mut miss = None;
            let _ = sqlparser::ast::visit_expressions_mut(&mut e2, |x| {
                if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    match column_of(&sides, x).ok().flatten().map(subst).and_then(|c| group_cols.iter().position(|g| *g == c)) {
                        Some(i) => *x = Expr::Identifier(Ident::new(format!("__g{i}"))),
                        None => miss = Some(x.to_string()),
                    }
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            if let Some(m) = miss {
                return Err(unsupported(format!("the WHERE condition {e}: it names several tables and {m}, which is not grouped on")));
            }
            preds.push(pexpr(std::slice::from_mut(&mut gside), 0, &e2)?);
        }
        let tys = gside.read.iter().map(|&c| kind_ty(gside.schema[c].kind)).collect();
        Some(Residual { preds, read: gside.read.clone(), tys })
    };
    // A CASE over the other table conditions on the build table only.
    for (i, sd) in sides.iter().enumerate() {
        // (A star plan's dimension may hold them too; its layout checks it
        // is one the probe reaches.)
        if !sd.flags.is_empty() && i != group_side && !(star_tree.is_some() && i >= 2) {
            return Err(unsupported(format!(
                "a CASE whose conditions name {} while it is not the GROUP BY (build) table",
                sd.name
            )));
        }
    }
    // Last: the star layout moves the dimensions out of `sides`.
    let (groups, star) = match &star_tree {
        None => (group_cols.iter().map(|&(s, c)| sides[s].want(c)).collect::<Vec<usize>>(), None),
        Some(t) => {
            // A GROUP BY column an ORDER BY before a LIMIT, or a condition
            // over the groups, needs is read with its rows; the rest of a
            // dimension's may be read late.
            let mut ordered: Vec<usize> = order.iter().filter_map(|&(i, _)| match out[i].1 {
                OutCol::Group(g) => Some(g),
                OutCol::Agg(_) | OutCol::Expr(_) => None,
            }).collect();
            if let Some(r) = &residual {
                ordered.extend(r.read.iter().copied());
            }
            let may_defer = |g: usize| limit.is_none() || !ordered.contains(&g);
            let st = star::layout(&mut sides, t, &group_cols, &may_defer)?;
            (Vec::new(), Some(st))
        }
    };
    let key_groups = keys.is_some_and(|k| {
        let (bs, bk) = k.into_iter().find(|x| x.0 == group_side).expect("keyed");
        let bcol = sides[bs].read[bk];
        group_cols.iter().any(|&(s, c)| s == bs && c == bcol)
    });
    Ok(Plan { sides, keys, group_side, groups, agg_side, aggs, out, order, limit, star, key_groups, residual, orx, gcoll, having })
}

/// `FROM (SELECT exprs AS a, … FROM … WHERE …) AS t` with no GROUP BY,
/// HAVING, DISTINCT, ORDER BY or LIMIT inside, as one SELECT: the outer
/// SELECT, WHERE and GROUP BY take the inner expressions for the derived
/// columns (an outer item keeps its name as an alias), the WHEREs are ANDed.
fn flatten(sel: &sqlparser::ast::Select) -> Result<Option<sqlparser::ast::Select>> {
    let [f] = sel.from.as_slice() else { return Ok(None) };
    let TableFactor::Derived { lateral, subquery, alias } = &f.relation else { return Ok(None) };
    let no = |w: &str| unsupported(format!("a derived table in FROM with {w}"));
    if *lateral || !f.joins.is_empty() {
        return Err(no("LATERAL or a join beside it"));
    }
    if subquery.with.is_some() || subquery.order_by.is_some() || subquery.limit.is_some() || subquery.offset.is_some() || subquery.fetch.is_some() || !subquery.limit_by.is_empty() {
        return Err(no("WITH, ORDER BY, LIMIT, OFFSET, FETCH or LIMIT BY"));
    }
    let SetExpr::Select(inner) = subquery.body.as_ref() else { return Err(no("a set operation")) };
    let grouped = !matches!(&inner.group_by, GroupByExpr::Expressions(es, _) if es.is_empty());
    if grouped || inner.having.is_some() || inner.distinct.is_some() {
        return Err(no("GROUP BY, HAVING or DISTINCT inside"));
    }
    let tname = alias.as_ref().map(|a| a.name.value.to_ascii_lowercase());
    let mut map: Vec<(String, Expr)> = Vec::new();
    for it in &inner.projection {
        let (name, e) = match it {
            SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr.clone()),
            SelectItem::UnnamedExpr(e @ Expr::Identifier(i)) => (i.value.clone(), e.clone()),
            SelectItem::UnnamedExpr(e @ Expr::CompoundIdentifier(p)) => (p.last().map(|x| x.value.clone()).unwrap_or_default(), e.clone()),
            _ => return Err(no("a column that is neither named nor a column")),
        };
        map.push((name.to_ascii_lowercase(), e));
    }
    let look = |name: &str| map.iter().find(|m| m.0 == name.to_ascii_lowercase()).map(|m| m.1.clone());
    let rewrite = |e: &Expr| -> Expr {
        let mut e = e.clone();
        let _ = sqlparser::ast::visit_expressions_mut(&mut e, |x| {
            let hit = match x {
                Expr::Identifier(i) => look(&i.value),
                Expr::CompoundIdentifier(p) if p.len() == 2 && tname.as_deref() == Some(&p[0].value.to_ascii_lowercase()) => look(&p[1].value),
                _ => None,
            };
            if let Some(h) = hit {
                *x = Expr::Nested(Box::new(h));
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
        e
    };
    let mut out = (**inner).clone();
    out.projection = sel
        .projection
        .iter()
        .map(|it| match it {
            SelectItem::UnnamedExpr(e @ Expr::Identifier(i)) => SelectItem::ExprWithAlias { expr: rewrite(e), alias: i.clone() },
            SelectItem::UnnamedExpr(e) => SelectItem::UnnamedExpr(rewrite(e)),
            SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias { expr: rewrite(expr), alias: alias.clone() },
            other => other.clone(),
        })
        .collect();
    out.selection = match (&inner.selection, &sel.selection) {
        (a, None) => a.clone(),
        (None, Some(b)) => Some(rewrite(b)),
        (Some(a), Some(b)) => Some(Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(a.clone()))), op: BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(rewrite(b)))) }),
    };
    out.group_by = match &sel.group_by {
        GroupByExpr::Expressions(es, m) => GroupByExpr::Expressions(es.iter().map(|e| rewrite(e)).collect(), m.clone()),
        other => other.clone(),
    };
    out.having = sel.having.as_ref().map(|h| rewrite(h));
    Ok(Some(out))
}

/// `EXTRACT(YEAR FROM col)` of a date or timestamp column → a virtual
/// column of col's table (computed as its rows arrive), named by col.
fn years_to_virtual(sides: &mut [Side], e: &mut Expr) -> Result<()> {
    let mut err = None;
    let _ = sqlparser::ast::visit_expressions_mut(e, |x| {
        // SUBSTRING(col FROM a FOR b), a and b integer literals: a virtual
        // text column of col's table (TPC-H Q22's country code).
        if let Expr::Substring { expr, substring_from, substring_for, .. } = x {
            let int = |e: &Option<Box<Expr>>| -> std::result::Result<Option<i64>, ()> {
                match e.as_deref() {
                    None => Ok(None),
                    Some(e) => match lit(e) {
                        Some(Lit::Num(v)) if v.fract() == 0.0 && v.abs() < 1e9 => Ok(Some(v as i64)),
                        _ => Err(()),
                    },
                }
            };
            let (Ok(from), Ok(len)) = (int(substring_from), int(substring_for)) else {
                err = Some(unsupported("SUBSTRING with a start or length that is not an integer literal"));
                return std::ops::ControlFlow::Break(());
            };
            let c = match column_of(sides, expr) {
                Ok(Some(c)) => c,
                Ok(None) => {
                    err = Some(unsupported("SUBSTRING of an expression (a column only)"));
                    return std::ops::ControlFlow::Break(());
                }
                Err(e) => {
                    err = Some(e);
                    return std::ops::ControlFlow::Break(());
                }
            };
            let side = &mut sides[c.0];
            let base = side.schema[c.1].clone();
            if base.kind != ArrowKind::Utf8 {
                err = Some(unsupported(format!("SUBSTRING of the {:?} column {}", base.kind, base.name)));
                return std::ops::ControlFlow::Break(());
            }
            let from = from.unwrap_or(1);
            let name = format!("__substr_{}_{}_{}", base.name, from, len.map_or("end".to_string(), |l| l.to_string()));
            if !side.schema.iter().any(|f| f.name == name) {
                side.schema.push(ArrowField { name: name.clone(), kind: ArrowKind::Utf8, nullable: true, rewritten: false, padded: base.padded, ci: base.ci, ai: base.ai });
                let vi = side.schema.len() - 1;
                side.virt.push((vi, c.1, Virt::Substr(from, len)));
                side.want(c.1);
            }
            *x = Expr::CompoundIdentifier(vec![Ident::new(side.name.clone()), Ident::new(name)]);
            return std::ops::ControlFlow::Continue(());
        }
        if let Expr::Extract { field, expr, .. } = x {
            if !matches!(field, sqlparser::ast::DateTimeField::Year) {
                err = Some(unsupported(format!("EXTRACT({field}) (YEAR only)")));
                return std::ops::ControlFlow::Break(());
            }
            let c = match column_of(sides, expr) {
                Ok(Some(c)) => c,
                Ok(None) => {
                    err = Some(unsupported("EXTRACT of an expression (a column only)"));
                    return std::ops::ControlFlow::Break(());
                }
                Err(e) => {
                    err = Some(e);
                    return std::ops::ControlFlow::Break(());
                }
            };
            let side = &mut sides[c.0];
            if !matches!(side.schema[c.1].kind, ArrowKind::Date32 | ArrowKind::TimestampUtc | ArrowKind::TimestampNaive) {
                err = Some(unsupported(format!("EXTRACT(YEAR FROM {}) of a {:?} column", side.schema[c.1].name, side.schema[c.1].kind)));
                return std::ops::ControlFlow::Break(());
            }
            let name = format!("__year_{}", side.schema[c.1].name);
            if !side.schema.iter().any(|f| f.name == name) {
                side.schema.push(ArrowField { name: name.clone(), kind: ArrowKind::Int32, nullable: true, rewritten: false, padded: false, ci: false, ai: false });
                let vi = side.schema.len() - 1;
                side.virt.push((vi, c.1, Virt::Year));
                side.want(c.1);
            }
            *x = Expr::CompoundIdentifier(vec![Ident::new(side.name.clone()), Ident::new(name)]);
        }
        std::ops::ControlFlow::Continue(())
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// One table's filters an OR over several tables implies (what DuckDB and
/// Postgres derive too): when every disjunct holds conjuncts of table t
/// alone, t's rows must pass the OR of those, whatever the other tables
/// hold. TPC-H Q7's `(n1 = FRANCE AND n2 = GERMANY) OR (n1 = GERMANY AND
/// n2 = FRANCE)` implies `n1 IN (FRANCE, GERMANY)` and the same of n2, so
/// 2 of 25 nations' suppliers and customers join, not all of them. The OR
/// itself still decides each row: an implied filter only drops rows it
/// would drop (a row it rejects fails every disjunct), and it is an ordinary
/// filter of one table, pushed to that table's server like any other.
fn implied_filters(sides: &[Side], pool: &[Expr]) -> Result<Vec<Expr>> {
    let mut out = Vec::new();
    for c in pool {
        let mut ds = Vec::new();
        disjuncts(c, &mut ds);
        if ds.len() < 2 {
            continue;
        }
        let mut all = Vec::new();
        sides_of(sides, c, &mut all)?;
        if all.len() < 2 {
            continue;
        }
        for &t in &all {
            let mut arms = Vec::new();
            for d in &ds {
                let mut cs = Vec::new();
                conjuncts(d, &mut cs);
                let mut mine = Vec::new();
                for x in cs {
                    let mut o = Vec::new();
                    sides_of(sides, &x, &mut o)?;
                    if o == [t] {
                        mine.push(x);
                    }
                }
                if mine.is_empty() {
                    break;
                }
                arms.push(and_of(mine));
            }
            if arms.len() == ds.len() {
                out.push(or_of(arms));
            }
        }
    }
    Ok(out)
}

/// The disjuncts of an OR (an OR inside parentheses too).
fn disjuncts(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
            disjuncts(left, out);
            disjuncts(right, out);
        }
        Expr::Nested(x) if matches!(x.as_ref(), Expr::BinaryOp { op: BinaryOperator::Or, .. }) => disjuncts(x, out),
        other => out.push(other.clone()),
    }
}

fn and_of(mut v: Vec<Expr>) -> Expr {
    let first = v.remove(0);
    v.into_iter().fold(first, |a, b| Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(a))), op: BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(b))) })
}

fn or_of(mut v: Vec<Expr>) -> Expr {
    let first = v.remove(0);
    v.into_iter().fold(first, |a, b| Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(a))), op: BinaryOperator::Or, right: Box::new(Expr::Nested(Box::new(b))) })
}

/// The GROUP BY expressions, an ordinal read as the SELECT item it names.
fn group_list(g: &GroupByExpr, items: &[(Expr, Option<String>)]) -> Result<Vec<Expr>> {
    let es = match g {
        GroupByExpr::Expressions(es, mods) if mods.is_empty() => es,
        GroupByExpr::Expressions(..) | GroupByExpr::All(..) => return Err(unsupported("GROUP BY ALL or GROUP BY modifiers")),
    };
    es.iter()
        .map(|g| match g {
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => {
                    let i: usize = n.parse().map_err(|_| bad(format!("GROUP BY {n}")))?;
                    items.get(i.wrapping_sub(1)).map(|x| x.0.clone()).ok_or_else(|| bad(format!("GROUP BY {i} is past the SELECT list")))
                }
                _ => Err(unsupported(format!("GROUP BY {g}"))),
            },
            // A SELECT alias names its item (`GROUP BY y` of `… AS y`).
            Expr::Identifier(i) => Ok(items
                .iter()
                .find(|(_, a)| a.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(&i.value)))
                .map(|(e, _)| e.clone())
                .unwrap_or_else(|| g.clone())),
            other => Ok(other.clone()),
        })
        .collect()
}

fn quote(d: Dialect, name: &str) -> String {
    name.split('.')
        .map(|p| match d {
            Dialect::Pg => format!("\"{}\"", p.replace('"', "\"\"")),
            Dialect::My | Dialect::Other => format!("`{}`", p.replace('`', "``")),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Unfiltered MIN/MAX of a side's join key, asked of its own server.
/// What a side's key range is known to be.
#[derive(Clone, Copy, Debug)]
enum Kr {
    Empty,
    Known(i64, i64),
    /// A source with no SQL server: nothing to ask.
    Unknown,
}

fn intersect(a: Kr, b: Kr) -> Kr {
    match (a, b) {
        (Kr::Empty, _) | (_, Kr::Empty) => Kr::Empty,
        (Kr::Known(a0, a1), Kr::Known(b0, b1)) => {
            if a0.max(b0) > a1.min(b1) {
                Kr::Empty
            } else {
                Kr::Known(a0.max(b0), a1.min(b1))
            }
        }
        (Kr::Known(x, y), Kr::Unknown) | (Kr::Unknown, Kr::Known(x, y)) => Kr::Known(x, y),
        (Kr::Unknown, Kr::Unknown) => Kr::Unknown,
    }
}

/// Whether an index leads with `col` on the side's table (its MIN/MAX is
/// then a lookup, not a scan). Unknown is `false`.
pub(crate) async fn key_indexed(side: &Side, col: &str) -> bool {
    let t = side.table.rsplit('.').next().unwrap_or(&side.table).trim_matches(|c| c == '"' || c == '`').to_string();
    match side.dialect {
        Dialect::Pg => {
            let Ok(src) = crate::source::postgres::PgSource::connect(&side.url, 1).await else { return false };
            sqlx::query_scalar::<_, bool>(
                // A valid, whole-table btree led by the column: what a range
                // read can use (not a partial, invalid or hash index).
                "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid \
                   AND a.attnum = i.indkey[0] JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am m ON m.oid = c.relam \
                   WHERE i.indrelid = $1::regclass AND a.attname = $2 AND i.indisvalid AND i.indpred IS NULL \
                   AND m.amname = 'btree')",
            )
            .bind(quote(Dialect::Pg, &side.table))
            .bind(col)
            .fetch_one(src.pool())
            .await
            .unwrap_or(false)
        }
        Dialect::My => {
            let Ok(src) = crate::source::mysql::MySqlSource::connect(&side.url, 1).await else { return false };
            sqlx::query_scalar::<_, i64>(
                // A whole-column BTREE led by it (not a prefix or a hash one).
                "SELECT COUNT(*) FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? AND SEQ_IN_INDEX = 1 \
                 AND SUB_PART IS NULL AND INDEX_TYPE = 'BTREE'",
            )
            .bind(&t)
            .bind(col)
            .fetch_one(src.pool())
            .await
            .is_ok_and(|n| n > 0)
        }
        Dialect::Other => false,
    }
}

async fn key_range(side: &Side, col: &str) -> Result<Kr> {
    Ok(key_range_of(side.dialect, side.name.clone(), side.url.clone(), side.table.clone(), col.to_string()).await?.0)
}

/// [`key_range`] on owned names, so it can run as its own task, with the
/// per-page-range answers when the range was asked in parts.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn key_range_of(dialect: Dialect, name: String, url: String, table: String, col: String) -> Result<(Kr, Vec<Part>)> {
    let mut parts_out: Vec<Part> = Vec::new();
    struct S<'a> {
        dialect: Dialect,
        name: &'a str,
        url: &'a str,
        table: &'a str,
    }
    let side = S { dialect, name: &name, url: &url, table: &table };
    let col = col.as_str();
    if side.dialect == Dialect::Other {
        return Ok((Kr::Unknown, Vec::new()));
    }
    let sql = format!(
        "SELECT CAST(MIN({c}) AS {t}), CAST(MAX({c}) AS {t}) FROM {tb}",
        c = quote(side.dialect, col),
        t = if side.dialect == Dialect::Pg { "BIGINT" } else { "SIGNED" },
        tb = quote(side.dialect, &side.table)
    );
    let row: (Option<i64>, Option<i64>) = match side.dialect {
        Dialect::Pg => {
            let src = crate::source::postgres::PgSource::connect(&side.url, KEY_RANGE_PARTS).await?;
            let pool = src.pool();
            // A key no index leads is a full scan for MIN/MAX: it runs as
            // page ranges on several backends at once (TID range scans,
            // PostgreSQL 14+), each answering its own part.
            let tb = quote(Dialect::Pg, &side.table);
            let probe: Option<(bool, i32, i64)> = sqlx::query_as(
                "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid \
                   AND a.attnum = i.indkey[0] WHERE i.indrelid = $1::regclass AND a.attname = $2), \
                 current_setting('server_version_num')::int4, \
                 (pg_relation_size($1::regclass) / current_setting('block_size')::int8)::int8",
            )
            .bind(&tb)
            .bind(col)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
            match probe {
                Some((false, ver, pages)) if ver >= 140_000 && pages >= 1024 => {
                    let n = KEY_RANGE_PARTS as i64;
                    let bounds: Vec<(i64, Option<i64>)> = (0..n).map(|i| (pages * i / n, (i < n - 1).then(|| pages * (i + 1) / n))).collect();
                    let parts = bounds.iter().map(|&(a, b)| {
                        let upper = b.map(|b| format!(" AND ctid < '({b},0)'::tid")).unwrap_or_default();
                        let q = format!("{sql} WHERE ctid >= '({a},0)'::tid{upper}");
                        let src = &src;
                        async move { src.fetch_one_pinned::<(Option<i64>, Option<i64>)>(&q).await }
                    });
                    futures::future::try_join_all(parts).await.map(|rs| {
                        for (&pg, &(a, b)) in bounds.iter().zip(&rs) {
                            parts_out.push(Part { pages: pg, keys: a.zip(b) });
                        }
                        rs.into_iter().fold((None, None), |(lo, hi): (Option<i64>, Option<i64>), (a, b)| {
                            (lo.into_iter().chain(a).min(), hi.into_iter().chain(b).max())
                        })
                    })
                }
                _ => src.fetch_one_pinned(&sql).await,
            }
        }
        Dialect::My => {
            let src = crate::source::mysql::MySqlSource::connect(&side.url, 1).await?;
            sqlx::query_as(&sql).fetch_one(src.pool()).await
        }
        Dialect::Other => unreachable!("answered above"),
    }
    .map_err(|e| Error::Transfer(format!("sql: key range of {}: {e}", side.name)))?;
    Ok((
        match row {
            (Some(a), Some(b)) => Kr::Known(a, b),
            _ => Kr::Empty,
        },
        parts_out,
    ))
}

/// The bitmap over keys [a, b] of a direct array based at `base` (a key past
/// its end is absent): eight slots per byte, in one pass over the slice.
fn bits_direct(arr: &[u8], base: i64, a: i64, b: i64) -> Vec<u8> {
    let n = (b as i128 - a as i128 + 1) as usize;
    let mut bits = vec![0u8; n.div_ceil(8)];
    let off = a as i128 - base as i128;
    for (j, byte) in bits.iter_mut().enumerate() {
        let mut v = 0u8;
        for t in 0..8 {
            let i = j * 8 + t;
            let at = off + i as i128;
            if i < n && at >= 0 && (at as usize) < arr.len() && arr[at as usize] != u8::MAX {
                v |= 1 << t;
            }
        }
        *byte = v;
    }
    bits
}

/// The bitmap over keys [a, b] of the keys `has` holds.
fn bits_of(a: i64, b: i64, has: impl Fn(i64) -> bool) -> Vec<u8> {
    let span = (b as i128 - a as i128 + 1) as usize;
    let mut bits = vec![0u8; span.div_ceil(8)];
    for i in 0..span {
        if has(a + i as i64) {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    bits
}

/// The runtime key filter over keys [a, b], from its bitmap.
fn key_filter_sql(col: &str, a: i64, b: i64, bits: &[u8]) -> String {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bits);
    let q = quote(Dialect::Pg, col);
    format!("CASE WHEN {q} BETWEEN {a} AND {b} THEN get_bit(decode('{b64}', 'base64'), ({q} - ({a}))::int4) = 1 ELSE false END")
}

/// A batch's exact decimal aggregate arguments, each evaluated a column at a
/// time (`eval_exact`) instead of a tree walk per row; `None` for the rest,
/// which go the row way. A column that errs (an overflow, maybe in a row the
/// filter or the join drops) is left to the row way too, so a query fails
/// exactly where it did.
type ArgCol = Option<(Vec<i128>, Vec<bool>)>;

fn arg_cols(p: &Plan, b: &ArrowBatch) -> Vec<ArgCol> {
    p.aggs
        .iter()
        .map(|a| match (&a.arg, a.ty) {
            (Some(e), Some(Ty::Dec(_))) => eval_exact(e, b).ok().flatten(),
            _ => None,
        })
        .collect()
}

/// Aggregate `a`'s argument at row `r`, off its batch column when it has one.
#[inline]
fn arg_at(a: &Agg, col: &ArgCol, b: &ArrowBatch, r: usize) -> Result<Num> {
    match col {
        Some((v, ok)) => Ok(if ok[r] { Num::Dec(v[r]) } else { Num::Null }),
        None => a.arg.as_ref().map_or(Ok(Num::Null), |e| eval(e, b, r)),
    }
}

/// Fold one probe batch into the accumulators through `look`.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn fold_probe<'m>(
    p: &Plan,
    ptys: &[Option<Ty>],
    pkey: usize,
    b: &mut ArrowBatch,
    look: impl Fn(i64) -> Look<'m>,
    ngroups: usize,
    accs: &mut Vec<Acc>,
    hit: &mut Vec<bool>,
    sx: Option<(&star::Star, &[star::DimMap], &[Vec<(u8, Vec<u8>)>])>,
    gflags: Option<&[u64]>,
    mut cg: Option<&mut Groups>,
) -> Result<u64> {
    let na = p.aggs.len();
    let ps = p.agg_side;
    let pgroup = sx.is_some_and(|x| !x.0.pgroup.is_empty());
    let mut ck = Vec::new();
    // Sized by the build's groups, or (probe-side GROUP BY) grown per
    // combined group below.
    if !pgroup {
        if accs.len() < ngroups * na {
            accs.resize(ngroups * na, Acc::None);
        }
        if hit.len() < ngroups {
            hit.resize(ngroups, false);
        }
    }
    let mut matched = 0u64;
    let mut pv = Vec::new();
    // The rows the fold takes, in one pass: past the local conjuncts, the
    // build's keys (one load, before any dimension is looked at) and a star
    // plan's dimensions. Each taken row's dimension fields are kept (`pvs`,
    // at `pvo`), so no row is looked up twice: the lookup columns and the
    // fold below read them (TPC-H Q8/Q9 looked every row of 60M up once per
    // lookup column and again in the fold).
    let mut sel: Vec<u32> = Vec::new();
    let mut pvs: Vec<u8> = Vec::new();
    let mut pvo: Vec<u32> = vec![0];
    {
        let b: &ArrowBatch = b;
        for r in 0..b.rows {
            if !local_ok(&p.sides[ps], ptys, b, r) {
                continue;
            }
            let Some(key) = key_at(&b.cols[pkey], r)? else { continue };
            if matches!(look(key), Look::Miss) {
                continue;
            }
            if let Some((st, maps, _)) = sx {
                if !star::probe_row(st, maps, b, r, &mut pv)? {
                    continue;
                }
                pvs.extend_from_slice(&pv);
                pvo.push(u32::try_from(pvs.len()).map_err(|_| bad("a probe batch's dimension fields past 4 GiB"))?);
            }
            sel.push(r as u32);
        }
    }
    let pv_of = |i: usize| -> &[u8] {
        if pvo.len() > 1 {
            &pvs[pvo[i] as usize..pvo[i + 1] as usize]
        } else {
            &[]
        }
    };
    // Dimensions' values the aggregates read: into the batch's lookup
    // columns, for the taken rows (in place; no row allocates).
    if let Some((st, _, _)) = sx.filter(|x| !x.0.plookups.is_empty()) {
        let n = b.rows;
        for &(at, pi, kind) in &st.plookups {
            let mut valid = vec![false; n];
            let mut i128s = Vec::new();
            let mut f64s = Vec::new();
            let mut i64s = Vec::new();
            match kind {
                ArrowKind::Decimal { .. } => i128s = vec![0i128; n],
                ArrowKind::Float32 | ArrowKind::Float64 => f64s = vec![0f64; n],
                ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64 => i64s = vec![0i64; n],
                k => return Err(unsupported(format!("an aggregate reading a dimension's {k:?} column"))),
            }
            for (i, &r) in sel.iter().enumerate() {
                let r = r as usize;
                let Some((t, v)) = nth_field(pv_of(i), pi) else { continue };
                if t == 0 {
                    continue;
                }
                valid[r] = true;
                match kind {
                    ArrowKind::Decimal { .. } => i128s[r] = i128::from_le_bytes(v.try_into().unwrap_or([0; 16])),
                    ArrowKind::Float32 | ArrowKind::Float64 => f64s[r] = f64::from_le_bytes(v.try_into().unwrap_or([0; 8])),
                    _ => i64s[r] = i64::from_le_bytes(v.try_into().unwrap_or([0; 8])),
                }
            }
            let validity = bitmap(&valid);
            b.cols[at] = match kind {
                ArrowKind::Decimal { .. } => FinishedCol::Dec128 { validity, data: i128s },
                ArrowKind::Float32 | ArrowKind::Float64 => FinishedCol::F64 { validity, data: f64s },
                _ => FinishedCol::I64 { validity, data: i64s },
            };
        }
    }
    let b: &ArrowBatch = b;
    // The arguments a column at a time over the batch, unless few rows are
    // taken (then each taken row's alone).
    let cols = if sel.len() * 8 >= b.rows { arg_cols(p, b) } else { vec![None; p.aggs.len()] };
    let mut vals: Vec<Num> = Vec::with_capacity(na);
    for (i, &r) in sel.iter().enumerate() {
        let r = r as usize;
        let pv = pv_of(i);
        let Some(key) = key_at(&b.cols[pkey], r)? else { continue };
        // An OR over both tables: the branches this probe row holds.
        let pm: u64 = if p.orx.is_empty() {
            0
        } else {
            let m = p.orx.iter().enumerate().filter(|(_, x)| x.1.as_ref().is_none_or(|pe| peval(pe, b, r, ptys) == Some(true))).fold(0u64, |m, (i, _)| m | (1 << i));
            if m == 0 {
                continue;
            }
            m
        };
        let one;
        let gs: &[u32] = match look(key) {
            Look::Miss => continue,
            Look::One(g) => {
                one = [g];
                &one
            }
            Look::Many(s) => s,
        };
        matched += gs.len() as u64;
        // The aggregate arguments once per probe row, folded per match.
        vals.clear();
        for (a, c) in p.aggs.iter().zip(&cols) {
            // A CASE over the build table is valued per matched group below.
            vals.push(if matches!(a.arg, Some(VExpr::CaseB { .. })) { Num::Null } else { arg_at(a, c, b, r)? });
        }
        let pf = match sx {
            Some((st, _, _)) if !st.cross.is_empty() || pgroup || !st.pflags.is_empty() => Some(group_fields(&pv)),
            _ => None,
        };
        for &g in gs {
            // A branch whose build condition (bit) and probe condition both hold.
            if !p.orx.is_empty() {
                let bm = gflags.map_or(0, |f| f[g as usize]);
                let ok = p.orx.iter().enumerate().any(|(i, x)| pm & (1 << i) != 0 && x.0.is_none_or(|bit| bm & (1 << bit) != 0));
                if !ok {
                    matched -= 1;
                    continue;
                }
            }
            if let (Some((st, _, gc)), Some(pf)) = (sx, pf.as_deref()) {
                if !st.cross.is_empty() && !star::cross_ok(st, gc, g, pf) {
                    matched -= 1;
                    continue;
                }
            }
            // The accumulators' group: the build's, or with probe-side GROUP
            // BY fields the (build group, those fields) pair's.
            let gi = match (pgroup, sx, pf.as_deref(), cg.as_deref_mut()) {
                (true, Some((st, _, _)), Some(pf), Some(cg)) => {
                    ck.clear();
                    ck.extend_from_slice(&g.to_le_bytes());
                    for &i in &st.pgroup {
                        let (t, v) = pf[i];
                        ck.push(t);
                        if t != 0 {
                            ck.extend_from_slice(&(v.len() as u32).to_le_bytes());
                            ck.extend_from_slice(v);
                        }
                    }
                    let c = cg.id(&ck)? as usize;
                    if accs.len() < (c + 1) * na {
                        accs.resize((c + 1) * na, Acc::None);
                    }
                    if hit.len() <= c {
                        hit.resize(c + 1, false);
                    }
                    c
                }
                _ => g as usize,
            };
            hit[gi] = true;
            let base = gi * na;
            for (i, a) in p.aggs.iter().enumerate() {
                let v = match &a.arg {
                    Some(VExpr::CaseB { arms, els, side }) => {
                        // The build group's flags, or a probe dimension's
                        // (the field its payload carries).
                        let m = match (*side >= 2, sx, pf.as_deref()) {
                            (true, Some((st, _, _)), Some(pf)) => {
                                let at = st.pflags.iter().find(|x| x.0 == side - 2).ok_or_else(|| bad("a CASE's dimension without its flags"))?.1;
                                i64::from_le_bytes(pf[at].1.try_into().map_err(|_| bad("a flag field"))?) as u64
                            }
                            _ => gflags.ok_or_else(|| bad("a CASE over the build table without its flags"))?[g as usize],
                        };
                        match arms.iter().find(|x| m & (1 << x.0) != 0).map(|x| &x.1).or(els.as_deref()) {
                            Some(x) => coerce(eval(x, b, r)?, type_of(x)?, a.ty.unwrap_or(Ty::Int))?,
                            None => Num::Null,
                        }
                    }
                    _ => vals[i],
                };
                fold(&mut accs[base + i], a.f, a.ty, v)?;
            }
        }
    }
    Ok(matched)
}

/// The probe of a join over Postgres page ranges whose key ranges are known
/// (idea 1c): each part is scanned with the key filter of its own key range
/// only, and a part starts the moment the build has passed its last key, so
/// the server scans while the build still reads.
struct Early {
    parts: Vec<Part>,
    /// Parts by the key their range ends at.
    order: Vec<usize>,
    next: usize,
    live: Vec<Rows>,
    probed: u64,
    matched: u64,
}

impl Early {
    fn new(parts: Vec<Part>) -> Early {
        let mut order: Vec<usize> = (0..parts.len()).collect();
        order.sort_by_key(|&i| parts[i].keys.map_or(i64::MIN, |k| k.1));
        Early { parts, order, next: 0, live: Vec::new(), probed: 0, matched: 0 }
    }

    /// Start every part whose keys within [lo, hi] all lie at or below `upto`.
    async fn start(&mut self, side: &Side, col: &str, lo: i64, hi: i64, upto: i64, bits: &dyn Fn(i64, i64) -> Vec<u8>) -> Result<()> {
        while self.next < self.order.len() {
            let part = self.parts[self.order[self.next]].clone();
            let Some((a, b)) = part.keys.map(|(x, y)| (x.max(lo), y.min(hi))).filter(|(x, y)| x <= y) else {
                self.next += 1;
                continue;
            };
            if b > upto {
                break;
            }
            self.next += 1;
            let upper = part.pages.1.map(|u| format!(" AND ctid < '({u},0)'::tid")).unwrap_or_default();
            let push = format!("ctid >= '({},0)'::tid{upper} AND ({})", part.pages.0, key_filter_sql(col, a, b, &bits(a, b)));
            self.live.push(open_filtered(side, Some(format!("{col} BETWEEN {a} AND {b}")), Some(1), Some(push)).await?);
            if dbg() {
                eprintln!("[sql]   part [{a}..{b}] scanning, cpu={:.2}s", cpu_s());
            }
        }
        Ok(())
    }

    /// Fold whatever the live parts have ready; `wait` = until all are done.
    #[allow(clippy::too_many_arguments)]
    async fn drain<'m>(
        &mut self,
        p: &Plan,
        ptys: &[Option<Ty>],
        pkey: usize,
        look: &dyn Fn(i64) -> Look<'m>,
        ngroups: usize,
        accs: &mut Vec<Acc>,
        hit: &mut Vec<bool>,
        wait: bool,
    ) -> Result<()> {
        loop {
            let mut moved = false;
            let mut i = 0;
            while i < self.live.len() {
                match self.live[i].try_recv()? {
                    Some(Some(mut b)) => {
                        self.probed += b.rows as u64;
                        self.matched += fold_probe(p, ptys, pkey, &mut b, look, ngroups, accs, hit, None, None, None)?;
                        moved = true;
                    }
                    Some(None) => {
                        self.live.swap_remove(i);
                        moved = true;
                        continue;
                    }
                    None => {}
                }
                i += 1;
            }
            if !wait || self.live.is_empty() {
                return Ok(());
            }
            if !moved {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
    }
}

async fn open(side: &Side, extra: Option<String>, parallel: Option<usize>) -> Result<Rows> {
    open_filtered(side, extra, parallel, None).await
}

/// A side's batches as the plan reads them: the server's columns, and the
/// virtual ones (`Side::virt`) computed into their read positions.
struct Rows {
    h: ReadHandle,
    slots: Option<Vec<Slot>>,
}

#[derive(Clone, Copy)]
enum Slot {
    /// The source batch's column `i`.
    Src(usize),
    /// The civil year of the source batch's column `i` (a date, or a
    /// timestamp in microseconds when `true`).
    Year(usize, bool),
    /// A dimension's value the probe fills (NULL here).
    Lookup,
    /// `SUBSTRING` of the source batch's text column `i`.
    Substr(usize, i64, Option<i64>),
}

/// Year of a day count from 1970-01-01 (Hinnant's civil_from_days).
fn year_of_days(z: i64) -> i32 {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2)) as i32
}

fn year_col(c: &FinishedCol, ts: bool) -> FinishedCol {
    match (c, ts) {
        (FinishedCol::I32 { validity, data }, false) => FinishedCol::I32 { validity: validity.clone(), data: data.iter().map(|&d| year_of_days(i64::from(d))).collect() },
        (FinishedCol::I64 { validity, data }, true) => {
            FinishedCol::I32 { validity: validity.clone(), data: data.iter().map(|&u| year_of_days(u.div_euclid(86_400_000_000))).collect() }
        }
        (other, _) => {
            // Not a date: every value NULL (the planner allows dates only).
            let n = match other {
                FinishedCol::I32 { data, .. } => data.len(),
                FinishedCol::I64 { data, .. } => data.len(),
                _ => 0,
            };
            FinishedCol::I32 { validity: Some(vec![0; n.div_ceil(8)]), data: vec![0; n] }
        }
    }
}

/// SQL `SUBSTRING(t FROM from FOR len)` of each value: characters, 1-based;
/// a start before 1 eats into the length, as the standard has it.
fn substr_col(c: &FinishedCol, from: i64, len: Option<i64>) -> FinishedCol {
    let FinishedCol::Utf8 { validity, offsets, data } = c else {
        let n = match c {
            FinishedCol::I32 { data, .. } => data.len(),
            FinishedCol::I64 { data, .. } => data.len(),
            _ => 0,
        };
        return FinishedCol::Utf8 { validity: Some(vec![0; n.div_ceil(8)]), offsets: vec![0; n + 1], data: Vec::new() };
    };
    let mut out = Vec::with_capacity(data.len() / 2);
    let mut offs = Vec::with_capacity(offsets.len());
    offs.push(0i32);
    let (skip, take) = substr_bounds(from, len);
    for w in offsets.windows(2) {
        let t = std::str::from_utf8(&data[w[0] as usize..w[1] as usize]).unwrap_or("");
        let mut it = t.char_indices().skip(skip);
        if let Some((a, _)) = it.next() {
            let b = match take {
                Some(0) => a,
                Some(n) => t[a..].char_indices().nth(n).map_or(t.len(), |(i, _)| a + i),
                None => t.len(),
            };
            out.extend_from_slice(&t.as_bytes()[a..b]);
        }
        offs.push(out.len() as i32);
    }
    FinishedCol::Utf8 { validity: validity.clone(), offsets: offs, data: out }
}

/// Characters skipped and taken (`None`: to the end) by `SUBSTRING(… FROM
/// from FOR len)`.
fn substr_bounds(from: i64, len: Option<i64>) -> (usize, Option<usize>) {
    let skip = usize::try_from(from - 1).unwrap_or(0);
    let take = len.map(|l| usize::try_from(l + from.min(1) - 1).unwrap_or(0));
    (skip, take)
}

impl Rows {
    fn of(h: ReadHandle, side: &Side) -> Rows {
        if side.virt.is_empty() && side.lookups.is_empty() {
            return Rows { h, slots: None };
        }
        let virt_of = |c: usize| side.virt.iter().find(|v| v.0 == c).map(|v| (v.1, v.2));
        let lookup = |c: usize| side.lookups.iter().any(|v| v.0 == c);
        let src: Vec<usize> = side.read.iter().copied().filter(|&c| virt_of(c).is_none() && !lookup(c)).collect();
        let slots = side
            .read
            .iter()
            .map(|&c| match virt_of(c) {
                None if lookup(c) => Slot::Lookup,
                None => Slot::Src(src.iter().position(|&x| x == c).expect("a source column")),
                Some((base, how)) => {
                    let at = src.iter().position(|&x| x == base).expect("a virtual column's base is read");
                    match how {
                        Virt::Year => Slot::Year(at, matches!(side.schema[base].kind, ArrowKind::TimestampUtc | ArrowKind::TimestampNaive)),
                        Virt::Substr(from, len) => Slot::Substr(at, from, len),
                    }
                }
            })
            .collect();
        Rows { h, slots: Some(slots) }
    }

    fn aug(&self, b: ArrowBatch) -> ArrowBatch {
        let Some(sl) = &self.slots else { return b };
        let years: Vec<Option<FinishedCol>> = sl.iter().map(|s| match *s {
            Slot::Year(i, ts) => Some(year_col(&b.cols[i], ts)),
            Slot::Substr(i, from, len) => Some(substr_col(&b.cols[i], from, len)),
            Slot::Lookup => Some(FinishedCol::I64 { validity: Some(vec![0; b.rows.div_ceil(8)]), data: vec![0; b.rows] }),
            Slot::Src(_) => None,
        }).collect();
        let mut src: Vec<Option<FinishedCol>> = b.cols.into_iter().map(Some).collect();
        let cols = sl
            .iter()
            .zip(years)
            .map(|(s, y)| match *s {
                Slot::Src(i) => src[i].take().unwrap_or_else(|| FinishedCol::I32 { validity: None, data: Vec::new() }),
                Slot::Year(..) | Slot::Lookup | Slot::Substr(..) => y.expect("computed"),
            })
            .collect();
        ArrowBatch { rows: b.rows, cols }
    }

    /// (Measured: the time a side's rows are waited for.)
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn recv(&mut self) -> Result<Option<ArrowBatch>> {
        Ok(self.h.recv().await?.map(|b| self.aug(b)))
    }

    fn try_recv(&mut self) -> Result<Option<Option<ArrowBatch>>> {
        Ok(self.h.try_recv()?.map(|o| o.map(|b| self.aug(b))))
    }

    fn cancel(&mut self) {
        self.h.cancel()
    }
}

/// [`open`] with a trusted server-side filter (`push_where`: the join's key
/// filter) on every span, one span per pipe.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn open_filtered(side: &Side, extra: Option<String>, parallel: Option<usize>, push: Option<String>) -> Result<Rows> {
    open_gated(side, extra, parallel, push, Vec::new()).await
}

/// [`open_filtered`] with row gates the decoder applies ([`KeyGate`]).
async fn open_gated(side: &Side, extra: Option<String>, parallel: Option<usize>, push: Option<String>, gates: Vec<Arc<KeyGate>>) -> Result<Rows> {
    let mut conj = side.conj.clone();
    conj.extend(extra);
    let opts = ReadOptions {
        max_spans: push.as_ref().and(parallel),
        push_where: push,
        parallel,
        columns: (!side.whole).then(|| {
            side.read
                .iter()
                .filter(|&&c| !side.virt.iter().any(|v| v.0 == c) && !side.lookups.iter().any(|v| v.0 == c))
                .map(|&c| side.schema[c].name.clone())
                .collect()
        }),
        where_sql: (!conj.is_empty()).then(|| conj.iter().map(|c| format!("({c})")).collect::<Vec<_>>().join(" AND ")),
        computed: side.computed.clone(),
        gates,
        ..Default::default()
    };
    Ok(Rows::of(crate::read::read_start(&side.url, &side.table, &opts).await?, side))
}

/// Pipes a probe read takes at `cores`: twice the cores (a probe is
/// decode-bound), unless the server filters the side by its own WHERE and
/// little is held: then the read waits on the server's scan, not on the
/// decode (TPC-H Q19 decoded 2.6M of the 60M rows its server scanned, at 4 s
/// of CPU in a 19 s wall), and more scans run beside each other. So too when
/// the decoder's gates drop most rows (TPC-H Q5 decoded 364K of 60M and
/// waited 29 s on two scans). The memory
/// left still bounds it (`read_pipes_for`); a large map keeps the kernel's
/// socket memory in mind (five pipes beside 57 MiB were OOM-killed).
fn probe_pipe_cap(cores: f64, side: &Side, held: u64, density: f64) -> usize {
    // At least four when the decode is light (a gate to an eighth) or the
    // map small: under a core such a probe waits on the server more than it
    // decodes (TPC-H Q12 at 0.5 core: 25% CPU at 2 pipes, 14.7 s → 11.7 s
    // at 4; Q10 30.1 → 23.3 s). A heavy decode beside a large map keeps two:
    // the T2 join (a quarter through, 64 MiB held) peaked at 243 MB of 256
    // at four.
    // 40 MiB: the statement's pinned Postgres snapshots hold their
    // connections from the start (~7 MiB resident), and at 32 a light read
    // beside them (TPC-H Q22's orders keys, 34 MiB held) fell to two pipes,
    // 15 s → 26 s. Far under the 64 MiB that peaked at 243 MB with four.
    // A stage table (no server) keeps 32: more pipes over local rows bought
    // TPC-H Q20 nothing and held 14-29 MB more.
    let light = if side.dialect == Dialect::Other { 32 << 20 } else { 40 << 20 };
    let floor = if density <= 1.0 / 8.0 || held <= light { PROBE_MIN_PIPES } else { 2 };
    let cap = ((cores.ceil() as usize) * 2).max(floor);
    if (!side.conj.is_empty() && held <= 48 << 20) || (density <= 1.0 / 16.0 && held <= 96 << 20) {
        cap.max(SERVER_SCAN_PIPES)
    } else if density <= 1.0 / 8.0 && held <= 96 << 20 {
        // An eighth through: the decode is an eighth, the pipes' memory
        // flat (the T0 join, 0.5 core / 256 MB: 2 pipes 24.7 s / 123 MB,
        // 4 pipes 15.8 s / 121 MB, 6 pipes 13.0 s / 152 MB).
        cap.max(GATED_SCAN_PIPES)
    } else {
        cap
    }
}

/// The fewest pipes a probe's CPU cap allows ([`probe_pipe_cap`]); the
/// memory left still bounds it, except for a gated probe (below).
const PROBE_MIN_PIPES: usize = 4;

/// Scans a probe gated to an eighth of its keys runs at once
/// ([`probe_pipe_cap`]).
const GATED_SCAN_PIPES: usize = 6;

/// The share of the key range the tightest gate lets through (1: none).
fn gate_density(gates: &[Arc<KeyGate>]) -> f64 {
    gates.iter().map(|g| g.density()).fold(1.0, f64::min)
}

/// A gate selective enough that its probe waits on the server's scan, not
/// on the decode ([`probe_pipe_cap`]): it lets a sixteenth of the keys or
/// fewer through. TPC-H Q7's lets a quarter through, decodes 18M rows, and at
/// eight pipes only took more memory (248 MB of a 256 MB cage).
fn selective(gates: &[Arc<KeyGate>]) -> bool {
    gates.iter().any(|g| g.density() <= 1.0 / 16.0)
}

/// Scans a filtered probe side runs at once ([`probe_pipe_cap`]).
const SERVER_SCAN_PIPES: usize = 8;

/// A probe read started beside the build (`join_pass`): its handle, the
/// batches taken from it meanwhile and their bytes, and the room for them.
struct Pre {
    rows: Rows,
    buf: Vec<ArrowBatch>,
    bytes: u64,
    room: u64,
    done: bool,
}

/// Memory a probe read started beside the build may hold (an eighth of the
/// limit), or `None` below 1 GiB: a small cage has no room for the batches
/// and a second set of pipes beside the build (a 256 MB one peaks past 200
/// MB with them one after the other), so it reads as it did.
fn pre_room() -> Option<u64> {
    if std::env::var("APITAP_SQL_PREFETCH").is_ok_and(|v| v == "0") || opt_in("APITAP_SQL_KEY_FILTER") {
        return None;
    }
    let m = crate::pipeline::mem_limit_bytes().unwrap_or(8 << 30);
    (m >= 1 << 30).then_some(m / 8)
}

/// The probe side's filter for a pass over [lo, hi]: the key range, and on
/// Postgres the pages the range lies in.
fn probe_extra(p: &Plan, ps: usize, between_of: &dyn Fn(usize, i64, i64) -> Option<String>, parts: &std::sync::Mutex<Option<Vec<Part>>>, lo: i64, hi: i64) -> Option<String> {
    let narrow = match parts.lock().unwrap_or_else(|e| e.into_inner()).as_deref() {
        Some(parts) if p.sides[ps].dialect == Dialect::Pg => ctid_span(parts, lo, hi),
        _ => None,
    };
    match (between_of(ps, lo, hi), narrow) {
        (Some(b), Some(n)) => Some(format!("{b} AND {n}")),
        (b, None) => b,
        (None, n) => n,
    }
}

/// A gate costs at most this much memory (16 MiB: 134M keys).
const GATE_MAX_BYTES: u64 = 16 << 20;

/// Side `side`'s read position `pos` as a column of its server read, when a
/// gate can be placed on it: an integer column the server sends (not a
/// virtual or lookup one) of a Postgres side read by columns.
fn gate_col(side: &Side, pos: usize) -> Option<usize> {
    if side.dialect != Dialect::Pg || side.whole || !side.computed.is_empty() || std::env::var("APITAP_SQL_GATE").is_ok_and(|v| v == "0") {
        return None;
    }
    let local = |x: usize| side.virt.iter().any(|v| v.0 == x) || side.lookups.iter().any(|v| v.0 == x);
    let c = *side.read.get(pos)?;
    if local(c) || !matches!(side.schema[c].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64) {
        return None;
    }
    side.read.iter().filter(|&&x| !local(x)).position(|&x| x == c)
}

/// A gate on `side`'s read position `pos` letting through `n` keys of
/// [lo, hi] (`fill` sets them): `None` when no gate can be placed, the range
/// is too wide, or half the keys or more pass (a gate that drops little
/// only costs its checks).
fn gate_of(side: &Side, pos: usize, lo: i64, hi: i64, n: u64, fill: impl FnOnce(&mut KeyGate)) -> Option<Arc<KeyGate>> {
    let col = gate_col(side, pos)?;
    let span = u64::try_from(hi as i128 - lo as i128 + 1).ok()?;
    if KeyGate::bytes_for(span) > GATE_MAX_BYTES || n.saturating_mul(2) >= span {
        return None;
    }
    let mut g = KeyGate::new(col, lo, hi)?;
    fill(&mut g);
    Some(Arc::new(g))
}

/// Gates for a read of `side` from the dimensions its columns must be in:
/// (read position, dimension map) pairs.
fn dim_gates(side: &Side, of: &[(usize, &star::DimMap)]) -> Vec<Arc<KeyGate>> {
    of.iter()
        .filter_map(|&(pos, m)| {
            let (lo, hi) = m.key_span()?;
            gate_of(side, pos, lo, hi, m.key_count(), |g| m.for_each_key(|k| g.set(k)))
        })
        .collect()
}

/// A heavy pushdown, off unless asked for (`=1`): the source database is
/// only asked to scan and send — the expression pushdown (idea 6) and the
/// join's key bitmap (idea 9) hand it the engine's work, so a measured run
/// would credit the server's cores to apitap. Both stay as opt-ins for a
/// deployment whose database has cores to spare.
fn opt_in(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| v == "1")
}

fn dbg() -> bool {
    std::env::var("APITAP_DEBUG").is_ok()
}

/// This process's CPU seconds so far (user + system), for the debug lines.
fn cpu_s() -> f64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let rest = &s[s.rfind(')')? + 2..];
            let f: Vec<&str> = rest.split_whitespace().collect();
            Some((f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?) / 100.0)
        })
        .unwrap_or(0.0)
}

/// Run `query` and land its answer in `dst_url` as `table`, through the
/// ordinary transfer: the destination's types, staging, atomic swap and lock.
pub async fn run_into(
    query: &str,
    sources: &HashMap<String, String>,
    dst_url: &str,
    table: &str,
    opts: &crate::TransferOptions,
) -> Result<crate::TransferReport> {
    if opts.mode != crate::Mode::Replace {
        return Err(bad("apitap.sql(dest=...) lands with mode='replace' (the answer is recomputed whole)"));
    }
    // The answer as it comes: a stage table past its budget lands a chunk at
    // a time (an answer of millions of rows held whole was OOM-killed in a
    // 256 MB cage before a destination saw a row).
    let (srcs, held) = pin_statement(query, sources).await?;
    let a = stage::run_answer(query, &srcs).await;
    for h in held {
        h.close().await;
    }
    let reg = match a? {
        Answer::Rows(r) => crate::source::sqlresult::register(r),
        Answer::Table(t) => crate::source::sqlresult::register_table(t),
    };
    let r = crate::transfer(&reg.url, dst_url, table, opts).await;
    drop(reg);
    r
}

/// SQL for `e` over side `s`'s columns, when Postgres evaluates it exactly as
/// apitap would: integer and decimal `+ - *` (numeric keeps every digit; its
/// product scale is the sum of the scales, as `type_of` has it). `None` for
/// anything else.
fn pg_sql(side: &Side, e: &VExpr) -> Option<String> {
    Some(match e {
        VExpr::Col(i, k) if matches!(k, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64 | ArrowKind::Decimal { .. }) => {
            quote(Dialect::Pg, &side.schema[side.read[*i]].name)
        }
        VExpr::Int(v) => format!("{v}::numeric"),
        VExpr::Dec(v, sc) => {
            let neg = *v < 0;
            let d = v.unsigned_abs().to_string();
            let sc = *sc as usize;
            let d = if d.len() <= sc { format!("{}{d}", "0".repeat(sc + 1 - d.len())) } else { d };
            let (w, f) = d.split_at(d.len() - sc);
            format!("{}{w}{}{f}::numeric", if neg { "-" } else { "" }, if sc > 0 { "." } else { "" })
        }
        VExpr::Neg(x) => format!("(-({}))", pg_sql(side, x)?),
        VExpr::Bin(l, op, r) if matches!(op, '+' | '-' | '*') => format!("(({}) {op} ({}))", pg_sql(side, l)?, pg_sql(side, r)?),
        _ => return None,
    })
}

/// Idea 6 for the probe side of a join over Postgres: every aggregate
/// argument is computed by the server and shipped as one `int8` already
/// scaled to the aggregate's decimal scale, instead of the numeric columns it
/// is built from. The probe then reads the key plus one 8-byte integer per
/// aggregate; `numeric` is neither framed nor decoded here. The server
/// refuses a value past `int8` (`bigint out of range`), and `run` then runs
/// the query again without this. `false` = the plan is unchanged.
fn pushdown_args(p: &mut Plan) -> bool {
    opt_in("APITAP_SQL_PUSH_ARGS") && rewrite_args(p)
}

/// The rewrite [`pushdown_args`] makes when asked for.
fn rewrite_args(p: &mut Plan) -> bool {
    let Some(k) = p.keys else { return false };
    // Branch conditions read the probe's columns by position: kept as read.
    if !p.orx.is_empty() {
        return false;
    }
    let s = p.agg_side;
    if s == p.group_side || p.sides[s].dialect != Dialect::Pg || !p.sides[s].local.is_empty() {
        return false;
    }
    let side = &p.sides[s];
    let mut sqls = Vec::new();
    for a in &p.aggs {
        let sql = a.arg.as_ref().map(|e| pg_sql(side, e));
        match (sql, a.ty) {
            (None, _) => sqls.push(None),
            (Some(Some(x)), Some(Ty::Int)) => sqls.push(Some((x, 0i8))),
            (Some(Some(x)), Some(Ty::Dec(sc))) => sqls.push(Some((format!("({x}) * 1{}::numeric", "0".repeat(sc as usize)), sc))),
            _ => return false,
        }
    }
    let ki = k.iter().position(|x| x.0 == s).expect("both sides keyed");
    let key_col = p.sides[s].read[k[ki].1];
    let side = &mut p.sides[s];
    side.read = vec![key_col];
    side.computed.clear();
    for (i, (a, sq)) in p.aggs.iter_mut().zip(sqls).enumerate() {
        let Some((sql, sc)) = sq else { continue };
        let name = format!("_apitap_a{i}");
        side.schema.push(ArrowField { name: name.clone(), kind: ArrowKind::Int64, nullable: true, rewritten: false, padded: false, ci: false, ai: false });
        side.read.push(side.schema.len() - 1);
        side.computed.push((name, sql));
        let col = VExpr::Col(side.read.len() - 1, ArrowKind::Int64);
        a.arg = Some(if a.ty == Some(Ty::Int) { col } else { VExpr::Scaled(Box::new(col), sc) });
    }
    let mut k2 = k;
    k2[ki].1 = 0;
    p.keys = Some(k2);
    true
}

/// Run `query` against `sources` (name → URL).
pub async fn run(query: &str, sources: &HashMap<String, String>) -> Result<SqlResult> {
    // One snapshot per Postgres server for the whole statement: its stages,
    // a LEFT JOIN's two halves, the parts of a GROUP BY past its budget and
    // a run again all read the same rows (each pinned its own before, so an
    // UPDATE between two of them could count a row twice or not at all).
    // `pin_snapshots` leaves a URL that carries one as it is. MySQL sources
    // have no exported snapshot: each statement there reads its own.
    let (srcs, held) = pin_statement(query, sources).await?;
    let r = stage::run(query, &srcs).await;
    for h in held {
        h.close().await;
    }
    r
}

/// [`run`] as a read stream: a large answer (past the stage budget, on disk)
/// goes out a chunk at a time instead of whole in memory (`apitap.sql()`'s
/// Arrow stream). The snapshots close once the answer is computed; its rows
/// are already the stage's.
pub async fn run_stream(query: &str, sources: &HashMap<String, String>) -> Result<ReadHandle> {
    let (srcs, held) = pin_statement(query, sources).await?;
    let r = stage::run_answer(query, &srcs).await;
    for h in held {
        h.close().await;
    }
    match r? {
        Answer::Rows(r) => Ok(ReadHandle::ready(r.schema, vec![r.batch])),
        // In memory (under the stage budget): handed on as it is, no copy.
        Answer::Table(t) if t.in_memory() => {
            let schema = t.schema.clone();
            Ok(ReadHandle::ready(schema, vec![t.into_batch()?]))
        }
        Answer::Table(t) => t.into_stream(),
    }
}

/// The Postgres sources the query names (`name.` in its text), each pinned
/// to a snapshot held until the statement ends.
async fn pin_statement(query: &str, sources: &HashMap<String, String>) -> Result<(HashMap<String, String>, Vec<crate::source::postgres::HeldSnapshot>)> {
    let mut srcs = sources.clone();
    let mut held: Vec<crate::source::postgres::HeldSnapshot> = Vec::new();
    if std::env::var("APITAP_SQL_SNAPSHOT").is_ok_and(|v| v == "0") {
        return Ok((srcs, held));
    }
    // The sources the statement's tables name (`<source>.<table>`, at any
    // depth), not every source whose name appears in its text: `a.x` may be
    // an alias's column. Unparsable: none pinned, the parse error comes next.
    let mut named: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Ok(stmts) = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, query) {
        let _ = sqlparser::ast::visit_relations(&stmts, |r| {
            if r.0.len() >= 2 {
                if let Some(ObjectNamePart::Identifier(i)) = r.0.first() {
                    named.insert(i.value.to_ascii_lowercase());
                }
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
    let mut pinned: Vec<(String, String)> = Vec::new();
    for (name, url) in sources {
        let pg = url.starts_with("postgres://") || url.starts_with("postgresql://");
        if !pg || url.contains("__apitap_snapshot=") || !named.contains(&name.to_ascii_lowercase()) {
            continue;
        }
        let to = match pinned.iter().find(|(u, _)| u == url) {
            Some((_, t)) => t.clone(),
            None => match crate::source::postgres::HeldSnapshot::open(url).await {
                Ok(h) => {
                    let t = format!("{url}{}__apitap_snapshot={}", if url.contains('?') { '&' } else { '?' }, h.id);
                    held.push(h);
                    pinned.push((url.clone(), t.clone()));
                    t
                }
                Err(e) => {
                    for h in held {
                        h.close().await;
                    }
                    return Err(e);
                }
            },
        };
        srcs.insert(name.clone(), to);
    }
    if dbg() && !held.is_empty() {
        eprintln!("[sql] {} snapshot(s) pinned for the statement", held.len());
    }
    Ok((srcs, held))
}

/// One SELECT the engine answers in one pass (`stage` splits the others).
pub(crate) async fn run_one(query: &str, sources: &HashMap<String, String>) -> Result<SqlResult> {
    run_one_out(query, sources).await?.into_result()
}

/// An answer: rows here, or a stage table a query answered in parts
/// wrote (on disk past its budget) for a later stage to read.
pub(crate) enum Answer {
    Rows(SqlResult),
    Table(mem::MemTable),
}

impl Answer {
    pub(crate) fn schema(&self) -> &[ArrowField] {
        match self {
            Answer::Rows(r) => &r.schema,
            Answer::Table(t) => &t.schema,
        }
    }

    pub(crate) fn schema_mut(&mut self) -> &mut Vec<ArrowField> {
        match self {
            Answer::Rows(r) => &mut r.schema,
            Answer::Table(t) => &mut t.schema,
        }
    }

    pub(crate) fn into_result(self) -> Result<SqlResult> {
        match self {
            Answer::Rows(r) => Ok(r),
            Answer::Table(t) => {
                let schema = t.schema.clone();
                Ok(SqlResult { schema, batch: t.into_batch()? })
            }
        }
    }

    pub(crate) fn into_table(self) -> Result<mem::MemTable> {
        match self {
            Answer::Rows(r) => mem::MemTable::new(r.schema, r.batch),
            Answer::Table(t) => Ok(t),
        }
    }

    /// Each batch in order, one at a time.
    pub(crate) fn for_each(self, f: &mut dyn FnMut(ArrowBatch) -> Result<()>) -> Result<()> {
        match self {
            Answer::Rows(r) => f(r.batch),
            Answer::Table(t) => t.for_each(f),
        }
    }

    /// No rows, the columns' shapes: what `for_each` skips for an empty
    /// stage table (`None` when there are rows).
    pub(crate) fn empty_batch(&self) -> Option<ArrowBatch> {
        match self {
            Answer::Rows(r) if r.batch.rows == 0 => Some(ArrowBatch { rows: 0, cols: r.batch.cols.clone() }),
            Answer::Rows(_) => None,
            Answer::Table(t) => t.empty_batch(),
        }
    }
}

/// A build [`join_pass`] made under [`partitioned`], kept for the next part:
/// the build side's statement is the same in every part when the part's
/// column is the probe side's (TPC-H Q16 10g seed read part_10g, 7.6 s, in each
/// of its 16 parts). Keyed by [`build_sig`]; one entry, the last build.
struct CachedBuild {
    sig: String,
    map: std::rc::Rc<BuildMap>,
    groups: Groups,
    lo: i64,
    hi: i64,
}

tokio::task_local! {
    /// Under a plain SELECT's rewrite as GROUP BY every output (`stage`):
    /// the groups are not folded by collation ([`collated`]).
    pub(crate) static GROUPS_EXACT: ();
}

tokio::task_local! {
    /// (kept past the join: under `partitioned`; the entry), scoped by
    /// [`run_one_out`]: a first run that broke the group budget hands its
    /// build to the parts, a run that did not drops it when its join ends,
    /// before its groups finalize.
    static BUILD_CACHE: std::cell::RefCell<(bool, Option<CachedBuild>)>;
}

/// A URL without the snapshot [`pin_snapshots`] gave it: each part pins its
/// own, and the build is the same table's.
pub(super) fn url_sig(u: &str) -> &str {
    u.split_once("__apitap_snapshot=").map_or(u, |(a, _)| a)
}

/// What a join's build reads and keeps, over keys [lo, hi]: two builds with
/// the same signature hold the same map and groups. A star plan only with no
/// dimensions to read (the shape a GROUP BY on probe-side columns takes,
/// TPC-H Q16's ps_suppkey) and its build layout in the signature. `None`: a
/// star plan's dimensions or late reads, a GROUP BY on the join key (the
/// groups are the pass's), or a subquery whose keys have no signature.
fn build_sig(p: &Plan, k: &[(usize, usize); 2], names: &[String], lo: i64, hi: i64) -> Option<String> {
    if p.key_groups || p.star.as_ref().is_some_and(|st| !st.dims.is_empty() || !st.late.is_empty()) {
        return None;
    }
    let star = p.star.as_ref().map(|st| format!("{:?}|{:?}|{:?}|{:?}|{:?}", st.bfields, st.bkids, st.keep, st.gpos, st.pgroup));
    let s = &p.sides[p.group_side];
    let mut out = format!(
        "{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{}|{:?}|{:?}|{star:?}|{lo}|{hi}",
        url_sig(&s.url),
        s.table,
        s.read,
        s.conj,
        s.local,
        s.flags,
        s.virt,
        s.computed,
        p.groups,
        s.whole,
        k,
        names
    );
    for sm in &s.semis {
        out.push('[');
        out.push_str(&semi::sig(sm)?);
        out.push(']');
    }
    Some(out)
}

tokio::task_local! {
    /// Under [`run_one_out`]: the stage table key-range passes write their
    /// final rows to as each ends (`execute`), when no ORDER BY or LIMIT
    /// needs them together.
    static PASS_SINK: std::cell::RefCell<Option<mem::Writer>>;
}

/// [`run_one`], an answer written in parts (passes, partitions) left as
/// their stage table.
pub(crate) async fn run_one_out(query: &str, sources: &HashMap<String, String>) -> Result<Answer> {
    // The subqueries' key sets live as long as the plan that read them; held
    // here they outlive a run that broke the group budget, and its parts
    // take them instead of reading them again (TPC-H Q13 10g seed: the anti
    // join's 69M orders keys, read by the first run and again by the parts).
    let go = BUILD_CACHE.scope(std::cell::RefCell::new((false, None)), run_one_keyed(query, sources));
    if semi::KEY_CACHE.try_with(|_| ()).is_ok() {
        go.await
    } else {
        semi::KEY_CACHE.scope(std::cell::RefCell::new(HashMap::new()), go).await
    }
}

async fn run_one_keyed(query: &str, sources: &HashMap<String, String>) -> Result<Answer> {
    let (r, w) = PASS_SINK
        .scope(std::cell::RefCell::new(None), async {
            let r = run_engine(query, sources).await;
            (r, PASS_SINK.with(|w| w.borrow_mut().take()))
        })
        .await;
    let r = match (r, w) {
        (Ok(r), Some(mut w)) => {
            w.push(r.batch)?;
            let t = w.finish()?;
            if dbg() {
                eprintln!("[sql] passes streamed to a stage table: {} rows", t.rows());
            }
            return Ok(Answer::Table(t));
        }
        (r, _) => r,
    };
    match r {
        Err(e) if e.to_string().contains(GROUPS_OVER) => {
            if dbg() {
                eprintln!("[sql] {e}; the query runs again in partitions of a GROUP BY column");
            }
            partitioned(query, sources, e).await
        }
        r => r.map(Answer::Rows),
    }
}

/// Marks the error a GROUP BY raises past [`group_budget`]: `run_one` answers
/// the query again in parts instead.
const GROUPS_OVER: &str = "groups past their memory budget";

/// The bytes a GROUP BY's groups may hold (keys, index, accumulators):
/// `APITAP_SQL_GROUP_BYTES` (a test lever), else an eighth of the memory
/// limit, else 512 MiB. An eighth: the finalize copies the groups up to
/// three times (`combine`, `strip`, `collate`), and a key-range pass holds
/// its own groups beside the kept ones (TPC-H Q13/Q16 10g seed died at a quarter).
fn group_budget() -> u64 {
    if let Some(b) = std::env::var("APITAP_SQL_GROUP_BYTES")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .filter(|b| *b > 0)
    {
        return b;
    }
    crate::pipeline::mem_limit_bytes().map_or(512 << 20, |m| m / 8)
}

fn groups_bytes(g: &Groups, accs: usize, hit: usize) -> u64 {
    g.bytes + g.ids.capacity() as u64 * 5 + (accs * std::mem::size_of::<Acc>()) as u64 + hit as u64
}

fn groups_check(g: &Groups, accs: usize, hit: usize) -> Result<()> {
    let b = groups_bytes(g, accs, hit);
    let budget = group_budget();
    if b > budget {
        return Err(bad(format!("{} groups ({b} B): {GROUPS_OVER} ({budget} B)", g.keys.len())));
    }
    Ok(())
}

/// A query whose groups outgrew [`group_budget`], answered in N parts by a
/// GROUP BY column of integers (`ABS(col % N) = i`, NULLs in part 0): the
/// parts' groups are disjoint, so their rows are the answer's, ordered and
/// limited here. N doubles while a part is still too large. Each part reads
/// its tables again: memory bounded, at the price of N scans.
async fn partitioned(query: &str, sources: &HashMap<String, String>, first: Error) -> Result<Answer> {
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, query).map_err(|e| bad(format!("parse: {e}")))?;
    let [Statement::Query(q)] = stmts.as_slice() else { return Err(first) };
    let SetExpr::Select(sel) = q.body.as_ref() else { return Err(first) };
    let GroupByExpr::Expressions(gby, _) = &sel.group_by else { return Err(first) };
    let cands: Vec<Expr> = gby.iter().filter(|e| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))).cloned().collect();
    let names: Vec<String> = sel
        .projection
        .iter()
        .enumerate()
        .map(|(i, it)| match it {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(e) => e.to_string(),
            _ => format!("_{i}"),
        })
        .collect();
    let exprs: Vec<String> = sel
        .projection
        .iter()
        .map(|it| match it {
            SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) => expr.to_string(),
            _ => String::new(),
        })
        .collect();
    // Integer columns only: a text column's `%` is the server's to coerce
    // (MySQL takes 'Brand#45' as 0, every row in part 0: TPC-H Q16 10g seed
    // spent 300 s on p_brand and p_type before ps_suppkey). The finest
    // last: a GROUP BY lists its coarse columns first, as Q16 does.
    let p = plan_probed(query, sources).await?;
    // A GROUP BY by an output's alias parts by the output's expression
    // (TPC-H Q15's `GROUP BY supplier_no`, `l_suppkey AS supplier_no`).
    let output = |c: &Expr| -> Option<usize> {
        let t = c.to_string();
        exprs.iter().position(|e| *e == t).or_else(|| names.iter().position(|n| n.eq_ignore_ascii_case(&t)))
    };
    let int_col = |c: &Expr| -> bool {
        let Some(i) = output(c) else { return false };
        let Some((_, OutCol::Group(g))) = p.out.get(i) else { return false };
        let kind = match &p.star {
            Some(st) => st.gkind[*g],
            None => {
                let gs = &p.sides[p.group_side];
                gs.schema[gs.read[p.groups[*g]]].kind
            }
        };
        matches!(kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64)
    };
    let cands: Vec<Expr> = cands
        .into_iter()
        .rev()
        .filter(|c| int_col(c))
        .map(|c| match output(&c) {
            Some(i) if exprs[i] != c.to_string() => sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
                .try_with_sql(&exprs[i])
                .and_then(|mut x| x.parse_expr())
                .unwrap_or(c),
            _ => c,
        })
        .collect();
    drop(p);
    if dbg() {
        eprintln!("[sql] part columns: {}", cands.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "));
    }
    let go = async {
        if BUILD_CACHE.try_with(|x| x.borrow_mut().0 = true).is_ok() {
            parts(sources, q, cands, &names, &exprs, first).await
        } else {
            BUILD_CACHE.scope(std::cell::RefCell::new((true, None)), parts(sources, q, cands, &names, &exprs, first)).await
        }
    };
    if semi::KEY_CACHE.try_with(|_| ()).is_ok() {
        go.await
    } else {
        semi::KEY_CACHE.scope(std::cell::RefCell::new(HashMap::new()), go).await
    }
}

/// [`partitioned`]'s parts, by each candidate column in turn.
async fn parts(
    sources: &HashMap<String, String>,
    q: &sqlparser::ast::Query,
    cands: Vec<Expr>,
    names: &[String],
    exprs: &[String],
    first: Error,
) -> Result<Answer> {
    let mut last = first;
    'cand: for col in cands {
        let mut n = 4u32;
        'size: while n <= 64 {
            // The parts go to a stage table as they come (on disk past
            // its budget): the whole answer is never in memory twice.
            let mut out: Option<mem::Writer> = None;
            for i in 0..n {
                let mut q2 = q.clone();
                // With a LIMIT each part keeps its own first rows (the parts'
                // groups are disjoint, so the answer's are among them): at most
                // LIMIT × parts rows are merged below, not every group.
                if q.limit.is_none() {
                    q2.order_by = None;
                }
                let SetExpr::Select(s2) = q2.body.as_mut() else { unreachable!() };
                let part = if i == 0 { format!("(ABS({col} % {n}) = 0 OR {col} IS NULL)") } else { format!("ABS({col} % {n}) = {i}") };
                let pe = sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
                    .try_with_sql(&part)
                    .and_then(|mut x| x.parse_expr())
                    .map_err(|e| bad(format!("parse: {e}")))?;
                s2.selection = Some(match s2.selection.take() {
                    Some(w) => Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(w))), op: BinaryOperator::And, right: Box::new(pe) },
                    None => pe,
                });
                match run_engine(&q2.to_string(), sources).await {
                    Ok(r) => {
                        let w = out.get_or_insert_with(|| mem::Writer::new(r.schema.clone()));
                        w.push(r.batch)?;
                    }
                    Err(e) if e.to_string().contains(GROUPS_OVER) => {
                        last = e;
                        n *= 2;
                        continue 'size;
                    }
                    Err(e) => {
                        last = e;
                        continue 'cand;
                    }
                }
            }
            let t = out.ok_or_else(|| bad("no parts"))?.finish()?;
            if dbg() {
                eprintln!("[sql] answered in {n} parts by {col}: {} rows", t.rows());
            }
            if q.order_by.is_none() && q.limit.is_none() {
                return Ok(Answer::Table(t));
            }
            let schema = t.schema.clone();
            let mut r = SqlResult { schema, batch: t.into_batch()? };
            order_limit(&mut r, q, names, exprs)?;
            return Ok(Answer::Rows(r));
        }
    }
    Err(last)
}

/// ORDER BY (an output's alias, its expression or its ordinal) and LIMIT
/// over rows put together from parts. NULLs last ascending, first
/// descending, as the engine orders them.
fn order_limit(r: &mut SqlResult, q: &sqlparser::ast::Query, names: &[String], exprs: &[String]) -> Result<()> {
    use std::cmp::Ordering as O;
    let mut keys: Vec<(usize, bool)> = Vec::new();
    if let Some(ob) = &q.order_by {
        let sqlparser::ast::OrderByKind::Expressions(es) = &ob.kind else { return Err(unsupported("ORDER BY ALL")) };
        for o in es {
            let t = o.expr.to_string();
            let at = match &o.expr {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<usize>().ok().and_then(|i| i.checked_sub(1)),
                    _ => None,
                },
                _ => names.iter().position(|n| n.eq_ignore_ascii_case(&t)).or_else(|| exprs.iter().position(|x| *x == t)),
            }
            .ok_or_else(|| unsupported(format!("ORDER BY {t} over a query answered in parts")))?;
            keys.push((at, o.options.asc != Some(false)));
        }
    }
    let rows = r.batch.rows;
    let mut idx: Vec<usize> = (0..rows).collect();
    if !keys.is_empty() {
        let valid = |c: &FinishedCol, i: usize| -> bool {
            let v = match c {
                FinishedCol::I16 { validity, .. } | FinishedCol::I32 { validity, .. } | FinishedCol::I64 { validity, .. } | FinishedCol::F32 { validity, .. } | FinishedCol::F64 { validity, .. } | FinishedCol::Dec128 { validity, .. } | FinishedCol::Bool { validity, .. } | FinishedCol::Utf8 { validity, .. } | FinishedCol::Bin { validity, .. } => validity,
            };
            v.as_ref().is_none_or(|b| b[i / 8] & (1 << (i % 8)) != 0)
        };
        // Text by its column's collation, as the engine orders it (a MySQL
        // _ci column's 'B' after 'a', not before).
        let colls: Vec<Coll> = r.schema.iter().map(Coll::of).collect();
        let cmp1 = |c: &FinishedCol, coll: Coll, a: usize, b: usize| -> O {
            match (valid(c, a), valid(c, b)) {
                (false, false) => return O::Equal,
                (false, true) => return O::Greater,
                (true, false) => return O::Less,
                _ => {}
            }
            match c {
                FinishedCol::I16 { data, .. } => data[a].cmp(&data[b]),
                FinishedCol::I32 { data, .. } => data[a].cmp(&data[b]),
                FinishedCol::I64 { data, .. } => data[a].cmp(&data[b]),
                // A total order: NaN beside `partial_cmp` broke the sort's.
                FinishedCol::F32 { data, .. } => data[a].total_cmp(&data[b]),
                FinishedCol::F64 { data, .. } => data[a].total_cmp(&data[b]),
                FinishedCol::Dec128 { data, .. } => data[a].cmp(&data[b]),
                FinishedCol::Bool { data, .. } => (data[a / 8] >> (a % 8) & 1).cmp(&(data[b / 8] >> (b % 8) & 1)),
                FinishedCol::Utf8 { offsets, data, .. } if !coll.plain() => {
                    let (x, y) = (&data[offsets[a] as usize..offsets[a + 1] as usize], &data[offsets[b] as usize..offsets[b + 1] as usize]);
                    coll.fold(x).cmp(&coll.fold(y)).then_with(|| x.cmp(y))
                }
                FinishedCol::Utf8 { offsets, data, .. } | FinishedCol::Bin { offsets, data, .. } => {
                    data[offsets[a] as usize..offsets[a + 1] as usize].cmp(&data[offsets[b] as usize..offsets[b + 1] as usize])
                }
            }
        };
        let cols = &r.batch.cols;
        idx.sort_by(|&a, &b| {
            for &(c, asc) in &keys {
                let o = cmp1(&cols[c], colls[c], a, b);
                let o = if asc { o } else { o.reverse() };
                if o != O::Equal {
                    return o;
                }
            }
            a.cmp(&b)
        });
    }
    if let Some(l) = &q.limit {
        let l: usize = l.to_string().parse().map_err(|_| unsupported(format!("LIMIT {l}")))?;
        idx.truncate(l);
    }
    if !keys.is_empty() || idx.len() != rows {
        r.batch = mem::take_rows(&r.batch, &idx);
    }
    Ok(())
}

async fn run_engine(query: &str, sources: &HashMap<String, String>) -> Result<SqlResult> {
    let t0 = std::time::Instant::now();
    let mut p = plan_probed(query, sources).await?;
    if !pushdown_args(&mut p) {
        return execute(p, t0).await;
    }
    match execute(p, t0).await {
        Err(e) if e.to_string().contains("out of range") => {
            if dbg() {
                eprintln!("[sql] a pushed aggregate argument overflowed int8 ({e}); running again without the pushdown");
            }
            // Passes the failed run streamed are not this run's rows.
            let _ = PASS_SINK.try_with(|w| w.borrow_mut().take());
            execute(plan_probed(query, sources).await?, t0).await
        }
        r => r,
    }
}

/// [`plan`] with each table's schema probed as the planner asks for it.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn plan_probed(query: &str, sources: &HashMap<String, String>) -> Result<Plan> {
    // The planner asks for schemas (and row estimates) synchronously; each
    // one not probed yet stops the pass, is probed, and the pass runs again.
    let mut cache: HashMap<(String, String), Vec<ArrowField>> = HashMap::new();
    let mut est: HashMap<(String, String), Option<f64>> = HashMap::new();
    loop {
        let mut missing = None;
        let mut missing_rows = None;
        let r = plan_with(
            query,
            sources,
            &mut |u, t| match cache.get(&(u.to_string(), t.to_string())) {
                Some(s) => Ok(s.clone()),
                None => {
                    missing = Some((u.to_string(), t.to_string()));
                    Err(bad("schema probe"))
                }
            },
            &mut |u, t| match est.get(&(u.to_string(), t.to_string())) {
                Some(r) => Ok(*r),
                None => {
                    missing_rows = Some((u.to_string(), t.to_string()));
                    Err(bad("rows probe"))
                }
            },
        );
        match (r, missing, missing_rows) {
            (Ok(p), _, _) => return Ok(p),
            (Err(_), Some((u, t)), _) => {
                let s = crate::read::read_schema(&u, &t).await?;
                cache.insert((u, t), s);
            }
            (Err(_), None, Some((u, t))) => {
                let r = row_estimate(&u, &t).await;
                est.insert((u, t), r);
            }
            (Err(e), None, None) => return Err(e),
        }
    }
}

/// A table's row count as its server's catalog estimates it (`None`: no
/// SQL server, or no answer — the planner then goes by shape alone).
async fn row_estimate(url: &str, table: &str) -> Option<f64> {
    if mem::is_mem(url) {
        return mem::rows(url, table);
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let src = crate::source::postgres::PgSource::connect(url, 1).await.ok()?;
        let r: (f64,) = sqlx::query_as("SELECT GREATEST(reltuples, 0)::float8 FROM pg_class WHERE oid = $1::regclass")
            .bind(quote(Dialect::Pg, table))
            .fetch_one(src.pool())
            .await
            .ok()?;
        Some(r.0)
    } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
        let src = crate::source::mysql::MySqlSource::connect(url, 1).await.ok()?;
        let (schema, name) = match table.rsplit_once('.') {
            Some((s, n)) => (Some(s.to_string()), n.to_string()),
            None => (None, table.to_string()),
        };
        let r: (Option<f64>,) = sqlx::query_as(
            "SELECT CAST(table_rows AS DOUBLE) FROM information_schema.tables WHERE table_schema = COALESCE(?, DATABASE()) AND table_name = ?",
        )
        .bind(schema)
        .bind(name)
        .fetch_one(src.pool())
        .await
        .ok()?;
        r.0
    } else {
        None
    }
}

/// The bytes the build structure may hold: `APITAP_SQL_BUILD_BYTES` (a test
/// lever), else 40% of the memory limit, else 4 GiB. The probe's pipes get
/// what the structure leaves.
fn build_budget() -> u64 {
    if let Some(b) = std::env::var("APITAP_SQL_BUILD_BYTES")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .filter(|b| *b > 0)
    {
        return b;
    }
    crate::pipeline::mem_limit_bytes().map_or(4 << 30, |m| m / 5 * 2)
}

/// What one join pass reads: the plan, the two keyed sides, the key column
/// names and the build budget.
struct PassCtx<'a> {
    p: &'a Plan,
    /// The probe side's per-part key ranges, once its server answered.
    parts: &'a std::sync::Mutex<Option<Vec<Part>>>,
    /// The dimensions of a star plan, loaded (empty otherwise).
    dims: &'a [star::DimMap],
    k: [(usize, usize); 2],
    names: &'a [String],
    budget: u64,
    t0: std::time::Instant,
    /// A build that outgrew the budget: what the whole range would have
    /// cost, projected from the keys it had reached (0: none).
    over: std::cell::Cell<f64>,
    /// The key ranges still to come after this pass.
    rest: &'a [(i64, i64)],
    /// Grace spill: probe rows an earlier pass read for a later range, on
    /// disk under that range.
    spill: &'a std::cell::RefCell<Vec<((i64, i64), mem::MemTable)>>,
    /// The grace spill is on for this join ([`grace_wanted`]).
    grace: bool,
}

/// Whether a join split into key ranges reads its probe once and spills the
/// later ranges' rows (`grace_on`), or reads each range from its server.
///
/// Read again, a range costs the server a scan and this side a decode the
/// build's keys gate (only matching rows decode). Spilled, it costs this
/// side the whole probe's decode (no gate: the rows of every range pass),
/// the routing, the disk writes and their page cache in the cage. So the
/// spill only where a pass is a slow server scan: a MySQL table whose key no
/// index leads (TPC-H Q13's o_custkey: 69M rows, one connection, 60-100 s a
/// pass). A Postgres side is read as parallel page spans (`ctid`), a pass
/// in seconds: there the spill cost TPC-H Q3 41 → 61 s and 176 → 262 MB and
/// had Q9 OOM-killed at 256 MB (lineitem, 60M rows, no index at all). A
/// side with no server (a stage table) is read whole each pass: spilled.
/// Known limit: a Postgres view or partitioned table is not split by `ctid`,
/// so each of its passes is a full scan on fewer connections: right
/// answers, slow (`APITAP_SQL_GRACE` cannot turn the spill on for it).
/// The re-read passes see one set of rows only through the statement's
/// snapshot (`pin_statement`): the two go together.
async fn grace_wanted(p: &Plan, names: &[String]) -> bool {
    if !grace_on() {
        return false;
    }
    let ps = p.agg_side;
    let Some(i) = p.keys.and_then(|k| k.iter().position(|x| x.0 == ps)) else { return false };
    let side = &p.sides[ps];
    let (on, why) = match side.dialect {
        Dialect::Pg => (false, "Postgres, each range read again as parallel page spans"),
        Dialect::Other => (true, "no server, read once and spilled by range (grace)"),
        Dialect::My => {
            if key_indexed(side, &side.schema[side.read[p.keys.expect("keyed")[i].1]].name).await {
                (false, "an index leads it, each range read from its server")
            } else {
                (true, "no index leads it, read once and spilled by range (grace)")
            }
        }
    };
    if dbg() {
        eprintln!("[sql] probe {} key {}: {why}", side.name, names[i]);
    }
    on
}

/// `APITAP_SQL_GRACE=0` turns the grace spill off (every pass reads its
/// probe range from the server again).
fn grace_on() -> bool {
    std::env::var("APITAP_SQL_GRACE").map_or(true, |v| v.trim() != "0")
}

/// Bytes a build over keys [lo, hi] holds once `pairs` (capacity, not
/// length: the Vec doubles) are in: the pairs themselves plus the structure
/// `key_map` would pick, a direct array over the range at the group count's
/// width when that is smaller than a hash map entry per key.
fn build_cost(pairs_cap: usize, n: usize, groups: usize, lo: i64, hi: i64) -> u64 {
    let width: u64 = if groups < 255 {
        1
    } else if groups < 65535 {
        2
    } else {
        4
    };
    let span = u64::try_from((hi as i128 - lo as i128 + 1).max(0)).unwrap_or(u64::MAX);
    let direct = span.saturating_mul(width);
    let structure = if direct <= DIRECT_MAX { direct.min(24 * n as u64) } else { 24 * n as u64 };
    12 * pairs_cap as u64 + structure
}

/// One join pass over keys [lo, hi]: build the GROUP BY side's key map, then
/// probe the other side into the accumulators. `false` = the build outgrew
/// the budget and nothing was probed; the caller splits the range.
/// A probe side's key range still being asked of its server.
/// A spawned task aborted when its handle is dropped (tokio's detaches).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type PendingKr<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Kr, Vec<Part>)>> + 'a>>;

/// Keys a build reads per statement while the probe side's range is not
/// known yet (idea 1b): the build streams in key order, chunk by chunk, and
/// stops at the probe side's upper end once that arrives.
const OVERLAP_CHUNK: i64 = 16 << 20;

fn overlap_chunk() -> i64 {
    std::env::var("APITAP_SQL_OVERLAP_CHUNK")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
        .unwrap_or(OVERLAP_CHUNK)
}

/// One join pass over keys `range`: build the GROUP BY side's key map, then
/// probe the other side into the accumulators. `false` = the build outgrew
/// the budget and nothing was probed; the caller splits the range.
///
/// With `pending`, `range` is the build side's own and the probe side's is
/// still being computed (an unindexed key is a full scan on its server): the
/// build reads chunk by chunk meanwhile, and `range` narrows to the
/// intersection the moment it is known. Rows read past it are never probed.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
async fn join_pass(
    c: &PassCtx<'_>,
    range: &mut (i64, i64),
    pending: &mut Option<PendingKr<'_>>,
    groups: &mut Groups,
    cgroups: &mut Groups,
    kb: &mut Vec<u8>,
    accs: &mut Vec<Acc>,
    hit: &mut Vec<bool>,
) -> Result<bool> {
    let p = c.p;
    let na = p.aggs.len();
    let (mut lo, mut hi) = *range;
    let between_of = |s: usize, a: i64, b: i64| {
        if p.sides[s].dialect == Dialect::Other || (a, b) == (i64::MIN, i64::MAX) {
            return None;
        }
        let i = c.k.iter().position(|x| x.0 == s).expect("both sides keyed");
        Some(format!("{} BETWEEN {a} AND {b}", c.names[i]))
    };
    let bs = p.group_side;
    let bkey = c.k.iter().find(|x| x.0 == bs).expect("both sides keyed").1;
    let btys = side_tys(&p.sides[bs]);
    let mut pairs: Vec<(i64, u32)> = Vec::new();
    // The highest key built: how far into its range a build that outgrows
    // the budget got (the spans read in key order, so about that far).
    let mut maxk = i64::MIN;
    let mut full = Vec::new();
    // An earlier part's build of the same statement (`BUILD_CACHE`): its map
    // and groups, over the range it narrowed to. Another signature's entry
    // goes before this build holds its own memory beside it. Only a build
    // that starts with no groups (a run's first pass): a later pass's group
    // ids carry on from the passes before it.
    let bsig = BUILD_CACHE.try_with(|_| ()).ok().filter(|_| groups.keys.len() == 0).and_then(|_| build_sig(p, &c.k, c.names, lo, hi));
    let cached: Option<std::rc::Rc<BuildMap>> = bsig.as_ref().and_then(|sg| {
        BUILD_CACHE
            .with(|x| {
                let mut x = x.borrow_mut();
                match x.1.as_ref() {
                    Some(e) if e.sig == *sg => Some((std::rc::Rc::clone(&e.map), e.groups.clone(), e.lo, e.hi)),
                    _ => {
                        x.1 = None;
                        None
                    }
                }
            })
            .map(|(m, g, a, b)| {
                *groups = g;
                (lo, hi) = (a, b);
                m
            })
    });
    // Unique keys under 255 groups over a known range (the common shape: a
    // primary key grouped by a low-cardinality column) fill the direct array
    // as they stream, with no (key, group) pair staged: the build holds the
    // array alone, so a range twice as wide fits one pass. The first repeated
    // key, the 255th group, or a key outside the range turns the array back
    // into pairs and the general path carries on. Over a range still
    // narrowing (`pending`) the array grows chunk by chunk from `base`.
    let base = lo;
    let span = u64::try_from(hi as i128 - lo as i128 + 1).unwrap_or(u64::MAX);
    let finite = (lo, hi) != (i64::MIN, i64::MAX);
    let mut direct: Option<Vec<u8>> = if cached.is_some() {
        None
    } else if pending.is_some() {
        finite.then(Vec::new)
    } else {
        (finite && span <= DIRECT_MAX && span <= c.budget).then(|| vec![u8::MAX; span as usize])
    };
    // The probe side, for the early probe (`Early`) while the build reads.
    let ps = p.agg_side;
    let pkey = c.k.iter().find(|x| x.0 == ps).expect("both sides keyed").1;
    let ptys = side_tys(&p.sides[ps]);
    let pcol = c.names[c.k.iter().position(|x| x.0 == ps).expect("both sides keyed")].clone();
    let mut early: Option<Early> = None;
    let direct_look = |a: &Vec<u8>, k: i64| -> Look<'static> {
        let i = k as i128 - base as i128;
        if i >= 0 && (i as usize) < a.len() && a[i as usize] != u8::MAX {
            Look::One(u32::from(a[i as usize]))
        } else {
            Look::Miss
        }
    };
    let chunk = overlap_chunk();
    let mut cur = lo;
    // The probe read started beside the build's last statement (`Pre`).
    let mut pre: Option<Pre> = None;
    while cached.is_none() {
        // The probe side's range, the moment it is there (or now, when the
        // array would otherwise outgrow its cap before knowing its end).
        if let Some(f) = pending.as_mut() {
            let need = cur.saturating_add(chunk) as i128 - base as i128;
            let must = direct.is_some() && need as u64 > DIRECT_MAX.min(c.budget);
            let got = if must {
                Some(f.as_mut().await?)
            } else {
                match futures::poll!(f.as_mut()) {
                    std::task::Poll::Ready(r) => Some(r?),
                    std::task::Poll::Pending => None,
                }
            };
            if let Some((kr, parts)) = got {
                *c.parts.lock().unwrap_or_else(|e| e.into_inner()) = Some(parts.clone());
                *pending = None;
                if p.sides[ps].dialect == Dialect::Pg
                    && p.star.is_none()
                    && p.sides[p.group_side].flags.is_empty()
                    && parts.iter().filter(|x| x.keys.is_some()).count() > 1
                    && opt_in("APITAP_SQL_KEY_FILTER")
                {
                    early = Some(Early::new(parts));
                }
                match intersect(Kr::Known(lo, hi), kr) {
                    Kr::Known(a, b) => {
                        lo = lo.max(a);
                        hi = b;
                    }
                    _ => {
                        *range = (1, 0);
                        return Ok(true);
                    }
                }
                if dbg() {
                    eprintln!("[sql] probe range known at {:.1}s cpu={:.2}s: build narrowed to [{lo}..{hi}]", c.t0.elapsed().as_secs_f64(), cpu_s());
                }
                cur = cur.max(lo);
                // Keys below `cur` are built already: their parts scan now.
                if let (Some(e), Some(a)) = (early.as_mut(), direct.as_ref()) {
                    e.start(&p.sides[ps], &pcol, lo, hi, cur - 1, &|x, y| bits_direct(a, base, x, y)).await?;
                }
            }
        }
        if cur > hi {
            break;
        }
        // Once the probe side's range is known the rest is one statement:
        // splitting it at the parts' ends was measured slower (12.1-12.8 s
        // against 11.8-12.0 s at 10 GB), the extra statements costing more
        // than the earlier part starts saved.
        let chunk_hi = if pending.is_some() { cur.saturating_add(chunk - 1).min(hi) } else { hi };
        if let Some(a) = &mut direct {
            let need = u64::try_from(chunk_hi as i128 - base as i128 + 1).unwrap_or(u64::MAX);
            if need > DIRECT_MAX || need > c.budget {
                pairs.extend(a.iter().enumerate().filter(|x| *x.1 != u8::MAX).map(|(i, &x)| (base + i as i64, u32::from(x))));
                direct = None;
            } else if (a.len() as u64) < need {
                a.resize(need as usize, u8::MAX);
            }
        }
        // The planner's own pipe count: one pipe per chunk was measured
        // 15.5-17.3 s against 8.6-9.5 s at 10 GB (the decode waits on the
        // server's sending instead of overlapping it).
        // No more pipes than the CPU decodes (twice the cores, at least 2):
        // a star plan's dimension maps share the memory with them.
        let bpipes = if p.star.is_some() { crate::pipeline::cpu_limit_cores().map(|c| ((c.ceil() as usize) * 2).max(2)) } else { None };
        let mut h = open(&p.sides[bs], between_of(bs, cur, chunk_hi), bpipes).await?;
        // The build's last statement, its range final: with memory to hold
        // what it reads meanwhile, the probe side's read starts now, on its
        // own server, instead of after the build (TPC-H Q8 at 8 cores read
        // orders from MySQL, then lineitem from Postgres, one after the other).
        if pre.is_none() && pending.is_none() && early.is_none() && chunk_hi >= hi {
            if let Some(room) = pre_room() {
                let extra = probe_extra(p, ps, &between_of, c.parts, lo, hi);
                // The probe dimensions' gates are known now (the build's
                // keys are not yet): TPC-H Q9's part drops 96% of lineitem,
                // and read ungated beside the build it cost 18 s of CPU more.
                let gates = match &p.star {
                    Some(st) => {
                        let of: Vec<(usize, &star::DimMap)> = st.pkids.iter().filter(|&&k| st.dims[k].key2.is_none()).map(|&k| (st.dims[k].fk, &c.dims[k])).collect();
                        dim_gates(&p.sides[ps], &of)
                    }
                    None => Vec::new(),
                };
                pre = Some(Pre { rows: open_gated(&p.sides[ps], extra, None, None, gates).await?, buf: Vec::new(), bytes: 0, room, done: false });
            }
        }
        loop {
            // The build first; the probe's batches are taken while there is
            // room for them, and wait in the read's channel past it.
            let next = match pre.as_mut().filter(|x| !x.done && x.bytes < x.room) {
                Some(x) => tokio::select! {
                    biased;
                    b = h.recv() => b?,
                    y = x.rows.recv() => {
                        match y? {
                            Some(y) => {
                                x.bytes += y.bytes() as u64;
                                x.buf.push(y);
                            }
                            None => x.done = true,
                        }
                        continue;
                    }
                },
                None => h.recv().await?,
            };
            let Some(b) = next else { break };
            for r in 0..b.rows {
                if !local_ok(&p.sides[bs], &btys, &b, r) {
                    continue;
                }
                let Some(key) = key_at(&b.cols[bkey], r)? else { continue };
                if let Some(st) = &p.star {
                    if !star::build_key(st, c.dims, &b, r, kb, &mut full)? {
                        continue;
                    }
                } else {
                    kb.clear();
                    for &g in &p.groups {
                        encode_group(&b.cols[g], r, kb);
                    }
                }
                if !p.sides[bs].flags.is_empty() {
                    let mut m = 0u64;
                    for (j, f) in p.sides[bs].flags.iter().enumerate() {
                        if peval(f, &b, r, &btys) == Some(true) {
                            m |= 1 << j;
                        }
                    }
                    kb.push(1);
                    kb.extend_from_slice(&8u32.to_le_bytes());
                    kb.extend_from_slice(&(m as i64).to_le_bytes());
                }
                maxk = maxk.max(key);
                let g = groups.id(kb)?;
                if let Some(a) = &mut direct {
                    let at = key as i128 - base as i128;
                    if g < 255 && at >= 0 && (at as usize) < a.len() && a[at as usize] == u8::MAX {
                        a[at as usize] = g as u8;
                        continue;
                    }
                    // Back to pairs: the array's keys first, then this one.
                    pairs.extend(a.iter().enumerate().filter(|x| *x.1 != u8::MAX).map(|(i, &x)| (base + i as i64, u32::from(x))));
                    direct = None;
                }
                pairs.push((key, g));
            }
            // The build's groups against the build budget (where a build's
            // memory is counted): a GROUP BY of a build-side column that is
            // not the key (orders' o_custkey over a join on o_orderkey, 70M
            // orders) grew past the cage and was killed instead of answered
            // in parts by that column. Not the group budget: a part by a
            // probe-side column (TPC-H Q16's ps_suppkey) leaves the build's
            // groups whole, and they fit the build's share. A GROUP BY on
            // the key (`key_groups`) is bounded by the key ranges instead.
            // Past three quarters of the build's share, parts by a GROUP BY
            // column: a range split cannot shrink the groups (the passes
            // share them), only the map, and with the groups near the whole
            // share the map's passes would run to 64 or to one key. Under
            // it the groups count in `cost` with the pairs and the map, so
            // together they stay in the share, a quarter at least for the
            // map. (Half the share sent TPC-H Q10's 550K customer groups,
            // 50 MB of 97 beside a 5 MB map, through four parts: 24.5 → 78 s.)
            let gb = if p.key_groups { 0 } else { groups_bytes(groups, groups.keys.len() * na, groups.keys.len()) };
            if gb > c.budget / 4 * 3 {
                return Err(bad(format!("{} build groups ({gb} B): {GROUPS_OVER} ({} B, 3/4 of the build's)", groups.keys.len(), c.budget / 4 * 3)));
            }
            let cost = gb + match &direct {
                Some(a) => a.len() as u64,
                None => build_cost(pairs.capacity(), pairs.len(), groups.keys.len(), lo, hi),
            } + if p.key_groups {
                // A pass's groups and their accumulators, all held through
                // its probe (TPC-H Q13 10g seed priced the keys alone, and the
                // 64 MiB of accumulators beside them left the probe of 69M
                // orders one pipe).
                groups.bytes + groups.keys.len() as u64 * (na * std::mem::size_of::<Acc>() + 1) as u64
            } else {
                0
            };
            if cost > c.budget {
                if early.as_ref().is_some_and(|e| e.next > 0) {
                    return Err(bad(format!(
                        "the build outgrew its budget ({} B) after the probe had begun; run again with APITAP_SQL_OVERLAP=0",
                        c.budget
                    )));
                }
                h.cancel();
                *range = (lo, hi);
                if maxk != i64::MIN {
                    let f = (maxk as f64 - lo as f64 + 1.0) / (hi as f64 - lo as f64 + 1.0);
                    c.over.set(cost as f64 / f.clamp(0.01, 1.0));
                }
                return Ok(false);
            }
            if let (Some(e), Some(a)) = (early.as_mut(), direct.as_ref()) {
                let ng = groups.keys.len();
                e.drain(p, &ptys, pkey, &|k| direct_look(a, k), ng, accs, hit, false).await?;
            }
        }
        drop(h);
        if dbg() && pending.is_none() && early.is_some() {
            eprintln!("[sql]   build chunk [{cur}..{chunk_hi}] done at {:.1}s cpu={:.2}s", c.t0.elapsed().as_secs_f64(), cpu_s());
        }
        // Every key up to `chunk_hi` is built: the parts that end there scan now.
        if let (Some(e), Some(a)) = (early.as_mut(), direct.as_ref()) {
            e.start(&p.sides[ps], &pcol, lo, hi, chunk_hi, &|x, y| bits_direct(a, base, x, y)).await?;
        }
        if chunk_hi >= hi {
            break;
        }
        cur = chunk_hi + 1;
    }
    *range = (lo, hi);
    let between = |s: usize| between_of(s, lo, hi);
    let reused = cached.is_some();
    let map = match cached {
        Some(m) => m,
        None => std::rc::Rc::new(match direct {
            Some(a) => BuildMap { map: KeyMap::Direct8(base, a), multi: None },
            None => build_map(&mut pairs, groups.keys.len()),
        }),
    };
    let nkeys = match &map.map {
        KeyMap::Direct8(_, a) if dbg() => a.iter().filter(|&&x| x != u8::MAX).count(),
        _ => pairs.len(),
    };
    if dbg() && reused {
        eprintln!("[sql] build {} [{lo}..{hi}]: an earlier part's, {} ({} MiB)", p.sides[bs].name, map.label(), map.bytes() >> 20);
    } else if dbg() {
        eprintln!(
            "[sql] build {} [{lo}..{hi}]: {} keys, {} groups, {} ({} MiB) in {:.1}s cpu={:.2}s",
            p.sides[bs].name,
            nkeys,
            groups.keys.len(),
            map.label(),
            map.bytes() >> 20,
            c.t0.elapsed().as_secs_f64(),
            cpu_s()
        );
    }
    drop(pairs);
    crate::trim_heap();
    // Kept for the next part while it is small beside the budget: between
    // parts it is held through this part's finalize too.
    if let (Some(sg), false) = (bsig, reused) {
        if map.bytes() + groups.bytes <= c.budget / 4 {
            BUILD_CACHE.with(|x| x.borrow_mut().1 = Some(CachedBuild { sig: sg, map: std::rc::Rc::clone(&map), groups: groups.clone(), lo, hi }));
            if dbg() {
                eprintln!("[sql] build {} kept for the next part", p.sides[bs].name);
            }
        }
    }
    // With probe-side GROUP BY fields the accumulators are the combined
    // groups' (grown as the probe meets them).
    if !p.star.as_ref().is_some_and(|st| !st.pgroup.is_empty()) {
        accs.resize(groups.keys.len() * na, Acc::None);
        hit.resize(groups.keys.len(), false);
    }
    if let Some(mut e) = early {
        let ng = groups.keys.len();
        match &map.map {
            KeyMap::Direct8(b0, a) => e.start(&p.sides[ps], &pcol, lo, hi, i64::MAX, &|x, y| bits_direct(a, *b0, x, y)).await?,
            _ => e.start(&p.sides[ps], &pcol, lo, hi, i64::MAX, &|x, y| bits_of(x, y, |k| !matches!(map.look(k), Look::Miss))).await?,
        }
        e.drain(p, &ptys, pkey, &|k| map.look(k), ng, accs, hit, true).await?;
        if dbg() {
            eprintln!(
                "[sql] probe {} by {} page range(s) [{lo}..{hi}]: {} rows, {} matched in {:.1}s cpu={:.2}s",
                p.sides[ps].name,
                e.parts.len(),
                e.probed,
                e.matched,
                c.t0.elapsed().as_secs_f64(),
                cpu_s()
            );
        }
        return Ok(true);
    }
    // Idea 9, the join's runtime key filter (ClickHouse's and DataFusion's
    // dynamic filters): a Postgres probe side is sent the build's keys as a
    // bitmap over [lo, hi], and the server ships only rows whose key the
    // build holds. The rows it drops would have missed the map here; the
    // probe below still looks every row up, so the filter moves bytes, never
    // the answer.
    // Grace spill (the grace hash join): with key ranges still to come, the
    // probe is read once for all of them and the rows of the later ranges
    // go to disk, each under its range; their passes read the disk, not the
    // server again (TPC-H Q13 10g seed: six passes each scanned 69M orders on
    // the server, 60-80 s apiece). A pass whose rows an earlier one spilled
    // reads them, and spills on for the ranges a split left inside them.
    let src = {
        let i = c.spill.borrow().iter().position(|(r, _)| r.0 <= lo && hi <= r.1);
        i.map(|i| c.spill.borrow_mut().remove(i))
    };
    let mut others: Vec<(i64, i64)> = if c.grace {
        c.rest.iter().copied().filter(|r| r.0 > hi && src.as_ref().is_none_or(|(s, _)| s.0 <= r.0 && r.1 <= s.1)).collect()
    } else {
        Vec::new()
    };
    others.sort_unstable();
    let cover_hi = others.iter().map(|r| r.1).fold(hi, i64::max);
    let plain = others.is_empty() && src.is_none();
    if dbg() && !plain {
        eprintln!(
            "[sql] probe {} [{lo}..{hi}]: {}{}",
            p.sides[ps].name,
            if src.is_some() { "rows from disk" } else { "read up to the last range" },
            if others.is_empty() { String::new() } else { format!(", {} later range(s) spilled to disk", others.len()) }
        );
    }
    let pkey_name = pcol.clone();
    let span = u64::try_from(hi as i128 - lo as i128 + 1).unwrap_or(u64::MAX);
    let filter = (plain
        && p.sides[ps].dialect == Dialect::Pg
        && (lo, hi) != (i64::MIN, i64::MAX)
        && span <= KEY_FILTER_MAX_KEYS
        && (map.keys() as u64) * 2 < span
        && opt_in("APITAP_SQL_KEY_FILTER"))
    .then(|| {
        let mut bits = vec![0u8; span.div_ceil(8) as usize];
        map.for_each_key(|k| {
            // A build side the range was not pushed into (github, sheets)
            // holds keys outside it; the probe's BETWEEN drops those rows.
            if (lo..=hi).contains(&k) {
                let i = (k as i128 - lo as i128) as usize;
                bits[i / 8] |= 1 << (i % 8);
            }
        });
        key_filter_sql(&pkey_name, lo, hi, &bits)
    });
    let filter_bytes = filter.as_ref().map_or(0, |f| f.len() as u64);
    // Probe: fold every match into its group, with the pipes the memory left
    // after the build structure holds.
    // What stays held through the probe: the map, the accumulators, the
    // groups' keys and a star plan's dimensions.
    // The decoder's gates (no server work): the build's keys over the pass,
    // and the probe dimensions' keys. A probe row they drop is one the fold
    // would drop, before its other columns decode.
    let mut gates = Vec::new();
    if filter.is_none() {
        // The build's keys gate this range's rows only; under the grace
        // spill the read carries later ranges' rows too, so that gate is
        // the plain read's. The dimensions' gates hold for every range: a
        // row they drop is dropped in whichever range it falls (TPC-H Q9's
        // part gate drops 96% of lineitem, which grace spilled to disk).
        if plain {
            gates.extend(gate_of(&p.sides[ps], pkey, lo, hi, map.keys() as u64, |g| map.for_each_key(|k| g.set(k))));
        }
        if let Some(st) = &p.star {
            let of: Vec<(usize, &star::DimMap)> = st.pkids.iter().filter(|&&k| st.dims[k].key2.is_none()).map(|&k| (st.dims[k].fk, &c.dims[k])).collect();
            gates.extend(dim_gates(&p.sides[ps], &of));
        }
        if dbg() && !gates.is_empty() {
            eprintln!("[sql] probe {} gated by {} key set(s), {} KiB", p.sides[ps].name, gates.len(), gates.iter().map(|g| g.bytes()).sum::<u64>() >> 10);
        }
    }
    let held = map.bytes() + (accs.len() as u64) * 32 + groups.bytes + c.dims.iter().map(|d| d.bytes()).sum::<u64>() + gates.iter().map(|g| g.bytes()).sum::<u64>();
    let mut parallel = crate::pipeline::mem_limit_bytes().map(|m| crate::read_impl::read_pipes_for(m.saturating_sub(held + PROBE_HEADROOM)));
    // And no more pipes than the CPU decodes: a probe is decode-bound, so a
    // pipe past twice the cores buys no speed, only its buffers and its
    // socket's (kernel memory the cgroup charges too: five 256 MB-cage
    // pipes beside a 57 MiB map measured 213 MiB anon + 40 MiB kernel, and
    // were OOM-killed). The cap rises with the cores.
    if let Some(c) = crate::pipeline::cpu_limit_cores() {
        let cap = probe_pipe_cap(c, &p.sides[ps], held, gate_density(&gates));
        parallel = Some(parallel.map_or(cap, |p| p.min(cap)));
    }
    // A probe gated to an eighth decodes an eighth: the memory model prices
    // each pipe at a whole decode and gave TPC-H Q3/Q8/Q10 two or three
    // pipes beside 88-101 MiB; at four they peaked at 174-198 MB of 256 and
    // ran 12-23% faster.
    if gate_density(&gates) <= 1.0 / 8.0 && held <= 112 << 20 {
        parallel = parallel.map(|n| n.max(PROBE_MIN_PIPES));
    }
    // (APITAP_SQL_PROBE_PIPES=n: the probe's pipes, for measuring the rule.)
    if let Some(n) = std::env::var("APITAP_SQL_PROBE_PIPES").ok().and_then(|v| v.parse::<usize>().ok()).filter(|&n| n > 0) {
        parallel = Some(n);
    }
    if dbg() {
        eprintln!("[sql] probe {}: {} pipe(s), held {} MiB, gate density {:.3}", p.sides[ps].name, parallel.map_or("auto".into(), |n| n.to_string()), held >> 20, gate_density(&gates));
    }
    if filter.is_some() {
        // A filtered probe is the server's scan, not this side's decode: one
        // span per pipe, each statement carrying the bitmap once.
        let room = crate::pipeline::mem_limit_bytes().map_or(u64::MAX, |m| m.saturating_sub(map.bytes() + (64 << 20)));
        // Each statement holds the bitmap's text more than once on its way
        // out (the span list, the query buffer): priced at 4x per pipe.
        let fit = (room / (4 * filter_bytes.max(1))).clamp(1, 5) as usize;
        parallel = Some(parallel.unwrap_or(5).max(5).min(fit));
        if dbg() {
            eprintln!("[sql] key filter: {} keys over {span} as a {} KiB bitmap, {} pipe(s)", map.keys(), filter_bytes >> 10, parallel.unwrap_or(1));
        }
    }
    // A pass over part of the keys scans the pages they lie in.
    let extra = if filter.is_some() { between(ps) } else { probe_extra(p, ps, &between_of, c.parts, lo, cover_hi) };
    if dbg() && extra.as_deref().is_some_and(|x| x.contains("ctid")) {
        eprintln!("[sql] probe pass [{lo}..{hi}] scans {}", extra.as_deref().unwrap_or(""));
    }
    // The read started beside the build (its filter is this one: the pass
    // and the probe range were final then), or a fresh one.
    let (mut h, held_back) = if src.is_some() {
        (None, Vec::new())
    } else {
        match pre.take().filter(|_| filter.is_none() && plain) {
            Some(x) => {
                if dbg() {
                    eprintln!("[sql] probe {} began beside the build: {} batch(es), {} MiB held", p.sides[ps].name, x.buf.len(), x.bytes >> 20);
                }
                (Some(x.rows), x.buf)
            }
            None => (Some(open_gated(&p.sides[ps], extra, parallel, filter, gates).await?), Vec::new()),
        }
    };
    let (mut probed, mut matched) = (0u64, 0u64);
    let gcross = p.star.as_ref().map(|st| star::cross_values(st, &groups.keys)).unwrap_or_default();
    let sx = p.star.as_ref().map(|st| (st, c.dims, gcross.as_slice()));
    let ng = groups.keys.len();
    // Each group's CASE flags (the key's last field), when the build has any.
    let gflags: Option<Vec<u64>> = (!p.sides[bs].flags.is_empty()).then(|| {
        groups
            .keys
            .iter()
            .map(|k| group_fields(k).last().map_or(0, |f| i64::from_le_bytes(f.1.try_into().unwrap_or([0; 8])) as u64))
            .collect()
    });
    let pg = p.star.as_ref().is_some_and(|st| !st.pgroup.is_empty());
    // The later ranges' writers (made on their first row), and the routing
    // of a batch's rows to them: a row of this range stays, a NULL key
    // matches nothing anywhere.
    let mut writers: Vec<((i64, i64), Option<mem::Writer>)> = others.iter().map(|r| (*r, None)).collect();
    let mut route = |b: &ArrowBatch| -> Result<()> {
        if writers.is_empty() {
            return Ok(());
        }
        let mut idx: Vec<Vec<usize>> = vec![Vec::new(); writers.len()];
        for r in 0..b.rows {
            let Some(k) = key_at(&b.cols[pkey], r)? else { continue };
            if (lo..=hi).contains(&k) {
                continue;
            }
            let i = writers.partition_point(|(g, _)| g.1 < k);
            if i < writers.len() && writers[i].0 .0 <= k {
                idx[i].push(r);
            }
        }
        for (i, ix) in idx.iter().enumerate() {
            if ix.is_empty() {
                continue;
            }
            let w = writers[i].1.get_or_insert_with(|| mem::Writer::on_disk(vec![p.sides[ps].schema[0].clone(); b.cols.len()]));
            w.push(mem::take_rows(b, ix))?;
        }
        Ok(())
    };
    let mut step = |b: &mut ArrowBatch| -> Result<()> {
        probed += b.rows as u64;
        matched += fold_probe(p, &ptys, pkey, b, |k| map.look(k), ng, accs, hit, sx, gflags.as_deref(), Some(&mut *cgroups))?;
        if pg {
            groups_check(cgroups, accs.len(), hit.len())?;
        }
        Ok(())
    };
    for mut b in held_back {
        route(&b)?;
        step(&mut b)?;
    }
    if let Some(h) = h.as_mut() {
        while let Some(mut b) = h.recv().await? {
            route(&b)?;
            step(&mut b)?;
        }
    }
    drop(h);
    if let Some((_, t)) = src {
        t.for_each(&mut |mut b| {
            route(&b)?;
            step(&mut b)
        })?;
    }
    drop(step);
    drop(route);
    for (r, w) in writers {
        // A range no row fell in is spilled empty: its pass reads nothing.
        let t = match w {
            Some(w) => w.finish()?,
            None => mem::Writer::on_disk(Vec::new()).finish()?,
        };
        c.spill.borrow_mut().push((r, t));
    }
    crate::trim_heap();
    if dbg() {
        eprintln!(
            "[sql] probe {} [{lo}..{hi}]: {probed} rows, {matched} matched in {:.1}s",
            p.sides[ps].name,
            c.t0.elapsed().as_secs_f64()
        );
    }
    Ok(true)
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn execute(mut p: Plan, t0: std::time::Instant) -> Result<SqlResult> {
    let held = pin_snapshots(&mut p).await?;
    let r = execute_pinned(p, t0).await;
    for h in held {
        h.close().await;
    }
    r
}

/// Every side in the plan: the tables, the subqueries' tables, the
/// dimensions and the late reads.
fn visit_sides(p: &mut Plan, f: &mut dyn FnMut(&mut Side)) {
    fn one(s: &mut Side, f: &mut dyn FnMut(&mut Side)) {
        for sm in &mut s.semis {
            one(&mut sm.sub.side, f);
        }
        f(s);
    }
    for s in &mut p.sides {
        one(s, f);
    }
    if let Some(st) = &mut p.star {
        for d in &mut st.dims {
            one(&mut d.side, f);
        }
        for l in &mut st.late {
            one(&mut l.side, f);
        }
    }
}

/// One exported snapshot per Postgres server the query reads, every read of
/// it pinned there. A query reads a table in several statements (key-range
/// passes, `ctid` page spans, a late read, a subquery's passes); each its
/// own snapshot let a row an UPDATE moved between two statements be missed
/// or counted twice (a pass reads only the pages its keys lay in when the
/// parts were asked). The key ranges are asked in the same snapshot.
/// MySQL sides keep one statement per key range, each row once by its key.
/// `APITAP_SQL_SNAPSHOT=0`: off.
async fn pin_snapshots(p: &mut Plan) -> Result<Vec<crate::source::postgres::HeldSnapshot>> {
    if std::env::var("APITAP_SQL_SNAPSHOT").is_ok_and(|v| v == "0") {
        return Ok(Vec::new());
    }
    let mut urls: Vec<String> = Vec::new();
    visit_sides(p, &mut |s| {
        if s.dialect == Dialect::Pg && !s.url.contains("__apitap_snapshot=") && !urls.contains(&s.url) {
            urls.push(s.url.clone());
        }
    });
    let mut held = Vec::with_capacity(urls.len());
    for u in &urls {
        match crate::source::postgres::HeldSnapshot::open(u).await {
            Ok(h) => held.push(h),
            Err(e) => {
                for h in held {
                    h.close().await;
                }
                return Err(e);
            }
        }
    }
    let pinned: Vec<(String, String)> = urls
        .iter()
        .zip(&held)
        .map(|(u, h)| (u.clone(), format!("{u}{}__apitap_snapshot={}", if u.contains('?') { '&' } else { '?' }, h.id)))
        .collect();
    visit_sides(p, &mut |s| {
        if let Some((_, to)) = pinned.iter().find(|(u, _)| *u == s.url) {
            s.url = to.clone();
        }
    });
    if dbg() && !held.is_empty() {
        eprintln!("[sql] {} snapshot(s) pinned", held.len());
    }
    Ok(held)
}

async fn execute_pinned(p: Plan, t0: std::time::Instant) -> Result<SqlResult> {
    // The subqueries' key sets, before any row they filter.
    semi::load(&p).await?;
    let mut groups = Groups::default();
    // Build group × probe GROUP BY fields (a star plan grouping at the probe).
    let mut cgroups = Groups::default();
    let mut kb = Vec::with_capacity(64);
    let mut accs: Vec<Acc> = Vec::new();
    // Join only: the groups some probe row matched. A build group nothing
    // matched is not an inner-join row and is left out of the answer.
    let mut hit: Vec<bool> = Vec::new();
    let na = p.aggs.len();

    // Idea 1: the join key's range, intersected, pushed into both scans.
    let mut range = None;
    let key_names: Vec<String> = p
        .keys
        .map(|k| k.iter().map(|&(s, at)| p.sides[s].schema[p.sides[s].read[at]].name.clone()).collect())
        .unwrap_or_default();
    let mut pending: Option<PendingKr<'_>> = None;
    let probe_parts: std::sync::Mutex<Option<Vec<Part>>> = std::sync::Mutex::new(None);
    if let Some(k) = p.keys {
        let names = &key_names;
        let bi = k.iter().position(|x| x.0 == p.group_side).expect("both sides keyed");
        let (bside, pside) = (&p.sides[k[bi].0], &p.sides[k[1 - bi].0]);
        // Idea 1b: when the probe side's range costs a scan, the build does
        // not wait for it (see `join_pass`).
        if bside.dialect != Dialect::Other && pside.dialect != Dialect::Other && !std::env::var("APITAP_SQL_OVERLAP").is_ok_and(|v| v == "0") {
            // Its own task: a future only moves when polled, and the build
            // polls it between chunks only.
            // Aborted when the query is done with it: a range still running
            // (an unindexed MIN/MAX, 30 s for Q13's o_custkey) no longer
            // outlives the query, nor runs once per part.
            let mut task = AbortOnDrop(tokio::spawn(key_range_of(pside.dialect, pside.name.clone(), pside.url.clone(), pside.table.clone(), names[1 - bi].clone())));
            let mut f: PendingKr<'_> = Box::pin(async move {
                (&mut task.0).await.map_err(|e| Error::Transfer(format!("sql: key range task: {e}")))?
            });
            let rb = key_range(bside, &names[bi]).await?;
            match rb {
                Kr::Known(a, b) => {
                    if let std::task::Poll::Ready(rp) = futures::poll!(f.as_mut()) {
                        let rp = rp?;
                        *probe_parts.lock().unwrap_or_else(|e| e.into_inner()) = Some(rp.1.clone());
                        range = Some(match intersect(rb, rp.0) {
                            Kr::Known(x, y) => (x, y),
                            _ => (1, 0),
                        });
                    } else {
                        range = Some((a, b));
                        pending = Some(f);
                    }
                }
                _ => {
                    let rp = f.await?;
                    *probe_parts.lock().unwrap_or_else(|e| e.into_inner()) = Some(rp.1);
                    let rp = rp.0;
                    range = Some(match intersect(rb, rp) {
                        Kr::Known(x, y) => (x, y),
                        Kr::Empty => (1, 0),
                        Kr::Unknown => (i64::MIN, i64::MAX),
                    });
                }
            }
            if dbg() {
                eprintln!(
                    "[sql] build side range {rb:?} in {:.1}s{}",
                    t0.elapsed().as_secs_f64(),
                    if pending.is_some() { "; the probe side's range is still running, the build starts" } else { "" }
                );
            }
        }
    }
    if let (Some(k), None) = (p.keys, range) {
        let names = &key_names;
        let (ra, rb) = tokio::try_join!(key_range(&p.sides[k[0].0], &names[0]), key_range(&p.sides[k[1].0], &names[1]))?;
        range = Some(match intersect(ra, rb) {
            Kr::Known(a, b) => (a, b),
            Kr::Empty => (1, 0),
            // Neither side has a server to ask: every key, split if needed.
            Kr::Unknown => (i64::MIN, i64::MAX),
        });
        if dbg() {
            eprintln!("[sql] key ranges {ra:?} ∩ {rb:?} = {range:?} in {:.1}s", t0.elapsed().as_secs_f64());
        }
    }
    let empty_range = range.is_some_and(|(lo, hi)| lo > hi);
    // A star plan's dimensions, each read once into its map.
    let dims = match (&p.star, empty_range) {
        (Some(st), false) => star::load(st).await?,
        _ => Vec::new(),
    };
    crate::trim_heap();

    if let (Some(k), false) = (p.keys, empty_range) {
        // Idea 5c: the build runs per key range. One that outgrows the build
        // budget is abandoned and its range halved; every pass shares the
        // groups and accumulators, so the answer is one.
        let (lo, hi) = range.expect("a join has a key range");
        // The dimensions' maps are held through every pass.
        let budget = build_budget().saturating_sub(dims.iter().map(|d| d.bytes()).sum());
        // Groups per key range (`key_groups`): the answer so far.
        let mut kept: (Groups, Vec<Acc>, Vec<bool>) = (Groups::default(), Vec::new(), Vec::new());
        let pgroup = p.star.as_ref().is_some_and(|st| !st.pgroup.is_empty());
        // Unordered and unlimited, under `run_one_out`: each pass's rows go
        // straight to a stage table (TPC-H Q13 10g seed: 9.9M customers' groups
        // kept across the passes broke the group budget, and the query ran
        // again in 16 parts, each reading 69M orders).
        let stream = p.key_groups
            && p.order.is_empty()
            && p.limit.is_none()
            && p.star.as_ref().is_none_or(|st| st.late.is_empty())
            && PASS_SINK.try_with(|_| ()).is_ok();
        let mut todo = vec![(lo, hi)];
        // Asked once, when the build first splits.
        let mut grace: Option<bool> = None;
        let spill = std::cell::RefCell::new(Vec::new());
        let mut passes = 0u32;
        while let Some(mut rg) = todo.pop() {
            let rest = todo.clone();
            let ctx = PassCtx { p: &p, parts: &probe_parts, dims: &dims, k, names: &key_names, budget, t0, over: std::cell::Cell::new(0.0), rest: &rest, spill: &spill, grace: grace.unwrap_or(false) };
            let done = join_pass(&ctx, &mut rg, &mut pending, &mut groups, &mut cgroups, &mut kb, &mut accs, &mut hit).await?;
            if p.key_groups {
                // This pass's groups are final: the rows the answer can
                // still use move to `kept`; the pass's memory goes.
                if done {
                    if pgroup {
                        groups = combine(&groups, std::mem::take(&mut cgroups))?;
                    }
                    let (g, a, h) = strip(&p, std::mem::take(&mut groups), std::mem::take(&mut accs), std::mem::take(&mut hit))?;
                    let (g, a, h) = collate(&p, g, a, h)?;
                    if stream {
                        let r = result(&p, &g, &a, Some(&h))?;
                        PASS_SINK.with(|w| -> Result<()> {
                            let mut w = w.borrow_mut();
                            w.get_or_insert_with(|| mem::Writer::new(r.schema.clone())).push(r.batch)
                        })?;
                    } else {
                        retain(&p, &g, &a, &h, &mut kept)?;
                    }
                } else {
                    (groups, accs, hit) = (Groups::default(), Vec::new(), Vec::new());
                    cgroups = Groups::default();
                }
            }
            if let Some(mut f) = pending.take() {
                // The pass stopped before the probe side's range arrived. A
                // range still running after a second is a full scan (a key
                // no index leads: TPC-H Q13's o_custkey, 69M MySQL rows, 30 s
                // of MIN/MAX); the next passes go on without it and narrow
                // when it comes.
                match tokio::time::timeout(std::time::Duration::from_secs(1), f.as_mut()).await {
                    Ok(r) => {
                        if let Kr::Known(x, y) = intersect(Kr::Known(rg.0, rg.1), r?.0) {
                            rg = (rg.0.max(x), y);
                        } else {
                            rg = (1, 0);
                        }
                    }
                    Err(_) => {
                        if dbg() {
                            eprintln!("[sql] the probe side's range is still running at {:.1}s: the passes go on without it", t0.elapsed().as_secs_f64());
                        }
                        pending = Some(f);
                    }
                }
            }
            let (a, b) = rg;
            match done {
                true => passes += 1,
                false if a > b => passes += 1,
                false if a < b && p.sides[p.group_side].dialect == Dialect::Other => {
                    // A build side with no server reads every row each pass
                    // (no BETWEEN to push): a narrower range would read and
                    // overflow again, about log2(span) times. Its groups
                    // count in the cost (under 3/4 of the share, beside the
                    // map): parts by a GROUP BY column shrink both, so that
                    // is tried first (GROUPS_OVER), unless the GROUP BY is
                    // the key itself (no column to part by but the key).
                    let parts = if p.key_groups { String::new() } else { format!(": {GROUPS_OVER}") };
                    return Err(bad(format!(
                        "the build side {} outgrew the build budget ({budget} B) and has no server to read it by key range{parts}; raise the memory limit",
                        p.sides[p.group_side].name
                    )));
                }
                false if a < b => {
                    // As many even key ranges as the projected cost needs at
                    // 60% of the budget each (halving re-read most of the
                    // range again when a build broke the budget early:
                    // TPC-H Q3 threw two builds away). The projection runs
                    // low: the parallel spans' highest key is ahead of the
                    // rows read (Q3 projected 162 MB of ~200), so a range
                    // sized at 85% broke the budget again. At least two.
                    let proj = ctx.over.get();
                    let span = b as i128 - a as i128 + 1;
                    // Under the grace spill at 40%: the ranges after the
                    // first read the disk, so one more costs a build's
                    // statement, and the first range's probe reads the whole
                    // side from its server with the pipes a smaller build
                    // leaves (TPC-H Q13 10g seed: ranges of 990K customers held
                    // 68 MiB and read 69M orders on 2 pipes in 70 s; of 550K,
                    // 38 MiB and 6 pipes, 47 s).
                    if grace.is_none() {
                        grace = Some(grace_wanted(&p, &key_names).await);
                    }
                    let share = if grace == Some(true) { 0.4 } else { 0.6 };
                    let n = ((proj / (share * budget.max(1) as f64)).ceil() as i128).clamp(2, 64).min(span);
                    if dbg() {
                        eprintln!("[sql] build of {a}..{b} past {budget} B (whole range ~{} B): {n} ranges", proj as u64);
                    }
                    for i in (0..n).rev() {
                        let x = a as i128 + span * i / n;
                        let y = a as i128 + span * (i + 1) / n - 1;
                        todo.push((x as i64, y as i64));
                    }
                }
                false => {
                    return Err(bad(format!(
                        "the build rows of one key ({a}) exceed the build budget ({budget} B)"
                    )))
                }
            }
        }
        if dbg() {
            eprintln!("[sql] join done in {passes} pass(es)");
        }
        let _ = BUILD_CACHE.try_with(|x| {
            let mut x = x.borrow_mut();
            if !x.0 {
                x.1 = None;
            }
        });
        if p.key_groups {
            (groups, accs, hit) = kept;
        } else if pgroup {
            groups = combine(&groups, std::mem::take(&mut cgroups))?;
        }
    } else if p.keys.is_none() {
        // One table: group and aggregate as it streams.
        let s = p.group_side;
        let tys = side_tys(&p.sides[s]);
        let mut h = open(&p.sides[s], None, None).await?;
        // The groups seen lately, matched on their encoded key before the
        // map is asked (a GROUP BY of a few groups asks it a few times).
        let mut seen: Vec<(Vec<u8>, usize)> = Vec::new();
        while let Some(b) = h.recv().await? {
            groups_check(&groups, accs.len(), 0)?;
            let cols = arg_cols(&p, &b);
            for r in 0..b.rows {
                if !local_ok(&p.sides[s], &tys, &b, r) {
                    continue;
                }
                kb.clear();
                for &g in &p.groups {
                    encode_group(&b.cols[g], r, &mut kb);
                }
                let g = match seen.iter().find(|x| x.0 == kb) {
                    Some(x) => x.1,
                    None => {
                        let g = groups.id(&kb)? as usize;
                        if seen.len() < 64 {
                            seen.push((kb.clone(), g));
                        }
                        g
                    }
                };
                if accs.len() < (g + 1) * na {
                    accs.resize((g + 1) * na, Acc::None);
                }
                for (i, (a, c)) in p.aggs.iter().zip(&cols).enumerate() {
                    fold(&mut accs[g * na + i], a.f, a.ty, arg_at(a, c, &b, r)?)?;
                }
            }
        }
    }
    // A star plan's check fields leave the group keys (per pass already when
    // the groups are a pass's own).
    if !p.key_groups {
        let (g, a, h) = strip(&p, std::mem::take(&mut groups), std::mem::take(&mut accs), std::mem::take(&mut hit))?;
        (groups, accs, hit) = collate(&p, g, a, h)?;
    }
    // A star plan's late GROUP BY columns, for the answer's rows only.
    if let Some(st) = p.star.as_ref().filter(|st| !st.late.is_empty()) {
        let rows: Vec<usize> = if p.limit.is_some() {
            order_rows(&p, &groups, &accs, Some(&hit), true)
        } else {
            (0..groups.keys.len()).filter(|&g| hit.get(g) == Some(&true)).collect()
        };
        (groups, accs, hit) = star::read_late(st, std::mem::take(&mut groups), std::mem::take(&mut accs), na, &rows).await?;
        if dbg() {
            eprintln!("[sql] late columns read for {} row(s) in {:.1}s", rows.len(), t0.elapsed().as_secs_f64());
        }
    }
    // A global aggregate over no rows is one row (COUNT 0, SUM NULL).
    let mut keep = p.keys.is_some().then_some(hit.as_slice());
    let no_groups = p.groups.is_empty() && p.star.as_ref().is_none_or(|st| st.gpos.is_empty());
    if no_groups && keep.is_none_or(|h| !h.iter().any(|&x| x)) && (keep.is_some() || groups.keys.is_empty()) {
        // A global aggregate over no (matched) rows: one row of empty
        // accumulators (COUNT 0, SUM NULL).
        groups = Groups::default();
        groups.id(&[])?;
        accs = vec![Acc::None; na];
        keep = None;
    }
    let res = result(&p, &groups, &accs, keep)?;
    if dbg() {
        eprintln!("[sql] done: {} rows in {:.1}s", res.batch.rows, t0.elapsed().as_secs_f64());
    }
    Ok(res)
}

/// Decode one group-key field back to its column value.
/// Field `n` of an encoded key, without collecting the others.
#[inline]
fn nth_field(key: &[u8], n: usize) -> Option<(u8, &[u8])> {
    let mut i = 0;
    let mut at = 0;
    while i < key.len() {
        let tag = key[i];
        i += 1;
        let (lo, hi) = if tag == 0 {
            (i, i)
        } else {
            let len = u32::from_le_bytes(key.get(i..i + 4)?.try_into().ok()?) as usize;
            i += 4;
            (i, i + len)
        };
        if at == n {
            return Some((tag, key.get(lo..hi)?));
        }
        i = hi;
        at += 1;
    }
    None
}

/// A numeric literal of a HAVING (`-` allowed, parentheses too).
fn number_lit(e: &Expr) -> Option<VExpr> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => number(n).ok(),
            _ => None,
        },
        Expr::Nested(x) => number_lit(x),
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => match number_lit(expr)? {
            VExpr::Int(i) => Some(VExpr::Int(-i)),
            VExpr::Dec(v, s) => Some(VExpr::Dec(-v, s)),
            VExpr::Float(f) => Some(VExpr::Float(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// One HAVING conjunct on a group's accumulator: exact for integer and
/// decimal aggregates, by floats for the rest; NULL fails.
fn having_ok(acc: Acc, ag: &Agg, op: star::Op, v: &VExpr) -> bool {
    use std::cmp::Ordering as O;
    let (lv, ls) = match v {
        VExpr::Int(i) => (Some(i128::from(*i)), 0i8),
        VExpr::Dec(m, s) => (Some(*m), *s),
        _ => (None, 0),
    };
    let fl = match v {
        VExpr::Int(i) => *i as f64,
        VExpr::Dec(m, s) => *m as f64 / pow10f(*s),
        VExpr::Float(f) => *f,
        _ => return false,
    };
    let ord = match acc {
        Acc::None if matches!(ag.f, AggFn::Count | AggFn::CountStar) => lv.and_then(|l| dec_cmp(0, 0, l, ls)).or_else(|| 0f64.partial_cmp(&fl)),
        Acc::None => None,
        Acc::I(x) => lv.and_then(|l| dec_cmp(i128::from(x), 0, l, ls)).or_else(|| (x as f64).partial_cmp(&fl)),
        Acc::N(n) => lv.and_then(|l| dec_cmp(i128::from(n), 0, l, ls)).or_else(|| (n as f64).partial_cmp(&fl)),
        Acc::D(x) => {
            let sc = match ag.ty {
                Some(Ty::Dec(s)) => s,
                _ => 0,
            };
            lv.and_then(|l| dec_cmp(x, sc, l, ls)).or_else(|| (x as f64 / pow10f(sc)).partial_cmp(&fl))
        }
        Acc::F(x) => x.partial_cmp(&fl),
        Acc::Avg(s, n) => (s / n as f64).partial_cmp(&fl),
    };
    let Some(ord) = ord else { return false };
    match op {
        star::Op::Eq => ord == O::Equal,
        star::Op::Ne => ord != O::Equal,
        star::Op::Lt => ord == O::Less,
        star::Op::Le => ord != O::Greater,
        star::Op::Gt => ord == O::Greater,
        star::Op::Ge => ord != O::Less,
    }
}

/// Field `at` of a group key, without listing the others.
fn group_field(key: &[u8], at: usize) -> (u8, &[u8]) {
    let mut i = 0;
    let mut f = 0;
    while i < key.len() {
        let tag = key[i];
        i += 1;
        let n = if tag == 0 { 0 } else {
            let n = u32::from_le_bytes(key[i..i + 4].try_into().expect("4 bytes")) as usize;
            i += 4;
            n
        };
        if f == at {
            return (tag, &key[i..i + n]);
        }
        i += n;
        f += 1;
    }
    (0, &key[0..0])
}

fn group_fields(key: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < key.len() {
        let tag = key[i];
        i += 1;
        if tag == 0 {
            out.push((0, &key[i..i]));
            continue;
        }
        let n = u32::from_le_bytes(key[i..i + 4].try_into().expect("4 bytes")) as usize;
        i += 4;
        out.push((tag, &key[i..i + n]));
        i += n;
    }
    out
}

struct ColOut {
    valid: Vec<bool>,
    i64s: Vec<i64>,
    f64s: Vec<f64>,
    d128: Vec<i128>,
    strs: Vec<Vec<u8>>,
}

fn bitmap(v: &[bool]) -> Option<Vec<u8>> {
    if v.iter().all(|&x| x) {
        return None;
    }
    let mut bm = vec![0u8; v.len().div_ceil(8)];
    for (i, &x) in v.iter().enumerate() {
        if x {
            bm[i / 8] |= 1 << (i % 8);
        }
    }
    Some(bm)
}

/// `keep`: when set, only the groups it marks are rows of the answer.
/// A finished pass's matched groups into `kept`, in ORDER BY order; with a
/// LIMIT only the rows still among the first `limit` stay.
fn retain(p: &Plan, groups: &Groups, accs: &[Acc], hit: &[bool], kept: &mut (Groups, Vec<Acc>, Vec<bool>)) -> Result<()> {
    let na = p.aggs.len();
    for r in order_rows(p, groups, accs, Some(hit), true) {
        let g = kept.0.id(&groups.keys[r])? as usize;
        if kept.1.len() < (g + 1) * na {
            kept.1.resize((g + 1) * na, Acc::None);
            kept.2.resize(g + 1, false);
        }
        kept.1[g * na..(g + 1) * na].copy_from_slice(&accs[r * na..(r + 1) * na]);
        kept.2[g] = true;
        if p.limit.is_none() && g % 65536 == 65535 {
            groups_check(&kept.0, kept.1.len(), kept.2.len())?;
        }
    }
    if p.limit.is_none() {
        groups_check(&kept.0, kept.1.len(), kept.2.len())?;
    }
    if let Some(l) = p.limit {
        if kept.0.keys.len() > 2 * l.max(64) {
            // Down to the first `limit` again.
            let rows = order_rows(p, &kept.0, &kept.1, Some(&kept.2), true);
            let mut next: (Groups, Vec<Acc>, Vec<bool>) = (Groups::default(), Vec::new(), Vec::new());
            for r in rows {
                let g = next.0.id(&kept.0.keys[r])? as usize;
                next.1.extend_from_slice(&kept.1[r * na..(r + 1) * na]);
                next.2.push(true);
                debug_assert_eq!(next.2.len(), g + 1);
            }
            *kept = next;
        }
    }
    Ok(())
}

/// The answer's groups in ORDER BY order, LIMIT applied (`keep`: the groups
/// an inner join matched).
fn order_rows(p: &Plan, groups: &Groups, accs: &[Acc], keep: Option<&[bool]>, built: bool) -> Vec<usize> {
    let na = p.aggs.len();
    let ng = groups.keys.len();
    // Row order.
    let mut rows: Vec<usize> = (0..ng).filter(|&g| keep.is_none_or(|k| k.get(g) == Some(&true))).collect();
    // A GROUP BY column's field in the key (a star plan lays them out by
    // table; `built`: the key as the passes build it, late columns not in).
    let fi = |g: usize| match &p.star {
        Some(st) if built => st.gpos_rt[g].expect("ORDER BY reads no late column"),
        Some(st) => st.gpos[g],
        None => g,
    };
    if let Some(res) = &p.residual {
        rows.retain(|&r| res.ok(&group_fields(&groups.keys[r]), &fi));
    }
    if !p.having.is_empty() {
        rows.retain(|&r| p.having.iter().all(|(a, op, v)| having_ok(accs[r * na + a], &p.aggs[*a], *op, v)));
    }
    // A sort value: exact for integers and decimals (scaled i128, one scale
    // per column; f64 dropped digits past 2^53 and tied distinct sums),
    // f64 for floats and expressions, bytes for text.
    #[derive(PartialEq)]
    enum K {
        Null,
        I(i128),
        F(f64),
        B(Vec<u8>),
    }
    fn kcmp(a: &K, b: &K) -> std::cmp::Ordering {
        use std::cmp::Ordering as O;
        match (a, b) {
            (K::Null, K::Null) => O::Equal,
            (K::Null, _) => O::Greater,
            (_, K::Null) => O::Less,
            (K::I(x), K::I(y)) => x.cmp(y),
            (K::F(x), K::F(y)) => x.partial_cmp(y).unwrap_or(O::Equal),
            (K::I(x), K::F(y)) => (*x as f64).partial_cmp(y).unwrap_or(O::Equal),
            (K::F(x), K::I(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(O::Equal),
            (K::B(x), K::B(y)) => x.cmp(y),
            (K::B(_), _) => O::Greater,
            (_, K::B(_)) => O::Less,
        }
    }
    let val = |r: usize, c: usize| -> K {
        match &p.out[c].1 {
            OutCol::Group(g) => {
                let (t, b) = group_field(&groups.keys[r], fi(*g));
                match t {
                    1 => K::I(i64::from_le_bytes(b.try_into().unwrap_or([0; 8])) as i128),
                    2 => K::F(f64::from_le_bytes(b.try_into().unwrap_or([0; 8]))),
                    3 => K::I(i128::from_le_bytes(b.try_into().unwrap_or([0; 16]))),
                    4 => K::I(i128::from(b[0])),
                    5 => K::B(p.gcoll.get(*g).map_or_else(|| b.to_vec(), |c| c.fold(b).into_owned())),
                    _ => K::Null,
                }
            }
            OutCol::Expr(o) => match oeval(o, &accs[r * na..(r + 1) * na], &p.aggs) {
                Some(x) => K::F(x),
                None => K::Null,
            },
            OutCol::Agg(a) => match accs[r * na + a] {
                Acc::None => K::Null,
                Acc::I(x) => K::I(x as i128),
                Acc::D(x) => K::I(x),
                Acc::F(x) => K::F(x),
                Acc::Avg(s, n) => K::F(s / n as f64),
                Acc::N(n) => K::I(n as i128),
            },
        }
    };
    if !p.order.is_empty() {
        // Each row's sort values once (they were made again, a Vec each, at
        // every comparison: TPC-H Q10 sorted 550K groups for 20), then under
        // a LIMIT the first rows selected before they are sorted. Ties go by
        // the row's place, as the stable sort had them. NULLs sort last
        // ascending, first descending.
        let keys: Vec<Vec<K>> = rows.iter().map(|&r| p.order.iter().map(|&(c, _)| val(r, c)).collect()).collect();
        let cmp = |x: &usize, y: &usize| {
            for (j, &(_, asc)) in p.order.iter().enumerate() {
                let o = kcmp(&keys[*x][j], &keys[*y][j]);
                let o = if asc { o } else { o.reverse() };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            x.cmp(y)
        };
        let mut idx: Vec<usize> = (0..rows.len()).collect();
        if let Some(l) = p.limit.filter(|&l| l > 0 && l < idx.len()) {
            idx.select_nth_unstable_by(l - 1, cmp);
            idx.truncate(l);
        }
        idx.sort_unstable_by(cmp);
        rows = idx.into_iter().map(|i| rows[i]).collect();
    }
    if let Some(l) = p.limit {
        rows.truncate(l);
    }
    rows
}

fn result(p: &Plan, groups: &Groups, accs: &[Acc], keep: Option<&[bool]>) -> Result<SqlResult> {
    let na = p.aggs.len();
    // Output kinds.
    let gs = &p.sides[p.group_side];
    let kinds: Vec<ArrowKind> = p
        .out
        .iter()
        .map(|(_, oc)| match oc {
            OutCol::Group(g) => match &p.star {
                Some(st) => st.gkind[*g],
                None => gs.schema[gs.read[p.groups[*g]]].kind,
            },
            OutCol::Expr(_) => ArrowKind::Float64,
            OutCol::Agg(a) => {
                let ag = &p.aggs[*a];
                match (ag.f, ag.ty) {
                    (AggFn::Count | AggFn::CountStar, _) => ArrowKind::Int64,
                    (AggFn::Avg, _) | (_, Some(Ty::Float)) => ArrowKind::Float64,
                    (_, Some(Ty::Dec(s))) => ArrowKind::Decimal { p: 38, s },
                    _ => ArrowKind::Int64,
                }
            }
        })
        .collect();
    let rows = order_rows(p, groups, accs, keep, false);
    // A GROUP BY column's field in the key (a star plan lays them out by table).
    let fi = |g: usize| p.star.as_ref().map_or(g, |st| st.gpos[g]);
    let mut cols = Vec::new();
    let mut schema = Vec::new();
    for (c, (name, oc)) in p.out.iter().enumerate() {
        // Only the vector this column's kind reads is filled: five per row
        // (an empty Vec among them) cost ~57 bytes a cell, and 2M groups of
        // a staged GROUP BY died in the 256 MiB cage here (TPC-H Q11 10g seed).
        let k = kinds[c];
        let (wi, wf, wd, ws) = match k {
            ArrowKind::Float32 | ArrowKind::Float64 => (false, true, false, false),
            ArrowKind::Decimal { .. } => (false, false, true, false),
            ArrowKind::Bool | ArrowKind::Utf8 | ArrowKind::Binary => (false, false, false, true),
            _ => (true, false, false, false),
        };
        let mut o = ColOut {
            valid: Vec::with_capacity(rows.len()),
            i64s: Vec::with_capacity(if wi { rows.len() } else { 0 }),
            f64s: Vec::with_capacity(if wf { rows.len() } else { 0 }),
            d128: Vec::with_capacity(if wd { rows.len() } else { 0 }),
            strs: Vec::new(),
        };
        let push = |o: &mut ColOut, ok: bool, i: i64, f: f64, d: i128, st: Option<&[u8]>| {
            o.valid.push(ok);
            if wi {
                o.i64s.push(i);
            }
            if wf {
                o.f64s.push(f);
            }
            if wd {
                o.d128.push(d);
            }
            if ws {
                o.strs.push(st.map(<[u8]>::to_vec).unwrap_or_default());
            }
        };
        for &r in &rows {
            match oc {
                OutCol::Group(g) => {
                    let (t, b) = group_field(&groups.keys[r], fi(*g));
                    push(
                        &mut o,
                        t != 0,
                        if t == 1 { i64::from_le_bytes(b.try_into().unwrap_or([0; 8])) } else { 0 },
                        if t == 2 { f64::from_le_bytes(b.try_into().unwrap_or([0; 8])) } else { 0.0 },
                        if t == 3 { i128::from_le_bytes(b.try_into().unwrap_or([0; 16])) } else { 0 },
                        (t == 5 || t == 4).then_some(b),
                    );
                }
                OutCol::Expr(x) => {
                    let v = oeval(x, &accs[r * na..(r + 1) * na], &p.aggs);
                    push(&mut o, v.is_some(), 0, v.unwrap_or(0.0), 0, None);
                }
                OutCol::Agg(a) => {
                    let acc = accs[r * na + a];
                    let ag = &p.aggs[*a];
                    let (ok, i, f, d) = match acc {
                        Acc::None if matches!(ag.f, AggFn::Count | AggFn::CountStar) => (true, 0, 0.0, 0),
                        Acc::None => (false, 0, 0.0, 0),
                        Acc::I(x) => (true, x, x as f64, x as i128),
                        Acc::D(x) => (true, 0, 0.0, x),
                        Acc::F(x) => (true, 0, x, 0),
                        Acc::Avg(s, n) => (true, 0, s / n as f64, 0),
                        Acc::N(n) => (true, i64::try_from(n).unwrap_or(i64::MAX), n as f64, 0),
                    };
                    // AVG of a decimal: the sum was folded as f64 of the
                    // scaled value already (`as_f64`).
                    push(&mut o, ok, i, f, d, None);
                }
            }
        }
        let validity = bitmap(&o.valid);
        let col = match k {
            ArrowKind::Int16 => FinishedCol::I16 { validity, data: o.i64s.iter().map(|&x| x as i16).collect() },
            ArrowKind::Int32 | ArrowKind::Date32 => FinishedCol::I32 { validity, data: o.i64s.iter().map(|&x| x as i32).collect() },
            ArrowKind::Int64 | ArrowKind::TimestampUtc | ArrowKind::TimestampNaive => FinishedCol::I64 { validity, data: o.i64s },
            ArrowKind::Float32 => FinishedCol::F32 { validity, data: o.f64s.iter().map(|&x| x as f32).collect() },
            ArrowKind::Float64 => FinishedCol::F64 { validity, data: o.f64s },
            ArrowKind::Decimal { .. } => FinishedCol::Dec128 { validity, data: o.d128 },
            ArrowKind::Bool => {
                let mut bm = vec![0u8; rows.len().div_ceil(8)];
                for (i, s) in o.strs.iter().enumerate() {
                    if s.first() == Some(&1) {
                        bm[i / 8] |= 1 << (i % 8);
                    }
                }
                FinishedCol::Bool { validity, data: bm }
            }
            ArrowKind::Utf8 | ArrowKind::Binary => {
                let mut offsets = Vec::with_capacity(rows.len() + 1);
                let mut data = Vec::new();
                offsets.push(0i32);
                for s in &o.strs {
                    data.extend_from_slice(s);
                    offsets.push(i32::try_from(data.len()).map_err(|_| bad("result string column past 2 GiB"))?);
                }
                if k == ArrowKind::Utf8 {
                    FinishedCol::Utf8 { validity, offsets, data }
                } else {
                    FinishedCol::Bin { validity, offsets, data }
                }
            }
        };
        cols.push(col);
        // A GROUP BY text output keeps its column's collation: a stage of it
        // groups and orders as the source column does, and so does the sort
        // of an answer put together from parts (`order_limit`).
        let coll = match oc {
            OutCol::Group(g) if matches!(k, ArrowKind::Utf8) => p.gcoll.get(*g).copied().unwrap_or_default(),
            _ => Coll::default(),
        };
        schema.push(ArrowField { name: name.clone(), kind: k, nullable: true, rewritten: false, padded: coll.pad, ci: coll.ci, ai: coll.ai });
    }
    Ok(SqlResult { schema, batch: ArrowBatch { rows: rows.len(), cols } })
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
                "lineitem" => vec![
                    f("l_orderkey", ArrowKind::Int64),
                    f("l_extendedprice", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("l_discount", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("l_shipdate", ArrowKind::Date32),
                    f("l_shipmode", ArrowKind::Utf8),
                    f("l_commitdate", ArrowKind::Date32),
                    f("l_receiptdate", ArrowKind::Date32),
                    f("l_suppkey", ArrowKind::Int64),
                    f("l_returnflag", ArrowKind::Utf8),
                    f("l_quantity", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("l_partkey", ArrowKind::Int64),
                    f("l_shipinstruct", ArrowKind::Utf8),
                ],
                "orders" => vec![
                    f("o_orderkey", ArrowKind::Int64),
                    f("o_orderdate", ArrowKind::Date32),
                    f("o_orderpriority", ArrowKind::Utf8),
                    f("o_custkey", ArrowKind::Int64),
                    f("o_shippriority", ArrowKind::Int32),
                    f("o_totalprice", ArrowKind::Decimal { p: 15, s: 2 }),
                ],
                "customer" => vec![
                    f("c_custkey", ArrowKind::Int64),
                    f("c_nationkey", ArrowKind::Int32),
                    f("c_mktsegment", ArrowKind::Utf8),
                    f("c_name", ArrowKind::Utf8),
                    f("c_acctbal", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("c_phone", ArrowKind::Utf8),
                    f("c_address", ArrowKind::Utf8),
                    f("c_comment", ArrowKind::Utf8),
                ],
                "supplier" => vec![f("s_suppkey", ArrowKind::Int64), f("s_nationkey", ArrowKind::Int32), f("s_name", ArrowKind::Utf8)],
                "nation" => vec![f("n_nationkey", ArrowKind::Int32), f("n_regionkey", ArrowKind::Int32), f("n_name", ArrowKind::Utf8)],
                "region" => vec![f("r_regionkey", ArrowKind::Int32), f("r_name", ArrowKind::Utf8)],
                "part" => vec![
                    f("p_partkey", ArrowKind::Int64),
                    f("p_type", ArrowKind::Utf8),
                    f("p_brand", ArrowKind::Utf8),
                    f("p_container", ArrowKind::Utf8),
                    f("p_size", ArrowKind::Int32),
                    f("p_name", ArrowKind::Utf8),
                ],
                "partsupp" => vec![
                    f("ps_partkey", ArrowKind::Int64),
                    f("ps_suppkey", ArrowKind::Int64),
                    f("ps_supplycost", ArrowKind::Decimal { p: 15, s: 2 }),
                ],
                _ => return Err(bad("no such table")),
            })
        }
    }

    fn srcs() -> HashMap<String, String> {
        HashMap::from([
            ("pg".to_string(), "postgres://u@h/db".to_string()),
            ("my".to_string(), "mysql://u@h/db".to_string()),
        ])
    }

    const Q: &str = "SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) AS n \
        FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey \
        WHERE l.l_shipdate > DATE '1995-03-15' AND o_orderdate < DATE '1995-03-15' \
        GROUP BY 1 ORDER BY 1";

    /// ORDER BY a decimal SUM is exact: two sums 1 apart past 2^53 compared
    /// equal as f64 and kept their arrival order.
    #[test]
    fn order_by_a_decimal_sum_is_exact_past_f64() {
        let q = Q.replace("ORDER BY 1", "ORDER BY 2 DESC");
        let p = plan(&q, &srcs(), &mut schemas()).unwrap();
        let mut g = Groups::default();
        for t in [&b"1-URGENT"[..], &b"2-HIGH"[..]] {
            let mut k = vec![5u8];
            k.extend_from_slice(&(t.len() as u32).to_le_bytes());
            k.extend_from_slice(t);
            g.id(&k).unwrap();
        }
        let big = 1i128 << 60;
        let accs = [Acc::D(big), Acc::N(1), Acc::D(big + 1), Acc::N(1)];
        assert_eq!(order_rows(&p, &g, &accs, None, false), vec![1, 0]);
    }

    /// GROUP BY a MySQL `_ai_ci` column: values equal under it are one group,
    /// shown as the first value met, its aggregates merged; ORDER BY folds.
    #[test]
    fn groups_equal_under_the_collation_are_one() {
        let mut p = plan(Q, &srcs(), &mut schemas()).unwrap();
        p.gcoll = vec![Coll { ci: true, ai: true, ..Coll::default() }];
        let mut g = Groups::default();
        for t in ["Brazil", "banana", "BRAZIL", "Brésil", "bresil", "Apple"] {
            let mut k = vec![5u8];
            k.extend_from_slice(&(t.len() as u32).to_le_bytes());
            k.extend_from_slice(t.as_bytes());
            g.id(&k).unwrap();
        }
        let accs: Vec<Acc> = (1..=6u64).flat_map(|n| [Acc::D(i128::from(n)), Acc::N(n)]).collect();
        let (g2, a2, h2) = collate(&p, g, accs, vec![true; 6]).unwrap();
        let names: Vec<String> = g2.keys.iter().map(|k| String::from_utf8(group_fields(k)[0].1.to_vec()).unwrap()).collect();
        assert_eq!(names, ["Brazil", "banana", "Brésil", "Apple"]);
        assert!(matches!(a2[1], Acc::N(4)), "Brazil + BRAZIL counted 1 + 3");
        assert!(matches!(a2[5], Acc::N(9)), "Brésil + bresil counted 4 + 5");
        // ORDER BY 1 under the collation: Apple, banana, Brazil, Brésil.
        assert_eq!(order_rows(&p, &g2, &a2, Some(&h2), false), vec![3, 1, 0, 2]);
    }

    /// The headline query plans as designed: the GROUP BY side builds, the
    /// other probes, each side gets its own conjunct (qualifier dropped) and
    /// only the columns it needs, and the aggregate is exact decimal at scale 4.
    #[test]
    fn the_headline_query_plans_build_probe_and_pushdown() {
        let p = plan(Q, &srcs(), &mut schemas()).unwrap();
        assert_eq!(p.group_side, 1, "orders holds the GROUP BY column");
        assert_eq!(p.agg_side, 0);
        assert_eq!(p.sides[0].conj, vec!["l_shipdate > DATE '1995-03-15'".to_string()]);
        assert_eq!(p.sides[1].conj, vec!["o_orderdate < DATE '1995-03-15'".to_string()]);
        let names = |s: &Side| s.read.iter().map(|&c| s.schema[c].name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&p.sides[0]), ["l_orderkey", "l_extendedprice", "l_discount"]);
        assert_eq!(names(&p.sides[1]), ["o_orderkey", "o_orderpriority"]);
        assert_eq!(p.aggs[0].ty, Some(Ty::Dec(4)));
        assert_eq!(p.aggs[1].f, AggFn::CountStar);
        assert_eq!(p.out.len(), 3);
        assert_eq!(p.order, vec![(0, true)]);
    }

    /// The probe side's aggregate argument is computed by Postgres as one
    /// scaled int8; the probe reads the key and that column only.
    #[test]
    fn the_headline_probe_ships_its_argument_as_one_scaled_int8() {
        let mut p = plan(Q, &srcs(), &mut schemas()).unwrap();
        // Off unless asked for: the plan is left as it was.
        if std::env::var("APITAP_SQL_PUSH_ARGS").is_err() {
            let before = p.sides[0].read.clone();
            assert!(!pushdown_args(&mut p));
            assert_eq!(p.sides[0].read, before);
        }
        assert!(rewrite_args(&mut p));
        let s = &p.sides[0];
        let names: Vec<&str> = s.read.iter().map(|&c| s.schema[c].name.as_str()).collect();
        assert_eq!(names, ["l_orderkey", "_apitap_a0"]);
        assert_eq!(
            s.computed,
            vec![(
                "_apitap_a0".to_string(),
                "(((\"l_extendedprice\") * (((1::numeric) - (\"l_discount\"))))) * 10000::numeric".to_string()
            )]
        );
        assert!(matches!(p.aggs[0].arg, Some(VExpr::Scaled(_, 4))));
        assert_eq!(p.keys.unwrap()[0], (0, 0));
        // A MySQL probe side, or a float argument, keeps the columns.
        let q2 = Q.replace("SUM(l_extendedprice * (1 - l_discount))", "SUM(l_extendedprice / 2)");
        let mut p2 = plan(&q2, &srcs(), &mut schemas()).unwrap();
        assert!(!rewrite_args(&mut p2));
    }

    /// TPC-H Q12's shape plans (the GROUP BY on lineitem, the CASE sums over
    /// orders), and a CASE evaluates to its first true arm, else its ELSE,
    /// else NULL, a NULL condition not true, its branches at one type.
    #[test]
    fn a_case_aggregate_plans_and_evaluates_per_row() {
        let q12 = "SELECT l_shipmode, \
            SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
            SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
            FROM my.orders o JOIN pg.lineitem l ON o.o_orderkey = l.l_orderkey \
            WHERE l.l_shipmode IN ('MAIL', 'SHIP') AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate \
            AND l.l_receiptdate >= DATE '1994-01-01' AND l.l_receiptdate < DATE '1995-01-01' \
            GROUP BY l_shipmode ORDER BY l_shipmode";
        let p = plan(q12, &srcs(), &mut schemas()).unwrap();
        let os = p.agg_side;
        assert_eq!(p.sides[os].table, "orders");
        assert_eq!(p.sides[p.group_side].table, "lineitem");
        assert!(matches!(p.aggs[0].arg, Some(VExpr::Case { .. })));
        assert_eq!(p.aggs[0].ty, Some(Ty::Int));
        // A batch of the orders side's read columns: one priority per row,
        // the fourth NULL.
        let side = &p.sides[os];
        let prio = ["1-URGENT", "3-MEDIUM", "2-HIGH", "", "5-LOW"];
        let cols: Vec<FinishedCol> = side
            .read
            .iter()
            .map(|&c| match side.schema[c].name.as_str() {
                "o_orderpriority" => {
                    let mut offsets = vec![0i32];
                    let mut data = Vec::new();
                    for x in prio {
                        data.extend_from_slice(x.as_bytes());
                        offsets.push(data.len() as i32);
                    }
                    FinishedCol::Utf8 { validity: Some(vec![0b1_0111]), offsets, data }
                }
                _ => FinishedCol::I64 { validity: None, data: vec![1, 2, 3, 4, 5] },
            })
            .collect();
        let b = ArrowBatch { rows: prio.len(), cols };
        let val = |e: &VExpr, r: usize| match eval(e, &b, r).unwrap() {
            Num::Int(x) => Some(x as i128),
            Num::Dec(x) => Some(x),
            Num::Null => None,
            Num::Float(_) => panic!("float"),
        };
        let high: Vec<_> = (0..5).map(|r| val(p.aggs[0].arg.as_ref().unwrap(), r)).collect();
        let low: Vec<_> = (0..5).map(|r| val(p.aggs[1].arg.as_ref().unwrap(), r)).collect();
        assert_eq!(high, [Some(1), Some(0), Some(1), Some(0), Some(0)]);
        // NULL <> '1-URGENT' is not true: the NULL row lands in the ELSE.
        assert_eq!(low, [Some(0), Some(1), Some(0), Some(0), Some(1)]);
        // No ELSE: NULL; an integer branch beside a decimal one, at its
        // scale; the simple form (CASE x WHEN v).
        let q = |agg: &str| format!("SELECT l_shipmode, {agg} FROM my.orders o JOIN pg.lineitem l ON o.o_orderkey = l.l_orderkey GROUP BY 1");
        let p2 = plan(&q("SUM(CASE o_orderpriority WHEN '2-HIGH' THEN 7 END)"), &srcs(), &mut schemas()).unwrap();
        let e2 = p2.aggs[0].arg.clone().unwrap();
        assert_eq!((0..5).map(|r| val(&e2, r)).collect::<Vec<_>>(), [None, None, Some(7), None, None]);
        let p3 = plan(&q("SUM(CASE WHEN o_orderpriority = '5-LOW' THEN 2.50 ELSE 1 END)"), &srcs(), &mut schemas()).unwrap();
        assert_eq!(p3.aggs[0].ty, Some(Ty::Dec(2)));
        let e3 = p3.aggs[0].arg.clone().unwrap();
        assert_eq!((0..5).map(|r| val(&e3, r)).collect::<Vec<_>>(), [Some(100), Some(100), Some(100), Some(100), Some(250)]);
        // A CASE naming both tables is refused by name.
        // Conditions on orders, results on lineitem, grouped by lineitem: the
        // conditions are not the build table's, refused.
        assert!(plan(&q("SUM(CASE WHEN o_orderpriority = '5-LOW' THEN l_discount ELSE 0 END)"), &srcs(), &mut schemas()).is_err());
    }

    /// TPC-H Q3 and Q5 as written (comma joins, the join conditions in the
    /// WHERE) plan as stars: lineitem streams, orders builds, the rest are
    /// dimensions; Q3's l_orderkey groups as o_orderkey; Q5's nation hangs
    /// under customer and the supplier's nation is a check across the parts.
    #[test]
    fn tpch_q3_and_q5_plan_as_stars() {
        let q3 = "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority \
            FROM pg.customer, my.orders, pg.lineitem \
            WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey \
            AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' \
            GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10";
        let p = plan(q3, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("orders", "lineitem"));
        assert_eq!((p.group_side, p.agg_side), (0, 1));
        assert_eq!(st.dims.len(), 1);
        assert_eq!(st.dims[0].side.table, "customer");
        assert_eq!(st.dims[0].side.conj.len(), 1, "c_mktsegment goes to customer's server");
        assert_eq!(st.bkids, vec![0]);
        assert!(st.pkids.is_empty() && st.cross.is_empty());
        assert_eq!(st.gkind, vec![ArrowKind::Int64, ArrowKind::Date32, ArrowKind::Int32]);
        let bnames: Vec<&str> = st.bfields.iter().map(|&i| p.sides[0].schema[p.sides[0].read[i]].name.as_str()).collect();
        assert_eq!(bnames, ["o_orderkey", "o_orderdate", "o_shippriority"]);
        assert_eq!(p.limit, Some(10));

        let q5 = "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue \
            FROM pg.customer, my.orders, pg.lineitem, my.supplier, my.nation, my.region \
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey \
            AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
            AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' \
            GROUP BY n_name ORDER BY revenue DESC";
        let p = plan(q5, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("orders", "lineitem"));
        let names: Vec<&str> = st.dims.iter().map(|d| d.side.table.as_str()).collect();
        assert_eq!(names, ["customer", "supplier", "nation", "region"]);
        assert_eq!(st.dims[2].up, star::Up::Dim(0), "nation under customer");
        assert_eq!(st.dims[3].up, star::Up::Dim(2), "region under nation");
        assert_eq!(st.dims[1].up, star::Up::Main(1), "supplier under lineitem");
        assert_eq!(st.cross.len(), 1, "the supplier's nation checked against the customer's");
        assert_eq!(st.hidden.iter().filter(|&&h| h).count(), 1);
        assert_eq!(st.gkind, vec![ArrowKind::Utf8]);

        // GROUP BY s_name alone builds on supplier instead (lineitem still
        // streams; orders, customer hang under it).
        let by_s = q5.replace("SELECT n_name,", "SELECT s_name,").replace("GROUP BY n_name", "GROUP BY s_name");
        let p = plan(&by_s, &srcs(), &mut schemas()).unwrap();
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("supplier", "lineitem"));
        // GROUP BY columns of two branches under the aggregated table: no
        // one table to build them on, refused by name.
        // GROUP BY columns of two branches under the aggregated table: the
        // build part holds one, the probe part groups the other.
        let two = q5.replace("SELECT n_name,", "SELECT s_name, o_orderpriority,").replace("GROUP BY n_name", "GROUP BY s_name, o_orderpriority");
        let p = plan(&two, &srcs(), &mut schemas()).unwrap();
        assert_eq!(p.star.as_ref().expect("a star").pgroup.len(), 1);
    }

    /// TPC-H Q10 plans with the customer's key from the order's FK and its
    /// wide columns read late (one value per customer, ORDER BY on revenue
    /// only); the nation's name stays in the payload (25 of them).
    #[test]
    fn tpch_q10_reads_the_customer_columns_late() {
        let q10 = "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment \
            FROM pg.customer, my.orders, pg.lineitem, my.nation \
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' \
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey \
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20";
        let p = plan(q10, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        let c = st.dims.iter().position(|d| d.side.table == "customer").expect("customer");
        assert!(st.dims[c].key_first);
        assert_eq!(st.late.len(), 1);
        let late: Vec<&str> = st.late[0].cols.iter().map(|&(_, i)| st.late[0].side.schema[st.late[0].side.read[i]].name.as_str()).collect();
        assert_eq!(late, ["c_name", "c_acctbal", "c_phone", "c_address", "c_comment"]);
        // c_custkey and n_name are built; the five are not.
        assert_eq!(st.gpos_rt.iter().filter(|x| x.is_some()).count(), 2);
        let cust_fields: Vec<&str> = st.dims[c].fields.iter().map(|&i| st.dims[c].side.schema[st.dims[c].side.read[i]].name.as_str()).collect();
        assert!(cust_fields.is_empty(), "the customer's payload is its nation's: {cust_fields:?}");
        // ORDER BY a wide column before the LIMIT: read with the rows.
        let by_name = q10.replace("ORDER BY revenue DESC", "ORDER BY c_name");
        let p = plan(&by_name, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert!(st.late.iter().all(|l| l.cols.iter().all(|&(g, _)| g != 1)), "c_name is not late");
    }

    /// TPC-H Q4's correlated EXISTS and Q18's IN (SELECT … GROUP BY …
    /// HAVING …) plan as key-set filters of orders: the subquery's own WHERE
    /// goes to its server, the correlation stays here.
    #[test]
    fn tpch_q4_and_q18_subqueries_filter_by_key_sets() {
        let q4 = "SELECT o_orderpriority, COUNT(*) AS order_count FROM my.orders \
            WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' \
            AND EXISTS (SELECT * FROM pg.lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) \
            GROUP BY o_orderpriority ORDER BY o_orderpriority";
        let p = plan(q4, &srcs(), &mut schemas()).unwrap();
        assert_eq!(p.sides.len(), 1);
        let s = &p.sides[0].semis;
        assert_eq!(s.len(), 1);
        assert!(!s[0].anti && !s[0].not_in && s[0].sub.having.is_none());
        assert_eq!(s[0].sub.side.table, "lineitem");
        assert_eq!(s[0].sub.side.conj.len(), 1, "l_commitdate < l_receiptdate goes to lineitem's server");
        assert_eq!(p.sides[0].conj.len(), 2, "the dates go to orders' server");
        let not_q = q4.replace("AND EXISTS", "AND NOT EXISTS");
        assert!(plan(&not_q, &srcs(), &mut schemas()).unwrap().sides[0].semis[0].anti);

        let q18 = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) \
            FROM pg.customer, my.orders, pg.lineitem \
            WHERE o_orderkey IN (SELECT l_orderkey FROM pg.lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) \
            AND c_custkey = o_custkey AND o_orderkey = l_orderkey \
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
            ORDER BY o_totalprice DESC, o_orderdate LIMIT 100";
        let p = plan(q18, &srcs(), &mut schemas()).unwrap();
        assert_eq!(p.sides[0].table, "orders");
        let s = &p.sides[0].semis;
        assert_eq!(s.len(), 1);
        let h = s[0].sub.having.as_ref().expect("a HAVING");
        assert_eq!(h.agg.f, AggFn::Sum);
        assert!(p.key_groups, "o_orderkey groups: each pass is final");
        // A correlated IN, or a subquery over a join, is refused by name.
        let bad_q = q4.replace(
            "EXISTS (SELECT * FROM pg.lineitem WHERE",
            "o_orderkey IN (SELECT l_orderkey FROM pg.lineitem l JOIN my.orders o2 ON l.l_orderkey = o2.o_orderkey WHERE",
        );
        assert!(plan(&bad_q, &srcs(), &mut schemas()).is_err());
    }

    /// LIKE by characters: `%` any run (empty too), `_` exactly one.
    /// A pass scans the pages of the parts its keys meet, bounded above
    /// too: the open-ended last part is not taken when its keys lie past
    /// the pass (it was, and every pass scanned to the table's end).
    #[test]
    fn a_pass_scans_only_its_pages() {
        let parts = vec![
            Part { pages: (0, Some(100)), keys: Some((1, 1000)) },
            Part { pages: (100, Some(200)), keys: Some((1001, 2000)) },
            Part { pages: (200, None), keys: Some((2001, 3000)) },
        ];
        assert_eq!(ctid_span(&parts, 1, 1500).as_deref(), Some("ctid >= '(0,0)'::tid AND ctid < '(200,0)'::tid"));
        assert_eq!(ctid_span(&parts, 1500, 2500).as_deref(), Some("ctid >= '(100,0)'::tid"));
        assert_eq!(ctid_span(&parts, 1, 3000), None);
        assert_eq!(ctid_span(&parts, 5000, 6000).as_deref(), Some("false"));
    }

    /// COUNT of a text or date column counts its non-NULL rows (it was
    /// refused as arithmetic on the column); SUM of one is still refused.
    #[test]
    fn count_of_a_text_column_counts_non_null_rows() {
        let p = plan("SELECT o_orderpriority, COUNT(o_orderpriority) FROM my.orders GROUP BY o_orderpriority", &srcs(), &mut schemas()).unwrap();
        let a = &p.aggs[0];
        assert_eq!(a.ty, Some(Ty::Int));
        let Some(e) = &a.arg else { panic!("an argument") };
        let at = match e {
            VExpr::Case { arms, .. } => match arms[0].0 {
                PExpr::IsNull(i, true) => i,
                _ => panic!("IS NOT NULL"),
            },
            _ => panic!("a CASE"),
        };
        let mut cols: Vec<FinishedCol> = (0..at).map(|_| FinishedCol::I64 { validity: None, data: vec![0, 0] }).collect();
        cols.push(FinishedCol::Utf8 { validity: Some(vec![0b01]), offsets: vec![0, 5, 5], data: b"1-URG".to_vec() });
        let b = ArrowBatch { rows: 2, cols };
        assert!(matches!(eval(e, &b, 0).unwrap(), Num::Int(1)));
        assert!(matches!(eval(e, &b, 1).unwrap(), Num::Null));
        assert!(plan("SELECT SUM(o_orderpriority) FROM my.orders", &srcs(), &mut schemas()).is_err());
    }

    #[test]
    fn like_matches_sql() {
        for (t, p, want) in [
            ("PROMO BURNISHED COPPER", "PROMO%", true),
            ("STANDARD PROMO", "PROMO%", false),
            ("STANDARD PROMO", "%PROMO", true),
            ("abc", "a_c", true),
            ("abbc", "a_c", false),
            ("", "%", true),
            ("x", "", false),
            ("forest green", "%green%", true),
            ("ä1b", "_1%", true),
            ("aXbXc", "a%b%c", true),
            ("aXbXd", "a%b%c", false),
        ] {
            assert_eq!(like(t, p), want, "{t:?} LIKE {p:?}");
        }
    }

    /// TPC-H Q14: the CASE's condition is part's (a flag bit at the build),
    /// its results lineitem's; the SELECT is arithmetic over two sums.
    #[test]
    fn tpch_q14_plans_a_build_side_case_and_an_output_expression() {
        let q14 = "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) \
            / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
            FROM pg.lineitem, my.part WHERE l_partkey = p_partkey \
            AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'";
        let p = plan(q14, &srcs(), &mut schemas()).unwrap();
        assert_eq!(p.sides[p.group_side].table, "part");
        assert_eq!(p.sides[p.group_side].flags.len(), 1);
        assert!(matches!(p.aggs[0].arg, Some(VExpr::CaseB { .. })));
        assert_eq!(p.aggs[0].ty, Some(Ty::Dec(4)));
        assert!(matches!(p.out[0].1, OutCol::Expr(_)));
        // The answer of two groups (flag set or not) merged: 100 * 30 / 120.
        let aggs = p.aggs.clone();
        let accs = [Acc::D(300_000), Acc::D(1_200_000)];
        let OutCol::Expr(o) = &p.out[0].1 else { panic!("expr") };
        assert_eq!(oeval(o, &accs, &aggs), Some(25.0));
        assert_eq!(oeval(o, &[Acc::D(1), Acc::D(0)], &aggs), None, "a division by zero is NULL");
        // A CASE over part inside an expression is refused by name.
        let bad_q = q14.replace("SUM(CASE WHEN", "SUM(2 * CASE WHEN");
        assert!(plan(&bad_q, &srcs(), &mut schemas()).is_err());
    }

    /// A date's civil year, either side of the epoch and of leap days.
    #[test]
    fn year_of_days_is_the_civil_year() {
        for (d, y) in [(0, 1970), (-1, 1969), (9131, 1995), (10956, 1999), (10957, 2000), (11016, 2000), (11322, 2000), (11323, 2001), (-719_468, 0)] {
            assert_eq!(year_of_days(d), y, "day {d}");
        }
    }

    /// A derived table in FROM is inlined, and EXTRACT(YEAR …) in it is a
    /// virtual column the GROUP BY (by alias) groups on.
    #[test]
    fn a_derived_table_and_extract_year_plan() {
        let q = "SELECT o_year, SUM(volume) AS v FROM \
            (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume \
             FROM my.orders o JOIN pg.lineitem l ON o.o_orderkey = l.l_orderkey WHERE o_orderdate < DATE '1996-01-01') AS t \
            GROUP BY o_year ORDER BY o_year";
        let p = plan(q, &srcs(), &mut schemas()).unwrap();
        let gs = &p.sides[p.group_side];
        assert_eq!(gs.table, "orders");
        assert_eq!(gs.virt.len(), 1);
        let g = gs.read[p.groups[0]];
        assert_eq!(gs.schema[g].name, "__year_o_orderdate");
        assert_eq!(gs.conj.len(), 1, "the inner WHERE reaches orders' server");
        assert_eq!(p.out[0].0, "o_year");
        // The server is never asked for the virtual column.
        assert!(gs.read.iter().any(|&c| gs.schema[c].name == "o_orderdate"));
    }

    /// TPC-H Q7: a derived table of six tables (nation twice), GROUP BY on
    /// both parts (the customer's nation at the build, the supplier's and the
    /// ship year at the probe), the nation pair a condition on the groups.
    #[test]
    fn tpch_q7_groups_on_both_parts_with_a_residual() {
        let q7 = "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM \
            (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, \
              l_extendedprice * (1 - l_discount) AS volume \
             FROM my.supplier, pg.lineitem, my.orders, pg.customer, my.nation n1, my.nation n2 \
             WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey \
               AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey \
               AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) \
               AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping \
            GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year";
        let p = plan(q7, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("orders", "lineitem"));
        assert_eq!(st.pgroup.len(), 2, "the supplier's nation and the year group at the probe");
        assert!(p.residual.is_some(), "the nation pair filters the groups");
        assert_eq!(p.sides[1].virt.len(), 1, "l_year is lineitem's virtual column");
        let names: Vec<&str> = st.dims.iter().map(|d| d.side.name.as_str()).collect();
        assert!(names.contains(&"n1") && names.contains(&"n2"), "{names:?}");
        assert_eq!(st.gkind, vec![ArrowKind::Utf8, ArrowKind::Utf8, ArrowKind::Int32]);
        // The nation pair's OR implies each nation's own filter, pushed to
        // its server: two nations of 25 load, not all of them.
        for n in ["n1", "n2"] {
            let d = st.dims.iter().find(|d| d.side.name == n).expect("the nation");
            assert_eq!(d.side.conj.len(), 1, "{n}: {:?}", d.side.conj);
            assert!(d.side.conj[0].contains("FRANCE") && d.side.conj[0].contains("GERMANY") && d.side.conj[0].contains(" OR "), "{:?}", d.side.conj);
        }
    }

    /// Two tables grouped on columns of both plan as a star whose probe
    /// part groups (it was refused before probe-side GROUP BY).
    #[test]
    fn two_tables_grouped_on_both_plan_as_a_star() {
        let q = "SELECT o_orderpriority, l_shipdate, COUNT(*) FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey GROUP BY 1, 2";
        let p = plan(q, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!(st.pgroup.len() + st.gpos.len(), 3, "one GROUP BY column at the probe, two in all");
    }

    /// TPC-H Q8: eight tables, the CASE's condition on the supplier's
    /// nation (a probe-part dimension: a flag in its payload), the year of
    /// the order the GROUP BY.
    #[test]
    fn tpch_q8_flags_a_probe_dimension() {
        let q8 = "SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM \
            (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation \
             FROM my.part, my.supplier, pg.lineitem, my.orders, pg.customer, my.nation n1, my.nation n2, my.region \
             WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey \
               AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' \
               AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
               AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations \
            GROUP BY o_year ORDER BY o_year";
        let p = plan(q8, &srcs(), &mut schemas()).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("orders", "lineitem"));
        assert_eq!(st.pflags.len(), 1, "n2's BRAZIL bit rides its payload");
        let d = st.pflags[0].0;
        assert_eq!(st.dims[d].side.name, "n2");
        assert!(st.dims[d].flag);
        assert!(matches!(p.aggs[0].arg, Some(VExpr::CaseB { .. })));
        assert!(matches!(p.out[1].1, OutCol::Expr(_)));
    }

    /// TPC-H Q19: the join repeated in each OR branch comes out; each
    /// server gets the OR of its own branches' conditions; part's become
    /// flag bits, lineitem's are checked per row.
    #[test]
    fn tpch_q19_lifts_the_join_and_matches_branches() {
        let q19 = "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM pg.lineitem, my.part WHERE \
            (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
              AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size BETWEEN 1 AND 5 \
              AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
            OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
              AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size BETWEEN 1 AND 10 \
              AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
            OR (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
              AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size BETWEEN 1 AND 15 \
              AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')";
        let p = plan(q19, &srcs(), &mut schemas()).unwrap();
        assert!(p.keys.is_some(), "the join came out of the OR");
        assert_eq!(p.orx.len(), 3);
        assert_eq!(p.sides[p.group_side].table, "part");
        assert_eq!(p.sides[p.group_side].flags.len(), 3);
        assert!(p.orx.iter().all(|x| x.0.is_some() && x.1.is_some()));
        assert_eq!(p.sides[p.group_side].conj.len(), 1, "part's server gets the OR of its branches");
        // lineitem's: the shipmode and shipinstruct every branch shares come
        // out as plain filters, beside the OR of its quantity ranges.
        assert_eq!(p.sides[p.agg_side].conj.len(), 3, "lineitem's too");
    }

    /// The part filter `ABS(x % n) = k` evaluates locally (a stage table
    /// has no server to send it to): negatives by their absolute
    /// remainder, a NULL unknown.
    #[test]
    fn part_filter_evaluates_locally() {
        let e: Expr = sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
            .try_with_sql("ABS(t.x % 4) = 1")
            .unwrap()
            .parse_expr()
            .unwrap();
        let Expr::BinaryOp { left, right, .. } = &e else { panic!() };
        let (x, n, k) = part_of(left, right).expect("a part filter");
        assert_eq!((x.to_string().as_str(), n, k), ("t.x", 4, 1));
        assert!(part_of(left, &Expr::Value(Value::Boolean(true).into())).is_none());
        let b = ArrowBatch { rows: 4, cols: vec![FinishedCol::I64 { validity: Some(vec![0b0111]), data: vec![5, -7, 8, 1] }] };
        let pe = PExpr::Part(0, 4, 1);
        let got: Vec<Option<bool>> = (0..4).map(|r| peval(&pe, &b, r, &[Some(Ty::Int)])).collect();
        assert_eq!(got, vec![Some(true), Some(false), Some(false), None]);
        assert_eq!(peval(&PExpr::Part(0, 4, 3), &b, 1, &[Some(Ty::Int)]), Some(true), "-7 % 4 = -3, ABS 3");
    }

    /// Folded constants, BETWEEN, and trailing spaces in text compares.
    #[test]
    fn local_predicates_fold_and_pad() {
        assert!(matches!(lit(&sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {}).try_with_sql("1 + 10").unwrap().parse_expr().unwrap()), Some(Lit::Num(x)) if x == 11.0));
        let b = ArrowBatch { rows: 1, cols: vec![FinishedCol::Utf8 { validity: None, offsets: vec![0, 10], data: b"AIR       ".to_vec() }] };
        let pad = Coll { pad: true, ..Coll::default() };
        let pe = PExpr::In(0, vec![Lit::Coll("AIR".into(), pad), Lit::Coll("AIR REG".into(), pad)], false);
        assert_eq!(peval(&pe, &b, 0, &[None]), Some(true), "CHAR(10) 'AIR' padded equals 'AIR'");
        let pe = PExpr::In(0, vec![Lit::Str("AIR".into())], false);
        assert_eq!(peval(&pe, &b, 0, &[None]), Some(false), "VARCHAR 'AIR       ' is not 'AIR'");
        assert_eq!(peval(&PExpr::Like(0, "%R".into(), false, pad), &b, 0, &[None]), Some(true), "CHAR LIKE ignores the padding");
        assert_eq!(peval(&PExpr::Like(0, "%R".into(), false, Coll::default()), &b, 0, &[None]), Some(false), "VARCHAR LIKE does not");
        // A MySQL _ci column: case folds in =, <, IN and LIKE.
        let ci = Coll { ci: true, ..Coll::default() };
        let b = ArrowBatch { rows: 1, cols: vec![FinishedCol::Utf8 { validity: None, offsets: vec![0, 6], data: b"BRAZIL".to_vec() }] };
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Eq, Lit::Coll("brazil".into(), ci)), &b, 0, &[None]), Some(true));
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Eq, Lit::Str("brazil".into())), &b, 0, &[None]), Some(false), "a case-sensitive column does not fold");
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Lt, Lit::Coll("canada".into(), ci)), &b, 0, &[None]), Some(true));
        assert_eq!(peval(&PExpr::Like(0, "bra%".into(), false, ci), &b, 0, &[None]), Some(true));
        let b = ArrowBatch { rows: 1, cols: vec![FinishedCol::Utf8 { validity: None, offsets: vec![0, "ÉCOLE".len() as i32], data: "ÉCOLE".as_bytes().to_vec() }] };
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Eq, Lit::Coll("école".into(), ci)), &b, 0, &[None]), Some(true), "non-ASCII case folds too");
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Eq, Lit::Coll("ecole".into(), ci)), &b, 0, &[None]), Some(false), "_as_ci keeps accents");
        let ai = Coll { ci: true, ai: true, ..Coll::default() };
        assert_eq!(peval(&PExpr::Cmp(0, BinaryOperator::Eq, Lit::Coll("ecole".into(), ai)), &b, 0, &[None]), Some(true), "_ai_ci: 'ÉCOLE' = 'ecole'");
        assert_eq!(ai.cmp("Straße".as_bytes(), b"STRASSE"), std::cmp::Ordering::Equal, "ß weighs as ss");
        assert_eq!(ai.cmp("Ærø".as_bytes(), b"aero"), std::cmp::Ordering::Equal);
        assert_eq!(peval(&PExpr::Like(0, "%cole".into(), false, ai), &b, 0, &[None]), Some(true), "LIKE folds accents too");
    }

    /// TPC-H Q9: lineitem streams (it joins the most tables), orders builds
    /// (the largest by the catalogs' estimates), partsupp hangs under
    /// lineitem by its two-column key and is filtered by part as it loads,
    /// and the profit reads partsupp's supply cost off its payload.
    #[test]
    fn tpch_q9_plans_a_two_column_key_and_a_lookup() {
        let q9 = "SELECT nation, o_year, SUM(amount) AS sum_profit FROM \
            (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
              l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
             FROM my.part, my.supplier, pg.lineitem, pg.partsupp, my.orders, my.nation \
             WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey \
               AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey \
               AND p_name LIKE '%green%') AS profit \
            GROUP BY nation, o_year ORDER BY nation, o_year DESC";
        let est = |t: &str| match t {
            "lineitem" => 60e6,
            "orders" => 70e6,
            "partsupp" => 52e6,
            "part" => 13e6,
            "supplier" => 6e5,
            _ => 25.0,
        };
        let p = plan_with(q9, &srcs(), &mut schemas(), &mut |_, t| Ok(Some(est(t)))).unwrap();
        let st = p.star.as_ref().expect("a star");
        assert_eq!((p.sides[0].table.as_str(), p.sides[1].table.as_str()), ("orders", "lineitem"));
        let ps = st.dims.iter().position(|d| d.side.table == "partsupp").expect("partsupp");
        assert!(st.dims[ps].key2.is_some(), "partsupp joins by (partkey, suppkey)");
        let part = st.dims.iter().position(|d| d.side.table == "part").expect("part");
        assert!(st.dims[ps].filters.iter().any(|f| f.1 == part), "part filters partsupp as it loads");
        assert_eq!(st.plookups.len(), 1, "ps_supplycost is a lookup column of lineitem");
        assert_eq!(st.pgroup.len(), 1, "nation groups at the probe, the year at the build");
    }

    /// What v1 cannot run is refused before any read, naming it.
    /// HAVING `aggregate op number` is planned (a hidden aggregate when the
    /// SELECT lacks it) and compared exactly on decimals.
    #[test]
    fn having_filters_final_groups_exactly() {
        let p = plan(
            "SELECT o_orderpriority, COUNT(*) FROM my.orders GROUP BY o_orderpriority HAVING SUM(o_shippriority) >= 2 AND COUNT(*) > 1",
            &srcs(),
            &mut schemas(),
        )
        .unwrap();
        assert_eq!(p.having.len(), 2);
        assert_eq!(p.aggs.len(), 2, "the SUM is hidden; the HAVING COUNT(*) is the SELECT's");
        assert_eq!(p.having[1].0, 0, "COUNT(*) reuses the SELECT's accumulator");
        let dec = Agg { f: AggFn::Sum, arg: None, ty: Some(Ty::Dec(2)) };
        // 1000.01 > 1000.005 by its last digit; f64 alone would say so too,
        // but 10^20 + 1 vs 10^20 it would not.
        assert!(having_ok(Acc::D(100001), &dec, star::Op::Gt, &VExpr::Dec(1000005, 3)));
        assert!(!having_ok(Acc::D(100000), &dec, star::Op::Gt, &VExpr::Dec(1000005, 3)));
        assert!(having_ok(Acc::D(10i128.pow(22) + 1), &dec, star::Op::Gt, &VExpr::Dec(10i128.pow(20), 0)));
        assert!(!having_ok(Acc::None, &dec, star::Op::Gt, &VExpr::Int(0)), "NULL fails");
        let cnt = Agg { f: AggFn::CountStar, arg: None, ty: None };
        assert!(having_ok(Acc::None, &cnt, star::Op::Eq, &VExpr::Int(0)), "an empty COUNT is 0");
        assert!(plan("SELECT o_orderpriority, COUNT(*) FROM my.orders GROUP BY o_orderpriority HAVING COUNT(*) > o_shippriority", &srcs(), &mut schemas()).is_err());
    }

    #[test]
    fn unsupported_shapes_are_refused_by_name() {
        for (q, want) in [
            ("SELECT * FROM pg.lineitem", "SELECT *"),
            ("SELECT l_orderkey FROM pg.lineitem", "neither aggregated nor in GROUP BY"),
            ("SELECT COUNT(*) FROM pg.lineitem l LEFT JOIN my.orders o ON l.l_orderkey = o.o_orderkey", "INNER JOIN"),
            ("SELECT COUNT(*) FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey WHERE l.l_discount + 1 > o.o_orderkey", "names more than one table"),
            ("SELECT COUNT(*) FROM xx.lineitem", "not in sources="),
            ("SELECT COUNT(*) FROM pg.lineitem; DROP TABLE x", "exactly one SELECT"),
        ] {
            let e = plan(q, &srcs(), &mut schemas()).err().map(|e| e.to_string()).unwrap_or_default();
            assert!(e.contains(want), "{q}: {e}");
        }
    }

    /// A comparison of a column of each table runs only between kinds that
    /// compare as the servers would: numbers (exact or float), two dates,
    /// two timestamps of one kind. A date against an integer, or text under
    /// collations apitap does not apply here, is refused by name.
    #[test]
    fn a_cross_table_compare_of_unlike_kinds_is_refused() {
        let j = "SELECT COUNT(*) FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey WHERE ";
        for c in ["l.l_shipdate > o.o_orderkey", "l.l_shipmode <> o.o_orderpriority", "l.l_shipmode < o.o_orderpriority",
                  "l.l_suppkey <> o.o_orderpriority", "o.o_orderdate < l.l_extendedprice"] {
            let e = plan(&format!("{j}{c}"), &srcs(), &mut schemas()).err().map(|e| e.to_string()).unwrap_or_default();
            assert!(e.contains("a comparison of") && e.contains("is not supported"), "{c}: {e}");
        }
        for c in ["l.l_shipdate > o.o_orderdate", "l.l_extendedprice > o.o_totalprice", "l.l_suppkey <> o.o_custkey",
                  "l.l_quantity < o.o_shippriority"] {
            assert!(plan(&format!("{j}{c}"), &srcs(), &mut schemas()).is_ok(), "{c}");
        }
    }

    /// Decimal arithmetic is exact: 1234.56 * (1 - 0.07) = 1148.1408 at scale 4.
    #[test]
    fn decimal_arithmetic_is_exact() {
        let b = ArrowBatch {
            rows: 1,
            cols: vec![
                FinishedCol::Dec128 { validity: None, data: vec![123456] },
                FinishedCol::Dec128 { validity: None, data: vec![7] },
            ],
        };
        let e = VExpr::Bin(
            Box::new(VExpr::Col(0, ArrowKind::Decimal { p: 15, s: 2 })),
            '*',
            Box::new(VExpr::Bin(Box::new(VExpr::Int(1)), '-', Box::new(VExpr::Col(1, ArrowKind::Decimal { p: 15, s: 2 })))),
        );
        assert_eq!(type_of(&e).unwrap(), Ty::Dec(4));
        match eval(&e, &b, 0).unwrap() {
            Num::Dec(v) => assert_eq!(v, 11481408),
            other => panic!("{other:?}"),
        }
    }

    /// The batch-at-once evaluation gives the row way's value, NULL and
    /// error on every row: decimal and integer columns with NULLs, nested
    /// arithmetic, a negation, literals, and an integer overflow.
    #[test]
    fn exact_batch_eval_equals_the_row_way() {
        let d = ArrowKind::Decimal { p: 15, s: 2 };
        let n = 300;
        let mut seed = 7u64;
        let mut next = |m: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % m
        };
        let bm = |v: &[bool]| {
            let mut b = vec![0u8; v.len().div_ceil(8)];
            for (i, &x) in v.iter().enumerate() {
                if x {
                    b[i / 8] |= 1 << (i % 8);
                }
            }
            b
        };
        let (va, vb, vc): (Vec<bool>, Vec<bool>, Vec<bool>) =
            ((0..n).map(|_| next(9) != 0).collect(), (0..n).map(|_| next(7) != 0).collect(), (0..n).map(|_| next(5) != 0).collect());
        let b = ArrowBatch {
            rows: n,
            cols: vec![
                FinishedCol::Dec128 { validity: Some(bm(&va)), data: (0..n).map(|_| next(2_000_000) as i128 - 1_000_000).collect() },
                FinishedCol::Dec128 { validity: Some(bm(&vb)), data: (0..n).map(|_| next(100) as i128).collect() },
                FinishedCol::I64 { validity: Some(bm(&vc)), data: (0..n).map(|_| next(1000) as i64 - 500).collect() },
                FinishedCol::I64 { validity: None, data: (0..n).map(|r| if r == 17 { i64::MAX } else { 3 }).collect() },
            ],
        };
        let col = |i: usize, k| Box::new(VExpr::Col(i, k));
        let bin = |l, o, r| Box::new(VExpr::Bin(l, o, r));
        let cases: Vec<VExpr> = vec![
            *bin(col(0, d), '*', bin(Box::new(VExpr::Int(1)), '-', col(1, d))),
            *bin(bin(col(0, d), '+', col(2, ArrowKind::Int64)), '-', Box::new(VExpr::Dec(125, 3))),
            VExpr::Neg(bin(col(2, ArrowKind::Int64), '*', col(2, ArrowKind::Int64))),
            *bin(col(2, ArrowKind::Int64), '-', Box::new(VExpr::Neg(col(1, d)))),
            *bin(Box::new(VExpr::Dec(-31, 1)), '*', col(0, d)),
            *bin(Box::new(VExpr::Dec(7, 1)), '+', col(2, ArrowKind::Int64)),
            *bin(col(1, d), '-', Box::new(VExpr::Int(2))),
            *bin(col(3, ArrowKind::Int64), '*', Box::new(VExpr::Int(5))),
        ];
        for (ci, e) in cases.iter().enumerate() {
            let fast = eval_exact(e, &b);
            let rows: Result<Vec<Num>> = (0..n).map(|r| eval(e, &b, r)).collect();
            match (fast, rows) {
                (Ok(Some((v, ok))), Ok(rows)) => {
                    for r in 0..n {
                        match rows[r] {
                            Num::Null => assert!(!ok[r], "case {ci} row {r}: NULL"),
                            Num::Int(x) => assert!(ok[r] && v[r] == i128::from(x), "case {ci} row {r}"),
                            Num::Dec(x) => assert!(ok[r] && v[r] == x, "case {ci} row {r}"),
                            Num::Float(_) => panic!("case {ci}: a float"),
                        }
                    }
                }
                (Err(a), Err(c)) => assert_eq!(a.to_string(), c.to_string(), "case {ci}"),
                (f, r) => panic!("case {ci}: batch {:?} vs rows {:?}", f.map(|x| x.is_some()), r.map(|x| x.len())),
            }
        }
        // The overflow case did fail both ways.
        assert!(eval_exact(&cases[cases.len() - 1], &b).is_err());
    }

    /// The build budget prices the structure the build would pick: the 5 GB
    /// case's 3.66M keys over a 30.1M range with 5 groups cost the pairs plus
    /// a 30 MB direct array (fits a 256 MiB cage's 40%), not a hash entry each.
    #[test]
    fn build_cost_prices_the_direct_array() {
        let n = 3_661_626;
        let c = build_cost(n, n, 5, 1, 30_140_292);
        assert!(c < 102 << 20, "{c}");
        assert_eq!(c, 12 * n as u64 + 30_140_292);
        // A sparse range prices the hash map.
        assert_eq!(build_cost(10, 10, 5, 0, 1 << 40), 120 + 240);
    }

    /// The build structure: a dense key range goes direct at the narrowest
    /// width; a sparse one hashes; repeated keys map to every group they carry.
    #[test]
    fn build_map_picks_direct_or_hash_and_keeps_every_match() {
        let m = build_map(&mut [(10, 0), (11, 1), (13, 2)], 3);
        assert!(m.multi.is_none() && matches!(m.map, KeyMap::Direct8(10, _)));
        assert!(matches!(m.look(13), Look::One(2)) && matches!(m.look(12), Look::Miss));
        let m = build_map(&mut [(0, 0), (1 << 40, 1)], 2);
        assert!(matches!(m.map, KeyMap::Hash(_)) && matches!(m.look(1 << 40), Look::One(1)));
        let m = build_map(&mut [(5, 0), (7, 2), (5, 1), (5, 0)], 3);
        match m.look(5) {
            Look::Many(g) => assert_eq!(g, [0, 1, 0]),
            _ => panic!("key 5 repeats"),
        }
        assert!(matches!(m.look(7), Look::Many(&[2])) && matches!(m.look(6), Look::Miss));
    }
}
