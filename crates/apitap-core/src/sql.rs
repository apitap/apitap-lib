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
use crate::wire::arrowcol::{ArrowBatch, ArrowKind, FinishedCol};
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
}

/// One table of the query.
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
        VExpr::Col(_, k) => kind_ty(*k).ok_or_else(|| unsupported(format!("arithmetic on a {k:?} column")))?,
        VExpr::Int(_) => Ty::Int,
        VExpr::Dec(_, s) => Ty::Dec(*s),
        VExpr::Float(_) => Ty::Float,
        VExpr::Neg(x) => type_of(x)?,
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

fn rescale(v: i128, from: i8, to: i8) -> Result<i128> {
    if to == from {
        return Ok(v);
    }
    let d = (to - from) as usize;
    v.checked_mul(*P10.get(d).ok_or_else(|| bad("decimal scale past 38"))?)
        .ok_or_else(|| bad("decimal overflow"))
}

fn as_f64(n: Num, t: Ty) -> Option<f64> {
    match n {
        Num::Null => None,
        Num::Int(i) => Some(i as f64),
        Num::Dec(d) => Some(d as f64 / 10f64.powi(i32::from(scale(t)))),
        Num::Float(f) => Some(f),
    }
}

/// Evaluate `e` (of type `t`) on row `r` of `b`.
fn eval(e: &VExpr, b: &ArrowBatch, r: usize) -> Result<Num> {
    Ok(match e {
        VExpr::Col(i, _) => cell_num(&b.cols[*i], r),
        VExpr::Int(v) => Num::Int(*v),
        VExpr::Dec(v, _) => Num::Dec(*v),
        VExpr::Float(v) => Num::Float(*v),
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

/// The join key of row `r`, if it is not NULL.
fn key_at(c: &FinishedCol, r: usize) -> Option<i64> {
    match cell_num(c, r) {
        Num::Int(v) => Some(v),
        _ => None,
    }
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

/// Group ids by encoded group key, and each group's first row's key bytes.
#[derive(Default)]
struct Groups {
    ids: HashMap<Box<[u8]>, u32, foldhash::fast::RandomState>,
    keys: Vec<Box<[u8]>>,
}

impl Groups {
    fn id(&mut self, kb: &[u8]) -> Result<u32> {
        if let Some(&g) = self.ids.get(kb) {
            return Ok(g);
        }
        let g = u32::try_from(self.keys.len()).map_err(|_| bad("more than 4 billion groups"))?;
        let k: Box<[u8]> = kb.into();
        self.ids.insert(k.clone(), g);
        self.keys.push(k);
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

/// Pick and fill the build structure from the (key, group) pairs. A key twice
/// is refused: the GROUP BY side must be unique on the join key in v1, or a
/// probe row would have to count once per match.
fn key_map(pairs: &[(i64, u32)], groups: usize, side: &str) -> Result<KeyMap> {
    let dup = || {
        bad(format!(
            "the join key of {side} (the GROUP BY side) repeats; v1 needs it unique (a primary key)"
        ))
    };
    let (Some(lo), Some(hi)) = (pairs.iter().map(|p| p.0).min(), pairs.iter().map(|p| p.0).max()) else {
        return Ok(KeyMap::Hash(HashMap::default()));
    };
    let span = (hi as i128 - lo as i128 + 1) as u64;
    let width: u64 = if groups < 255 {
        1
    } else if groups < 65535 {
        2
    } else {
        4
    };
    if span.saturating_mul(width) <= DIRECT_MAX {
        let span = span as usize;
        macro_rules! fill {
            ($t:ty, $v:ident) => {{
                let mut a = vec![<$t>::MAX; span];
                for &(k, g) in pairs {
                    let slot = &mut a[(k - lo) as usize];
                    if *slot != <$t>::MAX {
                        return Err(dup());
                    }
                    *slot = g as $t;
                }
                KeyMap::$v(lo, a)
            }};
        }
        return Ok(match width {
            1 => fill!(u8, Direct8),
            2 => fill!(u16, Direct16),
            _ => fill!(u32, Direct32),
        });
    }
    let mut m: HashMap<i64, u32, foldhash::fast::RandomState> =
        HashMap::with_capacity_and_hasher(pairs.len(), Default::default());
    for &(k, g) in pairs {
        if m.insert(k, g).is_some() {
            return Err(dup());
        }
    }
    Ok(KeyMap::Hash(m))
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
}

fn ident_name(i: &Ident) -> String {
    i.value.clone()
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
        Expr::Nested(x) if matches!(**x, Expr::BinaryOp { op: BinaryOperator::And, .. }) => conjuncts(x, out),
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
        if side.is_some_and(|x| x != s) {
            return Err(unsupported("an aggregate argument mixing both tables' columns"));
        }
        *side = Some(s);
        let k = sides[s].schema[c].kind;
        let at = sides[s].want(c);
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
        other => return Err(unsupported(format!("the expression {other} in an aggregate"))),
    })
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
            let ty = Some(type_of(&v)?);
            Ok(Some(Agg { f: func, arg: Some(v), ty }))
        }
        _ => Err(unsupported(format!("the argument of {}", f.name))),
    }
}

fn plan(query: &str, sources: &HashMap<String, String>, schemas: &mut dyn FnMut(&str, &str) -> Result<Vec<ArrowField>>) -> Result<Plan> {
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
    if sel.distinct.is_some() || sel.having.is_some() || sel.top.is_some() || sel.prewhere.is_some() {
        return Err(unsupported("DISTINCT, HAVING, TOP or PREWHERE"));
    }
    let [from] = sel.from.as_slice() else {
        return Err(unsupported("anything but one FROM item"));
    };
    if from.joins.len() > 1 {
        return Err(unsupported("more than one JOIN"));
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
        } else {
            return Err(unsupported(format!("the source {} (postgres:// and mysql:// today)", parts[0])));
        };
        let table = parts[1..].join(".");
        let schema = schemas(&url, &table)?;
        let nm = alias.as_ref().map(|a| ident_name(&a.name)).unwrap_or_else(|| parts.last().cloned().unwrap_or_default());
        if sides.iter().any(|s: &Side| s.name.eq_ignore_ascii_case(&nm)) {
            return Err(bad(format!("two tables named {nm}; alias one")));
        }
        sides.push(Side { name: nm, url, table, dialect, schema, read: Vec::new(), conj: Vec::new() });
        Ok(())
    };
    add(&from.relation, &mut sides)?;
    let mut join_on = None;
    if let Some(j) = from.joins.first() {
        add(&j.relation, &mut sides)?;
        match &j.join_operator {
            JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => join_on = Some(e.clone()),
            _ => return Err(unsupported("a join that is not INNER JOIN … ON")),
        }
    }

    // The join key: one equality of integer columns, one from each side.
    let mut keys = None;
    if let Some(on) = &join_on {
        let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = on else {
            return Err(unsupported("a join condition that is not one equality a.k = b.k"));
        };
        let (Some(a), Some(b)) = (column_of(&sides, left)?, column_of(&sides, right)?) else {
            return Err(unsupported("a join condition on expressions, not columns"));
        };
        if a.0 == b.0 {
            return Err(bad("the join condition compares two columns of one table"));
        }
        for (s, c) in [a, b] {
            if !matches!(sides[s].schema[c].kind, ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64) {
                return Err(unsupported(format!(
                    "a join key that is not an integer ({}.{} is {:?})",
                    sides[s].name, sides[s].schema[c].name, sides[s].schema[c].kind
                )));
            }
        }
        let ka = sides[a.0].want(a.1);
        let kb = sides[b.0].want(b.1);
        let mut k = [(a.0, ka), (b.0, kb)];
        k.sort_by_key(|x| x.0);
        keys = Some(k);
    }

    // WHERE: each conjunct goes to the one side it names.
    if let Some(w) = &sel.selection {
        let mut cs = Vec::new();
        conjuncts(w, &mut cs);
        for c in cs {
            let mut on = Vec::new();
            sides_of(&sides, &c, &mut on)?;
            match on.as_slice() {
                [s] => {
                    let r = render_for(&sides, *s, &c)?;
                    sides[*s].conj.push(r);
                }
                [] => return Err(unsupported(format!("the WHERE condition {c}, which names no column"))),
                _ => return Err(unsupported(format!("the WHERE condition {c}, which names both tables"))),
            }
        }
    }

    // GROUP BY: columns, by name or by SELECT ordinal.
    let group_exprs: Vec<Expr> = match &sel.group_by {
        GroupByExpr::Expressions(es, mods) if mods.is_empty() => es.clone(),
        GroupByExpr::Expressions(..) | GroupByExpr::All(..) => return Err(unsupported("GROUP BY ALL or GROUP BY modifiers")),
    };
    let item_expr = |i: &SelectItem| -> Result<(Expr, Option<String>)> {
        match i {
            SelectItem::UnnamedExpr(e) => Ok((e.clone(), None)),
            SelectItem::ExprWithAlias { expr, alias } => Ok((expr.clone(), Some(ident_name(alias)))),
            _ => Err(unsupported("SELECT *")),
        }
    };
    let items: Vec<(Expr, Option<String>)> = sel.projection.iter().map(item_expr).collect::<Result<_>>()?;
    let mut group_cols: Vec<(usize, usize)> = Vec::new();
    for g in &group_exprs {
        let e = match g {
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => {
                    let i: usize = n.parse().map_err(|_| bad(format!("GROUP BY {n}")))?;
                    items.get(i.wrapping_sub(1)).map(|x| x.0.clone()).ok_or_else(|| bad(format!("GROUP BY {i} is past the SELECT list")))?
                }
                _ => return Err(unsupported(format!("GROUP BY {g}"))),
            },
            other => other.clone(),
        };
        let c = column_of(&sides, &e)?.ok_or_else(|| unsupported(format!("GROUP BY on the expression {e}")))?;
        group_cols.push(c);
    }

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
        let c = column_of(&sides, e)?.ok_or_else(|| unsupported(format!("the SELECT expression {e}")))?;
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
    let group_side = match group_cols.first() {
        Some(g) => g.0,
        None => {
            // A global aggregate: the build side is the one without the
            // aggregate arguments (COUNT(*) alone reads from the first).
            match (keys, agg_side) {
                (Some(_), Some(a)) => 1 - a,
                _ => 0,
            }
        }
    };
    if group_cols.iter().any(|g| g.0 != group_side) {
        return Err(unsupported("GROUP BY columns from both tables"));
    }
    let agg_side = agg_side.unwrap_or(if keys.is_some() { 1 - group_side } else { group_side });
    if keys.is_some() && agg_side == group_side {
        return Err(unsupported(
            "aggregating the GROUP BY table's own columns across a join (v1 aggregates the other table)",
        ));
    }
    let groups: Vec<usize> = group_cols.iter().map(|&(s, c)| sides[s].want(c)).collect();

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
                        column_of(&sides, e).ok().flatten().and_then(|c| {
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
    Ok(Plan { sides, keys, group_side, groups, agg_side, aggs, out, order, limit })
}

fn quote(d: Dialect, name: &str) -> String {
    name.split('.')
        .map(|p| match d {
            Dialect::Pg => format!("\"{}\"", p.replace('"', "\"\"")),
            Dialect::My => format!("`{}`", p.replace('`', "``")),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Unfiltered MIN/MAX of a side's join key, asked of its own server.
async fn key_range(side: &Side, col: &str) -> Result<Option<(i64, i64)>> {
    let sql = format!(
        "SELECT CAST(MIN({c}) AS {t}), CAST(MAX({c}) AS {t}) FROM {tb}",
        c = quote(side.dialect, col),
        t = if side.dialect == Dialect::Pg { "BIGINT" } else { "SIGNED" },
        tb = quote(side.dialect, &side.table)
    );
    let row: (Option<i64>, Option<i64>) = match side.dialect {
        Dialect::Pg => {
            let src = crate::source::postgres::PgSource::connect(&side.url, 1).await?;
            sqlx::query_as(&sql).fetch_one(src.pool()).await
        }
        Dialect::My => {
            let src = crate::source::mysql::MySqlSource::connect(&side.url, 1).await?;
            sqlx::query_as(&sql).fetch_one(src.pool()).await
        }
    }
    .map_err(|e| Error::Transfer(format!("sql: key range of {}: {e}", side.name)))?;
    Ok(match row {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    })
}

async fn open(side: &Side, extra: Option<String>, parallel: Option<usize>) -> Result<ReadHandle> {
    let mut conj = side.conj.clone();
    conj.extend(extra);
    let opts = ReadOptions {
        parallel,
        columns: Some(side.read.iter().map(|&c| side.schema[c].name.clone()).collect()),
        where_sql: (!conj.is_empty()).then(|| conj.iter().map(|c| format!("({c})")).collect::<Vec<_>>().join(" AND ")),
        ..Default::default()
    };
    crate::read::read_start(&side.url, &side.table, &opts).await
}

fn dbg() -> bool {
    std::env::var("APITAP_DEBUG").is_ok()
}

/// Run `query` against `sources` (name → URL).
pub async fn run(query: &str, sources: &HashMap<String, String>) -> Result<SqlResult> {
    let t0 = std::time::Instant::now();
    // The planner asks for schemas synchronously; each table it asks for that
    // is not probed yet stops the pass, is probed, and the pass runs again.
    let mut cache: HashMap<(String, String), Vec<ArrowField>> = HashMap::new();
    loop {
        let mut missing = None;
        let r = plan(query, sources, &mut |u, t| match cache.get(&(u.to_string(), t.to_string())) {
            Some(s) => Ok(s.clone()),
            None => {
                missing = Some((u.to_string(), t.to_string()));
                Err(bad("schema probe"))
            }
        });
        match (r, missing) {
            (Ok(p), _) => return execute(p, t0).await,
            (Err(_), Some((u, t))) => {
                let s = crate::read::read_schema(&u, &t).await?;
                cache.insert((u, t), s);
            }
            (Err(e), None) => return Err(e),
        }
    }
}

async fn execute(mut p: Plan, t0: std::time::Instant) -> Result<SqlResult> {
    let mut groups = Groups::default();
    let mut kb = Vec::with_capacity(64);
    let mut accs: Vec<Acc> = Vec::new();
    let na = p.aggs.len();

    // Idea 1: the join key's range, intersected, pushed into both scans.
    let mut range = None;
    if let Some(k) = p.keys {
        let names: Vec<String> = k.iter().map(|&(s, at)| p.sides[s].schema[p.sides[s].read[at]].name.clone()).collect();
        let (ra, rb) = tokio::try_join!(key_range(&p.sides[k[0].0], &names[0]), key_range(&p.sides[k[1].0], &names[1]))?;
        range = match (ra, rb) {
            (Some(a), Some(b)) => Some((a.0.max(b.0), a.1.min(b.1))),
            _ => Some((1, 0)),
        };
        if dbg() {
            eprintln!("[sql] key ranges {ra:?} ∩ {rb:?} = {range:?} in {:.1}s", t0.elapsed().as_secs_f64());
        }
        for (i, &(s, _)) in k.iter().enumerate() {
            if let Some((lo, hi)) = range {
                p.sides[s].conj.push(format!("{} BETWEEN {lo} AND {hi}", names[i]));
            }
        }
    }
    let empty_range = range.is_some_and(|(lo, hi)| lo > hi);

    if let (Some(k), false) = (p.keys, empty_range) {
        // Build: the GROUP BY side, one group id per key.
        let bs = p.group_side;
        let bkey = k.iter().find(|x| x.0 == bs).expect("both sides keyed").1;
        let mut pairs: Vec<(i64, u32)> = Vec::new();
        let mut h = open(&p.sides[bs], None, None).await?;
        while let Some(b) = h.recv().await? {
            for r in 0..b.rows {
                let Some(key) = key_at(&b.cols[bkey], r) else { continue };
                kb.clear();
                for &g in &p.groups {
                    encode_group(&b.cols[g], r, &mut kb);
                }
                pairs.push((key, groups.id(&kb)?));
            }
        }
        drop(h);
        let map = key_map(&pairs, groups.keys.len(), &p.sides[bs].name)?;
        if dbg() {
            eprintln!(
                "[sql] build {}: {} keys, {} groups, {} ({} MiB) in {:.1}s",
                p.sides[bs].name,
                pairs.len(),
                groups.keys.len(),
                match &map {
                    KeyMap::Direct8(..) => "direct u8",
                    KeyMap::Direct16(..) => "direct u16",
                    KeyMap::Direct32(..) => "direct u32",
                    KeyMap::Hash(_) => "hash",
                },
                map.bytes() >> 20,
                t0.elapsed().as_secs_f64()
            );
        }
        drop(pairs);
        accs = vec![Acc::None; groups.keys.len() * na];
        // Probe: fold every match into its group, with the pipes the memory
        // left after the build structure holds.
        let ps = p.agg_side;
        let pkey = k.iter().find(|x| x.0 == ps).expect("both sides keyed").1;
        let parallel = crate::pipeline::mem_limit_bytes()
            .map(|m| crate::read_impl::read_pipes_for(m.saturating_sub(map.bytes() + (accs.len() as u64) * 32)));
        let mut h = open(&p.sides[ps], None, parallel).await?;
        let mut probed = 0u64;
        let mut matched = 0u64;
        while let Some(b) = h.recv().await? {
            probed += b.rows as u64;
            for r in 0..b.rows {
                let Some(key) = key_at(&b.cols[pkey], r) else { continue };
                let Some(g) = map.get(key) else { continue };
                matched += 1;
                let base = g as usize * na;
                for (i, a) in p.aggs.iter().enumerate() {
                    let v = match &a.arg {
                        Some(e) => eval(e, &b, r)?,
                        None => Num::Null,
                    };
                    fold(&mut accs[base + i], a.f, a.ty, v)?;
                }
            }
        }
        if dbg() {
            eprintln!(
                "[sql] probe {}: {probed} rows, {matched} matched in {:.1}s",
                p.sides[ps].name,
                t0.elapsed().as_secs_f64()
            );
        }
    } else if p.keys.is_none() {
        // One table: group and aggregate as it streams.
        let s = p.group_side;
        let mut h = open(&p.sides[s], None, None).await?;
        while let Some(b) = h.recv().await? {
            for r in 0..b.rows {
                kb.clear();
                for &g in &p.groups {
                    encode_group(&b.cols[g], r, &mut kb);
                }
                let g = groups.id(&kb)? as usize;
                if accs.len() < (g + 1) * na {
                    accs.resize((g + 1) * na, Acc::None);
                }
                for (i, a) in p.aggs.iter().enumerate() {
                    let v = match &a.arg {
                        Some(e) => eval(e, &b, r)?,
                        None => Num::Null,
                    };
                    fold(&mut accs[g * na + i], a.f, a.ty, v)?;
                }
            }
        }
    }
    // A global aggregate over no rows is one row (COUNT 0, SUM NULL).
    if p.groups.is_empty() && groups.keys.is_empty() {
        groups.id(&[])?;
        accs = vec![Acc::None; na];
    }
    let res = result(&p, &groups, &accs)?;
    if dbg() {
        eprintln!("[sql] done: {} rows in {:.1}s", res.batch.rows, t0.elapsed().as_secs_f64());
    }
    Ok(res)
}

/// Decode one group-key field back to its column value.
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

fn result(p: &Plan, groups: &Groups, accs: &[Acc]) -> Result<SqlResult> {
    let na = p.aggs.len();
    let ng = groups.keys.len();
    // Output kinds.
    let gs = &p.sides[p.group_side];
    let kinds: Vec<ArrowKind> = p
        .out
        .iter()
        .map(|(_, oc)| match oc {
            OutCol::Group(g) => gs.schema[gs.read[p.groups[*g]]].kind,
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
    // Row order.
    let mut rows: Vec<usize> = (0..ng).collect();
    let fields: Vec<Vec<(u8, &[u8])>> = groups.keys.iter().map(|k| group_fields(k)).collect();
    let val = |r: usize, c: usize| -> (u8, Vec<u8>, f64) {
        match &p.out[c].1 {
            OutCol::Group(g) => {
                let (t, b) = fields[r][*g];
                let f = match t {
                    1 => i64::from_le_bytes(b.try_into().unwrap_or([0; 8])) as f64,
                    2 => f64::from_le_bytes(b.try_into().unwrap_or([0; 8])),
                    3 => i128::from_le_bytes(b.try_into().unwrap_or([0; 16])) as f64,
                    4 => f64::from(b[0]),
                    _ => 0.0,
                };
                (t, b.to_vec(), f)
            }
            OutCol::Agg(a) => match accs[r * na + a] {
                Acc::None => (0, Vec::new(), 0.0),
                Acc::I(x) => (1, Vec::new(), x as f64),
                Acc::D(x) => (1, Vec::new(), x as f64),
                Acc::F(x) => (1, Vec::new(), x),
                Acc::Avg(s, n) => (1, Vec::new(), s / n as f64),
                Acc::N(n) => (1, Vec::new(), n as f64),
            },
        }
    };
    if !p.order.is_empty() {
        rows.sort_by(|&x, &y| {
            for &(c, asc) in &p.order {
                let (a, b) = (val(x, c), val(y, c));
                let o = (a.0 == 0)
                    .cmp(&(b.0 == 0))
                    .then(if a.0 == 5 || b.0 == 5 { a.1.cmp(&b.1) } else { a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal) });
                let o = if asc { o } else { o.reverse() };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(l) = p.limit {
        rows.truncate(l);
    }
    let mut cols = Vec::new();
    let mut schema = Vec::new();
    for (c, (name, oc)) in p.out.iter().enumerate() {
        let mut o = ColOut { valid: Vec::new(), i64s: Vec::new(), f64s: Vec::new(), d128: Vec::new(), strs: Vec::new() };
        for &r in &rows {
            match oc {
                OutCol::Group(g) => {
                    let (t, b) = fields[r][*g];
                    o.valid.push(t != 0);
                    o.i64s.push(if t == 1 { i64::from_le_bytes(b.try_into().unwrap_or([0; 8])) } else { 0 });
                    o.f64s.push(if t == 2 { f64::from_le_bytes(b.try_into().unwrap_or([0; 8])) } else { 0.0 });
                    o.d128.push(if t == 3 { i128::from_le_bytes(b.try_into().unwrap_or([0; 16])) } else { 0 });
                    o.strs.push(if t == 5 || t == 4 { b.to_vec() } else { Vec::new() });
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
                    o.valid.push(ok);
                    o.i64s.push(i);
                    o.f64s.push(f);
                    o.d128.push(d);
                }
            }
        }
        let k = kinds[c];
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
        schema.push(ArrowField { name: name.clone(), kind: k, nullable: true, rewritten: false });
    }
    Ok(SqlResult { schema, batch: ArrowBatch { rows: rows.len(), cols } })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(name: &str, kind: ArrowKind) -> ArrowField {
        ArrowField { name: name.into(), kind, nullable: true, rewritten: false }
    }

    fn schemas() -> impl FnMut(&str, &str) -> Result<Vec<ArrowField>> {
        |_u: &str, t: &str| {
            Ok(match t {
                "lineitem" => vec![
                    f("l_orderkey", ArrowKind::Int64),
                    f("l_extendedprice", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("l_discount", ArrowKind::Decimal { p: 15, s: 2 }),
                    f("l_shipdate", ArrowKind::Date32),
                ],
                "orders" => vec![
                    f("o_orderkey", ArrowKind::Int64),
                    f("o_orderdate", ArrowKind::Date32),
                    f("o_orderpriority", ArrowKind::Utf8),
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

    /// What v1 cannot run is refused before any read, naming it.
    #[test]
    fn unsupported_shapes_are_refused_by_name() {
        for (q, want) in [
            ("SELECT * FROM pg.lineitem", "SELECT *"),
            ("SELECT l_orderkey FROM pg.lineitem", "neither aggregated nor in GROUP BY"),
            ("SELECT COUNT(*) FROM pg.lineitem l LEFT JOIN my.orders o ON l.l_orderkey = o.o_orderkey", "INNER JOIN"),
            ("SELECT COUNT(*) FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey WHERE l.l_discount > o.o_orderkey", "names both tables"),
            ("SELECT COUNT(*) FROM xx.lineitem", "not in sources="),
            ("SELECT o_orderpriority, l_shipdate, COUNT(*) FROM pg.lineitem l JOIN my.orders o ON l.l_orderkey = o.o_orderkey GROUP BY 1, 2", "both tables"),
            ("SELECT COUNT(*) FROM pg.lineitem; DROP TABLE x", "exactly one SELECT"),
        ] {
            let e = plan(q, &srcs(), &mut schemas()).err().map(|e| e.to_string()).unwrap_or_default();
            assert!(e.contains(want), "{q}: {e}");
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

    /// The build structure: a dense key range goes direct at the narrowest
    /// width; a sparse one hashes; a repeated key is refused.
    #[test]
    fn key_map_picks_direct_or_hash_and_refuses_duplicates() {
        let m = key_map(&[(10, 0), (11, 1), (13, 2)], 3, "t").unwrap();
        assert!(matches!(m, KeyMap::Direct8(10, _)));
        assert_eq!((m.get(10), m.get(12), m.get(13), m.get(99)), (Some(0), None, Some(2), None));
        let m = key_map(&[(0, 0), (1 << 40, 1)], 2, "t").unwrap();
        assert!(matches!(m, KeyMap::Hash(_)));
        assert_eq!(m.get(1 << 40), Some(1));
        assert!(key_map(&[(5, 0), (5, 1)], 2, "t").err().unwrap().to_string().contains("repeats"));
    }
}
