//! Staged plans: a query whose parts the engine answers one SELECT at a time.
//!
//! The engine (`plan_with` + `execute`) answers one SELECT shape. A query
//! with a `WITH`, a scalar subquery, a `HAVING`, a grouped derived table, a
//! `COUNT(DISTINCT …)` or no aggregate at all is that shape applied more
//! than once: each part is answered first, kept in memory as a table of this
//! run (`mem://`), and read by the next part like any source. The servers
//! still only scan and send; every join and aggregate between the parts is
//! apitap's. Design: docs/design/2026-11-08-sql-0.67-staged.md.

use super::*;
use sqlparser::ast::{
    DuplicateTreatment, ObjectName, Query, Select, TableAlias, VisitMut, Visit, Visitor, VisitorMut,
};
use std::ops::ControlFlow;

/// The source name stages are read under (`__s.<stage>`).
const STAGE_SRC: &str = "__s";

pub(super) async fn run(query: &str, sources: &HashMap<String, String>) -> Result<SqlResult> {
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, query)
        .map_err(|e| bad(format!("parse error: {e}")))?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return Err(bad("expected exactly one SELECT statement"));
    };
    if !needs_stages(q) {
        return super::run_one(query, sources).await;
    }
    let run = mem::Run::new();
    let mut srcs = sources.clone();
    srcs.insert(STAGE_SRC.to_string(), run.url.clone());
    let mut st = Stager { run, sources: srcs, n: 0, ctes: HashMap::new(), cols: HashMap::new() };
    st.answer((**q).clone()).await
}

/// Whether the query has a part the engine cannot answer in one pass.
fn needs_stages(q: &Query) -> bool {
    if q.with.is_some() {
        return true;
    }
    let SetExpr::Select(sel) = q.body.as_ref() else { return false };
    if sel.having.is_some() || sel.distinct.is_some() || count_distinct(sel) || !aggregated(sel) {
        return true;
    }
    if sel.from.iter().any(|f| matches!(f.relation, TableFactor::Derived { .. }) && derived_needs_stage(sel)) {
        return true;
    }
    if has_subqueries(sel) {
        return true;
    }
    // A derived table's own WHERE may hold a scalar subquery (TPC-H Q22).
    sel.from.iter().any(|f| match &f.relation {
        TableFactor::Derived { subquery, .. } => subquery.with.is_some() || matches!(subquery.body.as_ref(), SetExpr::Select(s) if has_subqueries(s)),
        _ => false,
    })
}

/// A scalar subquery, or an `IN` subquery the semi-join cannot take, among
/// the SELECT's expressions of this level.
fn has_subqueries(sel: &Select) -> bool {
    let mut found = false;
    for e in select_exprs(sel) {
        let _ = top_exprs(e, &mut |x| {
            if matches!(x, Expr::Subquery(_)) || matches!(x, Expr::InSubquery { subquery, .. } if in_needs_stage(subquery)) || matches!(x, Expr::Exists { subquery, .. } if exists_cmp(subquery)) {
                found = true;
            }
        });
    }
    found
}

/// An EXISTS whose WHERE compares two tables' columns other than by `=`
/// (`l2.l_suppkey <> l1.l_suppkey`): its keys are narrowed first.
fn exists_cmp(q: &Query) -> bool {
    let SetExpr::Select(s) = q.body.as_ref() else { return false };
    let Some(w) = &s.selection else { return false };
    let mut c = Vec::new();
    conjuncts(w, &mut c);
    c.iter().any(|x| match x {
        Expr::BinaryOp { left, op, right } if !matches!(op, BinaryOperator::Eq) && star::Op::of(op).is_some() => match (left.as_ref(), right.as_ref()) {
            (Expr::CompoundIdentifier(a), Expr::CompoundIdentifier(b)) if a.len() >= 2 && b.len() >= 2 => {
                !a[a.len() - 2].value.eq_ignore_ascii_case(&b[b.len() - 2].value)
            }
            _ => false,
        },
        _ => false,
    })
}

/// The expressions of a SELECT that may hold subqueries of this level.
fn select_exprs(sel: &Select) -> Vec<&Expr> {
    let mut v: Vec<&Expr> = sel.selection.iter().chain(sel.having.iter()).collect();
    for it in &sel.projection {
        match it {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => v.push(e),
            _ => {}
        }
    }
    v
}

/// Calls `f` on each expression of `e` outside any subquery of it (the
/// subquery expressions themselves included).
fn top_exprs(e: &Expr, f: &mut dyn FnMut(&Expr)) -> ControlFlow<()> {
    struct V<'a> {
        depth: usize,
        f: &'a mut dyn FnMut(&Expr),
    }
    impl Visitor for V<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            if self.depth == 0 {
                (self.f)(e);
            }
            ControlFlow::Continue(())
        }
    }
    e.visit(&mut V { depth: 0, f })
}

/// [`top_exprs`], mutable: `f` may replace an expression of this level.
fn top_exprs_mut(e: &mut Expr, f: &mut dyn FnMut(&mut Expr)) {
    struct V<'a> {
        depth: usize,
        f: &'a mut dyn FnMut(&mut Expr),
    }
    impl VisitorMut for V<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _q: &mut Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _q: &mut Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
            if self.depth == 0 {
                (self.f)(e);
            }
            ControlFlow::Continue(())
        }
    }
    let _ = e.visit(&mut V { depth: 0, f });
}

const AGGS: [&str; 5] = ["sum", "count", "avg", "min", "max"];

fn is_agg(e: &Expr) -> bool {
    matches!(e, Expr::Function(f) if f.name.0.len() == 1 && AGGS.iter().any(|a| f.name.to_string().eq_ignore_ascii_case(a)))
}

/// Whether the SELECT aggregates (an aggregate in its list, or GROUP BY).
fn aggregated(sel: &Select) -> bool {
    if matches!(&sel.group_by, GroupByExpr::Expressions(es, _) if !es.is_empty()) || matches!(sel.group_by, GroupByExpr::All(_)) {
        return true;
    }
    let mut any = false;
    for it in &sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = it {
            let _ = top_exprs(e, &mut |x| any |= is_agg(x));
        }
    }
    any
}

fn is_count_distinct(e: &Expr) -> bool {
    match e {
        Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("count") => {
            matches!(&f.args, FunctionArguments::List(l) if l.duplicate_treatment == Some(DuplicateTreatment::Distinct))
        }
        _ => false,
    }
}

fn count_distinct(sel: &Select) -> bool {
    let mut any = false;
    for it in &sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = it {
            let _ = top_exprs(e, &mut |x| any |= is_count_distinct(x));
        }
    }
    any
}

/// A derived table the engine's flattener refuses: grouped, joined, aliased
/// columns, or more than one FROM item.
fn derived_needs_stage(sel: &Select) -> bool {
    if sel.from.len() > 1 {
        return true;
    }
    sel.from.iter().any(|f| {
        !f.joins.is_empty()
            || match &f.relation {
                TableFactor::Derived { lateral, subquery, alias } => {
                    *lateral
                        || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
                        || subquery.with.is_some()
                        || subquery.order_by.is_some()
                        || subquery.limit.is_some()
                        || match subquery.body.as_ref() {
                            SetExpr::Select(s) => s.having.is_some() || s.distinct.is_some() || aggregated(s) || count_distinct(s),
                            _ => true,
                        }
                }
                _ => false,
            }
    })
}

/// An `IN (subquery)` the semi-join cannot take as it is: more than one
/// table, or subqueries of its own.
fn in_needs_stage(q: &Query) -> bool {
    let SetExpr::Select(s) = q.body.as_ref() else { return true };
    if q.with.is_some() || s.from.len() != 1 || !s.from[0].joins.is_empty() || !matches!(s.from[0].relation, TableFactor::Table { .. }) {
        return true;
    }
    let mut nested = false;
    if let Some(w) = &s.selection {
        let _ = top_exprs(w, &mut |x| nested |= matches!(x, Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }) && !simple_semi(x));
    }
    nested
}

/// A nested semi-join the engine takes itself (one table, no subqueries).
fn simple_semi(e: &Expr) -> bool {
    match e {
        Expr::InSubquery { subquery, .. } => !in_needs_stage(subquery),
        _ => false,
    }
}

struct Stager {
    run: mem::Run,
    sources: HashMap<String, String>,
    n: usize,
    /// CTE name → stage table name.
    ctes: HashMap<String, String>,
    /// (source, table) → column names, as probed.
    cols: HashMap<(String, String), Vec<String>>,
}

/// What is done to the engine's answer before it is returned.
#[derive(Default)]
struct Post {
    /// Each row repeated by this column's count (then the column dropped),
    /// and at most this many rows kept.
    repeat: Option<(String, Option<usize>)>,
}

fn parse_expr(s: &str) -> Result<Expr> {
    sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
        .try_with_sql(s)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| bad(format!("internal rewrite produced bad SQL ({s}): {e}")))
}

fn parse_query(s: &str) -> Result<Query> {
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, s)
        .map_err(|e| bad(format!("internal rewrite produced bad SQL ({s}): {e}")))?;
    match stmts.into_iter().next() {
        Some(Statement::Query(q)) => Ok(*q),
        _ => Err(bad(format!("internal rewrite produced a non-query ({s})"))),
    }
}

fn item_name(it: &SelectItem, i: usize) -> String {
    match it {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.clone(),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(p)) => p.last().map(|x| x.value.clone()).unwrap_or_default(),
        _ => format!("__c{i}"),
    }
}

fn item_expr(it: &SelectItem) -> Result<&Expr> {
    match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Ok(e),
        _ => Err(unsupported("SELECT *")),
    }
}

fn days_to_date(z: i64) -> String {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{:02}-{:02}", yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn dec_text(v: i128, s: i8) -> String {
    if s <= 0 {
        return (v * 10i128.pow(u32::from(s.unsigned_abs()))).to_string();
    }
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let s = s as usize;
    let digits = if digits.len() <= s { format!("{}{digits}", "0".repeat(s + 1 - digits.len())) } else { digits };
    let (i, f) = digits.split_at(digits.len() - s);
    format!("{}{i}.{f}", if neg { "-" } else { "" })
}

/// A one-row, one-column answer as a SQL literal.
fn literal(r: &SqlResult) -> Result<Expr> {
    if r.schema.len() != 1 {
        return Err(bad("a scalar subquery must return one column"));
    }
    if r.batch.rows > 1 {
        return Err(bad("a scalar subquery returned more than one row"));
    }
    if r.batch.rows == 0 {
        return parse_expr("NULL");
    }
    let c = &r.batch.cols[0];
    let text = match (c, r.schema[0].kind) {
        (FinishedCol::I32 { validity, data }, ArrowKind::Date32) if valid(validity, 0) => {
            format!("CAST('{}' AS DATE)", days_to_date(i64::from(data[0])))
        }
        (FinishedCol::Dec128 { validity, data }, ArrowKind::Decimal { s, .. }) if valid(validity, 0) => dec_text(data[0], s),
        (FinishedCol::F64 { validity, data }, _) if valid(validity, 0) => {
            let v = data[0];
            if !v.is_finite() {
                return Err(bad("a scalar subquery gave a non-finite number"));
            }
            let t = format!("{v}");
            if t.contains('.') { t } else { format!("{t}.0") }
        }
        (FinishedCol::I16 { validity, data }, _) if valid(validity, 0) => data[0].to_string(),
        (FinishedCol::I32 { validity, data }, _) if valid(validity, 0) => data[0].to_string(),
        (FinishedCol::I64 { validity, data }, _) if valid(validity, 0) => data[0].to_string(),
        (FinishedCol::Bool { validity, data }, _) if valid(validity, 0) => (data[0] & 1 == 1).to_string().to_uppercase(),
        (FinishedCol::Utf8 { validity, offsets, data }, _) if valid(validity, 0) => {
            let t = String::from_utf8_lossy(&data[offsets[0] as usize..offsets[1] as usize]).replace('\'', "''");
            format!("'{t}'")
        }
        _ => "NULL".to_string(),
    };
    parse_expr(&text)
}

impl Stager {
    fn put(&mut self, hint: &str, r: Answer) -> Result<String> {
        self.n += 1;
        let name = format!("st{}_{}", self.n, hint.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').take(24).collect::<String>());
        self.run.put(&name, r.into_table()?);
        Ok(name)
    }

    /// The answer as rows (a scalar subquery, the query's own answer).
    async fn answer(&mut self, q: Query) -> Result<SqlResult> {
        self.answer_out(q).await?.into_result()
    }

    /// The column names of the table `tf` names, probed once.
    async fn table_cols(&mut self, tf: &TableFactor) -> Result<Vec<String>> {
        let TableFactor::Table { name, .. } = tf else { return Ok(Vec::new()) };
        let parts: Vec<String> = name.0.iter().map(|p| match p {
            ObjectNamePart::Identifier(i) => ident_name(i),
        }).collect();
        if parts.len() < 2 {
            return Ok(Vec::new());
        }
        let key = (parts[0].clone(), parts[1..].join("."));
        if let Some(c) = self.cols.get(&key) {
            return Ok(c.clone());
        }
        let Some(url) = self.sources.get(&key.0).cloned() else { return Ok(Vec::new()) };
        let schema = crate::read::read_schema(&url, &key.1).await?;
        let names: Vec<String> = schema.into_iter().map(|f| f.name.to_ascii_lowercase()).collect();
        self.cols.insert(key, names.clone());
        Ok(names)
    }

    /// Whether `q` names a column none of its own tables (at any depth) has:
    /// a reference to an enclosing query.
    async fn correlated(&mut self, q: &Query) -> Result<bool> {
        struct T(Vec<TableFactor>, Vec<String>);
        impl Visitor for T {
            type Break = ();
            fn pre_visit_table_factor(&mut self, tf: &TableFactor) -> ControlFlow<()> {
                if let TableFactor::Table { alias, name, .. } = tf {
                    self.0.push(tf.clone());
                    if let Some(a) = alias {
                        self.1.push(a.name.value.to_ascii_lowercase());
                    }
                    if let Some(ObjectNamePart::Identifier(i)) = name.0.last() {
                        self.1.push(i.value.to_ascii_lowercase());
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let mut t = T(Vec::new(), Vec::new());
        let _ = q.visit(&mut t);
        let mut own: Vec<String> = Vec::new();
        for tf in &t.0 {
            own.extend(self.table_cols(tf).await?);
        }
        // Output aliases of the query's own SELECT lists count as its own.
        struct A(Vec<String>);
        impl Visitor for A {
            type Break = ();
            fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
                if let SetExpr::Select(s) = q.body.as_ref() {
                    for it in &s.projection {
                        if let SelectItem::ExprWithAlias { alias, .. } = it {
                            self.0.push(alias.value.to_ascii_lowercase());
                        }
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let mut a = A(Vec::new());
        let _ = q.visit(&mut a);
        struct C<'a> {
            own: &'a [String],
            tables: &'a [String],
            aliases: &'a [String],
            outer: bool,
        }
        impl Visitor for C<'_> {
            type Break = ();
            fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
                match e {
                    Expr::Identifier(i) => {
                        let n = i.value.to_ascii_lowercase();
                        if !self.own.contains(&n) && !self.aliases.contains(&n) {
                            self.outer = true;
                        }
                    }
                    Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                        let t = p[p.len() - 2].value.to_ascii_lowercase();
                        if !self.tables.contains(&t) {
                            self.outer = true;
                        }
                    }
                    _ => {}
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = C { own: &own, tables: &t.1, aliases: &a.0, outer: false };
        let _ = q.visit(&mut c);
        Ok(c.outer)
    }

    /// CTE names in FROM → their stage tables (aliased by the CTE's name).
    fn point_ctes(&self, q: &mut Query) {
        struct P<'a>(&'a HashMap<String, String>);
        impl VisitorMut for P<'_> {
            type Break = ();
            fn pre_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<()> {
                if let TableFactor::Table { name, alias, .. } = tf {
                    if let [ObjectNamePart::Identifier(i)] = name.0.as_slice() {
                        if let Some(st) = self.0.get(&i.value.to_ascii_lowercase()) {
                            if alias.is_none() {
                                *alias = Some(TableAlias { name: i.clone(), columns: Vec::new() });
                            }
                            *name = ObjectName(vec![
                                ObjectNamePart::Identifier(Ident::new(STAGE_SRC)),
                                ObjectNamePart::Identifier(Ident::new(st.clone())),
                            ]);
                        }
                    }
                }
                ControlFlow::Continue(())
            }
        }
        if !self.ctes.is_empty() {
            let _ = q.visit(&mut P(&self.ctes));
        }
    }

    /// The answer of `q`, its parts staged as needed.
    /// The answer, left a stage table when it came in parts.
    fn answer_out(&mut self, q: Query) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Answer>> + '_>> {
        Box::pin(async move {
            let mut q = q;
            if let Some(w) = q.with.take() {
                if w.recursive {
                    return Err(unsupported("WITH RECURSIVE"));
                }
                for cte in w.cte_tables {
                    let mut cq = *cte.query;
                    self.point_ctes(&mut cq);
                    let mut r = self.answer_out(cq).await?;
                    rename(r.schema_mut(), &cte.alias)?;
                    let st = self.put(&cte.alias.name.value, r)?;
                    self.ctes.insert(cte.alias.name.value.to_ascii_lowercase(), st);
                }
            }
            self.point_ctes(&mut q);
            let SetExpr::Select(sel) = q.body.as_ref() else {
                return Err(unsupported("anything but a plain SELECT (UNION, VALUES, …)"));
            };
            let mut sel = (**sel).clone();
            self.prepare(&mut sel).await?;
            let order_by = q.order_by.clone();
            let limit = q.limit.clone();
            if let Some(r) = self.left_join(&sel, &order_by, &limit).await? {
                return Ok(r);
            }
            // `aggregate op number` conjuncts the engine filters itself, on
            // its final groups; anything else is staged (R4).
            if sel.having.as_ref().is_some_and(|h| !engine_having(h)) || (sel.having.is_some() && count_distinct(&sel)) {
                return self.having(sel, order_by, limit).await;
            }
            if count_distinct(&sel) {
                return self.count_distinct(sel, order_by, limit).await;
            }
            let mut post = Post::default();
            let mut out = Query { body: Box::new(SetExpr::Select(Box::new(sel.clone()))), order_by, limit, ..q.clone() };
            if !aggregated(&sel) {
                // No aggregate: GROUP BY every output, a hidden count of
                // each group's rows, and the rows repeated by it after.
                let items: Vec<Expr> = sel.projection.iter().map(|it| item_expr(it).cloned()).collect::<Result<_>>()?;
                let mut s2 = sel.clone();
                let distinct = s2.distinct.take().is_some();
                s2.group_by = GroupByExpr::Expressions(items, Vec::new());
                if !distinct {
                    s2.projection.push(SelectItem::ExprWithAlias { expr: parse_expr("COUNT(*)")?, alias: Ident::new("__n") });
                    let lim = match out.limit.take() {
                        None => None,
                        Some(e) => Some(e.to_string().parse::<usize>().map_err(|_| unsupported(format!("LIMIT {e}")))?),
                    };
                    post.repeat = Some(("__n".to_string(), lim));
                }
                out.body = Box::new(SetExpr::Select(Box::new(s2)));
            } else if sel.distinct.is_some() {
                return Err(unsupported("DISTINCT with aggregates"));
            }
            let text = out.to_string();
            if super::dbg() {
                eprintln!("[sql] stage query ({} MiB resident, {} anon): {text}", semi::rss_bytes().unwrap_or(0) >> 20, semi::anon_bytes().unwrap_or(0) >> 20);
            }
            if post.repeat.is_none() {
                return super::run_one_out(&text, &self.sources).await;
            }
            let r = super::run_one(&text, &self.sources).await?;
            apply(r, post).map(Answer::Rows)
        })
    }

    /// The parts of one SELECT answered first: derived tables the engine
    /// cannot flatten, scalar subqueries (→ literals), `IN` subqueries the
    /// semi-join cannot take (→ a stage's keys).
    fn prepare<'a>(&'a mut self, sel: &'a mut Select) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
        Box::pin(self.prepare_inner(sel))
    }

    async fn prepare_inner(&mut self, sel: &mut Select) -> Result<()> {
        let stage_derived = derived_needs_stage(sel);
        for f in sel.from.iter_mut() {
            let mut rels: Vec<&mut TableFactor> = vec![&mut f.relation];
            rels.extend(f.joins.iter_mut().map(|j| &mut j.relation));
            for tf in rels {
                let TableFactor::Derived { subquery, alias, .. } = tf else { continue };
                if stage_derived {
                    let mut r = self.answer_out((**subquery).clone()).await?;
                    let nm = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| "derived".into());
                    if let Some(a) = alias.as_ref() {
                        rename(r.schema_mut(), a)?;
                    }
                    let st = self.put(&nm, r)?;
                    *tf = TableFactor::Table {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(STAGE_SRC)), ObjectNamePart::Identifier(Ident::new(st))]),
                        alias: Some(TableAlias { name: Ident::new(nm), columns: Vec::new() }),
                        args: None,
                        with_hints: Vec::new(),
                        version: None,
                        with_ordinality: false,
                        partitions: Vec::new(),
                        json_path: None,
                        sample: None,
                        index_hints: Vec::new(),
                    };
                } else if let SetExpr::Select(inner) = subquery.body.as_mut() {
                    // Flattened by the engine: its own subqueries first.
                    self.prepare(inner).await?;
                }
            }
        }
        self.narrow_exists(sel).await?;
        // Subqueries of this level, answered first.
        let mut subs: Vec<(String, Query, bool)> = Vec::new();
        for e in select_exprs(sel) {
            let _ = top_exprs(e, &mut |x| match x {
                Expr::Subquery(q) => subs.push((q.to_string(), (**q).clone(), false)),
                Expr::InSubquery { subquery, .. } if in_needs_stage(subquery) => subs.push((subquery.to_string(), (**subquery).clone(), true)),
                _ => {}
            });
        }
        let mut done: HashMap<String, Expr> = HashMap::new();
        let mut keys: HashMap<String, Query> = HashMap::new();
        let mut joins: Vec<(TableFactor, Vec<Expr>)> = Vec::new();
        let in_where: Vec<String> = {
            let mut v = Vec::new();
            if let Some(w) = &sel.selection {
                let _ = top_exprs(w, &mut |x| {
                    if let Expr::Subquery(q) = x {
                        v.push(q.to_string());
                    }
                });
            }
            v
        };
        for (text, q, is_in) in subs {
            if done.contains_key(&text) || keys.contains_key(&text) {
                continue;
            }
            if self.correlated(&q).await? {
                if is_in || !in_where.contains(&text) {
                    return Err(unsupported(format!("the correlated subquery ({q}) outside WHERE or in IN (a scalar one in WHERE)")));
                }
                let (tf, eqs, v) = self.decorrelate(&q, sel).await?;
                done.insert(text, v);
                joins.push((tf, eqs));
                continue;
            }
            if is_in {
                let r = self.answer_out(q).await?;
                if r.schema().len() != 1 {
                    return Err(bad("an IN subquery must return one column"));
                }
                let col = r.schema()[0].name.clone();
                let st = self.put("in", r)?;
                keys.insert(text, parse_query(&format!("SELECT {col} FROM {STAGE_SRC}.{st}"))?);
            } else {
                let r = self.answer(q).await?;
                done.insert(text, literal(&r)?);
            }
        }
        let swap = |e: &mut Expr| {
            top_exprs_mut(e, &mut |x| match x {
                Expr::Subquery(q) => {
                    if let Some(l) = done.get(&q.to_string()) {
                        *x = match l {
                            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => l.clone(),
                            _ => Expr::Nested(Box::new(l.clone())),
                        };
                    }
                }
                Expr::InSubquery { subquery, .. } => {
                    if let Some(k) = keys.get(&subquery.to_string()) {
                        **subquery = k.clone();
                    }
                }
                _ => {}
            });
        };
        if let Some(w) = sel.selection.as_mut() {
            swap(w);
        }
        if let Some(h) = sel.having.as_mut() {
            swap(h);
        }
        for it in sel.projection.iter_mut() {
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = it {
                swap(e);
            }
        }
        for (tf, eqs) in joins {
            sel.from.push(sqlparser::ast::TableWithJoins { relation: tf, joins: Vec::new() });
            for e in eqs {
                sel.selection = Some(match sel.selection.take() {
                    Some(w) => Expr::BinaryOp { left: Box::new(w), op: BinaryOperator::And, right: Box::new(e) },
                    None => e,
                });
            }
        }
        Ok(())
    }

    /// `[NOT] EXISTS` with a comparison to the outer row beside its key
    /// equality (TPC-H Q21's `l2.l_suppkey <> l1.l_suppkey`): the engine
    /// keeps each key's least and greatest compared value, so the keys the
    /// subquery reads are narrowed first to the outer query's own: the
    /// outer FROM and WHERE without the EXISTS conditions, grouped by the
    /// correlation key, staged, and the subquery's key `IN` it.
    async fn narrow_exists(&mut self, sel: &mut Select) -> Result<()> {
        let Some(w) = sel.selection.clone() else { return Ok(()) };
        let mut conj = Vec::new();
        conjuncts(&w, &mut conj);
        if !conj.iter().any(|c| matches!(c, Expr::Exists { .. })) {
            return Ok(());
        }
        let outer_t = self.from_tables(sel).await?;
        let rest: Vec<String> = conj.iter().filter(|c| !matches!(c, Expr::Exists { .. })).map(|c| c.to_string()).collect();
        let mut magic: HashMap<String, String> = HashMap::new();
        let mut out = Vec::new();
        for c in conj {
            let Expr::Exists { subquery, negated } = &c else {
                out.push(c);
                continue;
            };
            let SetExpr::Select(sub) = subquery.body.as_ref() else {
                out.push(c);
                continue;
            };
            let inner = self.from_tables(sub).await?;
            let is_inner = |e: &Expr| -> Option<bool> {
                match e {
                    Expr::Identifier(i) => {
                        let n = i.value.to_ascii_lowercase();
                        Some(inner.iter().any(|t| t.2.contains(&n)))
                    }
                    Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                        let t = p[p.len() - 2].value.to_ascii_lowercase();
                        Some(inner.iter().any(|x| x.1.contains(&t)))
                    }
                    _ => None,
                }
            };
            let mut ic = Vec::new();
            if let Some(sw) = &sub.selection {
                conjuncts(sw, &mut ic);
            }
            let (mut key, mut cmp) = (None, false);
            for x in &ic {
                if let Expr::BinaryOp { left, op, right } = x {
                    let pair = match (is_inner(left), is_inner(right)) {
                        (Some(true), Some(false)) => Some(((**left).clone(), (**right).clone())),
                        (Some(false), Some(true)) => Some(((**right).clone(), (**left).clone())),
                        _ => None,
                    };
                    match (pair, op) {
                        (Some(p), BinaryOperator::Eq) if key.is_none() => key = Some(p),
                        (Some(_), _) => cmp = true,
                        _ => {}
                    }
                }
            }
            let (Some((ik, ok)), true) = (key, cmp) else {
                out.push(c);
                continue;
            };
            // The outer key's own table must be among the outer FROM.
            let known = match &ok {
                Expr::Identifier(i) => outer_t.iter().any(|t| t.2.contains(&i.value.to_ascii_lowercase())),
                Expr::CompoundIdentifier(p) if p.len() >= 2 => outer_t.iter().any(|t| t.1.contains(&p[p.len() - 2].value.to_ascii_lowercase())),
                _ => false,
            };
            if !known {
                out.push(c);
                continue;
            }
            let mtext = ok.to_string();
            let st = match magic.get(&mtext) {
                Some(st) => st.clone(),
                None => {
                    let from = sel.from.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ");
                    let wh = if rest.is_empty() { String::new() } else { format!(" WHERE {}", rest.join(" AND ")) };
                    let q = parse_query(&format!("SELECT {ok} AS __k, COUNT(*) AS __n FROM {from}{wh} GROUP BY {ok}"))?;
                    let r = self.answer_out(q).await?;
                    let st = self.put("keys", r)?;
                    magic.insert(mtext, st.clone());
                    st
                }
            };
            let mut sq = (**subquery).clone();
            if let SetExpr::Select(s2) = sq.body.as_mut() {
                let narrow = parse_expr(&format!("{ik} IN (SELECT __k FROM {STAGE_SRC}.{st})"))?;
                s2.selection = Some(match s2.selection.take() {
                    Some(w) => Expr::BinaryOp { left: Box::new(w), op: BinaryOperator::And, right: Box::new(narrow) },
                    None => narrow,
                });
            }
            out.push(Expr::Exists { subquery: Box::new(sq), negated: *negated });
        }
        sel.selection = Some(and_of(out));
        Ok(())
    }

    /// Per table of `sel`'s FROM: (the table, its qualifiers, its columns).
    async fn from_tables(&mut self, sel: &Select) -> Result<Vec<(TableFactor, Vec<String>, Vec<String>)>> {
        let mut out = Vec::new();
        for f in &sel.from {
            for tf in std::iter::once(&f.relation).chain(f.joins.iter().map(|j| &j.relation)) {
                let mut q = Vec::new();
                if let TableFactor::Table { name, alias, .. } = tf {
                    if let Some(a) = alias {
                        q.push(a.name.value.to_ascii_lowercase());
                    }
                    if let Some(ObjectNamePart::Identifier(i)) = name.0.last() {
                        q.push(i.value.to_ascii_lowercase());
                    }
                }
                let cols = self.table_cols(tf).await?;
                out.push((tf.clone(), q, cols));
            }
        }
        Ok(out)
    }

    /// A correlated scalar subquery of the WHERE (`l_quantity < (SELECT 0.2 *
    /// AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)`, TPC-H Q2
    /// Q17 Q20): its aggregate per correlation key, staged, narrowed to the
    /// keys the outer table's own filters keep (`l_partkey IN (SELECT
    /// p_partkey FROM part WHERE …)`, a semi-join apitap evaluates), and
    /// joined by those keys. Returns the stage as a FROM item, the join
    /// equalities, and the value expression that replaces the subquery. A
    /// key with no rows gives no stage row, so the comparison fails, as the
    /// subquery's NULL makes it fail; COUNT (0, not NULL) is refused.
    async fn decorrelate(&mut self, q: &Query, outer: &Select) -> Result<(TableFactor, Vec<Expr>, Expr)> {
        let no = |w: &str| unsupported(format!("the correlated subquery ({q}): {w}"));
        let SetExpr::Select(sub) = q.body.as_ref() else { return Err(no("not a plain SELECT")) };
        if q.with.is_some() || q.order_by.is_some() || q.limit.is_some() || sub.having.is_some() || sub.distinct.is_some() {
            return Err(no("WITH, ORDER BY, LIMIT, HAVING or DISTINCT"));
        }
        if !matches!(&sub.group_by, GroupByExpr::Expressions(es, _) if es.is_empty()) {
            return Err(no("GROUP BY"));
        }
        let [item] = sub.projection.as_slice() else { return Err(no("more than one column")) };
        let value = item_expr(item)?.clone();
        let mut counts = false;
        let _ = top_exprs(&value, &mut |x| {
            if let Expr::Function(f) = x {
                if f.name.to_string().eq_ignore_ascii_case("count") {
                    counts = true;
                }
            }
        });
        if counts || !aggregated(sub) {
            return Err(no("not an aggregate other than COUNT"));
        }
        let inner = self.from_tables(sub).await?;
        let outer_t = self.from_tables(outer).await?;
        // Which table list an expression's columns come from: inner (0),
        // outer (1), both (2); `None` names no column.
        let whose = |e: &Expr| -> Option<u8> {
            let mut m = 0u8;
            let _ = top_exprs(e, &mut |x| match x {
                Expr::Identifier(i) => {
                    let n = i.value.to_ascii_lowercase();
                    m |= if inner.iter().any(|t| t.2.contains(&n)) { 1 } else { 2 };
                }
                Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                    let t = p[p.len() - 2].value.to_ascii_lowercase();
                    m |= if inner.iter().any(|x| x.1.contains(&t)) { 1 } else { 2 };
                }
                _ => {}
            });
            match m {
                0 => None,
                1 => Some(0),
                2 => Some(1),
                _ => Some(2),
            }
        };
        let mut conj = Vec::new();
        if let Some(w) = &sub.selection {
            conjuncts(w, &mut conj);
        }
        let (mut corr, mut rest) = (Vec::new(), Vec::new());
        for c in conj {
            match whose(&c) {
                Some(2) => match &c {
                    Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => match (whose(left), whose(right)) {
                        (Some(0), Some(1)) => corr.push(((**left).clone(), (**right).clone())),
                        (Some(1), Some(0)) => corr.push(((**right).clone(), (**left).clone())),
                        _ => return Err(no(&format!("the correlation {c} (equalities of a column of each)"))),
                    },
                    _ => return Err(no(&format!("the correlation {c} (equalities of a column of each)"))),
                },
                Some(1) => return Err(no(&format!("the condition {c} on the outer query alone"))),
                _ => rest.push(c),
            }
        }
        if corr.is_empty() {
            return Err(no("no correlation equality"));
        }
        // The outer table of the first key and its own filters: the stage's
        // keys narrowed to the ones they keep.
        let oname = |e: &Expr| -> Option<usize> {
            match e {
                Expr::Identifier(i) => outer_t.iter().position(|t| t.2.contains(&i.value.to_ascii_lowercase())),
                Expr::CompoundIdentifier(p) if p.len() >= 2 => outer_t.iter().position(|t| t.1.contains(&p[p.len() - 2].value.to_ascii_lowercase())),
                _ => None,
            }
        };
        let mut narrow: Option<String> = None;
        if let Some(ot) = oname(&corr[0].1) {
            let mut oc = Vec::new();
            if let Some(w) = &outer.selection {
                conjuncts(w, &mut oc);
            }
            let only_ot = |e: &Expr| -> bool {
                let mut ok = true;
                let mut any = false;
                let _ = top_exprs(e, &mut |x| match x {
                    Expr::Identifier(i) => {
                        any = true;
                        let n = i.value.to_ascii_lowercase();
                        if !outer_t[ot].2.contains(&n) || outer_t.iter().enumerate().any(|(k, t)| k != ot && t.2.contains(&n)) {
                            ok = false;
                        }
                    }
                    Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                        any = true;
                        if !outer_t[ot].1.contains(&p[p.len() - 2].value.to_ascii_lowercase()) {
                            ok = false;
                        }
                    }
                    Expr::Subquery(_) | Expr::Exists { .. } => ok = false,
                    _ => {}
                });
                ok && any
            };
            let okey = &corr[0].1;
            // `key IN (subquery)` of the outer table: the same keys, asked
            // directly (TPC-H Q20's forest parts).
            let direct = oc.iter().find_map(|c| match c {
                Expr::InSubquery { expr, subquery, negated: false } if **expr == *okey || matches!((&**expr, okey), (Expr::Identifier(a), Expr::CompoundIdentifier(b)) | (Expr::CompoundIdentifier(b), Expr::Identifier(a)) if b.last().is_some_and(|x| x.value.eq_ignore_ascii_case(&a.value))) => Some(subquery.to_string()),
                _ => None,
            });
            let filters: Vec<String> = oc.iter().filter(|c| only_ot(c)).map(|c| c.to_string()).collect();
            narrow = match direct {
                Some(sq) => Some(sq),
                None if !filters.is_empty() => {
                    let col = match okey {
                        Expr::CompoundIdentifier(p) => p.last().map(|x| x.value.clone()).unwrap_or_default(),
                        e => e.to_string(),
                    };
                    let tname = match &outer_t[ot].0 {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => String::new(),
                    };
                    (!tname.is_empty()).then(|| format!("SELECT {col} FROM {tname} WHERE {}", filters.join(" AND ")))
                }
                None => None,
            };
        }
        let mut wh: Vec<String> = rest.iter().map(|c| c.to_string()).collect();
        if let Some(n) = &narrow {
            wh.push(format!("{} IN ({n})", corr[0].0));
        }
        let keys: Vec<String> = corr.iter().enumerate().map(|(i, (ic, _))| format!("{ic} AS __k{i}")).collect();
        let gby: Vec<String> = corr.iter().map(|(ic, _)| ic.to_string()).collect();
        let from = sub.from.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ");
        let text = format!(
            "SELECT {}, {value} AS __v FROM {from}{} GROUP BY {}",
            keys.join(", "),
            if wh.is_empty() { String::new() } else { format!(" WHERE {}", wh.join(" AND ")) },
            gby.join(", ")
        );
        let r = self.answer_out(parse_query(&text)?).await?;
        let st = self.put("corr", r)?;
        self.n += 1;
        let alias = format!("__sq{}", self.n);
        let tf = TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(STAGE_SRC)), ObjectNamePart::Identifier(Ident::new(st))]),
            alias: Some(TableAlias { name: Ident::new(alias.clone()), columns: Vec::new() }),
            args: None,
            with_hints: Vec::new(),
            version: None,
            with_ordinality: false,
            partitions: Vec::new(),
            json_path: None,
            sample: None,
            index_hints: Vec::new(),
        };
        let eqs = corr
            .iter()
            .enumerate()
            .map(|(i, (_, oc))| parse_expr(&format!("{oc} = {alias}.__k{i}")))
            .collect::<Result<Vec<_>>>()?;
        Ok((tf, eqs, parse_expr(&format!("{alias}.__v"))?))
    }

    /// Which of `names` (qualifiers: alias and table name per side) an
    /// expression's columns belong to: bit 0 = the preserved side, bit 1 =
    /// the other; bit 2 = a column of neither.
    fn sides_mask(e: &Expr, cols: &[Vec<String>; 2], quals: &[Vec<String>; 2]) -> u8 {
        let mut m = 0u8;
        let _ = top_exprs(e, &mut |x| match x {
            Expr::Identifier(i) => {
                let n = i.value.to_ascii_lowercase();
                match (cols[0].contains(&n), cols[1].contains(&n)) {
                    (true, false) => m |= 1,
                    (false, true) => m |= 2,
                    _ => m |= 4,
                }
            }
            Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                let q = p[p.len() - 2].value.to_ascii_lowercase();
                m |= if quals[0].contains(&q) { 1 } else if quals[1].contains(&q) { 2 } else { 4 };
            }
            _ => {}
        });
        m
    }

    /// `P LEFT JOIN N ON p.k = n.k [AND n-only conditions] … GROUP BY`
    /// (TPC-H Q13): the inner join's groups, and P's rows without a match
    /// (`NOT EXISTS`) grouped with N's values NULL: COUNT(n…) 0, COUNT(*)
    /// their rows, any other aggregate of N NULL.
    async fn left_join(&mut self, sel: &Select, order_by: &Option<sqlparser::ast::OrderBy>, limit: &Option<Expr>) -> Result<Option<Answer>> {
        let mut lj = None;
        for (fi, f) in sel.from.iter().enumerate() {
            for (ji, j) in f.joins.iter().enumerate() {
                match &j.join_operator {
                    JoinOperator::Left(JoinConstraint::On(e)) | JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
                        if lj.is_some() {
                            return Err(unsupported("more than one LEFT JOIN"));
                        }
                        lj = Some((fi, ji, e.clone()));
                    }
                    JoinOperator::Inner(JoinConstraint::On(_)) | JoinOperator::Join(JoinConstraint::On(_)) => {}
                    _ => return Err(unsupported("a join that is not INNER JOIN … ON or LEFT JOIN … ON")),
                }
            }
        }
        let Some((fi, ji, on)) = lj else { return Ok(None) };
        if sel.from.len() != 1 || sel.from[fi].joins.len() != 1 || ji != 0 {
            return Err(unsupported("a LEFT JOIN beside other tables (two tables only)"));
        }
        let (pt, nt) = (&sel.from[0].relation, &sel.from[0].joins[0].relation);
        let qual = |tf: &TableFactor| -> Vec<String> {
            let mut v = Vec::new();
            if let TableFactor::Table { name, alias, .. } = tf {
                if let Some(a) = alias {
                    v.push(a.name.value.to_ascii_lowercase());
                }
                if let Some(ObjectNamePart::Identifier(i)) = name.0.last() {
                    v.push(i.value.to_ascii_lowercase());
                }
            }
            v
        };
        let cols = [self.table_cols(pt).await?, self.table_cols(nt).await?];
        let quals = [qual(pt), qual(nt)];
        let mask = |e: &Expr| Self::sides_mask(e, &cols, &quals);
        // ON: one key equality, the rest conditions of N alone.
        let mut ons = Vec::new();
        conjuncts(&on, &mut ons);
        let (mut key, mut ncond) = (None, Vec::new());
        for c in ons {
            match (&c, mask(&c)) {
                (Expr::BinaryOp { left, op: BinaryOperator::Eq, right }, 3) if key.is_none() => {
                    let (l, r) = (mask(left), mask(right));
                    key = match (l, r) {
                        (1, 2) => Some(((**left).clone(), (**right).clone())),
                        (2, 1) => Some(((**right).clone(), (**left).clone())),
                        _ => return Err(unsupported(format!("the LEFT JOIN condition {c}"))),
                    };
                }
                (_, 2) => ncond.push(c),
                _ => return Err(unsupported(format!("the LEFT JOIN condition {c} (one key equality and conditions of the joined table only)"))),
            }
        }
        let (pk, nk) = key.ok_or_else(|| unsupported("a LEFT JOIN without a key equality"))?;
        if let Some(w) = &sel.selection {
            if mask(w) & !1 != 0 {
                return Err(unsupported("a WHERE over the LEFT JOIN's joined table (it would make the join inner)"));
            }
        }
        let names: Vec<String> = sel.projection.iter().enumerate().map(|(i, it)| item_name(it, i)).collect();
        let exprs: Vec<Expr> = sel.projection.iter().map(|it| item_expr(it).cloned()).collect::<Result<_>>()?;
        let gby = match &sel.group_by {
            GroupByExpr::Expressions(es, m) if m.is_empty() && !es.is_empty() => {
                let items: Vec<(Expr, Option<String>)> = exprs.iter().cloned().zip(names.iter().cloned().map(Some)).collect();
                group_list(&GroupByExpr::Expressions(es.clone(), Vec::new()), &items)?
            }
            _ => return Err(unsupported("a LEFT JOIN without GROUP BY")),
        };
        if gby.iter().any(|g| mask(g) != 1) {
            return Err(unsupported("a LEFT JOIN grouped by the joined table's columns"));
        }
        // Per output: the unmatched rows' value.
        enum Fill {
            Group(usize),
            Rows,
            Zero,
            Null,
        }
        let mut fills = Vec::new();
        for e in &exprs {
            if let Some(g) = gby.iter().position(|g| g == e) {
                fills.push(Fill::Group(g));
                continue;
            }
            let Expr::Function(f) = e else { return Err(unsupported(format!("the output {e} of a LEFT JOIN (GROUP BY columns and aggregates)"))) };
            let fname = f.name.to_string().to_ascii_lowercase();
            let star = matches!(&f.args, FunctionArguments::List(l) if matches!(l.args.as_slice(), [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]));
            let m = mask(e);
            fills.push(match (fname.as_str(), star, m) {
                ("count", true, _) => Fill::Rows,
                ("count", false, 2) => Fill::Zero,
                ("sum" | "avg" | "min" | "max", false, 2) => Fill::Null,
                _ => return Err(unsupported(format!("the aggregate {e} of a LEFT JOIN (over the joined table's columns, or COUNT(*))"))),
            });
        }
        // The inner join: the same SELECT with an inner join.
        let mut inner = sel.clone();
        inner.from[0].joins[0].join_operator = JoinOperator::Join(JoinConstraint::On(on.clone()));
        inner.projection = exprs.iter().zip(&names).map(|(e, n)| SelectItem::ExprWithAlias { expr: e.clone(), alias: Ident::new(n.clone()) }).collect();
        let a = self.answer_out(Query { body: Box::new(SetExpr::Select(Box::new(inner))), ..empty_query() }).await?;
        // The unmatched rows of P, grouped.
        let mut sub_where = vec![format!("{nk} = {pk}")];
        sub_where.extend(ncond.iter().map(|c| c.to_string()));
        let anti = format!("NOT EXISTS (SELECT * FROM {nt} WHERE {})", sub_where.join(" AND "));
        let wh = match &sel.selection {
            Some(w) => format!("({w}) AND {anti}"),
            None => anti,
        };
        let gl = gby.iter().map(|g| g.to_string()).collect::<Vec<_>>();
        let gsel = gl.iter().enumerate().map(|(i, g)| format!("{g} AS __g{i}")).collect::<Vec<_>>().join(", ");
        let bq = parse_query(&format!("SELECT {gsel}, COUNT(*) AS __cnt FROM {pt} WHERE {wh} GROUP BY {}", gl.join(", ")))?;
        let b = self.answer_out(bq).await?;
        let cnt = b.schema().len() - 1;
        // Both halves into one stage table, a batch at a time (TPC-H Q13
        // sf10: 6.6M matched and 3.3M unmatched customers).
        let mut w = mem::Writer::new(a.schema().to_vec());
        a.for_each(&mut |x| w.push(x))?;
        let like: Vec<FinishedCol> = w.like().ok_or_else(|| bad("internal: the LEFT JOIN's matched half has no columns"))?.to_vec();
        b.for_each(&mut |mut x| {
            let mut cols = Vec::new();
            for (k, f) in fills.iter().enumerate() {
                cols.push(match f {
                    Fill::Group(g) => std::mem::replace(&mut x.cols[*g], mem::zeros_like(&like[k], 0)),
                    Fill::Rows => std::mem::replace(&mut x.cols[cnt], mem::zeros_like(&like[k], 0)),
                    Fill::Zero => mem::zeros_like(&like[k], x.rows),
                    Fill::Null => mem::nulls_like(&like[k], x.rows),
                });
            }
            w.push(ArrowBatch { rows: x.rows, cols })
        })?;
        let mut r = Answer::Table(w.finish()?);
        if order_by.is_some() || limit.is_some() {
            let st = self.put("leftjoin", r)?;
            let order = order_by.as_ref().map(|o| format!(" {}", rewrite_order(o, &exprs, &names))).unwrap_or_default();
            let lim = limit.as_ref().map(|l| format!(" LIMIT {l}")).unwrap_or_default();
            let cols = names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ");
            r = self.answer_out(parse_query(&format!("SELECT {cols} FROM {STAGE_SRC}.{st}{order}{lim}"))?).await?;
        }
        Ok(Some(r))
    }

    /// `HAVING h`: the grouping staged (h's aggregates as hidden columns),
    /// then the stage filtered by h.
    async fn having(&mut self, mut sel: Select, order_by: Option<sqlparser::ast::OrderBy>, limit: Option<Expr>) -> Result<Answer> {
        let h = sel.having.take().expect("HAVING");
        let names: Vec<String> = sel.projection.iter().enumerate().map(|(i, it)| item_name(it, i)).collect();
        let exprs: Vec<Expr> = sel.projection.iter().map(|it| item_expr(it).cloned()).collect::<Result<_>>()?;
        let mut hidden: Vec<(String, Expr)> = Vec::new();
        let mut h2 = h.clone();
        top_exprs_mut(&mut h2, &mut |x| {
            if is_agg(x) {
                let name = match exprs.iter().position(|e| e == x) {
                    Some(i) => names[i].clone(),
                    None => match hidden.iter().find(|(_, e)| e == x) {
                        Some((n, _)) => n.clone(),
                        None => {
                            let n = format!("__h{}", hidden.len());
                            hidden.push((n.clone(), x.clone()));
                            n
                        }
                    },
                };
                *x = Expr::Identifier(Ident::new(name));
            }
        });
        sel.projection = exprs
            .iter()
            .zip(&names)
            .map(|(e, n)| SelectItem::ExprWithAlias { expr: e.clone(), alias: Ident::new(n.clone()) })
            .chain(hidden.iter().map(|(n, e)| SelectItem::ExprWithAlias { expr: e.clone(), alias: Ident::new(n.clone()) }))
            .collect();
        let inner = Query { body: Box::new(SetExpr::Select(Box::new(sel))), ..empty_query() };
        let r = self.answer_out(inner).await?;
        let st = self.put("having", r)?;
        let order = order_by.map(|o| format!(" {}", rewrite_order(&o, &exprs, &names))).unwrap_or_default();
        let lim = limit.map(|l| format!(" LIMIT {l}")).unwrap_or_default();
        let cols = names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ");
        let q = parse_query(&format!("SELECT {cols} FROM {STAGE_SRC}.{st} WHERE {h2}{order}{lim}"))?;
        self.answer_out(q).await
    }

    /// `COUNT(DISTINCT x) … GROUP BY g`: (g, x) staged, then counted per g.
    async fn count_distinct(&mut self, sel: Select, order_by: Option<sqlparser::ast::OrderBy>, limit: Option<Expr>) -> Result<Answer> {
        let names: Vec<String> = sel.projection.iter().enumerate().map(|(i, it)| item_name(it, i)).collect();
        let exprs: Vec<Expr> = sel.projection.iter().map(|it| item_expr(it).cloned()).collect::<Result<_>>()?;
        let mut arg: Option<Expr> = None;
        for e in &exprs {
            let mut bad_mix = false;
            let _ = top_exprs(e, &mut |x| {
                if is_count_distinct(x) {
                    let Expr::Function(f) = x else { return };
                    let FunctionArguments::List(l) = &f.args else { return };
                    let a = match l.args.as_slice() {
                        [FunctionArg::Unnamed(FunctionArgExpr::Expr(a))] => Some(a.clone()),
                        _ => None,
                    };
                    if a.is_none() || (arg.is_some() && arg != a) {
                        bad_mix = true;
                    }
                    arg = a;
                } else if is_agg(x) {
                    bad_mix = true;
                }
            });
            if bad_mix {
                return Err(unsupported("COUNT(DISTINCT …) beside other aggregates or of more than one argument"));
            }
        }
        let x = arg.ok_or_else(|| unsupported("COUNT(DISTINCT …) of that form"))?;
        let gby = match &sel.group_by {
            GroupByExpr::Expressions(es, m) if m.is_empty() => {
                let items: Vec<(Expr, Option<String>)> = exprs.iter().cloned().zip(names.iter().cloned().map(Some)).collect();
                group_list(&GroupByExpr::Expressions(es.clone(), Vec::new()), &items)?
            }
            _ => return Err(unsupported("GROUP BY ALL or GROUP BY modifiers")),
        };
        // Each group column under its output name when it is one, else __gN.
        let gnames: Vec<String> = gby
            .iter()
            .enumerate()
            .map(|(i, g)| exprs.iter().position(|e| e == g).map_or_else(|| format!("__g{i}"), |k| names[k].clone()))
            .collect();
        let mut s1 = sel.clone();
        s1.projection = gby
            .iter()
            .zip(&gnames)
            .map(|(g, n)| SelectItem::ExprWithAlias { expr: g.clone(), alias: Ident::new(n.clone()) })
            .chain([
                SelectItem::ExprWithAlias { expr: x.clone(), alias: Ident::new("__x") },
                SelectItem::ExprWithAlias { expr: parse_expr("COUNT(*)")?, alias: Ident::new("__n") },
            ])
            .collect();
        let mut g1 = gby.clone();
        g1.push(x.clone());
        s1.group_by = GroupByExpr::Expressions(g1, Vec::new());
        let notnull = parse_expr(&format!("({x}) IS NOT NULL"))?;
        s1.selection = Some(match s1.selection.take() {
            Some(w) => Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(w))), op: BinaryOperator::And, right: Box::new(notnull) },
            None => notnull,
        });
        let r = self.answer_out(Query { body: Box::new(SetExpr::Select(Box::new(s1))), ..empty_query() }).await?;
        let st = self.put("distinct", r)?;
        let mut items = Vec::new();
        for (e, n) in exprs.iter().zip(&names) {
            let mut e2 = e.clone();
            top_exprs_mut(&mut e2, &mut |y| {
                if is_count_distinct(y) {
                    *y = parse_expr("COUNT(*)").expect("COUNT(*)");
                } else if let Some(i) = gby.iter().position(|g| g == y) {
                    *y = Expr::Identifier(Ident::new(gnames[i].clone()));
                }
            });
            items.push(format!("{e2} AS {}", quote_ident(n)));
        }
        let gcols = gnames.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ");
        let group = if gnames.is_empty() { String::new() } else { format!(" GROUP BY {gcols}") };
        let order = order_by.map(|o| format!(" {}", rewrite_order(&o, &exprs, &names))).unwrap_or_default();
        let lim = limit.map(|l| format!(" LIMIT {l}")).unwrap_or_default();
        let q = parse_query(&format!("SELECT {} FROM {STAGE_SRC}.{st}{group}{order}{lim}", items.join(", ")))?;
        self.answer_out(q).await
    }
}

/// A HAVING the engine takes: every conjunct `aggregate op number`.
fn engine_having(h: &Expr) -> bool {
    let mut c = Vec::new();
    conjuncts(h, &mut c);
    c.iter().all(|x| match x {
        Expr::BinaryOp { left, op, right } if super::star::Op::of(op).is_some() => {
            (is_agg(left) && !is_count_distinct(left) && super::number_lit(right).is_some())
                || (is_agg(right) && !is_count_distinct(right) && super::number_lit(left).is_some())
        }
        _ => false,
    })
}

fn quote_ident(n: &str) -> String {
    if n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !n.starts_with(|c: char| c.is_ascii_digit()) {
        n.to_string()
    } else {
        format!("\"{}\"", n.replace('"', "\"\""))
    }
}

/// ORDER BY over a stage of the query's outputs: an expression that is an
/// output becomes its name.
fn rewrite_order(o: &sqlparser::ast::OrderBy, exprs: &[Expr], names: &[String]) -> String {
    let mut o = o.clone();
    if let OrderByKind::Expressions(es) = &mut o.kind {
        for x in es.iter_mut() {
            if let Some(i) = exprs.iter().position(|e| *e == x.expr) {
                x.expr = Expr::Identifier(Ident::new(names[i].clone()));
            }
        }
    }
    o.to_string()
}

fn empty_query() -> Query {
    parse_query("SELECT 1").expect("SELECT 1")
}

/// A stage's columns named by its alias's column list (`AS t (a, b)`).
fn rename(schema: &mut [ArrowField], a: &TableAlias) -> Result<()> {
    if a.columns.is_empty() {
        return Ok(());
    }
    if a.columns.len() != schema.len() {
        return Err(bad(format!("{} names {} columns, its query gives {}", a.name, a.columns.len(), schema.len())));
    }
    for (f, c) in schema.iter_mut().zip(&a.columns) {
        f.name = c.name.value.clone();
    }
    Ok(())
}

fn apply(mut r: SqlResult, post: Post) -> Result<SqlResult> {
    if let Some((col, limit)) = post.repeat {
        let i = r.schema.iter().position(|f| f.name == col).ok_or_else(|| bad("internal: the repeat count went missing"))?;
        let counts: Vec<usize> = match &r.batch.cols[i] {
            FinishedCol::I64 { data, .. } => data.iter().map(|&n| usize::try_from(n).unwrap_or(0)).collect(),
            FinishedCol::I32 { data, .. } => data.iter().map(|&n| usize::try_from(n).unwrap_or(0)).collect(),
            _ => return Err(bad("internal: the repeat count is not an integer")),
        };
        r.schema.remove(i);
        r.batch.cols.remove(i);
        r.batch = mem::repeat(&r.batch, &counts, limit);
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dec_text_keeps_every_digit() {
        assert_eq!(dec_text(17726272087, 4), "1772627.2087");
        assert_eq!(dec_text(-5, 2), "-0.05");
        assert_eq!(dec_text(12, 0), "12");
    }

    #[test]
    fn days_to_date_round_trips() {
        assert_eq!(days_to_date(0), "1970-01-01");
        assert_eq!(days_to_date(9131), "1995-01-01");
        assert_eq!(days_to_date(-1), "1969-12-31");
    }

    #[test]
    fn staging_is_asked_for_by_shape() {
        let q = |s: &str| match sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, s).unwrap().remove(0) {
            Statement::Query(q) => *q,
            _ => unreachable!(),
        };
        assert!(!needs_stages(&q("SELECT a, SUM(b) FROM s.t GROUP BY a")));
        assert!(needs_stages(&q("SELECT a FROM s.t")), "no aggregate");
        assert!(needs_stages(&q("SELECT a, SUM(b) FROM s.t GROUP BY a HAVING SUM(b) > 1")));
        assert!(needs_stages(&q("WITH r AS (SELECT a, SUM(b) AS v FROM s.t GROUP BY a) SELECT MAX(v) FROM r")));
        assert!(needs_stages(&q("SELECT SUM(b) FROM s.t WHERE b > (SELECT AVG(b) FROM s.t)")));
        assert!(needs_stages(&q("SELECT a, COUNT(DISTINCT b) FROM s.t GROUP BY a")));
        assert!(!needs_stages(&q("SELECT COUNT(*) FROM s.t WHERE a IN (SELECT k FROM s.u GROUP BY k HAVING SUM(v) > 3)")), "the engine's semi-join");
        assert!(needs_stages(&q("SELECT c, COUNT(*) FROM (SELECT a, COUNT(b) AS c FROM s.t GROUP BY a) AS x GROUP BY c")));
        assert!(!needs_stages(&q("SELECT y, SUM(v) FROM (SELECT EXTRACT(YEAR FROM d) AS y, v FROM s.t) AS x GROUP BY y")), "the engine flattens it");
    }

    #[test]
    fn repeat_expands_rows_by_count() {
        let b = ArrowBatch {
            rows: 3,
            cols: vec![
                FinishedCol::I64 { validity: Some(vec![0b101]), data: vec![1, 0, 3] },
                FinishedCol::Utf8 { validity: None, offsets: vec![0, 1, 3, 6], data: b"abbccc".to_vec() },
            ],
        };
        let r = mem::repeat(&b, &[2, 1, 0], None);
        assert_eq!(r.rows, 3);
        match &r.cols[0] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(data, &vec![1, 1, 0]);
                assert_eq!(validity.as_ref().map(|v| v[0] & 0b111), Some(0b011));
            }
            _ => panic!(),
        }
        match &r.cols[1] {
            FinishedCol::Utf8 { offsets, data, .. } => {
                assert_eq!(offsets, &vec![0, 1, 2, 4]);
                assert_eq!(data, b"aabb");
            }
            _ => panic!(),
        }
        assert_eq!(mem::repeat(&b, &[2, 1, 0], Some(1)).rows, 1);
    }
}
