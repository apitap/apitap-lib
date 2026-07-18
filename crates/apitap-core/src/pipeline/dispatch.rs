//! Route dispatch: the (source scheme, destination scheme) match that binds one
//! [`crate::source`] and one [`crate::sink`] onto the generic pipeline, plus the
//! budget derivation (per-route CPU heuristics × the cgroup memory model).
//!
//! ADDING A ROUTE = implement the trait in `source/` or `sink/`, then add ONE
//! arm to [`single`] and (if multi-table applies) [`multi`]. Nothing else moves.

use super::Profile;
use crate::error::{Error, Result};
use crate::sink::clickhouse::{ChConn, ChDdl, ChSink};
use crate::sink::mysql::MySqlSink;
use crate::sink::postgres::PgSink;
use crate::source::gsheets::GsheetsSource;
use crate::source::mysql::MySqlSource;
use crate::source::postgres::PgSource;
use crate::{MultiReport, TransferOptions, TransferReport};


// Per-route pipe heuristics (all measured — see benchmarks/README.md):
// byte-relay pipes into a Postgres COPY are writer-bound (more pipes contend on the
// destination), ClickHouse pipes are mostly I/O with a light transcode (~0.1 core
// each), MySQL feeders pay real decode CPU.
fn pg_pg_parallel(cores: usize) -> usize {
    cores.clamp(1, 8)
}
fn to_ch_parallel(cores: usize) -> usize {
    (cores * 8).clamp(2, 32)
}
fn my_pg_parallel(cores: usize) -> usize {
    (cores * 4).clamp(2, 16)
}
// MySQL→MySQL: source decode is real CPU (like MySQL→PG) and the dest LOAD DATA
// is server-bound; scale with cores, capped for the small tiers.
fn my_my_parallel(cores: usize) -> usize {
    (cores * 4).clamp(2, 16)
}
// BigQuery pipes: PG read + gzip (~fast level, light CPU) + upload; each is one
// parallel load job. Upload bandwidth saturates quickly — more pipes past 8 just
// shard the same uplink.
fn to_bq_parallel(cores: usize) -> usize {
    (cores * 2).clamp(2, 8)
}

// Named per-route profiles — an arm references ONE of these instead of
// restating the literal; a new route adds a line here.
const PG_PG: Profile = Profile { auto_parallel: pg_pg_parallel, span_mult: 6 };
const TO_CH: Profile = Profile { auto_parallel: to_ch_parallel, span_mult: 6 };
const MY_PG: Profile = Profile { auto_parallel: my_pg_parallel, span_mult: 6 };
const MY_MY: Profile = Profile { auto_parallel: my_my_parallel, span_mult: 6 };
const TO_BQ: Profile = Profile { auto_parallel: to_bq_parallel, span_mult: 6 };

// Google Sheets reads are one API stream; parallelism buys nothing.
fn gsheets_parallel(_cores: usize) -> usize {
    1
}
const GSHEETS: Profile = Profile { auto_parallel: gsheets_parallel, span_mult: 1 };

/// The single-table pipe resolver: exactly `parallel`, clamped to the span count.
fn exact(parallel: usize) -> impl FnOnce(usize) -> usize {
    move |n| parallel.min(n).max(1)
}

pub(crate) async fn single(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
) -> Result<TransferReport> {

    let started = std::time::Instant::now();
    let src_scheme = src_url.split("://").next().unwrap_or("");
    let dst_scheme = dst_url.split("://").next().unwrap_or("");
    let dest_table = opts.dest_table.as_deref().unwrap_or(table);
    let source_id = super::source_identity(src_url, table);
    let ch_ddl = ChDdl::from_opts(
        opts,
        matches!(dst_scheme, "clickhouse" | "clickhouse+https"),
    )?;

    match (src_scheme, dst_scheme) {
        ("postgres" | "postgresql", "postgres" | "postgresql") => {
            let profile = PG_PG;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = PgSink::connect(dst_url, dest_table, parallel + 1, true).await?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("postgres" | "postgresql", "clickhouse" | "clickhouse+https") => {
            let profile = TO_CH;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = ChSink::connect(dst_url, dest_table, ch_ddl.clone())?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("postgres" | "postgresql", "bigquery") => {
            let profile = TO_BQ;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, parallel + 1).await?;
            let sink = crate::sink::bigquery::BqSink::connect(dst_url, dest_table, parallel).await?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("mysql", "clickhouse" | "clickhouse+https") => {
            let profile = TO_CH;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            let sink = ChSink::connect(dst_url, dest_table, ch_ddl.clone())?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("mysql", "mysql") => {
            let profile = MY_MY;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            let sink = crate::sink::mysql::MySqlSink::connect(dst_url, dest_table).await?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("mysql", "postgres" | "postgresql") => {
            let profile = MY_PG;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, parallel + 1).await?;
            // Serial sends: this feeder's per-row encode is CPU-heavy and overlapping
            // it with the send was measured slower (see PgSink::overlap_send).
            let sink = PgSink::connect(dst_url, dest_table, parallel + 1, false).await?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
            )
            .await
        }
        ("gsheets", "postgres" | "postgresql") => {
            let profile = GSHEETS;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = GsheetsSource::connect(src_url).await?;
            let sink = PgSink::connect(dst_url, dest_table, parallel + 1, false).await?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started,
                &source_id,
            )
            .await
        }
        ("gsheets", "clickhouse" | "clickhouse+https") => {
            let profile = GSHEETS;
            let (chunk, parallel) = super::knobs(opts, &profile)?;
            let src = GsheetsSource::connect(src_url).await?;
            let sink = ChSink::connect(dst_url, dest_table, ch_ddl.clone())?;
            super::run(
                &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started,
                &source_id,
            )
            .await
        }
        (s, d) => Err(Error::InvalidInput(format!(
            "unsupported route {s}:// → {d}:// (supported: postgres→postgres, \
             postgres→clickhouse, postgres→bigquery, mysql→clickhouse, \
             mysql→postgres, mysql→mysql, gsheets→postgres, gsheets→clickhouse)"
        ))),
    }
}

/// Which tables a multi-table run moves.
pub(crate) enum TableSel<'a> {
    /// An explicit list, every name verified against the source catalog up front.
    List(&'a [String]),
    /// Every base table (+ Postgres matviews) in one schema. `None` = the MySQL
    /// URL's database; Postgres has no such default and requires the name.
    Schema(Option<&'a str>),
}


async fn jobs_for<S: crate::source::Source>(
    src: &S,
    sel: &TableSel<'_>,
    bare_dest: bool,
) -> Result<Vec<super::TableJob>> {
    let cat = match sel {
        TableSel::List(ts) => {
            if ts.is_empty() {
                return Err(Error::InvalidInput("tables list is empty".into()));
            }
            src.catalog(None, Some(ts)).await?
        }
        TableSel::Schema(s) => src.catalog(*s, None).await?,
    };
    if cat.is_empty() {
        return Err(Error::InvalidInput(
            "no tables to transfer (empty schema?)".into(),
        ));
    }
    // Multi-table keeps source names, and ClickHouse/MySQL/BigQuery sinks drop the
    // schema qualifier — `a.events` and `b.events` would silently overwrite each
    // other there. Postgres keeps qualifiers, but `events` and `public.events` are
    // ONE relation under the default search_path (the same normalization
    // `source_identity` applies), so unqualified names normalize before the check.
    // Refuse up front instead of racing two loads into one staging table.
    let mut seen = std::collections::HashSet::new();
    for (name, _) in &cat {
        let key = if bare_dest {
            name.rsplit_once('.')
                .map_or(name.as_str(), |(_, t)| t)
                .to_string()
        } else {
            // Same rule as source_identity — ONE copy, in the dialect.
            crate::dialect::postgres::canonical_table(name)
        };
        if !seen.insert(key.clone()) {
            return Err(Error::InvalidInput(format!(
                "two source tables land on the same destination table '{key}' — \
                 transfer them in separate calls"
            )));
        }
    }
    // A sibling named like another table's staging artifact would get DROPped by
    // that table's prepare mid-run — reserve the `__apitap_*` namespace up front.
    for key in &seen {
        for suffix in ["__apitap_staging", "__apitap_old"] {
            if let Some(base) = key.strip_suffix(suffix) {
                if seen.contains(base) {
                    return Err(Error::InvalidInput(format!(
                        "table '{key}' collides with the staging artifacts of \
                         '{base}' — transfer them in separate calls"
                    )));
                }
            }
        }
    }
    Ok(cat
        .into_iter()
        .map(|(table, est_rows)| super::TableJob { table, est_rows })
        .collect())
}

pub(crate) async fn multi(
    src_url: &str,
    dst_url: &str,
    sel: TableSel<'_>,
    opts: &TransferOptions,
) -> Result<MultiReport> {

    let started = std::time::Instant::now();
    let src_scheme = src_url.split("://").next().unwrap_or("");
    let dst_scheme = dst_url.split("://").next().unwrap_or("");
    if opts.dest_table.is_some() {
        return Err(Error::InvalidInput(
            "dest_table applies to single-table transfers — multi-table runs keep \
             the source names"
                .into(),
        ));
    }
    let ch_ddl = ChDdl::from_opts(
        opts,
        matches!(dst_scheme, "clickhouse" | "clickhouse+https"),
    )?;

    // Each arm: budget once, source pool once, sink resources once — then every
    // table runs the unchanged single-table lifecycle inside the shared budget.
    // Pools get a little headroom over the budget: pipes take most connections,
    // the +8 covers the short-lived control work (probe/DDL/finalize) of tables
    // waiting in flight.
    let (budget, results) = match (src_scheme, dst_scheme) {
        ("postgres" | "postgresql", "postgres" | "postgresql") => {
            let profile = PG_PG;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, false).await?;
            let pool = PgSink::shared_pool(dst_url, budget + 8).await?;
            let mk = |t: String| {
                let pool = pool.clone();
                async move { Ok(PgSink::bind(pool, &t, true)) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("postgres" | "postgresql", "clickhouse" | "clickhouse+https") => {
            let profile = TO_CH;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Parse once: the reqwest client inside ChConn is shared by every table.
            let ch = crate::sink::clickhouse::ChConn::parse(dst_url)?;
            let mk = |t: String| {
                let (ch, ddl) = (ch.clone(), ch_ddl.clone());
                async move { ChSink::bind(ch, &t, ddl) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("postgres" | "postgresql", "bigquery") => {
            let profile = TO_BQ;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = PgSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Authenticate once (JWT sign + OAuth round-trip live in parse) — a
            // 200-table schema must not hit the token endpoint 200 times.
            let bq = crate::sink::bigquery::BqConn::parse(dst_url).await?;
            let mk = |t: String| {
                let bq = bq.clone();
                async move {
                    // BigQuery has no schema qualifiers — land `public.events` as `events`.
                    let bare = t.rsplit_once('.').map_or(t.as_str(), |(_, b)| b);
                    crate::sink::bigquery::BqSink::bind(bq, bare, budget)
                }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "clickhouse" | "clickhouse+https") => {
            let profile = TO_CH;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            // Parse once: the reqwest client inside ChConn is shared by every table.
            let ch = crate::sink::clickhouse::ChConn::parse(dst_url)?;
            let mk = |t: String| {
                let (ch, ddl) = (ch.clone(), ch_ddl.clone());
                async move { ChSink::bind(ch, &t, ddl) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "mysql") => {
            let profile = MY_MY;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            let shared = MySqlSink::shared_pool(dst_url)?;
            let mk = |t: String| {
                let shared = shared.clone();
                async move { Ok(MySqlSink::bind(shared, &t)) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("mysql", "postgres" | "postgresql") => {
            let profile = MY_PG;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = MySqlSource::connect(src_url, budget + 8).await?;
            let jobs = jobs_for(&src, &sel, false).await?;
            let pool = PgSink::shared_pool(dst_url, budget + 8).await?;
            // overlap_send=false: same reasoning as the single-table arm.
            let mk = |t: String| {
                let pool = pool.clone();
                async move { Ok(PgSink::bind(pool, &t, false)) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("gsheets", "postgres" | "postgresql") => {
            let profile = GSHEETS;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = GsheetsSource::connect(src_url).await?;
            let jobs = jobs_for(&src, &sel, false).await?;
            let pool = PgSink::shared_pool(dst_url, budget + 8).await?;
            let mk = |t: String| {
                let pool = pool.clone();
                async move { Ok(PgSink::bind(pool, &t, false)) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        ("gsheets", "clickhouse" | "clickhouse+https") => {
            let profile = GSHEETS;
            let (chunk, budget) = super::knobs(opts, &profile)?;
            let src = GsheetsSource::connect(src_url).await?;
            let jobs = jobs_for(&src, &sel, true).await?;
            let ch = ChConn::parse(dst_url)?;
            let mk = |t: String| {
                let (ch, ddl) = (ch.clone(), ch_ddl.clone());
                async move { ChSink::bind(ch, &t, ddl) }
            };
            let r =
                super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
            (budget, r)
        }
        (s, d) => {
            return Err(Error::InvalidInput(format!(
                "unsupported route {s}:// → {d}:// (supported: postgres→postgres, \
                 postgres→clickhouse, postgres→bigquery, mysql→clickhouse, \
                 mysql→postgres, mysql→mysql, gsheets→postgres, gsheets→clickhouse)"
            )))
        }
    };

    Ok(MultiReport {
        rows: results
            .iter()
            .filter(|r| r.error.is_none())
            .map(|r| r.rows)
            .sum(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        budget,
        tables: results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Delta, Lane, TablePlan, WireFormat};

    /// A Source that only answers catalog() — jobs_for touches nothing else.
    struct FakeCatalog(Vec<(&'static str, i64)>);
    impl crate::source::Source for FakeCatalog {
        fn cursor_quoted(&self, _udt: &str) -> Result<bool> {
            unimplemented!()
        }
        async fn probe(&self, _t: &str) -> Result<TablePlan> {
            unimplemented!()
        }
        async fn catalog(
            &self,
            _schema: Option<&str>,
            _tables: Option<&[String]>,
        ) -> Result<Vec<(String, i64)>> {
            Ok(self.0.iter().map(|(n, e)| (n.to_string(), *e)).collect())
        }
        fn can_produce(&self, _p: &TablePlan, _f: WireFormat) -> bool {
            unimplemented!()
        }
        fn plan_lane(&self, _p: &TablePlan, _f: WireFormat) -> Lane {
            unimplemented!()
        }
        async fn span_stmts(
            &self,
            _t: &str,
            _p: &TablePlan,
            _l: &Lane,
            _w: usize,
            _d: Option<&Delta>,
        ) -> Result<Vec<String>> {
            unimplemented!()
        }
        async fn run_workers<L: crate::sink::Loader>(
            &self,
            _p: &TablePlan,
            _l: &Lane,
            _s: Vec<String>,
            _ld: Vec<L>,
            _c: usize,
        ) -> Result<u64> {
            unimplemented!()
        }
    }

    fn names(src: Vec<(&'static str, i64)>) -> FakeCatalog {
        FakeCatalog(src)
    }

    #[tokio::test]
    async fn jobs_for_passes_names_and_estimates_through() {
        let src = names(vec![("public.big", 1_000_000), ("public.small", -1)]);
        let jobs = jobs_for(&src, &TableSel::Schema(Some("public")), false)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].table, "public.big");
        assert_eq!(jobs[0].est_rows, 1_000_000);
        assert_eq!(jobs[1].est_rows, -1);
    }

    #[tokio::test]
    async fn jobs_for_rejects_empty_list_before_touching_the_source() {
        let src = names(vec![]);
        let err = jobs_for(&src, &TableSel::List(&[]), false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[tokio::test]
    async fn bare_dest_collision_is_caught_up_front() {
        // ClickHouse/MySQL/BigQuery sinks drop the qualifier — a.events and
        // b.events would silently overwrite each other.
        let src = names(vec![("a.events", 10), ("b.events", 10)]);
        let err = jobs_for(&src, &TableSel::Schema(Some("x")), true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("events"));
    }

    #[tokio::test]
    async fn pg_alias_of_one_relation_is_caught() {
        // 'events' and 'public.events' are ONE Postgres relation under the
        // default search_path — racing two loads into one staging table.
        let src = names(vec![("events", 10), ("public.events", 10)]);
        let err = jobs_for(&src, &TableSel::Schema(Some("public")), false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("public.events"));
        // Distinct schemas stay distinct on PG dests (qualifiers are kept).
        let src = names(vec![("a.events", 10), ("b.events", 10)]);
        assert!(jobs_for(&src, &TableSel::Schema(Some("x")), false)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn apitap_staging_namespace_is_reserved() {
        // A sibling named like another table's staging artifact would get
        // DROPped by that table's prepare mid-run.
        let src = names(vec![("foo", 10), ("foo__apitap_staging", 10)]);
        let err = jobs_for(&src, &TableSel::Schema(Some("public")), true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("staging artifacts"));
    }
}
