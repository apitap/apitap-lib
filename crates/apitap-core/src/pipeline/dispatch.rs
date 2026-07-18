//! Route dispatch: the (source scheme, destination scheme) table that binds one
//! [`crate::source`] and one [`crate::sink`] onto the generic pipeline, plus the
//! budget derivation (per-route CPU heuristics × the cgroup memory model).
//!
//! ADDING A ROUTE = one line in [`routes!`]. ADDING AN ENGINE = implement the
//! trait in `source/` or `sink/`, add a ~5-line scheme adapter here, then add
//! its route lines. Everything stays monomorphized: each table line instantiates
//! `one::<S, D>` / `many::<S, D>` at compile time — no dynamic dispatch anywhere
//! on the hot path, and per-route perf character lives in the [`Profile`] column.

use super::{norm, Profile};
use crate::error::{Error, Result};
use crate::sink::bigquery::{BqConn, BqSink};
use crate::sink::clickhouse::{ChConn, ChDdl, ChSink};
use crate::sink::mysql::MySqlSink;
use crate::sink::postgres::PgSink;
use crate::source::github::GithubSource;
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
// Google Sheets reads are one API stream; parallelism buys nothing.
fn gsheets_parallel(_cores: usize) -> usize {
    1
}
// GitHub: one stream per file (RFC-4180 can't byte-range split), so parallelism
// is ACROSS files — download + parse + encode is light-moderate CPU per pipe.
fn github_parallel(cores: usize) -> usize {
    (cores * 2).clamp(1, 16)
}

// Named per-route profiles — a route line references ONE of these instead of
// restating the literal; a new route adds a line here.
const PG_PG: Profile = Profile { auto_parallel: pg_pg_parallel, span_mult: 6, table_pipe_cap: usize::MAX };
const TO_CH: Profile = Profile { auto_parallel: to_ch_parallel, span_mult: 6, table_pipe_cap: usize::MAX };
const MY_PG: Profile = Profile { auto_parallel: my_pg_parallel, span_mult: 6, table_pipe_cap: usize::MAX };
const MY_MY: Profile = Profile { auto_parallel: my_my_parallel, span_mult: 6, table_pipe_cap: usize::MAX };
const TO_BQ: Profile = Profile { auto_parallel: to_bq_parallel, span_mult: 6, table_pipe_cap: usize::MAX };
const GSHEETS: Profile = Profile { auto_parallel: gsheets_parallel, span_mult: 1, table_pipe_cap: 1 };
const GITHUB: Profile = Profile { auto_parallel: github_parallel, span_mult: 1, table_pipe_cap: 1 };

/// The single-table pipe resolver: exactly `parallel`, clamped to the span count.
fn exact(parallel: usize) -> impl FnOnce(usize) -> usize {
    move |n| parallel.min(n).max(1)
}

/// Per-route sink configuration, one struct for every sink kind — each impl
/// reads the fields it cares about and ignores the rest.
#[derive(Clone)]
struct SinkCfg {
    /// Postgres only: overlap the encode with the COPY send. True for the raw
    /// pg→pg relay; false where the feeder's per-row encode is CPU-heavy and
    /// overlapping was measured slower (see `PgSink::overlap_send`).
    pg_overlap: bool,
    /// ClickHouse only: engine/order_by/on_cluster DDL options.
    ch_ddl: ChDdl,
    /// BigQuery only: the run's pipe budget (staging-table load fan-out).
    budget: usize,
}

/// A source engine, keyed by URL scheme: how to open it with a given pool size.
trait SrcScheme {
    type Src: crate::source::Source;
    async fn connect(url: &str, pool: usize) -> Result<Self::Src>;
}

struct PgFrom;
impl SrcScheme for PgFrom {
    type Src = PgSource;
    async fn connect(url: &str, pool: usize) -> Result<PgSource> {
        PgSource::connect(url, pool).await
    }
}
struct MyFrom;
impl SrcScheme for MyFrom {
    type Src = MySqlSource;
    async fn connect(url: &str, pool: usize) -> Result<MySqlSource> {
        MySqlSource::connect(url, pool).await
    }
}
struct GsFrom;
impl SrcScheme for GsFrom {
    type Src = GsheetsSource;
    async fn connect(url: &str, _pool: usize) -> Result<GsheetsSource> {
        GsheetsSource::connect(url).await
    }
}
struct GhFrom;
impl SrcScheme for GhFrom {
    type Src = GithubSource;
    async fn connect(url: &str, _pool: usize) -> Result<GithubSource> {
        GithubSource::connect(url).await
    }
}

/// A destination engine, keyed by URL scheme. `connect` serves single-table
/// runs; `shared` + `bind` serve multi-table runs (auth/pool once, then one
/// cheap bind per table — a 200-table schema must not open 200 pools).
trait DstScheme {
    type Sink: crate::sink::Sink;
    /// The once-per-run resource `bind` clones from (pool / HTTP client / token).
    type Shared: Clone;
    /// True where the sink drops schema qualifiers (`a.events` → `events`), so
    /// multi-table pre-flight can refuse silent overwrites up front.
    const BARE_DEST: bool;
    async fn connect(url: &str, dest_table: &str, parallel: usize, cfg: &SinkCfg)
        -> Result<Self::Sink>;
    async fn shared(url: &str, budget: usize, cfg: &SinkCfg) -> Result<Self::Shared>;
    fn bind(shared: Self::Shared, table: &str, cfg: &SinkCfg) -> Result<Self::Sink>;
}

struct PgTo;
impl DstScheme for PgTo {
    type Sink = PgSink;
    type Shared = sqlx::PgPool;
    const BARE_DEST: bool = false;
    async fn connect(url: &str, dest_table: &str, parallel: usize, cfg: &SinkCfg) -> Result<PgSink> {
        PgSink::connect(url, dest_table, parallel + 1, cfg.pg_overlap).await
    }
    async fn shared(url: &str, budget: usize, _cfg: &SinkCfg) -> Result<sqlx::PgPool> {
        // +8 headroom over the budget: pipes take most connections, the extra
        // covers the short-lived control work (probe/DDL/finalize) of tables
        // waiting in flight.
        PgSink::shared_pool(url, budget + 8).await
    }
    fn bind(shared: sqlx::PgPool, table: &str, cfg: &SinkCfg) -> Result<PgSink> {
        Ok(PgSink::bind(shared, table, cfg.pg_overlap))
    }
}
struct ChTo;
impl DstScheme for ChTo {
    type Sink = ChSink;
    type Shared = ChConn;
    const BARE_DEST: bool = true;
    async fn connect(url: &str, dest_table: &str, _parallel: usize, cfg: &SinkCfg) -> Result<ChSink> {
        ChSink::connect(url, dest_table, cfg.ch_ddl.clone())
    }
    async fn shared(url: &str, _budget: usize, _cfg: &SinkCfg) -> Result<ChConn> {
        // Parse once: the reqwest client inside ChConn is shared by every table.
        ChConn::parse(url)
    }
    fn bind(shared: ChConn, table: &str, cfg: &SinkCfg) -> Result<ChSink> {
        ChSink::bind(shared, table, cfg.ch_ddl.clone())
    }
}
struct MyTo;
impl DstScheme for MyTo {
    type Sink = MySqlSink;
    type Shared = crate::sink::mysql::MySqlShared;
    const BARE_DEST: bool = true;
    async fn connect(url: &str, dest_table: &str, _parallel: usize, _cfg: &SinkCfg) -> Result<MySqlSink> {
        MySqlSink::connect(url, dest_table).await
    }
    async fn shared(url: &str, _budget: usize, _cfg: &SinkCfg) -> Result<Self::Shared> {
        MySqlSink::shared_pool(url)
    }
    fn bind(shared: Self::Shared, table: &str, _cfg: &SinkCfg) -> Result<MySqlSink> {
        Ok(MySqlSink::bind(shared, table))
    }
}
struct BqTo;
impl DstScheme for BqTo {
    type Sink = BqSink;
    type Shared = BqConn;
    const BARE_DEST: bool = true;
    async fn connect(url: &str, dest_table: &str, parallel: usize, _cfg: &SinkCfg) -> Result<BqSink> {
        BqSink::connect(url, dest_table, parallel).await
    }
    async fn shared(url: &str, _budget: usize, _cfg: &SinkCfg) -> Result<BqConn> {
        // Authenticate once (JWT sign + OAuth round-trip live in parse) — a
        // 200-table schema must not hit the token endpoint 200 times.
        BqConn::parse(url).await
    }
    fn bind(shared: BqConn, table: &str, cfg: &SinkCfg) -> Result<BqSink> {
        // BigQuery has no schema qualifiers — land `public.events` as `events`.
        let bare = table.rsplit_once('.').map_or(table, |(_, b)| b);
        BqSink::bind(shared, bare, cfg.budget)
    }
}

/// One single-table transfer over a (source, destination) pair: resolve knobs,
/// open both ends, hand off to the shared pipeline.
#[allow(clippy::too_many_arguments)]
async fn one<S: SrcScheme, D: DstScheme>(
    profile: Profile,
    pg_overlap: bool,
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
    ch_ddl: ChDdl,
    started: std::time::Instant,
) -> Result<TransferReport> {
    let dest_table = opts.dest_table.as_deref().unwrap_or(table);
    let source_id = super::source_identity(src_url, table);
    let (chunk, parallel) = super::knobs(opts, &profile)?;
    // One table can never use more pipes than the profile's per-table cap —
    // clamping HERE also sizes the connection pools honestly for single-stream
    // sources (a 1-pipe github read must not open a 33-connection pool).
    let parallel = parallel.min(profile.table_pipe_cap).max(1);
    let cfg = SinkCfg { pg_overlap, ch_ddl, budget: parallel };
    let src = S::connect(src_url, parallel + 1).await?;
    let sink = D::connect(dst_url, dest_table, parallel, &cfg).await?;
    super::run(
        &src, sink, table, opts, &profile, chunk, parallel, exact(parallel), started, &source_id,
    )
    .await
}

/// One multi-table run: budget once, source pool once, sink resources once —
/// then every table runs the unchanged single-table lifecycle inside the
/// shared budget.
async fn many<S: SrcScheme, D: DstScheme>(
    profile: Profile,
    pg_overlap: bool,
    src_url: &str,
    dst_url: &str,
    sel: TableSel<'_>,
    opts: &TransferOptions,
    ch_ddl: ChDdl,
) -> Result<(usize, Vec<crate::TableResult>)> {
    let (chunk, budget) = super::knobs(opts, &profile)?;
    let cfg = SinkCfg { pg_overlap, ch_ddl, budget };
    let src = S::connect(src_url, budget + 8).await?;
    let jobs = jobs_for(&src, &sel, D::BARE_DEST).await?;
    let shared = D::shared(dst_url, budget, &cfg).await?;
    let mk = |t: String| {
        let (shared, cfg) = (shared.clone(), cfg.clone());
        async move { D::bind(shared, &t, &cfg) }
    };
    let r = super::run_many(&src, jobs, opts, &profile, chunk, budget, src_url, mk).await?;
    Ok((budget, r))
}

/// THE route table. One line per supported pair: source adapter, destination
/// adapter, profile, and the pg_overlap flag (dead on non-Postgres dests\n/// by construction — only PgTo reads it). The macro expands it into the
/// single-table match, the multi-table match, and the "supported:" list in the
/// unsupported-route error — the three can never drift apart.
macro_rules! routes {
    ($( $sname:literal -> $dname:literal : $S:ty => $D:ty, $prof:expr, pg_overlap = $ov:expr );+ $(;)?) => {
        #[allow(clippy::too_many_arguments)]
        async fn route_single(
            s: &str, d: &str,
            src_url: &str, dst_url: &str, table: &str,
            opts: &TransferOptions, ch_ddl: ChDdl, started: std::time::Instant,
        ) -> Result<TransferReport> {
            match (s, d) {
                $( ($sname, $dname) =>
                    one::<$S, $D>($prof, $ov, src_url, dst_url, table, opts, ch_ddl, started).await, )+
                (s, d) => Err(unsupported(s, d)),
            }
        }
        async fn route_multi(
            s: &str, d: &str,
            src_url: &str, dst_url: &str, sel: TableSel<'_>,
            opts: &TransferOptions, ch_ddl: ChDdl,
        ) -> Result<(usize, Vec<crate::TableResult>)> {
            match (s, d) {
                $( ($sname, $dname) =>
                    many::<$S, $D>($prof, $ov, src_url, dst_url, sel, opts, ch_ddl).await, )+
                (s, d) => Err(unsupported(s, d)),
            }
        }
        /// Every table pair, for guard tests: entries must be norm() fixed
        /// points (an alias spelling would be silently unreachable) and unique
        /// (a duplicate would silently lose to the first line).
        #[cfg(test)]
        const ROUTES: &[(&str, &str)] = &[ $( ($sname, $dname) ),+ ];
        fn unsupported(s: &str, d: &str) -> Error {
            Error::InvalidInput(format!(
                "unsupported route {s}:// → {d}:// (supported: {})",
                [ $( concat!($sname, "→", $dname) ),+ ].join(", "),
            ))
        }
    };
}

routes! {
    "postgres" -> "postgres"   : PgFrom => PgTo, PG_PG,   pg_overlap = true;
    "postgres" -> "clickhouse" : PgFrom => ChTo, TO_CH,   pg_overlap = false;
    "postgres" -> "bigquery"   : PgFrom => BqTo, TO_BQ,   pg_overlap = false;
    "mysql"    -> "clickhouse" : MyFrom => ChTo, TO_CH,   pg_overlap = false;
    "mysql"    -> "postgres"   : MyFrom => PgTo, MY_PG,   pg_overlap = false;
    "mysql"    -> "mysql"      : MyFrom => MyTo, MY_MY,   pg_overlap = false;
    "gsheets"  -> "postgres"   : GsFrom => PgTo, GSHEETS, pg_overlap = false;
    "gsheets"  -> "clickhouse" : GsFrom => ChTo, GSHEETS, pg_overlap = false;
    "gsheets"  -> "mysql"      : GsFrom => MyTo, GSHEETS, pg_overlap = false;
    "github"   -> "postgres"   : GhFrom => PgTo, GITHUB,  pg_overlap = false;
    "github"   -> "clickhouse" : GhFrom => ChTo, GITHUB,  pg_overlap = false;
    "github"   -> "mysql"      : GhFrom => MyTo, GITHUB,  pg_overlap = false;
}

pub(crate) async fn single(
    src_url: &str,
    dst_url: &str,
    table: &str,
    opts: &TransferOptions,
) -> Result<TransferReport> {
    let started = std::time::Instant::now();
    let src_scheme = norm(src_url.split("://").next().unwrap_or(""));
    let dst_scheme = norm(dst_url.split("://").next().unwrap_or(""));
    let ch_ddl = ChDdl::from_opts(opts, dst_scheme == "clickhouse")?;
    route_single(
        src_scheme, dst_scheme, src_url, dst_url, table, opts, ch_ddl, started,
    )
    .await
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
    let src_scheme = norm(src_url.split("://").next().unwrap_or(""));
    let dst_scheme = norm(dst_url.split("://").next().unwrap_or(""));
    if opts.dest_table.is_some() {
        return Err(Error::InvalidInput(
            "dest_table applies to single-table transfers — multi-table runs keep \
             the source names"
                .into(),
        ));
    }
    let ch_ddl = ChDdl::from_opts(opts, dst_scheme == "clickhouse")?;
    let (budget, results) = route_multi(
        src_scheme, dst_scheme, src_url, dst_url, sel, opts, ch_ddl,
    )
    .await?;

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

    #[test]
    fn scheme_aliases_normalize_and_unknowns_pass_through() {
        assert_eq!(norm("postgresql"), "postgres");
        assert_eq!(norm("clickhouse+https"), "clickhouse");
        assert_eq!(norm("gsheets"), "gsheets");
        assert_eq!(norm("sqlite"), "sqlite");
    }

    #[test]
    fn route_table_entries_are_norm_fixed_points_and_unique() {
        // An alias spelling ("postgresql") would compile fine yet be silently
        // unreachable (norm() runs before the match); a duplicate pair would
        // silently lose to the first line. Both are caught here.
        let mut seen = std::collections::HashSet::new();
        for (s, d) in ROUTES {
            assert_eq!(norm(s), *s, "route source '{s}' is not the canonical spelling");
            assert_eq!(norm(d), *d, "route dest '{d}' is not the canonical spelling");
            assert!(seen.insert((s, d)), "duplicate route line {s}→{d}");
        }
    }

    #[test]
    fn unsupported_error_lists_the_route_table() {
        let msg = unsupported("sqlite", "postgres").to_string();
        assert!(msg.contains("sqlite:// → postgres://"));
        // Generated from the SAME table as the match — spot-check both ends.
        assert!(msg.contains("postgres→postgres"));
        assert!(msg.contains("gsheets→clickhouse"));
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
