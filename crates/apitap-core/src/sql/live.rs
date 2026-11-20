//! `apitap.sql(..., dest=, mode="log_based")`: an incremental view kept by
//! each source's change stream (idea 7: C2 one table, C3 the join of a
//! Postgres and/or MySQL pair).
//!
//! First run: every side is read once at a point its change stream starts
//! from exactly, and every row is applied as an insert:
//!
//! - Postgres: the view's own slot is created with `EXPORT_SNAPSHOT` and the
//!   table is read pinned to that snapshot;
//! - MySQL: as mysqldump `--single-transaction --source-data` does it,
//!   `FLUSH TABLES WITH READ LOCK`, `START TRANSACTION WITH CONSISTENT
//!   SNAPSHOT`, the binlog position, `UNLOCK TABLES`, and the table read in
//!   that one transaction. (The replica lane's overlap-then-idempotent-upsert
//!   bootstrap cannot serve a view: an overlap event would count twice.)
//!
//! Later runs drain each side from its own position to its log's end, keep
//! each update's old image, and move the answer by the signed images. The
//! sides need no common order: every move of the state is a signed sum.
//!
//! Order, for crash safety: the state file (with every side's position) is
//! written first, then the answer (a replace of the whole small table), then
//! the Postgres slot is confirmed. A run after a crash drains from the state's
//! positions, which are never behind what the state holds. A state file that
//! is missing, torn, or built for another query or schema is never trusted:
//! the view is rebuilt by a full first run.

use super::view::View;
use super::*;
use crate::logbased::changelog::{ChangeOp, Changes};
use crate::logbased::drain::{drain, DrainSession};
use crate::logbased::window::{Bodies, DrainOutcome};
use crate::wire::pgoutput::{lsn_from_string, lsn_to_string, Cellv, Tuple};
use crate::wire::walsender::Walsender;
use sqlx::Row;
use std::path::{Path, PathBuf};

fn state_dir() -> Result<PathBuf> {
    let d = match std::env::var_os("APITAP_STATE_DIR") {
        Some(d) => PathBuf::from(d),
        None => std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".apitap").join("views"))
            .ok_or_else(|| bad("mode=\"log_based\" keeps view state on local disk: set APITAP_STATE_DIR"))?,
    };
    std::fs::create_dir_all(&d).map_err(|e| Error::Transfer(format!("view state dir {}: {e}", d.display())))?;
    Ok(d)
}

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based view: {e}"))
}

/// Plan `query` as a view, probing each table's schema as the planner asks.
async fn plan_view(query: &str, sources: &HashMap<String, String>) -> Result<View> {
    let mut cache: HashMap<(String, String), Vec<ArrowField>> = HashMap::new();
    loop {
        let mut missing = None;
        let r = View::plan(query, sources, &mut |u, t| match cache.get(&(u.to_string(), t.to_string())) {
            Some(s) => Ok(s.clone()),
            None => {
                missing = Some((u.to_string(), t.to_string()));
                Err(bad("schema probe"))
            }
        });
        match (r, missing) {
            (Ok(v), _) => return Ok(v),
            (Err(_), Some((u, t))) => {
                let s = super::sql_schema(&u, &t, query).await?;
                cache.insert((u, t), s);
            }
            (Err(e), None) => return Err(e),
        }
    }
}

/// One change image's cells in the view's column order. An unchanged-TOAST
/// cell takes the old image's value (REPLICA IDENTITY FULL ships it).
fn cells(t: &Tuple, at: &[usize], old: Option<&Tuple>) -> Result<Vec<Option<Vec<u8>>>> {
    at.iter()
        .map(|&i| {
            if i >= t.len() {
                return Err(bad("a change image is shorter than its table"));
            }
            Ok(match t.view(i) {
                Cellv::Null => None,
                Cellv::Text(b) => Some(b.to_vec()),
                Cellv::UnchangedToast => match old.map(|o| o.view(i)) {
                    Some(Cellv::Text(b)) => Some(b.to_vec()),
                    Some(Cellv::Null) => None,
                    _ => {
                        return Err(bad(
                            "an unchanged TOAST value with no old image to take it from (REPLICA IDENTITY FULL?)",
                        ))
                    }
                },
            })
        })
        .collect()
}

/// Images handed to the view at a time (round P p10, L14): a window's
/// images were all copied into one vector (a heap value per cell, ~200 B an
/// image at five columns) beside the window itself, and a drain of 332K
/// images died in the 256 MiB cage; a chunk is ~1–2 MB.
const IMAGE_CHUNK: usize = 8192;

/// The bytes one drained window may hold (round P p10, L14), priced with
/// the view's own state: the state's buckets take up to a fifth of the
/// memory limit and their pending moves a tenth (`view::bucket_budget`), so
/// the windows alive at once (`alive`: Postgres drains one, MySQL has one
/// filling, one queued and one applying) share an eighth, the images
/// applied from a window a chunk at a time ([`IMAGE_CHUNK`]). A single
/// transaction still drains whole (the CDC bound: memory follows the
/// largest transaction, L14c).
fn view_window_budget(alive: usize) -> usize {
    let total = crate::pipeline::mem_limit_bytes().map_or(64 << 20, |m| (m / 8) as usize);
    (total / alive.max(1)).max(1 << 20)
}

/// A window's signed images of one table, in change order, handed to `f`
/// [`IMAGE_CHUNK`] at a time; the count of them.
fn images(
    body: &Changes,
    cols: &[String],
    want: &[String],
    qualified: &str,
    path: &Path,
    f: &mut dyn FnMut(&[(i64, Vec<Option<Vec<u8>>>)]) -> Result<()>,
) -> Result<u64> {
    let at: Vec<usize> = want
        .iter()
        .map(|c| cols.iter().position(|x| x == c).ok_or_else(|| bad(format!("the column {c} is not in {qualified}'s change stream"))))
        .collect::<Result<_>>()?;
    let olds = body.olds.as_ref().ok_or_else(|| bad("a view window without old images"))?;
    let mut rows = Vec::with_capacity(IMAGE_CHUNK.min(2 * body.events.len()) + 1);
    let mut n = 0u64;
    for (i, ev) in body.events.iter().enumerate() {
        if rows.len() >= IMAGE_CHUNK {
            n += rows.len() as u64;
            f(&rows)?;
            rows.clear();
        }
        let row = ev.row.as_ref();
        let need = |what: &str| bad(format!("{what} without its row"));
        match ev.op {
            ChangeOp::Insert => rows.push((1, cells(row.ok_or_else(|| need("an insert"))?, &at, None)?)),
            ChangeOp::Delete => rows.push((-1, cells(row.ok_or_else(|| need("a delete"))?, &at, None)?)),
            ChangeOp::Update => {
                let new = row.ok_or_else(|| need("an update"))?;
                match (&olds[i], &ev.moved_from) {
                    (Some(o), _) => {
                        rows.push((-1, cells(o, &at, None)?));
                        rows.push((1, cells(new, &at, Some(o))?));
                    }
                    // A key move: its D, the event just before, carried the
                    // old image, and an unchanged-TOAST cell of the new one
                    // is that image's value (round P, p12: through p11 the
                    // view refused such a move as having no old image).
                    (None, Some(_)) => {
                        let d = i.checked_sub(1).and_then(|j| body.events.get(j)).filter(|e| e.op == ChangeOp::Delete).and_then(|e| e.row.as_ref());
                        rows.push((1, cells(new, &at, d)?))
                    }
                    (None, None) => {
                        return Err(Error::Transfer(format!(
                            "log_based view: an update of {qualified} arrived without its old image; delete {} and the view's replication slot (the next run names it) to rebuild",
                            path.display()
                        )))
                    }
                }
            }
            ChangeOp::Truncate => return Err(truncate_refused(qualified, path)),
        }
    }
    n += rows.len() as u64;
    if !rows.is_empty() {
        f(&rows)?;
    }
    Ok(n)
}

/// Move side `s` of the view by one drained window, if it holds `qualified`.
fn apply_window(v: &mut View, s: usize, out: &DrainOutcome, qualified: &str, path: &Path) -> Result<u64> {
    let Bodies::Changelog(m) = &out.bodies else {
        return Err(bad("a view drain returned replica bodies"));
    };
    let Some(w) = m.get(qualified) else { return Ok(0) };
    let want = v.columns(s);
    images(w.body(), w.layout().cols(), &want, qualified, path, &mut |rows| v.apply(s, rows))
}

/// A TRUNCATE in a view's change stream (Postgres or MySQL), in two
/// sentences: what is not supported, then the remedy (QA p20: the remedy
/// was spliced into the "… is not supported" template).
fn truncate_refused(qualified: &str, path: &Path) -> Error {
    bad(format!(
        "a TRUNCATE of {qualified} under a mode=\"log_based\" view is not supported (apitap.sql() v1). To rebuild the \
         view, delete {} and the view's replication slot (the next run names it)",
        path.display()
    ))
}

/// p22: a saved position and the relation oid its Postgres side was built
/// from (`pg:<lsn>#oid=<relid>`); `None` for a position saved before 0.68.0.
fn split_relid(pos: &str) -> (&str, Option<u32>) {
    match pos.rsplit_once("#oid=") {
        Some((at, oid)) => (at, oid.parse().ok()),
        None => (pos, None),
    }
}

/// Each Postgres side's position with its relation oid ([`split_relid`]).
fn with_relids(feeds: &[Feed], ps: &mut [String]) {
    for (f, p) in feeds.iter().zip(ps.iter_mut()) {
        if let Feed::Pg(side) = f {
            if !p.contains("#oid=") {
                p.push_str(&format!("#oid={}", side.relid));
            }
        }
    }
}

/// A Postgres side: its pool, names, and the slot it is kept by.
struct PgSide {
    url: String,
    pool: sqlx::PgPool,
    qualified: String,
    relid: u32,
    slot: String,
    publication: String,
    slot_exists: bool,
}

async fn pg_side(url: &str, table: &str, tag: &str, s: usize) -> Result<PgSide> {
    // One connection: the view's control questions run one after another.
    // A pool of two dialled a second connection whenever a query came before
    // sqlx's spawned task had returned the first, and on a role with no
    // connection free that ended in sqlx's bare "pool timed out" (QA, round P
    // p7); a connect refused here is named (`NO_CONNECTION`).
    let pool = crate::source::postgres::PgSource::connect(url, 1).await?.pool().clone();
    let (nsp, rel, partitioned) = crate::logbased::run::resolve_table(&pool, table).await?;
    if partitioned {
        return Err(unsupported(format!("mode=\"log_based\" over the partitioned table {nsp}.{rel} (v1)")));
    }
    let qualified = format!("{nsp}.{rel}");
    let quoted = crate::dialect::postgres::quote_ident_path(&qualified);
    let (ident, relid): (String, i64) =
        sqlx::query_as("SELECT relreplident::text, oid::bigint FROM pg_class WHERE oid = $1::regclass")
            .bind(&quoted)
            .fetch_one(&pool)
            .await
            .map_err(db_err)?;
    if ident != "f" {
        return Err(Error::InvalidInput(format!(
            "mode=\"log_based\" views move by each change's old AND new image, so {qualified} needs \
             REPLICA IDENTITY FULL. Run: ALTER TABLE {quoted} REPLICA IDENTITY FULL"
        )));
    }
    let name = format!("apitap_v_{tag}_{s}");
    let row: Option<(bool,)> = sqlx::query_as("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
        .bind(&name)
        .fetch_optional(&pool)
        .await
        .map_err(db_err)?;
    // p22 (QA D1): the publication must carry the table — the CDC lane's own
    // guard, shared. A first run (no slot yet) creates it or adds a missing
    // member before the slot's snapshot; with a slot, a missing member is a
    // hole in the stream (the table was dropped and recreated, or removed),
    // refused by name instead of draining "ok, 0 changes" for ever.
    let drained: Vec<&str> = if row.is_some() { vec![qualified.as_str()] } else { Vec::new() };
    crate::logbased::run::ensure_publication(&pool, &name, &[qualified.as_str()], &drained, false)
        .await
        .map_err(|e| Error::Transfer(format!("log_based view: {e}. For a view: drop its slot ({name}), remove its state file and run again")))?;
    if row.is_some_and(|r| r.0) {
        return Err(Error::Transfer(format!("log_based view: slot {name} is active: another run of this view is in progress")));
    }
    Ok(PgSide { url: url.to_string(), pool, qualified, relid: relid as u32, slot: name.clone(), publication: name, slot_exists: row.is_some() })
}

/// A MySQL side: its control pool and resolved member.
struct MySide {
    url: String,
    pool: sqlx::MySqlPool,
    ctx: crate::logbased::myrun::MyCtx,
}

async fn my_side(url: &str, table: &str) -> Result<MySide> {
    let pool = crate::logbased::myrun::control_pool(url).await?;
    crate::logbased::mysource::precheck(&pool).await?;
    let db = reqwest::Url::parse(url).map(|u| u.path().trim_matches('/').to_string()).unwrap_or_default();
    let opts = crate::TransferOptions::default();
    let mut ctxs = crate::logbased::myrun::resolve(&pool, &db, &[table.to_string()], true, &opts).await?;
    let ctx = ctxs.pop().ok_or_else(|| bad("no MySQL table resolved"))?;
    Ok(MySide { url: url.to_string(), pool, ctx })
}

enum Feed {
    Pg(PgSide),
    My(MySide),
}

/// Side `s`'s WHERE conjuncts the server can run, as the batch engine
/// pushes them. A bootstrap row they drop is one the view's own re-check
/// would drop: it never reaches the state.
fn pushed_where(v: &View, s: usize) -> Option<String> {
    let conj = &v.pl().sides[s].conj;
    (!conj.is_empty()).then(|| conj.iter().map(|c| format!("({c})")).collect::<Vec<_>>().join(" AND "))
}

/// A side pinned at the instant its change stream starts from: the position,
/// the URL a read sees that instant through, and what holds it open.
struct Pin {
    pos: String,
    url: String,
    keep: Keep,
}

// Held for what dropping them ends (the exported snapshot, the parked
// connections), never read.
#[allow(dead_code)]
enum Keep {
    /// The replication session that exported the snapshot, and its name.
    Pg(Walsender, String),
    My(crate::source::mysql::SnapshotGroup),
}

/// Read connections a MySQL side's first run pins and reads on.
const MY_PIPES: usize = 6;
/// Page ranges a Postgres side's server-side GROUP BY runs as at once.
const PG_AGG_PARTS: i64 = 8;

async fn pin(f: &Feed) -> Result<Pin> {
    match f {
        Feed::Pg(p) => {
            // A slot left by a run that never saved is dropped (a live one,
            // another run's, refuses the drop and so this run).
            sqlx::query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1")
                .bind(&p.slot)
                .execute(&p.pool)
                .await
                .map_err(db_err)?;
            let mut ws = Walsender::connect(&p.url).await?;
            let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
                .fetch_one(&p.pool)
                .await
                .map_err(db_err)?;
            if version >= 170_000 {
                ws.simple_query("SET transaction_timeout = 0").await?;
            }
            let rows = ws.simple_query(&format!("CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput EXPORT_SNAPSHOT", p.slot)).await?;
            let lsn = rows
                .first()
                .and_then(|r| r.get(1).cloned().flatten())
                .ok_or_else(|| Error::Transfer("log_based view: slot creation returned no LSN".into()))?;
            let snapshot = rows
                .first()
                .and_then(|r| r.get(2).cloned().flatten())
                .ok_or_else(|| Error::Transfer("log_based view: slot creation exported no snapshot".into()))?;
            let sep = if p.url.contains('?') { '&' } else { '?' };
            Ok(Pin {
                pos: format!("pg:{}", lsn_to_string(lsn_from_string(&lsn)?)),
                url: format!("{}{sep}__apitap_snapshot={snapshot}", p.url),
                keep: Keep::Pg(ws, snapshot),
            })
        }
        Feed::My(m) => {
            // MySQL exports no snapshot, so the parallel read is pinned the way
            // mydumper pins one: under FLUSH TABLES WITH READ LOCK every read
            // connection opens its consistent snapshot, the binlog position is
            // read, then the lock goes. No commit lands in between, so each
            // connection sees exactly the rows at that position.
            let wire_url = crate::dialect::mysql::secure_url(&m.url);
            // The read connections are opened (TCP, TLS, auth) before the
            // lock, all at once: the lock is then held for the snapshots
            // alone, not for six handshakes one after another.
            let mut conns = futures::future::try_join_all((0..MY_PIPES).map(|_| async {
                let mut w = crate::wire::mywire::MyWire::connect(&wire_url).await?;
                w.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;
                Ok::<_, Error>(w)
            }))
            .await?;
            let mut c = m.pool.acquire().await.map_err(db_err)?;
            sqlx::raw_sql("FLUSH TABLES WITH READ LOCK").execute(&mut *c).await.map_err(|e| {
                Error::Transfer(format!(
                    "log_based view: FLUSH TABLES WITH READ LOCK (a MySQL side's consistent first read needs RELOAD): {e}"
                ))
            })?;
            let snap = async {
                futures::future::try_join_all(conns.iter_mut().map(|w| w.exec("START TRANSACTION WITH CONSISTENT SNAPSHOT"))).await?;
                let mut pos = None;
                for q in ["SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"] {
                    if let Ok(r) = sqlx::raw_sql(q).fetch_one(&mut *c).await {
                        let file: String = r.try_get(0).map_err(db_err)?;
                        let at: u64 = r.try_get(1).map_err(db_err)?;
                        pos = Some(crate::logbased::mysource::pack_pos(&file, u32::try_from(at).map_err(|_| bad("binlog position past 4 GiB"))?));
                        break;
                    }
                }
                Ok::<_, Error>((pos.ok_or_else(|| bad("the MySQL side has no binary log position"))?, conns))
            }
            .await;
            let unlock = sqlx::raw_sql("UNLOCK TABLES").execute(&mut *c).await.map_err(db_err);
            let (pos, conns) = snap?;
            unlock?;
            drop(c);
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let token = format!("{}-{}", std::process::id(), SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
            let group = crate::source::mysql::snapshot_group(&token, conns);
            let sep = if m.url.contains('?') { '&' } else { '?' };
            Ok(Pin {
                pos: format!("my:{pos}"),
                url: format!("{}{sep}{}={token}", m.url, crate::source::mysql::SNAPSHOT_PARAM),
                keep: Keep::My(group),
            })
        }
    }
}

/// What a first run's readers hand the one thread that moves the state.
enum Msg {
    /// Rows of side `s`.
    Batch(usize, ArrowBatch),
    /// `S` records a server-side GROUP BY built.
    Recs(Vec<u8>),
    /// A reader of side `s` finished.
    Done(usize),
}

type Tx = tokio::sync::mpsc::Sender<Result<Msg>>;

/// Side `s`'s per-key GROUP BY on its Postgres server, as one COPY select
/// list after the key: the rows, then per aggregate the count of its
/// non-NULL argument and the argument's sum scaled to an exact `int8`
/// (none for COUNT(*)). `None`: an argument the server cannot compute so.
fn agg_select(v: &View, s: usize) -> Option<(String, Vec<bool>)> {
    let p = v.pl();
    let side = &p.sides[s];
    // Opt-in only: it moves the heavy work (a GROUP BY over the whole
    // table) onto the source's own server, which apitap is there to spare.
    if side.dialect != Dialect::Pg || s == p.group_side || !std::env::var("APITAP_SQL_VIEW_PG_AGG").is_ok_and(|x| x == "1") {
        return None;
    }
    let mut cols = vec!["count(*)::int8".to_string()];
    let mut sums = Vec::new();
    for a in &p.aggs {
        match (&a.arg, a.f) {
            (None, _) | (_, AggFn::CountStar) => {
                sums.push(false);
            }
            (Some(e), _) => {
                let x = pg_sql(side, e)?;
                let scaled = match a.ty {
                    Some(Ty::Int) => x.clone(),
                    Some(Ty::Dec(sc)) => format!("({x}) * 1{}::numeric", "0".repeat(sc as usize)),
                    _ => return None,
                };
                cols.push(format!("count({x})::int8"));
                cols.push(format!("sum({scaled})::int8"));
                sums.push(true);
            }
        }
    }
    Some((cols.join(", "), sums))
}

/// Per aggregate, whether it is an exact SUM (an int or a decimal) rather
/// than a COUNT; `None` when one is neither.
fn exact_sums(v: &View) -> Option<Vec<bool>> {
    v.pl()
        .aggs
        .iter()
        .map(|a| match (&a.arg, a.f) {
            (None, _) | (_, AggFn::CountStar) => Some(false),
            (Some(_), _) => matches!(a.ty, Some(Ty::Int) | Some(Ty::Dec(_))).then_some(true),
        })
        .collect()
}

/// Side `s` summed per key by its server, PG_AGG_PARTS page ranges at once,
/// each pinned to the slot's snapshot; the records go to `tx`.
async fn spawn_pg_agg(v: &View, s: usize, f: &PgSide, snapshot: &str, sel: String, sums: Vec<bool>, tx: &Tx) -> Result<usize> {
    let p = v.pl();
    let side = &p.sides[s];
    let at = p.keys.expect("a join").iter().find(|x| x.0 == s).expect("keyed").1;
    let key = quote(Dialect::Pg, &side.schema[side.read[at]].name);
    let tb = quote(Dialect::Pg, &side.table);
    let (pages, ver): (i64, i32) = sqlx::query_as(
        "SELECT (pg_relation_size($1::regclass) / current_setting('block_size')::int8)::int8, \
                current_setting('server_version_num')::int4",
    )
    .bind(&tb)
    .fetch_one(&f.pool)
    .await
    .map_err(db_err)?;
    let n = if ver >= 140_000 && pages >= 1024 { PG_AGG_PARTS } else { 1 };
    let mut filt: Vec<String> = side.conj.iter().map(|c| format!("({c})")).collect();
    filt.push(format!("{key} IS NOT NULL"));
    for i in 0..n {
        let mut w = filt.clone();
        if n > 1 {
            w.push(format!("ctid >= '({},0)'::tid", pages * i / n));
            if i < n - 1 {
                w.push(format!("ctid < '({},0)'::tid", pages * (i + 1) / n));
            }
        }
        let sql = format!("COPY (SELECT {key}::int8, {sel} FROM {tb} WHERE {} GROUP BY 1) TO STDOUT (FORMAT binary)", w.join(" AND "));
        let (url, snap, sums, tx) = (f.url.clone(), snapshot.to_string(), sums.clone(), tx.clone());
        tokio::spawn(async move {
            let r = async {
                let mut ws = Walsender::connect_sql(&url).await?;
                ws.simple_query(&format!(
                    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'; SET LOCAL work_mem = '32MB'",
                    snap.replace('\'', "''")
                ))
                .await?;
                ws.copy_out_start(&sql).await?;
                let mut cp = CopyRecs::new(sums);
                let mut buf = Vec::new();
                while ws.copy_out_next(&mut buf, 1 << 20).await? {
                    let recs = cp.feed(&buf)?;
                    if !recs.is_empty() && tx.send(Ok(Msg::Recs(recs))).await.is_err() {
                        return Ok(());
                    }
                }
                cp.finish()
            }
            .await;
            let _ = tx.send(r.map(|()| Msg::Done(s))).await;
        });
    }
    Ok(n as usize)
}

/// The bulk `S` side read straight off Postgres: the key and the
/// aggregates' argument columns (`bulk_s_columns`), PG_AGG_PARTS page ranges
/// at once on the slot's snapshot, each binary COPY decoded here into
/// column batches (no row framing, staging and re-decoding on the way).
/// `None`: a column this does not decode (it is not an integer or an exact
/// numeric), for the general read.
async fn spawn_pg_rows(v: &View, s: usize, f: &PgSide, snapshot: &str, tx: &Tx) -> Result<Option<usize>> {
    let p = v.pl();
    let side = &p.sides[s];
    let names = v.bulk_s_columns();
    let mut kinds = Vec::new();
    for n in &names {
        let fld = side.schema.iter().find(|x| &x.name == n).ok_or_else(|| bad("a bulk column not in its schema"))?;
        match fld.kind {
            ArrowKind::Int16 | ArrowKind::Int32 | ArrowKind::Int64 if !fld.rewritten => kinds.push(fld.kind),
            ArrowKind::Decimal { s, .. } if !fld.rewritten && s >= 0 => kinds.push(fld.kind),
            _ => return Ok(None),
        }
    }
    let tb = quote(Dialect::Pg, &side.table);
    let (pages, ver): (i64, i32) = sqlx::query_as(
        "SELECT (pg_relation_size($1::regclass) / current_setting('block_size')::int8)::int8, \
                current_setting('server_version_num')::int4",
    )
    .bind(&tb)
    .fetch_one(&f.pool)
    .await
    .map_err(db_err)?;
    let n = if ver >= 140_000 && pages >= 1024 { PG_AGG_PARTS } else { 1 };
    let cols = names.iter().map(|c| quote(Dialect::Pg, c)).collect::<Vec<_>>().join(", ");
    let filt: Vec<String> = side.conj.iter().map(|c| format!("({c})")).collect();
    for i in 0..n {
        let mut w = filt.clone();
        if n > 1 {
            w.push(format!("ctid >= '({},0)'::tid", pages * i / n));
            if i < n - 1 {
                w.push(format!("ctid < '({},0)'::tid", pages * (i + 1) / n));
            }
        }
        let wh = if w.is_empty() { String::new() } else { format!(" WHERE {}", w.join(" AND ")) };
        let sql = format!("COPY (SELECT {cols} FROM {tb}{wh}) TO STDOUT");
        let (url, snap, kinds, tx) = (f.url.clone(), snapshot.to_string(), kinds.clone(), tx.clone());
        tokio::spawn(async move {
            let r = async {
                let mut ws = Walsender::connect_sql(&url).await?;
                ws.simple_query(&format!(
                    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'",
                    snap.replace('\'', "''")
                ))
                .await?;
                ws.copy_out_start(&sql).await?;
                let mut cc = CopyCols::new(kinds);
                let mut buf = Vec::new();
                // Text holds twice the rows a byte of binary: a half-MiB
                // chunk keeps a batch's rows (and memory) where they were.
                while ws.copy_out_next(&mut buf, 1 << 19).await? {
                    if let Some(b) = cc.feed(&buf)? {
                        if tx.send(Ok(Msg::Batch(s, b))).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                cc.finish()
            }
            .await;
            let _ = tx.send(r.map(|()| Msg::Done(s))).await;
        });
    }
    Ok(Some(n as usize))
}

/// Text COPY lines of integer and numeric fields as column batches (a
/// numeric is half the bytes of its binary form, and its digits parse
/// faster than base-10000 groups decode).
struct CopyCols {
    kinds: Vec<ArrowKind>,
    /// A line the last chunk cut, waiting for the rest of it.
    carry: Vec<u8>,
    /// The batch being built: columns, validity, rows, capacity.
    cbs: Vec<Cb>,
    nulls: Vec<Option<Vec<u8>>>,
    rows: usize,
    cap: usize,
    /// Bytes a line took in the last batch: the next one's columns sized
    /// off it (a fixed guess over-allocates; batches in flight are the
    /// memory a 256 MB cage has).
    bpr: usize,
}

/// A column being built off COPY.
enum Cb {
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    Dec(Vec<i128>, u32),
}

impl Cb {
    fn for_kinds(kinds: &[ArrowKind], cap: usize) -> Vec<Cb> {
        kinds
            .iter()
            .map(|k| match k {
                ArrowKind::Int16 => Cb::I16(Vec::with_capacity(cap)),
                ArrowKind::Int32 => Cb::I32(Vec::with_capacity(cap)),
                ArrowKind::Int64 => Cb::I64(Vec::with_capacity(cap)),
                ArrowKind::Decimal { s, .. } => Cb::Dec(Vec::with_capacity(cap), u32::try_from(*s).unwrap_or(0)),
                _ => unreachable!("checked at the plan"),
            })
            .collect()
    }

    /// The built columns, a bitmap past the rows cut to them (bits of a row
    /// taken back cleared).
    fn finish(cbs: Vec<Cb>, nulls: Vec<Option<Vec<u8>>>, rows: usize) -> ArrowBatch {
        let cols = cbs
            .into_iter()
            .zip(nulls)
            .map(|(cb, bm)| {
                let validity = bm.map(|mut bm| {
                    bm.resize(rows.div_ceil(8), 0);
                    if rows % 8 != 0 {
                        let last = rows.div_ceil(8) - 1;
                        bm[last] &= (1u8 << (rows % 8)) - 1;
                    }
                    bm
                });
                match cb {
                    Cb::I16(data) => FinishedCol::I16 { validity, data },
                    Cb::I32(data) => FinishedCol::I32 { validity, data },
                    Cb::I64(data) => FinishedCol::I64 { validity, data },
                    Cb::Dec(data, _) => FinishedCol::Dec128 { validity, data },
                }
            })
            .collect();
        ArrowBatch { rows, cols }
    }
}

/// Row `rows` of column `c` NULL or not in its validity bitmap (a column's
/// stays `None` until its first NULL).
#[inline]
fn mark_null(nulls: &mut [Option<Vec<u8>>], c: usize, rows: usize, cap: usize, null: bool) {
    if null {
        let bm = nulls[c].get_or_insert_with(|| {
            let mut bm = vec![0u8; cap.div_ceil(8) + 1];
            for r in 0..rows {
                bm[r / 8] |= 1 << (r % 8);
            }
            bm
        });
        if bm.len() <= rows / 8 {
            bm.resize(rows / 8 + 1, 0);
        }
    } else if let Some(bm) = nulls[c].as_mut() {
        if bm.len() <= rows / 8 {
            bm.resize(rows / 8 + 1, 0);
        }
        bm[rows / 8] |= 1 << (rows % 8);
    }
}

/// A text COPY numeric (`-`? digits (`.` digits)?) scaled to `sc` places,
/// digits past them cut (toward zero, as the binary decode does), checked.
fn dec_text(f: &[u8], sc: u32) -> Result<i128> {
    let bad_num = || bad("a bulk COPY numeric that is not a finite number");
    let over = || bad("numeric overflow");
    let (neg, d) = match f.first() {
        Some(b'-') => (true, &f[1..]),
        _ => (false, f),
    };
    let mut acc: i128 = 0;
    let mut i = 0;
    while i < d.len() && d[i].is_ascii_digit() {
        acc = acc.checked_mul(10).and_then(|a| a.checked_add(i128::from(d[i] - b'0'))).ok_or_else(over)?;
        i += 1;
    }
    let ints = i;
    let mut taken = 0u32;
    if i < d.len() && d[i] == b'.' {
        i += 1;
        while i < d.len() && d[i].is_ascii_digit() {
            if taken < sc {
                acc = acc.checked_mul(10).and_then(|a| a.checked_add(i128::from(d[i] - b'0'))).ok_or_else(over)?;
                taken += 1;
            }
            i += 1;
        }
        if i == ints + 1 && ints == 0 {
            return Err(bad_num());
        }
    }
    if i != d.len() || i == 0 {
        return Err(bad_num());
    }
    if taken < sc {
        acc = 10i128.checked_pow(sc - taken).and_then(|m| acc.checked_mul(m)).ok_or_else(over)?;
    }
    Ok(if neg { -acc } else { acc })
}

impl CopyCols {
    fn new(kinds: Vec<ArrowKind>) -> CopyCols {
        CopyCols { kinds, carry: Vec::new(), cbs: Vec::new(), nulls: Vec::new(), rows: 0, cap: 0, bpr: 0 }
    }

    /// The whole lines `data` completes, as one batch (`None`: none). Only
    /// the line a chunk cuts is copied (joined to the next chunk's head);
    /// the rest is parsed where it lies.
    fn feed(&mut self, data: &[u8]) -> Result<Option<ArrowBatch>> {
        let n = self.carry.len() + data.len();
        let bpr = if self.bpr > 0 { self.bpr } else { 3 * self.kinds.len().max(1) };
        self.cap = n / bpr + n / (16 * bpr) + 8;
        self.cbs = Cb::for_kinds(&self.kinds, self.cap);
        self.nulls = vec![None; self.kinds.len()];
        self.rows = 0;
        let mut rest = data;
        if !self.carry.is_empty() {
            let Some(nl) = data.iter().position(|&c| c == b'\n') else {
                self.carry.extend_from_slice(data);
                return Ok(None);
            };
            let mut line = std::mem::take(&mut self.carry);
            line.extend_from_slice(&data[..=nl]);
            self.lines(&line)?;
            line.clear();
            self.carry = line;
            rest = &data[nl + 1..];
        }
        match rest.iter().rposition(|&c| c == b'\n') {
            Some(last) => {
                self.lines(&rest[..=last])?;
                self.carry.extend_from_slice(&rest[last + 1..]);
            }
            None => self.carry.extend_from_slice(rest),
        }
        if self.rows == 0 {
            return Ok(None);
        }
        self.bpr = (n / self.rows).max(1);
        let (cbs, nulls) = (std::mem::take(&mut self.cbs), std::mem::take(&mut self.nulls));
        Ok(Some(Cb::finish(cbs, nulls, self.rows)))
    }

    /// Whole lines `r` (it ends in a newline) onto the batch: fields split
    /// by tabs, `\\N` a NULL. Integers and numerics carry no escapes, so a
    /// line is parsed as is.
    fn lines(&mut self, r: &[u8]) -> Result<()> {
        let nf = self.kinds.len();
        let malformed = || bad("a malformed bulk text COPY line");
        if r.last() != Some(&b'\n') {
            return Err(malformed());
        }
        // SAFETY (every `at`): `r` ends in a newline, and each read below is
        // at most one past a byte that is not one (a digit, `-`, `.`, `\\`,
        // a tab) or is the scan's own stop; a scan halts at the first byte
        // that is not a digit, which the final newline is. So no index
        // reaches past `r`'s last byte.
        let at = |i: usize| -> u8 { unsafe { *r.get_unchecked(i) } };
        let mut q = 0usize;
        let rows = &mut self.rows;
        let (cbs, nulls, cap) = (&mut self.cbs, &mut self.nulls, self.cap);
        while q < r.len() {
            for c in 0..nf {
                let stop = if c + 1 < nf { b'\t' } else { b'\n' };
                let st = q;
                let null = at(q) == b'\\' && at(q + 1) == b'N';
                if null {
                    q += 2;
                    match &mut cbs[c] {
                        Cb::I16(v) => v.push(0),
                        Cb::I32(v) => v.push(0),
                        Cb::I64(v) => v.push(0),
                        Cb::Dec(v, _) => v.push(0),
                    }
                } else {
                    let neg = at(q) == b'-';
                    q += usize::from(neg);
                    let ds = q;
                    let mut acc: u64 = 0;
                    loop {
                        let d = at(q).wrapping_sub(b'0');
                        if d > 9 {
                            break;
                        }
                        acc = acc.wrapping_mul(10).wrapping_add(u64::from(d));
                        q += 1;
                    }
                    let mut nd = q - ds;
                    // Past 18 digits, an odd scale, or anything but the
                    // field's end: the checked decode of the whole field.
                    let slow = |q: &mut usize| -> Result<&[u8]> {
                        let mut e = *q;
                        while at(e) != stop && at(e) != b'\n' {
                            e += 1;
                        }
                        *q = e;
                        if at(e) != stop {
                            return Err(malformed());
                        }
                        Ok(&r[st..e])
                    };
                    match &mut cbs[c] {
                        Cb::Dec(v, sc) => {
                            let mut taken = 0u32;
                            if at(q) == b'.' {
                                q += 1;
                                loop {
                                    let d = at(q).wrapping_sub(b'0');
                                    if d > 9 {
                                        break;
                                    }
                                    if taken < *sc {
                                        acc = acc.wrapping_mul(10).wrapping_add(u64::from(d));
                                        taken += 1;
                                        nd += 1;
                                    }
                                    q += 1;
                                }
                            }
                            if nd > 0 && nd <= 18 && *sc - taken <= 18 && at(q) == stop {
                                let x = i128::from(acc) * i128::from(10u64.pow(*sc - taken));
                                v.push(if neg { -x } else { x });
                            } else {
                                let f = slow(&mut q)?;
                                v.push(dec_text(f, *sc)?);
                            }
                        }
                        cb => {
                            let n: i64 = if nd > 0 && nd <= 18 && at(q) == stop {
                                if neg { -(acc as i64) } else { acc as i64 }
                            } else {
                                let f = slow(&mut q)?;
                                std::str::from_utf8(f).ok().and_then(|s| s.parse::<i64>().ok()).ok_or_else(malformed)?
                            };
                            let range = || bad("a bulk COPY integer out of its column's range");
                            match cb {
                                Cb::I16(v) => v.push(i16::try_from(n).map_err(|_| range())?),
                                Cb::I32(v) => v.push(i32::try_from(n).map_err(|_| range())?),
                                Cb::I64(v) => v.push(n),
                                Cb::Dec(..) => unreachable!("matched above"),
                            }
                        }
                    }
                }
                if at(q) != stop {
                    return Err(malformed());
                }
                q += 1;
                if null || nulls[c].is_some() {
                    mark_null(nulls, c, *rows, cap, null);
                }
            }
            *rows += 1;
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        if !self.carry.is_empty() {
            return Err(bad("a bulk COPY ended early"));
        }
        Ok(())
    }
}

/// Binary COPY tuples of (key, rows, per aggregate count[, sum]) as `S`
/// records (key, rows, an accumulator per aggregate).
struct CopyRecs {
    carry: Vec<u8>,
    head: bool,
    end: bool,
    sums: Vec<bool>,
}

impl CopyRecs {
    fn new(sums: Vec<bool>) -> CopyRecs {
        CopyRecs { carry: Vec::new(), head: false, end: false, sums }
    }

    fn feed(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(data.len());
        if self.carry.is_empty() {
            // Most chunks start on a tuple: parsed where they lie, and only
            // the cut tail is kept.
            let at = self.parse(data, &mut out)?;
            self.carry.extend_from_slice(&data[at..]);
        } else {
            self.carry.extend_from_slice(data);
            let b = std::mem::take(&mut self.carry);
            let at = self.parse(&b, &mut out)?;
            self.carry.extend_from_slice(&b[at..]);
        }
        Ok(out)
    }

    /// The whole tuples of `b` into `out`; the bytes consumed.
    fn parse(&mut self, b: &[u8], out: &mut Vec<u8>) -> Result<usize> {
        let mut at = 0usize;
        if !self.head {
            if b.len() < 19 {
                return Ok(0);
            }
            if &b[..11] != b"PGCOPY\n\xff\r\n\0" {
                return Err(bad("a server-side GROUP BY's COPY has no binary header"));
            }
            let ext = u32::from_be_bytes(b[15..19].try_into().expect("4")) as usize;
            if b.len() < 19 + ext {
                return Ok(0);
            }
            at = 19 + ext;
            self.head = true;
        }
        let nf = 2 + 2 * self.sums.iter().filter(|x| **x).count();
        if nf > 64 {
            return Err(bad("a server-side GROUP BY of more than 31 sums"));
        }
        let mut vals = [0i64; 64];
        loop {
            if b.len() < at + 2 {
                break;
            }
            let cnt = i16::from_be_bytes(b[at..at + 2].try_into().expect("2"));
            if cnt == -1 {
                self.end = true;
                at += 2;
                break;
            }
            if cnt as usize != nf {
                return Err(bad(format!("a server-side GROUP BY's COPY row of {cnt} fields, wanted {nf}")));
            }
            // Every field is an int8 or NULL: 4 + 8 bytes at most.
            let mut q = at + 2;
            let mut nulls = 0u64;
            let mut whole = true;
            for (fi, slot) in vals.iter_mut().take(nf).enumerate() {
                if b.len() < q + 4 {
                    whole = false;
                    break;
                }
                let len = i32::from_be_bytes(b[q..q + 4].try_into().expect("4"));
                q += 4;
                if len < 0 {
                    nulls |= 1 << fi;
                    *slot = 0;
                    continue;
                }
                if len != 8 {
                    return Err(bad("a server-side GROUP BY's COPY field that is not an int8"));
                }
                if b.len() < q + 8 {
                    whole = false;
                    break;
                }
                *slot = i64::from_be_bytes(b[q..q + 8].try_into().expect("8"));
                q += 8;
            }
            if !whole {
                break;
            }
            at = q;
            if nulls & 3 != 0 {
                return Err(bad("a server-side GROUP BY's COPY row without its key or rows"));
            }
            out.extend_from_slice(&vals[0].to_le_bytes());
            out.extend_from_slice(&vals[1].to_le_bytes());
            // The staged layout of `View::bulk_layout(Some(sums))`: a count
            // and an int8 sum per summed aggregate, nothing for COUNT(*)
            // (a NULL, a sum of no rows, is 0).
            let mut f = 2;
            for &has_sum in &self.sums {
                if has_sum {
                    out.extend_from_slice(&vals[f].to_le_bytes());
                    out.extend_from_slice(&vals[f + 1].to_le_bytes());
                    f += 2;
                }
            }
        }
        Ok(at)
    }

    fn finish(&self) -> Result<()> {
        if !self.end || !self.carry.is_empty() {
            return Err(bad("a server-side GROUP BY's COPY ended early"));
        }
        Ok(())
    }
}

/// Side `s` read at its pin (WHERE pushed), its batches to `tx`.
async fn spawn_read(v: &View, s: usize, pin: &Pin, parallel: usize, columns: Vec<String>, tx: &Tx) -> Result<usize> {
    let ropts = ReadOptions {
        columns: Some(columns),
        parallel: Some(parallel),
        batch_bytes: Some(2 << 20),
        where_sql: pushed_where(v, s),
        ..Default::default()
    };
    let mut h = crate::read::read_start(&pin.url, &v.pl().sides[s].table, &ropts).await?;
    let tx = tx.clone();
    tokio::spawn(async move {
        let r = async {
            while let Some(b) = h.recv().await? {
                if tx.send(Ok(Msg::Batch(s, b))).await.is_err() {
                    return Ok(());
                }
            }
            Ok::<_, Error>(())
        }
        .await;
        let _ = tx.send(r.map(|()| Msg::Done(s))).await;
    });
    Ok(1)
}

/// The first run: every side pinned at its stream's start, then all read at
/// once into one state. A join whose every WHERE runs on its servers goes
/// the bulk way (fixed records, sorted per bucket), with a Postgres `S`
/// side summed per key by its server when `server_agg` lets it; any other
/// view moves change by change. Returns each side's position.
async fn first_run(v: &mut View, feeds: &[Feed], tag: &str, server_agg: bool, t0: std::time::Instant) -> Result<Vec<String>> {
    let dbg = dbg();
    // Each side is pinned on its own server: both at once.
    let pins = futures::future::try_join_all(feeds.iter().map(pin)).await?;
    let bulk = v.bulk_ok();
    if dbg {
        eprintln!("[sql] view {tag}: {} side(s) pinned{}, in {:.1}s", pins.len(), if bulk { ", bulk" } else { "" }, t0.elapsed().as_secs_f64());
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Msg>>(4);
    let mut readers = vec![0usize; feeds.len()];
    let mut agg_side = None;
    let mut agg_sums = None;
    let gs = v.pl().group_side;
    for (s, (f, pn)) in feeds.iter().zip(&pins).enumerate() {
        let sel = if bulk && server_agg { agg_select(v, s) } else { None };
        // A bulk run reads the GROUP BY side's key and groups only.
        let cols = match (bulk, s == gs) {
            (true, true) => v.bulk_g_columns(),
            (true, false) => v.bulk_s_columns(),
            (false, _) => v.columns(s),
        };
        readers[s] = match (f, &pn.keep, sel) {
            (Feed::Pg(p), Keep::Pg(_, snap), Some((sel, sums))) => {
                agg_side = Some(s);
                agg_sums = Some(sums.clone());
                spawn_pg_agg(v, s, p, snap, sel, sums, &tx).await?
            }
            (Feed::Pg(p), Keep::Pg(_, snap), None) if bulk && s != gs => match spawn_pg_rows(v, s, p, snap, &tx).await? {
                Some(n) => n,
                None => spawn_read(v, s, pn, 2, cols, &tx).await?,
            },
            (Feed::Pg(_), ..) => spawn_read(v, s, pn, 2, cols, &tx).await?,
            (Feed::My(_), ..) => spawn_read(v, s, pn, MY_PIPES, cols, &tx).await?,
        };
    }
    drop(tx);
    // Rows summed here per key use the server's narrow layout when every
    // aggregate is an exact sum or a count (a sum past it reruns wide).
    let lay = match (&agg_sums, server_agg) {
        (Some(s), _) => v.bulk_layout(Some(s)),
        (None, true) => v.bulk_layout(exact_sums(v).as_deref()),
        (None, false) => v.bulk_layout(None),
    };
    let mut bk = if bulk { Some(v.bulk_begin(lay)?) } else { None };
    let mut n = vec![0u64; feeds.len()];
    while let Some(m) = rx.recv().await {
        match m? {
            Msg::Batch(s, b) => {
                n[s] += b.rows as u64;
                match bk.as_mut() {
                    Some(bk) if s == gs => v.bulk_g(bk, &b, true)?,
                    Some(bk) => v.bulk_s_rows(bk, &b, true)?,
                    None => v.apply_batch(s, &b, &vec![1; b.rows])?,
                }
            }
            Msg::Recs(r) => {
                let bk = bk.as_mut().expect("records only in a bulk run");
                n[agg_side.expect("records come from a server-side GROUP BY")] += (r.len() / bk.srec()) as u64;
                v.bulk_s_recs(bk, &r)?;
            }
            Msg::Done(s) => {
                readers[s] -= 1;
                if readers[s] == 0 && dbg {
                    let what = if agg_side == Some(s) { "key(s) summed by the server" } else { "rows" };
                    eprintln!(
                        "[sql] view {tag}: bootstrap {} {} {what} at {} in {:.1}s",
                        v.pl().sides[s].table,
                        n[s],
                        pins[s].pos,
                        t0.elapsed().as_secs_f64()
                    );
                }
            }
        }
    }
    if readers.iter().any(|&r| r != 0) {
        return Err(bad("a first-run reader stopped without finishing"));
    }
    if let Some(bk) = bk {
        v.bulk_finish(bk)?;
        if dbg {
            eprintln!("[sql] view {tag}: bulk state settled, {} bucket(s), in {:.1}s", v.buckets(), t0.elapsed().as_secs_f64());
        }
    }
    Ok(pins.into_iter().map(|p| p.pos).collect())
}

/// A later run of side `s`: drain from `from` to the log's end. Returns the
/// new position, the images applied, and on Postgres the open session (the
/// slot is confirmed after the answer lands).
async fn catch_up(v: &mut View, s: usize, f: &Feed, from: &str, path: &Path) -> Result<(String, u64, Option<(Walsender, u64)>)> {
    let mut n = 0u64;
    match f {
        Feed::Pg(p) => {
            let p0 = lsn_from_string(from.strip_prefix("pg:").ok_or_else(|| bad("a Postgres side without a pg: position"))?)?;
            // The slot must still stand where this view's state left it: a
            // slot another consumer confirmed past that position has given
            // away the changes in between, and the view would land short
            // with no error (W1 round P, L7). Behind it is fine: the state
            // is written before the slot is confirmed.
            let confirmed: Option<(Option<String>,)> = sqlx::query_as("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1")
                .bind(&p.slot)
                .fetch_optional(&p.pool)
                .await
                .map_err(db_err)?;
            if let Some((Some(c),)) = confirmed {
                let c = lsn_from_string(&c)?;
                if c > p0 {
                    return Err(Error::Transfer(format!(
                        "log_based view: slot {} was advanced past this view's position ({}) to {} by another consumer \
                         (a copy of this view's state in another state dir or on another host, or a direct use of the slot); \
                         the changes in between are gone for this view. Rebuild it: remove its state file {} and drop the slot",
                        p.slot,
                        lsn_to_string(p0),
                        lsn_to_string(c),
                        path.display()
                    )));
                }
            }
            let stop: (String,) = sqlx::query_as(&format!("SELECT {}::text", crate::logbased::run::WAL_END))
                .fetch_one(&p.pool)
                .await
                .map_err(db_err)?;
            let stop = lsn_from_string(&stop.0)?;
            if p0 >= stop {
                return Ok((from.to_string(), 0, None));
            }
            let mut key = crate::logbased::run::pk_columns(&p.pool, &p.qualified).await?;
            if key.is_empty() {
                // No primary key: every column is the identity, and an
                // update is a delete of its old image plus an insert.
                key = sqlx::query("SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum")
                    .bind(crate::dialect::postgres::quote_ident_path(&p.qualified))
                    .fetch_all(&p.pool)
                    .await
                    .map_err(db_err)?
                    .iter()
                    .map(|r| r.get::<String, _>(0))
                    .collect();
            }
            let key_cols = HashMap::from([(p.qualified.clone(), key)]);
            let mut sess = DrainSession::expecting(HashMap::from([(p.qualified.clone(), (p.relid, p.qualified.clone()))]));
            sess.keep_old = true;
            // A window is applied after `drain` returns it, never beside it.
            sess.apply_after = true;
            let mut ws = Walsender::connect(&p.url).await?;
            ws.start_replication(&p.slot, p0, &p.publication, false).await?;
            let (_tx, rx) = tokio::sync::watch::channel(p0);
            let budget = view_window_budget(1);
            let (mut cur, mut cur_ver) = (p0, p0);
            loop {
                let out = drain(&mut ws, &mut sess, cur, stop, &key_cols, 30, budget, &rx, true, None, false).await?;
                let (end, ver, hit) = (out.id.end(), out.id.ver(), out.hit_budget);
                if ver > cur_ver {
                    n += apply_window(v, s, &out, &p.qualified, path)?;
                    cur_ver = ver;
                }
                cur = cur.max(end);
                if !hit {
                    break;
                }
            }
            Ok((format!("pg:{}", lsn_to_string(cur)), n, Some((ws, cur))))
        }
        Feed::My(m) => {
            let p0: u64 = from.strip_prefix("my:").and_then(|x| x.parse().ok()).ok_or_else(|| bad("a MySQL side without a my: position"))?;
            let budget = view_window_budget(3);
            // Depth 1 (p10, L14): a window queued, one applying, one filling;
            // two queued held a fourth window's images at once.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<DrainOutcome>(1);
            let ctxs = [crate::logbased::myrun::MyCtx {
                table_arg: m.ctx.table_arg.clone(),
                qualified: m.ctx.qualified.clone(),
                dest_table: m.ctx.dest_table.clone(),
                pk_cols: m.ctx.pk_cols.clone(),
                source_id: m.ctx.source_id.clone(),
            }];
            let seed = format!("view:{}", v.fp_hex());
            let drainer = crate::logbased::myrun::drain_windows(&m.url, &m.pool, &ctxs, p0, None, &seed, 30, budget, true, tx, true);
            let qualified = m.ctx.qualified.clone();
            let consume = async {
                let mut cur_ver = p0;
                let mut n = 0u64;
                while let Some(out) = rx.recv().await {
                    let ver = out.id.ver();
                    if ver > cur_ver {
                        n += apply_window(v, s, &out, &qualified, path)?;
                        cur_ver = ver;
                    }
                }
                Ok::<u64, Error>(n)
            };
            let (end, got) = tokio::join!(drainer, consume);
            let got = got?;
            let end = end?;
            n += got;
            Ok((format!("my:{end}"), n, None))
        }
    }
}

/// A view's tag (16 hex): its query and sources' fingerprint with its
/// destination (the URL without credentials or options, and the table).
fn view_tag(fp: &str, dst_url: &str, table: &str) -> String {
    use sha2::{Digest, Sha256};
    let (scheme, rest) = dst_url.split_once("://").unwrap_or(("", dst_url));
    let rest = rest.split('?').next().unwrap_or(rest);
    let rest = rest.rsplit_once('@').map_or(rest, |x| x.1);
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let mut h = Sha256::new();
    h.update(fp.as_bytes());
    h.update(b"\n");
    h.update(format!("{}://{}/{}", scheme.to_ascii_lowercase(), host.to_ascii_lowercase(), path).as_bytes());
    h.update(b"\n");
    h.update(table.as_bytes());
    h.finalize().iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// The names (tag) a view's Postgres slots and publications carry when they
/// are not its own tag's: a view adopted from a release that tagged it by
/// the query alone keeps the slots it had (W1 round P, L7 upgrade).
fn alias_path(dir: &Path, tag: &str) -> PathBuf {
    dir.join(format!("{tag}.alias"))
}

fn read_alias(dir: &Path, tag: &str) -> Option<String> {
    let s = std::fs::read_to_string(alias_path(dir, tag)).ok()?;
    let s = s.trim();
    (s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit())).then(|| s.to_string())
}

/// A state left by a release that tagged views by the query alone (through
/// 0.67.2): `{fp16}.state` in this state dir. It is adopted, never dropped:
/// when every Postgres side's old slot and publication are there, the slot
/// is idle and stands exactly at the position the old state recorded, the
/// state and its buckets are renamed to this view's tag and an alias file
/// keeps the old slot names, so the view drains on from where it was (no
/// rebuild, no second slot). An old state is adopted once (it is renamed):
/// the state is the query's, so whichever destination first runs the query
/// takes it, and its answer lands whole (a replace) wherever that is.
/// Otherwise the old slots that still exist are named in a warning and the
/// view bootstraps under its own tag.
/// Returns the adopted tag, and whether `v` holds a loaded state it did not
/// adopt (the caller plans afresh).
async fn adopt_old_state(v: &mut View, sides: &[(Dialect, String, String)], dir: &Path, tag: &str, fp16: &str) -> Result<(Option<String>, bool)> {
    let old = dir.join(format!("{fp16}.state"));
    // TEST ONLY, never set by default: the upgrade as it was before the
    // adoption (round P p1), so `e2e_sql_live_upgrade.py` can be seen RED.
    if std::env::var_os("APITAP_TEST_NO_VIEW_ADOPT").is_some() {
        return Ok((None, false));
    }
    if fp16 == tag || !old.exists() || dir.join(format!("{tag}.state")).exists() {
        return Ok((None, false));
    }
    let dbg = dbg();
    let positions = match v.load(&old, None) {
        Ok(ps) if ps.len() == sides.len() => Some(ps),
        Ok(_) => None,
        Err(e) => {
            if dbg {
                eprintln!("[sql] view {tag}: the older state {} is not usable: {e}", old.display());
            }
            None
        }
    };
    let mut why: Vec<String> = Vec::new();
    let mut existing: Vec<(String, String)> = Vec::new();
    for (s, (d, url, _)) in sides.iter().enumerate() {
        if *d != Dialect::Pg {
            continue;
        }
        let name = format!("apitap_v_{fp16}_{s}");
        let pool = crate::source::postgres::PgSource::connect(url, 1).await?.pool().clone();
        let slot: Option<(bool, Option<String>)> =
            sqlx::query_as("SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1")
                .bind(&name)
                .fetch_optional(&pool)
                .await
                .map_err(db_err)?;
        let has_pub: Option<i32> = sqlx::query_scalar("SELECT 1 FROM pg_publication WHERE pubname = $1")
            .bind(&name)
            .fetch_optional(&pool)
            .await
            .map_err(db_err)?;
        pool.close().await;
        let Some((active, confirmed)) = slot else {
            why.push(format!("slot {name} is gone"));
            continue;
        };
        existing.push((name.clone(), url.clone()));
        let at = positions.as_ref().and_then(|ps| ps[s].strip_prefix("pg:")).and_then(|p| lsn_from_string(p).ok());
        let c = confirmed.as_deref().and_then(|c| lsn_from_string(c).ok());
        if active {
            why.push(format!("slot {name} is in use by another run"));
        } else if has_pub.is_none() {
            why.push(format!("publication {name} is gone"));
        } else if at.is_none() || c.is_none() || at != c {
            why.push(format!("slot {name} does not stand at the older state's position"));
        }
    }
    if positions.is_none() {
        why.push("the older state is not usable".into());
    }
    if why.is_empty() {
        let io = |e: std::io::Error| Error::Transfer(format!("view state {}: {e}", dir.display()));
        // The alias first, then the buckets, then the manifest: a crash in
        // between leaves either the old state (adopted again next run) or
        // the new one naming the old slots.
        let al = alias_path(dir, tag);
        {
            use std::io::Write;
            let tmp = al.with_extension("alias.tmp");
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            f.write_all(fp16.as_bytes()).map_err(io)?;
            f.sync_all().map_err(io)?;
            std::fs::rename(&tmp, &al).map_err(io)?;
        }
        let (od, nd) = (old.with_extension("d"), dir.join(format!("{tag}.d")));
        if od.exists() {
            let _ = std::fs::remove_dir_all(&nd);
            std::fs::rename(&od, &nd).map_err(io)?;
        }
        std::fs::rename(&old, dir.join(format!("{tag}.state"))).map_err(io)?;
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        crate::progress::note(&format!(
            "sql: view {tag}: adopted the state and slot(s) an earlier release kept for this query ({fp16}); it drains on from there"
        ));
        return Ok((Some(fp16.to_string()), false));
    }
    for (name, _) in &existing {
        crate::progress::warn(&format!(
            "sql: view {tag}: replication slot {name} (kept by an earlier release for this query) is no longer used by this view \
             ({}); it holds WAL on its server until dropped: SELECT pg_drop_replication_slot('{name}') if no other view uses it",
            why.join("; ")
        ));
    }
    Ok((None, true))
}

/// Run the view: bootstrap it, or move it by the changes since its state.
pub async fn run_live(
    query: &str,
    sources: &HashMap<String, String>,
    dst_url: &str,
    table: &str,
    opts: &crate::TransferOptions,
) -> Result<crate::TransferReport> {
    let t0 = std::time::Instant::now();
    // A transaction cap the cage cannot honour is refused before anything is
    // read, a first run's bootstrap included (round P, p13).
    crate::logbased::drain::check_tx_env()?;
    let mut v = plan_view(query, sources).await?;
    let fp = v.fp_hex();
    // The view's identity names its slots and its state: the query and its
    // sources, and where it lands. By the query alone, two views of one
    // query into two tables shared a slot, and the one drained second lost
    // the changes the first had consumed, silently (W1 round P, L7).
    let tag = view_tag(&fp, dst_url, table);
    let dir = state_dir()?;
    let path = dir.join(format!("{tag}.state"));
    let sides: Vec<(Dialect, String, String)> = v.pl().sides.iter().map(|s| (s.dialect, s.url.clone(), s.table.clone())).collect();
    // The names its slots carry: its own tag's, or an adopted older one's.
    let mut alias = read_alias(&dir, &tag);
    let mut dirty = false;
    if alias.is_none() && !path.exists() {
        (alias, dirty) = adopt_old_state(&mut v, &sides, &dir, &tag, &fp[..16]).await?;
    }
    async fn feeds_of(sides: &[(Dialect, String, String)], names: &str) -> Result<Vec<Feed>> {
        let mut feeds = Vec::new();
        for (s, (d, url, t)) in sides.iter().enumerate() {
            feeds.push(match d {
                Dialect::Pg => Feed::Pg(pg_side(url, t, names, s).await?),
                Dialect::My => Feed::My(my_side(url, t).await?),
                Dialect::Other => return Err(unsupported("a mode=\"log_based\" side without a change stream")),
            });
        }
        Ok(feeds)
    }
    let mut feeds = feeds_of(&sides, alias.as_deref().unwrap_or(&tag)).await?;

    // The state, if it is the one this view left, with every slot it needs.
    let mut positions: Option<Vec<String>> = None;
    let mut tried = dirty;
    let slots_ok = feeds.iter().all(|f| match f {
        Feed::Pg(p) => p.slot_exists,
        Feed::My(_) => true,
    });
    if slots_ok && path.exists() {
        tried = true;
        match v.load(&path, None) {
            Ok(ps) if ps.len() == feeds.len() => positions = Some(ps),
            Ok(_) => eprintln!("[sql] view state {}: a position per side is missing; rebuilding", path.display()),
            Err(e) => eprintln!("[sql] {e}"),
        }
    }
    let dbg = dbg();
    let Some(from) = positions else {
        // First run, or a state that cannot be trusted.
        //
        // A first run (no state of this view in this state dir) that finds a
        // slot under the view's own name refuses instead of dropping it
        // (round P, p13). A view's ownership record is its state FILE, which
        // lives in one state dir on one host: the same query into the same
        // destination from another state dir or another host derives the same
        // tag and slot name, and dropping that slot silently threw away the
        // other view's position (it then had to rebuild). Nothing on the
        // server can tell the owner from a crashed first run, so the user
        // decides. Not the same as the CDC lane (logbased/run.rs
        // bootstrap_group), which keeps dropping an inactive slot of its own
        // name when the destination has no state: there the destination's
        // _apitap_state is the one ownership record every host shares, and a
        // live second host is refused by the lease. Do not harmonise the two.
        if !path.exists() && alias.is_none() {
            if let Some(p) = feeds.iter().find_map(|f| match f {
                Feed::Pg(p) if p.slot_exists => Some(p),
                _ => None,
            }) {
                return Err(Error::Transfer(format!(
                    "log_based view: replication slot {} already exists on its server and this state dir ({}) has no \
                     record of it: another state dir or another host may own this view. If it is a leftover of an aborted \
                     first run, drop it and run again: SELECT pg_drop_replication_slot('{}'); DROP PUBLICATION \"{}\";",
                    p.slot,
                    dir.display(),
                    p.slot,
                    p.publication
                )));
            }
        }
        let _ = std::fs::remove_file(&path);
        // A rebuild takes slots of its own tag: the adopted ones are named.
        if let Some(old) = alias.take() {
            let _ = std::fs::remove_file(alias_path(&dir, &tag));
            for f in &feeds {
                if let Feed::Pg(p) = f {
                    crate::progress::warn(&format!(
                        "sql: view {tag}: replication slot {} (adopted from an earlier release, {old}) is no longer used by this view \
                         (its state is rebuilt); it holds WAL on its server until dropped: SELECT pg_drop_replication_slot('{}') \
                         if no other view uses it",
                        p.slot, p.slot
                    ));
                }
            }
            feeds = feeds_of(&sides, &tag).await?;
            tried = true;
        }
        // A load that failed may have left part of a state: plan afresh
        // (a second schema round on both servers, so only then).
        if tried {
            v = plan_view(query, sources).await?;
        }
        let t1 = t0.elapsed().as_secs_f64();
        let splits = initial_splits(&v).await?;
        if dbg {
            eprintln!(
                "[sql] view {tag}: {} bucket(s) to start, in {:.1}s (plan and sides {t1:.1}s)",
                splits.len() + 1,
                t0.elapsed().as_secs_f64()
            );
        }
        v.begin(&path, splits.clone())?;
        let ps = match first_run(&mut v, &feeds, &tag, true, t0).await {
            // A per-key sum past int8 (on the server, or past the narrow
            // layout here): the same first run with the wide layout.
            Err(e) if e.to_string().contains("out of range") => {
                if dbg {
                    eprintln!("[sql] view {tag}: a server-side sum overflowed int8 ({e}); summing here instead");
                }
                v = plan_view(query, sources).await?;
                v.begin(&path, splits)?;
                first_run(&mut v, &feeds, &tag, false, t0).await?
            }
            r => r?,
        };
        let mut ps = ps;
        with_relids(&feeds, &mut ps);
        v.save(&path, &ps)?;
        if dbg {
            eprintln!("[sql] view {tag}: state saved, {} bucket(s), in {:.1}s", v.buckets(), t0.elapsed().as_secs_f64());
        }
        return land(v.answer()?, dst_url, table, opts).await;
    };
    let mut ps = Vec::new();
    let mut confirm = Vec::new();
    let mut total = 0u64;
    for (s, f) in feeds.iter().enumerate() {
        // p22: the table a Postgres side was built from, by relation oid; a
        // position without one (before 0.68.0) is adopted as it is.
        let (at, was) = split_relid(&from[s]);
        if let (Feed::Pg(p), Some(was)) = (f, was) {
            if was != p.relid {
                return Err(Error::Transfer(format!(
                    "log_based view: source table {} is not the table this view was built from (relation oids {was} and {}): \
                     it was dropped and recreated, and its changes in between are not in the stream. Reset the view: drop its \
                     slot ({}), remove its state file ({}) and run again",
                    p.qualified,
                    p.relid,
                    p.slot,
                    path.display()
                )));
            }
        }
        let (pos, n, open) = catch_up(&mut v, s, f, at, &path).await?;
        if dbg {
            eprintln!("[sql] view {tag}: side {s} {n} change image(s) {} -> {pos}", from[s]);
        }
        total += n;
        ps.push(pos);
        confirm.extend(open);
    }
    with_relids(&feeds, &mut ps);
    v.save(&path, &ps)?;
    let rep = land(v.answer()?, dst_url, table, opts).await?;
    for (mut ws, at) in confirm {
        ws.standby_status(at, false).await?;
        ws.stop_replication().await.ok();
    }
    if dbg {
        eprintln!(
            "[sql] view {tag}: {total} change image(s) in {:.1}s, {} bucket(s) ({} by overlay, {} rewritten)",
            t0.elapsed().as_secs_f64(),
            v.buckets(),
            v.flush_stats().0,
            v.flush_stats().1
        );
    }
    Ok(rep)
}

/// A join view's first cut of its key space into buckets: the union of both
/// sides' key ranges, in equal widths, as many as the estimate of the state
/// (half the range's keys present, one `G` and one `S` entry each) needs at
/// the bucket budget. A bucket that outgrows it later is split when saved.
async fn initial_splits(v: &View) -> Result<Vec<i64>> {
    let p = v.pl();
    let Some(k) = p.keys else { return Ok(Vec::new()) };
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    // Both sides' ranges at once (each a MIN/MAX on its own server).
    let names: Vec<String> = k.iter().map(|&(s, at)| p.sides[s].schema[p.sides[s].read[at]].name.clone()).collect();
    // A side whose key an index leads answers MIN/MAX at once; one with
    // none scans its whole table (a 10 GB lineitem: 2 s of the prep). The
    // indexed sides' range is enough: buckets are open at both ends, and
    // one that swells past the budget is cut when it is settled or saved.
    let idx = futures::future::join_all(k.iter().zip(&names).map(|(&(s, _), n)| super::key_indexed(&p.sides[s], n))).await;
    let ask: Vec<usize> = if idx.iter().any(|&b| b) { (0..k.len()).filter(|&i| idx[i]).collect() } else { (0..k.len()).collect() };
    let ranges = futures::future::try_join_all(ask.iter().map(|&i| key_range(&p.sides[k[i].0], &names[i]))).await?;
    for r in ranges {
        if let Kr::Known(a, b) = r {
            lo = lo.min(a);
            hi = hi.max(b);
        }
    }
    if lo > hi {
        return Ok(Vec::new());
    }
    let span = (hi as i128 - lo as i128 + 1) as f64;
    let per_key = (24 + 32 + 48 * p.aggs.len()) as f64;
    let n = ((span * 0.5 * per_key) / super::view::bucket_budget() as f64).ceil().clamp(1.0, 1024.0) as i64;
    let width = ((hi as i128 - lo as i128 + 1) / n as i128).max(1);
    Ok((1..n).map(|i| (lo as i128 + width * i as i128) as i64).collect())
}

async fn land(res: SqlResult, dst_url: &str, table: &str, opts: &crate::TransferOptions) -> Result<crate::TransferReport> {
    let mut o = opts.clone();
    o.mode = crate::Mode::Replace;
    let reg = crate::source::sqlresult::register(res);
    let r = crate::transfer(&reg.url, dst_url, table, &o).await;
    drop(reg);
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A view's tag names the destination too: one query into two tables (or
    /// two databases) gets two slots; credentials and options never change it.
    #[test]
    fn a_view_tag_names_its_destination() {
        let fp = "00112233445566778899aabbccddeeff";
        let a = super::view_tag(fp, "postgres://u:p@Db:5432/x?sslmode=disable", "out");
        assert_eq!(a.len(), 16);
        assert_eq!(a, super::view_tag(fp, "postgres://other:secret@db:5432/x", "out"), "credentials and options do not name it");
        assert_ne!(a, super::view_tag(fp, "postgres://u:p@db:5432/x", "out2"), "another table");
        assert_ne!(a, super::view_tag(fp, "postgres://u:p@db:5432/y", "out"), "another database");
        assert_ne!(a, super::view_tag("ff112233445566778899aabbccddeeff", "postgres://u:p@db:5432/x", "out"), "another query");
    }

    /// The text COPY decode: every cut of the stream, the same columns as
    /// the binary one; the general numeric decode as the fast one's oracle;
    /// what is not a line of the columns refused.
    #[test]
    fn bulk_text_copy_columns_decode_at_any_cut() {
        let kinds = vec![ArrowKind::Int64, ArrowKind::Decimal { p: 38, s: 2 }, ArrowKind::Int32];
        let rows: Vec<(&str, (i64, Option<i128>, Option<i32>))> = vec![
            ("1\t1234.56\t7", (1, Some(123456), Some(7))),
            ("1\t\\N\t-3", (1, None, Some(-3))),
            ("-9\t-42.00\t\\N", (-9, Some(-4200), None)),
            ("40\t0.00\t0", (40, Some(0), Some(0))),
            ("41\t123456.78\t2147483647", (41, Some(12345678), Some(i32::MAX))),
            // past the scale cut toward zero, short of it padded
            ("2\t1.239\t1", (2, Some(123), Some(1))),
            ("3\t-1.239\t1", (3, Some(-123), Some(1))),
            ("4\t5\t-2147483648", (4, Some(500), Some(i32::MIN))),
            ("5\t-0.5\t1", (5, Some(-50), Some(1))),
            // past a u64: the checked decode
            ("-9223372036854775808\t1234567890123456789012.5\t1", (i64::MIN, Some(123456789012345678901250), Some(1))),
            ("9223372036854775807\t-99999999999999999999.99\t1", (i64::MAX, Some(-9999999999999999999999), Some(1))),
        ];
        let w: Vec<u8> = rows.iter().flat_map(|(l, _)| format!("{l}\n").into_bytes()).collect();
        let want: Vec<_> = rows.iter().map(|(_, r)| *r).collect();
        for cut in 0..=w.len() {
            let mut cc = CopyCols::new(kinds.clone());
            let mut got: Vec<(i64, Option<i128>, Option<i32>)> = Vec::new();
            for part in [&w[..cut], &w[cut..]] {
                if let Some(b) = cc.feed(part).unwrap() {
                    let ok = |c: &FinishedCol, r: usize| match c {
                        FinishedCol::I64 { validity, .. } | FinishedCol::Dec128 { validity, .. } | FinishedCol::I32 { validity, .. } => {
                            validity.as_ref().is_none_or(|bm| bm[r / 8] & (1 << (r % 8)) != 0)
                        }
                        _ => panic!("kind"),
                    };
                    for r in 0..b.rows {
                        let (FinishedCol::I64 { data: k, .. }, FinishedCol::Dec128 { data: d, .. }, FinishedCol::I32 { data: i, .. }) = (&b.cols[0], &b.cols[1], &b.cols[2]) else {
                            panic!("kinds");
                        };
                        got.push((k[r], ok(&b.cols[1], r).then_some(d[r]), ok(&b.cols[2], r).then_some(i[r])));
                    }
                }
            }
            cc.finish().unwrap();
            assert_eq!(got, want, "cut at {cut}");
        }
        // A line cut short at the end is refused.
        let mut cc = CopyCols::new(kinds.clone());
        cc.feed(&w[..w.len() - 1]).unwrap();
        assert!(cc.finish().is_err());
        for line in ["1\tNaN\t3\n", "1\tInfinity\t3\n", "1\t2.00\n", "1\t2.00\t3\t4\n", "1\t2.00\t2147483648\n", "\n", "1\t-\t3\n", "1\t.\t3\n", "x\t2.00\t3\n", "1\t2.0x\t3\n"] {
            let mut cc = CopyCols::new(kinds.clone());
            assert!(cc.feed(line.as_bytes()).is_err(), "{line:?} taken");
        }
        // The fast decode against the checked one, every scale to 4.
        for f in ["0", "7", "-7", "12.5", "-12.5", "0.001", "99999999.9999", "123456789012345678", "-0.00", "3.14159"] {
            for sc in 0..=4u32 {
                let line = format!("1\t{f}\t1\n");
                let mut cc = CopyCols::new(vec![ArrowKind::Int64, ArrowKind::Decimal { p: 38, s: sc as i8 }, ArrowKind::Int32]);
                let b = cc.feed(line.as_bytes()).unwrap().unwrap();
                let FinishedCol::Dec128 { data, .. } = &b.cols[1] else { panic!("kind") };
                assert_eq!(data[0], dec_text(f.as_bytes(), sc).unwrap(), "{f} at scale {sc}");
            }
        }
    }
}

#[cfg(test)]
mod truncate_text_tests {
    #[test]
    fn a_truncate_refusal_reads_as_two_sentences() {
        let e = super::truncate_refused("bench.t", std::path::Path::new("/s/abc.state")).to_string();
        assert_eq!(
            e,
            "invalid input: sql: a TRUNCATE of bench.t under a mode=\"log_based\" view is not supported (apitap.sql() v1). To rebuild the \
             view, delete /s/abc.state and the view's replication slot (the next run names it)"
        );
    }
}
