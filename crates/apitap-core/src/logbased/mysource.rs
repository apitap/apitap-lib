//! The MySQL side of `mode="log_based"`: a binlog replica session that
//! fills the SAME [`DrainOutcome`] the Postgres drain produces, so collapse,
//! the four destination appliers and the watermark machinery never learn
//! which database the window came from.
//!
//! Two connections by design: `MyWire` is TERMINAL once it issues
//! COM_BINLOG_DUMP (it can only stream events afterwards), so schema
//! resolution — mandatory because `binlog_row_metadata=MINIMAL` omits
//! column names — runs on an ordinary sqlx pool alongside it.
//!
//! The watermark packs MySQL's (file, position) into the u64 every state
//! row, stop-line comparison and `>=` check already speaks:
//! `file_index << 32 | log_pos`. Binlog file names end in a zero-padded
//! ordinal (`binlog.000007`), so the packing is monotonic exactly when the
//! stream is — and `log_pos` is a u32 by protocol, so nothing is lost.

use crate::error::{Error, Result};
use crate::logbased::changelog::Changes;
use crate::logbased::collapse::Collapser;
use crate::logbased::replay::WindowId;
use crate::logbased::window::{Bodies, DrainOutcome, Layout};
use crate::wire::mybinlog::{self as bl, BinlogState, TableSchema};
use crate::wire::mywire::MyWire;
use crate::wire::pgoutput::{PgoMessage, Tuple};
use std::collections::HashMap;
use std::sync::Arc;

/// Pack a binlog coordinate into the pipeline's u64 watermark.
pub(crate) fn pack_pos(file: &str, pos: u32) -> u64 {
    let idx: u64 = file
        .rsplit('.')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    (idx << 32) | pos as u64
}

/// The file ordinal a packed watermark refers to.
pub(crate) fn unpack(mark: u64) -> (u64, u32) {
    (mark >> 32, (mark & 0xFFFF_FFFF) as u32)
}

/// Render a binlog file name for an ordinal, matching the server's own
/// zero-padded 6-digit convention (`binlog.000007`).
pub(crate) fn file_name(prefix: &str, idx: u64) -> String {
    format!("{prefix}.{idx:06}")
}

/// Current coordinates + the binlog file prefix, from the control pool.
pub(crate) async fn master_position(
    pool: &sqlx::MySqlPool,
) -> Result<(String, u32)> {
    // 8.4 renamed the statement; try the modern spelling first so the
    // 8.4 path never hits the removed one (see the known-hang note).
    for sql in ["SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"] {
        // Only the first two columns (File, Position) are ours. The rest of
        // the row differs per server — MySQL 5.6+ appends Executed_Gtid_Set,
        // MariaDB stops at Binlog_Ignore_DB — and binding all five turned a
        // MariaDB source into "column index out of bounds".
        match sqlx::query_as::<_, (String, u64)>(sql)
            .fetch_one(pool)
            .await
        {
            Ok(r) => return Ok((r.0, r.1 as u32)),
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("You have an error in your SQL syntax") {
                    return Err(Error::Transfer(format!("{sql}: {e}")));
                }
            }
        }
    }
    Err(Error::Transfer(
        "cannot read binlog coordinates (tried SHOW BINARY LOG STATUS and SHOW MASTER STATUS)".into(),
    ))
}

/// Is the binlog file our watermark points at still on the server?
///
/// MySQL and MariaDB purge binlogs on their own schedule
/// (`binlog_expire_logs_seconds`, `expire_logs_days`, `PURGE BINARY LOGS`), and
/// they do not care that a consumer still needs one. If a scheduled drain is
/// paused longer than that retention — a paused DAG, a long weekend, a broken
/// cron — the position we stored is simply gone. The server then answers
/// COM_BINLOG_DUMP with error 1236, whose text ("Could not find first log file
/// name in binary log index file") says nothing about what a user should do,
/// and the honest answer is that the change stream has a HOLE: the only correct
/// recovery is a fresh bootstrap, not a resume.
///
/// This is the mirror image of the Postgres risk. A Postgres slot keeps WAL
/// until it is consumed, so an abandoned consumer threatens the source's DISK.
/// MySQL keeps nothing, so an abandoned consumer threatens the DATA.
/// Report how much binlog retention this pipeline has left, every run.
///
/// The Postgres lane prints the WAL its slot is holding on every drain, so an
/// operator watching logs sees trouble coming. The MySQL lane printed nothing
/// at all until the position it needed was already purged — at which point the
/// only honest answer is a full re-bootstrap. The difference is not the
/// engine's, it is ours: MySQL will happily tell you what it still has.
///
/// The number that matters is not "how many bytes of binlog exist" — that grows
/// with write traffic and says nothing about safety. It is **how many files
/// stand between the one we must resume from and the one the server will purge
/// next**. At zero, the file we need IS the oldest the server has: one rotation
/// from a bootstrap.
///
/// Best-effort, and silent on failure. `SHOW BINARY LOGS` needs REPLICATION
/// CLIENT, and a source that withholds it must still be drainable — a
/// diagnostic that can fail a transfer is worse than no diagnostic.
pub(crate) async fn binlog_retention_report(pool: &sqlx::MySqlPool, our_file: &str) {
    use sqlx::Row;
    let Ok(rows) = sqlx::query("SHOW BINARY LOGS").fetch_all(pool).await else {
        return;
    };
    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| r.try_get::<String, _>(0).ok())
        .collect();
    // Empty means "no privilege" rather than "no logs" — the same reading
    // `binlog_file_present` makes, and the same reason not to say anything.
    if names.is_empty() {
        return;
    }
    let Some(idx) = names.iter().position(|n| n == our_file) else {
        // Not listed: either already purged (the drain's own check refuses
        // that loudly a moment later and says what to do) or the server renamed
        // the series. Either way this gauge has nothing true to report.
        return;
    };
    let total: u64 = rows
        .iter()
        .filter_map(|r| r.try_get::<u64, _>(1).ok())
        .sum();
    crate::progress::gauge(
        "binlog.retention",
        &[
            ("resume_file", our_file.to_string()),
            ("files_retained", names.len().to_string()),
            // Files between us and the purge edge. 0 = we are the edge.
            ("files_before_ours", idx.to_string()),
            ("retained_bytes", total.to_string()),
            ("at_purge_edge", (idx == 0).to_string()),
        ],
    );
    if idx == 0 && names.len() > 1 {
        crate::progress::warn(&format!(
            "binlog retention: this pipeline resumes from {our_file}, which is \
             the OLDEST binlog the server still has ({} retained). The next \
             rotation plus purge takes the position out from under it, and the \
             recovery from that is a full re-bootstrap, not a retry. Either \
             drain more often or raise the server's retention \
             (binlog_expire_logs_seconds) above the longest gap between runs.",
            names.len(),
        ));
    }
}

pub(crate) async fn binlog_file_present(pool: &sqlx::MySqlPool, file: &str) -> Result<bool> {
    // SHOW BINARY LOGS lists Log_name plus File_size (and Encrypted on newer
    // servers), so bind by name and read the first column only.
    use sqlx::Row;
    let rows = sqlx::query("SHOW BINARY LOGS")
        .fetch_all(pool)
        .await
        .map_err(|e| Error::Transfer(format!("SHOW BINARY LOGS: {e}")))?;
    // An empty listing means the privilege is missing rather than that no logs
    // exist (log_bin=ON is already checked); do not turn that into a refusal.
    if rows.is_empty() {
        return Ok(true);
    }
    Ok(rows
        .iter()
        .filter_map(|r| r.try_get::<String, _>(0).ok())
        .any(|name| name == file))
}

/// A stable 64-bit fingerprint of the server this pool is connected to.
///
/// The watermark is a `(file, position)` pair, and those numbers mean nothing
/// outside the server that issued them. Point the same URL at a different
/// server — a promoted replica, a restored backup, a DNS record moved during
/// a failover — and `binlog.000042 @ 1500` is a perfectly valid coordinate on
/// the new one, pointing at completely different changes. The existing
/// "position is AHEAD of the server" guard catches the case where the new
/// server happens to be behind; this catches the rest, which is the more
/// dangerous half because it resumes quietly.
///
/// MySQL has `@@server_uuid`, which survives restarts and is unique per
/// server. MariaDB does not, so `@@server_id` is used there — weaker (an
/// operator can set two servers to the same id) but it is what the dialect
/// offers, and a distinct id still catches the common failover.
pub(crate) async fn server_identity(pool: &sqlx::MySqlPool) -> Result<u64> {
    let raw = match sqlx::query_as::<_, (String,)>("SELECT @@server_uuid")
        .fetch_one(pool)
        .await
    {
        Ok((v,)) => format!("uuid:{v}"),
        Err(_) => {
            // Read it as text: `@@server_id` is BIGINT UNSIGNED on MariaDB and
            // BIGINT on MySQL, and binding the wrong one is a decode error, not
            // a number. The value is only ever hashed, so its spelling is all
            // this needs.
            let (id,): (String,) = sqlx::query_as("SELECT CAST(@@server_id AS CHAR)")
                .fetch_one(pool)
                .await
                .map_err(|e| Error::Transfer(format!("probe server identity: {e}")))?;
            format!("id:{id}")
        }
    };
    Ok(fnv1a(raw.as_bytes()))
}

/// FNV-1a. Explicitly written out rather than reached for from `std`, because
/// this value is WRITTEN to a destination and compared on later runs: it has
/// to mean the same thing next month, on another machine, under another
/// compiler. `DefaultHasher` promises none of that.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    // 0 is the "no marker stored" sentinel on the destination side, so the
    // one input that would collide with it is nudged.
    if h == 0 {
        1
    } else {
        h
    }
}

/// Refuse loudly the server settings that would silently corrupt a CDC
/// stream, instead of decoding garbage.
pub(crate) async fn precheck(pool: &sqlx::MySqlPool) -> Result<()> {
    let (ver,): (String,) = sqlx::query_as("SELECT VERSION()")
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Transfer(format!("probe version: {e}")))?;
    let is_mariadb = ver.to_lowercase().contains("mariadb");
    if is_mariadb {
        // MariaDB's dialect (v1 rows events, GTID-as-transaction-start,
        // ANNOTATE_ROWS frames) is decoded by the same reader — but its
        // event compression is a separate binlog encoding we do not speak,
        // and it is a MariaDB-only variable, so probe it here.
        if let Ok((_, v)) = sqlx::query_as::<_, (String, String)>(
            "SHOW VARIABLES LIKE 'log_bin_compress'",
        )
        .fetch_one(pool)
        .await
        {
            if v.eq_ignore_ascii_case("ON") {
                return Err(Error::InvalidInput(
                    "log_based: log_bin_compress=ON writes compressed binlog \
                     events — set it OFF for this source"
                        .into(),
                ));
            }
        }
    }
    let want = [
        ("log_bin", "ON", "binary logging is off — set log_bin=ON"),
        ("binlog_format", "ROW", "binlog_format must be ROW"),
        // MINIMAL/NOBLOB ship a PARTIAL after-image: the unchanged primary key
        // is omitted, so the row an UPDATE refers to cannot be identified from
        // the event alone, and unchanged columns arrive as holes rather than
        // values. Both modes silently produce wrong rows rather than an error,
        // which is exactly the failure a CDC tool must never have.
        (
            "binlog_row_image",
            "FULL",
            "binlog_row_image must be FULL — MINIMAL and NOBLOB omit the primary key \
             and unchanged columns from the after-image, which cannot be replicated \
             faithfully. SET GLOBAL binlog_row_image = 'FULL' (and restart writers so \
             their sessions pick it up)",
        ),
    ];
    for (var, expect, hint) in want {
        let (_, val): (String, String) =
            sqlx::query_as(&format!("SHOW VARIABLES LIKE '{var}'"))
                .fetch_one(pool)
                .await
                .map_err(|e| Error::Transfer(format!("probe {var}: {e}")))?;
        if !val.eq_ignore_ascii_case(expect) {
            return Err(Error::InvalidInput(format!(
                "log_based: {var}={val} — {hint}"
            )));
        }
    }
    // Row images must carry the key columns; MINIMAL only ships the PK for
    // before-images, which the collapse layer handles, but NOBLOB/PARTIAL
    // JSON diffs would arrive unreadable.
    if let Ok((_, v)) = sqlx::query_as::<_, (String, String)>(
        "SHOW VARIABLES LIKE 'binlog_row_value_options'",
    )
    .fetch_one(pool)
    .await
    {
        if !v.is_empty() {
            return Err(Error::InvalidInput(format!(
                "log_based: binlog_row_value_options={v} ships JSON diffs — set it empty"
            )));
        }
    }
    if let Ok((_, v)) = sqlx::query_as::<_, (String, String)>(
        "SHOW VARIABLES LIKE 'binlog_transaction_compression'",
    )
    .fetch_one(pool)
    .await
    {
        if v.eq_ignore_ascii_case("ON") {
            return Err(Error::InvalidInput(
                "log_based: binlog_transaction_compression=ON wraps events in \
                 compressed payloads — turn it off for this source"
                    .into(),
            ));
        }
    }
    Ok(())
}

/// Column names, PK membership and signedness for one table — the facts
/// `binlog_row_metadata=MINIMAL` leaves out.
pub(crate) async fn fetch_schema(
    pool: &sqlx::MySqlPool,
    db: &str,
    table: &str,
) -> Result<TableSchema> {
    let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_KEY AS CHAR), \
         CAST(COLUMN_TYPE AS CHAR), CAST(DATA_TYPE AS CHAR), \
         CAST(CHARACTER_SET_NAME AS CHAR) \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
    )
    .bind(db)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|e| Error::Transfer(format!("schema of {db}.{table}: {e}")))?;
    if rows.is_empty() {
        return Err(Error::InvalidInput(format!("{db}.{table} not found")));
    }
    // MySQL stores JSON as a BINARY envelope and the binlog ships that
    // envelope verbatim, while a bootstrap SELECT returns the document as
    // text. The same column would then read {"a": 1} after a full load and a
    // run of control bytes after a CDC update — measured on MySQL 8.0. Until
    // that encoding is rendered, the table is refused instead.
    //
    // The test is the catalog's own DATA_TYPE, which needs no version probe:
    // MariaDB's JSON is an alias for LONGTEXT and reports `longtext`, so only
    // a server with real binary JSON answers `json` here.
    // A string column in a non-UTF-8 charset ships its bytes RAW in the
    // binlog, while the bootstrap reads the same column through a utf8mb4
    // connection (decoded) — the destination would hold a mix of encodings
    // with no error (system review 2026-10-07, R2). Refuse and name the
    // columns; the fix is a source-side conversion.
    let bad_charset = non_utf8_string_columns(
        rows.iter().map(|r| (r.0.as_str(), r.3.as_str(), r.4.as_deref())),
    );
    if !bad_charset.is_empty() {
        return Err(Error::InvalidInput(format!(
            "log_based: {db}.{table} has string column(s) in a non-UTF-8 charset: {}. \
             The binlog carries their bytes in the column's own charset while a bootstrap \
             reads them decoded through the utf8mb4 connection, so the destination would \
             mix encodings with no error. Convert them at the source \
             (ALTER TABLE … MODIFY <col> … CHARACTER SET utf8mb4), expose the column \
             through a view that casts it, or use mode='replace'/'append' for this table.",
            bad_charset
                .iter()
                .map(|(n, cs)| format!("{n} ({cs})"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let json_cols: Vec<&str> = rows
        .iter()
        .filter(|r| r.3.eq_ignore_ascii_case("json"))
        .map(|r| r.0.as_str())
        .collect();
    if !json_cols.is_empty() {
        return Err(Error::InvalidInput(format!(
            "log_based: {db}.{table} has JSON column(s) {} and apitap cannot yet \
             render MySQL's binary JSON encoding from the binlog. A CDC update \
             would write the raw envelope where the full load wrote the document, \
             so the run refuses instead of corrupting the column. Use mode='replace' \
             or 'append' for this table, or store the document in a text column. \
             (MariaDB is unaffected — its JSON is LONGTEXT.)",
            json_cols.join(", ")
        )));
    }
    // COLUMN_KEY='PRI' marks primary-key members.
    Ok(TableSchema {
        names: rows.iter().map(|r| r.0.clone()).collect(),
        key: rows.iter().map(|r| r.1 == "PRI").collect(),
        unsigned: rows
            .iter()
            .map(|r| r.2.to_lowercase().contains("unsigned"))
            .collect(),
        // COLUMN_TYPE already carried these; the old code read the column and
        // threw the labels away, which is why a CDC update wrote "3" where the
        // bulk load had written 'shipped'.
        labels: rows.iter().map(|r| enum_set_labels(&r.2)).collect(),
    })
}

/// Pull the member list out of a catalog COLUMN_TYPE like
/// `enum('new','paid','shipped')` or `set('read','write')`. MySQL escapes an
/// embedded quote by doubling it, and the labels may contain commas, so this
/// walks the string rather than splitting on punctuation.
fn enum_set_labels(column_type: &str) -> Option<std::sync::Arc<Vec<String>>> {
    let lower = column_type.trim_start().to_ascii_lowercase();
    if !(lower.starts_with("enum(") || lower.starts_with("set(")) {
        return None;
    }
    let body = column_type
        .find('(')
        .and_then(|i| column_type.rfind(')').map(|j| &column_type[i + 1..j]))?;
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if in_quote && chars.peek() == Some(&'\'') => {
                chars.next();
                cur.push('\'');
            }
            '\'' => {
                if in_quote {
                    out.push(std::mem::take(&mut cur));
                }
                in_quote = !in_quote;
            }
            c if in_quote => cur.push(c),
            _ => {}
        }
    }
    Some(std::sync::Arc::new(out))
}

/// Per-session decode state that outlives one window (TABLE_MAP ids and
/// resolved schemas are announced per event group, but caching them across
/// windows keeps the second window from re-fetching information_schema).
#[derive(Default)]
pub(crate) struct MySession {
    pub st: BinlogState,
    /// The binlog file the stream is currently in.
    pub file: String,
    /// Tables we care about, as "db.table" — everything else is skipped
    /// before it is ever decoded — with the run's key columns for each.
    pub tracked: HashMap<String, Vec<String>>,
    /// Column layout per tracked "db.table", built from `st.schemas` and the
    /// run's keys; every body a window builds carries the one it was built
    /// with. Cleared in the same statement as the schemas, at every DDL.
    pub layouts: HashMap<String, Arc<Layout>>,
}

/// The layout of tracked table `q`: cached, else from the schema cache, else
/// from information_schema. The TABLE_MAP arm and the TRUNCATE arm both ask
/// here, so a table's first op of a window always finds one — a TRUNCATE is
/// often the only event a table has in its window, and it carries no columns.
async fn layout_for(sess: &mut MySession, pool: &sqlx::MySqlPool, q: &str) -> Result<Arc<Layout>> {
    if let Some(l) = sess.layouts.get(q) {
        return Ok(l.clone());
    }
    let (db, tb) = q
        .split_once('.')
        .ok_or_else(|| Error::Transfer(format!("log_based: {q} is not db.table")))?;
    if !sess.st.schemas.contains_key(q) {
        let sc = fetch_schema(pool, db, tb).await?;
        sess.st.schemas.insert(q.to_string(), sc);
    }
    let keys = sess
        .tracked
        .get(q)
        .ok_or_else(|| Error::Transfer(format!("log_based: {q} is not tracked")))?;
    let l = Arc::new(Layout::from_mysql(q, &sess.st.schemas[q], keys)?);
    sess.layouts.insert(q.to_string(), l.clone());
    Ok(l)
}

/// One drain window off a live binlog stream.
///
/// Mirrors `logbased::drain::drain`: buffer a transaction's ops, flush them
/// into per-table collapsers at XID (the commit boundary), stop only at a
/// commit — at the stop-line, the byte budget, or the wall clock.
#[allow(clippy::too_many_arguments)]
/// How often the stream asks the server for a heartbeat. Mirrored into
/// `binlog_dump`'s argument by `myrun`, and used here to reason about silence.
pub(crate) const HEARTBEAT_SECS: u64 = 5;

/// How long a read may see NOTHING — not an event, not a heartbeat — before the
/// peer is declared gone. Six heartbeats: wide enough that a loaded server or a
/// brief network stall never trips it, narrow enough that a scheduled run fails
/// in seconds instead of hanging until someone notices.
const EVENT_SILENCE_LIMIT: std::time::Duration = std::time::Duration::from_secs(HEARTBEAT_SECS * 6);

pub(crate) async fn drain_binlog(
    w: &mut MyWire,
    pool: &sqlx::MySqlPool,
    sess: &mut MySession,
    start: u64,
    stop_line: u64,
    max_secs: u64,
    max_buf_bytes: usize,
    changelog: bool,
) -> Result<DrainOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
    // changelog=true captures every operation verbatim instead of collapsing
    // the window; the layout's key is what lets a PK-changing update emit
    // D-then-U exactly like the Postgres lane does.
    let mut changelogs: HashMap<String, Changes> = HashMap::new();
    let mut tx_buf: Vec<(Arc<str>, TxOp)> = Vec::new();
    let mut end_mark = start;
    let mut buf_bytes = 0usize;
    let mut hit_budget = false;

    loop {
        if std::time::Instant::now() > deadline || crate::shutdown::requested() {
            // Wall-clock stop, or a SIGTERM. Either way the position this
            // window reports is the last COMPLETE transaction's, so the
            // window applies whole and the watermark that follows is true.
            break;
        }
        // A bound on the READ itself, not just between reads. The loop's
        // wall-clock deadline and the SIGTERM flag above are both checked
        // between events, so neither can end a run that is parked inside the
        // await — and a half-open socket (NAT reaped the flow, the host lost
        // power) produces exactly that park: no bytes, no EOF, no error, ever.
        //
        // The number is not arbitrary. `binlog_dump` asks the server for a
        // heartbeat every HEARTBEAT_SECS, so on a healthy connection something
        // arrives at that cadence even when nothing is happening on the source.
        // Silence for many multiples of it is not a quiet database, it is a
        // dead peer — and saying so is the whole value of the heartbeat we
        // already pay for.
        //
        // TCP keepalive (see `wire::mywire::set_keepalive`) usually notices
        // first; this is the backstop for the cases it cannot cover, and it
        // does not depend on the kernel honouring a socket option.
        let raw = tokio::time::timeout(EVENT_SILENCE_LIMIT, w.next_binlog_event())
            .await
            .map_err(|_| {
                Error::Transfer(format!(
                    "mysql binlog: no event and no heartbeat for {}s — the \
                     stream asks the server for a heartbeat every {}s, so this \
                     is a peer that is gone rather than a source that is quiet \
                     (half-open socket: NAT or firewall reaped the flow, or the \
                     host vanished without closing). Re-run: the drain resumes \
                     from its stored position.",
                    EVENT_SILENCE_LIMIT.as_secs(),
                    HEARTBEAT_SECS,
                ))
            })??;
        let Some(raw) = raw else {
            break;
        };
        // `raw` is an owned Bytes now — the old borrowed-slice shape forced a
        // full copy of every event here.
        let (h, body) = bl::split_event(&raw, true)?;

        match h.event_type {
            bl::TYPE_ROTATE => {
                let (_, name) = bl::parse_rotate(body)?;
                // Artificial rotate (ts=0) only restates where we are.
                if !name.is_empty() {
                    sess.file = name;
                }
            }
            bl::TYPE_FDE => {}
            bl::TYPE_TX_PAYLOAD => {
                return Err(Error::InvalidInput(
                    "log_based: compressed transaction payload — set \
                     binlog_transaction_compression=OFF"
                        .into(),
                ))
            }
            bl::TYPE_TABLE_MAP => {
                let map = bl::parse_table_map(body)?;
                let q = format!("{}.{}", map.db, map.table);
                if sess.tracked.contains_key(&q) {
                    if !sess.st.schemas.contains_key(&q) {
                        let sc = fetch_schema(pool, &map.db, &map.table).await?;
                        sess.st.schemas.insert(q.clone(), sc);
                    }
                    // Signedness and ENUM/SET members come from
                    // information_schema (binlog metadata omits both) — stamp
                    // them onto the column defs by POSITION.
                    let mut map = map;
                    if let Some(sc) = sess.st.schemas.get(&q) {
                        // Position is the ONLY link between the two lists, so
                        // a length mismatch means they describe different
                        // shapes of the same table: the catalog is read now,
                        // the event describes the table as it was when the
                        // change happened, and a DDL landed between them. A
                        // column dropped from the middle shifts every position
                        // after it, so stamping one column's signedness or
                        // members onto another is silent, wrong, and limited
                        // to the columns past the change — the hardest kind of
                        // corruption to notice.
                        //
                        // Re-running does NOT help: the same events replay
                        // against the same catalog. The only correct recovery
                        // is to bootstrap this table again from the schema it
                        // has now, which is why the message says so.
                        if sc.names.len() != map.cols.len() {
                            return Err(Error::InvalidInput(format!(
                                "log_based: {q} has {} columns in the source catalog \
                                 but the binlog events in this window describe {} — \
                                 the table's definition changed while these changes \
                                 were being written, and column positions are the only \
                                 way the two are matched. apitap will not guess which \
                                 column is which. Recovery: clear this table's apitap \
                                 state on the destination and re-run, which bootstraps \
                                 it from the schema it has now. Re-running as-is \
                                 replays the same events against the same catalog and \
                                 stops here again.",
                                sc.names.len(),
                                map.cols.len()
                            )));
                        }
                        for (i, c) in map.cols.iter_mut().enumerate() {
                            c.unsigned = sc.unsigned.get(i).copied().unwrap_or(false);
                            c.labels = sc.labels.get(i).cloned().flatten();
                        }
                    }
                    // Registered only with its layout: a rows event can only
                    // decode against a map, so every op of a table reaches
                    // `drain_tx` with a layout for it.
                    layout_for(sess, pool, &q).await?;
                    sess.st.maps.insert(map.table_id, map);
                }
            }
            t if bl::is_rows(t) => {
                // Cheap skip for untracked tables: the TABLE_MAP was never
                // registered, so there is nothing to decode against.
                let table_id = {
                    let mut b = [0u8; 8];
                    b[..6].copy_from_slice(&body[..6]);
                    u64::from_le_bytes(b)
                };
                let Some(map) = sess.st.maps.get(&table_id).cloned() else {
                    continue;
                };
                let q = format!("{}.{}", map.db, map.table);
                let ev = bl::parse_rows(body, t, &map)?;
                for msg in bl::to_messages(&mut sess.st, t, ev)? {
                    match msg {
                        // The layout came from the TABLE_MAP; bodies are built
                        // at the commit, in `drain_tx`.
                        PgoMessage::Relation(_) => {}
                        PgoMessage::Insert { new, .. } => {
                            buf_bytes += cells_bytes(&new);
                            tx_buf.push((Arc::from(q.as_str()), TxOp::Insert(new)));
                        }
                        PgoMessage::Update { old, new, .. } => {
                            // BOTH images: a changelog keeps the old one too (a
                            // PK change emits a D carrying it), so charging only
                            // the new image let the window run to roughly twice
                            // the budget before `hit_budget` noticed.
                            buf_bytes += cells_bytes(&new)
                                + old.as_ref().map_or(0, |o| cells_bytes(&o.tuple));
                            tx_buf.push((
                                Arc::from(q.as_str()),
                                TxOp::Update(old.map(|o| o.tuple), new),
                            ));
                        }
                        PgoMessage::Delete { old, .. } => {
                            buf_bytes += cells_bytes(&old.tuple);
                            tx_buf.push((Arc::from(q.as_str()), TxOp::Delete(old.tuple)));
                        }
                        _ => {}
                    }
                }
            }
            bl::TYPE_QUERY => {
                let (db, sql) = bl::parse_query(body)?;
                let head = sql.trim_start();
                let verb = query_verb(head);
                if verb == QueryVerb::Begin {
                    // Transaction opens: anything buffered from a torn
                    // previous attempt is stale.
                    tx_buf.clear();
                } else if verb == QueryVerb::Commit {
                    // A non-transactional engine (MyISAM, Aria, MEMORY) commits
                    // with an explicit `COMMIT` QUERY event and no XID: the
                    // buffered ops are real and the watermark advances past this
                    // event, exactly as for TYPE_XID. Without this arm MariaDB's
                    // next GTID event cleared the buffer before it was drained
                    // (the rows vanished silently) and MySQL held them until the
                    // next XID (system review 2026-10-07, R1).
                    drain_tx(&mut tx_buf, changelog, &sess.layouts, &mut changelogs, &mut collapsers)?;
                    end_mark = pack_pos(&sess.file, h.log_pos);
                    if end_mark >= stop_line {
                        break;
                    }
                    if buf_bytes >= max_buf_bytes {
                        hit_budget = true;
                        break;
                    }
                } else if verb == QueryVerb::Ddl {
                    // Anything already buffered belongs to the transaction this
                    // DDL implicitly committed, so it lands FIRST, under the
                    // layouts it was decoded against.
                    drain_tx(&mut tx_buf, changelog, &sess.layouts, &mut changelogs, &mut collapsers)?;
                    // A DDL may change any table's columns, and the QUERY
                    // event's `db` is the session's default database, not
                    // necessarily the altered table's (`ALTER TABLE other.t`),
                    // so every cached layout goes — with the schemas they were
                    // built from, in the same statement, so no layout can
                    // outlive the catalog read it came from.
                    sess.st.schemas.clear();
                    sess.layouts.clear();
                    sess.st.maps.clear();
                    // …and a TRUNCATE is not only a schema event: it empties the
                    // table, and the destination has to hear about it. MySQL
                    // writes it as a QUERY, so there is no rows event and no XID
                    // — it is DDL, it auto-commits, and it is its own commit
                    // boundary. Apply it here or it is lost, which is exactly
                    // what happened until 0.56.0.
                    if starts_with_word(head, "truncate") {
                        match truncate_target(head, &db) {
                            // `sess.tracked` is the authority on what this run
                            // follows. The first draft asked `collapsers` instead
                            // — and those are populated lazily, by the first ROWS
                            // event for a table. A window shaped
                            // `TRUNCATE t; INSERT …` puts the truncate BEFORE any
                            // rows event, so the map was still empty and the
                            // truncate was skipped exactly as before the fix. The
                            // unit test passed; the e2e leg caught it.
                            Some(t) if sess.tracked.contains_key(&t) => {
                                // The truncate needs the table's layout like any
                                // op: often it is the table's only event in the
                                // window, and it carries no columns. 0.56.0 had
                                // none to give it, and every apply of such a
                                // window failed for want of a column list.
                                layout_for(sess, pool, &t).await?;
                                tx_buf.push((Arc::from(t.as_str()), TxOp::Truncate));
                                drain_tx(&mut tx_buf, changelog, &sess.layouts, &mut changelogs, &mut collapsers)?;
                            }
                            // A truncate of a table this run does not track is
                            // none of our business.
                            Some(_) => {}
                            // A TRUNCATE we cannot read is refused, not skipped.
                            // Skipping is the bug; a second spelling of it would
                            // be no improvement, and this lane's standing rule is
                            // that an event it does not understand stops the run.
                            None => {
                                return Err(Error::Transfer(format!(
                                    "binlog: a TRUNCATE was seen but its table could not be \
                                     read from the statement: {head:?}. Refusing rather than \
                                     skipping it — a dropped TRUNCATE leaves the destination \
                                     holding rows the source no longer has, silently. Please \
                                     report the statement text."
                                )));
                            }
                        }
                    }
                    // The DDL is its own commit boundary, so a window can end
                    // right after it — and one holding a body must: its bodies
                    // were built with layouts this DDL may just have changed,
                    // and one window never spans a layout change. The next
                    // window starts at the next event, under fresh layouts. At
                    // the stop-line there is no next window to ask for.
                    if !collapsers.is_empty() || !changelogs.is_empty() {
                        end_mark = pack_pos(&sess.file, h.log_pos);
                        hit_budget = end_mark < stop_line;
                        break;
                    }
                }
            }
            bl::TYPE_XID => {
                // Commit boundary: the buffered ops become real, and the
                // watermark advances to the position AFTER this event.
                drain_tx(&mut tx_buf, changelog, &sess.layouts, &mut changelogs, &mut collapsers)?;
                end_mark = pack_pos(&sess.file, h.log_pos);
                if end_mark >= stop_line {
                    break;
                }
                if buf_bytes >= max_buf_bytes {
                    hit_budget = true;
                    break;
                }
            }
            t if bl::is_tx_start(t) => {
                // MariaDB opens a transaction with a GTID event — there is no
                // `BEGIN` QUERY event to clear a torn attempt's leftovers.
                tx_buf.clear();
            }
            t if bl::skippable(t) => {
                // Heartbeats carry the live position: they let an idle
                // stream reach the stop-line (and the deadline check above).
                if h.log_pos > 0 && tx_buf.is_empty() {
                    let m = pack_pos(&sess.file, h.log_pos);
                    if m > end_mark {
                        end_mark = m;
                    }
                    if end_mark >= stop_line {
                        break;
                    }
                }
            }
            // Anything left is REFUSED, never skipped: the old silent arm is
            // what made MariaDB sources apply zero changes without an error.
            t => return Err(bl::unhandled(t)),
        }
    }

    Ok(DrainOutcome {
        // The MySQL lane is text-native end to end; RowBinary bodies are a
        // Postgres-source feature (P4).
        binary: false,
        bodies: Bodies::seal(changelog, collapsers, changelogs)?,
        id: WindowId::new(start, end_mark),
        hit_budget,
    })
}

enum TxOp {
    Insert(Tuple),
    Update(Option<Tuple>, Tuple),
    Delete(Tuple),
    /// MySQL writes `TRUNCATE TABLE` into the binlog as a QUERY event, not as a
    /// rows event — so until 0.56.0 it was parsed, recognised as DDL, used to
    /// invalidate the schema cache, and then dropped. The window never carried
    /// `truncate`, no destination ever emptied the table, and the run reported
    /// success over a destination that kept every row the source had discarded.
    Truncate,
}

fn cells_bytes(row: &Tuple) -> usize {
    std::iter::once(row.frame.len() + row.cells.len() * 12)
        .sum::<usize>()
        + 48
}

/// Apply one transaction's buffered ops to whichever accumulator this run uses.
///
/// Extracted so the XID commit boundary and the TRUNCATE one cannot drift: a
/// `TRUNCATE` is DDL and auto-commits, so it is its own boundary and never
/// reaches an XID event. The first draft of the truncate fix pushed onto
/// `tx_buf` and relied on XID to drain it, which meant the record sat in the
/// buffer until the NEXT ordinary transaction committed — or was cleared by the
/// next `BEGIN`, losing it again.
///
/// A table's first op of the window builds its body with the table's layout,
/// whatever the op is. Until 0.57.0 the replica body was built by the first
/// ROWS event, so an op arriving before one — a TRUNCATE — found no body and
/// was skipped (`continue`), or was parked and wiped at the window's end into
/// Charsets whose bytes are UTF-8 (or a strict subset), so a binlog string can
/// be stored as text as-is. `utf8` is utf8mb3's alias.
fn charset_is_utf8_compatible(cs: &str) -> bool {
    matches!(cs.to_ascii_lowercase().as_str(), "utf8" | "utf8mb3" | "utf8mb4" | "ascii")
}

/// String-typed columns whose charset is not UTF-8-compatible, in catalog
/// order: `(name, charset)`. Input rows are `(name, data_type, charset)`.
fn non_utf8_string_columns<'a>(
    rows: impl Iterator<Item = (&'a str, &'a str, Option<&'a str>)>,
) -> Vec<(String, String)> {
    const STRING_TYPES: &[&str] =
        &["char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set"];
    rows.filter(|(_, dt, cs)| {
        STRING_TYPES.iter().any(|t| dt.eq_ignore_ascii_case(t))
            && cs.is_some_and(|c| !charset_is_utf8_compatible(c))
    })
    .map(|(n, _, cs)| (n.to_string(), cs.unwrap_or_default().to_string()))
    .collect()
}

/// a body with no layout that every apply refused. An op with no layout at all
/// is a broken decoder invariant (a map is registered only with its layout,
/// and the TRUNCATE arm resolves one first), and is an error, never a skip.
fn drain_tx(
    tx_buf: &mut Vec<(Arc<str>, TxOp)>,
    changelog: bool,
    layouts: &HashMap<String, Arc<Layout>>,
    changelogs: &mut HashMap<String, Changes>,
    collapsers: &mut HashMap<String, Collapser>,
) -> Result<()> {
    let layout_of = |t: &str| {
        layouts.get(t).cloned().ok_or_else(|| {
            Error::Transfer(format!("log_based: no column layout for {t} — decoder invariant broken"))
        })
    };
    for (table, op) in tx_buf.drain(..) {
        if changelog {
            if !changelogs.contains_key(table.as_ref()) {
                changelogs.insert(table.to_string(), Changes::new(layout_of(&table)?));
            }
            let c = changelogs.get_mut(table.as_ref()).expect("changelog just ensured");
            match op {
                TxOp::Insert(row) => c.insert(row),
                TxOp::Update(old, row) => c.update(old.as_ref(), row),
                TxOp::Delete(old) => c.delete(old),
                TxOp::Truncate => c.truncate(),
            }
            continue;
        }
        if !collapsers.contains_key(table.as_ref()) {
            collapsers.insert(table.to_string(), Collapser::new(layout_of(&table)?));
        }
        let c = collapsers.get_mut(table.as_ref()).expect("collapser just ensured");
        match op {
            TxOp::Insert(row) => c.insert(row)?,
            TxOp::Update(old, row) => c.update(old.as_ref(), row)?,
            TxOp::Delete(old) => c.delete(&old)?,
            TxOp::Truncate => c.truncate(),
        }
    }
    Ok(())
}

/// The table a `TRUNCATE` statement names, qualified the way the rows-event
/// path qualifies its own (`db.table`), or `None` if this is not a TRUNCATE.
///
/// Deliberately strict about the shapes it accepts. An unrecognised TRUNCATE
/// must not read as "not a truncate" — the caller turns a None-on-a-truncate
/// into a loud refusal, because silently continuing is the defect being fixed
/// and a second spelling of it would be no better than the first.
fn truncate_target(sql: &str, db: &str) -> Option<String> {
    let s = sql.trim_start();
    let rest = s.get(..8).filter(|h| h.eq_ignore_ascii_case("truncate"))?;
    let _ = rest;
    let mut rest = s[8..].trim_start();
    // The TABLE keyword is optional in MySQL.
    if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("table") {
        rest = rest[5..].trim_start();
    }
    // `db`.`t`, db.t, `t`, t — stop at whitespace or a statement terminator.
    let name: String = rest
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != ';')
        .filter(|c| *c != '`' && *c != '"')
        .collect();
    if name.is_empty() {
        return None;
    }
    Some(if name.contains('.') {
        name
    } else {
        format!("{db}.{name}")
    })
}

/// Case-insensitive WORD-prefix test: `s` starts with `w` and the following
/// character (if any) is not alphanumeric/underscore. The old spellings
/// (`s[..k.len()].eq_ignore_ascii_case(k)`) compared a fixed slice, which
/// could panic on a non-ASCII byte boundary — and the `begin` check even
/// compared SIX bytes to a five-letter word, so a transaction-open never
/// matched and a torn attempt's leftovers were never cleared.
fn starts_with_word(s: &str, w: &str) -> bool {
    s.get(..w.len()).is_some_and(|p| p.eq_ignore_ascii_case(w))
        && s[w.len()..]
            .chars()
            .next()
            .map_or(true, |c| !c.is_alphanumeric() && c != '_')
}

/// What a binlog QUERY event's statement means to the drain loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryVerb {
    Begin,
    Commit,
    Ddl,
    Other,
}

/// Classify the QUERY statement. `COMMIT` matters because a non-transactional
/// engine (MyISAM, Aria, MEMORY) ends its implicit transaction with an
/// explicit `COMMIT` QUERY event and no XID — treating it as `Other` left the
/// buffered rows to be cleared by the NEXT transaction's GTID event (MariaDB:
/// silently lost) or held until the next XID (MySQL). See system review
/// 2026-10-07, R1.
fn query_verb(head: &str) -> QueryVerb {
    if starts_with_word(head, "begin") {
        QueryVerb::Begin
    } else if starts_with_word(head, "commit") {
        QueryVerb::Commit
    } else if is_ddl(head) {
        QueryVerb::Ddl
    } else {
        QueryVerb::Other
    }
}

fn is_ddl(sql: &str) -> bool {
    let s = sql.trim_start();
    ["alter", "create", "drop", "rename", "truncate"]
        .iter()
        .any(|k| starts_with_word(s, k))
}

#[cfg(test)]
mod charset_tests {
    use super::{charset_is_utf8_compatible, non_utf8_string_columns};

    /// A string column in a non-UTF-8 charset ships raw bytes in the binlog
    /// while the bootstrap decodes it through a utf8mb4 connection; the CDC
    /// precheck must refuse it and name it (system review 2026-10-07, R2).
    #[test]
    fn only_utf8_compatible_charsets_pass_the_cdc_precheck() {
        assert!(charset_is_utf8_compatible("utf8mb4"));
        assert!(charset_is_utf8_compatible("UTF8"));
        assert!(charset_is_utf8_compatible("utf8mb3"));
        assert!(charset_is_utf8_compatible("ascii"));
        assert!(!charset_is_utf8_compatible("latin1"));
        assert!(!charset_is_utf8_compatible("ucs2"));

        let rows = [
            ("a", "varchar", Some("latin1")),
            ("b", "int", Some("latin1")), // not a string type
            ("c", "text", Some("utf8mb4")),
            ("d", "enum", Some("latin1")),
            ("e", "varchar", None), // no charset reported: not a string column
            ("f", "varchar", Some("ascii")),
        ];
        let bad = non_utf8_string_columns(rows.into_iter());
        assert_eq!(
            bad,
            vec![("a".to_string(), "latin1".to_string()), ("d".to_string(), "latin1".to_string())]
        );
    }
}

#[cfg(test)]
mod query_verbs_tests {
    use super::{is_ddl, query_verb, starts_with_word, QueryVerb};

    /// The old `begin` check compared SIX bytes to a five-letter word (so a
    /// transaction-open never matched) and `is_ddl` sliced fixed prefixes (a
    /// panic on a non-ASCII boundary). The classifier must be
    /// case-insensitive, word-bounded, and total — and `COMMIT` must be its
    /// own verb, because a non-transactional engine commits through it with
    /// no XID (system review 2026-10-07, R1).
    #[test]
    fn query_verbs_are_classified_by_word_not_by_fixed_slices() {
        assert_eq!(query_verb("BEGIN"), QueryVerb::Begin);
        assert_eq!(query_verb("begin;"), QueryVerb::Begin);
        assert_eq!(query_verb("start transaction"), QueryVerb::Other);
        assert_eq!(query_verb("COMMIT"), QueryVerb::Commit);
        assert_eq!(query_verb("commit /*x*/"), QueryVerb::Commit);
        assert_eq!(query_verb("COMMITMENT"), QueryVerb::Other);
        assert_eq!(query_verb("TRUNCATE TABLE t"), QueryVerb::Ddl);
        assert_eq!(query_verb("create table t (id int)"), QueryVerb::Ddl);
        assert_eq!(query_verb("/* leading */ TRUNCATE t"), QueryVerb::Other);
        // A byte at the old fixed-slice boundary that is not a char boundary:
        // the fixed-slice spellings panicked here; the classifier must not.
        assert_eq!(query_verb("12345é"), QueryVerb::Other);
        assert!(starts_with_word("truncate t", "truncate"));
        assert!(!starts_with_word("truncated", "truncate"));
        assert!(!starts_with_word("beginning", "begin"));
        assert!(is_ddl("drop table t"));
        assert!(!is_ddl("commit"));
    }
}

#[cfg(test)]
mod truncate_tests {
    use super::*;
    use crate::wire::pgoutput::Cell;

    /// MySQL writes TRUNCATE as a QUERY event, so the table name has to come out
    /// of the statement text. These are the spellings a server actually emits —
    /// the binlog carries what the client wrote, so all of them occur.
    #[test]
    fn a_truncate_names_its_table_in_every_spelling_mysql_emits() {
        for (sql, want) in [
            ("TRUNCATE TABLE t", "bench.t"),
            ("truncate table t", "bench.t"),
            ("TRUNCATE t", "bench.t"),
            ("TRUNCATE TABLE `t`", "bench.t"),
            ("TRUNCATE TABLE `bench`.`t`", "bench.t"),
            ("TRUNCATE TABLE other.t", "other.t"),
            ("  TRUNCATE   TABLE   t ;", "bench.t"),
        ] {
            assert_eq!(truncate_target(sql, "bench").as_deref(), Some(want), "{sql}");
        }
        // Not a truncate at all.
        for sql in ["DROP TABLE t", "ALTER TABLE t ADD c INT", "INSERT INTO t VALUES (1)"] {
            assert_eq!(truncate_target(sql, "bench"), None, "{sql}");
        }
        // A truncate whose target cannot be read returns None — the caller turns
        // that into a refusal, never a skip.
        assert_eq!(truncate_target("TRUNCATE TABLE", "bench"), None);
        assert_eq!(truncate_target("TRUNCATE", "bench"), None);
    }

    /// A2. A window holding a table's TRUNCATE and nothing else of it: the
    /// op builds the table's body itself, on both lanes, and the sealed window
    /// carries the table's layout. 0.56.0's replica lane found no collapser (the
    /// first ROWS event built it) and skipped the op, or parked it and wiped
    /// into a body with no layout at the window's end; every apply then failed
    /// for want of a column list, on every run (audit §3.4).
    #[test]
    fn drain_tx_truncate_only_builds_window() {
        use std::collections::HashMap;
        let l = Layout::for_test(&["id", "v"], &[], &["id"]);
        let layouts: HashMap<String, Arc<Layout>> = [("bench.t".to_string(), l.clone())].into_iter().collect();
        let key: Arc<str> = Arc::from("bench.t");

        // replica lane: no body yet, only the truncate
        let mut collapsers: HashMap<String, Collapser> = HashMap::new();
        let mut buf = vec![(key.clone(), TxOp::Truncate)];
        drain_tx(&mut buf, false, &layouts, &mut HashMap::new(), &mut collapsers).expect("drain");
        let w = collapsers.remove("bench.t").expect("the truncate built the table's body").seal("bench.t").unwrap();
        assert!(w.body().truncate, "the replica window must carry the truncate");
        assert_eq!(w.layout(), &*l, "…and the table's layout");

        // changelog lane: one T record, and the layout
        let mut changelogs: HashMap<String, Changes> = HashMap::new();
        let mut buf = vec![(key.clone(), TxOp::Truncate)];
        drain_tx(&mut buf, true, &layouts, &mut changelogs, &mut HashMap::new()).expect("drain");
        let w = changelogs.remove("bench.t").expect("the truncate built the table's body").seal("bench.t").unwrap();
        let ops: Vec<_> = w.body().events.iter().map(|e| e.op).collect();
        assert_eq!(ops, [crate::logbased::changelog::ChangeOp::Truncate]);
        assert_eq!(w.layout(), &*l);

        // An op of a table with no layout is a broken invariant, and says so:
        // 0.56.0 dropped it without a word.
        for changelog in [false, true] {
            let cell = |s: &str| Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()));
            let mut buf = vec![(Arc::from("bench.other"), TxOp::Insert(Tuple::from_cells(&[cell("1"), cell("x")])))];
            let e = drain_tx(&mut buf, changelog, &layouts, &mut HashMap::new(), &mut HashMap::new())
                .expect_err("an op with no layout");
            assert!(e.to_string().contains("bench.other"), "{e}");
        }
    }

    /// The truncate lands IN ORDER with the rows around it: rows before it are
    /// wiped with the table, rows after it survive.
    #[test]
    fn a_truncate_lands_between_the_rows_around_it() {
        use std::collections::HashMap;
        let cell = |s: &str| Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()));
        let ins = |k: &str| TxOp::Insert(Tuple::from_cells(&[cell(k), cell("x")]));
        let l = Layout::for_test(&["id", "v"], &[], &["id"]);
        let layouts: HashMap<String, Arc<Layout>> = [("bench.t".to_string(), l)].into_iter().collect();
        let key: Arc<str> = Arc::from("bench.t");
        let mut collapsers: HashMap<String, Collapser> = HashMap::new();
        let mut buf = vec![(key.clone(), ins("1")), (key.clone(), TxOp::Truncate), (key, ins("2"))];
        drain_tx(&mut buf, false, &layouts, &mut HashMap::new(), &mut collapsers).expect("drain");
        let w = collapsers.remove("bench.t").unwrap().seal("bench.t").unwrap();
        assert!(w.body().truncate);
        assert_eq!(w.body().upserts.iter().map(|t| t.to_cells()[0].clone()).collect::<Vec<_>>(), [cell("2")]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermarks_pack_monotonically_across_files() {
        let a = pack_pos("binlog.000007", 4);
        let b = pack_pos("binlog.000007", 900);
        let c = pack_pos("binlog.000008", 4);
        assert!(a < b && b < c, "{a} {b} {c}");
        assert_eq!(unpack(b), (7, 900));
        // A 4 GiB-1 position still fits beside the file ordinal.
        let big = pack_pos("binlog.000009", u32::MAX);
        assert_eq!(unpack(big), (9, u32::MAX));
        assert!(big > c);
        assert_eq!(file_name("binlog", 9), "binlog.000009");
    }

    #[test]
    fn ddl_is_recognised_but_dml_and_begin_are_not() {
        for s in ["ALTER TABLE t ADD c INT", "create table x(i int)", "  DROP TABLE t", "TRUNCATE t"] {
            assert!(is_ddl(s), "{s}");
        }
        for s in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT", "SAVEPOINT s"] {
            assert!(!is_ddl(s), "{s}");
        }
    }
}
