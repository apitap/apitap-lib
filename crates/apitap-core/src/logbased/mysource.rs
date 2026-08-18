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
use crate::logbased::collapse::{Collapsed, Collapser};
use crate::logbased::drain::DrainOutcome;
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
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_KEY AS CHAR), \
         CAST(COLUMN_TYPE AS CHAR), CAST(DATA_TYPE AS CHAR) \
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
    /// before it is ever decoded.
    pub tracked: HashMap<String, Vec<String>>,
}

/// One drain window off a live binlog stream.
///
/// Mirrors `logbased::drain::drain`: buffer a transaction's ops, flush them
/// into per-table collapsers at XID (the commit boundary), stop only at a
/// commit — at the stop-line, the byte budget, or the wall clock.
#[allow(clippy::too_many_arguments)]
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
    // the window; `key_idx` is what lets a PK-changing update emit D-then-U
    // exactly like the Postgres lane does.
    let mut changelogs: HashMap<String, Changes> = HashMap::new();
    let mut key_idx: HashMap<String, Vec<usize>> = HashMap::new();
    let mut wal_cols: HashMap<String, Vec<String>> = HashMap::new();
    let mut wal_oids: HashMap<String, Vec<u32>> = HashMap::new();
    let mut tx_buf: Vec<(Arc<str>, TxOp)> = Vec::new();
    let mut end_mark = start;
    let mut buf_bytes = 0usize;
    let mut hit_budget = false;

    loop {
        if std::time::Instant::now() > deadline {
            break;
        }
        let Some(raw) = w.next_binlog_event().await? else {
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
                    // Signedness comes from information_schema (MINIMAL
                    // metadata omits it) — stamp it onto the column defs.
                    let mut map = map;
                    if let Some(sc) = sess.st.schemas.get(&q) {
                        for (i, c) in map.cols.iter_mut().enumerate() {
                            c.unsigned = sc.unsigned.get(i).copied().unwrap_or(false);
                            c.labels = sc.labels.get(i).cloned().flatten();
                        }
                    }
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
                        PgoMessage::Relation(r) => {
                            let names: Vec<String> =
                                r.cols.iter().map(|c| c.name.clone()).collect();
                            wal_oids.insert(q.clone(), vec![0; names.len()]);
                            wal_cols.insert(q.clone(), names);
                            let idx: Vec<usize> = r
                                .cols
                                .iter()
                                .enumerate()
                                .filter(|(_, c)| c.key)
                                .map(|(i, _)| i)
                                .collect();
                            key_idx.entry(q.clone()).or_insert_with(|| idx.clone());
                            collapsers
                                .entry(q.clone())
                                .or_insert_with(|| Collapser::new(idx));
                        }
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
                if head.len() >= 6 && head[..6].eq_ignore_ascii_case("begin") {
                    // Transaction opens: anything buffered from a torn
                    // previous attempt is stale.
                    tx_buf.clear();
                } else if is_ddl(head) {
                    // DDL invalidates cached column layouts for that db.
                    sess.st.schemas.retain(|k, _| !k.starts_with(&format!("{db}.")));
                    sess.st.maps.clear();
                }
            }
            bl::TYPE_XID => {
                // Commit boundary: the buffered ops become real, and the
                // watermark advances to the position AFTER this event.
                for (table, op) in tx_buf.drain(..) {
                    if changelog {
                        let Some(ki) = key_idx.get(table.as_ref()) else { continue };
                        let c = changelogs.entry(table.to_string()).or_default();
                        match op {
                            TxOp::Insert(row) => c.insert(row),
                            TxOp::Update(old, row) => c.update(old.as_ref(), row, ki),
                            TxOp::Delete(old) => c.delete(old),
                        }
                        continue;
                    }
                    let Some(c) = collapsers.get_mut(table.as_ref()) else {
                        continue;
                    };
                    match op {
                        TxOp::Insert(row) => c.insert(row)?,
                        TxOp::Update(old, row) => c.update(old.as_ref(), row)?,
                        TxOp::Delete(old) => c.delete(&old)?,
                    }
                }
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

    let tables: HashMap<String, Collapsed> = collapsers
        .into_iter()
        .map(|(k, v)| (k, v.finish()))
        .collect();
    Ok(DrainOutcome {
        tables,
        changes: changelogs,
        end_lsn: end_mark,
        wal_cols,
        wal_oids,
        hit_budget,
    })
}

enum TxOp {
    Insert(Tuple),
    Update(Option<Tuple>, Tuple),
    Delete(Tuple),
}

fn cells_bytes(row: &Tuple) -> usize {
    std::iter::once(row.frame.len() + row.cells.len() * 12)
        .sum::<usize>()
        + 48
}

fn is_ddl(sql: &str) -> bool {
    let s = sql.trim_start();
    ["alter", "create", "drop", "rename", "truncate"]
        .iter()
        .any(|k| s.len() >= k.len() && s[..k.len()].eq_ignore_ascii_case(k))
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
