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
use crate::logbased::collapse::{Collapsed, Collapser};
use crate::logbased::drain::DrainOutcome;
use crate::wire::mybinlog::{self as bl, BinlogState, TableSchema};
use crate::wire::mywire::MyWire;
use crate::wire::pgoutput::{Cell, PgoMessage};
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
        match sqlx::query_as::<_, (String, u64, String, String, String)>(sql)
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

/// Refuse loudly the server settings that would silently corrupt a CDC
/// stream, instead of decoding garbage.
pub(crate) async fn precheck(pool: &sqlx::MySqlPool) -> Result<()> {
    // MariaDB's binlog dialect diverges (its own GTID events, capability
    // vars) — refuse loudly instead of desyncing mid-stream.
    let (ver,): (String,) = sqlx::query_as("SELECT VERSION()")
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Transfer(format!("probe version: {e}")))?;
    if ver.to_lowercase().contains("mariadb") {
        return Err(Error::InvalidInput(format!(
            "log_based: {ver} is MariaDB — its binlog dialect differs from \
             MySQL's and is not supported yet (MySQL 5.7/8.x works)"
        )));
    }
    let want = [
        ("log_bin", "ON", "binary logging is off — set log_bin=ON"),
        ("binlog_format", "ROW", "binlog_format must be ROW"),
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
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_KEY AS CHAR), CAST(COLUMN_TYPE AS CHAR) \
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
    // COLUMN_KEY='PRI' marks primary-key members.
    Ok(TableSchema {
        names: rows.iter().map(|r| r.0.clone()).collect(),
        key: rows.iter().map(|r| r.1 == "PRI").collect(),
        unsigned: rows
            .iter()
            .map(|r| r.2.to_lowercase().contains("unsigned"))
            .collect(),
    })
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
) -> Result<DrainOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
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
                            collapsers.entry(q.clone()).or_insert_with(|| {
                                let idx = r
                                    .cols
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, c)| c.key)
                                    .map(|(i, _)| i)
                                    .collect();
                                Collapser::new(idx)
                            });
                        }
                        PgoMessage::Insert { new, .. } => {
                            buf_bytes += cells_bytes(&new);
                            tx_buf.push((Arc::from(q.as_str()), TxOp::Insert(new)));
                        }
                        PgoMessage::Update { old, new, .. } => {
                            buf_bytes += cells_bytes(&new);
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
                    let Some(c) = collapsers.get_mut(table.as_ref()) else {
                        continue;
                    };
                    match op {
                        TxOp::Insert(row) => c.insert(row)?,
                        TxOp::Update(old, row) => c.update(old.as_deref(), row)?,
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
            _ => {}
        }
    }

    let tables: HashMap<String, Collapsed> = collapsers
        .into_iter()
        .map(|(k, v)| (k, v.finish()))
        .collect();
    Ok(DrainOutcome {
        tables,
        end_lsn: end_mark,
        wal_cols,
        wal_oids,
        hit_budget,
    })
}

enum TxOp {
    Insert(Vec<Cell>),
    Update(Option<Vec<Cell>>, Vec<Cell>),
    Delete(Vec<Cell>),
}

fn cells_bytes(row: &[Cell]) -> usize {
    row.iter()
        .map(|c| match c {
            Cell::Text(t) => t.len() + 24,
            _ => 8,
        })
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
