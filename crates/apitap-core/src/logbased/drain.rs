//! One batch drain: consume the CopyBoth stream from the start watermark to
//! the stop-line, collapsing per table — transactions land atomically (a
//! tx's events buffer until its Commit; a drain can therefore stop ONLY at
//! commit boundaries, and `end_lsn` is always a `Commit.end_lsn`).

use crate::error::{Error, Result};
use crate::logbased::collapse::{Collapsed, Collapser};
use crate::wire::pgoutput::{self, Cell, PgoMessage, Relation};
use crate::wire::walsender::{WalEvent, Walsender};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct DrainOutcome {
    /// Collapsed window per "schema.table".
    pub tables: HashMap<String, Collapsed>,
    /// The last collapsed transaction's `Commit.end_lsn` — the ONLY valid
    /// new watermark. Equal to the start watermark when nothing arrived.
    pub end_lsn: u64,
    /// Column names per table in WAL order (from Relation messages) — the
    /// apply layer aligns them to the destination plan by name.
    pub wal_cols: HashMap<String, Vec<String>>,
    /// Column type OIDs per table, parallel to `wal_cols` — non-Postgres
    /// destinations translate type-specific text forms (bytea, bool).
    pub wal_oids: HashMap<String, Vec<u32>>,
    /// The drain stopped at the memory budget, not the stop-line: the caller
    /// applies this window, confirms the LSN, and drains again.
    pub hit_budget: bool,
}

struct RelState {
    table: Arc<str>,
    key_idx: Vec<usize>,
    tracked: bool,
}

/// Per-STREAM decode state that outlives one window: pgoutput announces a
/// Relation ONCE per walsender session, so a windowed drain (budget loop)
/// must carry the registry from window to window or the second window sees
/// "row event for unknown relation".
#[derive(Default)]
pub(crate) struct DrainSession {
    rels: HashMap<u32, RelState>,
    /// Key-column indices by "schema.table" — later windows build their
    /// collapsers from this (no Relation message re-arrives for them).
    key_idx: HashMap<String, Vec<usize>>,
    wal_cols: HashMap<String, Vec<String>>,
    wal_oids: HashMap<String, Vec<u32>>,
}

/// `key_cols`: replica-identity/PK column NAMES per "schema.table" — chosen
/// by the caller from the destination plan (works for REPLICA IDENTITY FULL
/// tables too, where the WAL flags every column as key).
///
/// `max_buf_bytes` bounds the window's buffered row data so CDC fits small
/// containers: past the budget the drain stops at the NEXT COMMIT BOUNDARY
/// with `hit_budget` set (a single transaction larger than the budget still
/// buffers whole — Postgres only ships a v1-protocol transaction after its
/// commit, so sub-transaction spilling buys nothing upstream).
pub(crate) async fn drain(
    ws: &mut Walsender,
    sess: &mut DrainSession,
    start_lsn: u64,
    stop_line: u64,
    key_cols: &HashMap<String, Vec<String>>,
    max_secs: u64,
    max_buf_bytes: usize,
    applied: &tokio::sync::watch::Receiver<u64>,
) -> Result<DrainOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
    // Current transaction's buffered row ops — flushed at Commit, discarded
    // if the drain aborts mid-transaction.
    let mut tx_buf: Vec<(Arc<str>, TxOp)> = Vec::new();
    let mut end_lsn = start_lsn;
    // Approximate bytes buffered across tx_buf + collapsers. Collapse dedup
    // (last-write-wins) makes true memory smaller — the count is conservative.
    let mut buf_bytes = 0usize;
    let mut hit_budget = false;

    fn cells_bytes(row: &[Cell]) -> usize {
        row.iter()
            .map(|c| match c {
                Cell::Text(t) => t.len() + 24,
                _ => 8,
            })
            .sum::<usize>()
            + 48
    }

    enum TxOp {
        Insert(Vec<Cell>),
        Update(Option<Vec<Cell>>, Vec<Cell>),
        Delete(Vec<Cell>),
        Truncate,
    }

    loop {
        if std::time::Instant::now() > deadline {
            // Wall-clock stop: whatever transaction is mid-flight is
            // discarded; end_lsn still points at the last complete commit.
            break;
        }
        // No per-event timeout: cancelling next_event mid-read would tear a
        // half-consumed frame off the stream (protocol desync), and a timer
        // per message is real overhead at millions of events. The server's
        // keepalives (~wal_sender_timeout/2) wake this loop on idle streams,
        // so the deadline above is checked at least that often.
        let ev = ws.next_event().await?;
        match ev {
            None => break,
            Some(WalEvent::Keepalive { wal_end, reply_requested }) => {
                if reply_requested {
                    // Never confirm progress mid-drain: report the last lsn
                    // the APPLY side committed (under overlap the previous
                    // window may still be in flight — start_lsn would lie).
                    ws.standby_status(*applied.borrow(), false).await?;
                }
                if wal_end >= stop_line && tx_buf.is_empty() {
                    // Server has shipped everything up to the stop-line and
                    // we're at a boundary: caught up.
                    break;
                }
            }
            Some(WalEvent::XLogData { payload, .. }) => match pgoutput::decode(&payload)? {
                PgoMessage::Begin { .. } => tx_buf.clear(),
                PgoMessage::Commit { end_lsn: e, .. } => {
                    for (table, op) in tx_buf.drain(..) {
                        // Lazy per-window collapser: the Relation message came
                        // in THIS window or an earlier one — the session knows.
                        if !collapsers.contains_key(table.as_ref()) {
                            let ki = sess.key_idx.get(table.as_ref()).expect(
                                "session key_idx exists for tracked table",
                            );
                            collapsers
                                .insert(table.to_string(), Collapser::new(ki.clone()));
                        }
                        let c = collapsers
                            .get_mut(table.as_ref())
                            .expect("collapser just ensured");
                        match op {
                            TxOp::Insert(row) => c.insert(row)?,
                            TxOp::Update(old, row) => c.update(old.as_deref(), row)?,
                            TxOp::Delete(old) => c.delete(&old)?,
                            TxOp::Truncate => c.truncate(),
                        }
                    }
                    end_lsn = e;
                    if e >= stop_line {
                        break;
                    }
                    if buf_bytes >= max_buf_bytes {
                        hit_budget = true;
                        break;
                    }
                }
                PgoMessage::Relation(r) => {
                    let st = rel_state(&r, key_cols)?;
                    if st.tracked {
                        sess.key_idx.insert(st.table.to_string(), st.key_idx.clone());
                        sess.wal_cols.insert(
                            st.table.to_string(),
                            r.cols.iter().map(|c| c.name.clone()).collect(),
                        );
                        sess.wal_oids.insert(
                            st.table.to_string(),
                            r.cols.iter().map(|c| c.type_oid).collect(),
                        );
                    }
                    sess.rels.insert(r.rel_id, st);
                }
                PgoMessage::Insert { rel_id, new } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        buf_bytes += cells_bytes(&new);
                        tx_buf.push((t, TxOp::Insert(new)));
                    }
                }
                PgoMessage::Update { rel_id, old, new } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        let old = old.map(|o| o.tuple);
                        buf_bytes += cells_bytes(&new)
                            + old.as_deref().map_or(0, cells_bytes);
                        tx_buf.push((t, TxOp::Update(old, new)));
                    }
                }
                PgoMessage::Delete { rel_id, old } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        buf_bytes += cells_bytes(&old.tuple);
                        tx_buf.push((t, TxOp::Delete(old.tuple)));
                    }
                }
                PgoMessage::Truncate { rel_ids, .. } => {
                    for rid in rel_ids {
                        if let Some(t) = tracked(&sess.rels, rid)? {
                            tx_buf.push((t, TxOp::Truncate));
                        }
                    }
                }
                PgoMessage::Origin | PgoMessage::Type => {}
            },
        }
    }

    Ok(DrainOutcome {
        tables: collapsers
            .into_iter()
            .map(|(t, c)| (t, c.finish()))
            .collect(),
        end_lsn,
        wal_cols: sess.wal_cols.clone(),
        wal_oids: sess.wal_oids.clone(),
        hit_budget,
    })
}

fn rel_state(r: &Relation, key_cols: &HashMap<String, Vec<String>>) -> Result<RelState> {
    let table_s = format!("{}.{}", r.namespace, r.name);
    let table: Arc<str> = table_s.as_str().into();
    let Some(want) = key_cols.get(&table_s) else {
        return Ok(RelState { table, key_idx: Vec::new(), tracked: false });
    };
    if r.replica_identity == b'n' {
        return Err(Error::InvalidInput(format!(
            "log_based: table {table_s} has REPLICA IDENTITY NOTHING — updates and \
             deletes carry no key. Run: ALTER TABLE {table_s} REPLICA IDENTITY \
             DEFAULT (with a primary key) or FULL"
        )));
    }
    let key_idx = want
        .iter()
        .map(|k| {
            r.cols
                .iter()
                .position(|c| &c.name == k)
                .ok_or_else(|| {
                    Error::Transfer(format!(
                        "log_based: key column '{k}' not in WAL relation for {table_s}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    // With REPLICA IDENTITY DEFAULT/USING INDEX, old images only carry the
    // flagged key columns — our chosen key must be covered by them.
    if r.replica_identity != b'f' {
        for &i in &key_idx {
            if !r.cols[i].key {
                return Err(Error::InvalidInput(format!(
                    "log_based: key column '{}' of {table_s} is not part of the \
                     source's REPLICA IDENTITY — old images won't carry it. \
                     Use the source PK as the key, or ALTER TABLE {table_s} \
                     REPLICA IDENTITY FULL",
                    r.cols[i].name
                )));
            }
        }
    }
    Ok(RelState { table, key_idx, tracked: true })
}

fn tracked(rels: &HashMap<u32, RelState>, rel_id: u32) -> Result<Option<Arc<str>>> {
    match rels.get(&rel_id) {
        Some(st) if st.tracked => Ok(Some(st.table.clone())),
        Some(_) => Ok(None),
        None => Err(Error::Transfer(format!(
            "log_based: row event for unknown relation {rel_id} — pgoutput \
             must send Relation first; protocol desync?"
        ))),
    }
}
