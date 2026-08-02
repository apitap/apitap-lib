//! One batch drain: consume the CopyBoth stream from the start watermark to
//! the stop-line, collapsing per table — transactions land atomically (a
//! tx's events buffer until its Commit; a drain can therefore stop ONLY at
//! commit boundaries, and `end_lsn` is always a `Commit.end_lsn`).

use crate::error::{Error, Result};
use crate::logbased::collapse::{Collapsed, Collapser};
use crate::wire::pgoutput::{self, Cell, PgoMessage, Relation};
use crate::wire::walsender::{WalEvent, Walsender};
use std::collections::HashMap;

pub(crate) struct DrainOutcome {
    /// Collapsed window per "schema.table".
    pub tables: HashMap<String, Collapsed>,
    /// The last collapsed transaction's `Commit.end_lsn` — the ONLY valid
    /// new watermark. Equal to the start watermark when nothing arrived.
    pub end_lsn: u64,
    /// Column names per table in WAL order (from Relation messages) — the
    /// apply layer aligns them to the destination plan by name.
    pub wal_cols: HashMap<String, Vec<String>>,
}

struct RelState {
    table: String,
    key_idx: Vec<usize>,
    tracked: bool,
}

/// `key_cols`: replica-identity/PK column NAMES per "schema.table" — chosen
/// by the caller from the destination plan (works for REPLICA IDENTITY FULL
/// tables too, where the WAL flags every column as key).
pub(crate) async fn drain(
    ws: &mut Walsender,
    start_lsn: u64,
    stop_line: u64,
    key_cols: &HashMap<String, Vec<String>>,
    max_secs: u64,
) -> Result<DrainOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut rels: HashMap<u32, RelState> = HashMap::new();
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
    let mut wal_cols: HashMap<String, Vec<String>> = HashMap::new();
    // Current transaction's buffered row ops — flushed at Commit, discarded
    // if the drain aborts mid-transaction.
    let mut tx_buf: Vec<(String, TxOp)> = Vec::new();
    let mut end_lsn = start_lsn;

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
        let ev = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next_event()).await;
        let ev = match ev {
            Err(_) => continue, // poll tick — re-check the deadline
            Ok(r) => r?,
        };
        match ev {
            None => break,
            Some(WalEvent::Keepalive { wal_end, reply_requested }) => {
                if reply_requested {
                    // Never confirm progress mid-drain: report the start
                    // watermark; the real confirm happens after dest commit.
                    ws.standby_status(start_lsn, false).await?;
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
                        let c = collapsers
                            .get_mut(&table)
                            .expect("collapser exists for tracked table");
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
                }
                PgoMessage::Relation(r) => {
                    let st = rel_state(&r, key_cols)?;
                    if st.tracked {
                        collapsers
                            .entry(st.table.clone())
                            .or_insert_with(|| Collapser::new(st.key_idx.clone()));
                        wal_cols.insert(
                            st.table.clone(),
                            r.cols.iter().map(|c| c.name.clone()).collect(),
                        );
                    }
                    rels.insert(r.rel_id, st);
                }
                PgoMessage::Insert { rel_id, new } => {
                    if let Some(t) = tracked(&rels, rel_id)? {
                        tx_buf.push((t, TxOp::Insert(new)));
                    }
                }
                PgoMessage::Update { rel_id, old, new } => {
                    if let Some(t) = tracked(&rels, rel_id)? {
                        tx_buf.push((t, TxOp::Update(old.map(|o| o.tuple), new)));
                    }
                }
                PgoMessage::Delete { rel_id, old } => {
                    if let Some(t) = tracked(&rels, rel_id)? {
                        tx_buf.push((t, TxOp::Delete(old.tuple)));
                    }
                }
                PgoMessage::Truncate { rel_ids, .. } => {
                    for rid in rel_ids {
                        if let Some(t) = tracked(&rels, rid)? {
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
        wal_cols,
    })
}

fn rel_state(r: &Relation, key_cols: &HashMap<String, Vec<String>>) -> Result<RelState> {
    let table = format!("{}.{}", r.namespace, r.name);
    let Some(want) = key_cols.get(&table) else {
        return Ok(RelState { table, key_idx: Vec::new(), tracked: false });
    };
    if r.replica_identity == b'n' {
        return Err(Error::InvalidInput(format!(
            "log_based: table {table} has REPLICA IDENTITY NOTHING — updates and \
             deletes carry no key. Run: ALTER TABLE {table} REPLICA IDENTITY \
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
                        "log_based: key column '{k}' not in WAL relation for {table}"
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
                    "log_based: key column '{}' of {table} is not part of the \
                     source's REPLICA IDENTITY — old images won't carry it. \
                     Use the source PK as the key, or ALTER TABLE {table} \
                     REPLICA IDENTITY FULL",
                    r.cols[i].name
                )));
            }
        }
    }
    Ok(RelState { table, key_idx, tracked: true })
}

fn tracked(rels: &HashMap<u32, RelState>, rel_id: u32) -> Result<Option<String>> {
    match rels.get(&rel_id) {
        Some(st) if st.tracked => Ok(Some(st.table.clone())),
        Some(_) => Ok(None),
        None => Err(Error::Transfer(format!(
            "log_based: row event for unknown relation {rel_id} — pgoutput \
             must send Relation first; protocol desync?"
        ))),
    }
}
