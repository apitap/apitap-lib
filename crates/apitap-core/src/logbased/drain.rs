//! One batch drain: consume the CopyBoth stream from the start watermark to
//! the stop-line, collapsing per table — transactions land atomically (a
//! tx's events buffer until its Commit; a drain can therefore stop ONLY at
//! commit boundaries, and `end_lsn` is always a `Commit.end_lsn`).

use crate::error::{Error, Result};
use crate::logbased::changelog::Changes;
use crate::logbased::collapse::{Collapsed, Collapser};
use crate::wire::pgoutput::{self, PgoMessage, Relation, Tuple};
use crate::wire::walsender::{WalEvent, Walsender};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct DrainOutcome {
    /// Collapsed window per "schema.table". Empty in changelog mode.
    pub tables: HashMap<String, Collapsed>,
    /// RAW captured window per "schema.table" — every operation in WAL order,
    /// populated instead of `tables` when the run is `changelog=true`. See
    /// [`crate::logbased::changelog`].
    pub changes: HashMap<String, Changes>,
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
    /// The server's WAL end at the moment this drain saw it had shipped
    /// everything — 0 if the drain stopped for any other reason.
    ///
    /// `end_lsn` already carries this point (see the caught-up branch), so
    /// nothing needs to act on it; it is reported because "the drain caught
    /// up" and "the drain hit its budget" are different states and a caller
    /// that has to tell them apart should not have to infer it.
    pub caught_up_lsn: u64,
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
    /// Column type OIDs by rel_id (every Relation seen, tracked or not) —
    /// the schema `pgoutput::decode` needs to render `binary 'true'` tuples
    /// back to text. Unused (empty lookups) on text-mode streams.
    rel_oids: pgoutput::RelOids,
    /// Key-column indices by "schema.table" — later windows build their
    /// collapsers from this (no Relation message re-arrives for them).
    key_idx: HashMap<String, Vec<usize>>,
    wal_cols: HashMap<String, Vec<String>>,
    wal_oids: HashMap<String, Vec<u32>>,
    /// proto v2 streamed transactions in flight, by xid: the server ships a
    /// big transaction WHILE decoding it; its ops buffer here until Stream
    /// Commit makes them real (Abort drops them). May span windows — a
    /// budget break can only land between blocks, never inside one.
    streams: HashMap<u32, Vec<(Arc<str>, StreamOp)>>,
    /// Bytes held by `streams`. It lives on the SESSION and not in `drain`'s
    /// locals because the buffers do: a streamed transaction survives a
    /// window boundary, and a per-call counter forgets it the moment the next
    /// drain starts. The memory it holds does not go anywhere, so the budget
    /// that is supposed to bound this process's RSS has to keep counting it.
    stream_bytes: usize,
}

/// Truthful residency: a buffered row pins its WHOLE frame plus the range
/// vec. (Old accounting summed cell lengths, which under-counted exactly when
/// Bytes cells began pinning frames.) At module scope so the session's
/// streamed-transaction accounting measures the same thing the window's does.
fn cells_bytes(row: &Tuple) -> usize {
    row.frame.len() + row.cells.len() * 12 + 48
}

/// Bytes a buffered streamed op holds — the same measure `cells_bytes` takes
/// of a tuple, so the budget adds like with like.
fn op_bytes(op: &StreamOp) -> usize {
    match op {
        StreamOp::Insert(t) | StreamOp::Delete(t) => cells_bytes(t),
        StreamOp::Update(old, new) => {
            cells_bytes(new) + old.as_ref().map_or(0, cells_bytes)
        }
        StreamOp::Truncate => 0,
    }
}

/// Buffered op of a streamed (not-yet-committed) transaction.
pub(crate) enum StreamOp {
    Insert(Tuple),
    Update(Option<Tuple>, Tuple),
    Delete(Tuple),
    Truncate,
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
    changelog: bool,
) -> Result<DrainOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
    let mut changelogs: HashMap<String, Changes> = HashMap::new();
    // Current transaction's buffered row ops — flushed at Commit, discarded
    // if the drain aborts mid-transaction.
    let mut tx_buf: Vec<(Arc<str>, StreamOp)> = Vec::new();
    let mut end_lsn = start_lsn;
    // Approximate bytes buffered across tx_buf + collapsers. Collapse dedup
    // (last-write-wins) makes true memory smaller — the count is conservative.
    let mut buf_bytes = 0usize;
    let mut hit_budget = false;
    let mut caught_up_lsn = 0u64;

    let mut in_stream: Option<u32> = None;

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
                if wal_end >= stop_line && tx_buf.is_empty() && sess.streams.is_empty() {
                    // Server has shipped everything up to the stop-line and
                    // we're at a boundary: caught up.
                    //
                    // The window's end is that point, not the last commit that
                    // happened to be ours. Everything between them was traffic
                    // for tables we do not track, so "applied up to here" is
                    // true of `wal_end` — and saying so is what lets an idle
                    // published table release WAL. Without it the watermark
                    // never moves, the slot is never told about progress, and
                    // a busy instance fills its disk while every scheduled run
                    // reports success.
                    //
                    // It has to be the WATERMARK that moves, not just the
                    // confirmation. Confirming a point the destination has not
                    // recorded makes the slot's confirmed LSN overtake the
                    // stored watermark, which the next run correctly reads as
                    // tampered state and refuses. Measured: 7 gate legs went
                    // red that way. The end_lsn below travels through the
                    // ordinary window path, so every destination writes it the
                    // same way it writes any other watermark.
                    if wal_end > end_lsn {
                        end_lsn = wal_end;
                    }
                    caught_up_lsn = wal_end;
                    break;
                }
            }
            Some(WalEvent::XLogData { payload, .. }) => match pgoutput::decode(&payload, in_stream.is_some(), &sess.rel_oids)? {
                PgoMessage::Begin { .. } => tx_buf.clear(),
                PgoMessage::Commit { end_lsn: e, .. } => {
                    flush_ops(tx_buf.drain(..), &mut collapsers, &mut changelogs, &sess.key_idx, changelog)?;
                    end_lsn = e;
                    if e >= stop_line {
                        break;
                    }
                    if buf_bytes + sess.stream_bytes >= max_buf_bytes {
                        hit_budget = true;
                        break;
                    }
                }
                PgoMessage::StreamStart { xid } => {
                    in_stream = Some(xid);
                    sess.streams.entry(xid).or_default();
                }
                PgoMessage::StreamStop => in_stream = None,
                PgoMessage::StreamCommit { xid, end_lsn: e } => {
                    let ops = sess.streams.remove(&xid).unwrap_or_default();
                    // The ops leave the session's buffer and land in this
                    // window's collapsers, so the charge moves with them.
                    let n: usize = ops.iter().map(|(_, o)| op_bytes(o)).sum();
                    sess.stream_bytes = sess.stream_bytes.saturating_sub(n);
                    buf_bytes += n;
                    flush_ops(ops, &mut collapsers, &mut changelogs, &sess.key_idx, changelog)?;
                    end_lsn = e;
                    if e >= stop_line {
                        break;
                    }
                    if buf_bytes + sess.stream_bytes >= max_buf_bytes {
                        hit_budget = true;
                        break;
                    }
                }
                PgoMessage::StreamAbort { xid, sub_xid } => {
                    // A TOP-LEVEL abort really does discard the whole
                    // transaction: nothing in it ever committed.
                    if sub_xid == xid {
                        if let Some(ops) = sess.streams.remove(&xid) {
                            let n: usize = ops.iter().map(|(_, o)| op_bytes(o)).sum();
                            sess.stream_bytes = sess.stream_bytes.saturating_sub(n);
                        }
                    } else {
                        // A SUBtransaction aborted — a `ROLLBACK TO SAVEPOINT`
                        // inside a streamed transaction. Only that
                        // subtransaction's ops should disappear, and this
                        // buffer does not tag ops by subxid, so there is no
                        // way to remove the right ones.
                        //
                        // Dropping the whole transaction (what this arm used to
                        // do, because the decoder read only the first xid) lost
                        // every change in it and still advanced the watermark.
                        // Keeping everything would apply changes the source
                        // rolled back. Both are wrong and both are silent, so
                        // the run stops and says which one it is.
                        return Err(Error::Transfer(format!(
                            "log_based: transaction {xid} rolled back to a savepoint \
                             (subtransaction {sub_xid} aborted) while being streamed. \
                             apitap buffers a streamed transaction's rows without \
                             tagging them by subtransaction, so it cannot drop only \
                             the rolled-back ones — and applying or discarding all of \
                             them would both be wrong. Re-run: the window replays \
                             from the last watermark, and the transaction will have \
                             committed or aborted by then. To avoid it entirely, set \
                             logical_decoding_work_mem high enough that transactions \
                             are not streamed before they commit."
                        )));
                    }
                }
                PgoMessage::Relation(r) => {
                    sess.rel_oids.insert(
                        r.rel_id,
                        Arc::new(r.cols.iter().map(|c| c.type_oid).collect()),
                    );
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
                        let n = cells_bytes(&new);
                        let op = StreamOp::Insert(new);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                sess.streams.get_mut(&x).expect("stream open").push((t, op));
                            }
                            None => {
                                buf_bytes += n;
                                tx_buf.push((t, op));
                            }
                        }
                    }
                }
                PgoMessage::Update { rel_id, old, new } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        let old = old.map(|o| o.tuple);
                        let n = cells_bytes(&new) + old.as_ref().map_or(0, cells_bytes);
                        let op = StreamOp::Update(old, new);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                sess.streams.get_mut(&x).expect("stream open").push((t, op));
                            }
                            None => {
                                buf_bytes += n;
                                tx_buf.push((t, op));
                            }
                        }
                    }
                }
                PgoMessage::Delete { rel_id, old } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        let n = cells_bytes(&old.tuple);
                        let op = StreamOp::Delete(old.tuple);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                sess.streams.get_mut(&x).expect("stream open").push((t, op));
                            }
                            None => {
                                buf_bytes += n;
                                tx_buf.push((t, op));
                            }
                        }
                    }
                }
                PgoMessage::Truncate { rel_ids, .. } => {
                    for rid in rel_ids {
                        if let Some(t) = tracked(&sess.rels, rid)? {
                            let op = StreamOp::Truncate;
                            match in_stream {
                                Some(x) => sess.streams.get_mut(&x).expect("stream open").push((t, op)),
                                None => tx_buf.push((t, op)),
                            }
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
        changes: changelogs,
        end_lsn,
        wal_cols: sess.wal_cols.clone(),
        wal_oids: sess.wal_oids.clone(),
        hit_budget,
        caught_up_lsn,
    })
}

/// Land one committed transaction's buffered ops in the per-window
/// collapsers (lazy: the Relation may have arrived in an earlier window —
/// the session's key_idx remembers).
fn flush_ops(
    ops: impl IntoIterator<Item = (Arc<str>, StreamOp)>,
    collapsers: &mut HashMap<String, Collapser>,
    changelogs: &mut HashMap<String, Changes>,
    key_idx: &HashMap<String, Vec<usize>>,
    changelog: bool,
) -> Result<()> {
    for (table, op) in ops {
        // changelog=true captures every operation verbatim; the collapser is
        // bypassed entirely (it exists to reduce a window to one image per key,
        // which is the opposite of an audit trail).
        if changelog {
            let ki = key_idx
                .get(table.as_ref())
                .expect("session key_idx exists for tracked table");
            let c = changelogs.entry(table.to_string()).or_default();
            match op {
                StreamOp::Insert(row) => c.insert(row),
                StreamOp::Update(old, row) => c.update(old.as_ref(), row, ki),
                StreamOp::Delete(old) => c.delete(old),
                StreamOp::Truncate => c.truncate(),
            }
            continue;
        }
        if !collapsers.contains_key(table.as_ref()) {
            let ki = key_idx
                .get(table.as_ref())
                .expect("session key_idx exists for tracked table");
            collapsers.insert(table.to_string(), Collapser::new(ki.clone()));
        }
        let c = collapsers.get_mut(table.as_ref()).expect("collapser just ensured");
        match op {
            StreamOp::Insert(row) => c.insert(row)?,
            StreamOp::Update(old, row) => c.update(old.as_ref(), row)?,
            StreamOp::Delete(old) => c.delete(&old)?,
            StreamOp::Truncate => c.truncate(),
        }
    }
    Ok(())
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
