//! One batch drain: consume the CopyBoth stream from the start watermark to
//! the stop-line, collapsing per table — transactions land atomically (a
//! tx's events buffer until its Commit; a drain can therefore stop ONLY at
//! commit boundaries, and `end_lsn` is always a `Commit.end_lsn`).
//!
//! The window it returns is a [`DrainOutcome`]: one sealed body per table,
//! each with the column layout its rows are in, and the window's identity —
//! `start` (the watermark it was drained FROM, the one position a re-drain
//! reproduces, and so the changelog's stamp) and `end` (the last complete
//! commit, the only valid next watermark).

use crate::error::{Error, Result};
use crate::logbased::changelog::Changes;
use crate::logbased::collapse::Collapser;
use crate::logbased::replay::WindowId;
use crate::logbased::window::{Bodies, DrainOutcome, Layout};
use crate::wire::pgoutput::{self, PgoMessage, Tuple};
use crate::wire::walsender::{WalEvent, Walsender};
use std::collections::HashMap;
use std::sync::Arc;

/// How long a replication stream may say nothing before the drain treats it
/// as a dead connection (system review 2026-10-07, G0.5). Server keepalives
/// arrive every `wal_sender_timeout / 2` — 30 s by default — so two silent
/// minutes is unambiguous. `APITAP_REPLICATION_SILENCE_SECS` may lower it
/// (tests) but not disable it: zero or junk falls back to the default.
const REPLICATION_SILENCE: std::time::Duration = std::time::Duration::from_secs(120);

pub(crate) fn silence_budget() -> std::time::Duration {
    std::env::var("APITAP_REPLICATION_SILENCE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .filter(|d| !d.is_zero())
        .unwrap_or(REPLICATION_SILENCE)
}

/// The most one transaction may buffer before the drain refuses: the v1
/// protocol ships a transaction whole after its commit, so a giant write (an
/// UPDATE over a whole table) cannot be landed in parts — past this cap the
/// honest end is a refusal that names the cause, not an OOM (system review
/// 2026-10-07, G0.1). `APITAP_TX_BUF_BYTES` raises or lowers it.
const TX_BUF_LIMIT: usize = 256 << 20;

fn tx_buf_limit() -> usize {
    std::env::var("APITAP_TX_BUF_BYTES")
        .ok()
        .and_then(|v| crate::logbased::run::parse_size(&v))
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(TX_BUF_LIMIT)
}

fn tx_too_big(table: &str, bytes: usize, limit: usize) -> Error {
    Error::Transfer(format!(
        "log_based: one transaction's changes for {table} have buffered {} MiB \
         (APITAP_TX_BUF_BYTES = {} MiB) — Postgres ships a transaction whole, so it \
         cannot land in parts, and continuing would OOM. Split the write on the \
         source, or raise APITAP_TX_BUF_BYTES (bytes, or a K/M/G suffix) if this \
         process really has that much memory. The watermark is untouched: the next run re-reads the transaction.",
        bytes >> 20,
        limit >> 20
    ))
}

struct RelState {
    table: Arc<str>,
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
    /// Column layout by "schema.table", from the latest Relation — later
    /// windows build their bodies from this (no Relation message re-arrives
    /// for them), and every body carries the one it was built with.
    layouts: HashMap<String, Arc<Layout>>,
    /// A committed transaction the previous window did not take, and its
    /// commit's end: a Relation inside it changed the layout of a table the
    /// window already held a body for, and one window never spans a layout
    /// change. It opens the next window, whose bodies start from the new one.
    carry: Option<(TxOps, u64)>,
    /// proto v2 streamed transactions in flight, by xid: the server ships a
    /// big transaction WHILE decoding it; its ops buffer here until Stream
    /// Commit makes them real (Abort drops them). May span windows — a
    /// budget break can only land between blocks, never inside one.
    /// Ops buffered per TOP-LEVEL xid, each tagged with the xid of the
    /// (sub)transaction that produced it — pgoutput prefixes every streamed
    /// change with that, and it is the only way to undo a savepoint rollback
    /// instead of refusing it.
    streams: HashMap<u32, Vec<(Arc<str>, StreamOp, u32)>>,
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

/// One transaction's row ops, in WAL order, each with its table.
type TxOps = Vec<(Arc<str>, StreamOp)>;

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
/// commit, so sub-transaction spilling buys nothing upstream — but never past
/// the hard cap `APITAP_TX_BUF_BYTES`, where the drain refuses instead of
/// OOMing; system review G0.1).
///
/// It also stops, `hit_budget` set, before a transaction whose Relation
/// changed the layout of a table the window already holds: that transaction
/// opens the next window, so no window spans a layout change. A transaction
/// that changes a table's columns between two of its OWN row changes cannot be
/// split that way, and its window fails `TableWindow::seal`, loudly.
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
    follow: Option<(&sqlx::PgPool, usize, std::time::Instant)>,
    keep_binary: bool,
) -> Result<DrainOutcome> {
    let mut stop_line = stop_line;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    let mut collapsers: HashMap<String, Collapser> = HashMap::new();
    let mut changelogs: HashMap<String, Changes> = HashMap::new();
    // Current transaction's buffered row ops — flushed at Commit, discarded
    // if the drain aborts mid-transaction. `tx_bytes` is the current
    // transaction's own charge against the hard cap (system review G0.1).
    let mut tx_buf: TxOps = Vec::new();
    let mut tx_bytes = 0usize;
    let tx_limit = tx_buf_limit();
    let mut end_lsn = start_lsn;
    // Approximate bytes buffered across tx_buf + collapsers. Collapse dedup
    // (last-write-wins) makes true memory smaller — the count is conservative.
    let mut buf_bytes = 0usize;
    let mut hit_budget = false;
    let dbg_stream = std::env::var("APITAP_DEBUG").is_ok();

    // Raw tracked row-change messages decoded this window (APITAP_DEBUG):
    // compared against the collapsed event count to localise the ~14.5 %
    // accounting gap between witness, decode and collapse (design §14.6).
    let mut decoded = 0u64;
    let mut in_stream: Option<u32> = None;
    // A Relation in the transaction being read changed the layout of a table
    // this window already holds a body for: the transaction goes to the next
    // window at its commit (see `DrainSession::carry`).
    let mut relayout = false;

    // The transaction the previous window stopped short of opens this one,
    // under the layout its Relation announced.
    let mut carried_to_stop = false;
    if let Some((ops, e)) = sess.carry.take() {
        buf_bytes += ops.iter().map(|(_, o)| op_bytes(o)).sum::<usize>();
        flush_ops(ops, &mut collapsers, &mut changelogs, &sess.layouts, changelog)?;
        end_lsn = e;
        carried_to_stop = e >= stop_line;
    }

    loop {
        if carried_to_stop {
            break;
        }
        if std::time::Instant::now() > deadline || crate::shutdown::requested() {
            // Wall-clock stop, or a SIGTERM. Both leave the drain at the same
            // place: whatever transaction is mid-flight is discarded, and
            // `end_lsn` still points at the last COMPLETE commit — so the
            // window that gets applied is a whole one, and the watermark that
            // follows it is true.
            //
            // A scheduler that stops a task (Kubernetes evicting a pod,
            // Airflow clearing a run, systemd on `stop`) sends SIGTERM and
            // then, seconds later, SIGKILL. Ignoring the first one means the
            // second arrives mid-apply: correct, because the watermark is
            // written last and a replay is idempotent, but it throws away
            // every row the window had already collected. Reading it means the
            // run lands what it has and exits with its state consistent, which
            // is the difference between a redeploy costing nothing and costing
            // a window.
            //
            // NOT ^C: SIGINT is left to the interpreter, so Ctrl-C behaves
            // exactly as it always did — see `crate::shutdown`.
            break;
        }
        // The scanner owns the read half and bounds its own refills with the
        // silence budget — one timer per ~1 MiB refill, not per event (L1b).
        // The server keepalives every ~wal_sender_timeout/2 (30 s by default)
        // normally wake the refill; when they stop, the connection is dead
        // (half-open socket, NAT rebind) and the scanner errors at the last
        // committed watermark (system review 2026-10-07, G0.5).
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
                    // Follow floor (design §14.2): the per-window DELETE cost
                    // is ~constant, so sealing every caught-up delta kept
                    // windows at ~4k changes and follow at 36k/s. Keep THIS
                    // window open, roll the stop line, and fill to the floor
                    // (or the follow deadline) before sealing.
                    if let Some((src, floor, deadline)) = follow {
                        let buffered = buf_bytes + sess.stream_bytes;
                        if buffered < floor && std::time::Instant::now() < deadline {
                            let now: (String,) =
                                sqlx::query_as("SELECT pg_current_wal_lsn()::text")
                                    .fetch_one(src)
                                    .await
                                    .map_err(|e| {
                                        Error::Transfer(format!(
                                            "log_based: follow lsn read: {e}"
                                        ))
                                    })?;
                            let now = crate::wire::pgoutput::lsn_from_string(&now.0)?;
                            if now > stop_line {
                                stop_line = now;
                            } else {
                                tokio::time::sleep(std::time::Duration::from_millis(200))
                                    .await;
                            }
                            continue;
                        }
                    }
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
                    break;
                }
            }
            Some(WalEvent::XLogData { payload, .. }) => {
                let (msg, change_xid) =
                    pgoutput::decode(&payload, in_stream.is_some(), &sess.rel_oids, keep_binary)?;
                // Inside a stream block every change names its own
                // (sub)transaction; outside one there is nothing to name.
                let sub = change_xid.or(in_stream);
                match msg {
                PgoMessage::Begin { .. } => {
                    tx_buf.clear();
                    tx_bytes = 0;
                }
                PgoMessage::Commit { end_lsn: e, .. } => {
                    if relayout {
                        // The window ends at the previous commit; this one
                        // opens the next, whose bodies take the new layout.
                        sess.carry = Some((std::mem::take(&mut tx_buf), e));
                        hit_budget = true;
                        break;
                    }
                    flush_ops(tx_buf.drain(..), &mut collapsers, &mut changelogs, &sess.layouts, changelog)?;
                    tx_bytes = 0;
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
                    let n: usize = ops.iter().map(|(_, o, _)| op_bytes(o)).sum();
                    sess.stream_bytes = sess.stream_bytes.saturating_sub(n);
                    let ops = ops.into_iter().map(|(t, o, _)| (t, o));
                    if relayout {
                        sess.carry = Some((ops.collect(), e));
                        hit_budget = true;
                        break;
                    }
                    buf_bytes += n;
                    flush_ops(ops, &mut collapsers, &mut changelogs, &sess.layouts, changelog)?;
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
                    // Stream Abort names the top-level transaction and the
                    // (sub)transaction that aborted. Both cases are now
                    // handled by removing exactly what that xid produced:
                    // when they are equal the whole transaction goes, and
                    // when they differ only the rolled-back subtransaction's
                    // rows do.
                    //
                    // This is possible because every streamed change carries
                    // the xid that produced it and the buffer keeps it. The
                    // first version of this code read only the first xid and
                    // discarded the WHOLE transaction on any savepoint
                    // rollback — silently, watermark and all; the second
                    // refused the window, which was safe but stopped runs for
                    // rollbacks in traffic apitap does not even replicate.
                    if let Some(ops) = sess.streams.get_mut(&xid) {
                        let before = ops.len();
                        let freed: usize = ops
                            .iter()
                            .filter(|(_, _, sx)| *sx == sub_xid || sub_xid == xid)
                            .map(|(_, o, _)| op_bytes(o))
                            .sum();
                        if sub_xid == xid {
                            ops.clear();
                        } else {
                            ops.retain(|(_, _, sx)| *sx != sub_xid);
                        }
                        sess.stream_bytes = sess.stream_bytes.saturating_sub(freed);
                        if dbg_stream && before != ops.len() {
                            eprintln!(
                                "[log_based] stream {xid}: subtransaction {sub_xid} \
                                 rolled back, dropped {} of {before} buffered rows",
                                before - ops.len()
                            );
                        }
                        if sub_xid == xid {
                            sess.streams.remove(&xid);
                        }
                    }
                }
                PgoMessage::Relation(r) => {
                    sess.rel_oids.insert(
                        r.rel_id,
                        Arc::new(r.cols.iter().map(|c| c.type_oid).collect()),
                    );
                    let table = format!("{}.{}", r.namespace, r.name);
                    let layout = Layout::from_relation(&r, key_cols)?;
                    let tracked = layout.is_some();
                    if let Some(new) = layout {
                        // pgoutput re-sends a Relation on every relcache
                        // invalidation, and most change nothing (an ANALYZE,
                        // a GRANT): only a different layout replaces the one
                        // the window's bodies were built with, and only then
                        // does a window holding a body of the table end.
                        if sess.layouts.get(&table).map_or(true, |old| **old != new) {
                            relayout |= collapsers.contains_key(&table) || changelogs.contains_key(&table);
                            sess.layouts.insert(table.clone(), Arc::new(new));
                        }
                    }
                    sess.rels.insert(r.rel_id, RelState { table: table.as_str().into(), tracked });
                }
                PgoMessage::Insert { rel_id, new } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        decoded += 1;
                        let n = cells_bytes(&new);
                        let op = StreamOp::Insert(new);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                if sess.stream_bytes > tx_limit {
                                    return Err(tx_too_big(&t, sess.stream_bytes, tx_limit));
                                }
                                sess.streams
                                    .get_mut(&x)
                                    .expect("stream open")
                                    .push((t, op, sub.unwrap_or(x)));
                            }
                            None => {
                                buf_bytes += n;
                                tx_bytes += n;
                                if tx_bytes > tx_limit {
                                    return Err(tx_too_big(&t, tx_bytes, tx_limit));
                                }
                                tx_buf.push((t, op));
                            }
                        }
                    }
                }
                PgoMessage::Update { rel_id, old, new } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        decoded += 1;
                        let old = old.map(|o| o.tuple);
                        let n = cells_bytes(&new) + old.as_ref().map_or(0, cells_bytes);
                        let op = StreamOp::Update(old, new);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                if sess.stream_bytes > tx_limit {
                                    return Err(tx_too_big(&t, sess.stream_bytes, tx_limit));
                                }
                                sess.streams
                                    .get_mut(&x)
                                    .expect("stream open")
                                    .push((t, op, sub.unwrap_or(x)));
                            }
                            None => {
                                buf_bytes += n;
                                tx_bytes += n;
                                if tx_bytes > tx_limit {
                                    return Err(tx_too_big(&t, tx_bytes, tx_limit));
                                }
                                tx_buf.push((t, op));
                            }
                        }
                    }
                }
                PgoMessage::Delete { rel_id, old } => {
                    if let Some(t) = tracked(&sess.rels, rel_id)? {
                        decoded += 1;
                        let n = cells_bytes(&old.tuple);
                        let op = StreamOp::Delete(old.tuple);
                        match in_stream {
                            Some(x) => {
                                sess.stream_bytes += n;
                                if sess.stream_bytes > tx_limit {
                                    return Err(tx_too_big(&t, sess.stream_bytes, tx_limit));
                                }
                                sess.streams
                                    .get_mut(&x)
                                    .expect("stream open")
                                    .push((t, op, sub.unwrap_or(x)));
                            }
                            None => {
                                buf_bytes += n;
                                tx_bytes += n;
                                if tx_bytes > tx_limit {
                                    return Err(tx_too_big(&t, tx_bytes, tx_limit));
                                }
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
                                Some(x) => sess.streams.get_mut(&x).expect("stream open").push((
                                    t,
                                    op,
                                    sub.unwrap_or(x),
                                )),
                                None => tx_buf.push((t, op)),
                            }
                        }
                    }
                }
                PgoMessage::Origin | PgoMessage::Type => {}
                }
            }
        }
    }

    let outcome = DrainOutcome {
        bodies: Bodies::seal(changelog, collapsers, changelogs)?,
        id: WindowId::new(start_lsn, end_lsn),
        hit_budget,
        rb: keep_binary.then_some(crate::logbased::window::RbBody::PgBinary),
    };
    if dbg_stream {
        eprintln!(
            "[events] decoded={decoded} collapsed={}",
            outcome.events()
        );
    }
    Ok(outcome)
}

/// The layout a table's first op of the window builds its body with.
fn layout_of(layouts: &HashMap<String, Arc<Layout>>, table: &str) -> Result<Arc<Layout>> {
    layouts.get(table).cloned().ok_or_else(|| {
        Error::Transfer(format!("log_based: no column layout for {table} — decoder invariant broken"))
    })
}

/// Land one committed transaction's buffered ops in the per-window
/// collapsers (lazy: the Relation may have arrived in an earlier window —
/// the session's layouts remember).
fn flush_ops(
    ops: impl IntoIterator<Item = (Arc<str>, StreamOp)>,
    collapsers: &mut HashMap<String, Collapser>,
    changelogs: &mut HashMap<String, Changes>,
    layouts: &HashMap<String, Arc<Layout>>,
    changelog: bool,
) -> Result<()> {
    for (table, op) in ops {
        // changelog=true captures every operation verbatim; the collapser is
        // bypassed entirely (it exists to reduce a window to one image per key,
        // which is the opposite of an audit trail).
        if changelog {
            if !changelogs.contains_key(table.as_ref()) {
                changelogs.insert(table.to_string(), Changes::new(layout_of(layouts, &table)?));
            }
            let c = changelogs.get_mut(table.as_ref()).expect("changelog just ensured");
            match op {
                StreamOp::Insert(row) => c.insert(row),
                StreamOp::Update(old, row) => c.update(old.as_ref(), row),
                StreamOp::Delete(old) => c.delete(old),
                StreamOp::Truncate => c.truncate(),
            }
            continue;
        }
        if !collapsers.contains_key(table.as_ref()) {
            collapsers.insert(table.to_string(), Collapser::new(layout_of(layouts, &table)?));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The knob may shorten the wait, but never removes it: zero and junk
    /// must fall back to the default or a half-open socket parks forever
    /// (system review 2026-10-07, G0.5).
    #[test]
    fn a_zero_or_junk_silence_budget_falls_back_to_the_default() {
        std::env::set_var("APITAP_REPLICATION_SILENCE_SECS", "0");
        assert_eq!(silence_budget(), REPLICATION_SILENCE);
        std::env::set_var("APITAP_REPLICATION_SILENCE_SECS", "not-a-number");
        assert_eq!(silence_budget(), REPLICATION_SILENCE);
        std::env::set_var("APITAP_REPLICATION_SILENCE_SECS", "5");
        assert_eq!(silence_budget(), std::time::Duration::from_secs(5));
        std::env::remove_var("APITAP_REPLICATION_SILENCE_SECS");
        assert_eq!(silence_budget(), REPLICATION_SILENCE);
    }

    /// The transaction cap may be tuned but not silently disabled: zero
    /// falls back to the default (system review 2026-10-07, G0.1).
    #[test]
    fn a_zero_tx_cap_falls_back_to_the_default() {
        std::env::remove_var("APITAP_TX_BUF_BYTES");
        assert_eq!(tx_buf_limit(), TX_BUF_LIMIT);
        std::env::set_var("APITAP_TX_BUF_BYTES", "0");
        assert_eq!(tx_buf_limit(), TX_BUF_LIMIT);
        std::env::set_var("APITAP_TX_BUF_BYTES", "512M");
        assert_eq!(tx_buf_limit(), 512 << 20);
        std::env::remove_var("APITAP_TX_BUF_BYTES");
    }

    /// The refusal must be actionable: it names the table, the size, and the
    /// knob that changes the cap (system review 2026-10-07, G0.1).
    #[test]
    fn the_refusal_names_the_table_the_size_and_the_knob() {
        let s = tx_too_big("public.big", 300 << 20, TX_BUF_LIMIT).to_string();
        assert!(s.contains("public.big"), "{s}");
        assert!(s.contains("300 MiB"), "{s}");
        assert!(s.contains("APITAP_TX_BUF_BYTES"), "{s}");
    }
}
