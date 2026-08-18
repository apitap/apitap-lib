//! pgoutput logical-replication message decoding (protocol version 1).
//!
//! The walsender delivers XLogData payloads; each payload is ONE pgoutput
//! message. Text-mode tuples by default: every value arrives exactly as
//! `SELECT col::text` renders it under the session's UTC settings, which
//! feeds apitap's existing text→typed encoders unchanged.
//!
//! With `APITAP_PG_BINARY=1` the walsender ships `binary 'true'` tuples
//! (PG14+) and the `'b'` cells are rendered BACK to those exact text bytes
//! here (wire::pgbindec), at decode time, using the Relation's type OIDs.
//! Nothing downstream can tell the difference — the point is WHERE the text
//! rendering runs: the type output functions otherwise execute inside the
//! single pegged walsender process that is the per-slot ceiling
//! (benchmarks/gcp-cdc-100tables.md Part 7), while apitap's core idles.
//!
//! Correctness contracts baked in here (learned from ape-dts and
//! PipelineWise, see docs/design/log_based.md):
//! - `TupleData` keeps its THREE arms distinct — `Null`, `Text`, and
//!   `UnchangedToast`. Collapsing unchanged-TOAST into NULL silently
//!   destroys data on apply; collapsing empty-`Text` into NULL corrupts
//!   empty strings (a real bug in ape-dts's converter).
//! - Relation messages are the schema authority: tuple data is positional
//!   in WAL column order, which may differ from catalog order.
//! - Only `Commit.end_lsn` values are candidate watermarks; the drain
//!   layer stamps rows with the PREVIOUS transaction's end LSN so any
//!   restart replays whole transactions.

use crate::error::{Error, Result};
use crate::wire::pgbindec;
use std::collections::HashMap;
use std::sync::Arc;

/// Column type OIDs per relation id, harvested from Relation messages by the
/// drain layer — the schema needed to render `binary 'true'` tuples back to
/// text. Text-mode drains pass an empty map (nothing is ever looked up).
pub(crate) type RelOids = HashMap<u32, Arc<Vec<u32>>>;

/// One decoded pgoutput message.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PgoMessage {
    Begin {
        final_lsn: u64,
        commit_ts_us: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts_us: i64,
    },
    /// Replication origin — cross-cluster forwarding metadata; ignored.
    Origin,
    Relation(Relation),
    /// Composite-type metadata; we resolve types via OIDs, ignored.
    Type,
    Insert {
        rel_id: u32,
        new: Tuple,
    },
    Update {
        rel_id: u32,
        /// `K` (key) or `O` (old, REPLICA IDENTITY FULL) image when present.
        old: Option<OldImage>,
        new: Tuple,
    },
    Delete {
        rel_id: u32,
        old: OldImage,
    },
    Truncate {
        rel_ids: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
    /// proto v2 streaming: an in-progress transaction's block opens. Until
    /// the matching StreamStop, every DML/Relation message carries a leading
    /// xid (the decoder strips it when `in_stream`).
    StreamStart { xid: u32 },
    StreamStop,
    /// The streamed transaction committed — its buffered ops are real now.
    StreamCommit { xid: u32, end_lsn: u64 },
    /// The streamed transaction rolled back — drop everything buffered.
    StreamAbort { xid: u32, sub_xid: u32 },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OldImage {
    /// true = `O` (full old tuple); false = `K` (replica-identity key only).
    pub full: bool,
    pub tuple: Tuple,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Relation {
    pub rel_id: u32,
    pub namespace: String,
    pub name: String,
    /// 'd' default | 'n' nothing | 'f' full | 'i' index.
    pub replica_identity: u8,
    pub cols: Vec<RelationCol>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RelationCol {
    /// Part of the replica-identity key.
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_mod: i32,
}

/// One column of one tuple. The three arms are load-bearing — see module docs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell {
    Null,
    /// Text-format value bytes (UTF-8 by construction on the wire).
    ///
    /// `Bytes`, not `Vec<u8>`: one XLogData frame is one pgoutput message is
    /// one row, so every cell is a refcounted slice of the frame the row
    /// arrived in — a pointer bump instead of a heap allocation. A 15-column
    /// row cost 15 mallocs and 15 frees before; it costs none now. At 1.35M
    /// changes that is 22.5M allocation pairs removed. Measured motivation:
    /// `perf` put ~40% of a capped drain's samples in the glibc malloc/free
    /// region, and swapping in tcmalloc/jemalloc — which makes those
    /// allocations cheap without removing them — won 6 rounds out of 6.
    /// Every glibc tuning knob we tried (M_MMAP_THRESHOLD, M_TRIM_THRESHOLD
    /// at two sizes, both together) was a wash or worse; see
    /// benchmarks/cdc-apply-profile.md.
    Text(bytes::Bytes),
    /// The column is TOASTed and UNCHANGED — its value was not shipped.
    /// Apply must not touch this column at the destination.
    UnchangedToast,
}

/// One cell as a RANGE into its row's frame — `Copy`, 12 bytes against the
/// 40 of `Cell`, and crucially: no refcount. The 0.28.0 Bytes cells removed
/// 22.5M mallocs but left 45M atomic ops plus one Arc-promotion alloc per
/// frame; ranges remove those too. `Cell` (owned) survives for the residue
/// tail, which is rare and materializes explicitly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CellR {
    Null,
    /// (offset, len) into `Tuple::frame`.
    Text(u32, u32),
    UnchangedToast,
}

/// A borrowed view of one cell — consumers pattern-match this exactly like
/// they matched `Cell`, with `&[u8]` where `Bytes` used to be.
#[derive(Debug, PartialEq)]
pub(crate) enum Cellv<'a> {
    Null,
    Text(&'a [u8]),
    UnchangedToast,
}

/// One row: the frame it arrived in plus range cells. Clone = one refcount
/// bump + one small memcpy of the range vec, however wide the row is.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Tuple {
    pub frame: bytes::Bytes,
    pub cells: Vec<CellR>,
}

impl Tuple {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.cells.len()
    }
    #[inline]
    pub(crate) fn view(&self, i: usize) -> Cellv<'_> {
        match self.cells[i] {
            CellR::Null => Cellv::Null,
            CellR::Text(o, l) => Cellv::Text(&self.frame[o as usize..(o + l) as usize]),
            CellR::UnchangedToast => Cellv::UnchangedToast,
        }
    }
    #[inline]
    pub(crate) fn get(&self, i: usize) -> Option<Cellv<'_>> {
        if i < self.cells.len() {
            Some(self.view(i))
        } else {
            None
        }
    }
    pub(crate) fn views(&self) -> impl Iterator<Item = Cellv<'_>> + '_ {
        (0..self.cells.len()).map(|i| self.view(i))
    }
    pub(crate) fn has_toast(&self) -> bool {
        self.cells.iter().any(|c| matches!(c, CellR::UnchangedToast))
    }
    /// Materialize owned cells — the residue tail's format. Rare by design.
    pub(crate) fn to_cells(&self) -> Vec<Cell> {
        self.cells
            .iter()
            .map(|c| match *c {
                CellR::Null => Cell::Null,
                CellR::Text(o, l) => {
                    Cell::Text(self.frame.slice(o as usize..(o + l) as usize))
                }
                CellR::UnchangedToast => Cell::UnchangedToast,
            })
            .collect()
    }
    /// Build a Tuple that owns freshly rendered bytes (the MySQL lane, whose
    /// decoder renders values rather than slicing a shared frame).
    pub(crate) fn from_rendered(frame: Vec<u8>, cells: Vec<CellR>) -> Self {
        Self { frame: bytes::Bytes::from(frame), cells }
    }

    /// Build from owned cells by concatenation — test rigs and slow paths.
    #[allow(dead_code)]
    pub(crate) fn from_cells(cells: &[Cell]) -> Self {
        let mut frame = Vec::new();
        let mut rs = Vec::with_capacity(cells.len());
        for c in cells {
            match c {
                Cell::Null => rs.push(CellR::Null),
                Cell::UnchangedToast => rs.push(CellR::UnchangedToast),
                Cell::Text(t) => {
                    let o = frame.len() as u32;
                    frame.extend_from_slice(t);
                    rs.push(CellR::Text(o, t.len() as u32));
                }
            }
        }
        Tuple::from_rendered(frame, rs)
    }
}

struct Reader<'a> {
    b: &'a [u8],
    /// The frame, kept so tuples can carry it out whole — cells are ranges
    /// into it, resolved lazily by `Tuple::view`.
    src: &'a bytes::Bytes,
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(src: &'a bytes::Bytes) -> Self {
        Self { b: &src[..], src, pos: 0 }
    }
    fn u8(&mut self) -> Result<u8> {
        let v = *self.b.get(self.pos).ok_or_else(short)?;
        self.pos += 1;
        Ok(v)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Bytes still unread — used to bound a wire-supplied count before it
    /// becomes an allocation.
    fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(short)?;
        let s = self.b.get(self.pos..end).ok_or_else(short)?;
        self.pos = end;
        Ok(s)
    }
    fn cstr(&mut self) -> Result<String> {
        let rest = &self.b[self.pos..];
        let nul = rest
            .iter()
            .position(|&c| c == 0)
            .ok_or_else(|| Error::Transfer("pgoutput: unterminated string".into()))?;
        let s = std::str::from_utf8(&rest[..nul])
            .map_err(|_| Error::Transfer("pgoutput: non-UTF-8 identifier".into()))?
            .to_string();
        self.pos += nul + 1;
        Ok(s)
    }
    fn tuple(&mut self, oids: Option<&[u32]>) -> Result<Tuple> {
        let n = self.u16()? as usize;
        let mut cells = Vec::with_capacity(n);
        // Indices of `'b'` cells (their CellR ranges point at BINARY bytes
        // until the rebuild below). Vec::new() never allocates until the
        // first push, so the text-mode fast path pays nothing for this.
        let mut bin_idx: Vec<usize> = Vec::new();
        for i in 0..n {
            cells.push(match self.u8()? {
                b'n' => CellR::Null,
                b'u' => CellR::UnchangedToast,
                b't' => {
                    let len = self.u32()? as usize;
                    // A RANGE into the frame — no slice_ref, no refcount, no
                    // promotion. An empty value is Text(o, 0), still `Text`,
                    // never `Null` — that distinction is load-bearing (module
                    // docs + the empty-string test).
                    let start = self.pos as u32;
                    self.take(len)?;
                    CellR::Text(start, len as u32)
                }
                // 'b': send-format bytes (the session passed `binary 'true'`).
                b'b' => {
                    let len = self.u32()? as usize;
                    let start = self.pos as u32;
                    self.take(len)?;
                    bin_idx.push(i);
                    CellR::Text(start, len as u32)
                }
                other => {
                    return Err(Error::Transfer(format!(
                        "pgoutput: unexpected tuple-cell kind {:?}",
                        other as char
                    )))
                }
            });
        }
        if bin_idx.is_empty() {
            return Ok(Tuple { frame: self.src.clone(), cells });
        }
        // Binary tuple: rebuild an OWNED frame with every `'b'` cell rendered
        // to its PG-text form and every `'t'` cell copied through. One
        // allocation per row — the deliberate trade: this rendering otherwise
        // runs inside the pegged walsender.
        let oids = oids.ok_or_else(|| {
            Error::Transfer(
                "pgoutput: binary tuple for a relation whose Relation message \
                 was never seen — protocol desync?"
                    .into(),
            )
        })?;
        if oids.len() != n {
            return Err(Error::Transfer(format!(
                "pgoutput: binary tuple has {n} cells but the relation \
                 declared {} columns",
                oids.len()
            )));
        }
        let mut frame = Vec::with_capacity(self.pos);
        let mut out_cells = Vec::with_capacity(n);
        let mut bi = 0usize;
        for (i, c) in cells.iter().enumerate() {
            match *c {
                CellR::Null => out_cells.push(CellR::Null),
                CellR::UnchangedToast => out_cells.push(CellR::UnchangedToast),
                CellR::Text(o, l) => {
                    let start = frame.len() as u32;
                    let raw = &self.src[o as usize..(o + l) as usize];
                    if bin_idx.get(bi) == Some(&i) {
                        bi += 1;
                        pgbindec::render(oids[i], raw, &mut frame)?;
                    } else {
                        frame.extend_from_slice(raw);
                    }
                    out_cells.push(CellR::Text(start, frame.len() as u32 - start));
                }
            }
        }
        Ok(Tuple::from_rendered(frame, out_cells))
    }
}

fn short() -> Error {
    Error::Transfer("pgoutput: truncated message".into())
}

/// Decode one pgoutput message (one XLogData payload).
/// `in_stream`: between StreamStart and StreamStop, DML/Relation/Type
/// messages carry a leading Int32 xid (proto v2) — strip it before the
/// v1-shaped body.
/// `rel_oids`: per-relation column type OIDs, consulted ONLY when a tuple
/// carries `'b'` (binary) cells — pass an empty map for text-mode streams.
pub(crate) fn decode(
    payload: &bytes::Bytes,
    in_stream: bool,
    rel_oids: &RelOids,
) -> Result<PgoMessage> {
    let mut r = Reader::new(payload);
    let tag = r.u8()?;
    if in_stream && matches!(tag, b'R' | b'Y' | b'I' | b'U' | b'D' | b'T') {
        let _xid = r.u32()?;
    }
    Ok(match tag {
        b'S' => {
            let xid = r.u32()?;
            let _first_segment = r.u8()?;
            PgoMessage::StreamStart { xid }
        }
        b'E' => PgoMessage::StreamStop,
        b'c' => {
            let xid = r.u32()?;
            let _flags = r.u8()?;
            let _commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            PgoMessage::StreamCommit { xid, end_lsn }
        }
        b'A' => {
            // Stream Abort carries TWO xids: the top-level transaction and the
            // SUBtransaction that aborted. Reading only the first and dropping
            // that buffer discards the whole transaction whenever a
            // `ROLLBACK TO SAVEPOINT` happens inside a streamed one — the
            // later Stream Commit then finds nothing, applies nothing, and
            // still advances the watermark. Silent, permanent loss.
            //
            // A subtransaction abort is not something this decoder can undo
            // selectively (the ops it buffered are not tagged by subxid), so
            // it is reported for what it is and the caller refuses. Rare and
            // loud beats common and silent.
            let xid = r.u32()?;
            let sub_xid = r.u32().unwrap_or(xid);
            PgoMessage::StreamAbort { xid, sub_xid }
        }
        b'B' => PgoMessage::Begin {
            final_lsn: r.u64()?,
            commit_ts_us: r.i64()?,
            xid: r.u32()?,
        },
        b'C' => {
            let _flags = r.u8()?;
            PgoMessage::Commit {
                commit_lsn: r.u64()?,
                end_lsn: r.u64()?,
                commit_ts_us: r.i64()?,
            }
        }
        b'O' => PgoMessage::Origin,
        b'Y' => PgoMessage::Type,
        b'R' => {
            let rel_id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let ncols = r.u16()? as usize;
            let mut cols = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                cols.push(RelationCol {
                    key: r.u8()? == 1,
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    type_mod: r.i32()?,
                });
            }
            PgoMessage::Relation(Relation { rel_id, namespace, name, replica_identity, cols })
        }
        b'I' => {
            let rel_id = r.u32()?;
            let oids = rel_oids.get(&rel_id).map(|a| a.as_slice());
            match r.u8()? {
                b'N' => {}
                other => {
                    return Err(Error::Transfer(format!(
                        "pgoutput insert: expected N tuple, got {:?}",
                        other as char
                    )))
                }
            }
            PgoMessage::Insert { rel_id, new: r.tuple(oids)? }
        }
        b'U' => {
            let rel_id = r.u32()?;
            let oids = rel_oids.get(&rel_id).map(|a| a.as_slice());
            let mut old = None;
            let mut kind = r.u8()?;
            if kind == b'K' || kind == b'O' {
                old = Some(OldImage { full: kind == b'O', tuple: r.tuple(oids)? });
                kind = r.u8()?;
            }
            if kind != b'N' {
                return Err(Error::Transfer(format!(
                    "pgoutput update: expected N tuple, got {:?}",
                    kind as char
                )));
            }
            PgoMessage::Update { rel_id, old, new: r.tuple(oids)? }
        }
        b'D' => {
            let rel_id = r.u32()?;
            let oids = rel_oids.get(&rel_id).map(|a| a.as_slice());
            let kind = r.u8()?;
            if kind != b'K' && kind != b'O' {
                return Err(Error::Transfer(format!(
                    "pgoutput delete: expected K/O tuple, got {:?}",
                    kind as char
                )));
            }
            PgoMessage::Delete {
                rel_id,
                old: OldImage { full: kind == b'O', tuple: r.tuple(oids)? },
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let opts = r.u8()?;
            // `n` is a wire u32, and reserving on it would let a truncated or
            // hostile frame ask for 16 GB before the first read fails. Four
            // bytes per id is the floor, so the remaining buffer bounds how
            // many there can actually be.
            let mut rel_ids = Vec::with_capacity(n.min(r.remaining() / 4 + 1));
            for _ in 0..n {
                rel_ids.push(r.u32()?);
            }
            PgoMessage::Truncate {
                rel_ids,
                cascade: opts & 1 != 0,
                restart_identity: opts & 2 != 0,
            }
        }
        other => {
            return Err(Error::Transfer(format!(
                "pgoutput: unknown message tag {:?} — is the publication using \
                 proto_version 1?",
                other as char
            )))
        }
    })
}

/// Render an LSN the way Postgres prints `pg_lsn` (`X/Y`).
pub(crate) fn lsn_to_string(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse Postgres's `X/Y` LSN text form.
pub(crate) fn lsn_from_string(s: &str) -> Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| Error::Transfer(format!("bad LSN '{s}'")))?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| Error::Transfer(format!("bad LSN '{s}'")))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| Error::Transfer(format!("bad LSN '{s}'")))?;
    Ok((hi << 32) | lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(parts: &[&[u8]]) -> bytes::Bytes {
        bytes::Bytes::from(parts.concat())
    }

    #[test]
    fn begin_commit_roundtrip() {
        let b = frame(&[b"B", &7u64.to_be_bytes(), &99i64.to_be_bytes(), &5u32.to_be_bytes()]);
        assert_eq!(
            decode(&b, false, &RelOids::new()).unwrap(),
            PgoMessage::Begin { final_lsn: 7, commit_ts_us: 99, xid: 5 }
        );
        let c = frame(&[
            b"C",
            &[0u8],
            &7u64.to_be_bytes(),
            &8u64.to_be_bytes(),
            &99i64.to_be_bytes(),
        ]);
        assert_eq!(
            decode(&c, false, &RelOids::new()).unwrap(),
            PgoMessage::Commit { commit_lsn: 7, end_lsn: 8, commit_ts_us: 99 }
        );
    }

    #[test]
    fn relation_and_insert_decode_positionally() {
        let rel = frame(&[
            b"R",
            &42u32.to_be_bytes(),
            b"public\0",
            b"t\0",
            b"d",
            &2u16.to_be_bytes(),
            &[1u8],
            b"id\0",
            &20u32.to_be_bytes(),
            &(-1i32).to_be_bytes(),
            &[0u8],
            b"v\0",
            &25u32.to_be_bytes(),
            &(-1i32).to_be_bytes(),
        ]);
        let PgoMessage::Relation(r) = decode(&rel, false, &RelOids::new()).unwrap() else { panic!() };
        assert_eq!(r.rel_id, 42);
        assert_eq!(r.replica_identity, b'd');
        assert!(r.cols[0].key && !r.cols[1].key);
        assert_eq!((r.cols[0].type_oid, r.cols[1].type_oid), (20, 25));

        let ins = frame(&[
            b"I",
            &42u32.to_be_bytes(),
            b"N",
            &2u16.to_be_bytes(),
            b"t",
            &1u32.to_be_bytes(),
            b"9",
            b"n",
        ]);
        let PgoMessage::Insert { rel_id, new } = decode(&ins, false, &RelOids::new()).unwrap() else { panic!() };
        assert_eq!(rel_id, 42);
        assert_eq!(new.to_cells(), vec![Cell::Text(bytes::Bytes::from_static(b"9")), Cell::Null]);
    }

    #[test]
    fn update_arms_and_unchanged_toast_survive() {
        // Update with key old-image; new tuple has an unchanged-toast column.
        let upd = frame(&[
            b"U",
            &42u32.to_be_bytes(),
            b"K",
            &1u16.to_be_bytes(),
            b"t",
            &1u32.to_be_bytes(),
            b"9",
            b"N",
            &2u16.to_be_bytes(),
            b"t",
            &1u32.to_be_bytes(),
            b"9",
            b"u",
        ]);
        let PgoMessage::Update { old, new, .. } = decode(&upd, false, &RelOids::new()).unwrap() else { panic!() };
        let old = old.unwrap();
        assert!(!old.full);
        assert_eq!(old.tuple.to_cells(), vec![Cell::Text(bytes::Bytes::from_static(b"9"))]);
        assert_eq!(new.view(1), Cellv::UnchangedToast);
        // The empty string stays a VALUE, never Null.
        let ins = frame(&[
            b"I",
            &42u32.to_be_bytes(),
            b"N",
            &1u16.to_be_bytes(),
            b"t",
            &0u32.to_be_bytes(),
        ]);
        let PgoMessage::Insert { new, .. } = decode(&ins, false, &RelOids::new()).unwrap() else { panic!() };
        assert_eq!(new.view(0), Cellv::Text(b""));
        assert_ne!(new.view(0), Cellv::Null);
    }

    #[test]
    fn truncate_carries_all_relations() {
        let t = frame(&[
            b"T",
            &2u32.to_be_bytes(),
            &[3u8],
            &42u32.to_be_bytes(),
            &43u32.to_be_bytes(),
        ]);
        assert_eq!(
            decode(&t, false, &RelOids::new()).unwrap(),
            PgoMessage::Truncate { rel_ids: vec![42, 43], cascade: true, restart_identity: true }
        );
    }

    #[test]
    fn binary_cells_render_to_the_exact_text_plane() {
        // Insert for rel 42 (int8, text, numeric): b(int8 7), t("x"), b(numeric 1234.50).
        let mut numeric = Vec::new();
        numeric.extend(2u16.to_be_bytes()); // ndigits
        numeric.extend(0i16.to_be_bytes()); // weight
        numeric.extend(0u16.to_be_bytes()); // sign +
        numeric.extend(2u16.to_be_bytes()); // dscale
        numeric.extend(1234u16.to_be_bytes());
        numeric.extend(5000u16.to_be_bytes());
        let ins = frame(&[
            b"I",
            &42u32.to_be_bytes(),
            b"N",
            &3u16.to_be_bytes(),
            b"b",
            &8u32.to_be_bytes(),
            &7i64.to_be_bytes(),
            b"t",
            &1u32.to_be_bytes(),
            b"x",
            b"b",
            &(numeric.len() as u32).to_be_bytes(),
            &numeric,
        ]);
        let mut oids = RelOids::new();
        oids.insert(42, Arc::new(vec![20, 25, 1700]));
        let PgoMessage::Insert { new, .. } = decode(&ins, false, &oids).unwrap() else {
            panic!()
        };
        assert_eq!(new.view(0), Cellv::Text(b"7"));
        assert_eq!(new.view(1), Cellv::Text(b"x"));
        assert_eq!(new.view(2), Cellv::Text(b"1234.50"));
        // Without the relation's OIDs the same payload fails loudly.
        assert!(decode(&ins, false, &RelOids::new()).is_err());
    }

    #[test]
    fn lsn_text_roundtrips() {
        assert_eq!(lsn_to_string(0x1_0000_002A), "1/2A");
        assert_eq!(lsn_from_string("1/2A").unwrap(), 0x1_0000_002A);
        assert_eq!(lsn_from_string("0/0").unwrap(), 0);
        assert!(lsn_from_string("nope").is_err());
    }
}
