//! pgoutput logical-replication message decoding (protocol version 1).
//!
//! The walsender delivers XLogData payloads; each payload is ONE pgoutput
//! message. Text-mode tuples (we never pass the `binary` option): every
//! value arrives exactly as `SELECT col::text` renders it under the
//! session's `extra_float_digits=3` / UTC settings, which feeds apitap's
//! existing text→typed encoders unchanged.
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
    StreamAbort { xid: u32 },
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
    Text(Vec<u8>),
    /// The column is TOASTed and UNCHANGED — its value was not shipped.
    /// Apply must not touch this column at the destination.
    UnchangedToast,
}

pub(crate) type Tuple = Vec<Cell>;

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, pos: 0 }
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
    fn tuple(&mut self) -> Result<Tuple> {
        let n = self.u16()? as usize;
        let mut cols = Vec::with_capacity(n);
        for _ in 0..n {
            cols.push(match self.u8()? {
                b'n' => Cell::Null,
                b'u' => Cell::UnchangedToast,
                b't' => {
                    let len = self.u32()? as usize;
                    Cell::Text(self.take(len)?.to_vec())
                }
                // 'b' (binary) can only appear if we asked for it — we don't.
                other => {
                    return Err(Error::Transfer(format!(
                        "pgoutput: unexpected tuple-cell kind {:?}",
                        other as char
                    )))
                }
            });
        }
        Ok(cols)
    }
}

fn short() -> Error {
    Error::Transfer("pgoutput: truncated message".into())
}

/// Decode one pgoutput message (one XLogData payload).
/// `in_stream`: between StreamStart and StreamStop, DML/Relation/Type
/// messages carry a leading Int32 xid (proto v2) — strip it before the
/// v1-shaped body.
pub(crate) fn decode(payload: &[u8], in_stream: bool) -> Result<PgoMessage> {
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
            let xid = r.u32()?;
            PgoMessage::StreamAbort { xid }
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
            match r.u8()? {
                b'N' => {}
                other => {
                    return Err(Error::Transfer(format!(
                        "pgoutput insert: expected N tuple, got {:?}",
                        other as char
                    )))
                }
            }
            PgoMessage::Insert { rel_id, new: r.tuple()? }
        }
        b'U' => {
            let rel_id = r.u32()?;
            let mut old = None;
            let mut kind = r.u8()?;
            if kind == b'K' || kind == b'O' {
                old = Some(OldImage { full: kind == b'O', tuple: r.tuple()? });
                kind = r.u8()?;
            }
            if kind != b'N' {
                return Err(Error::Transfer(format!(
                    "pgoutput update: expected N tuple, got {:?}",
                    kind as char
                )));
            }
            PgoMessage::Update { rel_id, old, new: r.tuple()? }
        }
        b'D' => {
            let rel_id = r.u32()?;
            let kind = r.u8()?;
            if kind != b'K' && kind != b'O' {
                return Err(Error::Transfer(format!(
                    "pgoutput delete: expected K/O tuple, got {:?}",
                    kind as char
                )));
            }
            PgoMessage::Delete {
                rel_id,
                old: OldImage { full: kind == b'O', tuple: r.tuple()? },
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let opts = r.u8()?;
            let mut rel_ids = Vec::with_capacity(n);
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

    fn frame(parts: &[&[u8]]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn begin_commit_roundtrip() {
        let b = frame(&[b"B", &7u64.to_be_bytes(), &99i64.to_be_bytes(), &5u32.to_be_bytes()]);
        assert_eq!(
            decode(&b, false).unwrap(),
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
            decode(&c, false).unwrap(),
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
        let PgoMessage::Relation(r) = decode(&rel, false).unwrap() else { panic!() };
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
        assert_eq!(
            decode(&ins, false).unwrap(),
            PgoMessage::Insert {
                rel_id: 42,
                new: vec![Cell::Text(b"9".to_vec()), Cell::Null]
            }
        );
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
        let PgoMessage::Update { old, new, .. } = decode(&upd, false).unwrap() else { panic!() };
        let old = old.unwrap();
        assert!(!old.full);
        assert_eq!(old.tuple, vec![Cell::Text(b"9".to_vec())]);
        assert_eq!(new[1], Cell::UnchangedToast);
        // The empty string stays a VALUE, never Null.
        let ins = frame(&[
            b"I",
            &42u32.to_be_bytes(),
            b"N",
            &1u16.to_be_bytes(),
            b"t",
            &0u32.to_be_bytes(),
        ]);
        let PgoMessage::Insert { new, .. } = decode(&ins, false).unwrap() else { panic!() };
        assert_eq!(new[0], Cell::Text(Vec::new()));
        assert_ne!(new[0], Cell::Null);
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
            decode(&t, false).unwrap(),
            PgoMessage::Truncate { rel_ids: vec![42, 43], cascade: true, restart_identity: true }
        );
    }

    #[test]
    fn lsn_text_roundtrips() {
        assert_eq!(lsn_to_string(0x1_0000_002A), "1/2A");
        assert_eq!(lsn_from_string("1/2A").unwrap(), 0x1_0000_002A);
        assert_eq!(lsn_from_string("0/0").unwrap(), 0);
        assert!(lsn_from_string("nope").is_err());
    }
}
