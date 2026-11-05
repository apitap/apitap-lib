//! Transaction spill: one CDC transaction's buffered ops on local disk.
//!
//! A transaction is applied whole after its commit. Before 0.66 one larger than
//! `APITAP_TX_BUF_BYTES` was refused, so a single `UPDATE` over a big table
//! stopped the lane until someone split the write on the source. Its ops now
//! move to a file here once the limit is reached, and after the commit they
//! replay in window-sized chunks (see `drain::Replay`), so memory is bounded by
//! the window budget, not by the transaction.
//!
//! The format is private to one process and lives only as long as the
//! transaction does. It is deleted on drop, so a killed run leaves at most a
//! file in the spill directory and nothing that a later run reads: the slot
//! re-sends the transaction from its restart point. Records:
//!
//! ```text
//! slot u16 | sub_xid u32 | tag u8 | payload
//!   tag 0 Insert(tuple)  1 Update(has_old u8, [old], new, full u8)
//!       2 Delete(tuple)  3 Truncate
//! tuple: ncells u32, then per cell: kind u8 (0 null, 1 text, 2 unchanged
//!        toast) and, for text, len u32 + bytes
//! ```
//!
//! A cell is copied out of its frame, so a spilled row costs its values, not
//! the whole frame it arrived in.

use crate::error::{Error, Result};
use crate::logbased::drain::StreamOp;
use crate::wire::pgoutput::{CellR, Cellv, Tuple};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Where spill files go: `APITAP_SPILL_DIR`, else the system temp directory.
fn spill_dir() -> PathBuf {
    std::env::var_os("APITAP_SPILL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn io(what: &str, path: &std::path::Path, e: std::io::Error) -> Error {
    Error::Transfer(format!(
        "log_based: transaction spill {what} {} failed: {e} (APITAP_SPILL_DIR sets the directory)",
        path.display()
    ))
}

/// One transaction's ops, appended in WAL order, then read back once.
pub(crate) struct SpillFile {
    path: PathBuf,
    w: Option<BufWriter<File>>,
    /// Ops written: a replay reads exactly this many.
    ops: u64,
}

impl SpillFile {
    pub(crate) fn create() -> Result<Self> {
        let dir = spill_dir();
        std::fs::create_dir_all(&dir).map_err(|e| io("directory", &dir, e))?;
        let path = dir.join(format!(
            "apitap-txspill-{}-{}.bin",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let f = File::create(&path).map_err(|e| io("create", &path, e))?;
        Ok(Self {
            path,
            w: Some(BufWriter::with_capacity(1 << 20, f)),
            ops: 0,
        })
    }

    #[cfg(test)]
    fn ops(&self) -> u64 {
        self.ops
    }

    pub(crate) fn push(&mut self, slot: u16, op: &StreamOp, sub: u32) -> Result<()> {
        let mut rec = Vec::with_capacity(64);
        rec.extend_from_slice(&slot.to_le_bytes());
        rec.extend_from_slice(&sub.to_le_bytes());
        match op {
            StreamOp::Insert(t) => {
                rec.push(0);
                put_tuple(&mut rec, t);
            }
            StreamOp::Update(old, new, full) => {
                rec.push(1);
                rec.push(u8::from(old.is_some()));
                if let Some(o) = old {
                    put_tuple(&mut rec, o);
                }
                put_tuple(&mut rec, new);
                rec.push(u8::from(*full));
            }
            StreamOp::Delete(t) => {
                rec.push(2);
                put_tuple(&mut rec, t);
            }
            StreamOp::Truncate => rec.push(3),
        }
        let w = self.w.as_mut().ok_or_else(|| {
            Error::Transfer("log_based: transaction spill written after it was read".into())
        })?;
        w.write_all(&rec).map_err(|e| io("write", &self.path, e))?;
        self.ops += 1;
        Ok(())
    }

    /// Finish writing and read the ops back in order. The file is deleted
    /// when the reader drops.
    pub(crate) fn into_reader(mut self) -> Result<SpillReader> {
        if let Some(mut w) = self.w.take() {
            w.flush().map_err(|e| io("flush", &self.path, e))?;
        }
        let f = File::open(&self.path).map_err(|e| io("reopen", &self.path, e))?;
        let path = std::mem::take(&mut self.path);
        Ok(SpillReader {
            r: BufReader::with_capacity(1 << 20, f),
            left: self.ops,
            path,
        })
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        if !self.path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A spilled transaction read back once, front to back.
pub(crate) struct SpillReader {
    r: BufReader<File>,
    left: u64,
    path: PathBuf,
}

impl SpillReader {
    /// The next op, or `None` once every written op was read.
    pub(crate) fn next_op(&mut self) -> Result<Option<(u16, StreamOp, u32)>> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let slot = u16::from_le_bytes(self.take::<2>()?);
        let sub = u32::from_le_bytes(self.take::<4>()?);
        let op = match self.take::<1>()?[0] {
            0 => StreamOp::Insert(self.tuple()?),
            1 => {
                let old = match self.take::<1>()?[0] {
                    0 => None,
                    _ => Some(self.tuple()?),
                };
                let new = self.tuple()?;
                let full = self.take::<1>()?[0] != 0;
                StreamOp::Update(old, new, full)
            }
            2 => StreamOp::Delete(self.tuple()?),
            3 => StreamOp::Truncate,
            t => return Err(self.corrupt(&format!("op tag {t}"))),
        };
        Ok(Some((slot, op, sub)))
    }

    fn corrupt(&self, what: &str) -> Error {
        Error::Transfer(format!(
            "log_based: transaction spill {} is corrupt ({what})",
            self.path.display()
        ))
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut b = [0u8; N];
        self.r
            .read_exact(&mut b)
            .map_err(|e| io("read", &self.path, e))?;
        Ok(b)
    }

    fn tuple(&mut self) -> Result<Tuple> {
        let n = u32::from_le_bytes(self.take::<4>()?) as usize;
        let mut frame = Vec::new();
        let mut cells = Vec::with_capacity(n);
        for _ in 0..n {
            match self.take::<1>()?[0] {
                0 => cells.push(CellR::Null),
                2 => cells.push(CellR::UnchangedToast),
                1 => {
                    let len = u32::from_le_bytes(self.take::<4>()?);
                    let off = u32::try_from(frame.len()).map_err(|_| self.corrupt("row past 4 GiB"))?;
                    let start = frame.len();
                    frame.resize(start + len as usize, 0);
                    self.r
                        .read_exact(&mut frame[start..])
                        .map_err(|e| io("read", &self.path, e))?;
                    cells.push(CellR::Text(off, len));
                }
                k => return Err(self.corrupt(&format!("cell kind {k}"))),
            }
        }
        Ok(Tuple {
            frame: bytes::Bytes::from(frame),
            cells,
        })
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn put_tuple(rec: &mut Vec<u8>, t: &Tuple) {
    rec.extend_from_slice(&(t.len() as u32).to_le_bytes());
    for i in 0..t.len() {
        match t.view(i) {
            Cellv::Null => rec.push(0),
            Cellv::UnchangedToast => rec.push(2),
            Cellv::Text(b) => {
                rec.push(1);
                rec.extend_from_slice(&(b.len() as u32).to_le_bytes());
                rec.extend_from_slice(b);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tup(cells: &[Option<&str>]) -> Tuple {
        let mut frame = Vec::new();
        let mut cs = Vec::new();
        for c in cells {
            match c {
                None => cs.push(CellR::Null),
                Some("<toast>") => cs.push(CellR::UnchangedToast),
                Some(s) => {
                    cs.push(CellR::Text(frame.len() as u32, s.len() as u32));
                    frame.extend_from_slice(s.as_bytes());
                }
            }
        }
        Tuple {
            frame: bytes::Bytes::from(frame),
            cells: cs,
        }
    }

    fn same(a: &Tuple, b: &Tuple) -> bool {
        a.len() == b.len() && (0..a.len()).all(|i| a.view(i) == b.view(i))
    }

    /// Every op kind, NULLs, unchanged TOAST, empty strings and raw bytes
    /// come back as written, in order, and the file is gone after the read.
    #[test]
    fn ops_round_trip_in_order_and_the_file_is_deleted() {
        let dir = std::env::temp_dir().join(format!("apitap-spill-test-{}", std::process::id()));
        std::env::set_var("APITAP_SPILL_DIR", &dir);
        let ins = tup(&[Some("1"), None, Some(""), Some("ünï\\x00")]);
        let old = tup(&[Some("1"), Some("<toast>")]);
        let new = tup(&[Some("2"), Some("<toast>")]);
        let del = tup(&[Some("9")]);
        let mut f = SpillFile::create().unwrap();
        f.push(3, &StreamOp::Insert(ins.clone()), 77).unwrap();
        f.push(4, &StreamOp::Update(Some(old.clone()), new.clone(), true), 78).unwrap();
        f.push(4, &StreamOp::Update(None, new.clone(), false), 77).unwrap();
        f.push(5, &StreamOp::Delete(del.clone()), 77).unwrap();
        f.push(6, &StreamOp::Truncate, 79).unwrap();
        assert_eq!(f.ops(), 5);
        let path = f.path.clone();
        let mut r = f.into_reader().unwrap();
        match r.next_op().unwrap().unwrap() {
            (3, StreamOp::Insert(t), 77) => assert!(same(&t, &ins)),
            _ => panic!("insert"),
        }
        match r.next_op().unwrap().unwrap() {
            (4, StreamOp::Update(Some(o), n, true), 78) => assert!(same(&o, &old) && same(&n, &new)),
            _ => panic!("update with old"),
        }
        match r.next_op().unwrap().unwrap() {
            (4, StreamOp::Update(None, n, false), 77) => assert!(same(&n, &new)),
            _ => panic!("update without old"),
        }
        match r.next_op().unwrap().unwrap() {
            (5, StreamOp::Delete(t), 77) => assert!(same(&t, &del)),
            _ => panic!("delete"),
        }
        assert!(matches!(r.next_op().unwrap(), Some((6, StreamOp::Truncate, 79))));
        assert!(r.next_op().unwrap().is_none());
        assert!(path.exists());
        drop(r);
        assert!(!path.exists(), "the reader deletes the file");
        std::env::remove_var("APITAP_SPILL_DIR");
        let _ = std::fs::remove_dir(&dir);
    }

    /// A transaction that aborts never reads its file back: dropping the
    /// writer deletes it.
    #[test]
    fn an_unread_spill_is_deleted_on_drop() {
        let mut f = SpillFile::create().unwrap();
        f.push(1, &StreamOp::Truncate, 1).unwrap();
        let path = f.path.clone();
        assert!(path.exists());
        drop(f);
        assert!(!path.exists());
    }
}
