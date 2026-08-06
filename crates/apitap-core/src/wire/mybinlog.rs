//! MySQL binlog (ROW format) → the SAME message shape `pgoutput` produces,
//! so everything downstream — collapse, the four destination appliers, the
//! window/watermark machinery — is reused verbatim and never learns which
//! database the events came from.
//!
//! Scope: the events a ROW-format replica must understand —
//! FORMAT_DESCRIPTION(15), ROTATE(4), TABLE_MAP(19), WRITE/UPDATE/
//! DELETE_ROWS_v2(30/31/32), XID(16), QUERY(2, for BEGIN and DDL),
//! HEARTBEAT(27/41). Values decode from binlog's packed binary into TEXT
//! bytes, because that is the currency every apply path already speaks
//! (`Cell::Text`, UTC-normalised temporals, `\x` hex for binary).
//!
//! Confirmed against a live MySQL 8.0 (the `binlog_probe_live` test in
//! `mywire`): the declared `event_size` INCLUDES the CRC32 trailer, and
//! artificial events (the initial ROTATE, heartbeats) carry it too — so
//! stripping 4 bytes uniformly when checksums are on is correct.

use crate::error::{Error, Result};
use crate::wire::pgoutput::{Cell, OldImage, PgoMessage, Relation, RelationCol, Tuple};
use std::collections::HashMap;

/// The 19-byte common header every event after v4 carries.
pub(crate) const HEADER_LEN: usize = 19;

// Event type codes.
const EV_QUERY: u8 = 2;
const EV_ROTATE: u8 = 4;
const EV_FORMAT_DESCRIPTION: u8 = 15;
const EV_XID: u8 = 16;
const EV_TABLE_MAP: u8 = 19;
const EV_WRITE_ROWS: u8 = 30;
const EV_UPDATE_ROWS: u8 = 31;
const EV_DELETE_ROWS: u8 = 32;
const EV_HEARTBEAT: u8 = 27;
const EV_HEARTBEAT_V2: u8 = 41;
const EV_TRANSACTION_PAYLOAD: u8 = 40;

// The MYSQL_TYPE_* codes TABLE_MAP reports.
const MT_DECIMAL: u8 = 0x00;
const MT_TINY: u8 = 0x01;
const MT_SHORT: u8 = 0x02;
const MT_LONG: u8 = 0x03;
const MT_FLOAT: u8 = 0x04;
const MT_DOUBLE: u8 = 0x05;
const MT_NULL: u8 = 0x06;
const MT_TIMESTAMP: u8 = 0x07;
const MT_LONGLONG: u8 = 0x08;
const MT_INT24: u8 = 0x09;
const MT_DATE: u8 = 0x0a;
const MT_TIME: u8 = 0x0b;
const MT_DATETIME: u8 = 0x0c;
const MT_YEAR: u8 = 0x0d;
const MT_NEWDATE: u8 = 0x0e;
const MT_VARCHAR: u8 = 0x0f;
const MT_BIT: u8 = 0x10;
const MT_TIMESTAMP2: u8 = 0x11;
const MT_DATETIME2: u8 = 0x12;
const MT_TIME2: u8 = 0x13;
const MT_JSON: u8 = 0xf5;
const MT_NEWDECIMAL: u8 = 0xf6;
const MT_ENUM: u8 = 0xf7;
const MT_SET: u8 = 0xf8;
const MT_TINY_BLOB: u8 = 0xf9;
const MT_MEDIUM_BLOB: u8 = 0xfa;
const MT_LONG_BLOB: u8 = 0xfb;
const MT_BLOB: u8 = 0xfc;
const MT_VAR_STRING: u8 = 0xfd;
const MT_STRING: u8 = 0xfe;
const MT_GEOMETRY: u8 = 0xff;

fn bad(what: &str) -> Error {
    Error::Transfer(format!("mysql binlog: {what}"))
}

/// The parsed common header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub event_size: u32,
    /// Position of the NEXT event in the current file (0 on artificial ones).
    pub log_pos: u32,
    pub flags: u16,
}

/// Split one raw event into (header, body) with the CRC32 trailer removed
/// when the session negotiated checksums. Verified live: `event_size`
/// counts the trailer, and artificial events carry it too.
pub(crate) fn split_event(ev: &[u8], checksummed: bool) -> Result<(EventHeader, &[u8])> {
    if ev.len() < HEADER_LEN {
        return Err(bad("event shorter than its header"));
    }
    let h = EventHeader {
        timestamp: u32::from_le_bytes(ev[0..4].try_into().unwrap()),
        event_type: ev[4],
        event_size: u32::from_le_bytes(ev[9..13].try_into().unwrap()),
        log_pos: u32::from_le_bytes(ev[13..17].try_into().unwrap()),
        flags: u16::from_le_bytes(ev[17..19].try_into().unwrap()),
    };
    let end = if checksummed {
        ev.len().checked_sub(4).ok_or_else(|| bad("no room for checksum"))?
    } else {
        ev.len()
    };
    if end < HEADER_LEN {
        return Err(bad("event body underflow"));
    }
    Ok((h, &ev[HEADER_LEN..end]))
}

/// One column as TABLE_MAP describes it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColDef {
    pub kind: u8,
    /// Per-type metadata from TABLE_MAP (0, 1 or 2 bytes, packed as u16).
    pub meta: u16,
    pub nullable: bool,
    /// Unsigned integer column (from the optional SIGNEDNESS metadata, or
    /// resolved from information_schema when the server sends MINIMAL).
    pub unsigned: bool,
}

/// A live TABLE_MAP entry: what the numeric table id currently means.
#[derive(Debug, Clone)]
pub(crate) struct TableMap {
    pub table_id: u64,
    pub db: String,
    pub table: String,
    pub cols: Vec<ColDef>,
}

/// Schema facts the binlog itself does not carry under
/// `binlog_row_metadata=MINIMAL` — column names and key flags come from
/// information_schema, keyed by `db.table`.
#[derive(Debug, Clone)]
pub(crate) struct TableSchema {
    pub names: Vec<String>,
    /// Primary-key membership, positional with `names`.
    pub key: Vec<bool>,
    /// Unsigned flags, positional (binlog MINIMAL omits signedness).
    pub unsigned: Vec<bool>,
}

/// Decoder state across one replication session: the table-id map plus the
/// schema facts fetched out of band.
#[derive(Default)]
pub(crate) struct BinlogState {
    pub maps: HashMap<u64, TableMap>,
    pub schemas: HashMap<String, TableSchema>,
    /// `rel_id` handed downstream per `db.table` — stable for the session,
    /// mirroring pgoutput's relation ids.
    rel_ids: HashMap<String, u32>,
    next_rel_id: u32,
}

impl BinlogState {
    pub(crate) fn rel_id(&mut self, key: &str) -> u32 {
        if let Some(id) = self.rel_ids.get(key) {
            return *id;
        }
        self.next_rel_id += 1;
        self.rel_ids.insert(key.to_string(), self.next_rel_id);
        self.next_rel_id
    }
}

struct R<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> R<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    fn left(&self) -> usize {
        self.b.len() - self.p
    }
    fn u8(&mut self) -> Result<u8> {
        let v = *self.b.get(self.p).ok_or_else(|| bad("truncated u8"))?;
        self.p += 1;
        Ok(v)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let s = self
            .b
            .get(self.p..self.p + n)
            .ok_or_else(|| bad("truncated slice"))?;
        self.p += n;
        Ok(s)
    }
    fn u16le(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32le(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u48le(&mut self) -> Result<u64> {
        let s = self.take(6)?;
        Ok(u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], 0, 0]))
    }
    fn u64le(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Length-encoded integer (the same encoding the client protocol uses).
    fn lenenc(&mut self) -> Result<u64> {
        let first = self.u8()?;
        Ok(match first {
            0xFC => self.u16le()? as u64,
            0xFD => {
                let s = self.take(3)?;
                u32::from_le_bytes([s[0], s[1], s[2], 0]) as u64
            }
            0xFE => self.u64le()?,
            v => v as u64,
        })
    }
    fn nul_str(&mut self) -> Result<String> {
        let end = self.b[self.p..]
            .iter()
            .position(|&c| c == 0)
            .ok_or_else(|| bad("unterminated string"))?;
        let s = String::from_utf8_lossy(&self.b[self.p..self.p + end]).into_owned();
        self.p += end + 1;
        Ok(s)
    }
}

/// TABLE_MAP(19): table id, db, table, column types + per-type metadata,
/// null bitmap, then optional metadata TLVs (only SIGNEDNESS is read —
/// names come from information_schema, which is authoritative anyway).
pub(crate) fn parse_table_map(body: &[u8]) -> Result<TableMap> {
    let mut r = R::new(body);
    let table_id = r.u48le()?;
    r.take(2)?; // flags
    let dblen = r.u8()? as usize;
    let db = String::from_utf8_lossy(r.take(dblen)?).into_owned();
    r.u8()?; // NUL
    let tlen = r.u8()? as usize;
    let table = String::from_utf8_lossy(r.take(tlen)?).into_owned();
    r.u8()?; // NUL
    let ncols = r.lenenc()? as usize;
    let kinds = r.take(ncols)?.to_vec();
    let metalen = r.lenenc()? as usize;
    let metablob = r.take(metalen)?;

    // Per-type metadata widths: 0, 1 or 2 bytes, in column order.
    let mut cols = Vec::with_capacity(ncols);
    let mut mp = 0usize;
    for &kind in &kinds {
        let meta = match kind {
            MT_FLOAT | MT_DOUBLE | MT_BLOB | MT_TINY_BLOB | MT_MEDIUM_BLOB | MT_LONG_BLOB
            | MT_GEOMETRY | MT_JSON | MT_TIME2 | MT_DATETIME2 | MT_TIMESTAMP2 => {
                let v = *metablob.get(mp).ok_or_else(|| bad("meta underflow"))? as u16;
                mp += 1;
                v
            }
            MT_VARCHAR | MT_VAR_STRING => {
                let s = metablob
                    .get(mp..mp + 2)
                    .ok_or_else(|| bad("meta underflow"))?;
                mp += 2;
                u16::from_le_bytes([s[0], s[1]])
            }
            // STRING/ENUM/SET/NEWDECIMAL: two bytes, but big-endian-ish
            // packing (real type in the high byte for STRING).
            MT_STRING | MT_ENUM | MT_SET | MT_NEWDECIMAL | MT_DECIMAL | MT_BIT => {
                let s = metablob
                    .get(mp..mp + 2)
                    .ok_or_else(|| bad("meta underflow"))?;
                mp += 2;
                ((s[0] as u16) << 8) | s[1] as u16
            }
            _ => 0,
        };
        cols.push(ColDef { kind, meta, nullable: false, unsigned: false });
    }
    // Null bitmap (LSB-first, one bit per column).
    let bmlen = (ncols + 7) / 8;
    let bm = r.take(bmlen)?;
    for (i, c) in cols.iter_mut().enumerate() {
        c.nullable = bm[i / 8] & (1 << (i % 8)) != 0;
    }
    Ok(TableMap { table_id, db, table, cols })
}

/// Bits of a row-image bitmap (LSB-first within each byte).
fn bit(bm: &[u8], i: usize) -> bool {
    bm.get(i / 8).is_some_and(|b| b & (1 << (i % 8)) != 0)
}

/// One row image: `present` says which columns the image carries; absent
/// columns become `UnchangedToast` (the arm every apply path already
/// treats as "do not touch"), matching MINIMAL/NOBLOB row images.
fn read_row(r: &mut R<'_>, cols: &[ColDef], present: &[u8], ncols: usize) -> Result<Tuple> {
    let n_present = (0..ncols).filter(|&i| bit(present, i)).count();
    let nullbm = r.take((n_present + 7) / 8)?.to_vec();
    let mut out = Vec::with_capacity(ncols);
    let mut k = 0usize; // index within the present/null bitmaps
    for (i, c) in cols.iter().enumerate().take(ncols) {
        if !bit(present, i) {
            out.push(Cell::UnchangedToast);
            continue;
        }
        if bit(&nullbm, k) {
            out.push(Cell::Null);
        } else {
            out.push(Cell::Text(bytes::Bytes::from(decode_cell(r, c)?)));
        }
        k += 1;
    }
    Ok(out)
}

/// Decode one non-NULL packed binlog value into destination-ready TEXT.
fn decode_cell(r: &mut R<'_>, c: &ColDef) -> Result<Vec<u8>> {
    let s = |v: String| Ok(v.into_bytes());
    match c.kind {
        MT_TINY => {
            let v = r.u8()?;
            if c.unsigned { s(v.to_string()) } else { s((v as i8).to_string()) }
        }
        MT_SHORT => {
            let v = r.u16le()?;
            if c.unsigned { s(v.to_string()) } else { s((v as i16).to_string()) }
        }
        MT_INT24 => {
            let b = r.take(3)?;
            let raw = u32::from_le_bytes([b[0], b[1], b[2], 0]);
            if c.unsigned {
                s(raw.to_string())
            } else {
                // Sign-extend 24 bits.
                let v = if raw & 0x80_0000 != 0 { (raw | 0xFF00_0000) as i32 } else { raw as i32 };
                s(v.to_string())
            }
        }
        MT_LONG => {
            let v = r.u32le()?;
            if c.unsigned { s(v.to_string()) } else { s((v as i32).to_string()) }
        }
        MT_LONGLONG => {
            let v = r.u64le()?;
            if c.unsigned { s(v.to_string()) } else { s((v as i64).to_string()) }
        }
        MT_FLOAT => {
            let v = f32::from_le_bytes(r.take(4)?.try_into().unwrap());
            let mut buf = ryu::Buffer::new();
            s(buf.format(v).to_string())
        }
        MT_DOUBLE => {
            let v = f64::from_le_bytes(r.take(8)?.try_into().unwrap());
            let mut buf = ryu::Buffer::new();
            s(buf.format(v).to_string())
        }
        MT_YEAR => {
            let v = r.u8()? as u32;
            s(if v == 0 { "0000".into() } else { (1900 + v).to_string() })
        }
        MT_DATE => {
            let b = r.take(3)?;
            let v = u32::from_le_bytes([b[0], b[1], b[2], 0]);
            let (y, m, d) = (v >> 9, (v >> 5) & 0xF, v & 0x1F);
            s(format!("{y:04}-{m:02}-{d:02}"))
        }
        MT_TIMESTAMP => {
            let secs = r.u32le()? as i64;
            s(fmt_epoch(secs, 0))
        }
        MT_TIMESTAMP2 => {
            let secs = i32::from_be_bytes(r.take(4)?.try_into().unwrap()) as i64;
            let frac = read_frac(r, c.meta as u8)?;
            s(fmt_epoch(secs, frac))
        }
        MT_DATETIME2 => {
            // 5 bytes big-endian, 40 bits: sign(1) yearmonth(17) day(5)
            // hour(5) minute(6) second(6), then the fractional tail.
            let b = r.take(5)?;
            let packed = ((b[0] as u64) << 32)
                | ((b[1] as u64) << 24)
                | ((b[2] as u64) << 16)
                | ((b[3] as u64) << 8)
                | b[4] as u64;
            let v = packed & ((1u64 << 39) - 1); // drop the sign bit
            let ym = (v >> 22) & 0x1_FFFF;
            let (y, mo) = (ym / 13, ym % 13);
            let d = (v >> 17) & 0x1F;
            let h = (v >> 12) & 0x1F;
            let mi = (v >> 6) & 0x3F;
            let sec = v & 0x3F;
            let frac = read_frac(r, c.meta as u8)?;
            let mut out = format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec:02}");
            push_frac(&mut out, frac, c.meta as u8);
            s(out)
        }
        MT_TIME2 => {
            let b = r.take(3)?;
            let packed = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            let v = packed & 0x7F_FFFF;
            let h = (v >> 12) & 0x3FF;
            let mi = (v >> 6) & 0x3F;
            let sec = v & 0x3F;
            let frac = read_frac(r, c.meta as u8)?;
            let mut out = format!("{h:02}:{mi:02}:{sec:02}");
            push_frac(&mut out, frac, c.meta as u8);
            s(out)
        }
        MT_NEWDECIMAL => {
            let precision = (c.meta >> 8) as u32;
            let scale = (c.meta & 0xFF) as u32;
            s(decode_decimal(r, precision, scale)?)
        }
        MT_VARCHAR | MT_VAR_STRING => {
            // The prefix width follows the declared max length in BYTES.
            let n = if c.meta < 256 { r.u8()? as usize } else { r.u16le()? as usize };
            Ok(r.take(n)?.to_vec())
        }
        MT_STRING => {
            // meta packs (real_type << 8 | length) with the high bits of the
            // length folded into the type byte for lengths > 255.
            let (rt, len) = string_meta(c.meta);
            match rt {
                MT_ENUM | MT_SET => {
                    let v = match len {
                        1 => r.u8()? as u64,
                        _ => r.u16le()? as u64,
                    };
                    s(v.to_string())
                }
                _ => {
                    let n = if len < 256 { r.u8()? as usize } else { r.u16le()? as usize };
                    Ok(r.take(n)?.to_vec())
                }
            }
        }
        MT_ENUM => {
            let v = if c.meta & 0xFF == 1 { r.u8()? as u64 } else { r.u16le()? as u64 };
            s(v.to_string())
        }
        MT_SET => {
            let n = (c.meta & 0xFF).max(1) as usize;
            let b = r.take(n)?;
            let mut v = 0u64;
            for (i, byte) in b.iter().enumerate() {
                v |= (*byte as u64) << (8 * i);
            }
            s(v.to_string())
        }
        MT_BLOB | MT_TINY_BLOB | MT_MEDIUM_BLOB | MT_LONG_BLOB | MT_GEOMETRY => {
            let n = match c.meta {
                1 => r.u8()? as usize,
                2 => r.u16le()? as usize,
                3 => {
                    let b = r.take(3)?;
                    u32::from_le_bytes([b[0], b[1], b[2], 0]) as usize
                }
                _ => r.u32le()? as usize,
            };
            Ok(r.take(n)?.to_vec())
        }
        MT_JSON => {
            let n = match c.meta {
                1 => r.u8()? as usize,
                2 => r.u16le()? as usize,
                3 => {
                    let b = r.take(3)?;
                    u32::from_le_bytes([b[0], b[1], b[2], 0]) as usize
                }
                _ => r.u32le()? as usize,
            };
            // Binary JSON — the caller re-renders; carrying the raw bytes
            // keeps this decoder pure (see gotchas: PARTIAL_JSON refused).
            Ok(r.take(n)?.to_vec())
        }
        MT_BIT => {
            let bits = ((c.meta >> 8) * 8 + (c.meta & 0xFF)).max(1) as usize;
            let n = (bits + 7) / 8;
            let b = r.take(n)?;
            let mut v = 0u64;
            for byte in b {
                v = (v << 8) | *byte as u64;
            }
            s(v.to_string())
        }
        MT_NULL => Ok(Vec::new()),
        other => Err(bad(&format!("unsupported column type {other:#04x}"))),
    }
}

/// STRING metadata: MySQL folds the high length bits into the type byte.
fn string_meta(meta: u16) -> (u8, u16) {
    let mut rt = (meta >> 8) as u8;
    let mut len = meta & 0xFF;
    if rt >= 0xF1 {
        // The upper bits of the length hide in the type byte.
        len |= ((rt as u16 & 0x30) ^ 0x30) << 4;
        rt |= 0x30;
    }
    (rt, len)
}

/// Fractional-seconds tail: 0-3 bytes big-endian by fsp.
fn read_frac(r: &mut R<'_>, fsp: u8) -> Result<u32> {
    Ok(match fsp {
        0 => 0,
        1 | 2 => r.u8()? as u32 * 10_000,
        3 | 4 => {
            let b = r.take(2)?;
            u16::from_be_bytes([b[0], b[1]]) as u32 * 100
        }
        _ => {
            let b = r.take(3)?;
            ((b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32) & 0xFF_FFFF
        }
    })
}

fn push_frac(out: &mut String, frac: u32, fsp: u8) {
    if fsp > 0 {
        let digits = fsp.min(6) as usize;
        let scaled = frac / 10u32.pow(6 - digits as u32);
        out.push('.');
        out.push_str(&format!("{scaled:0digits$}", digits = digits));
    }
}

/// Unix seconds (+ micros) → `YYYY-MM-DD HH:MM:SS[.ffffff]+00` — the
/// timestamptz text every destination's renderer expects.
fn fmt_epoch(secs: i64, micros: u32) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let mut out = format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}");
    if micros > 0 {
        out.push_str(&format!(".{micros:06}"));
    }
    out.push_str("+00");
    out
}

/// Howard Hinnant's civil_from_days (the inverse of the days_from_civil we
/// already use on the MySQL bulk path — no chrono on the hot path).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// NEWDECIMAL: base-1e9 groups, big-endian, sign in the top bit of the
/// first byte (negative values arrive one's-complemented).
fn decode_decimal(r: &mut R<'_>, precision: u32, scale: u32) -> Result<String> {
    const DIG_PER: u32 = 9;
    const BYTES_FOR: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let int_digits = precision - scale;
    let (int_full, int_rest) = (int_digits / DIG_PER, int_digits % DIG_PER);
    let (frac_full, frac_rest) = (scale / DIG_PER, scale % DIG_PER);
    let total = (int_full + frac_full) as usize * 4
        + BYTES_FOR[int_rest as usize]
        + BYTES_FOR[frac_rest as usize];
    let raw = r.take(total)?.to_vec();
    let negative = raw[0] & 0x80 == 0;
    let mut b = raw;
    b[0] ^= 0x80; // clear/insert the sign bit
    if negative {
        for x in b.iter_mut() {
            *x = !*x;
        }
    }
    let mut rd = R::new(&b);
    let mut int_part = String::new();
    if int_rest > 0 {
        let n = BYTES_FOR[int_rest as usize];
        let mut v = 0u32;
        for byte in rd.take(n)? {
            v = (v << 8) | *byte as u32;
        }
        int_part.push_str(&v.to_string());
    }
    for _ in 0..int_full {
        let v = u32::from_be_bytes(rd.take(4)?.try_into().unwrap());
        if int_part.is_empty() {
            int_part.push_str(&v.to_string());
        } else {
            int_part.push_str(&format!("{v:09}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    let mut frac_part = String::new();
    for _ in 0..frac_full {
        let v = u32::from_be_bytes(rd.take(4)?.try_into().unwrap());
        frac_part.push_str(&format!("{v:09}"));
    }
    if frac_rest > 0 {
        let n = BYTES_FOR[frac_rest as usize];
        let mut v = 0u32;
        for byte in rd.take(n)? {
            v = (v << 8) | *byte as u32;
        }
        frac_part.push_str(&format!("{v:0w$}", w = frac_rest as usize));
    }
    let sign = if negative { "-" } else { "" };
    Ok(if scale > 0 {
        format!("{sign}{int_part}.{frac_part}")
    } else {
        format!("{sign}{int_part}")
    })
}

/// A decoded ROWS event, ready to become `PgoMessage`s.
pub(crate) struct RowsEvent {
    pub table_id: u64,
    pub rows: Vec<(Option<Tuple>, Option<Tuple>)>,
}

/// WRITE/UPDATE/DELETE_ROWS_v2 (30/31/32).
pub(crate) fn parse_rows(body: &[u8], kind: u8, map: &TableMap) -> Result<RowsEvent> {
    let mut r = R::new(body);
    let table_id = r.u48le()?;
    r.take(2)?; // flags
    let extra = r.u16le()? as usize; // v2 header: extra-data length (incl. itself)
    if extra >= 2 {
        r.take(extra - 2)?;
    }
    let ncols = r.lenenc()? as usize;
    let bmlen = (ncols + 7) / 8;
    let present1 = r.take(bmlen)?.to_vec();
    let present2 = if kind == EV_UPDATE_ROWS {
        r.take(bmlen)?.to_vec()
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    while r.left() > 0 {
        match kind {
            EV_WRITE_ROWS => {
                let t = read_row(&mut r, &map.cols, &present1, ncols)?;
                rows.push((None, Some(t)));
            }
            EV_DELETE_ROWS => {
                let t = read_row(&mut r, &map.cols, &present1, ncols)?;
                rows.push((Some(t), None));
            }
            EV_UPDATE_ROWS => {
                let before = read_row(&mut r, &map.cols, &present1, ncols)?;
                let after = read_row(&mut r, &map.cols, &present2, ncols)?;
                rows.push((Some(before), Some(after)));
            }
            _ => return Err(bad("not a rows event")),
        }
    }
    Ok(RowsEvent { table_id, rows })
}

/// QUERY(2): the statement text — BEGIN frames a transaction, everything
/// else is DDL the caller must react to (cache invalidation / loud refusal).
pub(crate) fn parse_query(body: &[u8]) -> Result<(String, String)> {
    let mut r = R::new(body);
    r.u32le()?; // slave proxy id
    r.u32le()?; // execution time
    let dblen = r.u8()? as usize;
    r.u16le()?; // error code
    let statuslen = r.u16le()? as usize;
    r.take(statuslen)?;
    let db = String::from_utf8_lossy(r.take(dblen)?).into_owned();
    r.u8()?; // NUL
    let sql = String::from_utf8_lossy(r.take(r.left())?).into_owned();
    Ok((db, sql))
}

/// ROTATE(4): the next file the stream continues in.
pub(crate) fn parse_rotate(body: &[u8]) -> Result<(u64, String)> {
    let mut r = R::new(body);
    let pos = r.u64le()?;
    let name = String::from_utf8_lossy(r.take(r.left())?).into_owned();
    Ok((pos, name))
}

/// Turn one decoded ROWS event into the `PgoMessage`s the collapse layer
/// consumes, materialising a `Relation` the first time a table appears.
pub(crate) fn to_messages(
    st: &mut BinlogState,
    kind: u8,
    ev: RowsEvent,
) -> Result<Vec<PgoMessage>> {
    let map = st
        .maps
        .get(&ev.table_id)
        .ok_or_else(|| bad("rows event before its TABLE_MAP"))?
        .clone();
    let qname = format!("{}.{}", map.db, map.table);
    let schema = st
        .schemas
        .get(&qname)
        .ok_or_else(|| bad(&format!("no resolved schema for {qname}")))?
        .clone();
    let rel_id = st.rel_id(&qname);

    let mut out = Vec::with_capacity(ev.rows.len() + 1);
    out.push(PgoMessage::Relation(Relation {
        rel_id,
        namespace: map.db.clone(),
        name: map.table.clone(),
        replica_identity: b'i',
        cols: schema
            .names
            .iter()
            .enumerate()
            .map(|(i, n)| RelationCol {
                key: schema.key.get(i).copied().unwrap_or(false),
                name: n.clone(),
                // Type identity travels with the value text; the MySQL
                // appliers key off the destination's own DDL.
                type_oid: 0,
                type_mod: -1,
            })
            .collect(),
    }));
    for (before, after) in ev.rows {
        match kind {
            EV_WRITE_ROWS => out.push(PgoMessage::Insert {
                rel_id,
                new: after.ok_or_else(|| bad("insert without image"))?,
            }),
            EV_DELETE_ROWS => out.push(PgoMessage::Delete {
                rel_id,
                old: OldImage { full: true, tuple: before.ok_or_else(|| bad("delete without image"))? },
            }),
            EV_UPDATE_ROWS => out.push(PgoMessage::Update {
                rel_id,
                old: before.map(|t| OldImage { full: true, tuple: t }),
                new: after.ok_or_else(|| bad("update without after-image"))?,
            }),
            _ => return Err(bad("not a rows event")),
        }
    }
    Ok(out)
}

/// Event codes the reader may skip without losing data.
pub(crate) fn skippable(t: u8) -> bool {
    matches!(t, EV_HEARTBEAT | EV_HEARTBEAT_V2 | 33 | 34 | 35 | 3 | 5 | 9)
}

pub(crate) fn is_rows(t: u8) -> bool {
    matches!(t, EV_WRITE_ROWS | EV_UPDATE_ROWS | EV_DELETE_ROWS)
}

pub(crate) const TYPE_QUERY: u8 = EV_QUERY;
pub(crate) const TYPE_XID: u8 = EV_XID;
pub(crate) const TYPE_TABLE_MAP: u8 = EV_TABLE_MAP;
pub(crate) const TYPE_ROTATE: u8 = EV_ROTATE;
pub(crate) const TYPE_FDE: u8 = EV_FORMAT_DESCRIPTION;
pub(crate) const TYPE_TX_PAYLOAD: u8 = EV_TRANSACTION_PAYLOAD;

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(t: u8, body: &[u8], log_pos: u32) -> Vec<u8> {
        let mut e = Vec::new();
        e.extend_from_slice(&1_785_882_720u32.to_le_bytes());
        e.push(t);
        e.extend_from_slice(&1u32.to_le_bytes());
        e.extend_from_slice(&((HEADER_LEN + body.len() + 4) as u32).to_le_bytes());
        e.extend_from_slice(&log_pos.to_le_bytes());
        e.extend_from_slice(&0u16.to_le_bytes());
        e.extend_from_slice(body);
        e.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // CRC32 placeholder
        e
    }

    #[test]
    fn header_split_strips_the_checksum_when_negotiated() {
        let e = hdr(EV_XID, &[7u8; 8], 2864);
        let (h, body) = split_event(&e, true).unwrap();
        assert_eq!(h.event_type, EV_XID);
        assert_eq!(h.log_pos, 2864);
        assert_eq!(h.event_size as usize, e.len(), "size counts the trailer (live-verified)");
        assert_eq!(body, &[7u8; 8]);
        // Without checksums the trailer belongs to the body.
        let (_, body) = split_event(&e, false).unwrap();
        assert_eq!(body.len(), 12);
        assert!(split_event(&[0u8; 4], true).is_err());
    }

    #[test]
    fn table_map_parses_names_types_and_nullability() {
        // db="bench", table="t", cols: LONG, VARCHAR(300 bytes), NEWDECIMAL(12,4)
        let mut b = Vec::new();
        b.extend_from_slice(&[0x2a, 0, 0, 0, 0, 0]); // table_id 42 (6B)
        b.extend_from_slice(&[0, 0]); // flags
        b.push(5);
        b.extend_from_slice(b"bench");
        b.push(0);
        b.push(1);
        b.extend_from_slice(b"t");
        b.push(0);
        b.push(3); // ncols
        b.extend_from_slice(&[MT_LONG, MT_VARCHAR, MT_NEWDECIMAL]);
        // meta length: LONG 0 + VARCHAR 2 + NEWDECIMAL 2 = 4 bytes
        b.push(4);
        b.extend_from_slice(&300u16.to_le_bytes()); // VARCHAR meta
        b.extend_from_slice(&[12, 4]); // NEWDECIMAL precision/scale
        b.push(0b0000_0110); // nullable: col1, col2
        let m = parse_table_map(&b).unwrap();
        assert_eq!((m.table_id, m.db.as_str(), m.table.as_str()), (42, "bench", "t"));
        assert_eq!(m.cols.len(), 3);
        assert_eq!(m.cols[1].kind, MT_VARCHAR);
        assert_eq!(m.cols[1].meta, 300);
        assert_eq!(m.cols[2].meta, (12 << 8) | 4);
        assert_eq!(
            m.cols.iter().map(|c| c.nullable).collect::<Vec<_>>(),
            vec![false, true, true]
        );
    }

    #[test]
    fn decimal_round_trips_both_signs_and_scales() {
        // 1234.5678 at (12,4): int part 8 digits -> 4 bytes, frac 4 -> 2 bytes.
        let mut enc = Vec::new();
        enc.extend_from_slice(&1234u32.to_be_bytes());
        enc.extend_from_slice(&(5678u16).to_be_bytes());
        enc[0] |= 0x80; // positive
        let c = ColDef { kind: MT_NEWDECIMAL, meta: (12 << 8) | 4, nullable: false, unsigned: false };
        let mut r = R::new(&enc);
        assert_eq!(decode_cell(&mut r, &c).unwrap(), b"1234.5678");

        // Negative: one's complement of the same payload, sign bit clear.
        let mut neg: Vec<u8> = enc.iter().map(|b| !b).collect();
        neg[0] &= 0x7F;
        let mut r = R::new(&neg);
        let got = decode_cell(&mut r, &c).unwrap();
        assert_eq!(String::from_utf8(got).unwrap(), "-1234.5678");
    }

    #[test]
    fn temporals_render_as_the_appliers_expect() {
        // DATETIME2 fsp=6: 2026-08-05 01:02:03.000004
        let ym = (2026u64 * 13 + 8) << 22;
        let v = ym | (5u64 << 17) | (1u64 << 12) | (2u64 << 6) | 3;
        let packed = v | (1u64 << 39); // sign bit set = non-negative
        let mut enc = vec![
            (packed >> 32) as u8,
            (packed >> 24) as u8,
            (packed >> 16) as u8,
            (packed >> 8) as u8,
            packed as u8,
        ];
        enc.extend_from_slice(&[0, 0, 4]); // 4 micros, 3-byte tail
        let c = ColDef { kind: MT_DATETIME2, meta: 6, nullable: false, unsigned: false };
        let mut r = R::new(&enc);
        assert_eq!(
            String::from_utf8(decode_cell(&mut r, &c).unwrap()).unwrap(),
            "2026-08-05 01:02:03.000004"
        );

        // TIMESTAMP2 renders UTC with the +00 suffix rowtext.rs demands.
        let secs: i32 = 1_785_882_723;
        let mut enc = secs.to_be_bytes().to_vec();
        enc.push(0); // fsp=1 tail
        let c = ColDef { kind: MT_TIMESTAMP2, meta: 1, nullable: false, unsigned: false };
        let mut r = R::new(&enc);
        let got = String::from_utf8(decode_cell(&mut r, &c).unwrap()).unwrap();
        assert!(got.ends_with("+00"), "{got}");
        assert!(got.starts_with("2026-"), "{got}");

        // DATE
        let c = ColDef { kind: MT_DATE, meta: 0, nullable: false, unsigned: false };
        let v: u32 = (2026 << 9) | (8 << 5) | 5;
        let enc = v.to_le_bytes();
        let mut r = R::new(&enc[..3]);
        assert_eq!(decode_cell(&mut r, &c).unwrap(), b"2026-08-05");
    }

    #[test]
    fn ints_honour_signedness_and_widths() {
        let cases: Vec<(ColDef, Vec<u8>, &str)> = vec![
            (ColDef { kind: MT_TINY, meta: 0, nullable: false, unsigned: false }, vec![0xFF], "-1"),
            (ColDef { kind: MT_TINY, meta: 0, nullable: false, unsigned: true }, vec![0xFF], "255"),
            (ColDef { kind: MT_INT24, meta: 0, nullable: false, unsigned: false }, vec![0xFF, 0xFF, 0xFF], "-1"),
            (ColDef { kind: MT_LONG, meta: 0, nullable: false, unsigned: false }, (-42i32).to_le_bytes().to_vec(), "-42"),
            (ColDef { kind: MT_LONGLONG, meta: 0, nullable: false, unsigned: true }, u64::MAX.to_le_bytes().to_vec(), "18446744073709551615"),
            (ColDef { kind: MT_YEAR, meta: 0, nullable: false, unsigned: false }, vec![126], "2026"),
        ];
        for (c, bytes, want) in cases {
            let mut r = R::new(&bytes);
            let got = String::from_utf8(decode_cell(&mut r, &c).unwrap()).unwrap();
            assert_eq!(got, want, "type {:#04x}", c.kind);
        }
    }

    #[test]
    fn varchar_prefix_width_follows_the_declared_byte_length() {
        // meta < 256 -> 1-byte prefix
        let c = ColDef { kind: MT_VARCHAR, meta: 100, nullable: false, unsigned: false };
        let mut enc = vec![2u8];
        enc.extend_from_slice(b"hi");
        let mut r = R::new(&enc);
        assert_eq!(decode_cell(&mut r, &c).unwrap(), b"hi");
        // meta >= 256 (utf8mb4 VARCHAR(64) = 256 bytes) -> 2-byte prefix
        let c = ColDef { kind: MT_VARCHAR, meta: 300, nullable: false, unsigned: false };
        let mut enc = 260u16.to_le_bytes().to_vec();
        enc.extend_from_slice(&[b'x'; 260]);
        let mut r = R::new(&enc);
        assert_eq!(decode_cell(&mut r, &c).unwrap().len(), 260);
    }

    #[test]
    fn rows_event_maps_images_and_absent_columns() {
        let map = TableMap {
            table_id: 42,
            db: "bench".into(),
            table: "t".into(),
            cols: vec![
                ColDef { kind: MT_LONG, meta: 0, nullable: false, unsigned: false },
                ColDef { kind: MT_VARCHAR, meta: 100, nullable: true, unsigned: false },
                ColDef { kind: MT_LONG, meta: 0, nullable: true, unsigned: false },
            ],
        };
        // WRITE_ROWS: all three present, third NULL.
        let mut b = Vec::new();
        b.extend_from_slice(&[0x2a, 0, 0, 0, 0, 0]);
        b.extend_from_slice(&[0, 0]); // flags
        b.extend_from_slice(&2u16.to_le_bytes()); // extra-data len (self only)
        b.push(3); // ncols
        b.push(0b0000_0111); // present: all
        b.push(0b0000_0100); // nulls: col3
        b.extend_from_slice(&7i32.to_le_bytes());
        b.push(2);
        b.extend_from_slice(b"hi");
        let ev = parse_rows(&b, EV_WRITE_ROWS, &map).unwrap();
        assert_eq!(ev.rows.len(), 1);
        let after = ev.rows[0].1.clone().unwrap();
        assert_eq!(after[0], Cell::Text(bytes::Bytes::from_static(b"7")));
        assert_eq!(after[1], Cell::Text(bytes::Bytes::from_static(b"hi")));
        assert_eq!(after[2], Cell::Null);

        // A partial image (MINIMAL): column 2 absent -> UnchangedToast.
        let mut b = Vec::new();
        b.extend_from_slice(&[0x2a, 0, 0, 0, 0, 0]);
        b.extend_from_slice(&[0, 0]);
        b.extend_from_slice(&2u16.to_le_bytes());
        b.push(3);
        b.push(0b0000_0101); // present: col1, col3
        b.push(0b0000_0000); // no nulls
        b.extend_from_slice(&7i32.to_le_bytes());
        b.extend_from_slice(&9i32.to_le_bytes());
        let ev = parse_rows(&b, EV_WRITE_ROWS, &map).unwrap();
        let after = ev.rows[0].1.clone().unwrap();
        assert_eq!(after[1], Cell::UnchangedToast, "absent column must not read as NULL");
        assert_eq!(after[2], Cell::Text(bytes::Bytes::from_static(b"9")));
    }

    #[test]
    fn messages_carry_a_relation_and_stable_rel_ids() {
        let mut st = BinlogState::default();
        st.maps.insert(
            42,
            TableMap {
                table_id: 42,
                db: "bench".into(),
                table: "t".into(),
                cols: vec![ColDef { kind: MT_LONG, meta: 0, nullable: false, unsigned: false }],
            },
        );
        st.schemas.insert(
            "bench.t".into(),
            TableSchema { names: vec!["id".into()], key: vec![true], unsigned: vec![false] },
        );
        let ev = RowsEvent { table_id: 42, rows: vec![(None, Some(vec![Cell::Text(bytes::Bytes::from_static(b"1"))]))] };
        let msgs = to_messages(&mut st, EV_WRITE_ROWS, ev).unwrap();
        match &msgs[0] {
            PgoMessage::Relation(r) => {
                assert_eq!((r.namespace.as_str(), r.name.as_str()), ("bench", "t"));
                assert!(r.cols[0].key);
                assert_eq!(r.cols[0].name, "id");
            }
            other => panic!("expected Relation, got {other:?}"),
        }
        let first_rel = st.rel_id("bench.t");
        assert!(matches!(&msgs[1], PgoMessage::Insert { rel_id, .. } if *rel_id == first_rel));
        // The id is stable across events for the session.
        assert_eq!(st.rel_id("bench.t"), first_rel);
    }

    #[test]
    fn query_and_rotate_parse() {
        let mut b = Vec::new();
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.push(5); // db len
        b.extend_from_slice(&0u16.to_le_bytes()); // error
        b.extend_from_slice(&0u16.to_le_bytes()); // status len
        b.extend_from_slice(b"bench");
        b.push(0);
        b.extend_from_slice(b"BEGIN");
        let (db, sql) = parse_query(&b).unwrap();
        assert_eq!((db.as_str(), sql.as_str()), ("bench", "BEGIN"));

        let mut b = 4u64.to_le_bytes().to_vec();
        b.extend_from_slice(b"binlog.000009");
        let (pos, name) = parse_rotate(&b).unwrap();
        assert_eq!((pos, name.as_str()), (4, "binlog.000009"));
    }
}
