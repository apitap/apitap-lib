//! PG binary COPY → Arrow columnar batch builders (no arrow crate — the
//! buffers are laid out exactly as the Arrow C Data Interface wants them
//! and exported by py-apitap's hand-rolled FFI). Modeled 1:1 on
//! [`crate::wire::bqparquet`]'s bounds-first two-pass tuple walk; the
//! decode arms share `wire::pgcopy`'s helpers (PG epochs, NUMERIC→i128).

use crate::error::{Error, Result};
use crate::plan::Delivered;
use crate::wire::pgcopy::{numeric_to_scaled_i128, PG_EPOCH_DAYS, PG_EPOCH_MICROS};

/// The v1 Arrow type vocabulary. Everything [`arrow_kind`] can't place
/// falls back to `::text` in the SELECT (and lands here as `Utf8`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArrowKind {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    /// Arrow decimal128 with the DECLARED precision/scale.
    Decimal { p: u8, s: i8 },
    Date32,
    /// timestamptz — micros since Unix epoch, tz "UTC" in the schema.
    TimestampUtc,
    /// timestamp — micros since Unix epoch, no tz.
    TimestampNaive,
    /// text/json/jsonb/uuid(hyphenated)/fallback ::text.
    Utf8,
    /// bytea.
    Binary,
}

/// Map a delivered wire type onto the v1 Arrow vocabulary. `None` means
/// the caller must rewrite the column's SELECT to `::text` (Utf8 lane).
pub fn arrow_kind(d: &Delivered) -> Option<ArrowKind> {
    Some(match d {
        Delivered::Int { bytes: 1 | 2, .. } => ArrowKind::Int16,
        Delivered::Int { bytes: 4, .. } => ArrowKind::Int32,
        Delivered::Int { .. } => ArrowKind::Int64,
        Delivered::Float32 => ArrowKind::Float32,
        Delivered::Float64 => ArrowKind::Float64,
        Delivered::Bool => ArrowKind::Bool,
        // Same gate as the parquet lane (bqparquet::parquet_col_ok):
        // unconstrained NUMERIC has no exact width, >38 digits exceed i128.
        // Scale clamps to precision like the parquet schema does.
        Delivered::Decimal { p, s } => {
            if *p == 0 || *p > 38 {
                return None;
            }
            ArrowKind::Decimal {
                p: *p as u8,
                s: (*s).min(*p) as i8,
            }
        }
        Delivered::Date => ArrowKind::Date32,
        Delivered::DateTime { utc: true } => ArrowKind::TimestampUtc,
        Delivered::DateTime { utc: false } => ArrowKind::TimestampNaive,
        Delivered::Text => ArrowKind::Utf8,
        // jsonb's 0x01 version byte and uuid's raw 16 bytes must not reach
        // the Utf8 passthrough — the planner rewrites both to ::text.
        Delivered::Json | Delivered::Uuid => return None,
        Delivered::Bytes => ArrowKind::Binary,
    })
}

/// One sealed column, buffers in Arrow C layout (validity bitmap LSB-first;
/// utf8/binary carry i32 offsets starting at 0).
pub enum FinishedCol {
    I16 { validity: Option<Vec<u8>>, data: Vec<i16> },
    I32 { validity: Option<Vec<u8>>, data: Vec<i32> },
    I64 { validity: Option<Vec<u8>>, data: Vec<i64> },
    F32 { validity: Option<Vec<u8>>, data: Vec<f32> },
    F64 { validity: Option<Vec<u8>>, data: Vec<f64> },
    /// Data is ALSO a bitmap for bool.
    Bool { validity: Option<Vec<u8>>, data: Vec<u8> },
    Dec128 { validity: Option<Vec<u8>>, data: Vec<i128> },
    Utf8 { validity: Option<Vec<u8>>, offsets: Vec<i32>, data: Vec<u8> },
    Bin { validity: Option<Vec<u8>>, offsets: Vec<i32>, data: Vec<u8> },
}

/// One sealed batch: `rows` rows across `cols` (parallel to the schema).
pub struct ArrowBatch {
    pub rows: usize,
    pub cols: Vec<FinishedCol>,
}

// ============================================================================
// Column builders
// ============================================================================

fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: unexpected {what}"))
}

/// Append the bit for `row` (LSB-first). Rows arrive in order, so a fresh
/// byte is needed exactly when `row % 8 == 0`.
fn push_bit(bm: &mut Vec<u8>, row: usize, on: bool) {
    if row % 8 == 0 {
        bm.push(0);
    }
    if on {
        *bm.last_mut().expect("byte pushed above") |= 1 << (row % 8);
    }
}

/// Lazy validity: stays `None` until the first NULL (sealed as `None` =
/// all-valid, so a NULL-free column costs no bitmap at all).
fn mark(v: &mut Option<Vec<u8>>, row: usize, valid: bool) {
    match v {
        Some(bm) => push_bit(bm, row, valid),
        None if valid => {}
        None => {
            // First NULL: backfill `row` set bits, then this row's zero.
            let mut bm = vec![0xFF; row / 8];
            if row % 8 != 0 {
                bm.push((1u8 << (row % 8)) - 1);
            }
            push_bit(&mut bm, row, false);
            *v = Some(bm);
        }
    }
}

enum ColB {
    I16 { v: Option<Vec<u8>>, d: Vec<i16> },
    I32 { v: Option<Vec<u8>>, d: Vec<i32> },
    I64 { v: Option<Vec<u8>>, d: Vec<i64> },
    F32 { v: Option<Vec<u8>>, d: Vec<f32> },
    F64 { v: Option<Vec<u8>>, d: Vec<f64> },
    /// Data bitmap, same LSB-first order as validity.
    Bool { v: Option<Vec<u8>>, d: Vec<u8> },
    Dec { v: Option<Vec<u8>>, d: Vec<i128>, s: u32 },
    /// Days since Unix epoch (seals as I32).
    D32 { v: Option<Vec<u8>>, d: Vec<i32> },
    /// Micros since Unix epoch (seals as I64; utc/naive decode identically).
    Ts { v: Option<Vec<u8>>, d: Vec<i64> },
    Utf8 { v: Option<Vec<u8>>, off: Vec<i32>, d: Vec<u8> },
    Bin { v: Option<Vec<u8>>, off: Vec<i32>, d: Vec<u8> },
}

/// Fresh offsets vec: Arrow wants a leading 0 plus one entry per row.
fn offsets0(cap: usize) -> Vec<i32> {
    let mut o = Vec::with_capacity(cap / 16 + 1);
    o.push(0);
    o
}

impl ColB {
    /// `cap` = per-column byte budget (batch_bytes / ncols), a modest
    /// pre-reserve so a steady stream never reallocates mid-batch.
    fn new(k: &ArrowKind, cap: usize) -> Self {
        match k {
            ArrowKind::Int16 => ColB::I16 { v: None, d: Vec::with_capacity(cap / 2) },
            ArrowKind::Int32 => ColB::I32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::Int64 => ColB::I64 { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Float32 => ColB::F32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::Float64 => ColB::F64 { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Bool => ColB::Bool { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Decimal { s, .. } => ColB::Dec {
                v: None,
                d: Vec::with_capacity(cap / 16),
                s: (*s).max(0) as u32,
            },
            ArrowKind::Date32 => ColB::D32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::TimestampUtc | ArrowKind::TimestampNaive => {
                ColB::Ts { v: None, d: Vec::with_capacity(cap / 8) }
            }
            ArrowKind::Utf8 => ColB::Utf8 {
                v: None,
                off: offsets0(cap),
                d: Vec::with_capacity(cap),
            },
            ArrowKind::Binary => ColB::Bin {
                v: None,
                off: offsets0(cap),
                d: Vec::with_capacity(cap),
            },
        }
    }

    /// NULL still occupies a slot: fixed types push a zero placeholder,
    /// varlen repeats the last offset.
    fn push_null(&mut self, row: usize) {
        match self {
            ColB::I16 { v, d } => { mark(v, row, false); d.push(0); }
            ColB::I32 { v, d } | ColB::D32 { v, d } => { mark(v, row, false); d.push(0); }
            ColB::I64 { v, d } | ColB::Ts { v, d } => { mark(v, row, false); d.push(0); }
            ColB::F32 { v, d } => { mark(v, row, false); d.push(0.0); }
            ColB::F64 { v, d } => { mark(v, row, false); d.push(0.0); }
            ColB::Bool { v, d } => { mark(v, row, false); push_bit(d, row, false); }
            ColB::Dec { v, d, .. } => { mark(v, row, false); d.push(0); }
            ColB::Utf8 { v, off, .. } | ColB::Bin { v, off, .. } => {
                mark(v, row, false);
                off.push(*off.last().expect("offsets seeded with 0"));
            }
        }
    }

    /// Decode one non-NULL Postgres binary field. Widths are strict: the
    /// declared kind fixes the wire width, so a mismatch is corruption.
    fn push_field(&mut self, f: &[u8], row: usize) -> Result<()> {
        match self {
            ColB::I16 { v, d } => {
                d.push(i16::from_be_bytes(
                    f.try_into().map_err(|_| bad(&format!("int2 width {}", f.len())))?,
                ));
                mark(v, row, true);
            }
            ColB::I32 { v, d } => {
                d.push(i32::from_be_bytes(
                    f.try_into().map_err(|_| bad(&format!("int4 width {}", f.len())))?,
                ));
                mark(v, row, true);
            }
            ColB::I64 { v, d } => {
                d.push(i64::from_be_bytes(
                    f.try_into().map_err(|_| bad(&format!("int8 width {}", f.len())))?,
                ));
                mark(v, row, true);
            }
            ColB::F32 { v, d } => {
                d.push(f32::from_be_bytes(f.try_into().map_err(|_| bad("float4"))?));
                mark(v, row, true);
            }
            ColB::F64 { v, d } => {
                d.push(f64::from_be_bytes(f.try_into().map_err(|_| bad("float8"))?));
                mark(v, row, true);
            }
            ColB::Bool { v, d } => {
                push_bit(d, row, f.first().copied().unwrap_or(0) != 0);
                mark(v, row, true);
            }
            ColB::Dec { v, d, s } => {
                d.push(numeric_to_scaled_i128(f, *s)?);
                mark(v, row, true);
            }
            ColB::D32 { v, d } => {
                let days = i32::from_be_bytes(f.try_into().map_err(|_| bad("date"))?);
                if days == i32::MAX || days == i32::MIN {
                    return Err(Error::Transfer(
                        "date 'infinity' has no Arrow representation — cast or \
                         filter it in a source view"
                            .into(),
                    ));
                }
                d.push(days.checked_add(PG_EPOCH_DAYS).ok_or_else(|| bad("date range"))?);
                mark(v, row, true);
            }
            ColB::Ts { v, d } => {
                let us = i64::from_be_bytes(f.try_into().map_err(|_| bad("timestamp"))?);
                if us == i64::MAX || us == i64::MIN {
                    return Err(Error::Transfer(
                        "timestamp 'infinity' has no Arrow representation — cast \
                         or filter it in a source view"
                            .into(),
                    ));
                }
                d.push(us.checked_add(PG_EPOCH_MICROS).ok_or_else(|| bad("timestamp range"))?);
                mark(v, row, true);
            }
            // Utf8 fields arrive as final UTF-8 bytes — jsonb/uuid were
            // rewritten to ::text upstream (arrow_kind returns None there).
            ColB::Utf8 { v, off, d } | ColB::Bin { v, off, d } => {
                d.extend_from_slice(f);
                off.push(
                    i32::try_from(d.len())
                        .map_err(|_| bad("varlen column past i32 offsets in one batch"))?,
                );
                mark(v, row, true);
            }
        }
        Ok(())
    }

    /// Resident bytes (data + offsets), for the batch seal gate.
    fn bytes(&self) -> usize {
        match self {
            ColB::I16 { d, .. } => d.len() * 2,
            ColB::I32 { d, .. } | ColB::D32 { d, .. } => d.len() * 4,
            ColB::I64 { d, .. } | ColB::Ts { d, .. } => d.len() * 8,
            ColB::F32 { d, .. } => d.len() * 4,
            ColB::F64 { d, .. } => d.len() * 8,
            ColB::Bool { d, .. } => d.len(),
            ColB::Dec { d, .. } => d.len() * 16,
            ColB::Utf8 { off, d, .. } | ColB::Bin { off, d, .. } => d.len() + off.len() * 4,
        }
    }

    /// Move the buffers out as-is and re-arm with the same modest reserve.
    fn seal(&mut self, cap: usize) -> FinishedCol {
        use std::mem::replace;
        match self {
            ColB::I16 { v, d } => FinishedCol::I16 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 2)),
            },
            ColB::I32 { v, d } | ColB::D32 { v, d } => FinishedCol::I32 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 4)),
            },
            ColB::I64 { v, d } | ColB::Ts { v, d } => FinishedCol::I64 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 8)),
            },
            ColB::F32 { v, d } => FinishedCol::F32 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 4)),
            },
            ColB::F64 { v, d } => FinishedCol::F64 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 8)),
            },
            ColB::Bool { v, d } => FinishedCol::Bool {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 8)),
            },
            ColB::Dec { v, d, .. } => FinishedCol::Dec128 {
                validity: v.take(),
                data: replace(d, Vec::with_capacity(cap / 16)),
            },
            ColB::Utf8 { v, off, d } => FinishedCol::Utf8 {
                validity: v.take(),
                offsets: replace(off, offsets0(cap)),
                data: replace(d, Vec::with_capacity(cap)),
            },
            ColB::Bin { v, off, d } => FinishedCol::Bin {
                validity: v.take(),
                offsets: replace(off, offsets0(cap)),
                data: replace(d, Vec::with_capacity(cap)),
            },
        }
    }
}

// ============================================================================
// Streaming builder: COPY-binary chunks in → sealed ArrowBatches out
// ============================================================================

/// Per-worker streaming builder: feed raw COPY-binary chunks (one 19-byte
/// header per worker stream, then tuples, 0xFFFF trailer at end — the
/// FrameStrip lane's shape), seal a batch whenever ~`batch_bytes` of column
/// data accumulated.
pub struct BatchBuilder {
    cols: Vec<ColB>,
    /// Per-column reserve target — batch_bytes / ncols.
    cap: usize,
    batch_bytes: usize,
    /// Rows in the current (unsealed) batch.
    rows: usize,
    rows_sealed: u64,
    // -- COPY framing state (bounds-first; see bqparquet's module docs)
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    finished: bool,
}

impl BatchBuilder {
    pub fn new(kinds: Vec<ArrowKind>, batch_bytes: usize) -> Self {
        let cap = batch_bytes / kinds.len().max(1);
        Self {
            cols: kinds.iter().map(|k| ColB::new(k, cap)).collect(),
            cap,
            batch_bytes,
            rows: 0,
            rows_sealed: 0,
            buf: Vec::with_capacity(1 << 20),
            pos: 0,
            header_done: false,
            finished: false,
        }
    }

    /// Consume one raw chunk. Complete tuples decode into the column
    /// builders; a partial tail is buffered for the next chunk (bounds-
    /// first two-pass — a tuple split across chunks never rolls back).
    pub fn push(&mut self, chunk: &[u8]) -> Result<()> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        if self.pos > (1 << 20) {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(chunk);

        if !self.header_done {
            if self.buf.len() - self.pos < 19 {
                return Ok(());
            }
            if &self.buf[self.pos..self.pos + 11] != b"PGCOPY\n\xff\r\n\0" {
                return Err(Error::Transfer("pg binary COPY: bad header".into()));
            }
            let ext = u32::from_be_bytes(self.buf[self.pos + 15..self.pos + 19].try_into().unwrap())
                as usize;
            if self.buf.len() - self.pos < 19 + ext {
                return Ok(());
            }
            self.pos += 19 + ext;
            self.header_done = true;
        }

        // O(1) swap frees `self` for the builders while we read the buffer.
        let buf = std::mem::take(&mut self.buf);
        let mut res = Ok(());
        while !self.finished {
            match self.try_tuple(&buf[self.pos..]) {
                Ok(Some((consumed, trailer))) => {
                    self.pos += consumed;
                    if trailer {
                        self.finished = true;
                    } else {
                        self.rows += 1;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    res = Err(e);
                    break;
                }
            }
        }
        self.buf = buf;
        res?;
        // The worker stream is ONE header + tuples + one trailer — anything
        // after the trailer means the framing upstream is broken.
        if self.finished && self.pos < self.buf.len() {
            return Err(Error::Transfer(
                "pg binary COPY: bytes after the trailer".into(),
            ));
        }
        Ok(())
    }

    /// Bounds-first: prove the tuple complete, then decode. `Ok(None)` =
    /// incomplete (wait for more input, nothing consumed or emitted).
    fn try_tuple(&mut self, b: &[u8]) -> Result<Option<(usize, bool)>> {
        if b.len() < 2 {
            return Ok(None);
        }
        let ncols = i16::from_be_bytes(b[..2].try_into().unwrap());
        if ncols == -1 {
            return Ok(Some((2, true)));
        }
        if ncols as usize != self.cols.len() {
            return Err(Error::Transfer(format!(
                "pg binary COPY: tuple has {ncols} fields, expected {}",
                self.cols.len()
            )));
        }
        // Pass 1: bounds walk.
        let mut off = 2usize;
        for _ in 0..self.cols.len() {
            if b.len() < off + 4 {
                return Ok(None);
            }
            let len = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
            off += 4;
            if len < -1 {
                return Err(Error::Transfer(format!(
                    "pg binary COPY: corrupt field length {len}"
                )));
            }
            if len > 0 {
                if b.len() < off + len as usize {
                    return Ok(None);
                }
                off += len as usize;
            }
        }
        // Pass 2: decode (complete by construction).
        let row = self.rows;
        let mut o = 2usize;
        for i in 0..self.cols.len() {
            let len = i32::from_be_bytes(b[o..o + 4].try_into().unwrap());
            o += 4;
            if len == -1 {
                self.cols[i].push_null(row);
                continue;
            }
            let f = &b[o..o + len as usize];
            o += len as usize;
            self.cols[i].push_field(f, row)?;
        }
        Ok(Some((off, false)))
    }

    fn acc_bytes(&self) -> usize {
        self.cols.iter().map(ColB::bytes).sum()
    }

    fn seal(&mut self) -> ArrowBatch {
        let rows = self.rows;
        self.rows = 0;
        self.rows_sealed += rows as u64;
        let cap = self.cap;
        ArrowBatch {
            rows,
            cols: self.cols.iter_mut().map(|c| c.seal(cap)).collect(),
        }
    }

    /// A sealed batch, if the size threshold was crossed. Push decodes whole
    /// tuples only, so any moment between push calls is a tuple boundary.
    pub fn take_ready(&mut self) -> Option<ArrowBatch> {
        if self.rows == 0 || self.acc_bytes() < self.batch_bytes {
            return None;
        }
        Some(self.seal())
    }

    /// Stream over (trailer seen): the final partial batch, if any rows.
    pub fn finish(&mut self) -> Result<Option<ArrowBatch>> {
        if !self.finished {
            return Err(Error::Transfer(
                "pg binary COPY: stream ended without the trailer".into(),
            ));
        }
        Ok((self.rows > 0).then(|| self.seal()))
    }

    /// Rows decoded so far (for reporting).
    pub fn rows_total(&self) -> u64 {
        self.rows_sealed + self.rows as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pgcopy;

    #[test]
    fn kind_map_mirrors_the_parquet_gate() {
        use Delivered as D;
        let int = |bytes| D::Int { bytes, unsigned: false };
        assert_eq!(arrow_kind(&int(2)), Some(ArrowKind::Int16));
        assert_eq!(arrow_kind(&int(4)), Some(ArrowKind::Int32));
        assert_eq!(arrow_kind(&int(8)), Some(ArrowKind::Int64));
        assert_eq!(
            arrow_kind(&D::Decimal { p: 18, s: 4 }),
            Some(ArrowKind::Decimal { p: 18, s: 4 })
        );
        assert_eq!(arrow_kind(&D::Decimal { p: 0, s: 0 }), None); // unconstrained
        assert_eq!(arrow_kind(&D::Decimal { p: 50, s: 2 }), None); // > i128 digits
        assert_eq!(arrow_kind(&D::Uuid), None); // ::text — hyphenated on the wire
        assert_eq!(arrow_kind(&D::Json), None); // ::text — jsonb version byte
        assert_eq!(arrow_kind(&D::Text), Some(ArrowKind::Utf8));
        assert_eq!(arrow_kind(&D::Bytes), Some(ArrowKind::Binary));
        assert_eq!(arrow_kind(&D::Date), Some(ArrowKind::Date32));
        assert_eq!(arrow_kind(&D::DateTime { utc: true }), Some(ArrowKind::TimestampUtc));
        assert_eq!(arrow_kind(&D::DateTime { utc: false }), Some(ArrowKind::TimestampNaive));
    }

    #[test]
    fn decodes_every_kind_byte_by_byte_into_exact_buffers() {
        let kinds = vec![
            ArrowKind::Int16,
            ArrowKind::Int32,
            ArrowKind::Int64,
            ArrowKind::Float32,
            ArrowKind::Float64,
            ArrowKind::Bool,
            ArrowKind::Decimal { p: 18, s: 4 },
            ArrowKind::Date32,
            ArrowKind::TimestampUtc,
            ArrowKind::TimestampNaive,
            ArrowKind::Utf8,
            ArrowKind::Binary,
        ];
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        // Row 0: every column valid.
        pgcopy::tuple_start(12, &mut s);
        pgcopy::field(&(-7i16).to_be_bytes(), &mut s);
        pgcopy::field(&123_456i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-9_876_543_210i64).to_be_bytes(), &mut s);
        pgcopy::field(&1.5f32.to_be_bytes(), &mut s);
        pgcopy::field(&(-2.25f64).to_be_bytes(), &mut s);
        pgcopy::field(&[1], &mut s);
        pgcopy::numeric_field_from_str("1234.5678", &mut s).unwrap();
        pgcopy::field(&0i32.to_be_bytes(), &mut s); // 2000-01-01
        pgcopy::field(&0i64.to_be_bytes(), &mut s); // PG epoch
        pgcopy::field(&1_500_000i64.to_be_bytes(), &mut s); // +1.5 s
        pgcopy::field(b"hello", &mut s);
        pgcopy::field(&[0xDE, 0xAD], &mut s);
        // Row 1: all NULL.
        pgcopy::tuple_start(12, &mut s);
        for _ in 0..12 {
            pgcopy::null_field(&mut s);
        }
        // Row 2: empty string, empty-field bool, IEEE specials, negatives.
        pgcopy::tuple_start(12, &mut s);
        pgcopy::field(&32_000i16.to_be_bytes(), &mut s);
        pgcopy::field(&(-1i32).to_be_bytes(), &mut s);
        pgcopy::field(&42i64.to_be_bytes(), &mut s);
        pgcopy::field(&f32::NEG_INFINITY.to_be_bytes(), &mut s);
        pgcopy::field(&f64::NAN.to_be_bytes(), &mut s);
        pgcopy::field(&[], &mut s); // empty field = false
        pgcopy::numeric_field_from_str("-0.5000", &mut s).unwrap();
        pgcopy::field(&365i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-1_000_000i64).to_be_bytes(), &mut s);
        pgcopy::field(&1i64.to_be_bytes(), &mut s);
        pgcopy::field(b"", &mut s);
        pgcopy::field(&[1, 2, 3], &mut s);
        pgcopy::trailer(&mut s);

        // Feed byte-by-byte: chunk boundaries anywhere must be safe.
        let mut b = BatchBuilder::new(kinds, 1 << 20);
        for byte in &s {
            b.push(std::slice::from_ref(byte)).unwrap();
        }
        assert!(b.take_ready().is_none()); // threshold never crossed
        let batch = b.finish().unwrap().expect("rows pending");
        assert_eq!(batch.rows, 3);
        assert_eq!(b.rows_total(), 3);

        // Every column: rows 0 and 2 valid, row 1 NULL → 0b101.
        let valid = Some(&[0b101u8][..]);
        match &batch.cols[0] {
            FinishedCol::I16 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[-7, 0, 32_000]);
            }
            _ => panic!("col 0 layout"),
        }
        match &batch.cols[1] {
            FinishedCol::I32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[123_456, 0, -1]);
            }
            _ => panic!("col 1 layout"),
        }
        match &batch.cols[2] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[-9_876_543_210, 0, 42]);
            }
            _ => panic!("col 2 layout"),
        }
        match &batch.cols[3] {
            FinishedCol::F32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[1.5, 0.0, f32::NEG_INFINITY]);
            }
            _ => panic!("col 3 layout"),
        }
        match &batch.cols[4] {
            FinishedCol::F64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data[0], -2.25);
                assert_eq!(data[1], 0.0);
                assert!(data[2].is_nan());
            }
            _ => panic!("col 4 layout"),
        }
        match &batch.cols[5] {
            FinishedCol::Bool { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[0b001]); // true, null→0, false
            }
            _ => panic!("col 5 layout"),
        }
        match &batch.cols[6] {
            FinishedCol::Dec128 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[12_345_678, 0, -5_000]);
            }
            _ => panic!("col 6 layout"),
        }
        match &batch.cols[7] {
            FinishedCol::I32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[10_957, 0, 11_322]); // + PG_EPOCH_DAYS
            }
            _ => panic!("col 7 layout"),
        }
        match &batch.cols[8] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[946_684_800_000_000, 0, 946_684_799_000_000]);
            }
            _ => panic!("col 8 layout"),
        }
        match &batch.cols[9] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[946_684_801_500_000, 0, 946_684_800_000_001]);
            }
            _ => panic!("col 9 layout"),
        }
        match &batch.cols[10] {
            FinishedCol::Utf8 { validity, offsets, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(offsets, &[0, 5, 5, 5]); // null and "" both repeat
                assert_eq!(data, b"hello");
            }
            _ => panic!("col 10 layout"),
        }
        match &batch.cols[11] {
            FinishedCol::Bin { validity, offsets, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(offsets, &[0, 2, 2, 5]);
                assert_eq!(data, &[0xDE, 0xAD, 1, 2, 3]);
            }
            _ => panic!("col 11 layout"),
        }
    }

    #[test]
    fn seals_at_the_byte_threshold_and_counts_add_up() {
        // 20 bytes/row (8 i64 + 8 text + 4 offset) + the 4-byte offsets
        // base → a 64-byte threshold crosses at exactly 3 rows.
        let mut b = BatchBuilder::new(vec![ArrowKind::Int64, ArrowKind::Utf8], 64);
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        b.push(&s).unwrap();
        let mut sealed = Vec::new();
        for i in 0..10i64 {
            let mut t = Vec::new();
            pgcopy::tuple_start(2, &mut t);
            pgcopy::field(&i.to_be_bytes(), &mut t);
            pgcopy::field(b"12345678", &mut t);
            b.push(&t).unwrap();
            if let Some(batch) = b.take_ready() {
                assert!(b.take_ready().is_none()); // counter reset at seal
                sealed.push(batch);
            }
        }
        let mut t = Vec::new();
        pgcopy::trailer(&mut t);
        b.push(&t).unwrap();
        let tail = b.finish().unwrap().expect("a partial remains");
        assert_eq!(sealed.iter().map(|x| x.rows).collect::<Vec<_>>(), vec![3, 3, 3]);
        assert_eq!(tail.rows, 1);
        assert_eq!(b.rows_total(), 10);
        // No NULLs anywhere → validity None; offsets re-seed at 0 per batch.
        match (&sealed[1].cols[0], &sealed[1].cols[1]) {
            (
                FinishedCol::I64 { validity, data },
                FinishedCol::Utf8 { validity: v2, offsets, .. },
            ) => {
                assert!(validity.is_none() && v2.is_none());
                assert_eq!(data, &[3, 4, 5]);
                assert_eq!(offsets, &[0, 8, 16, 24]);
            }
            _ => panic!("layout"),
        }
    }

    #[test]
    fn date_and_timestamp_infinity_error_loudly() {
        for (kind, field) in [
            (ArrowKind::Date32, i32::MAX.to_be_bytes().to_vec()),
            (ArrowKind::Date32, i32::MIN.to_be_bytes().to_vec()),
            (ArrowKind::TimestampUtc, i64::MAX.to_be_bytes().to_vec()),
            (ArrowKind::TimestampNaive, i64::MIN.to_be_bytes().to_vec()),
        ] {
            let mut s = Vec::new();
            pgcopy::header(&mut s);
            pgcopy::tuple_start(1, &mut s);
            pgcopy::field(&field, &mut s);
            let err = BatchBuilder::new(vec![kind], 1 << 20).push(&s).unwrap_err();
            assert!(err.to_string().contains("infinity"), "{err}");
        }
    }

    #[test]
    fn trailer_gates_finish_and_rejects_trailing_bytes() {
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        pgcopy::tuple_start(1, &mut s);
        pgcopy::field(&7i16.to_be_bytes(), &mut s);
        // No trailer yet = truncated stream.
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&s).unwrap();
        assert!(b.finish().is_err());
        // Trailer then finish = the partial batch.
        let mut with_trailer = s.clone();
        pgcopy::trailer(&mut with_trailer);
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&with_trailer).unwrap();
        let batch = b.finish().unwrap().expect("partial after trailer");
        assert_eq!(batch.rows, 1);
        assert_eq!(b.rows_total(), 1);
        // Bytes after the trailer: loud, whether in the same chunk or later.
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&with_trailer).unwrap();
        assert!(b.push(&[0]).is_err());
        let mut junk_tail = with_trailer.clone();
        junk_tail.push(0);
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        assert!(b.push(&junk_tail).is_err());
    }
}
