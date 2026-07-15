//! Postgres binary-COPY → ClickHouse RowBinary transcoder.
//!
//! Text was the wall: at 10M rows the pipeline plateaued ~300 MB/s with Postgres paying
//! per-row int/date/float FORMATTING and ClickHouse paying tokenize+unescape+PARSE — and
//! neither side's CPU fully used. Binary-to-binary moves that work to this process
//! (which has idle cores) and shrinks it: most fields are a byte-swap or a straight
//! copy.
//!
//! Postgres `COPY … (FORMAT binary)` wire: a 19-byte header (`PGCOPY\n\xff\r\n\0` +
//! 4-byte flags + 4-byte extension length), then per tuple an int16 column count and
//! per field an int32 byte length (−1 = NULL) + payload (big-endian), then a 0xFFFF
//! trailer. ClickHouse `RowBinary`: fields back-to-back per row — little-endian fixed
//! widths, varint-prefixed strings, and for `Nullable(T)` a 0/1 flag byte before the
//! value.

use crate::error::{Error, Result};

/// PG epoch (2000-01-01) → Unix epoch, in days / microseconds.
pub(crate) const PG_EPOCH_DAYS: i32 = 10_957;
pub(crate) const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// How to transcode one column. Field order mirrors the SELECT / DDL order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum RbType {
    /// int2/int4/int8/float4/float8: byte-swap BE→LE at the given width.
    Swap(usize),
    /// bool: single byte passes through.
    Bool,
    /// date: int32 days PG-epoch → Date32 days Unix-epoch.
    Date32,
    /// timestamp/timestamptz: int64 micros PG-epoch → DateTime64(6) micros Unix-epoch.
    Ts64,
    /// NUMERIC(p,s) → Decimal of `width` bytes (4/8/16) with `scale` s.
    Decimal { width: usize, scale: u32 },
    /// NUMERIC without declared precision → Float64 (documented lossy, matches the DDL).
    NumericF64,
    /// text-ish: varint length + raw bytes.
    String,
    /// jsonb: like String, minus the 1-byte version header.
    JsonB,
    /// uuid: 16 RFC bytes → ClickHouse's two reversed 8-byte halves.
    Uuid,
}

/// Which Postgres udt_names the binary path supports. Anything else → the caller falls
/// back to the TSV path for the whole table.
pub(crate) fn rb_type(udt: &str, precision: Option<i32>, scale: Option<i32>) -> Option<RbType> {
    Some(match udt {
        "int2" => RbType::Swap(2),
        "int4" => RbType::Swap(4),
        "int8" => RbType::Swap(8),
        "float4" => RbType::Swap(4),
        "float8" => RbType::Swap(8),
        "bool" => RbType::Bool,
        "date" => RbType::Date32,
        "timestamp" | "timestamptz" => RbType::Ts64,
        "numeric" => match (precision, scale) {
            // p ≤ 38 covers Decimal32/64/128; p > 38 would be Decimal256 (32-byte
            // little-endian) which this transcoder doesn't emit → whole-table TSV
            // fallback keeps it correct.
            (Some(p), Some(s)) if p <= 38 => RbType::Decimal {
                width: if p <= 9 {
                    4
                } else if p <= 18 {
                    8
                } else {
                    16
                },
                scale: s.max(0) as u32,
            },
            (Some(p), _) if p > 38 => return None,
            _ => RbType::NumericF64,
        },
        "varchar" | "bpchar" | "text" | "name" | "json" => RbType::String,
        "jsonb" => RbType::JsonB,
        "uuid" => RbType::Uuid,
        _ => return None,
    })
}

/// Streaming transcoder. Feed it Postgres binary-COPY bytes in arbitrary chunk sizes;
/// it emits RowBinary bytes for every COMPLETE tuple and buffers partial tuples across
/// chunk boundaries (sqlx yields one chunk per CopyData message, but never trust
/// framing).
pub(crate) struct Transcoder {
    cols: Vec<(RbType, bool)>, // (type, nullable)
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    finished: bool,
}

impl Transcoder {
    pub(crate) fn new(cols: Vec<(RbType, bool)>) -> Self {
        Self {
            cols,
            buf: Vec::with_capacity(1 << 20),
            pos: 0,
            header_done: false,
            finished: false,
        }
    }

    /// Feed input; append transcoded RowBinary to `out`.
    pub(crate) fn push(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        // Compact a fully-consumed buffer so the fast path stays reachable.
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }

        // Fast path: nothing pending — transcode complete tuples straight from `input`
        // and buffer only the partial tail. Postgres emits one CopyData per row, so
        // after the header this is ~every push; the unconditional copy-into-buf was an
        // extra full-stream memcpy.
        if self.header_done && self.buf.is_empty() {
            let mut off = 0usize;
            while !self.finished {
                match try_tuple_at(&self.cols, &input[off..], out)? {
                    Some((consumed, finished)) => {
                        off += consumed;
                        self.finished = finished;
                    }
                    None => break,
                }
            }
            if off < input.len() && !self.finished {
                self.buf.extend_from_slice(&input[off..]);
            }
            return Ok(());
        }

        // Slow path: header pending or a partial tuple carried over a chunk boundary.
        if self.pos > (1 << 20) {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(input);

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

        while !self.finished {
            match try_tuple_at(&self.cols, &self.buf[self.pos..], out)? {
                Some((consumed, finished)) => {
                    self.pos += consumed;
                    self.finished = finished;
                }
                None => break,
            }
        }
        Ok(())
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished
    }
}

/// Try to transcode ONE complete tuple from the start of `b`; returns
/// `(bytes consumed, reached_trailer)`, or None if the tuple is still incomplete
/// (`out` is left untouched in that case).
fn try_tuple_at(
    cols: &[(RbType, bool)],
    b: &[u8],
    out: &mut Vec<u8>,
) -> Result<Option<(usize, bool)>> {
    if b.len() < 2 {
        return Ok(None);
    }
    let ncols = i16::from_be_bytes(b[..2].try_into().unwrap());
    if ncols == -1 {
        return Ok(Some((2, true))); // trailer
    }
    if ncols as usize != cols.len() {
        return Err(Error::Transfer(format!(
            "pg binary COPY: tuple has {ncols} fields, expected {}",
            cols.len()
        )));
    }
    let out_start = out.len();
    let mut off = 2usize;
    for (ty, nullable) in cols {
        if b.len() < off + 4 {
            out.truncate(out_start);
            return Ok(None);
        }
        let len = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
        off += 4;
        if len == -1 {
            if !*nullable {
                out.truncate(out_start);
                return Err(Error::Transfer(
                    "NULL in a column ClickHouse declared non-nullable".into(),
                ));
            }
            out.push(1); // Nullable(T): null flag, no value
            continue;
        }
        let len = len as usize;
        if b.len() < off + len {
            out.truncate(out_start);
            return Ok(None);
        }
        if *nullable {
            out.push(0);
        }
        transcode_field(*ty, &b[off..off + len], out)?;
        off += len;
    }
    Ok(Some((off, false)))
}

/// Test hook: lets the pgcopy ENCODER prove itself against this decoder (roundtrip).
#[cfg(test)]
pub(crate) fn numeric_to_scaled_i128_for_tests(f: &[u8], scale: u32) -> Result<i128> {
    numeric_to_scaled_i128(f, scale)
}

pub(crate) fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn transcode_field(ty: RbType, f: &[u8], out: &mut Vec<u8>) -> Result<()> {
    match ty {
        // Fixed-width bswap forms (not iter().rev(): a runtime-length reversed iterator
        // compiles to a per-byte loop; this is the single most-executed match arm).
        RbType::Swap(w) => match (w, f.len()) {
            (2, 2) => out.extend_from_slice(&[f[1], f[0]]),
            (4, 4) => {
                out.extend_from_slice(&u32::from_be_bytes(f.try_into().unwrap()).to_le_bytes())
            }
            (8, 8) => {
                out.extend_from_slice(&u64::from_be_bytes(f.try_into().unwrap()).to_le_bytes())
            }
            _ => return Err(Error::Transfer(format!("field width {} != {w}", f.len()))),
        },
        RbType::Bool => out.push(f[0]),
        RbType::Date32 => {
            let d = i32::from_be_bytes(f.try_into().map_err(|_| bad("date"))?);
            out.extend((d + PG_EPOCH_DAYS).to_le_bytes());
        }
        RbType::Ts64 => {
            let t = i64::from_be_bytes(f.try_into().map_err(|_| bad("timestamp"))?);
            out.extend((t + PG_EPOCH_MICROS).to_le_bytes());
        }
        RbType::Decimal { width, scale } => {
            let v = numeric_to_scaled_i128(f, scale)?;
            match width {
                4 => out.extend((v as i32).to_le_bytes()),
                8 => out.extend((v as i64).to_le_bytes()),
                _ => out.extend(v.to_le_bytes()),
            }
        }
        RbType::NumericF64 => {
            let (v, dscale) = numeric_to_scaled_i128_raw(f)?;
            out.extend((v as f64 / 10f64.powi(dscale)).to_le_bytes());
        }
        RbType::String => {
            varint(f.len() as u64, out);
            out.extend_from_slice(f);
        }
        RbType::JsonB => {
            if f.is_empty() || f[0] != 1 {
                return Err(bad("jsonb version"));
            }
            varint((f.len() - 1) as u64, out);
            out.extend_from_slice(&f[1..]);
        }
        RbType::Uuid => {
            if f.len() != 16 {
                return Err(bad("uuid"));
            }
            out.extend_from_slice(&u64::from_be_bytes(f[..8].try_into().unwrap()).to_le_bytes());
            out.extend_from_slice(&u64::from_be_bytes(f[8..].try_into().unwrap()).to_le_bytes());
        }
    }
    Ok(())
}

fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: malformed {what} field"))
}

/// PG binary NUMERIC (ndigits, weight, sign, dscale + base-10000 digit groups) → an
/// integer scaled to exactly `scale` decimal places.
pub(crate) fn numeric_to_scaled_i128(f: &[u8], scale: u32) -> Result<i128> {
    let (acc_scaled_dscale, dscale) = numeric_to_scaled_i128_raw(f)?;
    // acc is scaled to dscale places; rescale to the declared scale.
    let diff = scale as i32 - dscale;
    Ok(match diff.cmp(&0) {
        std::cmp::Ordering::Equal => acc_scaled_dscale,
        std::cmp::Ordering::Greater => acc_scaled_dscale
            .checked_mul(10i128.pow(diff as u32))
            .ok_or_else(|| bad("numeric overflow"))?,
        std::cmp::Ordering::Less => acc_scaled_dscale / 10i128.pow((-diff) as u32),
    })
}

/// → (value scaled to `dscale` decimal places, dscale).
fn numeric_to_scaled_i128_raw(f: &[u8]) -> Result<(i128, i32)> {
    if f.len() < 8 {
        return Err(bad("numeric"));
    }
    let ndigits = i16::from_be_bytes(f[0..2].try_into().unwrap()) as i32;
    let weight = i16::from_be_bytes(f[2..4].try_into().unwrap()) as i32;
    let sign = u16::from_be_bytes(f[4..6].try_into().unwrap());
    let dscale = u16::from_be_bytes(f[6..8].try_into().unwrap()) as i32;
    if sign == 0xC000 {
        return Err(bad("numeric NaN"));
    }
    if f.len() < 8 + (ndigits as usize) * 2 {
        return Err(bad("numeric digits"));
    }
    // Accumulate all digit groups, tracking the decimal exponent of the LAST group:
    // value = acc × 10000^(weight − ndigits + 1).
    let mut acc: i128 = 0;
    for i in 0..ndigits {
        let d = u16::from_be_bytes(
            f[8 + i as usize * 2..10 + i as usize * 2]
                .try_into()
                .unwrap(),
        );
        acc = acc
            .checked_mul(10_000)
            .and_then(|a| a.checked_add(d as i128))
            .ok_or_else(|| bad("numeric overflow"))?;
    }
    let exp4 = weight - ndigits + 1; // exponent in base-10000 groups
    let mut exp10 = exp4 * 4 + dscale; // shift needed so acc is scaled to dscale places
                                       // acc currently = value × 10000^(ndigits-1-weight) = value × 10^(-exp4·4)
                                       // we want value × 10^dscale = acc × 10^(exp4·4 + dscale)
    while exp10 > 0 {
        acc = acc.checked_mul(10).ok_or_else(|| bad("numeric overflow"))?;
        exp10 -= 1;
    }
    while exp10 < 0 {
        acc /= 10;
        exp10 += 1;
    }
    if sign == 0x4000 {
        acc = -acc;
    }
    Ok((acc, dscale))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one PG-binary field: int32 length + payload.
    fn field(payload: &[u8]) -> Vec<u8> {
        let mut v = (payload.len() as i32).to_be_bytes().to_vec();
        v.extend_from_slice(payload);
        v
    }

    /// PG binary numeric for 1234.5678 → ndigits=2? digits base 10000: 1234, 5678 with
    /// weight 0, dscale 4.
    fn pg_numeric_1234_5678() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend(2i16.to_be_bytes()); // ndigits
        v.extend(0i16.to_be_bytes()); // weight
        v.extend(0u16.to_be_bytes()); // sign +
        v.extend(4u16.to_be_bytes()); // dscale
        v.extend(1234u16.to_be_bytes());
        v.extend(5678u16.to_be_bytes());
        v
    }

    #[test]
    fn numeric_scales_exactly() {
        // 1234.5678 at scale 4 → 12345678
        assert_eq!(
            numeric_to_scaled_i128(&pg_numeric_1234_5678(), 4).unwrap(),
            12_345_678
        );
        // …at scale 6 → ×100
        assert_eq!(
            numeric_to_scaled_i128(&pg_numeric_1234_5678(), 6).unwrap(),
            1_234_567_800
        );
        // 50.0000 as PG emits it for (g%1e6)/100: digits [50], weight 0, dscale 4:
        let mut v = Vec::new();
        v.extend(1i16.to_be_bytes());
        v.extend(0i16.to_be_bytes());
        v.extend(0u16.to_be_bytes());
        v.extend(4u16.to_be_bytes());
        v.extend(50u16.to_be_bytes());
        assert_eq!(numeric_to_scaled_i128(&v, 4).unwrap(), 500_000);
    }

    #[test]
    fn transcodes_a_full_tuple_split_across_chunks() {
        // Columns: id Int32 (non-null), name String (nullable), ok Bool (non-null).
        let cols = vec![
            (RbType::Swap(4), false),
            (RbType::String, true),
            (RbType::Bool, false),
        ];
        let mut input = b"PGCOPY\n\xff\r\n\0".to_vec();
        input.extend(0u32.to_be_bytes()); // flags
        input.extend(0u32.to_be_bytes()); // ext len
                                          // Tuple 1: id=7, name="hi", ok=true
        input.extend(3i16.to_be_bytes());
        input.extend(field(&7i32.to_be_bytes()));
        input.extend(field(b"hi"));
        input.extend(field(&[1u8]));
        // Tuple 2: id=8, name=NULL, ok=false
        input.extend(3i16.to_be_bytes());
        input.extend(field(&8i32.to_be_bytes()));
        input.extend((-1i32).to_be_bytes()); // NULL
        input.extend(field(&[0u8]));
        input.extend((-1i16).to_be_bytes()); // trailer

        let mut expected = Vec::new();
        expected.extend(7i32.to_le_bytes());
        expected.extend([0u8, 2, b'h', b'i', 1]); // notnull flag, varint 2, "hi", true
        expected.extend(8i32.to_le_bytes());
        expected.extend([1u8, 0]); // null flag, false

        // Feed in pathological 3-byte chunks to exercise partial-tuple buffering.
        let mut t = Transcoder::new(cols);
        let mut out = Vec::new();
        for c in input.chunks(3) {
            t.push(c, &mut out).unwrap();
        }
        assert!(t.finished());
        assert_eq!(out, expected);
    }

    #[test]
    fn fast_path_transcodes_from_input_without_buffering() {
        // PG's real framing: one CopyData per row. After the header push, every push is
        // whole tuples and must leave the internal buffer EMPTY (that's the zero-copy
        // claim), with output identical to the chunked slow path.
        let cols = vec![(RbType::Swap(4), false), (RbType::String, true)];
        let mut header = b"PGCOPY\n\xff\r\n\0".to_vec();
        header.extend(0u32.to_be_bytes());
        header.extend(0u32.to_be_bytes());
        let mut tuple1 = 2i16.to_be_bytes().to_vec();
        tuple1.extend(field(&7i32.to_be_bytes()));
        tuple1.extend(field(b"hi"));
        let mut tuple2 = 2i16.to_be_bytes().to_vec();
        tuple2.extend(field(&8i32.to_be_bytes()));
        tuple2.extend((-1i32).to_be_bytes()); // NULL

        let mut expected = Vec::new();
        expected.extend(7i32.to_le_bytes());
        expected.extend([0u8, 2, b'h', b'i']);
        expected.extend(8i32.to_le_bytes());
        expected.push(1);

        let mut t = Transcoder::new(cols.clone());
        let mut out = Vec::new();
        t.push(&header, &mut out).unwrap();
        t.push(&tuple1, &mut out).unwrap();
        assert!(t.buf.is_empty(), "fast path must not buffer whole tuples");
        t.push(&tuple2, &mut out).unwrap();
        assert!(t.buf.is_empty());
        t.push(&(-1i16).to_be_bytes(), &mut out).unwrap();
        assert!(t.finished());
        assert_eq!(out, expected);

        // Mixed framing: 1.5 tuples in one push (fast path emits tuple 1, buffers the
        // half), then the rest + trailer (slow path drains, hands back to fast path).
        let mut t = Transcoder::new(cols);
        let mut out = Vec::new();
        let split = tuple2.len() / 2;
        let mut push1 = header.clone();
        push1.extend_from_slice(&tuple1);
        push1.extend_from_slice(&tuple2[..split]);
        t.push(&push1, &mut out).unwrap();
        assert!(!t.buf.is_empty(), "partial tail must be buffered");
        let mut push2 = tuple2[split..].to_vec();
        push2.extend((-1i16).to_be_bytes());
        t.push(&push2, &mut out).unwrap();
        assert!(t.finished());
        assert_eq!(out, expected);
    }

    #[test]
    fn swap_and_uuid_reverse_bytes() {
        let mut out = Vec::new();
        transcode_field(RbType::Swap(2), &[1, 2], &mut out).unwrap();
        transcode_field(RbType::Swap(4), &[1, 2, 3, 4], &mut out).unwrap();
        transcode_field(RbType::Swap(8), &[1, 2, 3, 4, 5, 6, 7, 8], &mut out).unwrap();
        assert_eq!(out, [2, 1, 4, 3, 2, 1, 8, 7, 6, 5, 4, 3, 2, 1]);
        assert!(transcode_field(RbType::Swap(4), &[1, 2], &mut Vec::new()).is_err());
        out.clear();
        let uuid: Vec<u8> = (1..=16).collect();
        transcode_field(RbType::Uuid, &uuid, &mut out).unwrap();
        assert_eq!(out, [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9]);
    }

    #[test]
    fn date_and_timestamp_rebase_epochs() {
        let mut out = Vec::new();
        // 2020-01-01 = 7305 days after 2000-01-01 = 18262 days after 1970-01-01.
        transcode_field(RbType::Date32, &7305i32.to_be_bytes(), &mut out).unwrap();
        assert_eq!(out, 18262i32.to_le_bytes());
        out.clear();
        transcode_field(RbType::Ts64, &0i64.to_be_bytes(), &mut out).unwrap();
        assert_eq!(out, PG_EPOCH_MICROS.to_le_bytes());
    }
}
