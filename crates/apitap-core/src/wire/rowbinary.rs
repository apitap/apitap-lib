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
use crate::wire::pgcopy::{numeric_to_scaled_i128, numeric_to_scaled_i128_raw, PG_EPOCH_DAYS, PG_EPOCH_MICROS};

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

/// ClickHouse column type (as `DESCRIBE TABLE` spells it) → (RbType, nullable).
/// The CDC RowBinary body resolves the PHYSICAL destination table's types once
/// per table (dest_ch), so the body always matches what the bootstrap created:
/// a pgoutput Relation message carries no nullability, and only the table
/// knows it. Anything unrecognised returns `None` and the whole table falls
/// back to the TSV path — correctness first.
pub(crate) fn rb_type_from_ch(ch: &str) -> Option<(RbType, bool)> {
    let (inner, nullable) = match ch.strip_prefix("Nullable(").and_then(|s| s.strip_suffix(')')) {
        Some(i) => (i, true),
        None => (ch, false),
    };
    let ty = match inner {
        // One-byte passthrough: pg bool lands as UInt8 (the bulk lane's
        // shape); Bool and Int8 share the encoding.
        "Bool" | "UInt8" | "Int8" => RbType::Bool,
        "Int16" | "UInt16" => RbType::Swap(2),
        "Int32" | "UInt32" => RbType::Swap(4),
        "Int64" | "UInt64" => RbType::Swap(8),
        "Float32" => RbType::Swap(4),
        "Float64" => RbType::Swap(8),
        "Date32" => RbType::Date32,
        // ONLY microsecond DateTime64: the wire transcodes int64 micros, and
        // a bare DateTime / another precision would silently mis-scale.
        s if s.starts_with("DateTime64(6") => RbType::Ts64,
        "String" => RbType::String,
        "UUID" => RbType::Uuid,
        "JSON" => RbType::JsonB,
        s if s.starts_with("Decimal(") => {
            let (p, sc) = parse_ch_decimal(s)?;
            RbType::Decimal {
                width: if p <= 9 {
                    4
                } else if p <= 18 {
                    8
                } else {
                    16
                },
                scale: sc,
            }
        }
        _ => return None,
    };
    Some((ty, nullable))
}

/// The conversion rule for one CDC column: the PHYSICAL ClickHouse type gives
/// the encoding width and nullability (rb_type_from_ch), the pgoutput column
/// OID gives the CONVERSION — jsonb's version header must be stripped, bytea's
/// binary form (raw bytes) does not match its `\x…` text shape in ClickHouse,
/// and so on. `None` → the whole table stays on the TSV path.
pub(crate) fn rb_type_for_cdc(oid: u32, ch: RbType) -> Option<RbType> {
    Some(match oid {
        16 => match ch { RbType::Bool => ch, _ => return None },                  // bool
        20 => match ch { RbType::Swap(8) => ch, _ => return None },               // int8
        21 => match ch { RbType::Swap(2) => ch, _ => return None },               // int2
        23 => match ch { RbType::Swap(4) => ch, _ => return None },               // int4
        700 => match ch { RbType::Swap(4) => ch, _ => return None },              // float4
        701 => match ch { RbType::Swap(8) => ch, _ => return None },              // float8
        1082 => match ch { RbType::Date32 => ch, _ => return None },              // date
        1114 | 1184 => match ch { RbType::Ts64 => ch, _ => return None },         // ts/tstz
        1700 => match ch {                                                        // numeric
            RbType::Decimal { .. } | RbType::NumericF64 => ch,
            _ => return None,
        },
        2950 => match ch { RbType::Uuid => ch, _ => return None },                // uuid
        25 | 1043 | 1042 | 19 | 114 => match ch {                                 // text-ish
            RbType::String => ch,
            _ => return None,
        },
        3802 => match ch { RbType::String => RbType::JsonB, _ => return None },   // jsonb: strip version
        _ => return None, // bytea, arrays, oid/time/char, unknown — TSV fallback
    })
}

fn parse_ch_decimal(s: &str) -> Option<(u32, u32)> {
    let inner = s.strip_prefix("Decimal(")?.strip_suffix(')')?;
    let (p, sc) = inner.split_once(',')?;
    Some((p.trim().parse().ok()?, sc.trim().parse().ok()?))
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
    /// Tuples emitted since the last harvest — the honest live row count for
    /// this lane, free because the tuple loop already knows each boundary.
    rows: u64,
}

impl Transcoder {
    pub(crate) fn new(cols: Vec<(RbType, bool)>) -> Self {
        Self {
            cols,
            // Carry buffer only ever holds the header prefix or a partial tuple; both
            // are rare and small, so it grows on demand (a 1 MiB preallocation here
            // cost spans × 1 MiB of pure churn per run — measured, never used).
            buf: Vec::new(),
            pos: 0,
            header_done: false,
            finished: false,
            rows: 0,
        }
    }

    /// Feed input; append transcoded RowBinary to `out`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn push(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        // Compact a fully-consumed buffer so the fast path stays reachable.
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }

        let mut inp = input;

        // Pending header or partial tuple: feed it in doubling slices until it
        // parses through, then fall into the fast path with the rest of `inp`.
        // Appending the whole piece here instead would strand every coalesced
        // raw-plane piece (~256 KiB, arbitrary split points) on this path for
        // the rest of the stream — one extra full-stream memcpy.
        let mut step = 4 << 10;
        while !inp.is_empty() && !(self.header_done && self.buf.is_empty()) {
            let take = step.min(inp.len());
            step = (step * 2).min(1 << 20);
            self.buf.extend_from_slice(&inp[..take]);
            inp = &inp[take..];

            if !self.header_done {
                if self.buf.len() - self.pos < 19 {
                    continue;
                }
                if &self.buf[self.pos..self.pos + 11] != b"PGCOPY\n\xff\r\n\0" {
                    return Err(Error::Transfer("pg binary COPY: bad header".into()));
                }
                let ext =
                    u32::from_be_bytes(self.buf[self.pos + 15..self.pos + 19].try_into().unwrap())
                        as usize;
                if self.buf.len() - self.pos < 19 + ext {
                    continue;
                }
                self.pos += 19 + ext;
                self.header_done = true;
            }
            while !self.finished {
                match try_tuple_at(&self.cols, &self.buf[self.pos..], out)? {
                    Some((consumed, finished)) => {
                        self.pos += consumed;
                        if !finished {
                            self.rows += 1;
                        }
                        self.finished = finished;
                    }
                    None => break,
                }
            }
            if self.finished {
                return Ok(());
            }
            if self.pos == self.buf.len() {
                self.buf.clear();
                self.pos = 0;
            } else if self.pos > (1 << 20) {
                self.buf.drain(..self.pos);
                self.pos = 0;
            }
        }

        // Fast path: nothing pending — transcode complete tuples straight from `inp`
        // and buffer only the partial tail. Postgres emits one CopyData per row, so
        // on the sqlx plane this is ~every push; the raw plane lands here for the
        // bulk of each coalesced piece.
        let mut off = 0usize;
        while !self.finished {
            match try_tuple_at(&self.cols, &inp[off..], out)? {
                Some((consumed, finished)) => {
                    off += consumed;
                    if !finished {
                        self.rows += 1;
                    }
                    self.finished = finished;
                }
                None => break,
            }
        }
        if off < inp.len() && !self.finished {
            self.buf.extend_from_slice(&inp[off..]);
        }
        Ok(())
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished
    }

    /// Take the tuples counted since the last call, for progress reporting.
    pub(crate) fn take_rows(&mut self) -> u64 {
        std::mem::take(&mut self.rows)
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
        if len < 0 {
            out.truncate(out_start);
            return Err(Error::Transfer(format!(
                "pg binary COPY: negative field length {len}"
            )));
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

pub(crate) fn transcode_field(ty: RbType, f: &[u8], out: &mut Vec<u8>) -> Result<()> {
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
            // Postgres spells +infinity/-infinity as INT32_MAX/MIN days; adding
            // the epoch offset without the sentinel check wraps them into
            // garbage dates (system review 2026-10-07, P6). ClickHouse's
            // Date32 has no infinity, so refuse by name with the remedy.
            if d == i32::MAX || d == i32::MIN {
                return Err(Error::Transfer(
                    "pg binary COPY: date 'infinity' has no ClickHouse Date32 value — cast the \
                     column to text in a source view, or filter those rows"
                        .into(),
                ));
            }
            let unix = d.checked_add(PG_EPOCH_DAYS).ok_or_else(|| bad("date"))?;
            out.extend(unix.to_le_bytes());
        }
        RbType::Ts64 => {
            let t = i64::from_be_bytes(f.try_into().map_err(|_| bad("timestamp"))?);
            // Same for timestamp/timestamptz (INT64_MAX/MIN microseconds).
            if t == i64::MAX || t == i64::MIN {
                return Err(Error::Transfer(
                    "pg binary COPY: timestamp 'infinity' has no ClickHouse DateTime64 value — \
                     cast the column to text in a source view, or filter those rows"
                        .into(),
                ));
            }
            let unix = t.checked_add(PG_EPOCH_MICROS).ok_or_else(|| bad("timestamp"))?;
            out.extend(unix.to_le_bytes());
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
        let mut t = Transcoder::new(cols.clone());
        let mut out = Vec::new();
        for c in input.chunks(3) {
            t.push(c, &mut out).unwrap();
        }
        assert!(t.finished());
        assert_eq!(out, expected);

        // Raw-plane shape: a dangling carry followed by one big coalesced piece.
        // The pending prefix must complete through the carry and the remainder
        // must ride the fast path, leaving the carry buffer empty mid-stream.
        for split in [1usize, 7, 21, 25] {
            let mut t = Transcoder::new(cols.clone());
            let mut out = Vec::new();
            t.push(&input[..split], &mut out).unwrap();
            t.push(&input[split..], &mut out).unwrap();
            assert!(t.finished(), "split {split}");
            assert_eq!(out, expected, "split {split}");
        }
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
    fn corrupt_negative_field_length_errors_not_panics() {
        let mut input = b"PGCOPY\n\xff\r\n\0".to_vec();
        input.extend(0u32.to_be_bytes());
        input.extend(0u32.to_be_bytes());
        input.extend(1i16.to_be_bytes());
        input.extend((-2i32).to_be_bytes()); // hostile length
        let mut t = Transcoder::new(vec![(RbType::Swap(4), false)]);
        assert!(t.push(&input, &mut Vec::new()).is_err());
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

    /// Postgres spells infinity as INT32_MAX/MIN (date) and INT64_MAX/MIN
    /// (timestamp) since its own epoch; adding the epoch offset wrapped them
    /// into garbage dates with no error (system review 2026-10-07, P6).
    /// ClickHouse has no infinity here, so the transcode must refuse — and
    /// finite values must keep the checked arithmetic.
    #[test]
    fn infinity_dates_and_timestamps_are_refused_not_wrapped() {
        for d in [i32::MAX, i32::MIN] {
            let e = transcode_field(RbType::Date32, &d.to_be_bytes(), &mut Vec::new()).unwrap_err();
            assert!(e.to_string().contains("infinity"), "{e}");
        }
        for t in [i64::MAX, i64::MIN] {
            let e = transcode_field(RbType::Ts64, &t.to_be_bytes(), &mut Vec::new()).unwrap_err();
            assert!(e.to_string().contains("infinity"), "{e}");
        }
        let mut out = Vec::new();
        // A finite far-future date still rebases (checked, no wrap).
        let d = 100_000i32; // ~2293-10-27, inside ClickHouse Date32's range.
        transcode_field(RbType::Date32, &d.to_be_bytes(), &mut out).unwrap();
        assert_eq!(out, (d + PG_EPOCH_DAYS).to_le_bytes());
    }

    /// The P4 destination-side mapping: ClickHouse DESCRIBE spellings to the
    /// (RbType, nullable) the RowBinary body renderer uses. Anything outside
    /// the covered set MUST return None — that is the whole-table TSV
    /// fallback, not a guess.
    #[test]
    fn ch_column_types_map_to_rowbinary() {
        assert_eq!(rb_type_from_ch("Nullable(String)"), Some((RbType::String, true)));
        assert_eq!(rb_type_from_ch("Int32"), Some((RbType::Swap(4), false)));
        assert_eq!(rb_type_from_ch("Nullable(Int16)"), Some((RbType::Swap(2), true)));
        assert_eq!(rb_type_from_ch("Nullable(Int64)"), Some((RbType::Swap(8), true)));
        assert_eq!(rb_type_from_ch("Nullable(Float64)"), Some((RbType::Swap(8), true)));
        assert_eq!(rb_type_from_ch("Nullable(UInt8)"), Some((RbType::Bool, true)));
        assert_eq!(rb_type_from_ch("Nullable(Date32)"), Some((RbType::Date32, true)));
        assert_eq!(rb_type_from_ch("Nullable(DateTime64(6))"), Some((RbType::Ts64, true)));
        assert_eq!(rb_type_from_ch("DateTime64(6, 'UTC')"), Some((RbType::Ts64, false)));
        assert_eq!(
            rb_type_from_ch("Nullable(Decimal(18, 4))"),
            Some((RbType::Decimal { width: 8, scale: 4 }, true))
        );
        assert_eq!(rb_type_from_ch("Nullable(UUID)"), Some((RbType::Uuid, true)));
        // Outside the covered set: TSV fallback, never a mis-scaled write.
        assert_eq!(rb_type_from_ch("DateTime"), None);
        assert_eq!(rb_type_from_ch("DateTime64(3)"), None);
        assert_eq!(rb_type_from_ch("Array(Int32)"), None);
        assert_eq!(rb_type_from_ch("Nullable(FixedString(4))"), None);
    }

    /// The CDC conversion rule: the PG OID refines the CH encoding — jsonb
    /// strips its version header, bytea (raw binary vs the `\x…` text the
    /// column holds) and any unknown OID fall back to TSV.
    #[test]
    fn cdc_oid_rules_refine_the_ch_type() {
        assert_eq!(rb_type_for_cdc(3802, RbType::String), Some(RbType::JsonB));
        assert_eq!(rb_type_for_cdc(25, RbType::String), Some(RbType::String));
        assert_eq!(rb_type_for_cdc(1043, RbType::String), Some(RbType::String));
        assert_eq!(rb_type_for_cdc(114, RbType::String), Some(RbType::String));
        assert_eq!(rb_type_for_cdc(23, RbType::Swap(4)), Some(RbType::Swap(4)));
        assert_eq!(rb_type_for_cdc(23, RbType::Swap(8)), None); // width mismatch
        assert_eq!(rb_type_for_cdc(1700, RbType::Decimal { width: 8, scale: 4 }), Some(RbType::Decimal { width: 8, scale: 4 }));
        assert_eq!(rb_type_for_cdc(17, RbType::String), None); // bytea
        assert_eq!(rb_type_for_cdc(1009, RbType::String), None); // text[]
        assert_eq!(rb_type_for_cdc(1083, RbType::String), None); // time
    }
}
