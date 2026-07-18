//! MySQL connector: [`MySqlSource`] — a ROW source. MySQL has no COPY protocol, so
//! there is no byte-passthrough trick here: rows arrive through the regular wire
//! protocol and the worker DECODES each value straight off the binary protocol's raw
//! bytes (no typed-decode dispatch, no per-cell allocations) and ENCODES the lane's
//! wire format directly — ClickHouse RowBinary or Postgres binary COPY, no
//! intermediate text, no Arrow.

use super::{pop, spans, WorkQueue};
use crate::sink::Loader;
use crate::source::Source;
use crate::error::{Error, Result};
use crate::wire::pgcopy as pgc;
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::wire::rowbinary::varint;
use crate::dialect::mysql::{is_binary_udt, my_ident, my_ident_path};
use crate::wire::mytsv::tsv_escape;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::Row;

/// How to decode a MySQL column and encode it as RowBinary.
#[derive(Clone, Copy, Debug, PartialEq)]
enum MyRb {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    /// DECIMAL(p≤38, s): arrives as text (MySQL sends NEWDECIMAL as a string) → exact
    /// scaled integer of `width` bytes.
    Dec {
        width: usize,
        scale: u32,
    },
    /// date → Date32 (days since Unix epoch).
    Date,
    /// datetime → DateTime64(6): wall time as the UTC session serves it.
    DateTime,
    /// timestamp → DateTime64(6,'UTC'): an absolute instant.
    TsUtc,
    /// char/varchar/*text/enum/set/json/time and DECIMAL p>38 → String.
    Str,
    /// binary/varbinary/*blob/bit → String (raw bytes).
    Bytes,
}

/// How to encode one MySQL column as a Postgres binary-COPY field.
#[derive(Clone, Copy, Debug, PartialEq)]
enum PgEnc {
    SmallFromI8,
    SmallFromU8,
    Small,
    SmallFromYear,
    IntFromU16,
    Int,
    BigFromU32,
    Big,
    /// BIGINT UNSIGNED → numeric(20,0), exact.
    NumFromU64,
    F32,
    F64,
    /// DECIMAL of any precision: arrives as text, encodes as binary `numeric`, exact.
    NumStr,
    Date,
    Ts,
    TsTz,
    Text,
    /// JSON text → `jsonb` (version byte + text).
    JsonbText,
    Bytea,
}

/// (RowBinary decoder, delivery) for one MySQL column. The `unsigned` marker lives in
/// COLUMN_TYPE, not DATA_TYPE.
fn my_rb(c: &ColumnPlan) -> Result<(MyRb, Delivered)> {
    let unsigned = c.native_ddl.as_deref().unwrap_or("").contains("unsigned");
    let int = |bytes: u8| Delivered::Int { bytes, unsigned };
    Ok(match c.udt.as_str() {
        "tinyint" if unsigned => (MyRb::U8, int(1)),
        "tinyint" => (MyRb::I8, int(1)),
        "smallint" if unsigned => (MyRb::U16, int(2)),
        "smallint" => (MyRb::I16, int(2)),
        "mediumint" | "int" if unsigned => (MyRb::U32, int(4)),
        "mediumint" | "int" => (MyRb::I32, int(4)),
        "bigint" if unsigned => (MyRb::U64, int(8)),
        "bigint" => (MyRb::I64, int(8)),
        "float" => (MyRb::F32, Delivered::Float32),
        "double" => (MyRb::F64, Delivered::Float64),
        "decimal" => match (c.precision, c.scale) {
            (Some(p), Some(s)) if p <= 38 => (
                MyRb::Dec {
                    width: if p <= 9 {
                        4
                    } else if p <= 18 {
                        8
                    } else {
                        16
                    },
                    scale: s as u32,
                },
                Delivered::Decimal {
                    p: p as u16,
                    s: s as u16,
                },
            ),
            // MySQL DECIMAL goes up to p=65 — beyond Decimal128 it rides as exact text.
            _ => (MyRb::Str, Delivered::Text),
        },
        "date" => (MyRb::Date, Delivered::Date),
        "datetime" => (MyRb::DateTime, Delivered::DateTime { utc: false }),
        // TIMESTAMP is UTC-normalized by the session (`SET time_zone = '+00:00'`).
        "timestamp" => (MyRb::TsUtc, Delivered::DateTime { utc: true }),
        "year" => (
            MyRb::U16,
            Delivered::Int {
                bytes: 2,
                unsigned: true,
            },
        ),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" | "time" => (MyRb::Str, Delivered::Text),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            (MyRb::Bytes, Delivered::Bytes)
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "mysql type '{other}' is not supported yet"
            )))
        }
    })
}

/// (Postgres binary-COPY encoder, delivery) for one MySQL column — lossless: unsigned
/// types widen, BIGINT UNSIGNED and any-precision DECIMAL go through exact `numeric`.
fn my_pg(c: &ColumnPlan) -> Result<(PgEnc, Delivered)> {
    let unsigned = c.native_ddl.as_deref().unwrap_or("").contains("unsigned");
    let int = |bytes: u8| Delivered::Int {
        bytes,
        unsigned: false,
    };
    Ok(match c.udt.as_str() {
        "tinyint" if unsigned => (PgEnc::SmallFromU8, int(2)),
        "tinyint" => (PgEnc::SmallFromI8, int(2)),
        "smallint" if unsigned => (PgEnc::IntFromU16, int(4)),
        "smallint" => (PgEnc::Small, int(2)),
        "mediumint" | "int" if unsigned => (PgEnc::BigFromU32, int(8)),
        "mediumint" | "int" => (PgEnc::Int, int(4)),
        "bigint" if unsigned => (PgEnc::NumFromU64, Delivered::Decimal { p: 20, s: 0 }),
        "bigint" => (PgEnc::Big, int(8)),
        "float" => (PgEnc::F32, Delivered::Float32),
        "double" => (PgEnc::F64, Delivered::Float64),
        "decimal" => match (c.precision, c.scale) {
            (Some(p), Some(s)) => (
                PgEnc::NumStr,
                Delivered::Decimal {
                    p: p as u16,
                    s: s as u16,
                },
            ),
            _ => (PgEnc::NumStr, Delivered::Decimal { p: 0, s: 0 }),
        },
        "date" => (PgEnc::Date, Delivered::Date),
        "datetime" => (PgEnc::Ts, Delivered::DateTime { utc: false }),
        "timestamp" => (PgEnc::TsTz, Delivered::DateTime { utc: true }),
        "year" => (PgEnc::SmallFromYear, int(2)),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        | "time" => (PgEnc::Text, Delivered::Text),
        "json" => (PgEnc::JsonbText, Delivered::Json),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            (PgEnc::Bytea, Delivered::Bytes)
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "mysql type '{other}' is not supported yet"
            )))
        }
    })
}

/// Exact text-decimal → integer scaled to `scale` places ("1234.5678", 4 → 12345678).
/// Operates on raw ASCII bytes — the hot path hands wire bytes straight in, no String.
fn dec_bytes_to_scaled_i128(s: &[u8], scale: u32) -> Result<i128> {
    let bad = || {
        Error::Transfer(format!(
            "malformed decimal '{}'",
            String::from_utf8_lossy(s)
        ))
    };
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    let dot = digits.iter().position(|&b| b == b'.');
    let (int_part, frac_part) = match dot {
        Some(p) => (&digits[..p], &digits[p + 1..]),
        None => (digits, &[][..]),
    };
    let mut acc: i128 = 0;
    for &c in int_part {
        if !c.is_ascii_digit() {
            return Err(bad());
        }
        acc = acc
            .checked_mul(10)
            .and_then(|a| a.checked_add((c - b'0') as i128))
            .ok_or_else(bad)?;
    }
    for i in 0..scale as usize {
        let d = frac_part.get(i).copied().unwrap_or(b'0');
        if !d.is_ascii_digit() {
            return Err(bad());
        }
        acc = acc
            .checked_mul(10)
            .and_then(|a| a.checked_add((d - b'0') as i128))
            .ok_or_else(bad)?;
    }
    // Digits beyond the declared scale would mean silent truncation — MySQL doesn't
    // store them for a DECIMAL(p,s) column, so anything here is a real inconsistency.
    if frac_part.len() > scale as usize && frac_part[scale as usize..].iter().any(|&b| b != b'0') {
        return Err(bad());
    }
    Ok(if neg { -acc } else { acc })
}

/// Days from the Unix epoch for a civil date (Howard Hinnant's algorithm) — the hot
/// path avoids chrono entirely.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = ((m as i64) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + (d as i64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// MySQL binary-protocol DATE payload `[len][year u16 LE][month][day]` → days since
/// the Unix epoch. Zero-length = the invalid zero date; refuse it.
fn bin_date_days(b: &[u8]) -> Result<i64> {
    if b.len() < 5 || b[0] < 4 {
        return Err(Error::Transfer("malformed/zero binary DATE".into()));
    }
    let y = u16::from_le_bytes([b[1], b[2]]) as i32;
    Ok(days_from_civil(y, b[3] as u32, b[4] as u32))
}

/// MySQL binary-protocol DATETIME/TIMESTAMP payload
/// `[len][year u16][month][day][hour][min][sec][micros u32]` (len ∈ 4/7/11) → Unix
/// micros. The session runs UTC, so this is absolute for TIMESTAMP and wall-as-UTC for
/// DATETIME.
fn bin_datetime_micros(b: &[u8]) -> Result<i64> {
    if b.is_empty() || b[0] < 4 || b.len() < 1 + b[0] as usize {
        return Err(Error::Transfer("malformed/zero binary DATETIME".into()));
    }
    let days = bin_date_days(b)?;
    let (mut secs, mut micros) = (0i64, 0i64);
    if b[0] >= 7 {
        secs = b[5] as i64 * 3600 + b[6] as i64 * 60 + b[7] as i64;
    }
    if b[0] >= 11 {
        micros = u32::from_le_bytes([b[8], b[9], b[10], b[11]]) as i64;
    }
    Ok((days * 86_400 + secs) * 1_000_000 + micros)
}

/// Raw wire bytes for column `i`, or None for NULL. sqlx's `query()` always prepares,
/// so rows arrive in the BINARY protocol whose per-column slices sqlx has already cut —
/// `try_get_raw` hands them over without the typed-decode dispatch or the per-cell
/// String/Vec allocations that `try_get::<T>` pays (150M cells at 10M rows).
fn raw_cell(row: &MySqlRow, i: usize) -> Result<Option<&[u8]>> {
    use sqlx::ValueRef;
    let v =
        sqlx::Row::try_get_raw(row, i).map_err(|e| Error::Transfer(format!("column {i}: {e}")))?;
    if v.is_null() {
        return Ok(None);
    }
    let b: &[u8] = sqlx::Decode::<sqlx::MySql>::decode(v)
        .map_err(|e| Error::Transfer(format!("column {i} bytes: {e}")))?;
    Ok(Some(b))
}

fn want(b: &[u8], w: usize, i: usize) -> Result<()> {
    if b.len() != w {
        return Err(Error::Transfer(format!(
            "column {i}: width {} != {w}",
            b.len()
        )));
    }
    Ok(())
}

/// Encode one column as RowBinary straight from the wire bytes. MySQL's binary
/// protocol stores ints/floats little-endian fixed-width — exactly RowBinary's layout,
/// so those are pure copies.
fn encode_value(
    row: &MySqlRow,
    i: usize,
    ty: MyRb,
    nullable: bool,
    out: &mut Vec<u8>,
) -> Result<()> {
    let Some(b) = raw_cell(row, i)? else {
        if !nullable {
            return Err(Error::Transfer(format!("NULL in non-nullable column {i}")));
        }
        out.push(1);
        return Ok(());
    };
    if nullable {
        out.push(0);
    }
    match ty {
        MyRb::I8 | MyRb::U8 => {
            want(b, 1, i)?;
            out.extend_from_slice(b);
        }
        MyRb::I16 | MyRb::U16 => {
            want(b, 2, i)?;
            out.extend_from_slice(b);
        }
        MyRb::I32 | MyRb::U32 | MyRb::F32 => {
            want(b, 4, i)?;
            out.extend_from_slice(b);
        }
        MyRb::I64 | MyRb::U64 | MyRb::F64 => {
            want(b, 8, i)?;
            out.extend_from_slice(b);
        }
        MyRb::Dec { width, scale } => {
            let v = dec_bytes_to_scaled_i128(b, scale)?;
            match width {
                4 => out.extend((v as i32).to_le_bytes()),
                8 => out.extend((v as i64).to_le_bytes()),
                _ => out.extend(v.to_le_bytes()),
            }
        }
        MyRb::Date => out.extend((bin_date_days(b)? as i32).to_le_bytes()),
        MyRb::DateTime | MyRb::TsUtc => out.extend(bin_datetime_micros(b)?.to_le_bytes()),
        MyRb::Str | MyRb::Bytes => {
            varint(b.len() as u64, out);
            out.extend_from_slice(b);
        }
    }
    Ok(())
}

/// Encode one column as a Postgres binary-COPY field straight from the wire bytes
/// (little-endian in, big-endian out). The staging table carries no NOT NULL
/// constraints, so NULL is always legal.
fn encode_pg(row: &MySqlRow, i: usize, enc: PgEnc, out: &mut Vec<u8>) -> Result<()> {
    let Some(b) = raw_cell(row, i)? else {
        pgc::null_field(out);
        return Ok(());
    };
    match enc {
        PgEnc::SmallFromI8 => {
            want(b, 1, i)?;
            pgc::field(&(b[0] as i8 as i16).to_be_bytes(), out);
        }
        PgEnc::SmallFromU8 => {
            want(b, 1, i)?;
            pgc::field(&(b[0] as i16).to_be_bytes(), out);
        }
        PgEnc::Small => {
            want(b, 2, i)?;
            pgc::field(&[b[1], b[0]], out);
        }
        PgEnc::SmallFromYear => {
            want(b, 2, i)?;
            pgc::field(
                &(u16::from_le_bytes([b[0], b[1]]) as i16).to_be_bytes(),
                out,
            );
        }
        PgEnc::IntFromU16 => {
            want(b, 2, i)?;
            pgc::field(
                &(u16::from_le_bytes([b[0], b[1]]) as i32).to_be_bytes(),
                out,
            );
        }
        PgEnc::Int | PgEnc::F32 => {
            want(b, 4, i)?;
            pgc::field(&[b[3], b[2], b[1], b[0]], out);
        }
        PgEnc::BigFromU32 => {
            want(b, 4, i)?;
            pgc::field(
                &(u32::from_le_bytes(b.try_into().unwrap()) as i64).to_be_bytes(),
                out,
            );
        }
        PgEnc::Big | PgEnc::F64 => {
            want(b, 8, i)?;
            pgc::field(&[b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0]], out);
        }
        PgEnc::NumFromU64 => {
            want(b, 8, i)?;
            // Format into a stack buffer — a heap String per cell is pure overhead on
            // this CPU-tight feeder (u64::MAX is 20 digits).
            let mut v = u64::from_le_bytes(b.try_into().unwrap());
            let mut tmp = [0u8; 20];
            let mut p = tmp.len();
            loop {
                p -= 1;
                tmp[p] = b'0' + (v % 10) as u8;
                v /= 10;
                if v == 0 {
                    break;
                }
            }
            pgc::numeric_field_from_str(std::str::from_utf8(&tmp[p..]).unwrap(), out)?;
        }
        PgEnc::NumStr => {
            let s = std::str::from_utf8(b)
                .map_err(|_| Error::Transfer(format!("column {i}: decimal not ascii")))?;
            pgc::numeric_field_from_str(s, out)?;
        }
        PgEnc::Date => {
            pgc::field(
                &((bin_date_days(b)? as i32) - pgc::PG_EPOCH_DAYS).to_be_bytes(),
                out,
            );
        }
        PgEnc::Ts | PgEnc::TsTz => {
            pgc::field(
                &(bin_datetime_micros(b)? - pgc::PG_EPOCH_MICROS).to_be_bytes(),
                out,
            );
        }
        PgEnc::Text => pgc::field(b, out),
        PgEnc::JsonbText => pgc::jsonb_field(b, out),
        PgEnc::Bytea => pgc::field(b, out),
    }
    Ok(())
}

/// Per-column encoder for the MySQL text lane: read the BINARY wire value and
/// render the exact text `LOAD DATA` expects, client-side. Reading binary (not
/// `CAST … AS CHAR`) is ~3x cheaper on the wire and offloads nothing to the
/// source — the measured win that took mysql→mysql under the CAST-text lane's
/// floor. Strings/decimals/json are already text on the wire (StrEsc just
/// escapes); ints/floats/dates/datetimes are decoded + formatted here.
#[derive(Clone, Copy)]
enum MyTsv {
    Int {
        bytes: u8,
        unsigned: bool,
    },
    F32,
    F64,
    /// Already text on the wire (varchar/text/json/enum/set/decimal-as-text,
    /// and time via CAST) — escape and relay.
    StrEsc,
    /// Binary DATE payload → `YYYY-MM-DD`.
    DateBin,
    /// Binary DATETIME/TIMESTAMP payload → `YYYY-MM-DD HH:MM:SS[.ffffff]`.
    DateTimeBin,
    /// binary/blob/bit → uppercase HEX (the sink UNHEXes it back).
    Hex,
}

fn my_tsv(c: &ColumnPlan) -> MyTsv {
    let unsigned = c.native_ddl.as_deref().unwrap_or("").contains("unsigned");
    let int = |bytes: u8| MyTsv::Int { bytes, unsigned };
    match c.udt.as_str() {
        "tinyint" => int(1),
        "smallint" | "year" => int(2),
        "mediumint" | "int" => int(4),
        "bigint" => int(8),
        "float" => MyTsv::F32,
        "double" => MyTsv::F64,
        "date" => MyTsv::DateBin,
        "datetime" | "timestamp" => MyTsv::DateTimeBin,
        u if is_binary_udt(u) => MyTsv::Hex,
        // char/varchar/text*/enum/set/json/decimal/time → already text (time is
        // CAST to CHAR in the SELECT; decimal is NEWDECIMAL ascii on the wire).
        _ => MyTsv::StrEsc,
    }
}

const HEXDIG: &[u8; 16] = b"0123456789ABCDEF";

/// Render one non-NULL MySQL binary field as `LOAD DATA` text into `out`.
fn tsv_write(enc: MyTsv, b: &[u8], out: &mut Vec<u8>) -> Result<()> {
    match enc {
        MyTsv::StrEsc => tsv_escape(b, out),
        MyTsv::Int { bytes, unsigned } => {
            let mut buf = itoa::Buffer::new();
            let s = match (bytes, unsigned) {
                (1, false) => buf.format(i8::from_le_bytes([b[0]]) as i64),
                (1, true) => buf.format(b[0] as u64),
                (2, false) => buf.format(i16::from_le_bytes([b[0], b[1]]) as i64),
                (2, true) => buf.format(u16::from_le_bytes([b[0], b[1]]) as u64),
                (4, false) => {
                    buf.format(i32::from_le_bytes(b.try_into().map_err(|_| bad_int())?) as i64)
                }
                (4, true) => {
                    buf.format(u32::from_le_bytes(b.try_into().map_err(|_| bad_int())?) as u64)
                }
                (8, false) => buf.format(i64::from_le_bytes(b.try_into().map_err(|_| bad_int())?)),
                _ => buf.format(u64::from_le_bytes(b.try_into().map_err(|_| bad_int())?)),
            };
            out.extend_from_slice(s.as_bytes());
        }
        MyTsv::F32 => {
            let v = f32::from_le_bytes(b.try_into().map_err(|_| bad_int())?);
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format(v).as_bytes());
        }
        MyTsv::F64 => {
            let v = f64::from_le_bytes(b.try_into().map_err(|_| bad_int())?);
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format(v).as_bytes());
        }
        MyTsv::DateBin => write_date(b, out)?,
        MyTsv::DateTimeBin => write_datetime(b, out)?,
        MyTsv::Hex => {
            out.reserve(b.len() * 2);
            for &byte in b {
                out.push(HEXDIG[(byte >> 4) as usize]);
                out.push(HEXDIG[(byte & 0xf) as usize]);
            }
        }
    }
    Ok(())
}

fn bad_int() -> Error {
    Error::Transfer("mysql binary numeric: unexpected width".into())
}

fn w4(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 1000 % 10) as u8);
    out.push(b'0' + (v / 100 % 10) as u8);
    out.push(b'0' + (v / 10 % 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}
fn w2(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 10 % 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// MySQL binary DATE `[len][year u16 LE][month][day]` → `YYYY-MM-DD`.
fn write_date(b: &[u8], out: &mut Vec<u8>) -> Result<()> {
    // Length 0 = MySQL's all-zero date '0000-00-00' (legal under a lax sql_mode);
    // the dest loads it under the same lax mode, so relay the literal.
    if b == [0] {
        out.extend_from_slice(b"0000-00-00");
        return Ok(());
    }
    if b.is_empty() || b[0] < 4 || b.len() < 5 {
        return Err(Error::Transfer("malformed binary DATE".into()));
    }
    let y = u16::from_le_bytes([b[1], b[2]]) as u32;
    w4(out, y);
    out.push(b'-');
    w2(out, b[3] as u32);
    out.push(b'-');
    w2(out, b[4] as u32);
    Ok(())
}

/// MySQL binary DATETIME/TIMESTAMP `[len][y u16][mon][day]([h][m][s]([micros u32]))`
/// (len ∈ 4/7/11) → `YYYY-MM-DD HH:MM:SS[.ffffff]`. Trailing zero components are
/// omitted by MySQL; we mirror that so a DATETIME(0) round-trips without a
/// spurious `.000000` and a DATETIME(6) keeps its exact fraction.
fn write_datetime(b: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if b == [0] {
        out.extend_from_slice(b"0000-00-00 00:00:00");
        return Ok(());
    }
    if b.is_empty() || b[0] < 4 {
        return Err(Error::Transfer("malformed binary DATETIME".into()));
    }
    write_date(b, out)?;
    let len = b[0];
    let (h, mi, s) = if len >= 7 {
        (b[5] as u32, b[6] as u32, b[7] as u32)
    } else {
        (0, 0, 0)
    };
    out.push(b' ');
    w2(out, h);
    out.push(b':');
    w2(out, mi);
    out.push(b':');
    w2(out, s);
    if len >= 11 {
        let micros = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
        if micros > 0 {
            out.push(b'.');
            let mut buf = [0u8; 6];
            let mut m = micros;
            for i in (0..6).rev() {
                buf[i] = b'0' + (m % 10) as u8;
                m /= 10;
            }
            out.extend_from_slice(&buf);
        }
    }
    Ok(())
}




// ---------------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------------

pub(crate) struct MySqlSource {
    pool: MySqlPool,
}

impl MySqlSource {
    pub(crate) async fn connect(url: &str, max_conns: usize) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(max_conns as u32)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    // TIMESTAMP columns then arrive as UTC wall time.
                    sqlx::Executor::execute(conn, "SET time_zone = '+00:00'").await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(|e| Error::Connect(e.to_string()))?;
        Ok(Self { pool })
    }
}

impl Source for MySqlSource {
    async fn catalog(
        &self,
        schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        // TABLE_ROWS is InnoDB's estimate (may be stale) — it only orders and
        // sizes the work; NULL = unknown = -1 (treated as large).
        if let Some(ts) = tables {
            // Resolve the current database once so qualified and bare names get
            // EXACT (schema, table) matching — probe stays the final authority,
            // but a typo should fail here, before any table has moved.
            let curdb: Option<String> = sqlx::query_scalar("SELECT DATABASE()")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::Connect(e.to_string()))?;
            let mut pairs = Vec::with_capacity(ts.len());
            for t in ts {
                let (s, bare) = match t.rsplit_once('.') {
                    Some((s, b)) => (s.to_string(), b.to_string()),
                    None => (
                        curdb.clone().ok_or_else(|| {
                            Error::InvalidInput(format!(
                                "table '{t}' is unqualified and the mysql url has no \
                                 database — use 'db.{t}'"
                            ))
                        })?,
                        t.to_string(),
                    ),
                };
                pairs.push((s, bare));
            }
            let mut sql = String::from(
                "SELECT CAST(TABLE_SCHEMA AS CHAR) AS s, CAST(TABLE_NAME AS CHAR) AS t, \
                        CAST(COALESCE(TABLE_ROWS, -1) AS SIGNED) AS est \
                 FROM information_schema.tables WHERE ",
            );
            sql.push_str(
                &std::iter::repeat_n("(TABLE_SCHEMA = ? AND TABLE_NAME = ?)", pairs.len())
                    .collect::<Vec<_>>()
                    .join(" OR "),
            );
            let mut q = sqlx::query(&sql);
            for (s, b) in &pairs {
                q = q.bind(s).bind(b);
            }
            let rows = q
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::InvalidInput(format!("resolving tables: {e}")))?;
            let found: std::collections::HashMap<(String, String), i64> = rows
                .iter()
                .map(|r| {
                    (
                        (r.get::<String, _>("s"), r.get::<String, _>("t")),
                        r.get::<i64, _>("est"),
                    )
                })
                .collect();
            let mut out = Vec::with_capacity(ts.len());
            for (given, (s, b)) in ts.iter().zip(&pairs) {
                // Second chance lowercased: lower_case_table_names=1/2 servers
                // store (and return) lowercase while accepting any-case input —
                // probe and the read path take the given spelling fine.
                let est = found
                    .get(&(s.clone(), b.clone()))
                    .or_else(|| found.get(&(s.to_lowercase(), b.to_lowercase())));
                match est {
                    Some(est) => out.push((given.clone(), *est)),
                    None => {
                        return Err(Error::InvalidInput(format!(
                            "source table {given} not found"
                        )))
                    }
                }
            }
            return Ok(out);
        }
        // Whole schema (None = the URL's database). BASE TABLE only — views are
        // derived data; apitap's own staging/state artifacts never travel.
        let rows = sqlx::query(
            "SELECT CAST(TABLE_NAME AS CHAR) AS t, CAST(COALESCE(TABLE_ROWS, -1) AS SIGNED) AS est \
             FROM information_schema.tables \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_TYPE = 'BASE TABLE' \
               AND TABLE_NAME NOT LIKE '%|_|_apitap|_staging' ESCAPE '|' \
               AND TABLE_NAME NOT LIKE '%|_|_apitap|_old' ESCAPE '|' \
               AND TABLE_NAME <> '_apitap_state' \
             ORDER BY TABLE_ROWS DESC",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::InvalidInput(format!("listing tables: {e}")))?;
        Ok(rows
            .iter()
            .map(|r| {
                let t: String = r.get("t");
                let name = match schema {
                    Some(s) => format!("{s}.{t}"),
                    None => t,
                };
                (name, r.get::<i64, _>("est"))
            })
            .collect())
    }

    fn cursor_quoted(&self, udt: &str) -> Result<bool> {
        crate::dialect::mysql::cursor_quoted(udt)
    }

    async fn probe(&self, table: &str) -> Result<TablePlan> {
        let (schema, bare) = match table.rsplit_once('.') {
            Some((s, t)) => (Some(s.to_string()), t.to_string()),
            None => (None, table.to_string()),
        };
        // `CAST(… AS CHAR)` everywhere: MySQL 8 serves these columns with a BINARY
        // collation that drivers refuse to decode as text.
        let rows = sqlx::query(
            "SELECT CAST(COLUMN_NAME AS CHAR) AS name, CAST(DATA_TYPE AS CHAR) AS dt, \
                    CAST(COLUMN_TYPE AS CHAR) AS ct, NUMERIC_PRECISION AS p, NUMERIC_SCALE AS s, \
                    CAST(IS_NULLABLE AS CHAR) AS nullable, CAST(COLUMN_KEY AS CHAR) AS ckey, \
                    CAST(CHARACTER_SET_NAME AS CHAR) AS charset, \
                    CAST(COLLATION_NAME AS CHAR) AS collation \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
        )
        .bind(&schema)
        .bind(&bare)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::InvalidInput(format!("probing {table}: {e}")))?;
        if rows.is_empty() {
            return Err(Error::InvalidInput(format!(
                "source table {table} not found"
            )));
        }
        let mut cols = Vec::with_capacity(rows.len());
        let mut pk_cols: Vec<String> = Vec::new();
        for r in &rows {
            let dt: String = r.get("dt");
            let nullable: String = r.get("nullable");
            let ckey: String = r.get("ckey");
            if ckey == "PRI" {
                pk_cols.push(r.get::<String, _>("name"));
            }
            let col = ColumnPlan {
                name: r.get("name"),
                nullable: nullable == "YES",
                int_pk: ckey == "PRI"
                    && matches!(
                        dt.as_str(),
                        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
                    ),
                native_ddl: Some({
                    // Same-engine mirror: COLUMN_TYPE alone drops CHARACTER SET /
                    // COLLATE, so a case/accent-SENSITIVE text PK would be rebuilt
                    // case-insensitive and silently collapse distinct keys. Fold the
                    // source charset+collation back in (both NULL for numerics).
                    let ct: String = r.get("ct");
                    let charset: Option<String> = r.try_get("charset").unwrap_or(None);
                    let collation: Option<String> = r.try_get("collation").unwrap_or(None);
                    match (charset, collation) {
                        (Some(cs), Some(col)) => {
                            format!("{ct} CHARACTER SET {cs} COLLATE {col}")
                        }
                        _ => ct,
                    }
                }),
                udt: dt,
                precision: r
                    .try_get::<Option<u64>, _>("p")
                    .unwrap_or(None)
                    .map(|v| v as i32),
                scale: r
                    .try_get::<Option<u64>, _>("s")
                    .unwrap_or(None)
                    .map(|v| v as i32),
            };
            // Fail fast, with the type name, instead of at lane planning.
            my_rb(&col)?;
            cols.push(col);
        }
        Ok(TablePlan {
            engine: "mysql",
            cols,
            cursor: None,
            pk_cols,
        })
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        // Probe validated every column for the row encoders; the text lane
        // renders server-side (CAST/HEX) so it handles every column too.
        matches!(
            format,
            WireFormat::RowBinary | WireFormat::PgCopyBinary | WireFormat::MyTsv
        )
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        let cols = plan
            .cols
            .iter()
            .map(|c| {
                let q = my_ident(&c.name);
                // TIME arrives in a binary layout the decoders don't cover — cast it
                // server-side. DECIMAL and JSON need no cast: the raw-bytes path reads
                // NEWDECIMAL's ASCII digits and JSON's utf8 text straight off the wire.
                let (select, delivered) = match format {
                    WireFormat::PgCopyBinary => {
                        // TIME arrives in a binary layout the decoders don't cover.
                        let sel = if c.udt == "time" {
                            format!("CAST({q} AS CHAR)")
                        } else {
                            q.clone()
                        };
                        (sel, my_pg(c).expect("validated at probe").1)
                    }
                    WireFormat::RowBinary => {
                        let sel = if c.udt == "time" {
                            format!("CAST({q} AS CHAR)")
                        } else {
                            q.clone()
                        };
                        (sel, my_rb(c).expect("validated at probe").1)
                    }
                    // Same-engine text lane: let MySQL render each value as
                    // connection-charset text (round-trips exactly on reload),
                    // HEX for binary so bytes survive the charset. delivered is a
                    // marker — the sink mirrors native_ddl for its DDL, not this.
                    WireFormat::TabSeparated => unreachable!("can_produce refuses the PG text dialect for the MySQL source"),
                    WireFormat::MyTsv => {
                        // Read BINARY and format client-side (measured ~3x cheaper
                        // than CAST-to-text on the wire). Only TIME is CAST — its
                        // binary layout isn't covered by the decoders.
                        let sel = if c.udt == "time" {
                            format!("CAST({q} AS CHAR)")
                        } else {
                            q.clone()
                        };
                        (sel, Delivered::Text)
                    }
                };
                LaneCol { delivered, select }
            })
            .collect();
        Lane { format, cols }
    }

    async fn span_stmts(
        &self,
        table: &str,
        plan: &TablePlan,
        lane: &Lane,
        want: usize,
        delta: Option<&Delta>,
    ) -> Result<Vec<String>> {
        let src_t = my_ident_path(table);
        let select_list = lane
            .cols
            .iter()
            .map(|c| c.select.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        // Incremental predicate — pushed into every statement, min/max probe included.
        let dpred = delta
            .map(|d| format!(" AND {} {} {}", my_ident(&d.col), d.op, d.literal))
            .unwrap_or_default();
        // Integer cursors range-split; timestamp cursors (valid for incremental
        // watermarks) fall through to a single filtered stream.
        let int_cursor = plan.cursor.as_deref().and_then(|c| {
            plan.cols
                .iter()
                .find(|pc| pc.name == c)
                .filter(|pc| {
                    matches!(
                        pc.udt.as_str(),
                        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
                    )
                })
                .map(|_| c.to_string())
        });
        let mut stmts: Vec<String> = Vec::new();
        if want > 1 {
            if let Some(col) = &int_cursor {
                let qcol = my_ident(col);
                let (lo, hi): (Option<i64>, Option<i64>) = sqlx::query_as(&format!(
                    "SELECT MIN({qcol}), MAX({qcol}) FROM {src_t} WHERE true{dpred}"
                ))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::InvalidInput(format!("min/max of cursor {col}: {e}")))?;
                if let (Some(lo), Some(hi)) = (lo, hi) {
                    for (rlo, rhi) in spans(lo, hi, want) {
                        stmts.push(format!(
                            "SELECT {select_list} FROM {src_t} \
                             WHERE {qcol} >= {rlo} AND {qcol} <= {rhi}{dpred}"
                        ));
                    }
                } else if delta.is_some() {
                    stmts.push(format!("SELECT {select_list} FROM {src_t} WHERE false"));
                }
            }
        }
        if stmts.is_empty() {
            stmts.push(format!(
                "SELECT {select_list} FROM {src_t} WHERE true{dpred}"
            ));
        }
        Ok(stmts)
    }

    async fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> Result<u64> {
        let enc = match lane.format {
            WireFormat::PgCopyBinary => MyEnc::PgCopy(
                plan.cols
                    .iter()
                    .map(|c| my_pg(c).expect("validated at probe").0)
                    .collect(),
            ),
            WireFormat::RowBinary => MyEnc::RowBinary(
                plan.cols
                    .iter()
                    .map(|c| (my_rb(c).expect("validated at probe").0, c.nullable))
                    .collect(),
            ),
            WireFormat::MyTsv => MyEnc::Tsv(plan.cols.iter().map(my_tsv).collect()),
            WireFormat::TabSeparated => unreachable!("can_produce refuses the PG text dialect for the MySQL source"),
        };
        let queue = super::work_queue(stmts);
        let mut tasks = Vec::with_capacity(loaders.len());
        for loader in loaders {
            tasks.push(tokio::spawn(row_worker(
                self.pool.clone(),
                queue.clone(),
                enc.clone(),
                loader,
                chunk,
            )));
        }
        let mut rows = 0u64;
        for t in tasks {
            rows += t
                .await
                .map_err(|e| Error::Transfer(format!("join: {e}")))??;
        }
        Ok(rows)
    }
}

#[derive(Clone)]
enum MyEnc {
    RowBinary(Vec<(MyRb, bool)>),
    PgCopy(Vec<PgEnc>),
    /// MySQL text lane (for the MySQL sink's LOAD DATA): decode the binary wire
    /// value per column and render the exact LOAD DATA text client-side.
    Tsv(Vec<MyTsv>),
}

/// One worker: pulls SELECT statements, decodes rows off the wire, encodes the lane's
/// format, and streams into ONE sink loader, coalescing to ~`chunk`-byte sends.
async fn row_worker<L: Loader>(
    pool: MySqlPool,
    queue: WorkQueue,
    enc: MyEnc,
    mut loader: L,
    chunk: usize,
) -> Result<u64> {
    use futures::TryStreamExt;
    let dbg = std::env::var("APITAP_DEBUG").is_ok();
    let (mut t_fetch, mut t_enc, mut t_send) = (
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    );
    let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
    if let MyEnc::PgCopy(_) = &enc {
        pgc::header(&mut out);
    }
    while let Some(sql) = pop(&queue) {
        let mut rows = sqlx::query(&sql).fetch(&pool);
        loop {
            let tf = dbg.then(std::time::Instant::now);
            let row = match rows.try_next().await {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => {
                    return Err(loader
                        .abort(Error::Transfer(format!("mysql read: {e}")))
                        .await)
                }
            };
            if let Some(tf) = tf {
                t_fetch += tf.elapsed();
            }
            let te = dbg.then(std::time::Instant::now);
            let step = match &enc {
                MyEnc::RowBinary(plan) => {
                    let mut r = Ok(());
                    for (i, (ty, nullable)) in plan.iter().enumerate() {
                        r = encode_value(&row, i, *ty, *nullable, &mut out);
                        if r.is_err() {
                            break;
                        }
                    }
                    r
                }
                MyEnc::Tsv(encs) => {
                    let mut r = Ok(());
                    for (i, enc) in encs.iter().enumerate() {
                        if i > 0 {
                            out.push(b'\t');
                        }
                        match raw_cell(&row, i) {
                            Ok(None) => out.extend_from_slice(b"\\N"),
                            Ok(Some(b)) => {
                                if let Err(e) = tsv_write(*enc, b, &mut out) {
                                    r = Err(e);
                                    break;
                                }
                            }
                            Err(e) => {
                                r = Err(e);
                                break;
                            }
                        }
                    }
                    if r.is_ok() {
                        out.push(b'\n');
                    }
                    r
                }
                MyEnc::PgCopy(plan) => {
                    pgc::tuple_start(plan.len(), &mut out);
                    let mut r = Ok(());
                    for (i, e) in plan.iter().enumerate() {
                        r = encode_pg(&row, i, *e, &mut out);
                        if r.is_err() {
                            break;
                        }
                    }
                    r
                }
            };
            if let Some(te) = te {
                t_enc += te.elapsed();
            }
            if let Err(e) = step {
                return Err(loader.abort(e).await);
            }
            // mem::replace (not take): take leaves capacity 0 and the next chunk pays
            // ~1 extra full copy in geometric regrowth.
            if out.len() >= chunk {
                let full = std::mem::replace(&mut out, Vec::with_capacity(chunk + 64 * 1024));
                let ts = std::time::Instant::now();
                loader.send(full).await?;
                if dbg {
                    t_send += ts.elapsed();
                }
            }
        }
    }
    if let MyEnc::PgCopy(_) = &enc {
        pgc::trailer(&mut out);
    }
    if !out.is_empty() {
        loader.send(out).await?;
    }
    if dbg {
        eprintln!(
            "[my worker] fetch(wire+parse)={:.1}s encode(cpu)={:.1}s send(backpressure)={:.1}s",
            t_fetch.as_secs_f64(),
            t_enc.as_secs_f64(),
            t_send.as_secs_f64()
        );
    }
    loader.finish().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tw(enc: MyTsv, b: &[u8]) -> String {
        let mut o = Vec::new();
        tsv_write(enc, b, &mut o).unwrap();
        String::from_utf8(o).unwrap()
    }

    #[test]
    fn tsv_formatters_render_load_data_text() {
        // ints (LE binary → decimal text)
        assert_eq!(
            tw(
                MyTsv::Int {
                    bytes: 4,
                    unsigned: false
                },
                &(-42i32).to_le_bytes()
            ),
            "-42"
        );
        assert_eq!(
            tw(
                MyTsv::Int {
                    bytes: 1,
                    unsigned: true
                },
                &[255]
            ),
            "255"
        );
        assert_eq!(
            tw(
                MyTsv::Int {
                    bytes: 8,
                    unsigned: true
                },
                &u64::MAX.to_le_bytes()
            ),
            "18446744073709551615"
        );
        // float round-trips via ryu
        assert_eq!(tw(MyTsv::F64, &1.5f64.to_le_bytes()), "1.5");
        // hex for binary
        assert_eq!(tw(MyTsv::Hex, &[0x48, 0x69]), "4869");
        // DATE: [len=4][year LE][mon][day]
        assert_eq!(tw(MyTsv::DateBin, &[4, 0xe8, 0x07, 2, 8]), "2024-02-08"); // 2024=0x07e8
                                                                              // DATETIME no fraction (len=7)
        assert_eq!(
            tw(MyTsv::DateTimeBin, &[7, 0xe8, 0x07, 2, 8, 13, 46, 40]),
            "2024-02-08 13:46:40"
        );
        // DATETIME with micros (len=11)
        assert_eq!(
            tw(
                MyTsv::DateTimeBin,
                &[11, 0xe8, 0x07, 2, 8, 13, 46, 40, 0xa0, 0x86, 1, 0]
            ),
            "2024-02-08 13:46:40.100000"
        ); // 100000 micros = 0x000186a0 LE
           // date-only datetime (len=4) → time zeros
        assert_eq!(
            tw(MyTsv::DateTimeBin, &[4, 0xe8, 0x07, 2, 8]),
            "2024-02-08 00:00:00"
        );
        // StrEsc still escapes
        assert_eq!(tw(MyTsv::StrEsc, b"a\tb"), "a\\tb");
    }

    #[test]
    fn binary_udts_go_via_hex() {
        assert!(is_binary_udt("blob"));
        assert!(is_binary_udt("varbinary"));
        assert!(is_binary_udt("bit"));
        assert!(!is_binary_udt("varchar"));
        assert!(!is_binary_udt("json"));
        assert!(!is_binary_udt("datetime"));
    }

    fn col(udt: &str, ct: &str, p: Option<i32>, s: Option<i32>) -> ColumnPlan {
        ColumnPlan {
            name: "c".into(),
            nullable: true,
            int_pk: false,
            native_ddl: Some(ct.to_string()),
            udt: udt.into(),
            precision: p,
            scale: s,
        }
    }

    #[test]
    fn pg_lane_maps_lossless() {
        assert_eq!(
            my_pg(&col("tinyint", "tinyint(1)", None, None)).unwrap().0,
            PgEnc::SmallFromI8
        );
        assert_eq!(
            my_pg(&col("bigint", "bigint unsigned", None, None)).unwrap(),
            (PgEnc::NumFromU64, Delivered::Decimal { p: 20, s: 0 })
        );
        assert_eq!(
            my_pg(&col("smallint", "smallint unsigned", None, None))
                .unwrap()
                .1,
            Delivered::Int {
                bytes: 4,
                unsigned: false
            }
        );
        assert_eq!(
            my_pg(&col("decimal", "decimal(18,4)", Some(18), Some(4))).unwrap(),
            (PgEnc::NumStr, Delivered::Decimal { p: 18, s: 4 })
        );
        assert_eq!(
            my_pg(&col("json", "json", None, None)).unwrap().1,
            Delivered::Json
        );
        assert_eq!(
            my_pg(&col("timestamp", "timestamp", None, None)).unwrap().1,
            Delivered::DateTime { utc: true }
        );
        assert_eq!(
            my_pg(&col("blob", "blob", None, None)).unwrap().1,
            Delivered::Bytes
        );
    }

    #[test]
    fn rowbinary_lane_covers_the_bench_schema_and_unsigned() {
        assert_eq!(my_rb(&col("int", "int", None, None)).unwrap().0, MyRb::I32);
        assert_eq!(
            my_rb(&col("int", "int unsigned", None, None)).unwrap().0,
            MyRb::U32
        );
        assert_eq!(
            my_rb(&col("bigint", "bigint unsigned", None, None))
                .unwrap()
                .1,
            Delivered::Int {
                bytes: 8,
                unsigned: true
            }
        );
        let (rb, d) = my_rb(&col("decimal", "decimal(18,4)", Some(18), Some(4))).unwrap();
        assert_eq!(rb, MyRb::Dec { width: 8, scale: 4 });
        assert_eq!(d, Delivered::Decimal { p: 18, s: 4 });
        // p>38 rides as exact text.
        assert_eq!(
            my_rb(&col("decimal", "decimal(65,10)", Some(65), Some(10)))
                .unwrap()
                .0,
            MyRb::Str
        );
        let (rb, d) = my_rb(&col("timestamp", "timestamp", None, None)).unwrap();
        assert_eq!((rb, d), (MyRb::TsUtc, Delivered::DateTime { utc: true }));
        assert_eq!(
            my_rb(&col("datetime", "datetime", None, None)).unwrap().1,
            Delivered::DateTime { utc: false }
        );
        assert_eq!(
            my_rb(&col("json", "json", None, None)).unwrap().0,
            MyRb::Str
        );
        assert_eq!(
            my_rb(&col("blob", "blob", None, None)).unwrap().0,
            MyRb::Bytes
        );
        assert!(my_rb(&col("geometry", "geometry", None, None)).is_err());
    }

    #[test]
    fn civil_days_and_binary_datetime_layouts() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2020, 1, 1), 18_262);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        // DATE payload [4][year lo][year hi][month][day]
        let d = [4u8, 0xE4, 0x07, 1, 1]; // 2020-01-01
        assert_eq!(bin_date_days(&d).unwrap(), 18_262);
        // DATETIME payload [11][year][month][day][h][m][s][micros u32] = 2020-01-01 00:00:01.000005
        let mut dt = vec![11u8, 0xE4, 0x07, 1, 1, 0, 0, 1];
        dt.extend(5u32.to_le_bytes());
        assert_eq!(
            bin_datetime_micros(&dt).unwrap(),
            18_262i64 * 86_400 * 1_000_000 + 1_000_005
        );
        // Seconds-only variant (len 7).
        let dt7 = [7u8, 0xE4, 0x07, 1, 1, 13, 53, 20];
        assert_eq!(
            bin_datetime_micros(&dt7).unwrap(),
            (18_262i64 * 86_400 + 13 * 3600 + 53 * 60 + 20) * 1_000_000
        );
        // Zero date refused.
        assert!(bin_date_days(&[0u8]).is_err());
    }

    #[test]
    fn decimal_text_parses_exactly() {
        let p = |s: &str, sc| dec_bytes_to_scaled_i128(s.as_bytes(), sc);
        assert_eq!(p("1234.5678", 4).unwrap(), 12_345_678);
        assert_eq!(p("-1234.5678", 4).unwrap(), -12_345_678);
        assert_eq!(p("50.0000", 4).unwrap(), 500_000);
        assert_eq!(p("50", 4).unwrap(), 500_000);
        assert_eq!(p("0.5", 4).unwrap(), 5_000);
        assert_eq!(p("50.00", 4).unwrap(), 500_000); // short frac pads
        assert!(p("1.23456", 4).is_err()); // silent truncation refused
        assert!(p("abc", 4).is_err());
    }
}
