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
use crate::wire::arrowcol::{ArrowBatch, ArrowKind, BatchBuilder, CellVal};
use crate::wire::mywire::{self, MyWire};
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

/// How to decode a MySQL binary-protocol cell STRAIGHT into an Arrow column
/// builder — the read path's direct lane ([`MySqlSource::run_arrow_read`]).
/// No intermediate COPY stream: one dispatch and at most one copy per cell.
#[derive(Clone, Copy)]
enum MyAr {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    /// BIGINT UNSIGNED — exact as Decimal128(20,0), mirroring `my_pg`.
    U64Dec,
    F32,
    F64,
    /// NEWDECIMAL ASCII text → scaled i128, ONE conversion (the COPY lane
    /// pays text → pg-numeric groups → i128).
    Dec { scale: u32 },
    Date,
    /// DATETIME and TIMESTAMP decode identically (session is UTC-pinned);
    /// only the delivered tag differs.
    DateTime,
    Year,
    Str,
    Bin,
}

/// (direct-Arrow decoder, delivery) for one MySQL column. Deliveries MIRROR
/// [`my_pg`] exactly (asserted by test) — the read schema must not depend on
/// which lane runs.
fn my_ar(c: &ColumnPlan) -> Result<(MyAr, Delivered)> {
    let unsigned = c.native_ddl.as_deref().unwrap_or("").contains("unsigned");
    let int = |bytes: u8| Delivered::Int {
        bytes,
        unsigned: false,
    };
    Ok(match c.udt.as_str() {
        "tinyint" if unsigned => (MyAr::U8, int(2)),
        "tinyint" => (MyAr::I8, int(2)),
        "smallint" if unsigned => (MyAr::U16, int(4)),
        "smallint" => (MyAr::I16, int(2)),
        "mediumint" | "int" if unsigned => (MyAr::U32, int(8)),
        "mediumint" | "int" => (MyAr::I32, int(4)),
        "bigint" if unsigned => (MyAr::U64Dec, Delivered::Decimal { p: 20, s: 0 }),
        "bigint" => (MyAr::I64, int(8)),
        "float" => (MyAr::F32, Delivered::Float32),
        "double" => (MyAr::F64, Delivered::Float64),
        "decimal" => match (c.precision, c.scale) {
            (Some(p), Some(s)) => (
                MyAr::Dec {
                    scale: s.max(0) as u32,
                },
                Delivered::Decimal {
                    p: p as u16,
                    s: s as u16,
                },
            ),
            // arrow_kind(None) — the read planner rewrites the column to
            // longtext before encoders derive; this arm is a parity mirror.
            _ => (MyAr::Str, Delivered::Decimal { p: 0, s: 0 }),
        },
        "date" => (MyAr::Date, Delivered::Date),
        "datetime" => (MyAr::DateTime, Delivered::DateTime { utc: false }),
        "timestamp" => (MyAr::DateTime, Delivered::DateTime { utc: true }),
        "year" => (MyAr::Year, int(2)),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        | "time" => (MyAr::Str, Delivered::Text),
        // Parity mirror — rewritten to longtext by the read planner.
        "json" => (MyAr::Str, Delivered::Json),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            (MyAr::Bin, Delivered::Bytes)
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "mysql type '{other}' is not supported yet"
            )))
        }
    })
}

/// Fixed-width little-endian read with a loud width check (cold on the
/// error path; the happy path compiles to one unaligned load).
#[inline]
fn le<const N: usize>(b: &[u8]) -> Result<[u8; N]> {
    b.try_into()
        .map_err(|_| Error::Transfer(format!("mysql cell width {} (want {N})", b.len())))
}

/// One non-NULL binary-protocol cell → the column's [`CellVal`].
#[inline]
fn ar_cell<'a>(b: &'a [u8], enc: MyAr) -> Result<CellVal<'a>> {
    Ok(match enc {
        MyAr::I8 => CellVal::I16(i8::from_le_bytes(le::<1>(b)?) as i16),
        MyAr::U8 => CellVal::I16(b[0] as i16),
        MyAr::I16 => CellVal::I16(i16::from_le_bytes(le::<2>(b)?)),
        MyAr::U16 => CellVal::I32(u16::from_le_bytes(le::<2>(b)?) as i32),
        MyAr::I32 => CellVal::I32(i32::from_le_bytes(le::<4>(b)?)),
        MyAr::U32 => CellVal::I64(u32::from_le_bytes(le::<4>(b)?) as i64),
        MyAr::I64 => CellVal::I64(i64::from_le_bytes(le::<8>(b)?)),
        MyAr::U64Dec => CellVal::Dec128(u64::from_le_bytes(le::<8>(b)?) as i128),
        MyAr::F32 => CellVal::F32(f32::from_le_bytes(le::<4>(b)?)),
        MyAr::F64 => CellVal::F64(f64::from_le_bytes(le::<8>(b)?)),
        MyAr::Dec { scale } => CellVal::Dec128(dec_bytes_to_scaled_i128(b, scale)?),
        MyAr::Date => CellVal::DateDays(bin_date_days(b)? as i32),
        MyAr::DateTime => CellVal::TsMicros(bin_datetime_micros(b)?),
        MyAr::Year => CellVal::I16(u16::from_le_bytes(le::<2>(b)?) as i16),
        MyAr::Str => CellVal::Str(b),
        MyAr::Bin => CellVal::Bin(b),
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

/// Encode one column as RowBinary straight from the wire bytes (`cell` comes
/// from sqlx's [`raw_cell`] or the raw plane's [`walk_raw_cells`] — same
/// slices either way). MySQL's binary protocol stores ints/floats
/// little-endian fixed-width — exactly RowBinary's layout, so those are pure
/// copies.
fn encode_value(
    cell: Option<&[u8]>,
    i: usize,
    ty: MyRb,
    nullable: bool,
    out: &mut Vec<u8>,
) -> Result<()> {
    let Some(b) = cell else {
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
fn encode_pg(cell: Option<&[u8]>, i: usize, enc: PgEnc, out: &mut Vec<u8>) -> Result<()> {
    let Some(b) = cell else {
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
    /// Kept for the raw read plane's own worker connections.
    url: std::sync::Arc<str>,
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
        Ok(Self { pool, url: url.into() })
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
        Lane { format, cols, raw_frames: false, push_where: None }
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
        let dprobe = delta
            .map(|d| format!(" AND {} {} {}", my_ident(&d.col), d.op, d.literal))
            .unwrap_or_default();
        // Pushed-down query predicate (lazy filter) — span statements only:
        // in the min/max probe it kills the index shortcut and costs a
        // single-threaded full scan up front (measured +10s on 10M rows).
        let mut dpred = dprobe.clone();
        if let Some(w) = &lane.push_where {
            dpred.push_str(&format!(" AND ({w})"));
        }
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
                    "SELECT MIN({qcol}), MAX({qcol}) FROM {src_t} WHERE true{dprobe}"
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
                } else if delta.is_some() || lane.push_where.is_some() {
                    // min/max proved zero surviving rows — read nothing
                    // instead of a full scan that returns nothing.
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
        // Raw-plane canary, same dance as the Arrow lane: one probe connect
        // decides, refused dialects (TLS-verify, exotic auth) ride sqlx, and
        // APITAP_MY_RAW=0 forces the sqlx lane for A/B.
        let wire: Option<std::sync::Arc<Vec<MyAr>>> =
            if std::env::var("APITAP_MY_RAW").is_ok_and(|v| v == "0") {
                None
            } else {
                match plan
                    .cols
                    .iter()
                    .map(|c| my_ar(c).map(|(e, _)| e))
                    .collect::<Result<Vec<_>>>()
                {
                    Err(_) => None,
                    Ok(w) => match MyWire::connect(&self.url).await {
                        Ok(_) => Some(std::sync::Arc::new(w)),
                        Err(e) => {
                            if std::env::var_os("APITAP_DEBUG").is_some() {
                                eprintln!("[apitap] mysql raw transfer plane declined: {e}");
                            }
                            None
                        }
                    },
                }
            };
        let queue = super::work_queue(stmts);
        let mut tasks = Vec::with_capacity(loaders.len());
        for loader in loaders {
            tasks.push(match &wire {
                Some(w) => tokio::spawn(raw_transfer_worker(
                    self.url.clone(),
                    queue.clone(),
                    enc.clone(),
                    w.clone(),
                    loader,
                    chunk,
                )),
                None => tokio::spawn(row_worker(
                    self.pool.clone(),
                    queue.clone(),
                    enc.clone(),
                    loader,
                    chunk,
                )),
            });
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
                        r = raw_cell(&row, i)
                            .and_then(|c| encode_value(c, i, *ty, *nullable, &mut out));
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
                        r = raw_cell(&row, i).and_then(|c| encode_pg(c, i, *e, &mut out));
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
            // ~1 extra full copy in geometric regrowth. Recycled buffers from the
            // sink's back-channel replace fresh allocations — the same churn fix
            // the pg COPY worker got (benchmarks/profiling.md): a fresh multi-MB
            // Vec per chunk was kernel page-zeroing on every allocation.
            if out.len() >= chunk {
                let full = std::mem::replace(
                    &mut out,
                    loader
                        .reclaim()
                        .unwrap_or_else(|| Vec::with_capacity(chunk + 64 * 1024)),
                );
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

/// [`row_worker`]'s raw-plane twin: own TCP connection, prepared span
/// SELECTs, row payloads walked in place off the socket buffer straight into
/// the lane encoders — sqlx's per-row machinery (BinaryRow allocation,
/// async-stream yields, tracing spans) is gone. Wire slices are identical to
/// what [`raw_cell`] hands over, so the encoders don't know which plane fed
/// them.
async fn raw_transfer_worker<L: Loader>(
    url: std::sync::Arc<str>,
    queue: WorkQueue,
    enc: MyEnc,
    wire: std::sync::Arc<Vec<MyAr>>,
    mut loader: L,
    chunk: usize,
) -> Result<u64> {
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
    let mut w = match MyWire::connect(&url).await {
        Ok(w) => w,
        Err(e) => return Err(loader.abort(e).await),
    };
    while let Some(sql) = pop(&queue) {
        let (sid, ncols) = match w.prepare(&sql).await {
            Ok(v) => v,
            Err(e) => return Err(loader.abort(e).await),
        };
        if ncols != wire.len() {
            return Err(loader
                .abort(Error::Transfer(format!(
                    "mysql wire: span returned {ncols} columns, planned {}",
                    wire.len()
                )))
                .await);
        }
        if let Err(e) = w.execute(sid).await {
            return Err(loader.abort(e).await);
        }
        loop {
            let tf = dbg.then(std::time::Instant::now);
            let row = match w.next_row().await {
                Ok(r) => r,
                Err(e) => return Err(loader.abort(e).await),
            };
            if let Some(tf) = tf {
                t_fetch += tf.elapsed();
            }
            let Some(p) = row else { break };
            let te = dbg.then(std::time::Instant::now);
            let step = match &enc {
                MyEnc::RowBinary(plan) => walk_raw_cells(p, &wire, |i, c| {
                    encode_value(c, i, plan[i].0, plan[i].1, &mut out)
                }),
                MyEnc::Tsv(encs) => {
                    let r = walk_raw_cells(p, &wire, |i, c| {
                        if i > 0 {
                            out.push(b'\t');
                        }
                        match c {
                            None => {
                                out.extend_from_slice(b"\\N");
                                Ok(())
                            }
                            Some(b) => tsv_write(encs[i], b, &mut out),
                        }
                    });
                    if r.is_ok() {
                        out.push(b'\n');
                    }
                    r
                }
                MyEnc::PgCopy(plan) => {
                    pgc::tuple_start(plan.len(), &mut out);
                    walk_raw_cells(p, &wire, |i, c| encode_pg(c, i, plan[i], &mut out))
                }
            };
            if let Some(te) = te {
                t_enc += te.elapsed();
            }
            if let Err(e) = step {
                return Err(loader.abort(e).await);
            }
            if out.len() >= chunk {
                let full = std::mem::replace(
                    &mut out,
                    loader
                        .reclaim()
                        .unwrap_or_else(|| Vec::with_capacity(chunk + 64 * 1024)),
                );
                let ts = std::time::Instant::now();
                loader.send(full).await?;
                if dbg {
                    t_send += ts.elapsed();
                }
            }
        }
        if let Err(e) = w.stmt_close(sid).await {
            return Err(loader.abort(e).await);
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
            "[my raw transfer] fetch(wire)={:.1}s encode(cpu)={:.1}s send(backpressure)={:.1}s",
            t_fetch.as_secs_f64(),
            t_enc.as_secs_f64(),
            t_send.as_secs_f64()
        );
    }
    loader.finish().await
}

impl MySqlSource {
    /// The read path's DIRECT lane: binary-protocol cells decode straight
    /// into per-worker [`BatchBuilder`]s — no intermediate COPY stream, no
    /// bounds walk, varlen values copied exactly once. Sealed batches go
    /// into `tx` (bounded — backpressure is the channel). Returns total
    /// rows. `APITAP_MY_ARROW=0` keeps the COPY lane for A/B.
    pub(crate) async fn run_arrow_read(
        &self,
        plan: &TablePlan,
        stmts: Vec<String>,
        kinds: Vec<ArrowKind>,
        batch_bytes: usize,
        tx: tokio::sync::mpsc::Sender<Result<ArrowBatch>>,
        workers: usize,
    ) -> Result<u64> {
        let encs: std::sync::Arc<Vec<MyAr>> = std::sync::Arc::new(
            plan.cols
                .iter()
                .map(|c| my_ar(c).map(|(e, _)| e))
                .collect::<Result<_>>()?,
        );
        // Raw plane canary: one throwaway handshake decides the lane for
        // the whole read. Anything it can't do (TLS demanded, cold sha2
        // auth cache, exotic plugin) rides sqlx — never a hard failure.
        // APITAP_MY_RAW=0 forces the sqlx lane for A/B.
        let mut raw = !std::env::var("APITAP_MY_RAW").is_ok_and(|v| v == "0");
        if raw {
            match MyWire::connect(&self.url).await {
                Ok(w) => w.quit().await,
                Err(e) => {
                    if std::env::var_os("APITAP_DEBUG").is_some() {
                        eprintln!("[my raw] canary failed, sqlx lane: {e}");
                    }
                    raw = false;
                }
            }
        }
        let queue = super::work_queue(stmts);
        let mut tasks = Vec::with_capacity(workers);
        for _ in 0..workers {
            let bb = BatchBuilder::new(kinds.clone(), batch_bytes);
            tasks.push(if raw {
                tokio::spawn(raw_arrow_worker(
                    self.url.clone(),
                    queue.clone(),
                    encs.clone(),
                    bb,
                    tx.clone(),
                ))
            } else {
                tokio::spawn(arrow_row_worker(
                    self.pool.clone(),
                    queue.clone(),
                    encs.clone(),
                    bb,
                    tx.clone(),
                ))
            });
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

/// Walk one raw binary-protocol row payload, handing each column's wire
/// bytes (`None` for NULL) to `cell` in column order. Layout: 0x00 header,
/// NULL bitmap with a 2-bit offset, then values — fixed little-endian for
/// ints/floats, `[len][fields]` for temporals, length-encoded bytes for the
/// rest. The slices match what sqlx's [`raw_cell`] hands the encoders
/// (temporals keep their length byte, varlen values lose their prefix), so
/// the Arrow lane and the transfer lanes share this one frame.
fn walk_raw_cells<F>(p: &[u8], encs: &[MyAr], mut cell: F) -> Result<()>
where
    F: FnMut(usize, Option<&[u8]>) -> Result<()>,
{
    #[inline]
    fn take<'a>(p: &'a [u8], pos: usize, n: usize) -> Result<&'a [u8]> {
        p.get(pos..pos + n)
            .ok_or_else(|| Error::Transfer("mysql wire: truncated row".into()))
    }
    let n = encs.len();
    let bm_len = (n + 7 + 2) / 8;
    let bm = take(p, 1, bm_len)?;
    let mut pos = 1 + bm_len;
    for (i, enc) in encs.iter().enumerate() {
        let bit = i + 2;
        if bm[bit / 8] & (1 << (bit % 8)) != 0 {
            cell(i, None)?;
            continue;
        }
        let b = match enc {
            MyAr::I8 | MyAr::U8 => take(p, pos, 1)?,
            MyAr::I16 | MyAr::U16 | MyAr::Year => take(p, pos, 2)?,
            MyAr::I32 | MyAr::U32 | MyAr::F32 => take(p, pos, 4)?,
            MyAr::I64 | MyAr::U64Dec | MyAr::F64 => take(p, pos, 8)?,
            MyAr::Date | MyAr::DateTime => {
                let l = *p
                    .get(pos)
                    .ok_or_else(|| Error::Transfer("mysql wire: truncated row".into()))?
                    as usize;
                take(p, pos, 1 + l)?
            }
            MyAr::Dec { .. } | MyAr::Str | MyAr::Bin => {
                let (l, h) = mywire::lenenc(&p[pos..])?;
                let b = take(p, pos + h, l as usize)?;
                pos += h; // the prefix; the value advances below
                b
            }
        };
        cell(i, Some(b))?;
        pos += b.len();
    }
    Ok(())
}

/// Walk one raw row payload straight into the Arrow builder — the read
/// lane's binding of [`walk_raw_cells`].
fn walk_raw_row(p: &[u8], encs: &[MyAr], bb: &mut BatchBuilder) -> Result<()> {
    walk_raw_cells(p, encs, |i, c| match c {
        None => {
            bb.append_cell(i, CellVal::Null);
            Ok(())
        }
        Some(b) => {
            bb.append_cell(i, ar_cell(b, encs[i])?);
            Ok(())
        }
    })
}

/// One raw-plane worker: own TCP connection, prepared span SELECTs, rows
/// walked in place from the socket buffer into the column builders.
async fn raw_arrow_worker(
    url: std::sync::Arc<str>,
    queue: WorkQueue,
    encs: std::sync::Arc<Vec<MyAr>>,
    mut bb: BatchBuilder,
    tx: tokio::sync::mpsc::Sender<Result<ArrowBatch>>,
) -> Result<u64> {
    let dbg = std::env::var_os("APITAP_DEBUG").is_some();
    let (mut t_fetch, mut t_dec, mut t_send) = (
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    );
    let cancelled = || Error::Transfer("read cancelled by consumer".into());
    let mut w = MyWire::connect(&url).await?;
    let mut since_seal = 0u32;
    while let Some(sql) = pop(&queue) {
        let (sid, ncols) = w.prepare(&sql).await?;
        if ncols != encs.len() {
            return Err(Error::Transfer(format!(
                "mysql wire: span returned {ncols} columns, planned {}",
                encs.len()
            )));
        }
        w.execute(sid).await?;
        loop {
            let tf = dbg.then(std::time::Instant::now);
            let row = w.next_row().await?;
            if let Some(tf) = tf {
                t_fetch += tf.elapsed();
            }
            let Some(p) = row else { break };
            let td = dbg.then(std::time::Instant::now);
            walk_raw_row(p, &encs, &mut bb)?;
            bb.row_done();
            if let Some(td) = td {
                t_dec += td.elapsed();
            }
            since_seal += 1;
            if since_seal >= 64 {
                since_seal = 0;
                if let Some(batch) = bb.take_ready()? {
                    let ts = dbg.then(std::time::Instant::now);
                    tx.send(Ok(batch)).await.map_err(|_| cancelled())?;
                    if let Some(ts) = ts {
                        t_send += ts.elapsed();
                    }
                }
            }
        }
        w.stmt_close(sid).await?;
    }
    if let Some(batch) = bb.finish()? {
        tx.send(Ok(batch)).await.map_err(|_| cancelled())?;
    }
    if dbg {
        eprintln!(
            "[my raw worker] fetch(wire)={:.1}s decode(cpu)={:.1}s send(backpressure)={:.1}s",
            t_fetch.as_secs_f64(),
            t_dec.as_secs_f64(),
            t_send.as_secs_f64()
        );
    }
    Ok(bb.rows_total())
}

/// One direct-lane worker: pull span statements until the queue drains,
/// stream rows, append cells column-by-column, seal by size. The seal
/// check runs every 64 rows — `acc_bytes` sums every column and is too
/// hot for every row.
async fn arrow_row_worker(
    pool: MySqlPool,
    queue: WorkQueue,
    encs: std::sync::Arc<Vec<MyAr>>,
    mut bb: BatchBuilder,
    tx: tokio::sync::mpsc::Sender<Result<ArrowBatch>>,
) -> Result<u64> {
    use futures::TryStreamExt;
    let dbg = std::env::var_os("APITAP_DEBUG").is_some();
    let (mut t_fetch, mut t_dec, mut t_send) = (
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    );
    let cancelled = || Error::Transfer("read cancelled by consumer".into());
    let mut since_seal = 0u32;
    while let Some(sql) = pop(&queue) {
        let mut rows = sqlx::query(&sql).fetch(&pool);
        loop {
            let tf = dbg.then(std::time::Instant::now);
            let row = rows
                .try_next()
                .await
                .map_err(|e| Error::Transfer(format!("mysql read: {e}")))?;
            if let Some(tf) = tf {
                t_fetch += tf.elapsed();
            }
            let Some(row) = row else { break };
            let td = dbg.then(std::time::Instant::now);
            for (i, enc) in encs.iter().enumerate() {
                match raw_cell(&row, i)? {
                    None => bb.append_cell(i, CellVal::Null),
                    Some(b) => bb.append_cell(i, ar_cell(b, *enc)?),
                }
            }
            bb.row_done();
            if let Some(td) = td {
                t_dec += td.elapsed();
            }
            since_seal += 1;
            if since_seal >= 64 {
                since_seal = 0;
                if let Some(batch) = bb.take_ready()? {
                    let ts = dbg.then(std::time::Instant::now);
                    tx.send(Ok(batch)).await.map_err(|_| cancelled())?;
                    if let Some(ts) = ts {
                        t_send += ts.elapsed();
                    }
                }
            }
        }
    }
    if let Some(batch) = bb.finish()? {
        tx.send(Ok(batch)).await.map_err(|_| cancelled())?;
    }
    if dbg {
        eprintln!(
            "[my arrow worker] fetch(wire+parse)={:.1}s decode(cpu)={:.1}s send(backpressure)={:.1}s",
            t_fetch.as_secs_f64(),
            t_dec.as_secs_f64(),
            t_send.as_secs_f64()
        );
    }
    Ok(bb.rows_total())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The raw-plane row walker against a hand-built binary-protocol row:
    /// nulls via the offset-2 bitmap, fixed widths, temporal `[len][...]`
    /// payloads, and a lenenc string long enough to need a 2-byte prefix.
    #[test]
    fn raw_row_walker_decodes_a_synthetic_packet() {
        use crate::wire::arrowcol::FinishedCol;
        let encs = [
            MyAr::I32,
            MyAr::Str,
            MyAr::Dec { scale: 2 },
            MyAr::Date,
            MyAr::I64,
        ];
        let kinds = vec![
            ArrowKind::Int32,
            ArrowKind::Utf8,
            ArrowKind::Decimal { p: 10, s: 2 },
            ArrowKind::Date32,
            ArrowKind::Int64,
        ];
        // Bitmap: (5 cols + 2 offset + 7) / 8 = 1 byte.
        // Row 1: 42, "hi", "12.34", 2024-02-08, NULL(i64 → bit 6).
        let mut p = vec![0x00, 0b0100_0000];
        p.extend_from_slice(&42i32.to_le_bytes());
        p.extend_from_slice(&[2, b'h', b'i']);
        p.extend_from_slice(&[5, b'1', b'2', b'.', b'3', b'4']);
        p.extend_from_slice(&[4, 0xe8, 0x07, 2, 8]); // 2024-02-08
        let mut bb = BatchBuilder::new(kinds.clone(), usize::MAX >> 1);
        walk_raw_row(&p, &encs, &mut bb).unwrap();
        bb.row_done();
        // Row 2: NULL i32 (bit 2), 300-byte string (lenenc 0xFC), rest set.
        let mut p = vec![0x00, 0b0000_0100];
        p.extend_from_slice(&[0xFC, 0x2C, 0x01]); // 300
        p.extend_from_slice(&[b'x'; 300]);
        p.extend_from_slice(&[1, b'7']);
        p.extend_from_slice(&[0]); // zero-length date payload is the ZERO date
        p.extend_from_slice(&(-5i64).to_le_bytes());
        // zero date must error loudly (same policy as the COPY lane)
        let err = walk_raw_row(&p, &encs, &mut bb).unwrap_err();
        assert!(format!("{err}").contains("DATE"), "{err}");
        // Row 2 fixed: real date.
        let mut p = vec![0x00, 0b0000_0100];
        p.extend_from_slice(&[0xFC, 0x2C, 0x01]);
        p.extend_from_slice(&[b'x'; 300]);
        p.extend_from_slice(&[1, b'7']);
        p.extend_from_slice(&[4, 0xe8, 0x07, 2, 9]);
        p.extend_from_slice(&(-5i64).to_le_bytes());
        let mut bb = BatchBuilder::new(kinds, usize::MAX >> 1);
        walk_raw_row(&p, &encs, &mut bb).unwrap();
        bb.row_done();
        let batch = bb.finish().unwrap().expect("rows");
        assert_eq!(batch.rows, 1);
        match &batch.cols[0] {
            FinishedCol::I32 { validity, data } => {
                assert!(validity.is_some());
                assert_eq!(data, &vec![0]);
            }
            _ => unreachable!(),
        }
        match &batch.cols[1] {
            FinishedCol::Utf8 { offsets, data, .. } => {
                assert_eq!(offsets, &vec![0, 300]);
                assert!(data.iter().all(|&b| b == b'x'));
            }
            _ => unreachable!(),
        }
        match &batch.cols[2] {
            FinishedCol::Dec128 { data, .. } => assert_eq!(data, &vec![700]),
            _ => unreachable!(),
        }
        match &batch.cols[4] {
            FinishedCol::I64 { data, .. } => assert_eq!(data, &vec![-5]),
            _ => unreachable!(),
        }
    }

    /// The direct-Arrow lane must deliver EXACTLY what the COPY lane
    /// delivers — the read schema (and polars dtypes) cannot depend on
    /// which lane runs. One matrix over every udt the mappers know.
    #[test]
    fn my_ar_delivery_mirrors_my_pg() {
        let col = |udt: &str, ddl: Option<&str>, p: Option<i32>, s: Option<i32>| ColumnPlan {
            name: "c".into(),
            nullable: true,
            int_pk: false,
            native_ddl: ddl.map(|d| d.to_string()),
            udt: udt.into(),
            precision: p,
            scale: s,
        };
        let mut cases: Vec<ColumnPlan> = Vec::new();
        for udt in [
            "tinyint", "smallint", "mediumint", "int", "bigint", "float", "double", "date",
            "datetime", "timestamp", "year", "char", "varchar", "tinytext", "text",
            "mediumtext", "longtext", "enum", "set", "time", "json", "binary", "varbinary",
            "tinyblob", "blob", "mediumblob", "longblob", "bit",
        ] {
            cases.push(col(udt, None, None, None));
            cases.push(col(udt, Some(&format!("{udt} unsigned")), None, None));
        }
        cases.push(col("decimal", None, Some(18), Some(4)));
        cases.push(col("decimal", None, Some(65), Some(10)));
        cases.push(col("decimal", None, None, None));
        for c in &cases {
            let pg = my_pg(c).expect("my_pg").1;
            let ar = my_ar(c).expect("my_ar").1;
            assert_eq!(pg, ar, "udt={} ddl={:?}", c.udt, c.native_ddl);
        }
    }

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
