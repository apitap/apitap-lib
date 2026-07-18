//! Postgres binary-COPY → MySQL `LOAD DATA` TSV transcoder.
//!
//! The mirror of [`crate::wire::rowbinary`] for the MySQL sink: the Postgres
//! source streams `COPY … (FORMAT binary)` and this walker renders each field
//! in the `FIELDS ESCAPED BY '\\'` text dialect ([`crate::wire::mytsv`]) —
//! Postgres skips text formatting, and the values arrive in the exact spellings
//! MySQL's loader parses (`1`/`0` booleans, offset-free UTC datetimes, HEX for
//! `bytea`). Binary input also sidesteps the escape-dialect mismatch that made
//! relaying Postgres TEXT COPY wrong (`\v`, `\b`, `\f` escapes MySQL doesn't
//! unescape).
//!
//! Values MySQL cannot represent fail LOUDLY here (NaN/Infinity floats and
//! numerics, dates outside 1000-01-01..9999-12-31): the loader session runs
//! `sql_mode=''`, where an unparseable field silently coerces to zero — an
//! error in-flight is the only honest option.

use crate::error::{Error, Result};
use crate::wire::mytsv::tsv_escape;
use crate::wire::pgcopy::{PG_EPOCH_DAYS, PG_EPOCH_MICROS};

/// How to render one column. Order mirrors the SELECT / DDL order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum MyType {
    /// int2/int4/int8: sign-extended big-endian → decimal digits.
    Int,
    Float32,
    Float64,
    /// bool byte → `1` / `0` (the DDL is TINYINT(1)).
    Bool,
    /// date: int32 PG-epoch days → `YYYY-MM-DD`.
    Date,
    /// timestamp/timestamptz: int64 PG-epoch micros → `YYYY-MM-DD HH:MM:SS.ffffff`
    /// (wall-as-UTC; the loader session runs UTC, the DDL is DATETIME(6)).
    Ts,
    /// NUMERIC with a declared precision → exact decimal digit string (base-10000
    /// groups rendered directly — no i128 ceiling, so DECIMAL(65,s) stays exact).
    Numeric,
    /// Unconstrained NUMERIC — the DDL is DOUBLE (Delivered::Float64), so the
    /// exact digit string is range-checked against IEEE double: overflow or
    /// underflow-to-zero errors loudly instead of letting sql_mode='' clamp it.
    NumericDouble,
    /// text-ish: escaped bytes.
    Text,
    /// jsonb: escaped bytes minus the 1-byte version header.
    JsonB,
    /// uuid: 16 bytes → hyphenated lowercase hex (CHAR(36)).
    Uuid,
    /// bytea: uppercase HEX — the sink's LOAD DATA wraps the column in UNHEX().
    Bytea,
}

/// Which Postgres udt_names this lane covers. Anything else → no common wire
/// format for postgres → mysql, and the negotiation error tells the user to
/// cast the column in a source view.
pub(crate) fn my_tsv_type(
    udt: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<MyType> {
    Some(match udt {
        "int2" | "int4" | "int8" => MyType::Int,
        "float4" => MyType::Float32,
        "float8" => MyType::Float64,
        "bool" => MyType::Bool,
        "date" => MyType::Date,
        "timestamp" | "timestamptz" => MyType::Ts,
        // MySQL DECIMAL lives in p ≤ 65, 0 ≤ s ≤ 30, s ≤ p; a declared NUMERIC
        // outside that can't be mirrored exactly, so it's excluded (negotiation
        // error with the cast hint) rather than dying at CREATE TABLE — or
        // worse, silently rounded. No declared precision → the DDL is DOUBLE
        // and the renderer range-checks each value.
        "numeric" => match precision {
            None => MyType::NumericDouble,
            Some(p) => {
                let s = scale.unwrap_or(0);
                if p > 65 || s > 30 || s > p || s < 0 {
                    return None;
                }
                MyType::Numeric
            }
        },
        "varchar" | "bpchar" | "text" | "name" | "json" => MyType::Text,
        "jsonb" => MyType::JsonB,
        "uuid" => MyType::Uuid,
        "bytea" => MyType::Bytea,
        _ => return None,
    })
}

/// Streaming transcoder. Same framing contract as
/// [`crate::wire::rowbinary::Transcoder`]: feed Postgres binary-COPY bytes in
/// arbitrary chunks; complete tuples render immediately, a partial tail is
/// buffered across chunk boundaries.
pub(crate) struct PgToMyTsv {
    cols: Vec<(MyType, String)>, // (type, column name — names only feed error text)
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    finished: bool,
}

impl PgToMyTsv {
    pub(crate) fn new(cols: Vec<(MyType, String)>) -> Self {
        Self {
            cols,
            buf: Vec::with_capacity(1 << 20),
            pos: 0,
            header_done: false,
            finished: false,
        }
    }

    /// Feed input; append rendered TSV rows to `out`.
    pub(crate) fn push(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }

        // Fast path: nothing pending — render complete tuples straight from
        // `input`, buffer only a partial tail (one CopyData ≈ one row).
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

/// Render ONE complete tuple from the start of `b` as a TSV line; returns
/// `(bytes consumed, reached_trailer)`, or None if the tuple is incomplete
/// (`out` is left untouched in that case).
fn try_tuple_at(
    cols: &[(MyType, String)],
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
    for (i, (ty, name)) in cols.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        if b.len() < off + 4 {
            out.truncate(out_start);
            return Ok(None);
        }
        let len = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
        off += 4;
        if len == -1 {
            out.extend_from_slice(b"\\N");
            continue;
        }
        if len < 0 {
            out.truncate(out_start);
            return Err(Error::Transfer(format!(
                "pg binary COPY: negative field length {len} in column {name}"
            )));
        }
        let len = len as usize;
        if b.len() < off + len {
            out.truncate(out_start);
            return Ok(None);
        }
        render_field(*ty, name, &b[off..off + len], out)?;
        off += len;
    }
    out.push(b'\n');
    Ok(Some((off, false)))
}

fn render_field(ty: MyType, name: &str, f: &[u8], out: &mut Vec<u8>) -> Result<()> {
    match ty {
        MyType::Int => {
            let v: i64 = match f.len() {
                2 => i16::from_be_bytes(f.try_into().unwrap()) as i64,
                4 => i32::from_be_bytes(f.try_into().unwrap()) as i64,
                8 => i64::from_be_bytes(f.try_into().unwrap()),
                n => return Err(bad(name, &format!("int width {n}"))),
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        MyType::Float32 => {
            let v = f32::from_be_bytes(f.try_into().map_err(|_| bad(name, "float4"))?);
            if !v.is_finite() {
                return Err(nonfinite(name));
            }
            out.extend_from_slice(ryu::Buffer::new().format(v).as_bytes());
        }
        MyType::Float64 => {
            let v = f64::from_be_bytes(f.try_into().map_err(|_| bad(name, "float8"))?);
            if !v.is_finite() {
                return Err(nonfinite(name));
            }
            out.extend_from_slice(ryu::Buffer::new().format(v).as_bytes());
        }
        MyType::Bool => out.push(if f.first() == Some(&1) { b'1' } else { b'0' }),
        MyType::Date => {
            let d = i32::from_be_bytes(f.try_into().map_err(|_| bad(name, "date"))?);
            let (y, m, dd) = civil_from_days(d as i64 + PG_EPOCH_DAYS as i64);
            check_year(name, y)?;
            push_date(y, m, dd, out);
        }
        MyType::Ts => {
            let t = i64::from_be_bytes(f.try_into().map_err(|_| bad(name, "timestamp"))?);
            let us = t
                .checked_add(PG_EPOCH_MICROS)
                .ok_or_else(|| bad(name, "timestamp range"))?;
            let days = us.div_euclid(86_400_000_000);
            let in_day = us.rem_euclid(86_400_000_000);
            let (y, m, dd) = civil_from_days(days);
            check_year(name, y)?;
            push_date(y, m, dd, out);
            out.push(b' ');
            let secs = in_day / 1_000_000;
            push2((secs / 3600) as u8, out);
            out.push(b':');
            push2(((secs / 60) % 60) as u8, out);
            out.push(b':');
            push2((secs % 60) as u8, out);
            out.push(b'.');
            let micros = (in_day % 1_000_000) as u32;
            let mut buf = [b'0'; 6];
            let mut v = micros;
            for slot in buf.iter_mut().rev() {
                *slot = b'0' + (v % 10) as u8;
                v /= 10;
            }
            out.extend_from_slice(&buf);
        }
        MyType::Numeric => render_numeric(name, f, out)?,
        MyType::NumericDouble => {
            // Render the exact digits, then prove DOUBLE can hold the value:
            // sql_mode='' would otherwise clamp overflow to ±DBL_MAX and
            // underflow to 0 with only a warning.
            let start = out.len();
            render_numeric(name, f, out)?;
            let txt = std::str::from_utf8(&out[start..]).expect("digits are ascii");
            let approx: f64 = txt.parse().unwrap_or(f64::INFINITY);
            let has_digits = txt.bytes().any(|b| (b'1'..=b'9').contains(&b));
            if !approx.is_finite() || (approx == 0.0 && has_digits) {
                return Err(Error::Transfer(format!(
                    "column {name}: unconstrained NUMERIC value is outside DOUBLE \
                     range (the destination column) — declare a precision or cast \
                     it at the source (e.g. ::numeric(38,10))"
                )));
            }
        }
        MyType::Text => tsv_escape(f, out),
        MyType::JsonB => {
            if f.is_empty() || f[0] != 1 {
                return Err(bad(name, "jsonb version"));
            }
            tsv_escape(&f[1..], out);
        }
        MyType::Uuid => {
            if f.len() != 16 {
                return Err(bad(name, "uuid"));
            }
            for (i, byte) in f.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    out.push(b'-');
                }
                out.push(HEX_LOWER[(byte >> 4) as usize]);
                out.push(HEX_LOWER[(byte & 0xf) as usize]);
            }
        }
        MyType::Bytea => {
            for byte in f {
                out.push(HEX_UPPER[(byte >> 4) as usize]);
                out.push(HEX_UPPER[(byte & 0xf) as usize]);
            }
        }
    }
    Ok(())
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// PG binary NUMERIC → exact decimal text, straight from the base-10000 digit
/// groups (no integer ceiling). Wire layout: ndigits i16, weight i16 (base-10000
/// exponent of the FIRST group), sign u16, dscale u16, then ndigits × u16 groups.
fn render_numeric(name: &str, f: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if f.len() < 8 {
        return Err(bad(name, "numeric header"));
    }
    let ndigits = i16::from_be_bytes(f[0..2].try_into().unwrap()) as usize;
    let weight = i16::from_be_bytes(f[2..4].try_into().unwrap()) as i32;
    let sign = u16::from_be_bytes(f[4..6].try_into().unwrap());
    let dscale = u16::from_be_bytes(f[6..8].try_into().unwrap()) as usize;
    if f.len() < 8 + ndigits * 2 {
        return Err(bad(name, "numeric digits"));
    }
    match sign {
        0x0000 => {}
        0x4000 => out.push(b'-'),
        // NaN / ±Infinity: MySQL DECIMAL/DOUBLE has no spelling for these, and
        // sql_mode='' would coerce the text to 0 with only a warning.
        _ => return Err(nonfinite(name)),
    }
    let group = |i: usize| u16::from_be_bytes(f[8 + i * 2..10 + i * 2].try_into().unwrap());

    // Integer part: groups at weight ≥ 0. First group unpadded, the rest %04.
    if weight < 0 || ndigits == 0 {
        out.push(b'0');
    } else {
        for gi in 0..=(weight as usize) {
            let v = if gi < ndigits { group(gi) } else { 0 };
            if gi == 0 {
                out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
            } else {
                push4(v, out);
            }
        }
    }

    // Fraction part: exactly dscale digits (PG's display scale — trailing zeros
    // included, exactly as `numeric::text` prints).
    if dscale > 0 {
        out.push(b'.');
        let mut written = 0usize;
        let mut gi = (weight + 1).max(0) as usize;
        // Groups strictly below weight 0 that precede the stored ones are zeros.
        let mut zero_groups = (-(weight + 1)).max(0) as usize;
        while written < dscale {
            let v = if zero_groups > 0 {
                zero_groups -= 1;
                0
            } else if gi < ndigits {
                let g = group(gi);
                gi += 1;
                g
            } else {
                0
            };
            let mut buf = [b'0'; 4];
            let mut x = v;
            for slot in buf.iter_mut().rev() {
                *slot = b'0' + (x % 10) as u8;
                x /= 10;
            }
            let take = (dscale - written).min(4);
            out.extend_from_slice(&buf[..take]);
            written += take;
        }
    }
    Ok(())
}

/// Howard Hinnant's civil-from-days: days since 1970-01-01 → (y, m, d).
fn civil_from_days(z: i64) -> (i64, u8, u8) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u8;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// MySQL DATE/DATETIME live in 1000-01-01..9999-12-31; outside that (including
/// Postgres ±infinity sentinels) the loader would coerce to the zero date.
fn check_year(name: &str, y: i64) -> Result<()> {
    if !(1000..=9999).contains(&y) {
        return Err(Error::Transfer(format!(
            "column {name}: date/timestamp year {y} is outside MySQL's \
             1000..9999 range (Postgres ±infinity lands here too) — cast or \
             filter it at the source"
        )));
    }
    Ok(())
}

fn push_date(y: i64, m: u8, d: u8, out: &mut Vec<u8>) {
    let mut buf = [b'0'; 4];
    let mut v = y as u32;
    for slot in buf.iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
    out.extend_from_slice(&buf);
    out.push(b'-');
    push2(m, out);
    out.push(b'-');
    push2(d, out);
}

fn push2(v: u8, out: &mut Vec<u8>) {
    out.push(b'0' + v / 10);
    out.push(b'0' + v % 10);
}

fn push4(v: u16, out: &mut Vec<u8>) {
    let mut buf = [b'0'; 4];
    let mut x = v;
    for slot in buf.iter_mut().rev() {
        *slot = b'0' + (x % 10) as u8;
        x /= 10;
    }
    out.extend_from_slice(&buf);
}

fn bad(name: &str, what: &str) -> Error {
    Error::Transfer(format!(
        "pg binary COPY: malformed {what} field in column {name}"
    ))
}

fn nonfinite(name: &str) -> Error {
    Error::Transfer(format!(
        "column {name} holds NaN or Infinity, which MySQL cannot store — \
         filter or cast it at the source (e.g. NULLIF({name}, 'NaN'))"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(payload: &[u8]) -> Vec<u8> {
        let mut v = (payload.len() as i32).to_be_bytes().to_vec();
        v.extend_from_slice(payload);
        v
    }

    fn pg_numeric(ndigits: &[u16], weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend((ndigits.len() as i16).to_be_bytes());
        v.extend(weight.to_be_bytes());
        v.extend(sign.to_be_bytes());
        v.extend(dscale.to_be_bytes());
        for d in ndigits {
            v.extend(d.to_be_bytes());
        }
        v
    }

    fn render(ty: MyType, f: &[u8]) -> String {
        let mut out = Vec::new();
        render_field(ty, "c", f, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn numeric_renders_exact_text() {
        // 1234.5678 (the shared wire-test constant)
        assert_eq!(
            render(MyType::Numeric, &pg_numeric(&[1234, 5678], 0, 0, 4)),
            "1234.5678"
        );
        // 50.0000 — trailing zero groups omitted on the wire, restored by dscale
        assert_eq!(
            render(MyType::Numeric, &pg_numeric(&[50], 0, 0, 4)),
            "50.0000"
        );
        // -0.00042: digits [4, 2000] at weight -1 (PG's actual wire form)
        assert_eq!(
            render(MyType::Numeric, &pg_numeric(&[4, 2000], -1, 0x4000, 5)),
            "-0.00042"
        );
        // 42e-12: weight -3 → two implied zero groups before the stored one
        assert_eq!(
            render(MyType::Numeric, &pg_numeric(&[42], -3, 0, 12)),
            "0.000000000042"
        );
        // 40 integer digits — past i128, must still be exact
        let big: Vec<u16> = vec![1; 10]; // 1 0001 0001 … (10 groups)
        let s = render(MyType::Numeric, &pg_numeric(&big, 9, 0, 0));
        assert_eq!(s, format!("1{}", "0001".repeat(9)));
        assert_eq!(s.len(), 37);
        // integer with implied trailing zero groups: 12 × 10^4 (weight 1)
        assert_eq!(render(MyType::Numeric, &pg_numeric(&[12], 1, 0, 0)), "120000");
        // zero
        assert_eq!(render(MyType::Numeric, &pg_numeric(&[], 0, 0, 0)), "0");
        assert_eq!(render(MyType::Numeric, &pg_numeric(&[], 0, 0, 2)), "0.00");
        // NaN refuses loudly
        let mut out = Vec::new();
        assert!(render_field(
            MyType::Numeric,
            "c",
            &pg_numeric(&[], 0, 0xC000, 0),
            &mut out
        )
        .is_err());
    }

    #[test]
    fn temporal_renders_mysql_spellings() {
        // 2020-01-01 = 7305 days after the PG epoch
        assert_eq!(render(MyType::Date, &7305i32.to_be_bytes()), "2020-01-01");
        // PG epoch itself, with micros
        assert_eq!(
            render(MyType::Ts, &1_500_000i64.to_be_bytes()),
            "2000-01-01 00:00:01.500000"
        );
        // pre-epoch timestamp (negative micros) still renders correctly
        assert_eq!(
            render(MyType::Ts, &(-1i64).to_be_bytes()),
            "1999-12-31 23:59:59.999999"
        );
        // out-of-range year fails loudly instead of zero-date coercion
        let mut out = Vec::new();
        assert!(render_field(MyType::Date, "c", &(i32::MAX).to_be_bytes(), &mut out).is_err());
        assert!(render_field(MyType::Ts, "c", &(i64::MAX).to_be_bytes(), &mut out).is_err());
    }

    #[test]
    fn scalar_and_binary_fields() {
        assert_eq!(render(MyType::Int, &(-7i64).to_be_bytes()), "-7");
        assert_eq!(render(MyType::Int, &(300i16).to_be_bytes()), "300");
        assert_eq!(render(MyType::Bool, &[1]), "1");
        assert_eq!(render(MyType::Bool, &[0]), "0");
        assert_eq!(render(MyType::Float64, &1.5f64.to_be_bytes()), "1.5");
        let mut out = Vec::new();
        assert!(render_field(MyType::Float64, "c", &f64::NAN.to_be_bytes(), &mut out).is_err());
        let uuid: Vec<u8> = (0..16).collect();
        assert_eq!(
            render(MyType::Uuid, &uuid),
            "00010203-0405-0607-0809-0a0b0c0d0e0f"
        );
        assert_eq!(render(MyType::Bytea, &[0xde, 0xad]), "DEAD");
        // jsonb strips the version byte; text escapes the LOAD DATA dialect
        assert_eq!(render(MyType::JsonB, b"\x01{\"a\":1}"), "{\"a\":1}");
        assert_eq!(render(MyType::Text, b"a\tb\\c"), "a\\tb\\\\c");
    }

    #[test]
    fn transcodes_tuples_and_nulls_at_any_chunking() {
        // Columns: id int, name text (NULL in row 2), ok bool.
        let cols = vec![
            (MyType::Int, "id".to_string()),
            (MyType::Text, "name".to_string()),
            (MyType::Bool, "ok".to_string()),
        ];
        let mut input = b"PGCOPY\n\xff\r\n\0".to_vec();
        input.extend(0u32.to_be_bytes());
        input.extend(0u32.to_be_bytes());
        input.extend(3i16.to_be_bytes());
        input.extend(field(&7i32.to_be_bytes()));
        input.extend(field(b"hi\tthere"));
        input.extend(field(&[1u8]));
        input.extend(3i16.to_be_bytes());
        input.extend(field(&8i32.to_be_bytes()));
        input.extend((-1i32).to_be_bytes()); // NULL
        input.extend(field(&[0u8]));
        input.extend((-1i16).to_be_bytes()); // trailer

        let expected = "7\thi\\tthere\t1\n8\t\\N\t0\n";
        for chunk_size in [3usize, 5, 1024] {
            let mut t = PgToMyTsv::new(cols.clone());
            let mut out = Vec::new();
            for c in input.chunks(chunk_size) {
                t.push(c, &mut out).unwrap();
            }
            assert!(t.finished(), "chunk_size={chunk_size}");
            assert_eq!(
                String::from_utf8(out).unwrap(),
                expected,
                "chunk_size={chunk_size}"
            );
        }
    }

    #[test]
    fn lane_gate_covers_and_refuses() {
        assert_eq!(my_tsv_type("int8", None, None), Some(MyType::Int));
        assert_eq!(
            my_tsv_type("numeric", Some(65), Some(30)),
            Some(MyType::Numeric)
        );
        assert_eq!(my_tsv_type("numeric", None, None), Some(MyType::NumericDouble));
        assert_eq!(my_tsv_type("numeric", Some(66), Some(0)), None); // p past 65
        assert_eq!(my_tsv_type("numeric", Some(50), Some(35)), None); // s past 30
        assert_eq!(my_tsv_type("numeric", Some(2), Some(5)), None); // s > p (PG 15)
        assert_eq!(my_tsv_type("numeric", Some(5), Some(-2)), None); // negative scale
        assert_eq!(my_tsv_type("bytea", None, None), Some(MyType::Bytea));
        assert_eq!(my_tsv_type("interval", None, None), None);
        assert_eq!(my_tsv_type("_int4", None, None), None); // arrays
    }

    #[test]
    fn numeric_double_range_checks() {
        // 1e400: digits [1], weight 100 — exact render, but DOUBLE can't hold it
        let mut out = Vec::new();
        assert!(
            render_field(MyType::NumericDouble, "c", &pg_numeric(&[1], 100, 0, 0), &mut out)
                .is_err()
        );
        // 1e-400: dscale 400 → renders sub-denormal, parses to 0.0 while nonzero
        let mut out = Vec::new();
        assert!(
            render_field(MyType::NumericDouble, "c", &pg_numeric(&[1], -100, 0, 400), &mut out)
                .is_err()
        );
        // in-range value passes and stays exact text
        assert_eq!(
            render(MyType::NumericDouble, &pg_numeric(&[1234, 5678], 0, 0, 4)),
            "1234.5678"
        );
        // true zero still renders
        assert_eq!(render(MyType::NumericDouble, &pg_numeric(&[], 0, 0, 0)), "0");
    }

    #[test]
    fn corrupt_negative_field_length_errors_not_panics() {
        let cols = vec![(MyType::Int, "id".to_string())];
        let mut input = b"PGCOPY\n\xff\r\n\0".to_vec();
        input.extend(0u32.to_be_bytes());
        input.extend(0u32.to_be_bytes());
        input.extend(1i16.to_be_bytes());
        input.extend((-2i32).to_be_bytes()); // hostile length
        let mut t = PgToMyTsv::new(cols);
        let mut out = Vec::new();
        assert!(t.push(&input, &mut out).is_err());
    }
}
