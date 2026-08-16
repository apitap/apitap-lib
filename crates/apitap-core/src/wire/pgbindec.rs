//! Postgres BINARY-format values → the exact TEXT bytes the type's output
//! function would have produced (under the walsender session's pinned
//! `TimeZone=UTC`). This is the receiving half of pgoutput's `binary 'true'`
//! option (PG14+): the walsender then ships `send`-format tuples instead of
//! running every column through its text output function — and those output
//! functions (EncodeDateTime, numeric_out, pg_lltoa) run inside the ONE
//! pegged walsender process that is the measured per-slot ceiling
//! (benchmarks/gcp-cdc-100tables.md Part 7). Rendering here moves that text
//! cost onto apitap's core, which idles 60%+ in every receipt.
//!
//! The contract is byte-identity with the text plane: every apply path
//! (TSV escape allowlists, `strip_utc_offset`, `decode_bytea`, bool `t`/`f`
//! mapping, collapse keys, digests) consumes PG text forms. A renderer that
//! is "close" corrupts silently, so anything not proven identical returns a
//! loud error naming the OID instead of guessing.

use crate::error::{Error, Result};

/// Days from 1970-01-01 to 2000-01-01 (the Postgres date epoch).
const PG_EPOCH_UNIX_DAYS: i64 = 10_957;
const USECS_PER_DAY: i64 = 86_400_000_000;

/// Append the PG-text rendering of one binary `send`-format value.
pub(crate) fn render(oid: u32, raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    match oid {
        // bool_out: 't' / 'f' (NOT 'true'/'false' — that is the text cast).
        16 => {
            let b = *raw.first().ok_or_else(|| short(oid))?;
            out.push(if b != 0 { b't' } else { b'f' });
        }
        // int2 / int4 / int8.
        21 => push_int(i16::from_be_bytes(fixed(raw, oid)?), out),
        23 => push_int(i32::from_be_bytes(fixed(raw, oid)?), out),
        20 => push_int(i64::from_be_bytes(fixed(raw, oid)?), out),
        // oid / xid / cid are unsigned 32-bit.
        26 | 28 | 29 => push_int(u32::from_be_bytes(fixed(raw, oid)?), out),
        700 => push_float(f32::from_be_bytes(fixed(raw, oid)?) as f64, true, out),
        701 => push_float(f64::from_be_bytes(fixed(raw, oid)?), false, out),
        1700 => numeric(raw, out)?,
        // Text-shaped types: `send` format IS the text bytes.
        25 | 1043 | 1042 | 19 | 18 | 114 | 142 | 705 => out.extend_from_slice(raw),
        // bytea: the WAL text plane always renders \x hex (bytea_output does
        // not apply to logical decoding) — match it.
        17 => {
            out.extend_from_slice(b"\\x");
            push_hex(raw, out);
        }
        // jsonb_send = 1-byte version header + the jsonb_out text.
        3802 => {
            if raw.first() != Some(&1) {
                return Err(Error::Transfer(format!(
                    "pgoutput binary: jsonb version {:?} (only 1 is known)",
                    raw.first()
                )));
            }
            out.extend_from_slice(&raw[1..]);
        }
        2950 => uuid(raw, out)?,
        1082 => date(i32::from_be_bytes(fixed(raw, oid)?), out),
        1083 => {
            time(i64::from_be_bytes(fixed(raw, oid)?), out);
        }
        1114 => timestamp(i64::from_be_bytes(fixed(raw, oid)?), false, out),
        1184 => timestamp(i64::from_be_bytes(fixed(raw, oid)?), true, out),
        1266 => timetz(raw, out)?,
        other => {
            return Err(Error::Transfer(format!(
                "pgoutput binary: no text renderer for type OID {other} yet — \
                 unset APITAP_PG_BINARY to stream this table in text mode"
            )))
        }
    }
    Ok(())
}

fn short(oid: u32) -> Error {
    Error::Transfer(format!("pgoutput binary: truncated value for type OID {oid}"))
}

fn fixed<const N: usize>(raw: &[u8], oid: u32) -> Result<[u8; N]> {
    raw.try_into().map_err(|_| short(oid))
}

fn push_int(v: impl itoa::Integer, out: &mut Vec<u8>) {
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(v).as_bytes());
}

fn push_hex(raw: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in raw {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0xf) as usize]);
    }
}

/// float4/float8 the way PG's shortest_dec.c prints them: shortest
/// round-trip digits (Ryu — the same algorithm PG ports), then PG's OWN
/// notation rule, probed against a live 16.x server: scientific iff the
/// decimal exponent is >= 15 or < -4 (`1e14` prints fixed, `1e+15` and
/// `1e-05` scientific, `0.0001` fixed), exponent always signed with two
/// digits minimum, no trailing `.0` on whole values. Ryu-the-crate has its
/// own (different) fixed range, so its output is renormalized from digits +
/// exponent rather than trusted.
fn push_float(v: f64, single: bool, out: &mut Vec<u8>) {
    if v.is_nan() {
        out.extend_from_slice(b"NaN");
        return;
    }
    if v.is_infinite() {
        out.extend_from_slice(if v > 0.0 { b"Infinity" } else { b"-Infinity" });
        return;
    }
    let mut b = ryu::Buffer::new();
    let s = if single { b.format_finite(v as f32) } else { b.format_finite(v) };
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    // Normalize ryu's output to (significant digits, decimal exponent of the
    // FIRST digit). ryu scientific mantissa is always d[.ddd].
    let (digits, exp10): (String, i32) = match s.split_once(['e', 'E']) {
        Some((m, e)) => {
            let exp: i32 = e.parse().unwrap_or(0);
            (m.replace('.', ""), exp)
        }
        None => match s.split_once('.') {
            Some((int, frac)) => {
                if int == "0" {
                    let lead = frac.len() - frac.trim_start_matches('0').len();
                    let sig = frac.trim_start_matches('0');
                    if sig.is_empty() {
                        ("0".into(), 0) // 0.0
                    } else {
                        (sig.to_string(), -(lead as i32) - 1)
                    }
                } else {
                    (format!("{int}{frac}"), int.len() as i32 - 1)
                }
            }
            None => (s.to_string(), s.len() as i32 - 1),
        },
    };
    let digits = {
        // Shortest form has no significant trailing zeros; anything trailing
        // here is positional (e.g. "100.0" → "100") and re-derivable.
        let t = digits.trim_end_matches('0');
        if t.is_empty() { "0" } else { t }
    };
    if neg {
        out.push(b'-'); // PG prints "-0" for negative zero
    }
    if digits == "0" {
        out.push(b'0');
        return;
    }
    if (-4..15).contains(&exp10) {
        // Fixed notation.
        if exp10 < 0 {
            out.extend_from_slice(b"0.");
            for _ in 0..(-exp10 - 1) {
                out.push(b'0');
            }
            out.extend_from_slice(digits.as_bytes());
        } else {
            let int_len = (exp10 + 1) as usize;
            if digits.len() <= int_len {
                out.extend_from_slice(digits.as_bytes());
                for _ in 0..int_len - digits.len() {
                    out.push(b'0');
                }
            } else {
                out.extend_from_slice(digits[..int_len].as_bytes());
                out.push(b'.');
                out.extend_from_slice(digits[int_len..].as_bytes());
            }
        }
    } else {
        // Scientific: d[.rest]e±NN.
        out.push(digits.as_bytes()[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(digits[1..].as_bytes());
        }
        out.push(b'e');
        out.push(if exp10 < 0 { b'-' } else { b'+' });
        let a = exp10.unsigned_abs();
        if a < 10 {
            out.push(b'0');
        }
        push_int(a, out);
    }
}

/// numeric `send` format → `numeric_out` text: base-10000 digit groups,
/// `weight` groups before the point, exactly `dscale` decimal digits after.
fn numeric(raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if raw.len() < 8 {
        return Err(short(1700));
    }
    let ndigits = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = (u16::from_be_bytes([raw[6], raw[7]]) & 0x3FFF) as usize;
    if raw.len() < 8 + 2 * ndigits {
        return Err(short(1700));
    }
    match sign {
        0x0000 => {}
        0x4000 => out.push(b'-'),
        0xC000 => {
            out.extend_from_slice(b"NaN");
            return Ok(());
        }
        0xD000 => {
            out.extend_from_slice(b"Infinity");
            return Ok(());
        }
        0xF000 => {
            out.extend_from_slice(b"-Infinity");
            return Ok(());
        }
        other => {
            return Err(Error::Transfer(format!(
                "pgoutput binary: numeric sign word {other:#06x}"
            )))
        }
    }
    let dig = |i: i64| -> u16 {
        if i >= 0 && (i as usize) < ndigits {
            let o = 8 + 2 * i as usize;
            u16::from_be_bytes([raw[o], raw[o + 1]])
        } else {
            0
        }
    };
    // Integer part: groups 0..=weight; the first prints unpadded.
    if weight < 0 {
        out.push(b'0');
    } else {
        push_int(dig(0), out);
        for d in 1..=weight {
            let g = dig(d);
            out.extend_from_slice(&[
                b'0' + (g / 1000) as u8,
                b'0' + (g / 100 % 10) as u8,
                b'0' + (g / 10 % 10) as u8,
                b'0' + (g % 10) as u8,
            ]);
        }
    }
    // Fraction: exactly dscale digits, zero-padded (numeric_out never trims).
    if dscale > 0 {
        out.push(b'.');
        for i in 0..dscale {
            let g = dig(weight + 1 + (i / 4) as i64);
            let digit = match i % 4 {
                0 => g / 1000,
                1 => g / 100 % 10,
                2 => g / 10 % 10,
                _ => g % 10,
            };
            out.push(b'0' + digit as u8);
        }
    }
    Ok(())
}

fn uuid(raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if raw.len() != 16 {
        return Err(short(2950));
    }
    for (i, chunk) in [&raw[0..4], &raw[4..6], &raw[6..8], &raw[8..10], &raw[10..16]]
        .iter()
        .enumerate()
    {
        if i > 0 {
            out.push(b'-');
        }
        push_hex(chunk, out);
    }
    Ok(())
}

/// Proleptic-Gregorian civil date from days since 1970-01-01
/// (Howard Hinnant's algorithm — matches PG's j2date across the full range).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn push2(v: u32, out: &mut Vec<u8>) {
    out.push(b'0' + (v / 10 % 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// `YYYY-MM-DD`, with BC years rendered PG-style (year 0 = 1 BC). Returns
/// whether the caller must append " BC" (after the time/zone for timestamps).
fn push_date(days_since_2000: i64, out: &mut Vec<u8>) -> bool {
    let (y, m, d) = civil_from_days(days_since_2000 + PG_EPOCH_UNIX_DAYS);
    let (disp, bc) = if y <= 0 { (1 - y, true) } else { (y, false) };
    if disp < 1000 {
        // %04d
        out.push(b'0' + (disp / 1000 % 10) as u8);
        out.push(b'0' + (disp / 100 % 10) as u8);
        out.push(b'0' + (disp / 10 % 10) as u8);
        out.push(b'0' + (disp % 10) as u8);
    } else {
        push_int(disp, out);
    }
    out.push(b'-');
    push2(m, out);
    out.push(b'-');
    push2(d, out);
    bc
}

fn date(days: i32, out: &mut Vec<u8>) {
    if days == i32::MAX {
        out.extend_from_slice(b"infinity");
        return;
    }
    if days == i32::MIN {
        out.extend_from_slice(b"-infinity");
        return;
    }
    if push_date(days as i64, out) {
        out.extend_from_slice(b" BC");
    }
}

/// `HH:MM:SS[.ffffff]` with trailing fraction zeros trimmed (AppendSeconds).
fn time(us: i64, out: &mut Vec<u8>) {
    let h = us / 3_600_000_000;
    let m = (us / 60_000_000) % 60;
    let s = (us / 1_000_000) % 60;
    let f = us % 1_000_000;
    push2(h as u32, out);
    out.push(b':');
    push2(m as u32, out);
    out.push(b':');
    push2(s as u32, out);
    if f > 0 {
        out.push(b'.');
        let digits = [
            (f / 100_000 % 10) as u8,
            (f / 10_000 % 10) as u8,
            (f / 1_000 % 10) as u8,
            (f / 100 % 10) as u8,
            (f / 10 % 10) as u8,
            (f % 10) as u8,
        ];
        let keep = 6 - digits.iter().rev().take_while(|&&d| d == 0).count();
        for &d in &digits[..keep] {
            out.push(b'0' + d);
        }
    }
}

/// timestamp/timestamptz: µs since 2000-01-01 (UTC when tz). The session is
/// pinned to UTC so timestamptz always renders a literal `+00`.
fn timestamp(t: i64, tz: bool, out: &mut Vec<u8>) {
    if t == i64::MAX {
        out.extend_from_slice(b"infinity");
        return;
    }
    if t == i64::MIN {
        out.extend_from_slice(b"-infinity");
        return;
    }
    let days = t.div_euclid(USECS_PER_DAY);
    let us = t.rem_euclid(USECS_PER_DAY);
    let bc = push_date(days, out);
    out.push(b' ');
    time(us, out);
    if tz {
        out.extend_from_slice(b"+00");
    }
    if bc {
        out.extend_from_slice(b" BC");
    }
}

/// timetz `send`: int64 µs + int32 zone (seconds WEST of UTC — the text
/// offset is its negation). EncodeTimezone prints hours always, minutes and
/// seconds only when nonzero.
fn timetz(raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    if raw.len() != 12 {
        return Err(short(1266));
    }
    let us = i64::from_be_bytes(raw[0..8].try_into().unwrap());
    let zone = i32::from_be_bytes(raw[8..12].try_into().unwrap());
    time(us, out);
    let off = -(zone as i64);
    out.push(if off < 0 { b'-' } else { b'+' });
    let a = off.unsigned_abs();
    push2((a / 3600) as u32, out);
    let (mm, ss) = ((a / 60 % 60) as u32, (a % 60) as u32);
    if mm != 0 || ss != 0 {
        out.push(b':');
        push2(mm, out);
        if ss != 0 {
            out.push(b':');
            push2(ss, out);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(oid: u32, raw: &[u8]) -> String {
        let mut out = Vec::new();
        render(oid, raw, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn bools_and_ints() {
        assert_eq!(r(16, &[1]), "t");
        assert_eq!(r(16, &[0]), "f");
        assert_eq!(r(21, &(-7i16).to_be_bytes()), "-7");
        assert_eq!(r(23, &123456i32.to_be_bytes()), "123456");
        assert_eq!(r(20, &i64::MIN.to_be_bytes()), "-9223372036854775808");
        assert_eq!(r(26, &4000000000u32.to_be_bytes()), "4000000000");
    }

    #[test]
    fn floats_match_pg_shortest_form() {
        assert_eq!(r(701, &1.5f64.to_be_bytes()), "1.5");
        assert_eq!(r(701, &100.0f64.to_be_bytes()), "100");
        assert_eq!(r(701, &1e20f64.to_be_bytes()), "1e+20");
        assert_eq!(r(701, &0.00001f64.to_be_bytes()), "1e-05");
        assert_eq!(r(701, &(-0.0f64).to_be_bytes()), "-0");
        assert_eq!(r(701, &f64::NAN.to_be_bytes()), "NaN");
        assert_eq!(r(701, &f64::INFINITY.to_be_bytes()), "Infinity");
        assert_eq!(r(700, &1.25f32.to_be_bytes()), "1.25");
        // The notation boundary, probed on a live PG 16: fixed through 1e14
        // and 0.0001, scientific from 1e+15 and 1e-05.
        assert_eq!(r(701, &1e14f64.to_be_bytes()), "100000000000000");
        assert_eq!(r(701, &1e15f64.to_be_bytes()), "1e+15");
        assert_eq!(r(701, &0.0001f64.to_be_bytes()), "0.0001");
        assert_eq!(r(701, &0.00015f64.to_be_bytes()), "0.00015");
        assert_eq!(r(701, &123456789012345.6f64.to_be_bytes()), "123456789012345.6");
        assert_eq!(r(701, &1.79e308f64.to_be_bytes()), "1.79e+308");
        assert_eq!(r(701, &5e-324f64.to_be_bytes()), "5e-324");
        assert_eq!(r(701, &1234.5678f64.to_be_bytes()), "1234.5678");
    }

    /// numeric send images built by hand: (ndigits, weight, sign, dscale, groups).
    fn num(weight: i16, sign: u16, dscale: u16, groups: &[u16]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend((groups.len() as u16).to_be_bytes());
        v.extend(weight.to_be_bytes());
        v.extend(sign.to_be_bytes());
        v.extend(dscale.to_be_bytes());
        for g in groups {
            v.extend(g.to_be_bytes());
        }
        v
    }

    #[test]
    fn numeric_matches_numeric_out() {
        // 1234.50 as numeric(12,2): groups [1234, 5000], weight 0, dscale 2.
        assert_eq!(r(1700, &num(0, 0, 2, &[1234, 5000])), "1234.50");
        // 0 with dscale 2 → "0.00" (ndigits 0).
        assert_eq!(r(1700, &num(0, 0, 2, &[])), "0.00");
        // -0.001 dscale 3: weight -1, group [10].
        assert_eq!(r(1700, &num(-1, 0x4000, 3, &[10])), "-0.001");
        // 10000 → groups [1, 0] weight 1 — the zero group pads to 0000.
        assert_eq!(r(1700, &num(1, 0, 0, &[1])), "10000");
        // 12345678.999 → [1234, 5678, 9990] weight 1 dscale 3.
        assert_eq!(r(1700, &num(1, 0, 3, &[1234, 5678, 9990])), "12345678.999");
        // Trailing-zero group elided by the sender: 100.00 = [1] weight 0? No:
        // 100 → [100]; with dscale 2 the fraction groups are absent → zeros.
        assert_eq!(r(1700, &num(0, 0, 2, &[100])), "100.00");
        assert_eq!(r(1700, &num(0, 0xC000, 0, &[])), "NaN");
    }

    #[test]
    fn dates_times_timestamps() {
        // 2000-01-01 is day 0 / µs 0.
        assert_eq!(r(1082, &0i32.to_be_bytes()), "2000-01-01");
        assert_eq!(r(1114, &0i64.to_be_bytes()), "2000-01-01 00:00:00");
        assert_eq!(r(1184, &0i64.to_be_bytes()), "2000-01-01 00:00:00+00");
        // 2026-08-16 12:34:56.5 UTC.
        let us = (9724 * USECS_PER_DAY) + (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 500_000;
        assert_eq!(r(1184, &us.to_be_bytes()), "2026-08-16 12:34:56.5+00");
        // Fraction keeps inner zeros, trims trailing ones.
        let us2 = us - 500_000 + 120_000;
        assert_eq!(r(1184, &us2.to_be_bytes()), "2026-08-16 12:34:56.12+00");
        let us3 = us - 500_000 + 102_030;
        assert_eq!(r(1184, &us3.to_be_bytes()), "2026-08-16 12:34:56.10203+00");
        // Pre-2000 (negative µs): 1999-12-31 23:59:59.
        assert_eq!(r(1114, &(-1_000_000i64).to_be_bytes()), "1999-12-31 23:59:59");
        assert_eq!(r(1082, &i32::MAX.to_be_bytes()), "infinity");
        assert_eq!(r(1184, &i64::MIN.to_be_bytes()), "-infinity");
        // 0001-01-01 BC is ISO year 0: 719528 days before 1970-01-01, so
        // -719528 - 10957 from the 2000-01-01 epoch.
        assert_eq!(r(1082, &(-730_485i32).to_be_bytes()), "0001-01-01 BC");
        assert_eq!(r(1083, &(3_600_000_000i64 * 24).to_be_bytes()), "24:00:00");
    }

    #[test]
    fn text_shapes_pass_through() {
        assert_eq!(r(25, b"hello\tworld"), "hello\tworld");
        assert_eq!(r(25, b""), "");
        assert_eq!(r(3802, b"\x01{\"a\": 1}"), "{\"a\": 1}");
        assert_eq!(r(17, &[0xde, 0xad, 0x00]), "\\xdead00");
        assert_eq!(
            r(2950, &[0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                      0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00]),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn timetz_offsets() {
        let mut raw = Vec::new();
        raw.extend(43_200_000_000i64.to_be_bytes()); // 12:00:00
        raw.extend(0i32.to_be_bytes());
        assert_eq!(r(1266, &raw), "12:00:00+00");
        let mut raw = Vec::new();
        raw.extend(0i64.to_be_bytes());
        raw.extend((-19800i32).to_be_bytes()); // zone -19800 s west = +05:30
        assert_eq!(r(1266, &raw), "00:00:00+05:30");
    }

    #[test]
    fn unsupported_oid_fails_loudly() {
        let mut out = Vec::new();
        let e = render(1186, &[0; 16], &mut out).unwrap_err();
        assert!(format!("{e}").contains("1186"));
    }
}
