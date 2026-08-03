//! Postgres binary-COPY format: the ENCODER (for routes writing INTO Postgres),
//! the shared epoch/NUMERIC parsing vocabulary, and the SpanStrip framing stripper.
//! used by routes that write INTO Postgres from a non-Postgres source. Binary beats
//! text here for the same reason twice over: the destination skips text parsing, and
//! the encoder has no escaping rules to get wrong.

use crate::error::{Error, Result};

pub(crate) const PG_EPOCH_DAYS: i32 = 10_957;
pub(crate) const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// 19-byte stream header (signature + flags + extension length).
pub(crate) fn header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&[0u8; 8]);
}

/// End-of-stream trailer.
pub(crate) fn trailer(out: &mut Vec<u8>) {
    out.extend((-1i16).to_be_bytes());
}

pub(crate) fn tuple_start(ncols: usize, out: &mut Vec<u8>) {
    out.extend((ncols as i16).to_be_bytes());
}

pub(crate) fn null_field(out: &mut Vec<u8>) {
    out.extend((-1i32).to_be_bytes());
}

pub(crate) fn field(payload: &[u8], out: &mut Vec<u8>) {
    out.extend((payload.len() as i32).to_be_bytes());
    out.extend_from_slice(payload);
}

/// `jsonb` binary payload = a 1-byte version header + the JSON text.
pub(crate) fn jsonb_field(json_text: &[u8], out: &mut Vec<u8>) {
    out.extend(((json_text.len() + 1) as i32).to_be_bytes());
    out.push(1);
    out.extend_from_slice(json_text);
}

/// Encode a decimal TEXT literal ("−1234.5678") as a Postgres binary `numeric` field.
/// Digit-string based — no integer-width ceiling, so MySQL's DECIMAL(65,s) fits.
/// dscale is taken from the literal's fractional length (MySQL's CAST emits the
/// column's full scale).
pub(crate) fn numeric_field_from_str(s: &str, out: &mut Vec<u8>) -> Result<()> {
    let bad = || Error::Transfer(format!("malformed decimal '{s}'"));
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(bad());
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    let dscale = frac_part.len() as i16;
    // Stack buffer bound: MySQL DECIMAL tops out at 65 digits; 100 total digits pad to
    // ≤ 27 base-10000 groups, so [i16; 32] covers it with no heap allocation. This runs
    // once per DECIMAL cell — 3 Vecs here was 60M allocs on a 10M-row × 2-col table.
    if int_part.len() + frac_part.len() > 100 {
        return Err(bad());
    }
    let (int_part, frac_part) = (int_part.as_bytes(), frac_part.as_bytes());

    // Group the digit string into base-10000 groups aligned on the decimal point:
    // pad the integer part LEFT and the fractional part RIGHT to multiples of 4.
    let mut groups = [0i16; 32];
    let mut ng = 0usize;
    // Integer groups, most significant first.
    {
        let pad = (4 - int_part.len() % 4) % 4;
        let mut acc: i16 = 0;
        let mut n = 0;
        for i in 0..pad + int_part.len() {
            let d = if i < pad { 0 } else { int_part[i - pad] - b'0' };
            acc = acc * 10 + d as i16;
            n += 1;
            if n == 4 {
                groups[ng] = acc;
                ng += 1;
                acc = 0;
                n = 0;
            }
        }
    }
    let int_groups = ng as i16;
    // Fractional groups.
    {
        let mut acc: i16 = 0;
        let mut n = 0;
        for i in 0..frac_part.len().div_ceil(4) * 4 {
            let d = frac_part.get(i).map_or(0, |b| b - b'0');
            acc = acc * 10 + d as i16;
            n += 1;
            if n == 4 {
                groups[ng] = acc;
                ng += 1;
                acc = 0;
                n = 0;
            }
        }
    }
    // Canonical form: trim leading and trailing zero groups by narrowing a window
    // (adjusting the weight) — no drain/pop.
    let lead = groups[..ng].iter().take_while(|&&g| g == 0).count();
    let mut end = ng;
    while end > lead && groups[end - 1] == 0 {
        end -= 1;
    }
    let (ndigits, weight) = if lead == end {
        (0i16, 0i16)
    } else {
        ((end - lead) as i16, int_groups - 1 - lead as i16)
    };
    let sign: u16 = if neg && ndigits > 0 { 0x4000 } else { 0x0000 };

    out.extend(((8 + ndigits as usize * 2) as i32).to_be_bytes());
    out.extend(ndigits.to_be_bytes());
    out.extend(weight.to_be_bytes());
    out.extend(sign.to_be_bytes());
    out.extend(dscale.to_be_bytes());
    for g in &groups[lead..end] {
        out.extend(g.to_be_bytes());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode then run it through the DECODER from rowbinary.rs — exact roundtrip.
    fn roundtrip(s: &str, scale: u32) -> i128 {
        let mut out = Vec::new();
        numeric_field_from_str(s, &mut out).unwrap();
        // Strip the 4-byte length header; the rest is the numeric payload.
        numeric_to_scaled_i128(&out[4..], scale).unwrap()
    }

    #[test]
    fn numeric_roundtrips_through_the_decoder() {
        assert_eq!(roundtrip("1234.5678", 4), 12_345_678);
        assert_eq!(roundtrip("-1234.5678", 4), -12_345_678);
        assert_eq!(roundtrip("50.0000", 4), 500_000);
        assert_eq!(roundtrip("0.5000", 4), 5_000);
        assert_eq!(roundtrip("0.0000", 4), 0);
        assert_eq!(
            roundtrip("12345678901234567890.12", 2),
            1_234_567_890_123_456_789_012
        );
        assert_eq!(roundtrip("10000", 0), 10_000);
        assert_eq!(roundtrip("100000000", 0), 100_000_000); // group boundary
    }

    #[test]
    fn numeric_handles_mysql_max_decimal_65_30() {
        // DECIMAL(65,30) worst case: 35 int digits + 30 frac digits. Too wide for the
        // i128 roundtrip, so assert the wire header fields directly.
        let s = format!("{}.{}", "9".repeat(35), "9".repeat(30));
        let mut out = Vec::new();
        numeric_field_from_str(&s, &mut out).unwrap();
        // int: 35 digits pad to 36 → 9 groups; frac: 30 pad to 32 → 8 groups.
        let ndigits = i16::from_be_bytes(out[4..6].try_into().unwrap());
        let weight = i16::from_be_bytes(out[6..8].try_into().unwrap());
        let dscale = i16::from_be_bytes(out[10..12].try_into().unwrap());
        assert_eq!(ndigits, 17);
        assert_eq!(weight, 8);
        assert_eq!(dscale, 30);
        assert_eq!(out.len(), 4 + 8 + 17 * 2);

        // Beyond the stack-buffer bound → clean error, not a panic.
        let too_wide = "9".repeat(101);
        assert!(numeric_field_from_str(&too_wide, &mut Vec::new()).is_err());
    }

    /// Encode {ndigits, weight, sign, dscale, groups} to the PG binary numeric wire
    /// payload (the part after the 4-byte field length): four be i16/u16 header
    /// fields, then the u16 be digit groups.
    fn pg_numeric_payload(
        ndigits: i16,
        weight: i16,
        sign: u16,
        dscale: u16,
        groups: &[u16],
    ) -> Vec<u8> {
        let mut v = Vec::with_capacity(8 + groups.len() * 2);
        v.extend(ndigits.to_be_bytes());
        v.extend(weight.to_be_bytes());
        v.extend(sign.to_be_bytes());
        v.extend(dscale.to_be_bytes());
        for g in groups {
            v.extend(g.to_be_bytes());
        }
        v
    }

    /// The dispatching decoder (fast path + fallback) must agree with the general
    /// decoder on both values and error messages.
    fn assert_same(f: &[u8], ctx: &str) {
        let fast = numeric_to_scaled_i128_raw(f);
        let general = numeric_to_scaled_i128_raw_general(f);
        match (fast, general) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "value mismatch for {ctx}"),
            (Err(a), Err(b)) => {
                assert_eq!(a.to_string(), b.to_string(), "error mismatch for {ctx}")
            }
            (a, b) => panic!("Ok/Err mismatch for {ctx}: fast={a:?} general={b:?}"),
        }
    }

    #[test]
    fn numeric_fast_path_matches_general_path() {
        // Grid over every header field the fast path dispatches on, with digit
        // patterns covering 0, 1, and the max group 9999. Cases land on both sides
        // of every fast-path precondition (ndigits <= 3, |exp10| <= 26), so this
        // also proves the fallback boundary is seamless.
        const PATTERNS: [[u16; 6]; 3] = [
            [0, 1, 9999, 0, 9999, 1],
            [9999, 9999, 9999, 9999, 9999, 9999],
            [1234, 0, 42, 9999, 1, 7],
        ];
        for ndigits in 0..=6i16 {
            for weight in -3..=5i16 {
                for sign in [0x0000u16, 0x4000] {
                    for dscale in 0..=8u16 {
                        for pat in &PATTERNS {
                            let groups = &pat[..ndigits as usize];
                            let f = pg_numeric_payload(ndigits, weight, sign, dscale, groups);
                            assert_same(
                                &f,
                                &format!(
                                    "ndigits={ndigits} weight={weight} sign={sign:#06x} \
                                     dscale={dscale} groups={groups:?}"
                                ),
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn numeric_fast_path_boundary_shifts() {
        // With ndigits=1, exp10 = weight*4 + dscale: this sweep crosses the ±26
        // fast-path window and reaches the general path's overflow error
        // (weight=8, dscale=6 → 10^38 shift on 9999) — parity on every cell.
        for dscale in 0..=6u16 {
            for weight in [-9i16, -7, 5, 6, 7, 8] {
                let f = pg_numeric_payload(1, weight, 0x0000, dscale, &[9999]);
                assert_same(&f, &format!("boundary weight={weight} dscale={dscale}"));
            }
        }
    }

    #[test]
    fn numeric_fast_path_defers_on_specials_and_malformed() {
        // NaN / +Inf / -Inf sentinels and an unknown sign word: identical errors.
        for sign in [0xC000u16, 0xD000, 0xF000, 0x1234] {
            let f = pg_numeric_payload(0, 0, sign, 0, &[]);
            assert_same(&f, &format!("special sign {sign:#06x}"));
            assert!(numeric_to_scaled_i128_raw(&f).is_err());
        }
        // Truncated header and truncated digit area: identical errors.
        assert_same(&[], "empty payload");
        assert_same(&[0, 1, 0, 0, 0, 0], "short header");
        let mut f = pg_numeric_payload(3, 0, 0x0000, 0, &[1, 2, 3]);
        f.truncate(10); // claims 3 groups, carries 1
        assert_same(&f, "truncated digits");
        // Extreme weights: nonzero value overflows in the general path (the fast
        // path must defer and surface the same error); zero stays zero.
        let f = pg_numeric_payload(1, 12_000, 0x0000, 0, &[1]);
        assert_same(&f, "huge weight, nonzero");
        assert!(numeric_to_scaled_i128_raw(&f).is_err());
        let f = pg_numeric_payload(0, 12_000, 0x0000, 3, &[]);
        assert_same(&f, "huge weight, zero");
        assert_eq!(numeric_to_scaled_i128_raw(&f).unwrap(), (0, 3));
    }

    #[test]
    fn numeric_wrapper_rescale_unchanged() {
        // 1234.5678 as (ndigits=2, weight=0, dscale=4) through the scale wrapper.
        let f = pg_numeric_payload(2, 0, 0x0000, 4, &[1234, 5678]);
        assert_eq!(numeric_to_scaled_i128(&f, 4).unwrap(), 12_345_678);
        assert_eq!(numeric_to_scaled_i128(&f, 6).unwrap(), 1_234_567_800);
        assert_eq!(numeric_to_scaled_i128(&f, 2).unwrap(), 123_456); // truncates
        let f = pg_numeric_payload(2, 0, 0x4000, 4, &[1234, 5678]);
        assert_eq!(numeric_to_scaled_i128(&f, 4).unwrap(), -12_345_678);
        // 0.5000 stored with negative weight (ndigits=1, weight=-1, group 5000).
        let f = pg_numeric_payload(1, -1, 0x0000, 4, &[5000]);
        assert_eq!(numeric_to_scaled_i128(&f, 4).unwrap(), 5_000);
    }

    #[test]
    fn framing_helpers_emit_the_wire_shapes() {
        let mut out = Vec::new();
        header(&mut out);
        assert_eq!(out.len(), 19);
        assert_eq!(&out[..11], b"PGCOPY\n\xff\r\n\0");
        out.clear();
        tuple_start(3, &mut out);
        field(b"hi", &mut out);
        null_field(&mut out);
        jsonb_field(b"{}", &mut out);
        trailer(&mut out);
        let expected: Vec<u8> = [
            &3i16.to_be_bytes()[..],
            &2i32.to_be_bytes(),
            b"hi",
            &(-1i32).to_be_bytes(),
            &3i32.to_be_bytes(),
            &[1u8],
            b"{}",
            &(-1i16).to_be_bytes(),
        ]
        .concat();
        assert_eq!(out, expected);
    }
}



fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: malformed {what} field"))
}

/// PG binary NUMERIC (ndigits, weight, sign, dscale + base-10000 digit groups) → an
/// integer scaled to exactly `scale` decimal places.
#[inline]
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

/// 10^0 ..= 10^26 — the largest decimal shift the fast path in
/// `numeric_to_scaled_i128_raw` applies without checked arithmetic:
/// three base-10000 groups keep acc < 10^12, and 10^12 · 10^26 = 10^38 < i128::MAX.
const POW10: [i128; 27] = {
    let mut t = [1i128; 27];
    let mut i = 1;
    while i < 27 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
};

/// → (value scaled to `dscale` decimal places, dscale).
///
/// Fast path for the money-shaped common case (this runs once per row per Decimal
/// column on the Arrow read hot path): 0..=3 digit groups, a plain +/- sign word,
/// and a total decimal shift within ±26 — decoded with unchecked arithmetic, which
/// the bounds above prove safe. Every other shape — NaN/Infinity sentinels, unknown
/// sign words, truncated payloads, long digit strings, extreme weights — falls
/// through to the general decoder, which is the behavioral oracle.
#[inline]
pub(crate) fn numeric_to_scaled_i128_raw(f: &[u8]) -> Result<(i128, i32)> {
    // One 8-byte big-endian load covers the whole header; fields fall out of shifts.
    if let Some(hdr) = f.get(..8) {
        let hdr = u64::from_be_bytes(hdr.try_into().unwrap());
        let ndigits = (hdr >> 48) as u16; // i16 on the wire; negative → > 3 → defer
        let sign = (hdr >> 16) as u16;
        if ndigits <= 3 && (sign == 0x0000 || sign == 0x4000) {
            let nd = ndigits as usize;
            if let Some(d) = f.get(8..8 + nd * 2) {
                let dscale = (hdr & 0xFFFF) as i32;
                if nd == 0 {
                    // Zero: the general path yields (0, dscale) for every weight.
                    return Ok((0, dscale));
                }
                let weight = (hdr >> 32) as u16 as i16 as i32;
                // Accumulate in u64 (three 0..=9999 groups < 10^12); widen once.
                let mut acc: u64 = 0;
                for ch in d.chunks_exact(2) {
                    acc = acc * 10_000 + u16::from_be_bytes([ch[0], ch[1]]) as u64;
                }
                let exp10 = (weight - nd as i32 + 1) * 4 + dscale;
                if (-26..=26).contains(&exp10) {
                    let v = if exp10 >= 0 {
                        acc as i128 * POW10[exp10 as usize]
                    } else {
                        // acc >= 0 here, so one division by 10^k is bit-identical
                        // to the general path's k successive divisions by 10.
                        acc as i128 / POW10[(-exp10) as usize]
                    };
                    return Ok((if sign == 0x4000 { -v } else { v }, dscale));
                }
            }
        }
    }
    numeric_to_scaled_i128_raw_general(f)
}

/// The general decoder: handles every wire shape, and serves as the oracle the fast
/// path must match bit-for-bit (see `numeric_fast_path_matches_general_path`).
fn numeric_to_scaled_i128_raw_general(f: &[u8]) -> Result<(i128, i32)> {
    if f.len() < 8 {
        return Err(bad("numeric"));
    }
    let ndigits = i16::from_be_bytes(f[0..2].try_into().unwrap()) as i32;
    let weight = i16::from_be_bytes(f[2..4].try_into().unwrap()) as i32;
    let sign = u16::from_be_bytes(f[4..6].try_into().unwrap());
    let dscale = u16::from_be_bytes(f[6..8].try_into().unwrap()) as i32;
    match sign {
        0x0000 | 0x4000 => {}
        0xC000 => return Err(bad("numeric NaN")),
        // pinf/ninf sentinels — decoding them as 0 would be silent corruption.
        0xD000 | 0xF000 => return Err(bad("numeric Infinity")),
        _ => return Err(bad("numeric sign")),
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

/// Per-span framing stripper for the raw binary passthrough: skips the 19-byte header
/// (+ extension area) at stream start and withholds the last 2 bytes so the trailer
/// never reaches the destination mid-stream (the worker emits one synthetic header up
/// front and one trailer at the very end). The withheld tail doubles as the "did the
/// stream end cleanly" check.
pub(crate) struct SpanStrip {
    hdr: [u8; 19],
    hdr_len: usize,
    skip: usize,
    pending: [u8; 2],
    npending: usize,
}

impl SpanStrip {
    pub(crate) fn new() -> Self {
        Self {
            hdr: [0; 19],
            hdr_len: 0,
            skip: 0,
            pending: [0; 2],
            npending: 0,
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn push(&mut self, mut b: &[u8], out: &mut Vec<u8>) -> Result<()> {
        if self.hdr_len < 19 {
            let take = (19 - self.hdr_len).min(b.len());
            self.hdr[self.hdr_len..self.hdr_len + take].copy_from_slice(&b[..take]);
            self.hdr_len += take;
            b = &b[take..];
            if self.hdr_len < 19 {
                return Ok(());
            }
            if &self.hdr[..11] != b"PGCOPY\n\xff\r\n\0" {
                return Err(Error::Transfer("pg binary COPY: bad header".into()));
            }
            self.skip = u32::from_be_bytes(self.hdr[15..19].try_into().unwrap()) as usize;
        }
        if self.skip > 0 {
            let take = self.skip.min(b.len());
            self.skip -= take;
            b = &b[take..];
        }
        // Body relay with a 2-byte holdback (the eventual trailer).
        match b.len() {
            0 => {}
            1 => {
                if self.npending == 2 {
                    out.push(self.pending[0]);
                    self.pending[0] = self.pending[1];
                    self.pending[1] = b[0];
                } else {
                    self.pending[self.npending] = b[0];
                    self.npending += 1;
                }
            }
            n => {
                out.extend_from_slice(&self.pending[..self.npending]);
                out.extend_from_slice(&b[..n - 2]);
                self.pending.copy_from_slice(&b[n - 2..]);
                self.npending = 2;
            }
        }
        Ok(())
    }

    /// Did the span end exactly on the 2-byte trailer?
    pub(crate) fn finished(&self) -> bool {
        self.hdr_len == 19 && self.skip == 0 && self.npending == 2 && self.pending == [0xFF, 0xFF]
    }
}
