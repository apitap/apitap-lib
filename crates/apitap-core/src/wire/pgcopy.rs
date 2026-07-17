//! Postgres binary-COPY ENCODER — the inverse of the parser in [`crate::wire::rowbinary`],
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
pub(crate) fn numeric_to_scaled_i128_raw(f: &[u8]) -> Result<(i128, i32)> {
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
