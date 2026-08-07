//! Text-cell rendering shared by the log_based apply paths.
//!
//! pgoutput delivers column values as RAW text bytes (no COPY escaping).
//! Postgres COPY text and ClickHouse TabSeparated share the same escape
//! dialect (`\\` `\t` `\n` `\r`, NULL = `\N`), so one renderer serves both.
//! Destinations that are not Postgres also need TYPE-aware rendering: the
//! WAL's text forms for bytea (`\x…` hex) and bool (`t`/`f`) are Postgres
//! dialect, not what other engines parse.

use crate::error::{Error, Result};
use crate::wire::pgoutput::{Cell, Cellv, Tuple};

pub(crate) const BYTEA_OID: u32 = 17;
pub(crate) const BOOL_OID: u32 = 16;
pub(crate) const TIMESTAMPTZ_OID: u32 = 1184;
pub(crate) const TIMETZ_OID: u32 = 1266;
const NUMERIC_OIDS: &[u32] = &[20, 21, 23, 26, 700, 701, 1700];

/// True for the four bytes COPY-text / TabSeparated framing must escape.
#[inline(always)]
fn is_special(b: u8) -> bool {
    matches!(b, b'\\' | b'\t' | b'\n' | b'\r')
}

/// SWAR probe: does this little-endian word contain any of the four special
/// bytes? The classic zero-byte trick per needle — `(x-0x01…) & !x & 0x80…`
/// has a bit set iff some byte of `x` is zero — applied to `w ^ (needle*L)`.
/// False positives are impossible; the scalar rescan below is authoritative
/// either way.
#[inline(always)]
fn word_has_special(w: u64) -> bool {
    const L: u64 = 0x0101_0101_0101_0101;
    const H: u64 = 0x8080_8080_8080_8080;
    let a = w ^ (L * 0x5C); // \
    let b = w ^ (L * 0x09); // \t
    let c = w ^ (L * 0x0A); // \n
    let d = w ^ (L * 0x0D); // \r
    ((a.wrapping_sub(L) & !a)
        | (b.wrapping_sub(L) & !b)
        | (c.wrapping_sub(L) & !c)
        | (d.wrapping_sub(L) & !d))
        & H
        != 0
}

/// Escape one raw value into COPY-text / TabSeparated framing.
///
/// Run-copy shape: skip ahead over clean bytes (eight at a time via the SWAR
/// word test — no unsafe, no table), then bulk-copy the clean run and emit
/// the rare escape. The previous byte-at-a-time match loop profiled at 12.0%
/// of a capped CDC drain's samples over ~810 MB of mostly-clean data; the
/// clean runs now move at extend_from_slice speed. Output is byte-identical
/// by construction — the scalar rescan decides every escape, the SWAR word
/// only decides how far to skip — and the differential test below proves it.
pub(crate) fn copy_escape(v: &[u8], out: &mut Vec<u8>) {
    out.reserve(v.len());
    let mut start = 0;
    let mut i = 0;
    while i < v.len() {
        while i + 8 <= v.len() {
            let w = u64::from_le_bytes(v[i..i + 8].try_into().unwrap());
            if word_has_special(w) {
                break;
            }
            i += 8;
        }
        while i < v.len() && !is_special(v[i]) {
            i += 1;
        }
        out.extend_from_slice(&v[start..i]);
        if i == v.len() {
            return;
        }
        match v[i] {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            _ => out.extend_from_slice(b"\\r"),
        }
        i += 1;
        start = i;
    }
}

/// One full row, Postgres-dialect text (COPY text for a pg destination).
pub(crate) fn render_copy_row(row: &Tuple, out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.views().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cellv::Null => out.extend_from_slice(b"\\N"),
            Cellv::Text(t) => copy_escape(t, out),
            Cellv::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell reached the bulk path — \
                     collapse bug"
                        .into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

/// Indices of the PK columns within the WAL column order.
pub(crate) fn pk_indices(pk_cols: &[String], wal_cols: &[String]) -> Result<Vec<usize>> {
    pk_cols
        .iter()
        .map(|k| {
            wal_cols.iter().position(|c| c == k).ok_or_else(|| {
                Error::Transfer(format!("log_based: PK column '{k}' not in WAL columns"))
            })
        })
        .collect()
}

/// Borrow an upsert row's key cells (key cells are `Text` by construction).
pub(crate) fn row_key_refs<'a>(row: &'a Tuple, pk_idx: &[usize]) -> Vec<&'a [u8]> {
    pk_idx
        .iter()
        .map(|&i| match row.view(i) {
            Cellv::Text(t) => t,
            _ => b"".as_slice(),
        })
        .collect()
}

/// Decode Postgres bytea TEXT form to raw bytes. The WAL always renders
/// `\x…` hex (`bytea_output` does not apply to logical decoding).
pub(crate) fn decode_bytea(t: &[u8]) -> Result<Vec<u8>> {
    let hex_part = t
        .strip_prefix(b"\\x")
        .ok_or_else(|| Error::Transfer("log_based: bytea value without \\x prefix".into()))?;
    hex::decode(hex_part)
        .map_err(|e| Error::Transfer(format!("log_based: bad bytea hex from WAL: {e}")))
}

/// Render one raw text value for a ClickHouse TabSeparated body, translating
/// Postgres-dialect forms by type OID. bytea stays in its `\x…` TEXT form —
/// that is what the bulk pg→ch lane lands in the String column, and CDC must
/// match the bootstrap byte-for-byte.
pub(crate) fn render_ch_value(v: &[u8], oid: u32, out: &mut Vec<u8>) -> Result<()> {
    match oid {
        // The bulk lane lands pg bool as UInt8 — 1/0 parses into both UInt8
        // and Bool columns; "true" only into Bool.
        BOOL_OID => out.push(if v == b"t" { b'1' } else { b'0' }),
        // Types whose Postgres TEXT rendering cannot contain a byte that
        // TabSeparated escapes (digits, sign, dot, colon, dash, space, 'e'):
        // ints, floats, numeric, date, time(tz), timestamp(tz), uuid, oid.
        // Copy straight through — most cells of a wide row are these, and
        // scanning them was pure tax. STRICT allowlist: any OID not listed
        // (text, json, bytea, arrays, unknown) takes the scanning path.
        20 | 21 | 23 | 26 | 700 | 701 | 1700 | 1082 | 1083 | 1114 | 1184 | 1266 | 2950 => {
            out.extend_from_slice(v)
        }
        _ => copy_escape(v, out),
    }
    Ok(())
}

/// One full row for ClickHouse TabSeparated, type-aware (frame-native path).
pub(crate) fn render_ch_row(row: &Tuple, oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.views().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cellv::Null => out.extend_from_slice(b"\\N"),
            Cellv::Text(t) => render_ch_value(t, oids[i], out)?,
            Cellv::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell reached the bulk path — \
                     collapse bug"
                        .into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

/// Residue-tail variants over OWNED cells (`ResidueOp` materializes rows out
/// of the frame-native fast path; rare by design).
pub(crate) fn render_ch_row_cells(row: &[Cell], oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) => render_ch_value(t, oids[i], out)?,
            Cell::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell reached the bulk path — \
                     collapse bug"
                        .into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

pub(crate) fn row_key_refs_cells<'a>(row: &'a [Cell], pk_idx: &[usize]) -> Vec<&'a [u8]> {
    pk_idx
        .iter()
        .map(|&i| match &row[i] {
            Cell::Text(t) => &t[..],
            _ => b"".as_slice(),
        })
        .collect()
}

/// One key row (owned or borrowed cells) for ClickHouse TabSeparated.
pub(crate) fn render_ch_key(key: &[&[u8]], oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, k) in key.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        render_ch_value(k, oids[i], out)?;
    }
    out.push(b'\n');
    Ok(())
}

/// A ClickHouse SQL literal for one key value, typed by OID: numeric types
/// render bare (validated), everything else (bytea's `\x…` text included) is
/// a quoted string literal.
pub(crate) fn ch_key_literal(v: &[u8], oid: u32) -> Result<String> {
    let s = std::str::from_utf8(v)
        .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
    if NUMERIC_OIDS.contains(&oid) {
        if !s.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E')) {
            return Err(Error::Transfer(format!(
                "log_based: numeric key value '{s}' has unexpected characters"
            )));
        }
        return Ok(s.to_string());
    }
    Ok(format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")))
}

/// Strip the `+00` / `+00:00` suffix the WAL renders on timestamptz/timetz
/// (the walsender pins `TimeZone=UTC`, so the offset is always zero) —
/// MySQL's DATETIME/TIME parsers reject trailing offsets.
pub(crate) fn strip_utc_offset(v: &[u8]) -> Result<&[u8]> {
    if let Some(s) = v.strip_suffix(b"+00") {
        return Ok(s);
    }
    if let Some(s) = v.strip_suffix(b"+00:00") {
        return Ok(s);
    }
    Err(Error::Transfer(format!(
        "log_based: timestamptz '{}' does not carry a +00 offset — the \
         replication session should be pinned to UTC (walsender bug?)",
        String::from_utf8_lossy(v)
    )))
}

/// The bytea hex digits WITHOUT the `\x` prefix (for MySQL's `UNHEX(@var)`
/// LOAD DATA binding and `UNHEX('…')` literals).
pub(crate) fn bytea_hex(v: &[u8]) -> Result<&[u8]> {
    v.strip_prefix(b"\\x")
        .ok_or_else(|| Error::Transfer("log_based: bytea value without \\x prefix".into()))
}

/// Render one raw text value for a MySQL LOAD DATA body (mytsv dialect),
/// translating Postgres-dialect forms by type OID. Binary (bytea) columns
/// ride as bare hex — the LOAD DATA statement UNHEXes them via a user var.
pub(crate) fn render_my_value(v: &[u8], oid: u32, out: &mut Vec<u8>) -> Result<()> {
    match oid {
        BYTEA_OID => out.extend_from_slice(bytea_hex(v)?),
        BOOL_OID => out.push(if v == b"t" { b'1' } else { b'0' }),
        TIMESTAMPTZ_OID | TIMETZ_OID => {
            crate::wire::mytsv::tsv_escape(strip_utc_offset(v)?, out)
        }
        _ => crate::wire::mytsv::tsv_escape(v, out),
    }
    Ok(())
}

/// One full row for a MySQL LOAD DATA body, type-aware.
pub(crate) fn render_my_row(row: &Tuple, oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.views().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cellv::Null => out.extend_from_slice(b"\\N"),
            Cellv::Text(t) => render_my_value(t, oids[i], out)?,
            Cellv::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell reached the bulk path — \
                     collapse bug"
                        .into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

/// One key row for a MySQL LOAD DATA body.
pub(crate) fn render_my_key(key: &[&[u8]], oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, k) in key.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        render_my_value(k, oids[i], out)?;
    }
    out.push(b'\n');
    Ok(())
}

/// Unescape one field of ClickHouse TabSeparated OUTPUT (its escape set is a
/// superset of what we emit). Returns `None` for `\N`.
pub(crate) fn tsv_unescape(field: &str) -> Option<Vec<u8>> {
    if field == "\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut it = field.bytes();
    while let Some(b) = it.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match it.next() {
            Some(b't') => out.push(b'\t'),
            Some(b'n') => out.push(b'\n'),
            Some(b'r') => out.push(b'\r'),
            Some(b'\\') => out.push(b'\\'),
            Some(b'\'') => out.push(b'\''),
            Some(b'b') => out.push(0x08),
            Some(b'f') => out.push(0x0C),
            Some(b'0') => out.push(0x00),
            Some(other) => {
                out.push(b'\\');
                out.push(other);
            }
            None => out.push(b'\\'),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference implementation the SWAR run-copy version replaced. Kept
    /// verbatim as the oracle: the differential test below asserts the two
    /// agree byte-for-byte, which is the entire correctness argument for the
    /// fast path.
    fn copy_escape_reference(v: &[u8], out: &mut Vec<u8>) {
        for &b in v {
            match b {
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'\t' => out.extend_from_slice(b"\\t"),
                b'\n' => out.extend_from_slice(b"\\n"),
                b'\r' => out.extend_from_slice(b"\\r"),
                _ => out.push(b),
            }
        }
    }

    fn assert_escape_matches(v: &[u8]) {
        let (mut fast, mut slow) = (Vec::new(), Vec::new());
        copy_escape(v, &mut fast);
        copy_escape_reference(v, &mut slow);
        assert_eq!(fast, slow, "input: {v:?}");
    }

    #[test]
    fn swar_escape_differential() {
        // Every special at every offset within and across the 8-byte stride.
        for &sp in &[b'\\', b'\t', b'\n', b'\r'] {
            for pos in 0..25 {
                for len in [pos + 1, 8, 9, 16, 17, 24, 31] {
                    if pos >= len {
                        continue;
                    }
                    let mut v = vec![b'a'; len];
                    v[pos] = sp;
                    assert_escape_matches(&v);
                }
            }
        }
        // Shape extremes.
        assert_escape_matches(b"");
        assert_escape_matches(b"\\\\\\\\\\\\\\\\\\");
        assert_escape_matches(&[b'\t'; 40]);
        assert_escape_matches(&vec![b'x'; 4096]);
        assert_escape_matches(b"plain then\ttab\nand\rall\\of it mixed in one value");
        // 0x0B / 0x0C sit inside the SWAR needles' neighbourhood but must NOT
        // be escaped — the scalar rescan is authoritative.
        assert_escape_matches(&[0x0B, 0x0C, 0x09, 0x0B, 0x0C]);
        // Deterministic pseudo-random sweep, all byte values, varied lengths.
        let mut state = 0x243F_6A88_85A3_08D3u64;
        for len in 0..300 {
            let v: Vec<u8> = (0..len)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    (state >> 33) as u8
                })
                .collect();
            assert_escape_matches(&v);
        }
    }

    #[test]
    fn oid_gate_only_skips_types_that_cannot_hold_specials() {
        // A gated OID copies verbatim…
        let mut out = Vec::new();
        render_ch_value(b"12345.6789", 1700, &mut out).unwrap();
        assert_eq!(out, b"12345.6789");
        // …an ungated OID still escapes.
        let mut out = Vec::new();
        render_ch_value(b"a\tb", 25, &mut out).unwrap();
        assert_eq!(out, b"a\\tb");
    }

    #[test]
    fn bytea_hex_decodes() {
        assert_eq!(decode_bytea(b"\\x48690a").unwrap(), b"Hi\n");
        assert!(decode_bytea(b"4869").is_err());
    }

    #[test]
    fn ch_bool_translates_and_bytea_stays_text() {
        let mut out = Vec::new();
        render_ch_value(b"t", BOOL_OID, &mut out).unwrap();
        out.push(b'|');
        render_ch_value(b"f", BOOL_OID, &mut out).unwrap();
        out.push(b'|');
        // bytea keeps its \x-text form (the bulk-lane convention), TSV-escaped.
        render_ch_value(b"\\x0102", BYTEA_OID, &mut out).unwrap();
        assert_eq!(out, b"1|0|\\\\x0102");
    }

    #[test]
    fn ch_key_literal_types() {
        assert_eq!(ch_key_literal(b"123", 20).unwrap(), "123");
        assert_eq!(ch_key_literal(b"a'b", 25).unwrap(), "'a\\'b'");
        assert_eq!(ch_key_literal(b"\\x00", BYTEA_OID).unwrap(), "'\\\\x00'");
        assert!(ch_key_literal(b"1; DROP", 20).is_err());
    }

    #[test]
    fn tsv_roundtrip() {
        let mut esc = Vec::new();
        copy_escape(b"a\tb\nc\\d", &mut esc);
        let back = tsv_unescape(std::str::from_utf8(&esc).unwrap()).unwrap();
        assert_eq!(back, b"a\tb\nc\\d");
        assert_eq!(tsv_unescape("\\N"), None);
    }
}
