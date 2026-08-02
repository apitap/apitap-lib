//! Text-cell rendering shared by the log_based apply paths.
//!
//! pgoutput delivers column values as RAW text bytes (no COPY escaping).
//! Postgres COPY text and ClickHouse TabSeparated share the same escape
//! dialect (`\\` `\t` `\n` `\r`, NULL = `\N`), so one renderer serves both.
//! Destinations that are not Postgres also need TYPE-aware rendering: the
//! WAL's text forms for bytea (`\x…` hex) and bool (`t`/`f`) are Postgres
//! dialect, not what other engines parse.

use crate::error::{Error, Result};
use crate::wire::pgoutput::Cell;

pub(crate) const BYTEA_OID: u32 = 17;
pub(crate) const BOOL_OID: u32 = 16;
pub(crate) const TIMESTAMPTZ_OID: u32 = 1184;
pub(crate) const TIMETZ_OID: u32 = 1266;
const NUMERIC_OIDS: &[u32] = &[20, 21, 23, 26, 700, 701, 1700];

/// Escape one raw value into COPY-text / TabSeparated framing.
pub(crate) fn copy_escape(v: &[u8], out: &mut Vec<u8>) {
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

/// One full row, Postgres-dialect text (COPY text for a pg destination).
pub(crate) fn render_copy_row(row: &[Cell], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) => copy_escape(t, out),
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
pub(crate) fn row_key_refs<'a>(row: &'a [Cell], pk_idx: &[usize]) -> Vec<&'a [u8]> {
    pk_idx
        .iter()
        .map(|&i| match &row[i] {
            Cell::Text(t) => t.as_slice(),
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
/// Postgres-dialect forms by type OID (bytea → raw bytes, bool → true/false).
pub(crate) fn render_ch_value(v: &[u8], oid: u32, out: &mut Vec<u8>) -> Result<()> {
    match oid {
        BYTEA_OID => {
            let raw = decode_bytea(v)?;
            copy_escape(&raw, out);
        }
        BOOL_OID => out.extend_from_slice(if v == b"t" { b"true" } else { b"false" }),
        _ => copy_escape(v, out),
    }
    Ok(())
}

/// One full row for ClickHouse TabSeparated, type-aware.
pub(crate) fn render_ch_row(row: &[Cell], oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
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
/// render bare (validated), bytea keys are refused, everything else is a
/// quoted string literal.
pub(crate) fn ch_key_literal(v: &[u8], oid: u32) -> Result<String> {
    if oid == BYTEA_OID {
        return Err(Error::InvalidInput(
            "log_based: bytea replica-identity keys are not supported for \
             ClickHouse destinations"
                .into(),
        ));
    }
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
pub(crate) fn render_my_row(row: &[Cell], oids: &[u32], out: &mut Vec<u8>) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) => render_my_value(t, oids[i], out)?,
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

    #[test]
    fn bytea_hex_decodes() {
        assert_eq!(decode_bytea(b"\\x48690a").unwrap(), b"Hi\n");
        assert!(decode_bytea(b"4869").is_err());
    }

    #[test]
    fn ch_bool_and_bytea_translate() {
        let mut out = Vec::new();
        render_ch_value(b"t", BOOL_OID, &mut out).unwrap();
        out.push(b'|');
        render_ch_value(b"f", BOOL_OID, &mut out).unwrap();
        out.push(b'|');
        render_ch_value(b"\\x09", BYTEA_OID, &mut out).unwrap();
        assert_eq!(out, b"true|false|\\t");
    }

    #[test]
    fn ch_key_literal_types() {
        assert_eq!(ch_key_literal(b"123", 20).unwrap(), "123");
        assert_eq!(ch_key_literal(b"a'b", 25).unwrap(), "'a\\'b'");
        assert!(ch_key_literal(b"\\x00", BYTEA_OID).is_err());
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
