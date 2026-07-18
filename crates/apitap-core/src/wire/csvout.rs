//! TSV → CSV transcoding, shared by every destination that emits CSV from the
//! Postgres text lane (the BigQuery load lane, the GCS file destination).
//! Buffers arrive RECORD-ALIGNED per the [`crate::sink::Loader`] contract.
//!
//! CSV semantics (probed live against BigQuery, and the de-facto convention):
//! unquoted-empty = NULL, quoted `""` = empty string; values with a delimiter,
//! quote, or newline are quoted with `""` doubling.

use crate::error::{Error, Result};
use crate::plan::{wm_max, Delivered};
use crate::wire::pgtext::unescape_into;

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub(crate) fn base64_into(data: &[u8], out: &mut Vec<u8>) {
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        out.push(B64[(b[0] >> 2) as usize]);
        out.push(B64[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize]);
        out.push(if chunk.len() > 1 {
            B64[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            B64[(b[2] & 0x3f) as usize]
        } else {
            b'='
        });
    }
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Bytes that force a CSV field into quotes.
static CSV_NEEDS_QUOTE: [bool; 256] = {
    let mut t = [false; 256];
    t[b',' as usize] = true;
    t[b'"' as usize] = true;
    t[b'\n' as usize] = true;
    t[b'\r' as usize] = true;
    t
};

/// Quote a CSV field: wrap in quotes, double any embedded quote. Bulk-copies
/// clean runs; embedded newlines are legal (the load sets allowQuotedNewlines).
pub(crate) fn csv_quote_into(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    let mut i = 0;
    while i < s.len() {
        let mut j = i;
        while j < s.len() && s[j] != b'"' {
            j += 1;
        }
        out.extend_from_slice(&s[i..j]);
        if j == s.len() {
            break;
        }
        out.extend_from_slice(b"\"\"");
        i = j + 1;
    }
    out.push(b'"');
}

/// Append one field's CSV form for `d`. `raw` is the still-escaped TSV field.
/// BigQuery CSV semantics (probed live): unquoted empty = NULL, quoted "" =
/// empty string — so a present value that is empty or contains a delimiter,
/// quote, or newline gets quoted; everything else rides raw.
pub(crate) fn append_csv_value(raw: &[u8], d: &Delivered, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
    scratch.clear();
    let val: &[u8] = if raw.contains(&b'\\') {
        unescape_into(raw, scratch);
        scratch.as_slice()
    } else {
        raw
    };
    match d {
        // Numbers, dates, uuids: never contain CSV specials — raw. NaN and
        // Infinity are accepted FLOAT64 tokens; a NUMERIC NaN fails the load
        // loudly, same as every other destination.
        Delivered::Int { .. }
        | Delivered::Decimal { .. }
        | Delivered::Float32
        | Delivered::Float64
        | Delivered::Date
        | Delivered::DateTime { utc: false }
        | Delivered::Uuid => out.extend_from_slice(val),
        Delivered::Bool => out.extend_from_slice(if val == b"t" { b"true" } else { b"false" }),
        Delivered::DateTime { utc: true } => {
            // Postgres (UTC session) renders "…+00"; BigQuery wants a full offset.
            out.extend_from_slice(val);
            if val.len() > 3 && matches!(val[val.len() - 3], b'+' | b'-') {
                out.extend_from_slice(b":00");
            }
        }
        Delivered::Bytes => {
            // bytea_output=hex: "\x<hex>" — base64 has no CSV specials. An
            // EMPTY bytea must land quoted: an unquoted empty field is NULL.
            let hex = val.strip_prefix(b"\\x").unwrap_or(val);
            if hex.is_empty() {
                out.extend_from_slice(b"\"\"");
            } else {
                let bytes: Vec<u8> = hex
                    .chunks(2)
                    .map(|p| (hex_val(p[0]) << 4) | p.get(1).map(|&b| hex_val(b)).unwrap_or(0))
                    .collect();
                base64_into(&bytes, out);
            }
        }
        Delivered::Json | Delivered::Text => {
            if val.is_empty() || val.iter().any(|&b| CSV_NEEDS_QUOTE[b as usize]) {
                csv_quote_into(val, out);
            } else {
                out.extend_from_slice(val);
            }
        }
    }
}


/// Transcode record-aligned TSV bytes into CSV. Returns rows converted.
/// NULL fields become unquoted-empty (= NULL to BigQuery; probed live), and
/// `cursor` = (column index, numeric) tracks the running MAX of that column
/// into `wm` — the staged watermark comes from here for free instead of a
/// billed MAX() query against the staging table.
pub(crate) fn tsv_to_csv(
    input: &[u8],
    delivered: &[Delivered],
    null_marker: bool,
    cursor: Option<(usize, bool)>,
    wm: &mut Option<String>,
    out: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> Result<u64> {
    let mut rows = 0u64;
    // split_inclusive: no trailing artifact to skip, so a genuinely empty line
    // (a single empty-string text column) stays a REAL record.
    for piece in input.split_inclusive(|&b| b == b'\n') {
        let line = piece.strip_suffix(b"\n").unwrap_or(piece);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let mut col = 0usize;
        for field in line.split(|&b| b == b'\t') {
            if col >= delivered.len() {
                return Err(Error::Transfer(format!(
                    "tsv row has more fields than the {} planned columns",
                    delivered.len()
                )));
            }
            if col > 0 {
                out.push(b',');
            }
            if field == b"\\N" {
                // Single-column tables set an explicit marker: a NULL row would
                // otherwise be a BLANK line, which CSV parsers drop.
                if null_marker {
                    out.extend_from_slice(b"\\N");
                }
            } else {
                // In marker mode a REAL value equal to the marker must be quoted
                // or it would read back as NULL.
                if null_marker && field == b"\\\\N" {
                    out.extend_from_slice(b"\"\\N\"");
                    col += 1;
                    continue;
                }
                append_csv_value(field, &delivered[col], out, scratch);
                if let Some((idx, numeric)) = cursor {
                    if col == idx {
                        // Cursor values (ints, timestamps) never carry escapes.
                        let v = String::from_utf8_lossy(field).into_owned();
                        *wm = wm_max(wm.take(), Some(v), numeric);
                    }
                }
            }
            col += 1;
        }
        if col != delivered.len() {
            return Err(Error::Transfer(format!(
                "tsv row has {col} fields, expected {}",
                delivered.len()
            )));
        }
        out.push(b'\n');
        rows += 1;
    }
    Ok(rows)
}
