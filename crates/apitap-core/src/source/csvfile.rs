//! CSV-objects-as-tables machinery, shared by every file-ish source (GitHub
//! today; S3 and friends later). A "table" is one `.csv` file: row 1 is the
//! header, every column is nullable TEXT exactly as written (typed casts belong
//! in the destination, where they fail loudly per value), an empty field is
//! NULL. Parsing is RFC-4180 (quoted fields, embedded delimiters/newlines/
//! doubled quotes) via csv-core's push parser — objects STREAM through a
//! bounded buffer that only ever grows to the largest single record.

use crate::error::{Error, Result};
use crate::plan::{ColumnPlan, Lane, TablePlan};
use crate::sink::Loader;
use crate::wire::textrow::TextEnc;
use futures::StreamExt;

/// One parsed record: field slices into the pump's buffers.
pub(crate) struct CsvRow<'a> {
    out: &'a [u8],
    ends: &'a [usize],
}

impl CsvRow<'_> {
    pub(crate) fn field(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.out[start..self.ends[i]]
    }
    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }
}

/// Incremental RFC-4180 reader over streamed chunks. csv-core's contract is
/// RESUMPTION: a record (even a single field) may span many `read_record`
/// calls, each writing a further slice of output — so the pump accumulates
/// `outlen`/`ends` across calls and only resets after a delivered record.
pub(crate) struct CsvPump {
    rdr: csv_core::Reader,
    input: Vec<u8>,
    consumed: usize,
    out: Vec<u8>,
    outlen: usize,
    /// Absolute end offset of each completed field in `out`.
    ends: Vec<usize>,
    /// Per-call relative ends, translated into `ends` after every call.
    scratch: Vec<usize>,
    row_ready: bool,
}

impl CsvPump {
    pub(crate) fn new() -> Self {
        Self {
            rdr: csv_core::Reader::new(),
            input: Vec::with_capacity(256 * 1024),
            consumed: 0,
            out: vec![0; 64 * 1024],
            outlen: 0,
            ends: Vec::with_capacity(256),
            scratch: vec![0; 512],
            row_ready: false,
        }
    }

    pub(crate) fn feed(&mut self, chunk: &[u8]) {
        // Compact before extending: the buffer stays ~one chunk + one record.
        if self.consumed > 0 {
            self.input.drain(..self.consumed);
            self.consumed = 0;
        }
        self.input.extend_from_slice(chunk);
    }

    /// Next complete record, or None when more input is needed (or, with
    /// `eof=true`, when the stream is exhausted). Empty input is exactly
    /// csv-core's end-of-data signal (it flushes a final unterminated record) —
    /// which is why the parser must never see an empty slice BEFORE eof.
    pub(crate) fn next_record(&mut self, eof: bool) -> Option<CsvRow<'_>> {
        if self.row_ready {
            self.outlen = 0;
            self.ends.clear();
            self.row_ready = false;
        }
        loop {
            let input = &self.input[self.consumed..];
            if input.is_empty() && !eof {
                return None;
            }
            let (res, nin, nout, nend) =
                self.rdr
                    .read_record(input, &mut self.out[self.outlen..], &mut self.scratch);
            // csv-core writes end positions "as if there was a single contiguous
            // buffer containing the entire row" — they are already record-
            // absolute, no base translation.
            self.ends.extend_from_slice(&self.scratch[..nend]);
            self.outlen += nout;
            self.consumed += nin;
            match res {
                csv_core::ReadRecordResult::Record => {
                    // True blank lines never reach here — csv-core discards
                    // them in its DFA. A `""` line DOES arrive as one empty
                    // field and is delivered: it's the RFC-4180 spelling of a
                    // single empty (= NULL) cell, and every other CSV reader
                    // returns it too.
                    self.row_ready = true;
                    return Some(CsvRow {
                        out: &self.out[..self.outlen],
                        ends: &self.ends,
                    });
                }
                csv_core::ReadRecordResult::End => return None,
                csv_core::ReadRecordResult::InputEmpty => {
                    if !eof {
                        return None;
                    }
                }
                csv_core::ReadRecordResult::OutputFull => {
                    let len = self.out.len();
                    self.out.resize(len * 2, 0);
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let len = self.scratch.len();
                    self.scratch.resize(len * 2, 0);
                }
            }
        }
    }
}

/// A record's fields as UTF-8 strings (header parsing).
pub(crate) fn row_strings(row: &CsvRow<'_>) -> Result<Vec<String>> {
    (0..row.len())
        .map(|i| {
            String::from_utf8(row.field(i).to_vec())
                .map_err(|e| Error::InvalidInput(format!("csv header is not UTF-8: {e}")))
        })
        .collect()
}

/// Build the all-text plan from a header row: names trimmed, blanks become
/// `col_N`, duplicates fail loudly (case-insensitive).
pub(crate) fn text_plan(engine: &'static str, label: &str, headers: &[String]) -> Result<TablePlan> {
    let mut seen = std::collections::HashSet::new();
    let mut cols = Vec::with_capacity(headers.len());
    for (i, h) in headers.iter().enumerate() {
        let name = if h.trim().is_empty() {
            format!("col_{}", i + 1)
        } else {
            h.trim().to_string()
        };
        if !seen.insert(name.to_lowercase()) {
            return Err(Error::InvalidInput(format!(
                "'{label}' has a duplicate column header '{name}' — headers must be \
                 unique (fix the file)"
            )));
        }
        cols.push(ColumnPlan {
            name,
            nullable: true,
            int_pk: false,
            native_ddl: None,
            udt: "text".into(),
            precision: None,
            scale: None,
        });
    }
    Ok(TablePlan {
        engine,
        cols,
        cursor: None,
        pk_cols: Vec::new(),
    })
}

/// (stem, key, est_rows) for every `.csv` under `prefix`. The stem — the file
/// name minus `.csv` — is the table name. Duplicate stems are ALLOWED here:
/// they only matter when actually used (resolving that stem, or a whole-schema
/// run) — an unrelated collision elsewhere in the tree must not block a
/// single-table transfer. est is size-derived (≥32 bytes/row assumed): it only
/// drives largest-first scheduling and the initial pipe ask, both
/// self-correcting.
pub(crate) fn csv_tables(keys: &[(String, i64)], prefix: &str) -> Vec<(String, String, i64)> {
    let mut out: Vec<(String, String, i64)> = Vec::new();
    for (key, size) in keys {
        let Some(rel) = key.strip_prefix(prefix) else {
            continue;
        };
        let base = rel.rsplit_once('/').map_or(rel, |(_, b)| b);
        let Some(stem) = base.strip_suffix(".csv") else {
            continue;
        };
        if stem.is_empty() {
            continue;
        }
        out.push((stem.to_string(), key.clone(), (size / 32).max(1)));
    }
    out
}

/// Match one requested table against the listing: a stem when unique, or the
/// prefix-relative path WITHOUT the `.csv` extension (always unambiguous).
/// Loud on misses, on ambiguity, and on a `.csv`-suffixed request — the
/// extension would leak into destination naming, where `.` means a schema
/// qualifier.
pub(crate) fn match_table<'a>(
    tables: &'a [(String, String, i64)],
    prefix: &str,
    want: &str,
    origin: &str,
) -> Result<&'a (String, String, i64)> {
    if let Some(bare) = want.strip_suffix(".csv") {
        return Err(Error::InvalidInput(format!(
            "address tables without the .csv extension: use '{bare}'"
        )));
    }
    let hits: Vec<_> = tables
        .iter()
        .filter(|(stem, key, _)| stem == want || rel_no_ext(key, prefix) == want)
        .collect();
    match hits.len() {
        1 => Ok(hits[0]),
        0 => Err(Error::InvalidInput(format!(
            "table '{want}' not found under {origin} (tables: {})",
            tables
                .iter()
                .map(|(s, _, _)| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
        _ => Err(Error::InvalidInput(format!(
            "table name '{want}' is ambiguous under {origin}: {} — use the path \
             relative to the url directory instead (without .csv)",
            hits.iter()
                .map(|(_, k, _)| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The prefix-relative path minus the `.csv` extension — the always-unique
/// table address.
pub(crate) fn rel_no_ext<'a>(key: &'a str, prefix: &str) -> &'a str {
    let rel = key.strip_prefix(prefix).unwrap_or(key);
    rel.strip_suffix(".csv").unwrap_or(rel)
}

/// The CANONICAL table name for one listing entry: the stem when it is unique
/// in the listing, else the relative path without `.csv` (so two `full_data`
/// files in different directories get distinct destination tables instead of
/// blocking each other). A name containing '.' fails loudly — every database
/// sink reads '.' as a schema qualifier, which would silently land
/// `2026.01.csv` in a table named `01`.
pub(crate) fn canonical_name(
    tables: &[(String, String, i64)],
    entry: &(String, String, i64),
    prefix: &str,
) -> Result<String> {
    let (stem, key, _) = entry;
    let unique = tables.iter().filter(|(s, _, _)| s == stem).count() == 1;
    let name = if unique {
        stem.clone()
    } else {
        rel_no_ext(key, prefix).to_string()
    };
    if name.contains('.') {
        return Err(Error::InvalidInput(format!(
            "table name '{name}' (from '{key}') contains '.', which database \
             destinations read as a schema qualifier — rename the file"
        )));
    }
    Ok(name)
}

/// The whole worker body for a CSV object: pump the byte stream through the
/// parser, skip the header row, frame every record for the negotiated wire
/// format, flush by chunk. Short rows pad with NULLs (trailing fields omitted);
/// long rows are refused — silently dropping fields is data loss.
pub(crate) async fn stream_rows<L, S>(
    label: &str,
    mut stream: S,
    plan: &TablePlan,
    lane: &Lane,
    mut loader: L,
    chunk: usize,
) -> Result<u64>
where
    L: Loader,
    S: futures::Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let ncols = plan.cols.len();
    let enc = TextEnc::of(lane.format);
    let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
    enc.open(&mut out);
    let mut pump = CsvPump::new();
    let mut rows_sent: u64 = 0;
    let mut header_skipped = false;
    let mut eof = false;
    while !eof {
        match stream.next().await {
            Some(Ok(bytes)) => pump.feed(&bytes),
            Some(Err(e)) => {
                return Err(loader
                    .abort(Error::Transfer(format!("read '{label}': {e}")))
                    .await)
            }
            None => eof = true,
        }
        while let Some(row) = pump.next_record(eof) {
            if !header_skipped {
                header_skipped = true;
                continue;
            }
            if row.len() > ncols {
                let n = row.len();
                return Err(loader
                    .abort(Error::InvalidInput(format!(
                        "'{label}' record {} (header counted, blank lines not) has \
                         {n} fields but the header has {ncols} — fix the file (short \
                         rows pad with NULLs; a long row is refused, it would drop \
                         data)",
                        rows_sent + 2,
                    )))
                    .await);
            }
            enc.row_start(ncols, &mut out);
            for i in 0..ncols {
                let cell = if i < row.len() { row.field(i) } else { b"" };
                enc.cell(i, cell, &mut out);
            }
            enc.row_end(&mut out);
            rows_sent += 1;
            if out.len() >= chunk {
                let buf = std::mem::replace(&mut out, Vec::with_capacity(chunk + 64 * 1024));
                loader.send(buf).await?;
            }
        }
    }
    enc.close(&mut out);
    if !out.is_empty() {
        loader.send(out).await?;
    }
    let reported = loader.finish().await?;
    Ok(if reported > 0 { reported } else { rows_sent })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(input: &[u8]) -> Vec<Vec<String>> {
        let mut pump = CsvPump::new();
        pump.feed(input);
        let mut out = Vec::new();
        while let Some(r) = pump.next_record(true) {
            out.push(
                (0..r.len())
                    .map(|i| String::from_utf8(r.field(i).to_vec()).unwrap())
                    .collect(),
            );
        }
        out
    }

    #[test]
    fn csv_pump_handles_rfc4180() {
        // Quoted comma, quoted newline, doubled quote, empty field, blank line,
        // no final newline.
        let data = b"a,b,c\n1,\"x,y\",\"l1\nl2\"\n\n2,\"he said \"\"hi\"\"\",\n3,plain,last";
        let r = rows(data);
        assert_eq!(r[0], vec!["a", "b", "c"]);
        assert_eq!(r[1], vec!["1", "x,y", "l1\nl2"]);
        assert_eq!(r[2], vec!["2", "he said \"hi\"", ""]);
        assert_eq!(r[3], vec!["3", "plain", "last"]);
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn csv_pump_survives_any_chunk_split() {
        // Splitting the byte stream at EVERY position must not change the rows
        // — records and even single fields span calls (the resumption bug the
        // first implementation had).
        let data = b"a,b\n\"long\nvalue\",2\nplain,\"q\"\"q\"\n";
        for split in 1..data.len() {
            let mut pump = CsvPump::new();
            pump.feed(&data[..split]);
            let mut got: Vec<Vec<String>> = Vec::new();
            while let Some(r) = pump.next_record(false) {
                got.push(
                    (0..r.len())
                        .map(|i| String::from_utf8(r.field(i).to_vec()).unwrap())
                        .collect(),
                );
            }
            pump.feed(&data[split..]);
            while let Some(r) = pump.next_record(true) {
                got.push(
                    (0..r.len())
                        .map(|i| String::from_utf8(r.field(i).to_vec()).unwrap())
                        .collect(),
                );
            }
            assert_eq!(
                got,
                vec![
                    vec!["a".to_string(), "b".to_string()],
                    vec!["long\nvalue".to_string(), "2".to_string()],
                    vec!["plain".to_string(), "q\"q".to_string()],
                ],
                "split at {split}"
            );
        }
    }

    #[test]
    fn csv_pump_grows_past_tiny_buffers() {
        // A field bigger than the initial output buffer forces OutputFull mid-
        // record; the accumulate-and-grow path must keep the bytes already
        // written.
        let big = "x".repeat(200 * 1024);
        let data = format!("a,b\n\"{big}\",2\n");
        let r = rows(data.as_bytes());
        assert_eq!(r[1][0].len(), big.len());
        assert_eq!(r[1][0], big);
        assert_eq!(r[1][1], "2");
    }

    #[test]
    fn quoted_empty_row_is_delivered_not_dropped() {
        // "" is the RFC-4180 spelling of one empty (= NULL) cell; every other
        // reader returns it — dropping it silently diverges row counts.
        let r = rows(b"name\nalice\n\"\"\nbob\n");
        assert_eq!(r.len(), 4);
        assert_eq!(r[2], vec![""]);
        // A true blank line IS discarded (by csv-core itself).
        assert_eq!(rows(b"name\nalice\n\nbob\n").len(), 3);
    }

    #[test]
    fn csv_tables_naming_and_addressing() {
        let keys = vec![
            ("data/users.csv".to_string(), 3200),
            ("data/2026/orders.csv".to_string(), 64),
            ("data/readme.md".to_string(), 10),
        ];
        let t = csv_tables(&keys, "data/");
        assert_eq!(t[0], ("users".into(), "data/users.csv".into(), 100));
        assert_eq!(t[1], ("orders".into(), "data/2026/orders.csv".into(), 2));
        assert_eq!(t.len(), 2);
        // Unique stems keep the short name; the rel path also addresses them.
        assert_eq!(canonical_name(&t, &t[1], "data/").unwrap(), "orders");
        assert_eq!(
            match_table(&t, "data/", "2026/orders", "x://").unwrap().1,
            "data/2026/orders.csv"
        );

        // Duplicate stems: each gets its rel path as the canonical name, both
        // stay addressable, the bare stem is loudly ambiguous, and .csv-form
        // addressing is redirected (the extension would poison dest naming).
        let dup = csv_tables(
            &[
                ("a/users.csv".to_string(), 32),
                ("b/users.csv".to_string(), 32),
                ("main.csv".to_string(), 32),
            ],
            "",
        );
        assert_eq!(canonical_name(&dup, &dup[0], "").unwrap(), "a/users");
        assert_eq!(canonical_name(&dup, &dup[2], "").unwrap(), "main");
        assert_eq!(match_table(&dup, "", "b/users", "x://").unwrap().1, "b/users.csv");
        let amb = match_table(&dup, "", "users", "x://").unwrap_err().to_string();
        assert!(amb.contains("ambiguous") && amb.contains("a/users.csv"));
        let ext = match_table(&dup, "", "b/users.csv", "x://").unwrap_err().to_string();
        assert!(ext.contains("without the .csv"));
        assert!(match_table(&dup, "", "nope", "x://")
            .unwrap_err()
            .to_string()
            .contains("not found"));

        // Dotted stems would schema-split in the destinations — loud.
        let dotted = csv_tables(&[("2026.01.csv".to_string(), 32)], "");
        let err = canonical_name(&dotted, &dotted[0], "").unwrap_err().to_string();
        assert!(err.contains("schema qualifier"));
    }

    #[test]
    fn text_plan_names_blanks_and_refuses_duplicates() {
        let p = text_plan("github", "x.csv", &["id".into(), " ".into(), "Note".into()]).unwrap();
        assert_eq!(p.cols[1].name, "col_2");
        assert_eq!(p.cols[2].name, "Note");
        assert!(text_plan("github", "x.csv", &["a".into(), "A".into()]).is_err());
    }
}
