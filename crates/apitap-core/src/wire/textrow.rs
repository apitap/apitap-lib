//! Row framing for ALL-TEXT sources (Google Sheets, S3 CSV): the same cells,
//! encoded for whichever wire format the sink negotiated. One implementation so
//! a new text source can't drift from the proven encodings.
//!
//! The shared semantic: an EMPTY cell is NULL — text sources have no separate
//! empty-string representation, and every wired destination follows this rule.

use crate::plan::WireFormat;
use crate::wire::mytsv::tsv_escape;
use crate::wire::pgcopy as pgc;
use crate::wire::rowbinary::varint;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum TextEnc {
    /// Postgres binary COPY (header/tuples/trailer framing).
    PgBinary,
    /// ClickHouse RowBinary, every column `Nullable(String)`.
    RowBinary,
    /// MySQL `LOAD DATA` TSV ([`crate::wire::mytsv`] escapes, `\N` NULLs).
    MyTsv,
}

impl TextEnc {
    pub(crate) fn of(format: WireFormat) -> Self {
        match format {
            WireFormat::PgCopyBinary => Self::PgBinary,
            WireFormat::RowBinary => Self::RowBinary,
            WireFormat::TabSeparated => Self::MyTsv,
        }
    }

    /// Stream prologue, once before the first row.
    pub(crate) fn open(self, out: &mut Vec<u8>) {
        if self == Self::PgBinary {
            pgc::header(out);
        }
    }

    pub(crate) fn row_start(self, ncols: usize, out: &mut Vec<u8>) {
        if self == Self::PgBinary {
            pgc::tuple_start(ncols, out);
        }
    }

    /// Cell `i` of the current row; empty = NULL.
    pub(crate) fn cell(self, i: usize, cell: &[u8], out: &mut Vec<u8>) {
        match self {
            Self::PgBinary => {
                if cell.is_empty() {
                    pgc::null_field(out);
                } else {
                    pgc::field(cell, out);
                }
            }
            // Nullable(String): null flag, then varint+bytes.
            Self::RowBinary => {
                if cell.is_empty() {
                    out.push(1);
                } else {
                    out.push(0);
                    varint(cell.len() as u64, out);
                    out.extend_from_slice(cell);
                }
            }
            // \t between fields, \N NULLs; the row terminator is row_end's.
            Self::MyTsv => {
                if i > 0 {
                    out.push(b'\t');
                }
                if cell.is_empty() {
                    out.extend_from_slice(b"\\N");
                } else {
                    tsv_escape(cell, out);
                }
            }
        }
    }

    pub(crate) fn row_end(self, out: &mut Vec<u8>) {
        if self == Self::MyTsv {
            out.push(b'\n');
        }
    }

    /// Stream epilogue, once after the last row.
    pub(crate) fn close(self, out: &mut Vec<u8>) {
        if self == Self::PgBinary {
            pgc::trailer(out);
        }
    }
}
