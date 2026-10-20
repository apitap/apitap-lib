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
            WireFormat::MyTsv => Self::MyTsv,
            // Text sources declare MyTsv, never the Postgres text dialect —
            // can_produce() upstream keeps this arm unreachable.
            WireFormat::TabSeparated => {
                unreachable!("text sources never negotiate the PG text dialect")
            }
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

    /// Cell `i` of the current row; empty = NULL, unless the column is not
    /// `nullable`, where it is the empty string.
    pub(crate) fn cell(self, i: usize, cell: &[u8], nullable: bool, out: &mut Vec<u8>) {
        match self {
            Self::PgBinary => {
                if cell.is_empty() {
                    pgc::null_field(out);
                } else {
                    pgc::field(cell, out);
                }
            }
            // Nullable(String): null flag, then varint+bytes. A column the
            // sink declared non-Nullable (an order_by or engine column, the
            // cursor) has no flag byte: framed with one, ClickHouse read the
            // flag as the length and the stream shifted for the rest of the
            // insert — a failed load, or garbage rows (deep review D32).
            Self::RowBinary if !nullable => {
                varint(cell.len() as u64, out);
                out.extend_from_slice(cell);
            }
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

#[cfg(test)]
mod tests {
    use super::TextEnc;

    /// D32: a column the sink made non-Nullable carries no flag byte; an
    /// empty cell there is the empty string, not a NULL.
    #[test]
    fn a_non_nullable_cell_has_no_null_flag() {
        let enc = TextEnc::RowBinary;
        let mut out = Vec::new();
        enc.cell(0, b"id7", false, &mut out);
        enc.cell(1, b"", false, &mut out);
        enc.cell(2, b"v", true, &mut out);
        enc.cell(3, b"", true, &mut out);
        assert_eq!(
            out,
            [&[3u8][..], b"id7", &[0], &[0, 1], b"v", &[1]].concat()
        );
    }
}
