//! PG binary COPY → Arrow columnar batch builders (no arrow crate — the
//! buffers are laid out exactly as the Arrow C Data Interface wants them
//! and exported by py-apitap's hand-rolled FFI). Modeled 1:1 on
//! [`crate::wire::bqparquet`]'s bounds-first two-pass tuple walk; the
//! decode arms share `wire::pgcopy`'s helpers (PG epochs, NUMERIC→i128).

use crate::error::{Error, Result};
use crate::plan::Delivered;

/// The v1 Arrow type vocabulary. Everything [`arrow_kind`] can't place
/// falls back to `::text` in the SELECT (and lands here as `Utf8`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArrowKind {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    /// Arrow decimal128 with the DECLARED precision/scale.
    Decimal { p: u8, s: i8 },
    Date32,
    /// timestamptz — micros since Unix epoch, tz "UTC" in the schema.
    TimestampUtc,
    /// timestamp — micros since Unix epoch, no tz.
    TimestampNaive,
    /// text/json/jsonb/uuid(hyphenated)/fallback ::text.
    Utf8,
    /// bytea.
    Binary,
}

/// Map a delivered wire type onto the v1 Arrow vocabulary. `None` means
/// the caller must rewrite the column's SELECT to `::text` (Utf8 lane).
pub fn arrow_kind(d: &Delivered) -> Option<ArrowKind> {
    let _ = d;
    todo!("agent: implement per the parquet gate (bqparquet parquet_col_ok)")
}

/// One sealed column, buffers in Arrow C layout (validity bitmap LSB-first;
/// utf8/binary carry i32 offsets starting at 0).
pub enum FinishedCol {
    I16 { validity: Option<Vec<u8>>, data: Vec<i16> },
    I32 { validity: Option<Vec<u8>>, data: Vec<i32> },
    I64 { validity: Option<Vec<u8>>, data: Vec<i64> },
    F32 { validity: Option<Vec<u8>>, data: Vec<f32> },
    F64 { validity: Option<Vec<u8>>, data: Vec<f64> },
    /// Data is ALSO a bitmap for bool.
    Bool { validity: Option<Vec<u8>>, data: Vec<u8> },
    Dec128 { validity: Option<Vec<u8>>, data: Vec<i128> },
    Utf8 { validity: Option<Vec<u8>>, offsets: Vec<i32>, data: Vec<u8> },
    Bin { validity: Option<Vec<u8>>, offsets: Vec<i32>, data: Vec<u8> },
}

/// One sealed batch: `rows` rows across `cols` (parallel to the schema).
pub struct ArrowBatch {
    pub rows: usize,
    pub cols: Vec<FinishedCol>,
}

/// Per-worker streaming builder: feed raw COPY-binary chunks (one 19-byte
/// header per worker stream, then tuples, 0xFFFF trailer at end — the
/// FrameStrip lane's shape), seal a batch whenever ~`batch_bytes` of column
/// data accumulated.
pub struct BatchBuilder {
    _p: std::marker::PhantomData<()>,
}

impl BatchBuilder {
    pub fn new(kinds: Vec<ArrowKind>, batch_bytes: usize) -> Self {
        let _ = (kinds, batch_bytes);
        todo!("agent")
    }

    /// Consume one raw chunk. Complete tuples decode into the column
    /// builders; a partial tail is buffered for the next chunk (bounds-
    /// first two-pass — a tuple split across chunks never rolls back).
    pub fn push(&mut self, chunk: &[u8]) -> Result<()> {
        let _ = chunk;
        todo!("agent")
    }

    /// A sealed batch, if the size threshold was crossed.
    pub fn take_ready(&mut self) -> Option<ArrowBatch> {
        todo!("agent")
    }

    /// Stream over (trailer seen): the final partial batch, if any rows.
    pub fn finish(&mut self) -> Result<Option<ArrowBatch>> {
        todo!("agent")
    }

    /// Rows decoded so far (for reporting).
    pub fn rows_total(&self) -> u64 {
        todo!("agent")
    }
}

fn _touch(_: &Error) {}
