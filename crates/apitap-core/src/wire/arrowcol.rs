//! PG binary COPY → Arrow columnar batch builders (no arrow crate — the
//! buffers are laid out exactly as the Arrow C Data Interface wants them
//! and exported by py-apitap's hand-rolled FFI). Modeled 1:1 on
//! [`crate::wire::bqparquet`]'s bounds-first two-pass tuple walk; the
//! decode arms share `wire::pgcopy`'s helpers (PG epochs, NUMERIC→i128).

use crate::error::{Error, Result};
use crate::plan::Delivered;
use crate::wire::pgcopy::{numeric_to_scaled_i128, PG_EPOCH_DAYS, PG_EPOCH_MICROS};

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
    Some(match d {
        Delivered::Int { bytes: 1 | 2, .. } => ArrowKind::Int16,
        Delivered::Int { bytes: 4, .. } => ArrowKind::Int32,
        Delivered::Int { .. } => ArrowKind::Int64,
        Delivered::Float32 => ArrowKind::Float32,
        Delivered::Float64 => ArrowKind::Float64,
        Delivered::Bool => ArrowKind::Bool,
        // Same gate as the parquet lane (bqparquet::parquet_col_ok):
        // unconstrained NUMERIC has no exact width, >38 digits exceed i128.
        // Scale clamps to precision like the parquet schema does.
        Delivered::Decimal { p, s } => {
            if *p == 0 || *p > 38 {
                return None;
            }
            ArrowKind::Decimal {
                p: *p as u8,
                s: (*s).min(*p) as i8,
            }
        }
        Delivered::Date => ArrowKind::Date32,
        Delivered::DateTime { utc: true } => ArrowKind::TimestampUtc,
        Delivered::DateTime { utc: false } => ArrowKind::TimestampNaive,
        Delivered::Text => ArrowKind::Utf8,
        // jsonb's 0x01 version byte and uuid's raw 16 bytes must not reach
        // the Utf8 passthrough — the planner rewrites both to ::text.
        Delivered::Json | Delivered::Uuid => return None,
        Delivered::Bytes => ArrowKind::Binary,
    })
}

/// One sealed column, buffers in Arrow C layout (validity bitmap LSB-first;
/// utf8/binary carry i32 offsets starting at 0).
#[derive(Debug, PartialEq)]
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
#[derive(Debug, PartialEq)]
pub struct ArrowBatch {
    pub rows: usize,
    pub cols: Vec<FinishedCol>,
}

// ============================================================================
// Column builders
// ============================================================================

fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: unexpected {what}"))
}

/// Append the bit for `row` (LSB-first). Rows arrive in order, so a fresh
/// byte is needed exactly when `row % 8 == 0`.
fn push_bit(bm: &mut Vec<u8>, row: usize, on: bool) {
    if row % 8 == 0 {
        bm.push(0);
    }
    if on {
        *bm.last_mut().expect("byte pushed above") |= 1 << (row % 8);
    }
}

/// Lazy validity: stays `None` until the first NULL (sealed as `None` =
/// all-valid, so a NULL-free column costs no bitmap at all).
fn mark(v: &mut Option<Vec<u8>>, row: usize, valid: bool) {
    match v {
        Some(bm) => push_bit(bm, row, valid),
        None if valid => {}
        None => {
            // First NULL: backfill `row` set bits, then this row's zero.
            let mut bm = vec![0xFF; row / 8];
            if row % 8 != 0 {
                bm.push((1u8 << (row % 8)) - 1);
            }
            push_bit(&mut bm, row, false);
            *v = Some(bm);
        }
    }
}

enum ColB {
    I16 { v: Option<Vec<u8>>, d: Vec<i16> },
    I32 { v: Option<Vec<u8>>, d: Vec<i32> },
    I64 { v: Option<Vec<u8>>, d: Vec<i64> },
    F32 { v: Option<Vec<u8>>, d: Vec<f32> },
    F64 { v: Option<Vec<u8>>, d: Vec<f64> },
    /// Data bitmap, same LSB-first order as validity.
    Bool { v: Option<Vec<u8>>, d: Vec<u8> },
    Dec { v: Option<Vec<u8>>, d: Vec<i128>, s: u32 },
    /// Days since Unix epoch (seals as I32).
    D32 { v: Option<Vec<u8>>, d: Vec<i32> },
    /// Micros since Unix epoch (seals as I64; utc/naive decode identically).
    Ts { v: Option<Vec<u8>>, d: Vec<i64> },
    Utf8 { v: Option<Vec<u8>>, off: Vec<i32>, d: Vec<u8> },
    Bin { v: Option<Vec<u8>>, off: Vec<i32>, d: Vec<u8> },
}

/// Fresh offsets vec: Arrow wants a leading 0 plus one entry per row.
fn offsets0(entries: usize) -> Vec<i32> {
    let mut o = Vec::with_capacity(entries);
    o.push(0);
    o
}

/// Adaptive re-reserve for the next batch's buffer: the batch that just
/// sealed ended at `last_bytes`, so reserve that plus 1/8 slack, clamped
/// to [4 KiB, 32 MiB]. A threshold seal fires when the columns together
/// hold ~batch_bytes, so sum(last_bytes) ≈ batch_bytes and the total
/// reservation stays near 1.13x batch_bytes — but skewed per column, so
/// a varlen column that eats most of the batch no longer pays geometric
/// growth reallocs against a uniform batch_bytes/ncols split.
fn adaptive(last_bytes: usize) -> usize {
    (last_bytes + last_bytes / 8).clamp(4096, 32 << 20)
}

impl ColB {
    /// `cap` = per-column byte budget (batch_bytes / ncols) for the FIRST
    /// batch only — there is no size history yet, so the budget splits
    /// uniformly. Every later batch re-reserves adaptively at seal.
    fn new(k: &ArrowKind, cap: usize) -> Self {
        match k {
            ArrowKind::Int16 => ColB::I16 { v: None, d: Vec::with_capacity(cap / 2) },
            ArrowKind::Int32 => ColB::I32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::Int64 => ColB::I64 { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Float32 => ColB::F32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::Float64 => ColB::F64 { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Bool => ColB::Bool { v: None, d: Vec::with_capacity(cap / 8) },
            ArrowKind::Decimal { s, .. } => ColB::Dec {
                v: None,
                d: Vec::with_capacity(cap / 16),
                s: (*s).max(0) as u32,
            },
            ArrowKind::Date32 => ColB::D32 { v: None, d: Vec::with_capacity(cap / 4) },
            ArrowKind::TimestampUtc | ArrowKind::TimestampNaive => {
                ColB::Ts { v: None, d: Vec::with_capacity(cap / 8) }
            }
            ArrowKind::Utf8 => ColB::Utf8 {
                v: None,
                off: offsets0(cap / 16 + 1),
                d: Vec::with_capacity(cap),
            },
            ArrowKind::Binary => ColB::Bin {
                v: None,
                off: offsets0(cap / 16 + 1),
                d: Vec::with_capacity(cap),
            },
        }
    }



    /// Resident bytes (data + offsets), for the batch seal gate.
    fn bytes(&self) -> usize {
        match self {
            ColB::I16 { d, .. } => d.len() * 2,
            ColB::I32 { d, .. } | ColB::D32 { d, .. } => d.len() * 4,
            ColB::I64 { d, .. } | ColB::Ts { d, .. } => d.len() * 8,
            ColB::F32 { d, .. } => d.len() * 4,
            ColB::F64 { d, .. } => d.len() * 8,
            ColB::Bool { d, .. } => d.len(),
            ColB::Dec { d, .. } => d.len() * 16,
            ColB::Utf8 { off, d, .. } | ColB::Bin { off, d, .. } => d.len() + off.len() * 4,
        }
    }

    /// Move the buffers out as-is and re-arm ADAPTIVELY: each fresh Vec
    /// pre-reserves from the sealed batch's FINAL byte size (+1/8 slack,
    /// clamped — see [`adaptive`]). Validity stays lazy (`None` until the
    /// first NULL), so it carries no pre-reserve to adapt.
    fn seal(&mut self) -> FinishedCol {
        use std::mem::replace;
        match self {
            ColB::I16 { v, d } => {
                let cap = adaptive(d.len() * 2) / 2;
                FinishedCol::I16 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::I32 { v, d } | ColB::D32 { v, d } => {
                let cap = adaptive(d.len() * 4) / 4;
                FinishedCol::I32 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::I64 { v, d } | ColB::Ts { v, d } => {
                let cap = adaptive(d.len() * 8) / 8;
                FinishedCol::I64 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::F32 { v, d } => {
                let cap = adaptive(d.len() * 4) / 4;
                FinishedCol::F32 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::F64 { v, d } => {
                let cap = adaptive(d.len() * 8) / 8;
                FinishedCol::F64 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::Bool { v, d } => {
                let cap = adaptive(d.len());
                FinishedCol::Bool { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::Dec { v, d, .. } => {
                let cap = adaptive(d.len() * 16) / 16;
                FinishedCol::Dec128 { validity: v.take(), data: replace(d, Vec::with_capacity(cap)) }
            }
            ColB::Utf8 { v, off, d } => {
                let dcap = adaptive(d.len());
                let ocap = adaptive(off.len() * 4) / 4;
                FinishedCol::Utf8 {
                    validity: v.take(),
                    offsets: replace(off, offsets0(ocap)),
                    data: replace(d, Vec::with_capacity(dcap)),
                }
            }
            ColB::Bin { v, off, d } => {
                let dcap = adaptive(d.len());
                let ocap = adaptive(off.len() * 4) / 4;
                FinishedCol::Bin {
                    validity: v.take(),
                    offsets: replace(off, offsets0(ocap)),
                    data: replace(d, Vec::with_capacity(dcap)),
                }
            }
        }
    }
}

// ============================================================================
// Streaming builder: COPY-binary chunks in → sealed ArrowBatches out
// ============================================================================

/// Per-worker streaming builder: feed raw COPY-binary bytes as a sequence
/// of SPANS (19-byte header, tuples, 0xFFFF trailer — then possibly the
/// next span's header). One synthetic-stream worker feed is simply the
/// one-span case. Seals a batch whenever ~`batch_bytes` accumulated.
pub struct BatchBuilder {
    cols: Vec<ColB>,
    batch_bytes: usize,
    /// Rows in the current (unsealed) batch.
    rows: usize,
    rows_sealed: u64,
    // -- COPY framing state (bounds-first; see bqparquet's module docs).
    // `buf` holds ONLY the unconsumed tail (header-in-progress or one
    // straddling tuple) — chunks are otherwise walked in place.
    buf: Vec<u8>,
    /// Inside a span (header consumed, trailer not yet seen).
    in_span: bool,
    // -- micro-batch transpose staging: (offset, len) per field for up to
    // ST_K complete tuples, COLUMN-major. The decode then runs per COLUMN:
    // one variant dispatch per column per micro-batch instead of per field
    // per row — the per-field jump table alone profiled ~10% of a 0.5-core
    // read, and the tight per-column loops vectorize.
    st_off: Vec<u32>,
    st_len: Vec<i32>,
    st_rows: usize,
    /// Framed mode ([`Self::push_framed`]): payload bytes remaining of the
    /// current CopyData message — 0 means the next bytes are a 5-byte
    /// message header.
    fr_left: usize,
}

/// One decoded cell for the row-major append surface
/// ([`BatchBuilder::append_cell`]). Lifetimes borrow the source's wire
/// buffer — varlen bytes copy exactly once, into the column buffer.
pub enum CellVal<'a> {
    Null,
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Dec128(i128),
    /// Days since the Unix epoch (Date32 column).
    DateDays(i32),
    /// Micros since the Unix epoch (either timestamp column).
    TsMicros(i64),
    Str(&'a [u8]),
    Bin(&'a [u8]),
}

/// Why [`BatchBuilder::push_framed`] returned.
pub enum FramedPush {
    /// Consumed up to an element that needs more bytes — REFILL the window
    /// (append more data after the unconsumed tail) and call again. The
    /// window owner must GROW on zero progress; a fixed re-lease of the
    /// same bytes would spin (the fill_buf lease bug this replaced).
    NeedMore,
    /// A non-CopyData message header starts at the consumed offset — the
    /// wire layer handles it (CommandComplete, ReadyForQuery, …).
    Control,
    /// A tuple straddles CopyData messages (bytes not contiguous): the
    /// caller falls back to the copying plane, resuming INSIDE the current
    /// payload with this many bytes left. Never observed from Postgres
    /// (one row per message), but the protocol does not forbid it.
    Straddle(usize),
}

/// Tuples staged per micro-batch. 15 cols × 64 × 8 B ≈ 7.7 KB of staging —
/// comfortably L1-resident next to the wire window it points into.
///
/// SWEPT 2026-08-08 and left alone: 64 vs 128 on the 10M × 15-col read leg
/// @0.5cpu/256MB, 3 interleaved rounds with the installed binary md5-verified
/// per leg, medians 13.6 s / 7.3 s CPU (64) vs 13.5 s / 7.2 s (128) — 0.7%,
/// under the lane wall, and round 3 flipped sign. A first sweep did show a
/// 3/3 win for 128; it was an artifact of a harness whose wheel swap silently
/// no-op'd (pip rejects a renamed wheel), so all three legs ran ONE binary.
const ST_K: usize = 64;

/// Why the FRAMED staging stopped.
enum StageStopF {
    /// ST_K tuples staged — decode and stage again.
    Full,
    /// Window exhausted mid-element — refill (append) and call again.
    NeedMore,
    /// A non-CopyData header at the returned offset.
    Control,
    /// Tuple bytes are split across CopyData messages — fall back.
    Straddle,
    /// The span trailer marker is next (not consumed).
    Trailer,
}

/// Why `stage_tuples` stopped.
enum StageStop {
    /// ST_K tuples staged — decode and stage again.
    Full,
    /// Data ran out mid-tuple — decode what staged, keep the tail.
    Incomplete,
    /// The next item is the span trailer (not consumed).
    Trailer,
}

impl BatchBuilder {
    pub fn new(kinds: Vec<ArrowKind>, batch_bytes: usize) -> Self {
        // First-batch reserve only (no size history yet): a uniform
        // batch_bytes/ncols split, clamped — a huge batch_bytes (the
        // materialize fast path never seals mid-stream) must not
        // pre-allocate huge Vecs. Each seal then re-reserves per column
        // from that column's actual final sizes (see ColB::seal).
        let cap = (batch_bytes / kinds.len().max(1)).min(32 << 20);
        Self {
            cols: kinds.iter().map(|k| ColB::new(k, cap)).collect(),
            batch_bytes,
            rows: 0,
            rows_sealed: 0,
            buf: Vec::with_capacity(64 << 10),
            in_span: false,
            st_off: vec![0; kinds.len() * ST_K],
            st_len: vec![0; kinds.len() * ST_K],
            st_rows: 0,
            fr_left: 0,
        }
    }

    /// Append one already-decoded cell of the CURRENT row (columns in
    /// order, then [`Self::row_done`]) — the row-major surface for sources
    /// that decode their own wire (MySQL binary protocol). Varlen values
    /// land in the column buffer in ONE copy; there is no intermediate
    /// COPY stream, no bounds walk, no staging. The variant must match the
    /// column's kind — encoders and kinds derive from the same plan, so a
    /// mismatch is a construction bug and panics loudly.
    #[inline]
    pub fn append_cell(&mut self, c: usize, val: CellVal) {
        let r = self.rows;
        match (&mut self.cols[c], val) {
            (ColB::I16 { v, d }, CellVal::I16(x)) => { d.push(x); mark(v, r, true); }
            (ColB::I32 { v, d }, CellVal::I32(x)) => { d.push(x); mark(v, r, true); }
            (ColB::I64 { v, d }, CellVal::I64(x)) => { d.push(x); mark(v, r, true); }
            (ColB::F32 { v, d }, CellVal::F32(x)) => { d.push(x); mark(v, r, true); }
            (ColB::F64 { v, d }, CellVal::F64(x)) => { d.push(x); mark(v, r, true); }
            (ColB::Bool { v, d }, CellVal::Bool(x)) => { push_bit(d, r, x); mark(v, r, true); }
            (ColB::Dec { v, d, .. }, CellVal::Dec128(x)) => { d.push(x); mark(v, r, true); }
            (ColB::D32 { v, d }, CellVal::DateDays(x)) => { d.push(x); mark(v, r, true); }
            (ColB::Ts { v, d }, CellVal::TsMicros(x)) => { d.push(x); mark(v, r, true); }
            (ColB::Utf8 { v, off, d }, CellVal::Str(b)) => {
                d.extend_from_slice(b);
                off.push(d.len() as i32);
                mark(v, r, true);
            }
            (ColB::Bin { v, off, d }, CellVal::Bin(b)) => {
                d.extend_from_slice(b);
                off.push(d.len() as i32);
                mark(v, r, true);
            }
            (col, CellVal::Null) => match col {
                ColB::I16 { v, d } => { d.push(0); mark(v, r, false); }
                ColB::I32 { v, d } | ColB::D32 { v, d } => { d.push(0); mark(v, r, false); }
                ColB::I64 { v, d } | ColB::Ts { v, d } => { d.push(0); mark(v, r, false); }
                ColB::F32 { v, d } => { d.push(0.0); mark(v, r, false); }
                ColB::F64 { v, d } => { d.push(0.0); mark(v, r, false); }
                ColB::Bool { v, d } => { push_bit(d, r, false); mark(v, r, false); }
                ColB::Dec { v, d, .. } => { d.push(0); mark(v, r, false); }
                ColB::Utf8 { v, off, d } | ColB::Bin { v, off, d } => {
                    off.push(d.len() as i32);
                    mark(v, r, false);
                }
            },
            _ => unreachable!("append_cell: cell/kind mismatch (planner bug)"),
        }
    }

    /// Close the current row (every column appended exactly once). Seal
    /// checks stay with the caller ([`Self::take_ready`] at its own
    /// cadence — `acc_bytes` sums every column, too hot for every row).
    #[inline]
    pub fn row_done(&mut self) {
        self.rows += 1;
    }

    /// Framed-window entry: `win` is the RAW wire window — CopyData headers
    /// still embedded. Stages and decodes tuples in place, skipping the
    /// 5-byte 'd' headers inline (`fr_left` counts down each payload; one
    /// payload is one row from Postgres, so headers land on tuple
    /// boundaries). The buffered-tail path is NOT used here: an element
    /// that runs past the window edge is left unconsumed and the caller
    /// refills (appending — see [`FramedPush::NeedMore`]).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn push_framed(&mut self, win: &[u8]) -> Result<(usize, FramedPush)> {
        debug_assert!(self.buf.is_empty(), "framed and buffered modes don't mix");
        let mut pos = 0usize;
        loop {
            // Frame layer: position ourselves inside a 'd' payload.
            if self.fr_left == 0 {
                if win.len() - pos < 5 {
                    return Ok((pos, FramedPush::NeedMore));
                }
                if win[pos] != b'd' {
                    return Ok((pos, FramedPush::Control));
                }
                let len =
                    u32::from_be_bytes(win[pos + 1..pos + 5].try_into().unwrap()) as usize;
                if len < 4 {
                    return Err(Error::Transfer("copy_out: bad message length".into()));
                }
                pos += 5;
                self.fr_left = len - 4;
                continue;
            }
            let payload_end = (pos + self.fr_left).min(win.len());
            // Span layer: header/trailer arrive as their own small payloads.
            if !self.in_span {
                let b = &win[pos..payload_end];
                if b.len() < 19 {
                    if pos + self.fr_left > win.len() {
                        return Ok((pos, FramedPush::NeedMore));
                    }
                    let left = self.fr_left;
                    self.fr_left = 0;
                    return Ok((pos, FramedPush::Straddle(left)));
                }
                if &b[..11] != b"PGCOPY\n\xff\r\n\0" {
                    return Err(Error::Transfer("pg binary COPY: bad header".into()));
                }
                let ext = u32::from_be_bytes(b[15..19].try_into().unwrap()) as usize;
                if b.len() < 19 + ext {
                    if pos + self.fr_left > win.len() {
                        return Ok((pos, FramedPush::NeedMore));
                    }
                    let left = self.fr_left;
                    self.fr_left = 0;
                    return Ok((pos, FramedPush::Straddle(left)));
                }
                pos += 19 + ext;
                self.fr_left -= 19 + ext;
                self.in_span = true;
                continue;
            }
            // Tuple layer: stage up to ST_K rows ACROSS payloads (the
            // staging hops 'd' headers itself — one payload is one row).
            let (end, stop) = self.stage_framed(win, pos)?;
            if self.st_rows > 0 {
                self.decode_staged(win)?;
            }
            pos = end;
            match stop {
                StageStopF::Full => {}
                StageStopF::NeedMore => return Ok((pos, FramedPush::NeedMore)),
                StageStopF::Control => return Ok((pos, FramedPush::Control)),
                StageStopF::Straddle => {
                    let left = self.fr_left;
                    self.fr_left = 0;
                    return Ok((pos, FramedPush::Straddle(left)));
                }
                StageStopF::Trailer => {
                    if self.fr_left < 2 {
                        let left = self.fr_left;
                        self.fr_left = 0;
                        return Ok((pos, FramedPush::Straddle(left)));
                    }
                    pos += 2;
                    self.fr_left -= 2;
                    self.in_span = false;
                }
            }
        }
    }

    /// Consume one raw chunk. Complete tuples decode into the column
    /// builders; a partial tail is buffered for the next chunk (bounds-
    /// first two-pass — a tuple split across chunks never rolls back).
    ///
    /// The chunk is walked IN PLACE: `self.buf` only ever holds the header
    /// (until complete) or one straddling tuple's bytes. Buffering whole
    /// chunks instead measured ~1.4 GB of extra memcpy per 10M-row stream.
    /// Invariant at every return: `buf` holds exactly the unconsumed tail.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn push(&mut self, chunk: &[u8]) -> Result<()> {
        let mut start = 0usize;
        // Top-up: while a straddling header/tuple sits in `buf`, feed it
        // bites (doubling, so a huge single value stays O(n)) until it
        // completes, then fall through to the in-place walk.
        let mut bite = 16 << 10;
        while !self.buf.is_empty() {
            if start >= chunk.len() {
                return Ok(());
            }
            let take = (chunk.len() - start).min(bite);
            bite = (bite * 2).min(64 << 20);
            self.buf.extend_from_slice(&chunk[start..start + take]);
            start += take;
            // O(1) swap frees `self` for the builders while we read the buffer.
            let buf = std::mem::take(&mut self.buf);
            let r = self.walk(&buf);
            self.buf = buf;
            let consumed = r?;
            if consumed == self.buf.len() {
                self.buf.clear();
                break;
            }
            // Still incomplete: compact the tail to the front and top up.
            let len = self.buf.len();
            self.buf.copy_within(consumed.., 0);
            self.buf.truncate(len - consumed);
        }

        if start < chunk.len() {
            let consumed = start + self.walk(&chunk[start..])?;
            if consumed < chunk.len() {
                self.buf.extend_from_slice(&chunk[consumed..]);
            }
        }
        Ok(())
    }

    /// Walk headers, tuples and trailers in sequence until the data runs
    /// out mid-element; returns bytes consumed. A trailer closes the span
    /// and the next bytes (if any) must open the next span's header.
    /// Tuples move in micro-batches: stage up to [`ST_K`] complete tuples'
    /// field descriptors, then decode them COLUMN by column.
    fn walk(&mut self, data: &[u8]) -> Result<usize> {
        let mut pos = 0usize;
        loop {
            if !self.in_span {
                let b = &data[pos..];
                if b.len() < 19 {
                    return Ok(pos);
                }
                if &b[..11] != b"PGCOPY\n\xff\r\n\0" {
                    return Err(Error::Transfer("pg binary COPY: bad header".into()));
                }
                let ext = u32::from_be_bytes(b[15..19].try_into().unwrap()) as usize;
                if b.len() < 19 + ext {
                    return Ok(pos);
                }
                pos += 19 + ext;
                self.in_span = true;
            }
            let (end, stop) = self.stage_tuples(data, pos)?;
            if self.st_rows > 0 {
                self.decode_staged(data)?;
            }
            pos = end;
            match stop {
                StageStop::Full => {}
                StageStop::Incomplete => return Ok(pos),
                StageStop::Trailer => {
                    pos += 2;
                    self.in_span = false;
                }
            }
        }
    }

    /// Bounds-walk up to [`ST_K`] complete tuples starting at `start`,
    /// recording each field's (absolute offset, len) column-major into the
    /// staging arrays. Nothing is decoded here — a tuple that turns out
    /// incomplete is simply not staged, so there is never anything to
    /// roll back. Returns (end of staged bytes, why we stopped).
    fn stage_tuples(&mut self, data: &[u8], start: usize) -> Result<(usize, StageStop)> {
        let ncols = self.cols.len();
        let mut pos = start;
        self.st_rows = 0;
        while self.st_rows < ST_K {
            if data.len() - pos < 2 {
                return Ok((pos, StageStop::Incomplete));
            }
            let nc = i16::from_be_bytes(data[pos..pos + 2].try_into().unwrap());
            if nc == -1 {
                return Ok((pos, StageStop::Trailer));
            }
            if nc as usize != ncols {
                return Err(Error::Transfer(format!(
                    "pg binary COPY: tuple has {nc} fields, expected {ncols}"
                )));
            }
            let k = self.st_rows;
            let mut o = pos + 2;
            for c in 0..ncols {
                if data.len() - o < 4 {
                    return Ok((pos, StageStop::Incomplete));
                }
                let len = i32::from_be_bytes(data[o..o + 4].try_into().unwrap());
                o += 4;
                if len < -1 {
                    return Err(Error::Transfer(format!(
                        "pg binary COPY: corrupt field length {len}"
                    )));
                }
                if len == -1 {
                    self.st_len[c * ST_K + k] = -1;
                } else {
                    if data.len() - o < len as usize {
                        return Ok((pos, StageStop::Incomplete));
                    }
                    self.st_off[c * ST_K + k] = o as u32;
                    self.st_len[c * ST_K + k] = len;
                    o += len as usize;
                }
            }
            pos = o;
            self.st_rows += 1;
        }
        Ok((pos, StageStop::Full))
    }

    /// Framed staging: like [`Self::stage_tuples`], but hops the 5-byte
    /// CopyData headers INLINE at tuple boundaries (updating `fr_left`), so
    /// a micro-batch still collects up to [`ST_K`] rows even though
    /// Postgres ships one row per message — bounding the batch by a single
    /// payload measured 15.4s vs 13.1s (per-column dispatch per ROW).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn stage_framed(&mut self, win: &[u8], start: usize) -> Result<(usize, StageStopF)> {
        let ncols = self.cols.len();
        let mut pos = start;
        self.st_rows = 0;
        while self.st_rows < ST_K {
            if self.fr_left == 0 {
                if win.len() - pos < 5 {
                    return Ok((pos, StageStopF::NeedMore));
                }
                if win[pos] != b'd' {
                    return Ok((pos, StageStopF::Control));
                }
                let len =
                    u32::from_be_bytes(win[pos + 1..pos + 5].try_into().unwrap()) as usize;
                if len < 4 {
                    return Err(Error::Transfer("copy_out: bad message length".into()));
                }
                pos += 5;
                self.fr_left = len - 4;
                continue;
            }
            let pe = (pos + self.fr_left).min(win.len());
            if pe - pos < 2 {
                return Ok((pos, if pos + self.fr_left > win.len() {
                    StageStopF::NeedMore
                } else {
                    StageStopF::Straddle
                }));
            }
            let nc = i16::from_be_bytes(win[pos..pos + 2].try_into().unwrap());
            if nc == -1 {
                return Ok((pos, StageStopF::Trailer));
            }
            if nc as usize != ncols {
                return Err(Error::Transfer(format!(
                    "pg binary COPY: tuple has {nc} fields, expected {ncols}"
                )));
            }
            let k = self.st_rows;
            let mut o = pos + 2;
            for c in 0..ncols {
                if pe - o < 4 {
                    return Ok((pos, if pos + self.fr_left > win.len() {
                        StageStopF::NeedMore
                    } else {
                        StageStopF::Straddle
                    }));
                }
                let len = i32::from_be_bytes(win[o..o + 4].try_into().unwrap());
                o += 4;
                if len < -1 {
                    return Err(Error::Transfer(format!(
                        "pg binary COPY: corrupt field length {len}"
                    )));
                }
                if len == -1 {
                    self.st_len[c * ST_K + k] = -1;
                } else {
                    if pe - o < len as usize {
                        return Ok((pos, if pos + self.fr_left > win.len() {
                            StageStopF::NeedMore
                        } else {
                            StageStopF::Straddle
                        }));
                    }
                    self.st_off[c * ST_K + k] = o as u32;
                    self.st_len[c * ST_K + k] = len;
                    o += len as usize;
                }
            }
            self.fr_left -= o - pos;
            pos = o;
            self.st_rows += 1;
        }
        Ok((pos, StageStopF::Full))
    }

    /// Decode the staged micro-batch COLUMN by column: one variant dispatch
    /// per column, then a tight loop over its staged values. The unchecked
    /// reads are justified exactly as the old two-pass was: `stage_tuples`
    /// proved every (offset, len) against `data.len()`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn decode_staged(&mut self, data: &[u8]) -> Result<()> {
        let n = self.st_rows;
        let row0 = self.rows;
        for (c, col) in self.cols.iter_mut().enumerate() {
            let offs = &self.st_off[c * ST_K..c * ST_K + n];
            let lens = &self.st_len[c * ST_K..c * ST_K + n];
            // SAFETY (whole match): every non-NULL (offs[k], lens[k]) was
            // bounds-proven against `data` by stage_tuples this micro-batch.
            match col {
                ColB::I16 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        if len != 2 {
                            return Err(bad(&format!("int2 width {len}")));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        d.push(i16::from_be_bytes(unsafe { p.cast::<[u8; 2]>().read() }));
                        mark(v, row0 + k, true);
                    }
                }
                ColB::I32 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        if len != 4 {
                            return Err(bad(&format!("int4 width {len}")));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        d.push(i32::from_be_bytes(unsafe { p.cast::<[u8; 4]>().read() }));
                        mark(v, row0 + k, true);
                    }
                }
                ColB::I64 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        if len != 8 {
                            return Err(bad(&format!("int8 width {len}")));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        d.push(i64::from_be_bytes(unsafe { p.cast::<[u8; 8]>().read() }));
                        mark(v, row0 + k, true);
                    }
                }
                ColB::F32 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0.0);
                            continue;
                        }
                        if len != 4 {
                            return Err(bad("float4"));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        d.push(f32::from_be_bytes(unsafe { p.cast::<[u8; 4]>().read() }));
                        mark(v, row0 + k, true);
                    }
                }
                ColB::F64 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0.0);
                            continue;
                        }
                        if len != 8 {
                            return Err(bad("float8"));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        d.push(f64::from_be_bytes(unsafe { p.cast::<[u8; 8]>().read() }));
                        mark(v, row0 + k, true);
                    }
                }
                ColB::Bool { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            push_bit(d, row0 + k, false);
                            continue;
                        }
                        let on = len > 0
                            && unsafe { *data.as_ptr().add(offs[k] as usize) } != 0;
                        push_bit(d, row0 + k, on);
                        mark(v, row0 + k, true);
                    }
                }
                ColB::Dec { v, d, s } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        let f = unsafe {
                            std::slice::from_raw_parts(
                                data.as_ptr().add(offs[k] as usize),
                                len as usize,
                            )
                        };
                        d.push(numeric_to_scaled_i128(f, *s)?);
                        mark(v, row0 + k, true);
                    }
                }
                ColB::D32 { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        if len != 4 {
                            return Err(bad("date"));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        let days =
                            i32::from_be_bytes(unsafe { p.cast::<[u8; 4]>().read() });
                        if days == i32::MAX || days == i32::MIN {
                            return Err(Error::Transfer(
                                "date 'infinity' has no Arrow representation — cast or \
                                 filter it in a source view"
                                    .into(),
                            ));
                        }
                        d.push(
                            days.checked_add(PG_EPOCH_DAYS)
                                .ok_or_else(|| bad("date range"))?,
                        );
                        mark(v, row0 + k, true);
                    }
                }
                ColB::Ts { v, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            d.push(0);
                            continue;
                        }
                        if len != 8 {
                            return Err(bad("timestamp"));
                        }
                        let p = unsafe { data.as_ptr().add(offs[k] as usize) };
                        let us = i64::from_be_bytes(unsafe { p.cast::<[u8; 8]>().read() });
                        if us == i64::MAX || us == i64::MIN {
                            return Err(Error::Transfer(
                                "timestamp 'infinity' has no Arrow representation — cast \
                                 or filter it in a source view"
                                    .into(),
                            ));
                        }
                        d.push(
                            us.checked_add(PG_EPOCH_MICROS)
                                .ok_or_else(|| bad("timestamp range"))?,
                        );
                        mark(v, row0 + k, true);
                    }
                }
                // Utf8 fields arrive as final UTF-8 bytes — jsonb/uuid were
                // rewritten to ::text upstream (arrow_kind returns None there).
                ColB::Utf8 { v, off, d } | ColB::Bin { v, off, d } => {
                    for k in 0..n {
                        let len = lens[k];
                        if len == -1 {
                            mark(v, row0 + k, false);
                            off.push(*off.last().expect("offsets seeded with 0"));
                            continue;
                        }
                        let f = unsafe {
                            std::slice::from_raw_parts(
                                data.as_ptr().add(offs[k] as usize),
                                len as usize,
                            )
                        };
                        d.extend_from_slice(f);
                        off.push(i32::try_from(d.len()).map_err(|_| {
                            bad("varlen column past i32 offsets in one batch")
                        })?);
                        mark(v, row0 + k, true);
                    }
                }
            }
        }
        self.rows += n;
        self.st_rows = 0;
        Ok(())
    }


    fn acc_bytes(&self) -> usize {
        self.cols.iter().map(ColB::bytes).sum()
    }

    /// Every varlen column must still address inside i32 offsets. The staged
    /// path checks per push because it has a `Result` to hand back; the
    /// direct-Arrow `append_cell` is `#[inline]` on the per-CELL hot path and
    /// returns `()`, so its check lives here. Detection power is identical —
    /// offsets are monotonic and the data buffer only grows, so if any push
    /// wrapped, `d.len()` is past `i32::MAX` at seal too — and the hot path
    /// pays nothing.
    ///
    /// Without it, `off.push(d.len() as i32)` wraps past 2 GiB in one column:
    /// negative offsets (the consumer builds a slice from them UNCHECKED —
    /// polars trusts FFI offsets) below 4 GiB, and silently WRONG strings
    /// above it. Reachable on the default MySQL lane, because `to_polars()`
    /// asks for one unbounded batch per worker.
    fn check_offsets(&self) -> Result<()> {
        self.check_offsets_to(i32::MAX as usize)
    }

    /// `check_offsets` with the ceiling injected, so a test can prove the
    /// detection without allocating 2 GiB (the real-size proof is the
    /// `#[ignore]`d test below).
    fn check_offsets_to(&self, limit: usize) -> Result<()> {
        for c in &self.cols {
            let n = match c {
                ColB::Utf8 { d, .. } | ColB::Bin { d, .. } => d.len(),
                _ => continue,
            };
            if n > limit {
                return Err(bad("varlen column past i32 offsets in one batch"));
            }
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<ArrowBatch> {
        self.check_offsets()?;
        let rows = self.rows;
        self.rows = 0;
        self.rows_sealed += rows as u64;
        Ok(ArrowBatch {
            rows,
            cols: self.cols.iter_mut().map(ColB::seal).collect(),
        })
    }

    /// A sealed batch, if the size threshold was crossed. Push decodes whole
    /// tuples only, so any moment between push calls is a tuple boundary.
    pub fn take_ready(&mut self) -> Result<Option<ArrowBatch>> {
        if self.rows == 0 || self.acc_bytes() < self.batch_bytes {
            return Ok(None);
        }
        self.seal().map(Some)
    }

    /// Stream over (every span closed): the final partial batch, if any
    /// rows. Mid-span or mid-element leftovers mean truncation — loud.
    pub fn finish(&mut self) -> Result<Option<ArrowBatch>> {
        if self.in_span || !self.buf.is_empty() {
            return Err(Error::Transfer(
                "pg binary COPY: stream ended without the trailer".into(),
            ));
        }
        if self.rows == 0 {
            return Ok(None);
        }
        self.seal().map(Some)
    }

    /// Rows decoded so far (for reporting).
    pub fn rows_total(&self) -> u64 {
        self.rows_sealed + self.rows as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pgcopy;

    /// GOLDEN cross-check: the row-major append surface must produce the
    /// byte-identical batch the COPY-stream decode produces for the same
    /// logical rows — validity backfill, empty strings, IEEE specials,
    /// epoch rebasing, everything.
    #[test]
    fn append_surface_matches_the_copy_decode() {
        let kinds = vec![
            ArrowKind::Int16,
            ArrowKind::Int32,
            ArrowKind::Int64,
            ArrowKind::Float32,
            ArrowKind::Float64,
            ArrowKind::Bool,
            ArrowKind::Decimal { p: 18, s: 4 },
            ArrowKind::Date32,
            ArrowKind::TimestampUtc,
            ArrowKind::Utf8,
            ArrowKind::Binary,
        ];
        let n = kinds.len();

        // Path A: the COPY stream.
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        pgcopy::tuple_start(n, &mut s);
        pgcopy::field(&(-7i16).to_be_bytes(), &mut s);
        pgcopy::field(&123_456i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-9_876_543_210i64).to_be_bytes(), &mut s);
        pgcopy::field(&1.5f32.to_be_bytes(), &mut s);
        pgcopy::field(&(-2.25f64).to_be_bytes(), &mut s);
        pgcopy::field(&[1], &mut s);
        pgcopy::numeric_field_from_str("1234.5678", &mut s).unwrap();
        pgcopy::field(&0i32.to_be_bytes(), &mut s); // 2000-01-01
        pgcopy::field(&1_500_000i64.to_be_bytes(), &mut s);
        pgcopy::field(b"hello", &mut s);
        pgcopy::field(&[0xDE, 0xAD], &mut s);
        pgcopy::tuple_start(n, &mut s);
        for _ in 0..n {
            pgcopy::null_field(&mut s);
        }
        pgcopy::tuple_start(n, &mut s);
        pgcopy::field(&32_000i16.to_be_bytes(), &mut s);
        pgcopy::field(&(-1i32).to_be_bytes(), &mut s);
        pgcopy::field(&42i64.to_be_bytes(), &mut s);
        pgcopy::field(&f32::NEG_INFINITY.to_be_bytes(), &mut s);
        pgcopy::field(&f64::NAN.to_be_bytes(), &mut s);
        pgcopy::field(&[], &mut s); // empty field = false
        pgcopy::numeric_field_from_str("-0.5000", &mut s).unwrap();
        pgcopy::field(&365i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-1_000_000i64).to_be_bytes(), &mut s);
        pgcopy::field(b"", &mut s);
        pgcopy::field(&[1, 2, 3], &mut s);
        pgcopy::trailer(&mut s);
        let mut a = BatchBuilder::new(kinds.clone(), usize::MAX >> 1);
        a.push(&s).unwrap();
        let batch_a = a.finish().unwrap().expect("rows");

        // Path B: the same logical rows through append_cell.
        let mut b = BatchBuilder::new(kinds, usize::MAX >> 1);
        let ed = pgcopy::PG_EPOCH_DAYS; // unix days of 2000-01-01
        let em = pgcopy::PG_EPOCH_MICROS;
        b.append_cell(0, CellVal::I16(-7));
        b.append_cell(1, CellVal::I32(123_456));
        b.append_cell(2, CellVal::I64(-9_876_543_210));
        b.append_cell(3, CellVal::F32(1.5));
        b.append_cell(4, CellVal::F64(-2.25));
        b.append_cell(5, CellVal::Bool(true));
        b.append_cell(6, CellVal::Dec128(12_345_678));
        b.append_cell(7, CellVal::DateDays(ed));
        b.append_cell(8, CellVal::TsMicros(em + 1_500_000));
        b.append_cell(9, CellVal::Str(b"hello"));
        b.append_cell(10, CellVal::Bin(&[0xDE, 0xAD]));
        b.row_done();
        for c in 0..11 {
            b.append_cell(c, CellVal::Null);
        }
        b.row_done();
        b.append_cell(0, CellVal::I16(32_000));
        b.append_cell(1, CellVal::I32(-1));
        b.append_cell(2, CellVal::I64(42));
        b.append_cell(3, CellVal::F32(f32::NEG_INFINITY));
        b.append_cell(4, CellVal::F64(f64::NAN));
        b.append_cell(5, CellVal::Bool(false));
        b.append_cell(6, CellVal::Dec128(-5_000));
        b.append_cell(7, CellVal::DateDays(ed + 365));
        b.append_cell(8, CellVal::TsMicros(em - 1_000_000));
        b.append_cell(9, CellVal::Str(b""));
        b.append_cell(10, CellVal::Bin(&[1, 2, 3]));
        b.row_done();
        let batch_b = b.finish().unwrap().expect("rows");

        assert_eq!(batch_a.rows, 3);
        assert_eq!(batch_b.rows, 3);
        // NaN != NaN under PartialEq — compare the f64 column by bits.
        let (fa, fb) = match (&batch_a.cols[4], &batch_b.cols[4]) {
            (
                FinishedCol::F64 { validity: va, data: da },
                FinishedCol::F64 { validity: vb, data: db },
            ) => {
                assert_eq!(va, vb);
                (
                    da.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    db.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                )
            }
            _ => unreachable!(),
        };
        assert_eq!(fa, fb);
        for c in [0, 1, 2, 3, 5, 6, 7, 8, 9, 10] {
            assert_eq!(batch_a.cols[c], batch_b.cols[c], "col {c}");
        }
    }

    #[test]
    fn kind_map_mirrors_the_parquet_gate() {
        use Delivered as D;
        let int = |bytes| D::Int { bytes, unsigned: false };
        assert_eq!(arrow_kind(&int(2)), Some(ArrowKind::Int16));
        assert_eq!(arrow_kind(&int(4)), Some(ArrowKind::Int32));
        assert_eq!(arrow_kind(&int(8)), Some(ArrowKind::Int64));
        assert_eq!(
            arrow_kind(&D::Decimal { p: 18, s: 4 }),
            Some(ArrowKind::Decimal { p: 18, s: 4 })
        );
        assert_eq!(arrow_kind(&D::Decimal { p: 0, s: 0 }), None); // unconstrained
        assert_eq!(arrow_kind(&D::Decimal { p: 50, s: 2 }), None); // > i128 digits
        assert_eq!(arrow_kind(&D::Uuid), None); // ::text — hyphenated on the wire
        assert_eq!(arrow_kind(&D::Json), None); // ::text — jsonb version byte
        assert_eq!(arrow_kind(&D::Text), Some(ArrowKind::Utf8));
        assert_eq!(arrow_kind(&D::Bytes), Some(ArrowKind::Binary));
        assert_eq!(arrow_kind(&D::Date), Some(ArrowKind::Date32));
        assert_eq!(arrow_kind(&D::DateTime { utc: true }), Some(ArrowKind::TimestampUtc));
        assert_eq!(arrow_kind(&D::DateTime { utc: false }), Some(ArrowKind::TimestampNaive));
    }

    #[test]
    fn decodes_every_kind_byte_by_byte_into_exact_buffers() {
        let kinds = vec![
            ArrowKind::Int16,
            ArrowKind::Int32,
            ArrowKind::Int64,
            ArrowKind::Float32,
            ArrowKind::Float64,
            ArrowKind::Bool,
            ArrowKind::Decimal { p: 18, s: 4 },
            ArrowKind::Date32,
            ArrowKind::TimestampUtc,
            ArrowKind::TimestampNaive,
            ArrowKind::Utf8,
            ArrowKind::Binary,
        ];
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        // Row 0: every column valid.
        pgcopy::tuple_start(12, &mut s);
        pgcopy::field(&(-7i16).to_be_bytes(), &mut s);
        pgcopy::field(&123_456i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-9_876_543_210i64).to_be_bytes(), &mut s);
        pgcopy::field(&1.5f32.to_be_bytes(), &mut s);
        pgcopy::field(&(-2.25f64).to_be_bytes(), &mut s);
        pgcopy::field(&[1], &mut s);
        pgcopy::numeric_field_from_str("1234.5678", &mut s).unwrap();
        pgcopy::field(&0i32.to_be_bytes(), &mut s); // 2000-01-01
        pgcopy::field(&0i64.to_be_bytes(), &mut s); // PG epoch
        pgcopy::field(&1_500_000i64.to_be_bytes(), &mut s); // +1.5 s
        pgcopy::field(b"hello", &mut s);
        pgcopy::field(&[0xDE, 0xAD], &mut s);
        // Row 1: all NULL.
        pgcopy::tuple_start(12, &mut s);
        for _ in 0..12 {
            pgcopy::null_field(&mut s);
        }
        // Row 2: empty string, empty-field bool, IEEE specials, negatives.
        pgcopy::tuple_start(12, &mut s);
        pgcopy::field(&32_000i16.to_be_bytes(), &mut s);
        pgcopy::field(&(-1i32).to_be_bytes(), &mut s);
        pgcopy::field(&42i64.to_be_bytes(), &mut s);
        pgcopy::field(&f32::NEG_INFINITY.to_be_bytes(), &mut s);
        pgcopy::field(&f64::NAN.to_be_bytes(), &mut s);
        pgcopy::field(&[], &mut s); // empty field = false
        pgcopy::numeric_field_from_str("-0.5000", &mut s).unwrap();
        pgcopy::field(&365i32.to_be_bytes(), &mut s);
        pgcopy::field(&(-1_000_000i64).to_be_bytes(), &mut s);
        pgcopy::field(&1i64.to_be_bytes(), &mut s);
        pgcopy::field(b"", &mut s);
        pgcopy::field(&[1, 2, 3], &mut s);
        pgcopy::trailer(&mut s);

        // Feed byte-by-byte: chunk boundaries anywhere must be safe.
        let mut b = BatchBuilder::new(kinds, 1 << 20);
        for byte in &s {
            b.push(std::slice::from_ref(byte)).unwrap();
        }
        assert!(b.take_ready().unwrap().is_none()); // threshold never crossed
        let batch = b.finish().unwrap().expect("rows pending");
        assert_eq!(batch.rows, 3);
        assert_eq!(b.rows_total(), 3);

        // Every column: rows 0 and 2 valid, row 1 NULL → 0b101.
        let valid = Some(&[0b101u8][..]);
        match &batch.cols[0] {
            FinishedCol::I16 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[-7, 0, 32_000]);
            }
            _ => panic!("col 0 layout"),
        }
        match &batch.cols[1] {
            FinishedCol::I32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[123_456, 0, -1]);
            }
            _ => panic!("col 1 layout"),
        }
        match &batch.cols[2] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[-9_876_543_210, 0, 42]);
            }
            _ => panic!("col 2 layout"),
        }
        match &batch.cols[3] {
            FinishedCol::F32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[1.5, 0.0, f32::NEG_INFINITY]);
            }
            _ => panic!("col 3 layout"),
        }
        match &batch.cols[4] {
            FinishedCol::F64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data[0], -2.25);
                assert_eq!(data[1], 0.0);
                assert!(data[2].is_nan());
            }
            _ => panic!("col 4 layout"),
        }
        match &batch.cols[5] {
            FinishedCol::Bool { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[0b001]); // true, null→0, false
            }
            _ => panic!("col 5 layout"),
        }
        match &batch.cols[6] {
            FinishedCol::Dec128 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[12_345_678, 0, -5_000]);
            }
            _ => panic!("col 6 layout"),
        }
        match &batch.cols[7] {
            FinishedCol::I32 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[10_957, 0, 11_322]); // + PG_EPOCH_DAYS
            }
            _ => panic!("col 7 layout"),
        }
        match &batch.cols[8] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[946_684_800_000_000, 0, 946_684_799_000_000]);
            }
            _ => panic!("col 8 layout"),
        }
        match &batch.cols[9] {
            FinishedCol::I64 { validity, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(data, &[946_684_801_500_000, 0, 946_684_800_000_001]);
            }
            _ => panic!("col 9 layout"),
        }
        match &batch.cols[10] {
            FinishedCol::Utf8 { validity, offsets, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(offsets, &[0, 5, 5, 5]); // null and "" both repeat
                assert_eq!(data, b"hello");
            }
            _ => panic!("col 10 layout"),
        }
        match &batch.cols[11] {
            FinishedCol::Bin { validity, offsets, data } => {
                assert_eq!(validity.as_deref(), valid);
                assert_eq!(offsets, &[0, 2, 2, 5]);
                assert_eq!(data, &[0xDE, 0xAD, 1, 2, 3]);
            }
            _ => panic!("col 11 layout"),
        }
    }

    #[test]
    fn seals_at_the_byte_threshold_and_counts_add_up() {
        // 20 bytes/row (8 i64 + 8 text + 4 offset) + the 4-byte offsets
        // base → a 64-byte threshold crosses at exactly 3 rows.
        let mut b = BatchBuilder::new(vec![ArrowKind::Int64, ArrowKind::Utf8], 64);
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        b.push(&s).unwrap();
        let mut sealed = Vec::new();
        for i in 0..10i64 {
            let mut t = Vec::new();
            pgcopy::tuple_start(2, &mut t);
            pgcopy::field(&i.to_be_bytes(), &mut t);
            pgcopy::field(b"12345678", &mut t);
            b.push(&t).unwrap();
            if let Some(batch) = b.take_ready().unwrap() {
                assert!(b.take_ready().unwrap().is_none()); // counter reset at seal
                sealed.push(batch);
            }
        }
        let mut t = Vec::new();
        pgcopy::trailer(&mut t);
        b.push(&t).unwrap();
        let tail = b.finish().unwrap().expect("a partial remains");
        assert_eq!(sealed.iter().map(|x| x.rows).collect::<Vec<_>>(), vec![3, 3, 3]);
        assert_eq!(tail.rows, 1);
        assert_eq!(b.rows_total(), 10);
        // No NULLs anywhere → validity None; offsets re-seed at 0 per batch.
        match (&sealed[1].cols[0], &sealed[1].cols[1]) {
            (
                FinishedCol::I64 { validity, data },
                FinishedCol::Utf8 { validity: v2, offsets, .. },
            ) => {
                assert!(validity.is_none() && v2.is_none());
                assert_eq!(data, &[3, 4, 5]);
                assert_eq!(offsets, &[0, 8, 16, 24]);
            }
            _ => panic!("layout"),
        }
    }

    #[test]
    fn adaptive_reseal_keeps_content_exact_across_many_batches() {
        // Varlen-heavy: the Utf8 column dwarfs the uniform batch_bytes/2
        // split, so every seal re-reserves from that column's history.
        // Only correctness is observable (capacity is not), so walk every
        // sealed batch and check each row's string and int round-tripped.
        let make = |i: i64| format!("row-{i}-{}", "x".repeat(200 + (i as usize * 37) % 57));
        let mut b = BatchBuilder::new(vec![ArrowKind::Utf8, ArrowKind::Int64], 4096);
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        b.push(&s).unwrap();
        let n = 64i64;
        let mut sealed = Vec::new();
        for i in 0..n {
            let mut t = Vec::new();
            pgcopy::tuple_start(2, &mut t);
            pgcopy::field(make(i).as_bytes(), &mut t);
            pgcopy::field(&i.to_be_bytes(), &mut t);
            b.push(&t).unwrap();
            if let Some(batch) = b.take_ready().unwrap() {
                sealed.push(batch);
            }
        }
        let mut t = Vec::new();
        pgcopy::trailer(&mut t);
        b.push(&t).unwrap();
        if let Some(tail) = b.finish().unwrap() {
            sealed.push(tail);
        }
        assert!(sealed.len() >= 3, "want >= 3 seals, got {}", sealed.len());
        assert_eq!(sealed.iter().map(|x| x.rows).sum::<usize>(), n as usize);
        assert_eq!(b.rows_total(), n as u64);
        let mut i = 0i64;
        for batch in &sealed {
            assert!(batch.rows > 0);
            match (&batch.cols[0], &batch.cols[1]) {
                (
                    FinishedCol::Utf8 { validity, offsets, data },
                    FinishedCol::I64 { validity: v2, data: ints },
                ) => {
                    assert!(validity.is_none() && v2.is_none());
                    assert_eq!(offsets.len(), batch.rows + 1);
                    assert_eq!(offsets[0], 0); // re-seeded at every seal
                    assert_eq!(*offsets.last().unwrap() as usize, data.len());
                    assert_eq!(ints.len(), batch.rows);
                    for r in 0..batch.rows {
                        let (a, z) = (offsets[r] as usize, offsets[r + 1] as usize);
                        assert_eq!(&data[a..z], make(i).as_bytes(), "row {i}");
                        assert_eq!(ints[r], i);
                        i += 1;
                    }
                }
                _ => panic!("layout"),
            }
        }
        assert_eq!(i, n);
    }

    #[test]
    fn date_and_timestamp_infinity_error_loudly() {
        for (kind, field) in [
            (ArrowKind::Date32, i32::MAX.to_be_bytes().to_vec()),
            (ArrowKind::Date32, i32::MIN.to_be_bytes().to_vec()),
            (ArrowKind::TimestampUtc, i64::MAX.to_be_bytes().to_vec()),
            (ArrowKind::TimestampNaive, i64::MIN.to_be_bytes().to_vec()),
        ] {
            let mut s = Vec::new();
            pgcopy::header(&mut s);
            pgcopy::tuple_start(1, &mut s);
            pgcopy::field(&field, &mut s);
            let err = BatchBuilder::new(vec![kind], 1 << 20).push(&s).unwrap_err();
            assert!(err.to_string().contains("infinity"), "{err}");
        }
    }

    #[test]
    fn trailer_gates_finish_and_rejects_trailing_bytes() {
        let mut s = Vec::new();
        pgcopy::header(&mut s);
        pgcopy::tuple_start(1, &mut s);
        pgcopy::field(&7i16.to_be_bytes(), &mut s);
        // No trailer yet = truncated stream.
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&s).unwrap();
        assert!(b.finish().is_err());
        // Trailer then finish = the partial batch.
        let mut with_trailer = s.clone();
        pgcopy::trailer(&mut with_trailer);
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&with_trailer).unwrap();
        let batch = b.finish().unwrap().expect("partial after trailer");
        assert_eq!(batch.rows, 1);
        assert_eq!(b.rows_total(), 1);
        // Bytes after the trailer must be the NEXT span's header: a short
        // junk tail buffers and turns loud at finish() (truncation), and 19
        // bytes of non-header junk are loud at push (bad header).
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        b.push(&with_trailer).unwrap();
        b.push(&[0]).unwrap();
        assert!(b.finish().is_err());
        let mut junk_tail = with_trailer.clone();
        junk_tail.extend_from_slice(&[0u8; 19]);
        let mut b = BatchBuilder::new(vec![ArrowKind::Int16], 1 << 20);
        assert!(b.push(&junk_tail).is_err());
    }

    /// Wrap `payload` as one CopyData message ('d' + len + payload).
    fn dmsg(payload: &[u8], out: &mut Vec<u8>) {
        out.push(b'd');
        out.extend(((payload.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(payload);
    }

    #[test]
    fn framed_owned_window_decodes_and_stops_at_control() {
        // The wire shape the framed plane sees: span header, one message
        // per ROW, trailer, then a control message ('C'). The window GROWS
        // on refill (appends) — the driver mimics co_refill, including the
        // tiny-grow case that deadlocked the fill_buf lease design.
        let mut hdr = Vec::new();
        pgcopy::header(&mut hdr);
        let mut wire = Vec::new();
        dmsg(&hdr, &mut wire);
        for i in 0..200i64 {
            let mut t = Vec::new();
            pgcopy::tuple_start(2, &mut t);
            pgcopy::field(&i.to_be_bytes(), &mut t);
            pgcopy::field(format!("row-{i}").as_bytes(), &mut t);
            dmsg(&t, &mut wire);
        }
        let mut tr = Vec::new();
        pgcopy::trailer(&mut tr);
        dmsg(&tr, &mut wire);
        let control_at = wire.len();
        wire.extend_from_slice(b"CxxxxRUBBISH"); // 'C' header starts here

        for grow in [3usize, 17, 1024, wire.len()] {
            let mut b = BatchBuilder::new(vec![ArrowKind::Int64, ArrowKind::Utf8], 1 << 20);
            let mut lo = 0usize; // consumed
            let mut hi = 0usize; // window end — grows like a refill
            let mut spins = 0usize;
            let stopped_at = loop {
                if hi < wire.len() {
                    hi = (hi + grow).min(wire.len());
                }
                let (consumed, stop) = b.push_framed(&wire[lo..hi]).unwrap();
                lo += consumed;
                match stop {
                    FramedPush::NeedMore => {
                        // Progress = consumed bytes OR a grown window;
                        // both stalled would be the deadlock we killed.
                        if consumed == 0 && hi == wire.len() {
                            spins += 1;
                            assert!(spins < 3, "zero progress at grow={grow}");
                        }
                    }
                    FramedPush::Control => break lo,
                    FramedPush::Straddle(_) => panic!("no straddles in this stream"),
                }
            };
            assert_eq!(stopped_at, control_at, "grow={grow}");
            let batch = b.finish().unwrap().expect("rows");
            assert_eq!(batch.rows, 200, "grow={grow}");
            match (&batch.cols[0], &batch.cols[1]) {
                (
                    FinishedCol::I64 { data, .. },
                    FinishedCol::Utf8 { offsets, data: d, .. },
                ) => {
                    assert_eq!(data[0], 0);
                    assert_eq!(data[199], 199);
                    let (a, z) = (offsets[199] as usize, offsets[200] as usize);
                    assert_eq!(&d[a..z], b"row-199");
                }
                _ => panic!("layout"),
            }
        }
    }

    #[test]
    fn multi_span_streams_concatenate_rows() {
        // The FrameRaw lane: several spans, each with its own header and
        // trailer, fed at pathological chunk sizes — rows just continue.
        let mut spans = Vec::new();
        for base in [0i64, 100, 200] {
            let mut s = Vec::new();
            pgcopy::header(&mut s);
            for i in 0..3i64 {
                pgcopy::tuple_start(1, &mut s);
                pgcopy::field(&(base + i).to_be_bytes(), &mut s);
            }
            pgcopy::trailer(&mut s);
            spans.extend_from_slice(&s);
        }
        for chunk_size in [1usize, 7, spans.len()] {
            let mut b = BatchBuilder::new(vec![ArrowKind::Int64], 1 << 20);
            for c in spans.chunks(chunk_size) {
                b.push(c).unwrap();
            }
            let batch = b.finish().unwrap().expect("rows");
            assert_eq!(batch.rows, 9, "chunk_size={chunk_size}");
            match &batch.cols[0] {
                FinishedCol::I64 { validity, data } => {
                    assert!(validity.is_none());
                    assert_eq!(data, &[0, 1, 2, 100, 101, 102, 200, 201, 202]);
                }
                _ => panic!("layout"),
            }
        }
    }

    /// A varlen column past i32 offsets must ERROR, never seal. The
    /// direct-Arrow lane (the MySQL default) writes `d.len() as i32`
    /// unchecked on the per-cell hot path, so without the seal-time guard a
    /// 2 GiB text column wraps: negative offsets below 4 GiB (the consumer
    /// builds its slice from them unchecked) and silently WRONG strings
    /// above it. `to_polars()` asks for one unbounded batch per worker, so
    /// nothing else caps it.
    #[test]
    fn varlen_past_i32_offsets_errors_instead_of_wrapping() {
        let mut b = BatchBuilder::new(vec![ArrowKind::Int64, ArrowKind::Utf8], usize::MAX >> 1);
        b.append_cell(0, CellVal::I64(1));
        b.append_cell(1, CellVal::Str(b"0123456789"));
        b.row_done();
        // Under the ceiling: seals normally.
        assert!(b.check_offsets_to(10).is_ok());
        // Past it: loud, and the same message the staged pg path uses.
        let e = b.check_offsets_to(9).expect_err("must reject");
        assert!(
            e.to_string().contains("past i32 offsets"),
            "unexpected error: {e}"
        );
        // The real guard is not tripped by an ordinary batch.
        assert!(b.finish().unwrap().is_some());
    }

    /// The real-size proof: 2 GiB in one Utf8 column. Allocates ~2 GiB, so it
    /// is opt-in — run with `cargo test -- --ignored varlen_past_i32_real`.
    #[test]
    #[ignore]
    fn varlen_past_i32_real_size_errors() {
        let mut b = BatchBuilder::new(vec![ArrowKind::Utf8], usize::MAX >> 1);
        let chunk = vec![b'x'; 1 << 20];
        for _ in 0..2049 {
            b.append_cell(0, CellVal::Str(&chunk));
            b.row_done();
        }
        let e = b.finish().expect_err("2 GiB of text must not seal");
        assert!(e.to_string().contains("past i32 offsets"), "{e}");
    }
}
