//! Postgres binary COPY → Parquet (SNAPPY), for the BigQuery lane.
//!
//! Why this exists: the CSV lane renders every value to text in Postgres,
//! re-escapes it here, and gzips ~monomorphic text; BigQuery then re-parses
//! it. Parquet skips all three: values arrive BINARY from `COPY (FORMAT
//! binary)`, land in typed column chunks (SNAPPY compresses at several
//! hundred MB/s/core vs gzip's ~100), and BigQuery ingests Parquet on its
//! fastest path. No `arrow` dependency — the `parquet` crate's low-level
//! column writer is driven directly.
//!
//! Framing is a bounds-first two-pass: walk one tuple's length prefixes to
//! prove it is complete, THEN decode fields into the builders — so a tuple
//! split across chunk boundaries never needs columnar rollback. (The CH
//! RowBinary transcoder keeps its tuned single-pass; this module shares its
//! epoch constants and exact NUMERIC decoder instead of its emit loop.)

use crate::error::{Error, Result};
use crate::plan::Delivered;
use crate::sink::PipeResidency;
use crate::wire::pgcopy::{numeric_to_scaled_i128, PG_EPOCH_DAYS, PG_EPOCH_MICROS};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
    FloatType, Int64Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;
use std::io::Write;
use std::sync::{Arc, Mutex};

/// Encoded bytes a parquet loader holds before shipping a part / resumable
/// chunk. S3 keeps its `>= MIN_PART` assert; GCS and BigQuery assert the
/// `UPLOAD_ALIGN` multiple. One symbol, so a loader cannot charge one price
/// and drain at another.
pub(crate) const SEND_THRESHOLD: usize = 8 << 20;
/// The frame buffer `push` starts with and drains above; one chunk of consumed
/// COPY prefix is dead weight between calls, not part of the resident builders.
pub(crate) const FRAME_BUF: usize = 1 << 20;
/// One open data page + dictionary page. parquet-rs's defaults are 1 MiB each,
/// and the writer properties here do not override them.
pub(crate) const PAGE_TRANSIENT: usize = 2 << 20;
/// Builders during one row group (≤ 1 rg + one row) plus compressed pages
/// landing in `out` before the part ships (≤ 1 rg), priced with E1's measured
/// margin: at 2 the fitted (2 MiB, rg4, 2 pipes) plan peaked 134 MB in a
/// 128 MB cage (model 126), so a capped tier got a pipe the row group's own
/// transient could not afford. 3 sends the 128 MiB cell to its measured-safe
/// one-pipe plan (75 MB peak). E1's failure text names this knob: raise it and
/// re-derive T2/T3.
pub(crate) const PER_ROW_GROUP: u64 = 3;
/// `push` copies the input into `buf` while the worker's own `Vec<u8>` argument
/// stays alive until `send` returns.
pub(crate) const LOADER_CHUNKS: u64 = 2;

/// A row-group size the planner can choose. There is no integer constructor:
/// every value is a rung the planner priced, or the CDC window size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowGroup {
    Mib24,
    Mib8,
    Mib4,
    #[cfg(test)]
    Test(usize),
}

impl RowGroup {
    /// Top rung = 0.56.0's ROW_GROUP_BYTES. 8 MiB lets 4 pipes fit 256 MiB/2
    /// cores and 2 merge pipes fit 256 MiB/0.5 CPU. 4 MiB (floor) lets 2 pipes
    /// fit 128 MiB (2×43+40 = 126). Below 4 MiB, per-row-group metadata and
    /// page count start to cost readers.
    pub(crate) const LADDER: [RowGroup; 3] = [RowGroup::Mib24, RowGroup::Mib8, RowGroup::Mib4];

    pub(crate) const fn bytes(self) -> usize {
        match self {
            Self::Mib24 => 24 << 20,
            Self::Mib8 => 8 << 20,
            Self::Mib4 => 4 << 20,
            #[cfg(test)]
            Self::Test(n) => n,
        }
    }

    /// CDC window files are bounded by the drain's byte budget, not by pipes.
    pub(crate) const fn cdc_window() -> Self {
        Self::Mib24
    }
}

/// The only spelling of a parquet lane's per-pipe residency: the 8 MiB part
/// buffer, a 1 MiB frame buffer and a 2 MiB page — plus one more part buffer
/// and page for an Iceberg merge's companion key file.
pub(crate) fn parquet_residency(key_companion: bool) -> PipeResidency {
    let companion = if key_companion {
        SEND_THRESHOLD + PAGE_TRANSIENT
    } else {
        0
    };
    PipeResidency {
        fixed: (SEND_THRESHOLD + FRAME_BUF + PAGE_TRANSIENT + companion) as u64,
        per_row_group: PER_ROW_GROUP,
        chunks: LOADER_CHUNKS,
    }
}

/// Merge-key type gate, written once. `new_ext` and the Iceberg CDC delete
/// file both ask here, so the two spellings cannot drift.
pub(crate) fn merge_key_ok(d: &Delivered) -> Result<()> {
    match d {
        Delivered::Int { .. } | Delivered::Uuid | Delivered::Text => Ok(()),
        other => Err(Error::InvalidInput(format!(
            "merge key column has type {other:?} — supported merge \
             key types on this destination: integer, text, uuid"
        ))),
    }
}

/// Postgres binary-COPY udts this lane can decode. Everything else must be
/// cast in a source view — better a loud, early error than a garbled column.
/// Column-level go/no-go for the parquet lane: the udt must have a known
/// binary layout AND numerics must carry an exact ≤38-digit declaration
/// (unconstrained NUMERIC rides as Float64 whose PG bytes are digit groups,
/// and >38 digits exceed i128 — both fall back to the text lane).
pub(crate) fn parquet_col_ok(udt: &str, precision: Option<i32>) -> bool {
    if !parquet_decodable(udt) {
        return false;
    }
    !matches!(udt, "numeric" | "decimal")
        || matches!(precision, Some(p) if (1..=38).contains(&p))
}

pub(crate) fn parquet_decodable(udt: &str) -> bool {
    matches!(
        udt,
        "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "bool"
            | "date"
            | "timestamp"
            | "timestamptz"
            | "uuid"
            | "json"
            | "jsonb"
            | "text"
            | "varchar"
            | "bpchar"
            | "name"
            // MySQL DATA_TYPE vocabulary — the delivered types are the same
            // standard set (the MySQL reader encodes typed PgCopyBinary);
            // only the udt SPELLINGS differ. binary/blob stay excluded.
            | "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "bigint"
            | "float"
            | "double"
            | "decimal"
            | "datetime"
            | "char"
            | "tinytext"
            | "mediumtext"
            | "longtext"
            | "enum"
            | "set"
            | "year"
            | "time"
    )
}

// ============================================================================
// Column builders
// ============================================================================

enum ColBuf {
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    /// Scaled two's-complement decimals, FIXED_LEN_BYTE_ARRAY(16).
    Dec {
        vals: Vec<FixedLenByteArray>,
        scale: u32,
    },
    /// Days since Unix epoch (logical DATE rides INT64 physical? No — INT32;
    /// stored here as i64 and narrowed at write).
    Date(Vec<i64>),
    /// Micros since Unix epoch.
    Ts(Vec<i64>),
    Bytes(Vec<ByteArray>),
}

impl ColBuf {
    fn new(d: &Delivered) -> Self {
        match d {
            Delivered::Int { .. } => ColBuf::I64(Vec::new()),
            Delivered::Float32 => ColBuf::F32(Vec::new()),
            Delivered::Float64 => ColBuf::F64(Vec::new()),
            Delivered::Bool => ColBuf::Bool(Vec::new()),
            Delivered::Decimal { p, s } => {
                // MUST mirror parquet_field's clamping — a value scaled to a
                // different exponent than the declared scale reads wrong.
                let precision = if *p == 0 || *p > 38 { 38 } else { *p as u32 };
                ColBuf::Dec {
                    vals: Vec::new(),
                    scale: (*s as u32).min(precision),
                }
            }
            Delivered::Date => ColBuf::Date(Vec::new()),
            Delivered::DateTime { .. } => ColBuf::Ts(Vec::new()),
            Delivered::Uuid | Delivered::Json | Delivered::Text | Delivered::Bytes => {
                ColBuf::Bytes(Vec::new())
            }
        }
    }

    /// RESIDENT bytes, for row-group sizing — ByteArray/FLBA hold a struct
    /// (~32 B) plus a heap allocation with allocator quantum; undercounting
    /// here is how a 24 MiB gate turns into a 256 MB OOM at 4 pipes.
    /// `#[cfg(test)]`: the live path carries a running counter, and this
    /// re-sum is its oracle (T4b).
    #[cfg(test)]
    fn bytes(&self) -> usize {
        match self {
            ColBuf::I64(v) => v.len() * 8,
            ColBuf::F32(v) => v.len() * 4,
            ColBuf::F64(v) => v.len() * 8,
            ColBuf::Bool(v) => v.len(),
            ColBuf::Dec { vals, .. } => vals.len() * 48,
            ColBuf::Date(v) | ColBuf::Ts(v) => v.len() * 8,
            ColBuf::Bytes(v) => v.iter().map(|b| 48 + b.len()).sum(),
        }
    }

    fn clear(&mut self) {
        match self {
            ColBuf::I64(v) => v.clear(),
            ColBuf::F32(v) => v.clear(),
            ColBuf::F64(v) => v.clear(),
            ColBuf::Bool(v) => v.clear(),
            ColBuf::Dec { vals, .. } => vals.clear(),
            ColBuf::Date(v) | ColBuf::Ts(v) => v.clear(),
            ColBuf::Bytes(v) => v.clear(),
        }
    }

    /// Decode one non-NULL Postgres binary field into this builder, returning
    /// the RESIDENT bytes that push added — the same unit costs `bytes()`
    /// sums. Undercounting here is how a 24 MiB gate turns into a 256 MB OOM
    /// at 4 pipes.
    fn push_pg(&mut self, f: &[u8], d: &Delivered) -> Result<usize> {
        Ok(match self {
            ColBuf::I64(v) => {
                v.push(match f.len() {
                    2 => i16::from_be_bytes(f.try_into().unwrap()) as i64,
                    4 => i32::from_be_bytes(f.try_into().unwrap()) as i64,
                    8 => i64::from_be_bytes(f.try_into().unwrap()),
                    n => return Err(bad(&format!("int width {n}"))),
                });
                8
            }
            ColBuf::F32(v) => {
                v.push(f32::from_be_bytes(f.try_into().map_err(|_| bad("float4"))?));
                4
            }
            ColBuf::F64(v) => {
                v.push(f64::from_be_bytes(f.try_into().map_err(|_| bad("float8"))?));
                8
            }
            ColBuf::Bool(v) => {
                v.push(f.first().copied().unwrap_or(0) != 0);
                1
            }
            ColBuf::Dec { vals, scale } => {
                let x = numeric_to_scaled_i128(f, *scale)?;
                vals.push(FixedLenByteArray::from(x.to_be_bytes().to_vec()));
                48
            }
            ColBuf::Date(v) => {
                let days = i32::from_be_bytes(f.try_into().map_err(|_| bad("date"))?);
                if days == i32::MAX || days == i32::MIN {
                    return Err(Error::Transfer(
                        "date 'infinity' has no BigQuery representation — cast or \
                         filter it in a source view"
                            .into(),
                    ));
                }
                v.push((days + PG_EPOCH_DAYS) as i64);
                8
            }
            ColBuf::Ts(v) => {
                let us = i64::from_be_bytes(f.try_into().map_err(|_| bad("timestamp"))?);
                if us == i64::MAX || us == i64::MIN {
                    return Err(Error::Transfer(
                        "timestamp 'infinity' has no BigQuery representation — cast \
                         or filter it in a source view"
                            .into(),
                    ));
                }
                v.push(us + PG_EPOCH_MICROS);
                8
            }
            ColBuf::Bytes(v) => match d {
                Delivered::Uuid => {
                    let b: [u8; 16] = f.try_into().map_err(|_| bad("uuid"))?;
                    let mut s = String::with_capacity(36);
                    for (i, byte) in b.iter().enumerate() {
                        if matches!(i, 4 | 6 | 8 | 10) {
                            s.push('-');
                        }
                        s.push_str(&format!("{byte:02x}"));
                    }
                    v.push(ByteArray::from(s.into_bytes()));
                    48 + 36 // the fixed 36-byte hyphenated text, not `f.len()`
                }
                // jsonb = version byte then text; json/text/bytea = raw.
                Delivered::Json if f.first() == Some(&1) => {
                    v.push(ByteArray::from(f[1..].to_vec()));
                    48 + (f.len() - 1)
                }
                _ => {
                    v.push(ByteArray::from(f.to_vec()));
                    48 + f.len()
                }
            },
        })
    }
}

fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: unexpected {what}"))
}

// ============================================================================
// Parquet schema from the delivered types
// ============================================================================

/// `id` is the 1-based column ordinal, written as the parquet field id. For
/// BigQuery/GCS/S3 it is inert metadata; for Iceberg it is load-bearing — the
/// table schema assigns the same ids, and readers resolve columns BY ID, so
/// the two assignments must never drift. Shared with the Iceberg CDC delete
/// file so both lanes spell a column's parquet schema exactly once.
pub(crate) fn parquet_field(name: &str, d: &Delivered, id: i32) -> Result<Arc<Type>> {
    use PhysicalType as P;
    let b = |p| {
        Type::primitive_type_builder(name, p)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(id))
    };
    let t = match d {
        Delivered::Int { .. } => b(P::INT64).build(),
        Delivered::Float32 => b(P::FLOAT).build(),
        Delivered::Float64 => b(P::DOUBLE).build(),
        Delivered::Bool => b(P::BOOLEAN).build(),
        Delivered::Decimal { p, s } => {
            // i128 (16 bytes) carries every precision we can decode exactly;
            // declared precision drives BigQuery's NUMERIC/BIGNUMERIC pick.
            let precision = if *p == 0 || *p > 38 { 38 } else { *p as i32 };
            let scale = (*s as i32).min(precision);
            b(P::FIXED_LEN_BYTE_ARRAY)
                .with_length(16)
                .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
                .with_precision(precision)
                .with_scale(scale)
                .build()
        }
        Delivered::Date => b(P::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .build(),
        Delivered::DateTime { utc } => b(P::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: *utc,
                unit: TimeUnit::MICROS(Default::default()),
            }))
            .build(),
        Delivered::Uuid | Delivered::Json | Delivered::Text => b(P::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .build(),
        Delivered::Bytes => b(P::BYTE_ARRAY).build(),
    };
    t.map(Arc::new)
        .map_err(|e| Error::Transfer(format!("parquet schema: {e}")))
}

/// The output sink the parquet writer writes through — the loader drains
/// aligned chunks out of it between row groups.
#[derive(Clone, Default)]
pub(crate) struct SharedBuf(pub(crate) Arc<Mutex<Vec<u8>>>);

impl SharedBuf {
    /// Take the buffer once it holds a full part's worth. The threshold lives
    /// HERE so every loader drains at the same price it declared.
    pub(crate) fn take_ready(&self) -> Option<Vec<u8>> {
        let mut b = self.0.lock().expect("parquet buf");
        (b.len() >= SEND_THRESHOLD).then(|| std::mem::take(&mut *b))
    }

    /// Take whatever is buffered, ready or not (a file's footer tail).
    pub(crate) fn take_all(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().expect("parquet buf"))
    }
}

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("parquet buf").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ============================================================================
// Streaming encoder: PG binary COPY chunks in → parquet bytes in SharedBuf
// ============================================================================

pub(crate) struct ParquetEncoder {
    delivered: Vec<Delivered>,
    schema: Arc<Type>,
    props: Arc<WriterProperties>,
    writer: Option<SerializedFileWriter<SharedBuf>>,
    pub(crate) out: SharedBuf,
    cols: Vec<ColBuf>,
    defs: Vec<Vec<i16>>,
    // -- COPY framing state (bounds-first; see module docs)
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    finished: bool,
    // -- cursor watermark tracking (col index, numeric compare)
    cursor: Option<(usize, bool)>,
    pub(crate) wm: Option<String>,
    // -- row-group bound, enforced per row (see `try_tuple`/`push`)
    row_group: RowGroup,
    group_bytes: usize,
    // -- merge-key companion: a one-column parquet file written in lockstep
    // with the data file, row group by row group. Nothing survives a flush,
    // so a merge's memory is flat in the delta (I4).
    pub(crate) key_file: Option<KeyFile>,
}

/// A one-column parquet file written IN LOCKSTEP with the data file. Every
/// data row group is followed by a key row group built from the same builder
/// (`cols[col]`), before the builders clear. Nothing is copied and nothing
/// survives a flush.
pub(crate) struct KeyFile {
    pub(crate) col: usize,
    /// `group("schema"){ parquet_field(&names[col], &delivered[col], id) }`,
    /// id = ids[col] or col+1 — the data column's own fn, so the two schemas
    /// cannot drift.
    schema: Arc<Type>,
    writer: Option<SerializedFileWriter<SharedBuf>>,
    pub(crate) out: SharedBuf,
    /// Rows in the OPEN key file; reset when `finish_file` closes it.
    rows: u64,
}

impl ParquetEncoder {
    pub(crate) fn new(
        names: Vec<String>,
        delivered: Vec<Delivered>,
        cursor: Option<(usize, bool)>,
        row_group: RowGroup,
    ) -> Result<Self> {
        Self::new_ext(names, delivered, cursor, None, None, row_group)
    }

    /// `ids`: explicit parquet field ids (Iceberg tables that already exist own
    /// their ids; ordinal 1..N otherwise). `key_col`: column mirrored into the
    /// streamed key companion ([`Self::key_file`]); a NULL in it is an error.
    /// `row_group`: the planner-priced bound the builders flush at (per row,
    /// not per input chunk).
    pub(crate) fn new_ext(
        names: Vec<String>,
        delivered: Vec<Delivered>,
        cursor: Option<(usize, bool)>,
        ids: Option<Vec<i32>>,
        key_col: Option<usize>,
        row_group: RowGroup,
    ) -> Result<Self> {
        if let Some(ids) = &ids {
            if ids.len() != names.len() {
                return Err(Error::Transfer(format!(
                    "parquet schema: {} field ids for {} columns",
                    ids.len(),
                    names.len()
                )));
            }
        }
        let key_file = match key_col {
            None => None,
            Some(i) => {
                // The type gate is shared with the Iceberg CDC delete file.
                merge_key_ok(&delivered[i])?;
                let id = ids.as_ref().map_or(i as i32 + 1, |v| v[i]);
                let field = parquet_field(&names[i], &delivered[i], id)?;
                let schema = Arc::new(
                    Type::group_type_builder("schema")
                        .with_fields(vec![field])
                        .build()
                        .map_err(|e| Error::Transfer(format!("parquet key schema: {e}")))?,
                );
                Some(KeyFile {
                    col: i,
                    schema,
                    writer: None,
                    out: SharedBuf::default(),
                    rows: 0,
                })
            }
        };
        let fields: Vec<Arc<Type>> = names
            .iter()
            .zip(delivered.iter())
            .enumerate()
            .map(|(i, (n, d))| {
                let id = ids.as_ref().map_or(i as i32 + 1, |v| v[i]);
                parquet_field(n, d, id)
            })
            .collect::<Result<_>>()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .map_err(|e| Error::Transfer(format!("parquet schema: {e}")))?,
        );
        let props = Arc::new(
            WriterProperties::builder()
                // ZSTD-1: gzip-class ratio at snappy-class speed — upload bytes
                // halve vs SNAPPY (measured: capped boxes are upload-bound).
                .set_compression(Compression::ZSTD(
                    parquet::basic::ZstdLevel::try_new(1).expect("zstd level 1 is always valid"),
                ))
                .build(),
        );
        let cols = delivered.iter().map(ColBuf::new).collect();
        let defs = vec![Vec::new(); delivered.len()];
        let mut enc = Self {
            delivered,
            schema,
            props,
            writer: None,
            out: SharedBuf::default(),
            cols,
            defs,
            buf: Vec::with_capacity(FRAME_BUF),
            pos: 0,
            header_done: false,
            finished: false,
            cursor,
            wm: None,
            row_group,
            group_bytes: 0,
            key_file,
        };
        enc.open_writer()?;
        Ok(enc)
    }

    fn open_writer(&mut self) -> Result<()> {
        self.writer = Some(
            SerializedFileWriter::new(self.out.clone(), self.schema.clone(), self.props.clone())
                .map_err(|e| Error::Transfer(format!("parquet writer: {e}")))?,
        );
        if let Some(k) = &mut self.key_file {
            k.writer = Some(
                SerializedFileWriter::new(k.out.clone(), k.schema.clone(), self.props.clone())
                    .map_err(|e| Error::Transfer(format!("parquet key writer: {e}")))?,
            );
        }
        Ok(())
    }

    /// Feed COPY bytes; returns rows completed in this call. Flushes a row
    /// group into `out` whenever the builders reach the priced bound — checked
    /// per ROW, so one wide input chunk cannot build a group many times over.
    pub(crate) fn push(&mut self, input: &[u8]) -> Result<u64> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        if self.pos > FRAME_BUF {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(input);

        if !self.header_done {
            if self.buf.len() - self.pos < 19 {
                return Ok(0);
            }
            if &self.buf[self.pos..self.pos + 11] != b"PGCOPY\n\xff\r\n\0" {
                return Err(Error::Transfer("pg binary COPY: bad header".into()));
            }
            let ext = u32::from_be_bytes(self.buf[self.pos + 15..self.pos + 19].try_into().unwrap())
                as usize;
            if self.buf.len() - self.pos < 19 + ext {
                return Ok(0);
            }
            self.pos += 19 + ext;
            self.header_done = true;
        }

        let mut rows = 0u64;
        // O(1) swap frees `self` for the builders while we read the buffer.
        let buf = std::mem::take(&mut self.buf);
        let mut res = Ok(());
        while !self.finished {
            match self.try_tuple(&buf[self.pos..]) {
                Ok(Some((consumed, trailer))) => {
                    self.pos += consumed;
                    if trailer {
                        self.finished = true;
                    } else {
                        rows += 1;
                        // Per ROW: `try_tuple` already added this row's
                        // resident bytes, so the bound holds no matter how
                        // many rows one input chunk carries.
                        if self.group_bytes >= self.row_group.bytes() {
                            if let Err(e) = self.flush_row_group() {
                                res = Err(e);
                                break;
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    res = Err(e);
                    break;
                }
            }
        }
        self.buf = buf;
        res?;
        Ok(rows)
    }

    /// Bounds-first: prove the tuple complete, then decode. `Ok(None)` =
    /// incomplete (wait for more input, nothing consumed or emitted).
    fn try_tuple(&mut self, b: &[u8]) -> Result<Option<(usize, bool)>> {
        if b.len() < 2 {
            return Ok(None);
        }
        let ncols = i16::from_be_bytes(b[..2].try_into().unwrap());
        if ncols == -1 {
            return Ok(Some((2, true)));
        }
        if ncols as usize != self.cols.len() {
            return Err(Error::Transfer(format!(
                "pg binary COPY: tuple has {ncols} fields, expected {}",
                self.cols.len()
            )));
        }
        // Pass 1: bounds walk.
        let mut off = 2usize;
        for _ in 0..self.cols.len() {
            if b.len() < off + 4 {
                return Ok(None);
            }
            let len = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
            off += 4;
            if len < -1 {
                return Err(Error::Transfer(format!(
                    "pg binary COPY: corrupt field length {len}"
                )));
            }
            if len > 0 {
                if b.len() < off + len as usize {
                    return Ok(None);
                }
                off += len as usize;
            }
        }
        // Pass 2: decode (complete by construction).
        let mut o = 2usize;
        for i in 0..self.cols.len() {
            let len = i32::from_be_bytes(b[o..o + 4].try_into().unwrap());
            o += 4;
            if len == -1 {
                if self.key_file.as_ref().is_some_and(|k| k.col == i) {
                    return Err(Error::Transfer(
                        "merge key column contains NULL — a merge key must \
                         identify its row"
                            .into(),
                    ));
                }
                self.defs[i].push(0);
                self.group_bytes += 2; // the definition level vector's entry
                continue;
            }
            let f = &b[o..o + len as usize];
            o += len as usize;
            self.group_bytes += self.cols[i].push_pg(f, &self.delivered[i])? + 2;
            self.defs[i].push(1);
            if let Some((idx, numeric)) = self.cursor {
                if i == idx {
                    let v = render_cursor(&self.delivered[i], f)?;
                    self.wm =
                        crate::plan::wm_max(self.wm.take(), Some(v), numeric);
                }
            }
        }
        Ok(Some((off, false)))
    }

    pub(crate) fn rows_buffered(&self) -> usize {
        self.defs.first().map(|d| d.len()).unwrap_or(0)
    }

    /// Write the buffered rows as one row group into `out`.
    pub(crate) fn flush_row_group(&mut self) -> Result<()> {
        if self.rows_buffered() == 0 {
            return Ok(());
        }
        let writer = self.writer.as_mut().expect("writer open");
        let mut rg = writer
            .next_row_group()
            .map_err(|e| Error::Transfer(format!("parquet row group: {e}")))?;
        let mut i = 0usize;
        while let Some(mut col) = rg
            .next_column()
            .map_err(|e| Error::Transfer(format!("parquet column: {e}")))?
        {
            let defs = &self.defs[i];
            let err = |e| Error::Transfer(format!("parquet write: {e}"));
            match &self.cols[i] {
                ColBuf::I64(v) | ColBuf::Ts(v) => {
                    col.typed::<Int64Type>()
                        .write_batch(v, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::Date(v) => {
                    let narrowed: Vec<i32> = v.iter().map(|&d| d as i32).collect();
                    col.typed::<parquet::data_type::Int32Type>()
                        .write_batch(&narrowed, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::F32(v) => {
                    col.typed::<FloatType>()
                        .write_batch(v, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::F64(v) => {
                    col.typed::<DoubleType>()
                        .write_batch(v, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::Bool(v) => {
                    col.typed::<BoolType>()
                        .write_batch(v, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::Dec { vals, .. } => {
                    col.typed::<FixedLenByteArrayType>()
                        .write_batch(vals, Some(defs), None)
                        .map_err(err)?;
                }
                ColBuf::Bytes(v) => {
                    col.typed::<ByteArrayType>()
                        .write_batch(v, Some(defs), None)
                        .map_err(err)?;
                }
            }
            col.close().map_err(err)?;
            i += 1;
        }
        rg.close()
            .map_err(|e| Error::Transfer(format!("parquet row group close: {e}")))?;
        // Read the row count BEFORE borrowing `key_file`: `rows_buffered` is a
        // `&self` method and the mutable borrow below is live through it.
        let buffered = self.rows_buffered() as u64;
        if let Some(k) = &mut self.key_file {
            // The companion mirrors the data row groups one-for-one: built
            // from the same builder, written in the same flush, before the
            // builders clear. `k.rows` counts the OPEN key file only.
            let pe = |e| Error::Transfer(format!("parquet key file: {e}"));
            let mut krg = k
                .writer
                .as_mut()
                .expect("key writer open")
                .next_row_group()
                .map_err(pe)?;
            let mut col = krg.next_column().map_err(pe)?.expect("one column");
            match &self.cols[k.col] {
                ColBuf::I64(v) => {
                    col.typed::<Int64Type>()
                        .write_batch(v, Some(&self.defs[k.col]), None)
                        .map_err(pe)?;
                }
                // uuid already hyphenated at push_pg: byte-identical by construction.
                ColBuf::Bytes(v) => {
                    col.typed::<ByteArrayType>()
                        .write_batch(v, Some(&self.defs[k.col]), None)
                        .map_err(pe)?;
                }
                _ => {
                    return Err(Error::Transfer(
                        "merge key builder is not I64/Bytes: gate bypassed".into(),
                    ))
                }
            }
            col.close().map_err(pe)?;
            krg.close().map_err(pe)?;
            k.rows += buffered;
        }
        for c in &mut self.cols {
            c.clear();
        }
        for d in &mut self.defs {
            d.clear();
        }
        self.group_bytes = 0;
        Ok(())
    }

    /// Close the CURRENT parquet file (footer lands in `out`) and open a
    /// fresh writer for the next file. Returns the closed key file's rows when
    /// a key companion is open — S3, GCS, BigQuery and dest_ice ignore it;
    /// the Iceberg merge checks it against its own row count.
    pub(crate) fn finish_file(&mut self) -> Result<Option<u64>> {
        self.flush_row_group()?;
        let writer = self.writer.take().expect("writer open");
        writer
            .close()
            .map_err(|e| Error::Transfer(format!("parquet close: {e}")))?;
        let key_rows = if let Some(k) = &mut self.key_file {
            let kw = k.writer.take().expect("key writer open");
            kw.close()
                .map_err(|e| Error::Transfer(format!("parquet key close: {e}")))?;
            let n = k.rows;
            k.rows = 0;
            Some(n)
        } else {
            None
        };
        self.open_writer()?;
        Ok(key_rows)
    }
}

/// Render UNIX-epoch microseconds the way `render_cursor` renders a pgcopy
/// timestamp — the Iceberg sink derives watermarks from parquet footer stats
/// (already unix-based) and those strings must compare against state written
/// from the wire path.
pub(crate) fn render_ts_micros(unix_us: i64, utc: bool) -> Result<String> {
    let secs = unix_us.div_euclid(1_000_000);
    let micros = unix_us.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, micros * 1000)
        .ok_or_else(|| bad("cursor ts range"))?;
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let frac = if micros == 0 {
        String::new()
    } else {
        format!(".{}", format!("{micros:06}").trim_end_matches('0'))
    };
    Ok(if utc { format!("{base}{frac}+00") } else { format!("{base}{frac}") })
}

/// UNIX-epoch days → `YYYY-MM-DD` (same rendering as the wire path).
pub(crate) fn render_date_days(unix_days: i64) -> Result<String> {
    chrono::DateTime::from_timestamp(unix_days * 86_400, 0)
        .ok_or_else(|| bad("cursor date range"))
        .map(|d| d.format("%Y-%m-%d").to_string())
}

/// Render a cursor value the way the TEXT lane would (PG's own style), so
/// state rows stay comparable across lanes and runs.
fn render_cursor(d: &Delivered, f: &[u8]) -> Result<String> {
    Ok(match d {
        Delivered::Int { .. } => match f.len() {
            2 => i16::from_be_bytes(f.try_into().unwrap()).to_string(),
            4 => i32::from_be_bytes(f.try_into().unwrap()).to_string(),
            _ => i64::from_be_bytes(f.try_into().map_err(|_| bad("cursor int"))?).to_string(),
        },
        Delivered::DateTime { utc } => {
            let us =
                i64::from_be_bytes(f.try_into().map_err(|_| bad("cursor ts"))?) + PG_EPOCH_MICROS;
            render_ts_micros(us, *utc)?
        }
        Delivered::Date => {
            let days = i32::from_be_bytes(f.try_into().map_err(|_| bad("cursor date"))?);
            render_date_days(days as i64 + PG_EPOCH_DAYS as i64)?
        }
        Delivered::Decimal { s, .. } => {
            // BIGINT UNSIGNED rides as Decimal{20,0} on MySQL sources — an
            // unsigned auto-increment PK is the default cursor idiom there.
            let scale = *s as u32;
            let scaled = crate::wire::pgcopy::numeric_to_scaled_i128(f, scale)?;
            if scale == 0 {
                scaled.to_string()
            } else {
                let neg = scaled < 0;
                let abs = scaled.unsigned_abs().to_string();
                let abs = if abs.len() <= scale as usize {
                    format!("{}{}", "0".repeat(scale as usize + 1 - abs.len()), abs)
                } else {
                    abs
                };
                let (int, frac) = abs.split_at(abs.len() - scale as usize);
                format!("{}{int}.{frac}", if neg { "-" } else { "" })
            }
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "cursor column type {other:?} isn't supported on the binary lane"
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn field(payload: &[u8]) -> Vec<u8> {
        let mut f = (payload.len() as i32).to_be_bytes().to_vec();
        f.extend_from_slice(payload);
        f
    }

    fn copy_stream(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
        let mut s = b"PGCOPY\n\xff\r\n\0".to_vec();
        s.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // flags + ext len
        for row in rows {
            s.extend_from_slice(&(row.len() as i16).to_be_bytes());
            for f in row {
                match f {
                    Some(p) => s.extend_from_slice(&field(p)),
                    None => s.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
        }
        s.extend_from_slice(&(-1i16).to_be_bytes());
        s
    }

    #[test]
    fn roundtrips_typed_rows_through_a_real_parquet_reader() {
        let names = vec!["id".into(), "name".into(), "ok".into(), "ts".into()];
        let delivered = vec![
            Delivered::Int {
                bytes: 8,
                unsigned: false,
            },
            Delivered::Text,
            Delivered::Bool,
            Delivered::DateTime { utc: true },
        ];
        let mut enc =
            ParquetEncoder::new(names, delivered, Some((0, true)), RowGroup::Mib24).unwrap();
        let stream = copy_stream(&[
            vec![
                Some(7i64.to_be_bytes().to_vec()),
                Some(b"hello".to_vec()),
                Some(vec![1]),
                Some(0i64.to_be_bytes().to_vec()), // PG epoch = 2000-01-01
            ],
            vec![
                Some(42i64.to_be_bytes().to_vec()),
                None,
                Some(vec![0]),
                None,
            ],
        ]);
        // Feed byte-by-byte: chunk boundaries anywhere must be safe.
        let mut rows = 0;
        for b in &stream {
            rows += enc.push(std::slice::from_ref(b)).unwrap();
        }
        assert_eq!(rows, 2);
        assert_eq!(enc.wm.as_deref(), Some("42"));
        enc.finish_file().unwrap();

        let bytes = enc.out.0.lock().unwrap().clone();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let mut it = reader.get_row_iter(None).unwrap();
        let r1 = it.next().unwrap().unwrap().to_string();
        let r2 = it.next().unwrap().unwrap().to_string();
        assert!(r1.contains("id: 7") && r1.contains("hello"), "{r1}");
        assert!(r1.contains("2000-01-01"), "{r1}");
        assert!(r2.contains("id: 42") && r2.contains("name: null"), "{r2}");
        assert!(it.next().is_none());
    }

    #[test]
    fn decimal_and_date_encode_exactly() {
        // numeric 1234.5678 (from rowbinary's own test vector), scale 4.
        let pg_numeric: Vec<u8> = {
            let mut f = Vec::new();
            f.extend_from_slice(&2i16.to_be_bytes()); // ndigits
            f.extend_from_slice(&0i16.to_be_bytes()); // weight
            f.extend_from_slice(&0i16.to_be_bytes()); // sign +
            f.extend_from_slice(&4i16.to_be_bytes()); // dscale
            f.extend_from_slice(&1234i16.to_be_bytes());
            f.extend_from_slice(&5678i16.to_be_bytes());
            f
        };
        let names = vec!["d".into(), "day".into()];
        let delivered = vec![Delivered::Decimal { p: 18, s: 4 }, Delivered::Date];
        let mut enc = ParquetEncoder::new(names, delivered, None, RowGroup::Mib24).unwrap();
        let stream = copy_stream(&[vec![
            Some(pg_numeric),
            Some(0i32.to_be_bytes().to_vec()), // 2000-01-01
        ]]);
        assert_eq!(enc.push(&stream).unwrap(), 1);
        enc.finish_file().unwrap();
        let bytes = enc.out.0.lock().unwrap().clone();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let row = reader
            .get_row_iter(None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .to_string();
        assert!(row.contains("1234.5678"), "{row}");
        assert!(row.contains("2000-01-01"), "{row}");
    }

    #[test]
    fn lane_gate_rejects_inexact_columns() {
        assert!(parquet_col_ok("int8", None));
        assert!(parquet_col_ok("numeric", Some(18)));
        assert!(!parquet_col_ok("numeric", None)); // unconstrained -> Float64 bytes
        assert!(!parquet_col_ok("numeric", Some(50))); // > i128 digits
        assert!(!parquet_col_ok("bytea", None)); // raw bytes into STRING
        assert!(!parquet_col_ok("inet", None));
    }

    #[test]
    fn cursor_renders_pg_style() {
        assert_eq!(
            render_cursor(&Delivered::DateTime { utc: true }, &0i64.to_be_bytes()).unwrap(),
            "2000-01-01 00:00:00+00"
        );
        assert_eq!(
            render_cursor(
                &Delivered::DateTime { utc: false },
                &1_500_000i64.to_be_bytes()
            )
            .unwrap(),
            "2000-01-01 00:00:01.5"
        );
    }

    /// numeric 1.23 (ndigits 1, weight 0, sign +, dscale 2).
    fn pg_numeric() -> Vec<u8> {
        let mut f = Vec::new();
        f.extend_from_slice(&1i16.to_be_bytes());
        f.extend_from_slice(&0i16.to_be_bytes());
        f.extend_from_slice(&0i16.to_be_bytes());
        f.extend_from_slice(&2i16.to_be_bytes());
        f.extend_from_slice(&123i16.to_be_bytes());
        f
    }

    /// T4b. The running counter is the only thing between the builders and an
    /// out-of-memory kill, so it must equal the resident sum after every push
    /// — the `ColBuf::bytes` re-sum is the oracle. A uuid's resident bytes are
    /// its fixed 36-byte hyphenated text plus the ByteArray overhead, never
    /// the 16 wire bytes.
    #[test]
    fn running_group_bytes_equals_the_resident_sum() {
        let names: Vec<String> = ["i", "f4", "f8", "b", "d", "day", "ts", "u", "j", "t", "raw"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let delivered = vec![
            Delivered::Int { bytes: 8, unsigned: false },
            Delivered::Float32,
            Delivered::Float64,
            Delivered::Bool,
            Delivered::Decimal { p: 18, s: 2 },
            Delivered::Date,
            Delivered::DateTime { utc: true },
            Delivered::Uuid,
            Delivered::Json,
            Delivered::Text,
            Delivered::Bytes,
        ];
        let mut enc =
            ParquetEncoder::new(names, delivered.clone(), None, RowGroup::Test(64 << 10)).unwrap();
        // xorshift64: random shapes, one fixed seed.
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut stream = b"PGCOPY\n\xff\r\n\0".to_vec();
        stream.extend_from_slice(&[0u8; 8]);
        for _ in 0..500 {
            stream.extend_from_slice(&(delivered.len() as i16).to_be_bytes());
            for (i, d) in delivered.iter().enumerate() {
                if next() % 5 == 0 {
                    stream.extend_from_slice(&(-1i32).to_be_bytes());
                    continue;
                }
                let f: Vec<u8> = match (i, d) {
                    (0, _) => next().to_be_bytes().to_vec(),
                    (1, _) => (next() as u32).to_be_bytes().to_vec(),
                    (2, _) => next().to_be_bytes().to_vec(),
                    (3, _) => vec![(next() & 1) as u8],
                    (4, _) => pg_numeric(),
                    (5, _) => ((next() % 10_000) as i32).to_be_bytes().to_vec(),
                    (6, _) => ((next() % (1 << 40)) as i64).to_be_bytes().to_vec(),
                    (7, _) => (0..16).map(|_| next() as u8).collect(),
                    (8, _) => {
                        let mut v = vec![1u8]; // jsonb version byte
                        v.extend_from_slice(b"{}");
                        v
                    }
                    (9, _) => b"hello".to_vec(),
                    (10, _) => (0..4).map(|_| next() as u8).collect(),
                    _ => unreachable!(),
                };
                stream.extend_from_slice(&(f.len() as i32).to_be_bytes());
                stream.extend_from_slice(&f);
            }
        }
        stream.extend_from_slice(&(-1i16).to_be_bytes());
        // Irregular slices: tuple boundaries and flushes land inside a push.
        for part in stream.chunks(997) {
            enc.push(part).unwrap();
            let oracle = enc.cols.iter().map(|c| c.bytes()).sum::<usize>()
                + enc.defs.iter().map(|d| d.len() * 2).sum::<usize>();
            assert_eq!(enc.group_bytes, oracle);
        }
    }

    /// T4c. A wide-text row's resident bytes run up to ~10× its wire bytes
    /// (the 48-byte ByteArray overhead), so a per-chunk flush would build a
    /// row group dozens of times the priced bound from one 4 MiB chunk. The
    /// per-row check cannot: every written row group fits the bound, and one
    /// chunk yields many of them.
    #[test]
    fn short_wide_rows_cannot_overshoot_the_row_group() {
        const RG: usize = 256 << 10;
        let names: Vec<String> = (0..12).map(|i| format!("c{i}")).collect();
        let mut enc = ParquetEncoder::new(
            names,
            vec![Delivered::Text; 12],
            None,
            RowGroup::Test(RG),
        )
        .unwrap();
        // ~4 MiB of COPY bytes: 70 000 rows × (2 + 12×(4+1)) wire bytes.
        let rows: Vec<Vec<Option<Vec<u8>>>> = (0..70_000)
            .map(|_| (0..12).map(|_| Some(b"x".to_vec())).collect())
            .collect();
        let stream = copy_stream(&rows);
        assert_eq!(enc.push(&stream).unwrap(), 70_000);
        // 12 × (48 + 1 stored byte + 2 definition bytes) resident per row.
        let per_row = 12 * (48 + 1 + 2);
        assert!(enc.group_bytes < RG + per_row, "{}", enc.group_bytes);
        enc.finish_file().unwrap();
        let bytes = enc.out.0.lock().unwrap().clone();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let groups = reader.metadata().row_groups();
        assert!(
            groups.len() > 1,
            "one input chunk built one {} MiB row group",
            groups.iter().map(|g| g.num_rows()).sum::<i64>() as usize * per_row >> 20
        );
        for g in groups {
            assert!(
                g.num_rows() as usize * per_row <= RG + per_row,
                "row group holds {} resident bytes",
                g.num_rows() as usize * per_row
            );
        }
    }

    /// T4. A merge's key companion is written row group by row group from the
    /// same builders, so nothing about it grows with the delta — the 0.56.0
    /// `KeyCap` retained every key until commit (~150 MB for the 2.1M keys of
    /// E2's OOM). The key file must mirror the data file: same row groups,
    /// same rows, and the key column's own schema.
    #[test]
    fn merge_key_file_streams_per_row_group_and_retains_nothing() {
        use parquet::record::Field;
        const RG: usize = 64 << 10;
        let mut enc = ParquetEncoder::new_ext(
            vec!["id".into()],
            vec![Delivered::Int { bytes: 8, unsigned: false }],
            None,
            None,
            Some(0),
            RowGroup::Test(RG),
        )
        .unwrap();
        let rows: Vec<Vec<Option<Vec<u8>>>> = (0..20_000i64)
            .map(|i| vec![Some(i.to_be_bytes().to_vec())])
            .collect();
        let stream = copy_stream(&rows);
        // Fed in 1 KiB slices: flushes land inside a push, and the running
        // counter must never hold more than one row over the priced bound.
        for part in stream.chunks(1024) {
            enc.push(part).unwrap();
            assert!(enc.group_bytes < RG + 10, "{}", enc.group_bytes);
        }
        assert_eq!(enc.finish_file().unwrap(), Some(20_000));
        assert_eq!(enc.key_file.as_ref().unwrap().rows, 0);

        let data_bytes = enc.out.0.lock().unwrap().clone();
        let key_bytes = enc.key_file.as_ref().unwrap().out.0.lock().unwrap().clone();
        let data = SerializedFileReader::new(bytes::Bytes::from(data_bytes)).unwrap();
        let key = SerializedFileReader::new(bytes::Bytes::from(key_bytes)).unwrap();
        let groups = data.metadata().row_groups().len();
        assert!(groups > 1, "one row group for 20k rows: bound never fired");
        assert_eq!(key.metadata().row_groups().len(), groups);
        let vals: Vec<i64> = key
            .get_row_iter(None)
            .unwrap()
            .map(|r| match r.unwrap().get_column_iter().next().unwrap().1 {
                Field::Long(v) => *v,
                other => panic!("key value {other:?}"),
            })
            .collect();
        assert_eq!(vals, (0..20_000).collect::<Vec<i64>>());
        let dcol = data.metadata().file_metadata().schema_descr().column(0);
        let kcol = key.metadata().file_metadata().schema_descr().column(0);
        assert_eq!(kcol.physical_type(), dcol.physical_type());
        assert_eq!(
            kcol.self_type().get_basic_info().id(),
            dcol.self_type().get_basic_info().id()
        );

        // A uuid key is mirrored as the data column's own 36-byte hyphenated
        // strings (never the 16 wire bytes).
        let mut enc = ParquetEncoder::new_ext(
            vec!["uid".into()],
            vec![Delivered::Uuid],
            None,
            None,
            Some(0),
            RowGroup::Test(RG),
        )
        .unwrap();
        let rows: Vec<Vec<Option<Vec<u8>>>> = (0..500u16)
            .map(|i| {
                let mut b = [0u8; 16];
                b[0] = i as u8;
                b[15] = (i >> 8) as u8;
                vec![Some(b.to_vec())]
            })
            .collect();
        enc.push(&copy_stream(&rows)).unwrap();
        assert_eq!(enc.finish_file().unwrap(), Some(500));
        let data_bytes = enc.out.0.lock().unwrap().clone();
        let key_bytes = enc.key_file.as_ref().unwrap().out.0.lock().unwrap().clone();
        let values = |b: Vec<u8>| -> Vec<String> {
            SerializedFileReader::new(bytes::Bytes::from(b))
                .unwrap()
                .get_row_iter(None)
                .unwrap()
                .map(|r| match r.unwrap().get_column_iter().next().unwrap().1 {
                    Field::Str(s) => s.clone(),
                    Field::Bytes(b) => String::from_utf8(b.data().to_vec()).unwrap(),
                    other => panic!("uuid key value {other:?}"),
                })
                .collect()
        };
        let data_vals = values(data_bytes);
        assert_eq!(values(key_bytes), data_vals);
        assert!(data_vals.iter().all(|s| s.len() == 36 && s.as_bytes()[8] == b'-'));
    }
}
