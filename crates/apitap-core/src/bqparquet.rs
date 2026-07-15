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
use crate::rowbinary::{numeric_to_scaled_i128, PG_EPOCH_DAYS, PG_EPOCH_MICROS};
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

/// Flush a row group once its builders hold about this much data — small
/// enough that two workers fit a 256 MB container, big enough for good pages.
const ROW_GROUP_BYTES: usize = 24 * 1024 * 1024;

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
    udt != "numeric" || matches!(precision, Some(p) if (1..=38).contains(&p))
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

    fn len(&self) -> usize {
        match self {
            ColBuf::I64(v) => v.len(),
            ColBuf::F32(v) => v.len(),
            ColBuf::F64(v) => v.len(),
            ColBuf::Bool(v) => v.len(),
            ColBuf::Dec { vals, .. } => vals.len(),
            ColBuf::Date(v) | ColBuf::Ts(v) => v.len(),
            ColBuf::Bytes(v) => v.len(),
        }
    }

    /// RESIDENT bytes, for row-group sizing — ByteArray/FLBA hold a struct
    /// (~32 B) plus a heap allocation with allocator quantum; undercounting
    /// here is how a 24 MiB gate turns into a 256 MB OOM at 4 pipes.
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

    /// Decode one non-NULL Postgres binary field into this builder.
    fn push_pg(&mut self, f: &[u8], d: &Delivered) -> Result<()> {
        match self {
            ColBuf::I64(v) => v.push(match f.len() {
                2 => i16::from_be_bytes(f.try_into().unwrap()) as i64,
                4 => i32::from_be_bytes(f.try_into().unwrap()) as i64,
                8 => i64::from_be_bytes(f.try_into().unwrap()),
                n => return Err(bad(&format!("int width {n}"))),
            }),
            ColBuf::F32(v) => {
                v.push(f32::from_be_bytes(f.try_into().map_err(|_| bad("float4"))?));
            }
            ColBuf::F64(v) => {
                v.push(f64::from_be_bytes(f.try_into().map_err(|_| bad("float8"))?));
            }
            ColBuf::Bool(v) => v.push(f.first().copied().unwrap_or(0) != 0),
            ColBuf::Dec { vals, scale } => {
                let x = numeric_to_scaled_i128(f, *scale)?;
                vals.push(FixedLenByteArray::from(x.to_be_bytes().to_vec()));
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
                }
                // jsonb = version byte then text; json/text/bytea = raw.
                Delivered::Json if f.first() == Some(&1) => {
                    v.push(ByteArray::from(f[1..].to_vec()))
                }
                _ => v.push(ByteArray::from(f.to_vec())),
            },
        }
        Ok(())
    }
}

fn bad(what: &str) -> Error {
    Error::Transfer(format!("pg binary COPY: unexpected {what}"))
}

// ============================================================================
// Parquet schema from the delivered types
// ============================================================================

fn parquet_field(name: &str, d: &Delivered) -> Result<Arc<Type>> {
    use PhysicalType as P;
    let b = |p| Type::primitive_type_builder(name, p).with_repetition(Repetition::OPTIONAL);
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
    names: Vec<String>,
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
}

impl ParquetEncoder {
    pub(crate) fn new(
        names: Vec<String>,
        delivered: Vec<Delivered>,
        cursor: Option<(usize, bool)>,
    ) -> Result<Self> {
        let fields: Vec<Arc<Type>> = names
            .iter()
            .zip(delivered.iter())
            .map(|(n, d)| parquet_field(n, d))
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
            names,
            delivered,
            schema,
            props,
            writer: None,
            out: SharedBuf::default(),
            cols,
            defs,
            buf: Vec::with_capacity(1 << 20),
            pos: 0,
            header_done: false,
            finished: false,
            cursor,
            wm: None,
        };
        enc.open_writer()?;
        Ok(enc)
    }

    fn open_writer(&mut self) -> Result<()> {
        self.writer = Some(
            SerializedFileWriter::new(self.out.clone(), self.schema.clone(), self.props.clone())
                .map_err(|e| Error::Transfer(format!("parquet writer: {e}")))?,
        );
        Ok(())
    }

    /// Feed COPY bytes; returns rows completed in this call. Flushes a row
    /// group into `out` whenever the builders grow past ROW_GROUP_BYTES.
    pub(crate) fn push(&mut self, input: &[u8]) -> Result<u64> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        if self.pos > (1 << 20) {
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
        if self.group_bytes() >= ROW_GROUP_BYTES {
            self.flush_row_group()?;
        }
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
                self.defs[i].push(0);
                continue;
            }
            let f = &b[o..o + len as usize];
            o += len as usize;
            self.cols[i].push_pg(f, &self.delivered[i])?;
            self.defs[i].push(1);
            if let Some((idx, numeric)) = self.cursor {
                if i == idx {
                    let v = render_cursor(&self.delivered[i], f)?;
                    self.wm =
                        crate::connectors::bigquery::wm_max_pub(self.wm.take(), Some(v), numeric);
                }
            }
        }
        Ok(Some((off, false)))
    }

    fn group_bytes(&self) -> usize {
        self.cols.iter().map(|c| c.bytes()).sum::<usize>()
            + self.defs.iter().map(|d| d.len() * 2).sum::<usize>()
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
        for c in &mut self.cols {
            c.clear();
        }
        for d in &mut self.defs {
            d.clear();
        }
        Ok(())
    }

    /// Close the CURRENT parquet file (footer lands in `out`) and open a
    /// fresh writer for the next file. Returns rows in the closed file? No —
    /// the loader tracks rows; this only finalizes bytes.
    pub(crate) fn finish_file(&mut self) -> Result<()> {
        self.flush_row_group()?;
        let writer = self.writer.take().expect("writer open");
        writer
            .close()
            .map_err(|e| Error::Transfer(format!("parquet close: {e}")))?;
        self.open_writer()
    }
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
            let secs = us.div_euclid(1_000_000);
            let micros = us.rem_euclid(1_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, micros * 1000)
                .ok_or_else(|| bad("cursor ts range"))?;
            let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
            let frac = if micros == 0 {
                String::new()
            } else {
                format!(".{}", format!("{micros:06}").trim_end_matches('0'))
            };
            if *utc {
                format!("{base}{frac}+00")
            } else {
                format!("{base}{frac}")
            }
        }
        Delivered::Date => {
            let days = i32::from_be_bytes(f.try_into().map_err(|_| bad("cursor date"))?);
            let secs = (days as i64 + PG_EPOCH_DAYS as i64) * 86_400;
            chrono::DateTime::from_timestamp(secs, 0)
                .ok_or_else(|| bad("cursor date range"))?
                .format("%Y-%m-%d")
                .to_string()
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
        let mut enc = ParquetEncoder::new(names, delivered, Some((0, true))).unwrap();
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
        let mut enc = ParquetEncoder::new(names, delivered, None).unwrap();
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
}
