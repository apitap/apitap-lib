//! ClickHouse as a SOURCE: `SELECT … FORMAT RowBinary` streamed straight into
//! the sink's loaders. On a ch→ch route the sink speaks the same format, so the
//! bytes cross untouched — the lane's SELECT casts every column to exactly the
//! type the destination DDL will declare, which makes the passthrough
//! byte-correct by construction (and makes a concurrent `ALTER` on the source
//! either fail the query or be coerced, never silently reshape the stream).
//!
//! Three hazards the live server taught us, all handled here:
//!   * `wait_end_of_query=1` (what the sink uses, so an INSERT's HTTP status is
//!     trustworthy) buffers the WHOLE result server-side — a 452 MB materialize
//!     for a 1M-row table. Reads must use `=0` and stream.
//!   * With `=0` a mid-stream failure still returns **HTTP 200**, with the
//!     exception text appended to a body of otherwise-valid RowBinary, and there
//!     is no trailer to notice it. So every span is row-counted while it streams
//!     and checked against `count()` — a short span aborts the transfer before
//!     the staging table is ever swapped in.
//!   * Splitting a ReplacingMergeTree (or Collapsing/Summing/Aggregating) across
//!     spans is not a snapshot: a merge between two requests changes the row
//!     multiset. Those engines read as ONE span.

use crate::error::{Error, Result};
use crate::plan::{ColumnPlan, Delivered, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::clickhouse::ChConn;
use crate::sink::Loader;
use crate::source::{pop, spans, work_queue, WorkQueue};

pub(crate) struct ChSource {
    conn: ChConn,
    /// Engine of the table probed last — the span planner needs it (see the
    /// module note on non-plain MergeTree engines).
    engine: std::sync::Mutex<String>,
}

/// Quote an identifier the ClickHouse way (backticks, doubled inside).
fn q(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// `db.table` → a quoted, fully-qualified name (the URL's database when elided).
fn qualify(conn_db: &str, table: &str) -> (String, String, String) {
    match table.split_once('.') {
        Some((db, t)) => (db.to_string(), t.to_string(), format!("{}.{}", q(db), q(t))),
        None => (
            conn_db.to_string(),
            table.to_string(),
            format!("{}.{}", q(conn_db), q(table)),
        ),
    }
}

/// SQL string literal (ClickHouse escapes with backslashes).
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Strip the wrappers that do not change the RowBinary payload, reporting
/// nullability. `LowCardinality(T)` is fully transparent on the wire (the
/// dictionary is never sent) — but it still has to come off before the inner
/// type can be matched.
fn peel(ty: &str) -> (String, bool) {
    let mut t = ty.trim().to_string();
    let mut nullable = false;
    loop {
        if let Some(inner) = t
            .strip_prefix("Nullable(")
            .and_then(|r| r.strip_suffix(')'))
        {
            nullable = true;
            t = inner.trim().to_string();
            continue;
        }
        if let Some(inner) = t
            .strip_prefix("LowCardinality(")
            .and_then(|r| r.strip_suffix(')'))
        {
            t = inner.trim().to_string();
            continue;
        }
        return (t, nullable);
    }
}

/// Inner ClickHouse type → this engine's delivery vocabulary. Anything outside
/// it (Array, Map, Tuple, Enum, IPv4/6, Int128+, …) rides as text: every table
/// transfers, exotic columns land as String.
fn delivered_of(inner: &str) -> Delivered {
    let head = inner.split('(').next().unwrap_or(inner).trim();
    match head {
        "Int8" => Delivered::Int { bytes: 1, unsigned: false },
        "Int16" => Delivered::Int { bytes: 2, unsigned: false },
        "Int32" => Delivered::Int { bytes: 4, unsigned: false },
        "Int64" => Delivered::Int { bytes: 8, unsigned: false },
        "UInt8" => Delivered::Int { bytes: 1, unsigned: true },
        "UInt16" => Delivered::Int { bytes: 2, unsigned: true },
        "UInt32" => Delivered::Int { bytes: 4, unsigned: true },
        "UInt64" => Delivered::Int { bytes: 8, unsigned: true },
        // ClickHouse's Bool is a UInt8 alias; keeping it Int8-unsigned means the
        // destination declares UInt8 and the byte passes through unchanged.
        "Bool" => Delivered::Int { bytes: 1, unsigned: true },
        "Float32" => Delivered::Float32,
        "Float64" => Delivered::Float64,
        "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" => {
            match decimal_ps(inner) {
                // Decimal256 and anything unparseable travel as text rather than
                // silently losing digits.
                Some((p, s)) if p <= 38 => Delivered::Decimal { p, s },
                _ => Delivered::Text,
            }
        }
        "Date" | "Date32" => Delivered::Date,
        "DateTime" | "DateTime64" => Delivered::DateTime { utc: true },
        "UUID" => Delivered::Uuid,
        "String" | "FixedString" => Delivered::Text,
        _ => Delivered::Text,
    }
}

/// `Decimal(18, 4)` / `Decimal64(4)` → (precision, scale).
fn decimal_ps(ty: &str) -> Option<(u16, u16)> {
    let args = ty.split_once('(')?.1.strip_suffix(')')?;
    let nums: Vec<u16> = args
        .split(',')
        .filter_map(|a| a.trim().parse::<u16>().ok())
        .collect();
    match (ty.split('(').next().unwrap_or(""), nums.as_slice()) {
        (_, [p, s]) => Some((*p, *s)),
        ("Decimal32", [s]) => Some((9, *s)),
        ("Decimal64", [s]) => Some((18, *s)),
        ("Decimal128", [s]) => Some((38, *s)),
        _ => None,
    }
}

/// The ClickHouse type the DESTINATION will declare for this delivery — the sink's
/// `ch_type_of` mirrored here, because the source's SELECT must cast to exactly it
/// for the passthrough to be byte-correct.
fn dest_type(d: &Delivered) -> String {
    match d {
        Delivered::Int { bytes, unsigned } => {
            let w = match bytes {
                1 => "8",
                2 => "16",
                4 => "32",
                _ => "64",
            };
            format!("{}Int{w}", if *unsigned { "U" } else { "" })
        }
        Delivered::Float32 => "Float32".into(),
        Delivered::Float64 => "Float64".into(),
        Delivered::Decimal { p: 0, .. } => "Float64".into(),
        Delivered::Decimal { p, s } => format!("Decimal({p}, {s})"),
        Delivered::Bool => "UInt8".into(),
        Delivered::Date => "Date32".into(),
        Delivered::DateTime { utc: false } => "DateTime64(6)".into(),
        Delivered::DateTime { utc: true } => "DateTime64(6, 'UTC')".into(),
        Delivered::Uuid => "UUID".into(),
        Delivered::Json | Delivered::Text | Delivered::Bytes => "String".into(),
    }
}

/// RowBinary payload width of a delivered type: `None` = length-prefixed varlen.
fn wire_width(d: &Delivered) -> Option<usize> {
    Some(match d {
        Delivered::Int { bytes, .. } => *bytes as usize,
        Delivered::Bool => 1,
        Delivered::Float32 => 4,
        Delivered::Float64 => 8,
        Delivered::Decimal { p: 0, .. } => 8,
        Delivered::Decimal { p, .. } if *p <= 9 => 4,
        Delivered::Decimal { p, .. } if *p <= 18 => 8,
        Delivered::Decimal { .. } => 16,
        Delivered::Date => 4,             // Date32
        Delivered::DateTime { .. } => 8,  // DateTime64(6)
        Delivered::Uuid => 16,
        Delivered::Json | Delivered::Text | Delivered::Bytes => return None,
    })
}

/// Per-column skip plan for the row counter.
#[derive(Clone, Copy)]
struct Skip {
    nullable: bool,
    width: Option<usize>,
}

/// Count whole rows in `buf`, returning `(rows, consumed_bytes)`. Never panics on
/// a truncated tail — it simply stops, and the caller carries the remainder into
/// the next chunk. A stream that ends with a non-empty remainder is corrupt (the
/// ClickHouse mid-stream exception lands there), which the caller turns into a
/// loud error.
fn count_rows(buf: &[u8], cols: &[Skip]) -> (u64, usize) {
    let mut pos = 0usize;
    let mut rows = 0u64;
    'outer: loop {
        let row_start = pos;
        for c in cols {
            if c.nullable {
                match buf.get(pos) {
                    // NULL writes the flag and NO payload — the trap that
                    // desynchronises every naive reader.
                    Some(1) => {
                        pos += 1;
                        continue;
                    }
                    Some(_) => pos += 1,
                    None => break 'outer,
                }
            }
            match c.width {
                Some(w) => {
                    if pos + w > buf.len() {
                        break 'outer;
                    }
                    pos += w;
                }
                None => match read_varint(buf, pos) {
                    Some((len, used)) => {
                        if pos + used + len > buf.len() {
                            break 'outer;
                        }
                        pos += used + len;
                    }
                    None => break 'outer,
                },
            }
        }
        rows += 1;
        debug_assert!(pos > row_start || cols.is_empty());
        if pos >= buf.len() {
            break;
        }
    }
    // `pos` may sit mid-row when the loop broke out; rewind to the last complete
    // row boundary by replaying the count (cheap: only the final partial row).
    let mut end = 0usize;
    let mut p = 0usize;
    for _ in 0..rows {
        for c in cols {
            if c.nullable {
                if buf[p] == 1 {
                    p += 1;
                    continue;
                }
                p += 1;
            }
            match c.width {
                Some(w) => p += w,
                None => {
                    let (len, used) = read_varint(buf, p).expect("counted row is complete");
                    p += used + len;
                }
            }
        }
        end = p;
    }
    (rows, end)
}

/// LEB128 varint (ClickHouse string lengths) → (value, bytes used).
fn read_varint(buf: &[u8], mut pos: usize) -> Option<(usize, usize)> {
    let start = pos;
    let mut val = 0usize;
    let mut shift = 0u32;
    loop {
        let b = *buf.get(pos)?;
        pos += 1;
        val |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            return Some((val, pos - start));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

impl ChSource {
    pub(crate) async fn connect(url: &str, _max_conns: usize) -> Result<Self> {
        let conn = ChConn::parse(url)?;
        // Fail fast on a bad URL/credentials, like every other source's connect.
        conn.exec("SELECT 1").await?;
        Ok(Self {
            conn,
            engine: std::sync::Mutex::new(String::new()),
        })
    }

    fn database(&self) -> String {
        self.conn.database().to_string()
    }
}

impl crate::source::Source for ChSource {
    async fn probe(&self, table: &str) -> Result<TablePlan> {
        let (db, name, _) = qualify(&self.database(), table);
        let rows = self
            .conn
            .exec(&format!(
                "SELECT name, type, is_in_primary_key FROM system.columns \
                 WHERE database = {} AND table = {} ORDER BY position FORMAT TSV",
                lit(&db),
                lit(&name)
            ))
            .await?;
        let mut cols = Vec::new();
        for line in rows.lines().filter(|l| !l.trim().is_empty()) {
            let mut f = line.split('\t');
            let (cname, ty, pk) = (
                f.next().unwrap_or_default().to_string(),
                f.next().unwrap_or_default().to_string(),
                f.next().unwrap_or("0") == "1",
            );
            let (inner, nullable) = peel(&ty);
            let d = delivered_of(&inner);
            let int_pk = pk && matches!(d, Delivered::Int { .. }) && !nullable;
            let (precision, scale) = match d {
                Delivered::Decimal { p, s } => (Some(p as i32), Some(s as i32)),
                _ => (None, None),
            };
            cols.push(ColumnPlan {
                name: cname,
                nullable,
                int_pk,
                native_ddl: Some(ty.clone()),
                udt: inner,
                precision,
                scale,
            });
        }
        if cols.is_empty() {
            return Err(Error::InvalidInput(format!(
                "clickhouse: table {db}.{name} not found (or has no columns)"
            )));
        }

        let meta = self
            .conn
            .exec(&format!(
                "SELECT engine, sorting_key FROM system.tables \
                 WHERE database = {} AND name = {} FORMAT TSV",
                lit(&db),
                lit(&name)
            ))
            .await?;
        let mut mf = meta.trim().split('\t');
        let engine = mf.next().unwrap_or_default().to_string();
        let sorting_key = mf.next().unwrap_or_default().to_string();
        *self.engine.lock().unwrap() = engine;

        // Cursor = the first sorting-key column when it is a non-nullable
        // integer. That is the only split that prunes granules; see span_stmts.
        let first_key = sorting_key
            .split(',')
            .next()
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_default();
        let cursor = cols
            .iter()
            .find(|c| c.name == first_key && matches!(delivered_of(&c.udt), Delivered::Int { .. }) && !c.nullable)
            .map(|c| c.name.clone());
        let pk_cols: Vec<String> = cols.iter().filter(|c| c.int_pk).map(|c| c.name.clone()).collect();

        Ok(TablePlan {
            engine: "clickhouse",
            cols,
            cursor,
            pk_cols,
        })
    }

    async fn catalog(
        &self,
        schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        let db = schema.map(|s| s.to_string()).unwrap_or_else(|| self.database());
        // total_rows is Nullable(UInt64): cast BEFORE ifNull or ClickHouse refuses
        // with NO_COMMON_TYPE (Int8 vs UInt64).
        let rows = self
            .conn
            .exec(&format!(
                "SELECT name, ifNull(toInt64(total_rows), -1) FROM system.tables \
                 WHERE database = {} AND engine LIKE '%MergeTree%' \
                   AND name NOT LIKE '\\_apitap%' AND name NOT LIKE '__apitap%' \
                 ORDER BY name FORMAT TSV",
                lit(&db)
            ))
            .await?;
        let found: Vec<(String, i64)> = rows
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let mut f = l.split('\t');
                (
                    f.next().unwrap_or_default().to_string(),
                    f.next().and_then(|v| v.parse().ok()).unwrap_or(-1),
                )
            })
            .collect();
        match tables {
            None => Ok(found),
            Some(want) => want
                .iter()
                .map(|w| {
                    let bare = w.split_once('.').map(|(_, t)| t).unwrap_or(w);
                    found
                        .iter()
                        .find(|(n, _)| n == bare)
                        .map(|(_, est)| (w.clone(), *est))
                        .ok_or_else(|| {
                            Error::InvalidInput(format!("clickhouse: table {w} not found in {db}"))
                        })
                })
                .collect(),
        }
    }

    /// Backslash is an escape character in this dialect, so it has to be
    /// doubled as well — see the trait's note.
    fn cursor_literal(&self, raw: &str) -> String {
        format!("'{}'", raw.replace('\\', "\\\\").replace('\'', "''"))
    }

    fn cursor_quoted(&self, udt: &str) -> Result<bool> {
        let (inner, _) = peel(udt);
        let head = inner.split('(').next().unwrap_or(&inner);
        match head {
            "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
                Ok(false)
            }
            "Date" | "Date32" | "DateTime" | "DateTime64" | "String" => Ok(true),
            other => Err(Error::InvalidInput(format!(
                "clickhouse: {other} is not usable as an incremental cursor"
            ))),
        }
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        // RowBinary only for now: it is what ClickHouse emits natively and what
        // the ClickHouse sink consumes, so a ch→ch hop needs no encoder at all.
        matches!(format, WireFormat::RowBinary)
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        debug_assert!(matches!(format, WireFormat::RowBinary));
        let cols = plan
            .cols
            .iter()
            .map(|c| {
                let d = delivered_of(&c.udt);
                let target = dest_type(&d);
                let col = q(&c.name);
                // Cast to the destination's declared type so the wire matches its
                // DDL byte-for-byte: Date is 2 bytes but lands as Date32 (4),
                // DateTime is 4 but lands as DateTime64(6) (8) — a raw passthrough
                // of those would corrupt every row after the first.
                let select = if matches!(d, Delivered::Text | Delivered::Json | Delivered::Bytes)
                    && c.udt != "String"
                {
                    format!("toString({col})")
                } else if c.udt == target {
                    col
                } else {
                    let full = if c.nullable {
                        format!("Nullable({target})")
                    } else {
                        target.clone()
                    };
                    format!("CAST({col} AS {full})")
                };
                LaneCol { delivered: d, select }
            })
            .collect();
        Lane {
            format,
            cols,
            raw_frames: false,
            push_where: None,
        }
    }

    async fn span_stmts(
        &self,
        table: &str,
        plan: &TablePlan,
        lane: &Lane,
        want: usize,
        delta: Option<&crate::plan::Delta>,
    ) -> Result<Vec<String>> {
        let (_, _, src) = qualify(&self.database(), table);
        let select_list = lane
            .cols
            .iter()
            .map(|c| c.select.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let dpred = delta
            .map(|d| format!(" AND {} {} {}", q(&d.col), d.op, d.literal))
            .unwrap_or_default();
        let one = |pred: String| {
            format!("SELECT {select_list} FROM {src} WHERE {pred} FORMAT RowBinary")
        };

        let engine = self.engine.lock().unwrap().clone();
        // Only a plain MergeTree gives every span the same row multiset. On the
        // collapsing family a merge between two span requests rewrites rows, so
        // N snapshots disagree — one span, one snapshot.
        let splittable = engine == "MergeTree" || engine == "ReplicatedMergeTree";
        let int_cursor = plan.cursor.as_deref().filter(|c| {
            plan.cols
                .iter()
                .any(|pc| pc.name == *c && matches!(delivered_of(&pc.udt), Delivered::Int { .. }))
        });

        if want > 1 && splittable {
            if let Some(col) = int_cursor {
                let qcol = q(col);
                // The count()=0 guard matters: on an empty table min/max return 0
                // (not NULL) and would fabricate a span.
                let probe = self
                    .conn
                    .exec(&format!(
                        "SELECT if(count() = 0, 'empty', concat(toString(min({qcol})), '\\t', \
                         toString(max({qcol})))) FROM {src} WHERE true{dpred} FORMAT TSVRaw"
                    ))
                    .await?;
                let probe = probe.trim();
                if probe == "empty" {
                    return Ok(vec![one("false".into())]);
                }
                if let Some((lo, hi)) = probe.split_once('\t') {
                    let (lo, hi) = (
                        lo.trim().parse::<i64>().map_err(|e| {
                            Error::Transfer(format!("clickhouse: cursor min → {e}"))
                        })?,
                        hi.trim().parse::<i64>().map_err(|e| {
                            Error::Transfer(format!("clickhouse: cursor max → {e}"))
                        })?,
                    );
                    return Ok(spans(lo, hi, want)
                        .into_iter()
                        .map(|(rlo, rhi)| {
                            one(format!("{qcol} >= {rlo} AND {qcol} <= {rhi}{dpred}"))
                        })
                        .collect());
                }
            }
        }
        Ok(vec![one(format!("true{dpred}"))])
    }

    async fn run_workers<L: Loader>(
        &self,
        _plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> Result<u64> {
        let skips: std::sync::Arc<Vec<Skip>> = std::sync::Arc::new(
            lane.cols
                .iter()
                .zip(_plan.cols.iter())
                .map(|(lc, pc)| Skip {
                    nullable: pc.nullable,
                    width: wire_width(&lc.delivered),
                })
                .collect(),
        );
        let queue = work_queue(stmts);
        let mut tasks = Vec::with_capacity(loaders.len());
        for loader in loaders {
            let conn = self.conn.clone();
            let queue = queue.clone();
            let skips = skips.clone();
            tasks.push(tokio::spawn(async move {
                worker(conn, queue, skips, loader, chunk).await
            }));
        }
        let mut rows = 0u64;
        for t in tasks {
            rows += t
                .await
                .map_err(|e| Error::Transfer(format!("join: {e}")))??;
        }
        Ok(rows)
    }
}

/// One worker: pull a span, stream its RowBinary body, count rows as they pass,
/// coalesce to ~`chunk` bytes on ROW boundaries (the loader framing contract),
/// and verify the span's row count before letting the transfer proceed.
async fn worker<L: Loader>(
    conn: ChConn,
    queue: WorkQueue,
    skips: std::sync::Arc<Vec<Skip>>,
    mut loader: L,
    chunk: usize,
) -> Result<u64> {
    let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
    let mut carry: Vec<u8> = Vec::new();
    let mut total = 0u64;
    while let Some(sql) = pop(&queue) {
        let expected = match count_of(&conn, &sql).await {
            Ok(n) => n,
            Err(e) => return Err(loader.abort(e).await),
        };
        let mut resp = match conn.query_stream(&sql).await {
            Ok(r) => r,
            Err(e) => return Err(loader.abort(e).await),
        };
        let mut got = 0u64;
        carry.clear();
        loop {
            let piece = match resp.chunk().await {
                Ok(Some(b)) => b,
                Ok(None) => break,
                Err(e) => {
                    return Err(loader
                        .abort(Error::Transfer(format!("clickhouse read: {e}")))
                        .await)
                }
            };
            carry.extend_from_slice(&piece);
            let (rows, consumed) = count_rows(&carry, &skips);
            got += rows;
            // Ship only whole rows; the tail waits for the next chunk.
            out.extend_from_slice(&carry[..consumed]);
            carry.drain(..consumed);
            if out.len() >= chunk {
                let fresh = loader
                    .reclaim()
                    .unwrap_or_else(|| Vec::with_capacity(chunk + 64 * 1024));
                let full = std::mem::replace(&mut out, fresh);
                loader.send(full).await?;
            }
        }
        // A ClickHouse read that dies mid-stream still answers HTTP 200 and glues
        // its exception onto the end of the body. Both symptoms land here.
        if !carry.is_empty() {
            return Err(loader
                .abort(Error::Transfer(format!(
                    "clickhouse: span ended mid-row with {} trailing bytes — the \
                     server aborted the query after sending a 200 (tail: {:?})",
                    carry.len(),
                    String::from_utf8_lossy(&carry[..carry.len().min(180)])
                )))
                .await);
        }
        if got != expected {
            return Err(loader
                .abort(Error::Transfer(format!(
                    "clickhouse: span returned {got} rows, count() says {expected} — \
                     refusing to swap a short read in"
                )))
                .await);
        }
        total += got;
    }
    if !out.is_empty() {
        loader.send(out).await?;
    }
    loader.finish().await?;
    Ok(total)
}

async fn count_of(conn: &ChConn, stmt: &str) -> Result<u64> {
    let tail = stmt
        .split_once(" FROM ")
        .map(|(_, r)| r)
        .and_then(|r| r.split(" FORMAT ").next())
        .ok_or_else(|| Error::Transfer("clickhouse: malformed span statement".into()))?;
    let body = conn.exec(&format!("SELECT count() FROM {tail}")).await?;
    body.trim()
        .parse::<u64>()
        .map_err(|e| Error::Transfer(format!("clickhouse: count() → {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peels_nullable_and_lowcardinality() {
        assert_eq!(peel("Int32"), ("Int32".into(), false));
        assert_eq!(peel("Nullable(String)"), ("String".into(), true));
        assert_eq!(peel("LowCardinality(String)"), ("String".into(), false));
        assert_eq!(
            peel("LowCardinality(Nullable(String))"),
            ("String".into(), true)
        );
        assert_eq!(peel("Decimal(18, 4)"), ("Decimal(18, 4)".into(), false));
    }

    #[test]
    fn maps_types_to_the_destination_spelling() {
        assert_eq!(dest_type(&delivered_of("Int32")), "Int32");
        assert_eq!(dest_type(&delivered_of("UInt64")), "UInt64");
        assert_eq!(dest_type(&delivered_of("Bool")), "UInt8");
        // The width traps: a 2-byte Date lands as a 4-byte Date32, a 4-byte
        // DateTime as an 8-byte DateTime64 — the lane must cast, never relay.
        assert_eq!(dest_type(&delivered_of("Date")), "Date32");
        assert_eq!(dest_type(&delivered_of("DateTime")), "DateTime64(6, 'UTC')");
        assert_eq!(dest_type(&delivered_of("Decimal(18, 4)")), "Decimal(18, 4)");
        assert_eq!(dest_type(&delivered_of("Array(String)")), "String");
        assert_eq!(dest_type(&delivered_of("Enum8('a' = 1)")), "String");
        assert_eq!(wire_width(&delivered_of("Date")), Some(4));
        assert_eq!(wire_width(&delivered_of("DateTime64(3)")), Some(8));
        assert_eq!(wire_width(&delivered_of("String")), None);
    }

    #[test]
    fn counts_rows_including_the_null_no_payload_rule() {
        let cols = [
            Skip { nullable: false, width: Some(4) },  // Int32
            Skip { nullable: true, width: None },      // Nullable(String)
        ];
        // row 1: 1, "hi"   row 2: 2, NULL
        let mut buf = vec![1, 0, 0, 0, 0, 2, b'h', b'i'];
        buf.extend_from_slice(&[2, 0, 0, 0, 1]);
        let (rows, used) = count_rows(&buf, &cols);
        assert_eq!((rows, used), (2, buf.len()));

        // A truncated tail counts only the whole rows and reports where they end.
        let (rows, used) = count_rows(&buf[..buf.len() - 2], &cols);
        assert_eq!(rows, 1);
        assert_eq!(used, 8);
    }

    #[test]
    fn varint_roundtrip() {
        assert_eq!(read_varint(&[0x05], 0), Some((5, 1)));
        assert_eq!(read_varint(&[0xac, 0x02], 0), Some((300, 2)));
        assert_eq!(read_varint(&[0x80], 0), None); // truncated
    }

    #[test]
    fn identifiers_and_literals_are_escaped() {
        assert_eq!(q("plain"), "`plain`");
        assert_eq!(q("we`ird"), "`we``ird`");
        assert_eq!(lit("o'hara"), "'o\\'hara'");
    }
}
