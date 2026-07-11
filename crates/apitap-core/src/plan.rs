//! The neutral table model shared by every connector.
//!
//! Sources probe their catalog into a [`TablePlan`] (plus the source-native facts a
//! same-engine sink needs to mirror types exactly). Lane planning then turns the plan
//! into a [`Lane`]: the wire format that will cross the process and, per column, what
//! logical value the encoder DELIVERS in that format — which is all a sink needs to
//! declare its DDL. This is what keeps type mapping O(sources + sinks) instead of a
//! table per (source, sink) pair.

/// What actually crosses the wire for one column, after the lane's encoder ran.
/// Sinks map this — and only this — to their column DDL.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Delivered {
    /// Fixed-width integer of `bytes` ∈ {1,2,4,8}.
    Int {
        bytes: u8,
        unsigned: bool,
    },
    Float32,
    Float64,
    /// Exact decimal scaled to `s`. `p == 0` means "unconstrained" (declare a bare
    /// `numeric`); sinks that cannot express that fall back to their widest form.
    Decimal {
        p: u16,
        s: u16,
    },
    Bool,
    /// Days since the Unix epoch.
    Date,
    /// Microsecond timestamp; `utc` = the value is an absolute instant.
    DateTime {
        utc: bool,
    },
    Uuid,
    Json,
    Text,
    Bytes,
}

/// One source column: neutral facts + the source-native facts (`native_ddl`, `udt`,
/// precision/scale) that per-source lane planners and same-engine sinks consult.
#[derive(Clone, Debug)]
pub(crate) struct ColumnPlan {
    pub name: String,
    pub nullable: bool,
    /// Member of a single-column integer primary key (cursor auto-detect).
    pub int_pk: bool,
    /// The source's own full type spelling (`format_type` on Postgres, COLUMN_TYPE on
    /// MySQL) — lets a same-engine sink mirror the type byte-exactly.
    pub native_ddl: Option<String>,
    /// Source type name the lane planners key on (udt_name / DATA_TYPE).
    pub udt: String,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

/// A probed source table with the resolved range-split cursor.
#[derive(Clone, Debug)]
pub(crate) struct TablePlan {
    /// Source engine tag (`"postgres"`, `"mysql"`) — sinks use it to decide whether
    /// `native_ddl` can be mirrored.
    pub engine: &'static str,
    pub cols: Vec<ColumnPlan>,
    /// Range-split column (caller's choice or the auto-detected integer PK).
    pub cursor: Option<String>,
}

impl TablePlan {
    /// The auto-detect rule: exactly one integer-PK column.
    pub(crate) fn single_int_pk(&self) -> Option<String> {
        let mut it = self.cols.iter().filter(|c| c.int_pk);
        match (it.next(), it.next()) {
            (Some(c), None) => Some(c.name.clone()),
            _ => None,
        }
    }
}

/// Stream formats that can cross the process. Negotiation picks the FIRST format the
/// sink accepts (ranked best-first) that the source can produce for this plan.
///
/// Cost model for contributors: formats are the unit of reuse. The lifecycle (probe,
/// staging, spans, workers, swap) is O(sources + sinks), but a DATA-PLANE encoder is
/// per (source, format) — a new sink that introduces a format no source produces yet
/// costs one encoder per source that wants the pair. That trade is deliberate: there
/// is no neutral in-memory IR (Arrow etc.), because the fast lanes ARE the product.
/// Prefer reusing an existing format when the sink's parser can be configured to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WireFormat {
    /// Postgres binary-COPY framing (header + length-prefixed tuples + trailer).
    /// Byte-relay: buffers are NOT record-aligned.
    PgCopyBinary,
    /// ClickHouse RowBinary (fields back-to-back, little-endian). Row-oriented:
    /// buffers end on a record boundary.
    RowBinary,
    /// Postgres `text` COPY dialect: tab delimiter, `\N` NULLs, `\t\n\r\\` escapes
    /// (corner case: a literal vertical tab escapes as `\v`, which not every parser
    /// unescapes). ClickHouse TabSeparated parses it natively; other sinks can often
    /// be configured to (e.g. FIELD_DELIMITER='\t', NULL_IF=('\\N')). Row-oriented:
    /// buffers end on a record boundary.
    TabSeparated,
}

/// One column as the lane's encoder will deliver it.
#[derive(Clone, Debug)]
pub(crate) struct LaneCol {
    pub delivered: Delivered,
    /// Source-side SELECT expression (casts the lane needs, e.g. `bool::int` for text).
    pub select: String,
}

/// A negotiated (format, per-column delivery) pair.
#[derive(Clone, Debug)]
pub(crate) struct Lane {
    pub format: WireFormat,
    pub cols: Vec<LaneCol>,
}
