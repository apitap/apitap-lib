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
    /// The source's full primary-key column list (any types, composite included) —
    /// a merge-mode bootstrap recreates it on the destination so the NEXT run can
    /// upsert against it.
    pub pk_cols: Vec<String>,
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
    /// MySQL `LOAD DATA ... FIELDS ESCAPED BY '\\'` dialect: tab delimiter, `\N`
    /// NULLs, `\t\n\r\\\0\Z` escapes ([`crate::wire::mytsv`]). SPLIT from
    /// [`WireFormat::TabSeparated`] on purpose: the two vocabularies differ in
    /// corner bytes, and one enum value for both let negotiation pair a MySQL
    /// producer with a Postgres-dialect consumer — silent corruption. Now the
    /// type system forbids it. Row-oriented: buffers end on a record boundary.
    MyTsv,
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

/// Incremental delta filter, pushed into every source read statement (the min/max
/// probe, every range span, the PK-less fallbacks, and the single-stream statement).
#[derive(Clone, Debug)]
pub(crate) struct Delta {
    /// Cursor column name (source-side).
    pub col: String,
    /// `>` for append (rows at the watermark are already loaded), `>=` for merge
    /// (the upsert dedupes, so re-reading the boundary is safe and loses nothing).
    pub op: &'static str,
    /// Watermark as a ready-to-embed SQL literal (quoted if the type needs it).
    pub literal: String,
}

/// What the destination looks like before an incremental run.
#[derive(Clone, Debug)]
pub(crate) struct DestState {
    /// Does the final table exist? `false` → the run bootstraps as a full replace.
    pub exists: bool,
    /// `max(cursor)` in the destination as text; `None` when the table is empty.
    pub watermark: Option<String>,
}

/// The larger of two watermark texts — numeric cursors compare numerically,
/// text cursors (timestamps in the source's own rendering) lexicographically.
pub(crate) fn wm_max(a: Option<String>, b: Option<String>, numeric: bool) -> Option<String> {
    match (a, b) {
        (Some(x), Some(y)) => {
            let x_wins = if numeric {
                match (x.parse::<i128>(), y.parse::<i128>()) {
                    (Ok(xi), Ok(yi)) => xi >= yi,
                    (Ok(_), Err(_)) => true,
                    (Err(_), Ok(_)) => false,
                    _ => x >= y,
                }
            } else {
                x >= y
            };
            Some(if x_wins { x } else { y })
        }
        (a, None) => a,
        (None, b) => b,
    }
}

/// Pick the fresher of a STATE watermark and a DATA watermark. Numeric cursors
/// compare numerically; text (timestamps render sortably) lexicographically.
/// An unparseable state value loses to the data max — the safe direction: worst
/// case is a bounded re-read, never a skip. (One copy — the 0.3.0-reviewed
/// semantics; per-sink clones of this drifted once already.)
pub(crate) fn wm_pick(numeric: bool, state: String, data: String) -> String {
    if numeric {
        match (state.parse::<i128>(), data.parse::<i128>()) {
            (Ok(s), Ok(d)) => {
                if s >= d {
                    state
                } else {
                    data
                }
            }
            (Ok(_), Err(_)) => state,
            _ => data,
        }
    } else if state >= data {
        state
    } else {
        data
    }
}

/// How a sink arbitrates when BOTH a state row and a data max exist — this
/// differs BY DESIGN with the sink's atomicity, it is not drift:
pub(crate) enum WmArbitration {
    /// The state row is written in the SAME transaction as the land (Postgres):
    /// it is authoritative alone, and the data max is deliberately distrusted —
    /// foreign writes to the destination would poison it.
    StateAuthoritative,
    /// The state write lands AFTER the swap (ClickHouse ATTACH, MySQL RENAME):
    /// a crash between them leaves state one run behind, so the effective
    /// watermark is the fresher of the two — a bounded re-read, never a skip.
    Greatest { numeric: bool },
}

/// The incremental-watermark DECISION shared by every sink (the fetches stay
/// per-sink — each database reads its own state its own way, as lazily as it
/// likes). Invariants, from the 0.3.0 review, in one place instead of four:
///
/// - a missing own-state row with SIBLING rows present fails loudly (fan-in:
///   the shared data max belongs to the most advanced source — falling back
///   would silently skip THIS source's backlog),
/// - no state anywhere → the data max is the only truth,
/// - both present → per-sink [`WmArbitration`].
pub(crate) fn resolve_watermark(
    own_state: Option<String>,
    data_max: Option<String>,
    sibling_state_rows: bool,
    arb: WmArbitration,
    dest: &str,
    source_id: &str,
) -> crate::error::Result<Option<String>> {
    match own_state {
        Some(s) => Ok(Some(match arb {
            WmArbitration::StateAuthoritative => s,
            WmArbitration::Greatest { numeric } => match data_max {
                Some(d) => wm_pick(numeric, s, d),
                None => s,
            },
        })),
        None if sibling_state_rows => Err(crate::error::Error::InvalidInput(format!(
            "destination {dest} has state rows from other sources but none for \
             '{source_id}' — a data-derived watermark would be wrong here. \
             Run mode='replace' to rebuild, or seed a state row manually"
        ))),
        None => Ok(data_max),
    }
}

#[cfg(test)]
mod wm_tests {
    use super::*;

    #[test]
    fn resolve_watermark_holds_the_three_invariants() {
        use WmArbitration::*;
        let r = |o: Option<&str>, d: Option<&str>, sib, arb| {
            resolve_watermark(
                o.map(String::from), d.map(String::from), sib, arb, "t", "src",
            )
        };
        // state-authoritative: data max is ignored when a state row exists
        assert_eq!(r(Some("5"), Some("9"), false, StateAuthoritative).unwrap(), Some("5".into()));
        // greatest: fresher of the two, garbage state loses
        assert_eq!(r(Some("5"), Some("9"), false, Greatest { numeric: true }).unwrap(), Some("9".into()));
        assert_eq!(r(Some("garbage"), Some("9"), false, Greatest { numeric: true }).unwrap(), Some("9".into()));
        // fan-in: no own row + siblings = loud error
        assert!(r(None, Some("9"), true, Greatest { numeric: true }).is_err());
        assert!(r(None, None, true, StateAuthoritative).is_err());
        // no state anywhere: data is the only truth
        assert_eq!(r(None, Some("9"), false, StateAuthoritative).unwrap(), Some("9".into()));
        assert_eq!(r(None, None, false, Greatest { numeric: false }).unwrap(), None);
    }
}
