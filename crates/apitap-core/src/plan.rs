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
    /// The cursor column, kept when a read() projection drops it from
    /// `cols`: span planning finds the cursor's type there, and a projection
    /// without it fell back to ONE statement on MySQL — `read(columns=..,
    /// parallel=8)` ran single-stream, silently (P2).
    pub cursor_col: Option<ColumnPlan>,
}

impl TablePlan {
    /// The cursor's column plan: in `cols`, or kept aside by a projection.
    pub(crate) fn cursor_plan(&self) -> Option<&ColumnPlan> {
        let c = self.cursor.as_deref()?;
        self.cols.iter().find(|pc| pc.name == c).or(self.cursor_col.as_ref().filter(|pc| pc.name == c))
    }

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
    /// PgCopyBinary only: deliver span payloads verbatim (each span keeps its
    /// own COPY header/trailer) instead of stripping to one synthetic stream.
    /// The Arrow read path sets this — its builder walks spans natively, so
    /// the strip-and-recopy stage would be a pure memcpy tax.
    pub raw_frames: bool,
    /// Pushed-down query predicate, ANDed into every span statement
    /// (including min/max probes — it only narrows the ranges). Set by the
    /// read path from [`crate::ReadOptions::push_where`]; transfers leave it
    /// `None`.
    pub push_where: Option<String>,
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

/// Whether a cursor column's source type compares as a NUMBER: a positive
/// list, case-insensitive, over the Postgres, MySQL and ClickHouse spellings.
/// The sinks asked "not a date/timestamp?", so text/varchar/String and
/// ClickHouse DateTime64/Date32 cursors were treated as numeric, both
/// watermarks failed to parse, and the arbitration silently took the
/// destination's max — a regressed max re-landed rows (P2).
pub(crate) fn is_numeric_udt(udt: &str) -> bool {
    let lower = udt.trim().to_ascii_lowercase();
    let inner = lower
        .strip_prefix("nullable(")
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(&lower);
    let head = inner.split(|c: char| c == '(' || c == ' ').next().unwrap_or(inner);
    matches!(
        head,
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" | "tinyint" | "mediumint" | "int"
            | "numeric" | "decimal" | "float4" | "float8" | "real" | "double" | "float"
            | "int16" | "int32" | "int64" | "int128" | "int256"
            | "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "uint256"
            | "float32" | "float64" | "decimal32" | "decimal64" | "decimal128" | "decimal256"
            | "oid"
    )
}

/// A Postgres numeric(p, s) as a destination decimal can hold it. PG15+
/// accepts a negative scale (numeric(5,-2) holds 9999900: p - s integer
/// digits) and a scale past the precision (numeric(2,5) holds 0.00012):
/// declared as-is they became Decimal(5,0), which stores 9999900 out of its
/// precision, and Decimal(2,5), which ClickHouse refuses (P2).
pub(crate) fn decimal_shape(p: i32, s: i32) -> (i32, i32) {
    if s < 0 { (p - s, 0) } else { (p.max(s), s) }
}

/// Compare two numeric watermark texts exactly: integers, decimals
/// (`-12.50`), and — through f64 — exponent forms. `None` when either is not
/// a number. Integers alone used to parse, so two DECIMAL cursors fell back
/// to string order and "9.75" beat "12.5": the watermark stalled on the
/// smaller value (P2, S7-16).
pub(crate) fn cmp_numeric_text(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    fn parts(s: &str) -> Option<(bool, &str, &str)> {
        let s = s.trim();
        let (neg, s) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        let (i, f) = s.split_once('.').unwrap_or((s, ""));
        if (i.is_empty() && f.is_empty())
            || !i.bytes().all(|c| c.is_ascii_digit())
            || !f.bytes().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        Some((neg, i.trim_start_matches('0'), f.trim_end_matches('0')))
    }
    use std::cmp::Ordering;
    match (parts(a), parts(b)) {
        (Some((an, ai, af)), Some((bn, bi, bf))) => {
            let zero = |i: &str, f: &str| i.is_empty() && f.is_empty();
            let (an, bn) = (an && !zero(ai, af), bn && !zero(bi, bf));
            if an != bn {
                return Some(if an {
                    Ordering::Less
                } else {
                    Ordering::Greater
                });
            }
            let mag = ai
                .len()
                .cmp(&bi.len())
                .then_with(|| ai.cmp(bi))
                .then_with(|| af.cmp(bf));
            Some(if an { mag.reverse() } else { mag })
        }
        _ => {
            let (x, y) = (a.trim().parse::<f64>().ok()?, b.trim().parse::<f64>().ok()?);
            x.partial_cmp(&y)
        }
    }
}

/// The larger of two watermark texts — numeric cursors compare numerically,
/// text cursors (timestamps in the source's own rendering) lexicographically.
pub(crate) fn wm_max(a: Option<String>, b: Option<String>, numeric: bool) -> Option<String> {
    match (a, b) {
        (Some(x), Some(y)) => {
            let x_wins = if numeric {
                match cmp_numeric_text(&x, &y) {
                    Some(o) => o != std::cmp::Ordering::Less,
                    None => match (cmp_numeric_text(&x, &x), cmp_numeric_text(&y, &y)) {
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        _ => x >= y,
                    },
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
        match cmp_numeric_text(&state, &data) {
            Some(o) if o != std::cmp::Ordering::Less => state,
            Some(_) => data,
            None if cmp_numeric_text(&state, &state).is_some() => state,
            None => data,
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
    /// The state write lands AFTER the swap (ClickHouse ATTACH, MySQL RENAME,
    /// BigQuery copy job): a crash between them leaves state one run behind, so
    /// the effective watermark is the fresher of the two.
    ///
    /// This is a real safety net and it covers the common direction: when the
    /// replace moved the cursor FORWARD, the data max is higher than the stale
    /// state row, the data max wins, and the next append does not re-land rows
    /// the replace already holds.
    ///
    /// It does NOT cover the other direction, and the comment here used to say
    /// "never a skip", which was wrong. If the replace moved the cursor
    /// BACKWARD — the source shrank, a partition was dropped, a purge ran —
    /// the stale state row is the HIGHER value and wins, so the next append
    /// starts above the data that is actually there and skips whatever lands
    /// in between. `Greatest` cannot tell that case from an ordinary foreign
    /// delete against the destination, where trusting the state row is exactly
    /// right. Telling them apart needs to know whether the last replace
    /// finished its bookkeeping, which is what the `pending` column in 0.56.0
    /// records. Until then this is a partial guard, and saying so is the point
    /// of this paragraph.
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
            // Sibling state rows mean FAN-IN, and the data max covers the
            // other sources' rows too: source B at 40 reading max(40, 100)
            // resumes at 100 and its 41..100 are never loaded — green
            // (MASTER M-5). With siblings present the state row alone is the
            // truth; the data max only arbitrates a single-source table.
            WmArbitration::Greatest { .. } if sibling_state_rows => s,
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
    /// P2: a projection that drops the cursor keeps it for span planning.
    #[test]
    fn a_projection_without_the_cursor_still_knows_it() {
        let col = |n: &str, udt: &str| super::ColumnPlan {
            name: n.into(),
            nullable: false,
            int_pk: n == "id",
            native_ddl: None,
            udt: udt.into(),
            precision: None,
            scale: None,
        };
        let mut plan = super::TablePlan {
            engine: "mysql",
            cols: vec![col("id", "int"), col("name", "varchar")],
            cursor: Some("id".into()),
            pk_cols: vec!["id".into()],
            cursor_col: None,
        };
        // As read_impl narrows a projection to ["name"].
        plan.cursor_col = plan.cursor_plan().cloned();
        plan.cols.retain(|c| c.name == "name");
        assert_eq!(plan.cursor_plan().map(|c| c.udt.as_str()), Some("int"));
        plan.cursor = Some("other".into());
        assert!(plan.cursor_plan().is_none(), "a different cursor is not the kept one");
    }

    /// P2: numeric(5,-2) needs 7 integer digits; numeric(2,5) needs p >= s.
    #[test]
    fn decimal_shape_widens_negative_and_oversized_scales() {
        assert_eq!(super::decimal_shape(5, -2), (7, 0));
        assert_eq!(super::decimal_shape(2, 5), (5, 5));
        assert_eq!(super::decimal_shape(10, 2), (10, 2));
    }

    /// P2: only number types arbitrate numerically.
    #[test]
    fn numeric_cursor_types_are_a_positive_list() {
        for t in ["int8", "INT", "bigint", "numeric", "decimal(10,2)", "UInt64", "Nullable(Int32)", "Float64", "double"] {
            assert!(super::is_numeric_udt(t), "{t}");
        }
        for t in ["text", "varchar", "String", "DateTime64(3)", "Date32", "timestamptz", "date", "uuid", "bool"] {
            assert!(!super::is_numeric_udt(t), "{t}");
        }
    }

    /// P2 (S7-16): DECIMAL watermarks compare as numbers, not text.
    #[test]
    fn decimal_watermarks_compare_numerically() {
        use std::cmp::Ordering::*;
        assert_eq!(super::cmp_numeric_text("12.5", "9.75"), Some(Greater));
        assert_eq!(super::cmp_numeric_text("-12.5", "-9.75"), Some(Less));
        assert_eq!(super::cmp_numeric_text("1.50", "1.5"), Some(Equal));
        assert_eq!(super::cmp_numeric_text("-0.0", "0"), Some(Equal));
        assert_eq!(super::cmp_numeric_text("100", "99.999"), Some(Greater));
        assert_eq!(super::cmp_numeric_text("1e3", "999.5"), Some(Greater));
        assert_eq!(super::cmp_numeric_text("abc", "1"), None);
        assert_eq!(
            super::wm_max(Some("9.75".into()), Some("12.5".into()), true).as_deref(),
            Some("12.5")
        );
        assert_eq!(super::wm_pick(true, "9.75".into(), "12.5".into()), "12.5");
        assert_eq!(super::wm_pick(true, "12.5".into(), "9.75".into()), "12.5");
        assert_eq!(super::wm_pick(true, "junk".into(), "9.75".into()), "9.75");
    }

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
        // MASTER M-5: with sibling state rows the state row is authoritative
        // — max(own, data) would adopt a more advanced sibling's cursor and
        // skip this source's backlog.
        assert_eq!(r(Some("40"), Some("100"), true, Greatest { numeric: true }).unwrap(), Some("40".into()));
        assert_eq!(r(Some("40"), Some("100"), true, StateAuthoritative).unwrap(), Some("40".into()));
        assert_eq!(r(Some("garbage"), Some("9"), false, Greatest { numeric: true }).unwrap(), Some("9".into()));
        // fan-in: no own row + siblings = loud error
        assert!(r(None, Some("9"), true, Greatest { numeric: true }).is_err());
        assert!(r(None, None, true, StateAuthoritative).is_err());
        // no state anywhere: data is the only truth
        assert_eq!(r(None, Some("9"), false, StateAuthoritative).unwrap(), Some("9".into()));
        assert_eq!(r(None, None, false, Greatest { numeric: false }).unwrap(), None);
    }

    /// What `Greatest` does and does not protect, both pinned.
    ///
    /// The audit filed the swap-before-state ordering on ClickHouse, MySQL and
    /// BigQuery as unguarded. Half of that was already wrong: `Greatest` is the
    /// guard, and it has been there the whole time. This test says which half.
    #[test]
    fn greatest_covers_a_stale_state_row_forward_but_not_backward() {
        use WmArbitration::*;
        let r = |o: &str, d: &str| {
            resolve_watermark(
                Some(o.into()), Some(d.into()), false, Greatest { numeric: true }, "t", "src",
            )
            .unwrap()
            .unwrap()
        };
        // COVERED. A replace moved the cursor forward and then failed before
        // clearing state. The stale row says 100, the table actually holds
        // 5000. Taking 5000 means the next append does not re-land 100..5000 —
        // which on a keyless destination would be 4900 duplicate rows under a
        // green run.
        assert_eq!(r("100", "5000"), "5000");
        // NOT COVERED, and this assertion exists to keep that visible. The
        // replace moved the cursor BACKWARD (the source shrank) and then failed
        // the same way. The stale row says 5000, the table holds 3000, and 5000
        // wins — so rows landing in 3000..5000 later are skipped. `Greatest`
        // cannot distinguish this from a foreign delete against the
        // destination, where trusting 5000 is correct. 0.56.0's `pending`
        // column is what tells the two apart; until then this is the gap.
        assert_eq!(r("5000", "3000"), "5000");
    }
}
