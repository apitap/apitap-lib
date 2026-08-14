//! Raw change capture for `changelog=true` — the append-only CDC shape.
//!
//! The default apply path COLLAPSES a window (last write wins per key) because
//! the destination is a replica and only the final image matters. A changelog
//! destination wants the opposite: EVERY operation, in WAL order, so the table
//! is an audit trail. This accumulator is the collapse-free sibling of
//! [`crate::logbased::collapse::Collapser`] — same input events, no dedup.
//!
//! Cost of keeping everything: a key updated ten times inside one window lands
//! ten rows instead of one. Memory is unchanged — the drain's byte budget
//! already counts every buffered event; collapse only ever shrank the window
//! BELOW that budget, it never raised the ceiling.

use crate::wire::pgoutput::Tuple;

/// What happened to a row, as it appears in the destination's `_apitap_op`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl ChangeOp {
    /// The single character written to `_apitap_op`.
    pub(crate) fn code(self) -> &'static str {
        match self {
            ChangeOp::Insert => "I",
            ChangeOp::Update => "U",
            ChangeOp::Delete => "D",
            ChangeOp::Truncate => "T",
        }
    }
}

/// One captured operation. `row` is the NEW image for insert/update and the
/// OLD image for delete (under REPLICA IDENTITY DEFAULT that carries the key
/// columns and NULLs elsewhere — which is exactly what a delete record means).
/// A truncate carries no row.
#[derive(Debug)]
pub(crate) struct Change {
    pub op: ChangeOp,
    pub row: Option<Tuple>,
}

/// One table's captured window, in WAL order.
#[derive(Debug, Default)]
pub(crate) struct Changes {
    pub events: Vec<Change>,
    /// Row events consumed (for reporting) — matches `Collapsed::events`.
    pub count: u64,
}

impl Changes {
    pub(crate) fn insert(&mut self, row: Tuple) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Insert, row: Some(row) });
    }

    /// An update lands as ONE `U` carrying the new image. A PK-changing update
    /// additionally lands a `D` for the old identity FIRST, so a consumer
    /// replaying the log sees the old key die before the new one appears —
    /// the same ordering the collapsed path encodes as delete-then-upsert.
    pub(crate) fn update(&mut self, old: Option<&Tuple>, row: Tuple, pk_idx: &[usize]) {
        self.count += 1;
        if let Some(old) = old {
            if key_of(old, pk_idx) != key_of(&row, pk_idx) {
                self.events.push(Change { op: ChangeOp::Delete, row: Some(old.clone()) });
            }
        }
        self.events.push(Change { op: ChangeOp::Update, row: Some(row) });
    }

    pub(crate) fn delete(&mut self, old: Tuple) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Delete, row: Some(old) });
    }

    /// TRUNCATE is captured as its own record, NOT as a wipe: the log keeps
    /// what came before it. A consumer deriving current state treats every
    /// row of that table older than the truncate as gone.
    pub(crate) fn truncate(&mut self) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Truncate, row: None });
    }
}

/// Key cells of a row at the given indices — only used to spot a PK change.
fn key_of(row: &Tuple, pk_idx: &[usize]) -> Vec<Vec<u8>> {
    crate::logbased::rowtext::row_key_refs(row, pk_idx)
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pgoutput::Cell;

    fn t(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
    fn row(cells: &[Cell]) -> Tuple {
        Tuple::from_cells(cells)
    }

    #[test]
    fn every_operation_is_kept_in_order() {
        let mut c = Changes::default();
        c.insert(row(&[t("1"), t("a")]));
        c.update(None, row(&[t("1"), t("a2")]), &[0]);
        c.update(None, row(&[t("1"), t("a3")]), &[0]);
        c.delete(row(&[t("1"), Cell::Null]));
        // Collapse would leave ONE delete. The changelog keeps all four.
        assert_eq!(c.count, 4);
        let ops: Vec<ChangeOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Update, ChangeOp::Delete]
        );
    }

    #[test]
    fn pk_change_emits_delete_of_the_old_identity_first() {
        let mut c = Changes::default();
        c.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("9"), t("a")]), &[0]);
        let ops: Vec<ChangeOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(ops, vec![ChangeOp::Delete, ChangeOp::Update]);
        // …but it is ONE source event.
        assert_eq!(c.count, 1);
    }

    #[test]
    fn same_key_update_does_not_emit_a_delete() {
        let mut c = Changes::default();
        c.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("1"), t("z")]), &[0]);
        assert_eq!(c.events.len(), 1);
        assert_eq!(c.events[0].op, ChangeOp::Update);
    }

    #[test]
    fn truncate_is_a_record_not_a_wipe() {
        let mut c = Changes::default();
        c.insert(row(&[t("1"), t("a")]));
        c.truncate();
        c.insert(row(&[t("2"), t("b")]));
        let ops: Vec<ChangeOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(ops, vec![ChangeOp::Insert, ChangeOp::Truncate, ChangeOp::Insert]);
    }
}
