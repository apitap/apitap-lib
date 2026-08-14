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

use crate::error::{Error, Result};
use crate::wire::pgoutput::{Cell, Cellv, Tuple};
use std::collections::HashMap;

/// A row's replica-identity key, owned — only used to line an event up with the
/// value a masked column still holds.
pub(crate) type CKey = Vec<Vec<u8>>;

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
    /// Any event carries an unchanged-TOAST cell, so the apply has to resolve
    /// them before writing. Tracked here so the overwhelmingly common window —
    /// no TOAST at all — pays nothing for the machinery below.
    pub masked: bool,
}

impl Changes {
    /// O(cols) once, and only until the first masked row is seen.
    fn note_mask(&mut self, row: &Tuple) {
        if !self.masked {
            self.masked = (0..row.len()).any(|i| matches!(row.view(i), Cellv::UnchangedToast));
        }
    }

    pub(crate) fn insert(&mut self, row: Tuple) {
        self.count += 1;
        self.note_mask(&row);
        self.events.push(Change { op: ChangeOp::Insert, row: Some(row) });
    }

    /// An update lands as ONE `U` carrying the new image. A PK-changing update
    /// additionally lands a `D` for the old identity FIRST, so a consumer
    /// replaying the log sees the old key die before the new one appears —
    /// the same ordering the collapsed path encodes as delete-then-upsert.
    pub(crate) fn update(&mut self, old: Option<&Tuple>, row: Tuple, pk_idx: &[usize]) {
        self.count += 1;
        self.note_mask(&row);
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

    /// Which keys carry a masked cell, and the union of the columns they are
    /// missing — the shopping list for ONE readback per window.
    pub(crate) fn mask_plan(&self, pk_idx: &[usize]) -> (Vec<CKey>, Vec<usize>) {
        let mut keys: Vec<CKey> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut cols = std::collections::BTreeSet::new();
        for ev in &self.events {
            let Some(row) = &ev.row else { continue };
            let mut any = false;
            for i in 0..row.len() {
                if matches!(row.view(i), Cellv::UnchangedToast) {
                    cols.insert(i);
                    any = true;
                }
            }
            if any {
                let k = key_of(row, pk_idx);
                if seen.insert(k.clone()) {
                    keys.push(k);
                }
            }
        }
        (keys, cols.into_iter().collect())
    }

    /// Replace every unchanged-TOAST cell with the value that column still
    /// holds, returning the rebuilt rows by event index (masked rows only —
    /// everything else keeps its zero-copy frame).
    ///
    /// An UPDATE that leaves a TOASTed column alone omits it from the WAL. The
    /// replica path handles that with a per-row mask and keeps the
    /// destination's value; a changelog cannot, because each record is read on
    /// its own and `<table>__current` picks whole records. Writing NULL there
    /// would silently destroy the column for every reader of the view, which is
    /// the single worst thing a CDC tool can do — so the value is reconstructed
    /// instead, from the last event IN THIS WINDOW that carried it, else from
    /// `base`: the destination's current value, read back once per window.
    ///
    /// A cell neither source can fill is a row the destination has never seen —
    /// a torn window, not a NULL. It is refused loudly.
    pub(crate) fn resolve_masked(
        &self,
        pk_idx: &[usize],
        cols: &[usize],
        base: &HashMap<CKey, Vec<Option<bytes::Bytes>>>,
        names: &[String],
    ) -> Result<HashMap<usize, Tuple>> {
        let mut carry: HashMap<CKey, HashMap<usize, Option<bytes::Bytes>>> = HashMap::new();
        let mut out = HashMap::new();
        for (idx, ev) in self.events.iter().enumerate() {
            let Some(row) = &ev.row else { continue };
            let n = row.len();
            let mut masked_any = false;
            let mut cells: Vec<Cell> = Vec::with_capacity(n);
            for i in 0..n {
                cells.push(match row.view(i) {
                    Cellv::Null => Cell::Null,
                    Cellv::Text(t) => Cell::Text(bytes::Bytes::copy_from_slice(t)),
                    Cellv::UnchangedToast => {
                        masked_any = true;
                        Cell::UnchangedToast
                    }
                });
            }
            let key = key_of(row, pk_idx);
            // What THIS event carried becomes the value later events inherit.
            let slot = carry.entry(key.clone()).or_default();
            for (i, c) in cells.iter().enumerate() {
                match c {
                    Cell::UnchangedToast => {}
                    Cell::Null => {
                        slot.insert(i, None);
                    }
                    Cell::Text(t) => {
                        slot.insert(i, Some(t.clone()));
                    }
                }
            }
            if !masked_any {
                continue;
            }
            for i in 0..n {
                if !matches!(cells[i], Cell::UnchangedToast) {
                    continue;
                }
                let from_window = slot.get(&i).cloned();
                let from_dest = || {
                    cols.iter()
                        .position(|&c| c == i)
                        .and_then(|p| base.get(&key).and_then(|b| b.get(p).cloned()))
                };
                let Some(v) = from_window.or_else(from_dest) else {
                    return Err(Error::Transfer(format!(
                        "log_based changelog: column '{}' arrived as unchanged-TOAST for a \
                         row the destination has never seen — the window is torn. Clear \
                         this table's _apitap_state row to re-bootstrap",
                        names.get(i).map(String::as_str).unwrap_or("?")
                    )));
                };
                cells[i] = match v {
                    Some(b) => Cell::Text(b),
                    None => Cell::Null,
                };
            }
            out.insert(idx, Tuple::from_cells(&cells));
        }
        Ok(out)
    }

    /// TRUNCATE is captured as its own record, NOT as a wipe: the log keeps
    /// what came before it. A consumer deriving current state treats every
    /// row of that table older than the truncate as gone.
    pub(crate) fn truncate(&mut self) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Truncate, row: None });
    }
}

/// Key cells of a row at the given indices.
///
/// `Tuple::view` INDEXES and panics past the end; a short old image is a real
/// wire shape, not a bug, so this reads through `get` and treats a missing or
/// non-text cell as empty — the same shape the collapse path's typed error
/// path produces, without the panic.
fn key_of(row: &Tuple, pk_idx: &[usize]) -> CKey {
    pk_idx
        .iter()
        .map(|&i| match row.get(i) {
            Some(Cellv::Text(t)) => t.to_vec(),
            _ => Vec::new(),
        })
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
    fn masked_cells_are_rebuilt_from_the_window_then_the_destination() {
        // id, title, body — body is the TOASTed column.
        let names = vec!["id".to_string(), "title".to_string(), "body".to_string()];
        let mut c = Changes::default();
        c.insert(row(&[t("1"), t("a"), t("BIG")]));               // full image
        c.update(None, row(&[t("1"), t("a2"), Cell::UnchangedToast]), &[0]);
        c.update(None, row(&[t("2"), t("b"), Cell::UnchangedToast]), &[0]);
        assert!(c.masked);
        let (keys, cols) = c.mask_plan(&[0]);
        assert_eq!(cols, vec![2]);
        assert_eq!(keys, vec![vec![b"1".to_vec()], vec![b"2".to_vec()]]);

        // key 2 was never seen in this window, so it comes from the readback.
        let mut base = HashMap::new();
        base.insert(vec![b"2".to_vec()], vec![Some(bytes::Bytes::from_static(b"OLD"))]);
        let fixed = c.resolve_masked(&[0], &cols, &base, &names).unwrap();

        // Event 0 carried everything; only the two masked updates are rebuilt.
        assert_eq!(fixed.len(), 2);
        let got = |i: usize| match fixed[&i].view(2) {
            Cellv::Text(t) => String::from_utf8_lossy(t).to_string(),
            other => format!("{other:?}"),
        };
        assert_eq!(got(1), "BIG"); // carried forward from the insert in-window
        assert_eq!(got(2), "OLD"); // read back from the destination
    }

    #[test]
    fn a_masked_cell_no_source_can_fill_is_refused_not_nulled() {
        let names = vec!["id".to_string(), "body".to_string()];
        let mut c = Changes::default();
        c.update(None, row(&[t("7"), Cell::UnchangedToast]), &[0]);
        let (_, cols) = c.mask_plan(&[0]);
        let err = c.resolve_masked(&[0], &cols, &HashMap::new(), &names).unwrap_err();
        assert!(format!("{err}").contains("torn"), "{err}");
    }

    #[test]
    fn a_window_with_no_toast_never_sets_the_mask_flag() {
        let mut c = Changes::default();
        c.insert(row(&[t("1"), t("a")]));
        c.update(None, row(&[t("1"), Cell::Null]), &[0]);
        c.delete(row(&[t("1"), Cell::Null]));
        assert!(!c.masked);
    }

    #[test]
    fn key_of_survives_a_short_old_image() {
        // A composite key whose second column is past the end of the tuple:
        // the collapse path returns an error here, and this must not panic.
        let r = row(&[t("1")]);
        assert_eq!(key_of(&r, &[0, 1]), vec![b"1".to_vec(), Vec::new()]);
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
