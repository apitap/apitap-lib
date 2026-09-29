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
use crate::logbased::window::{Layout, TableWindow};
use crate::wire::pgoutput::{Cell, Cellv, Tuple};
use std::collections::HashMap;
use std::sync::Arc;

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
    /// The key the row lived at before THIS update moved it: set on the `U`
    /// half of a key-changing update, `None` everywhere else. An untouched
    /// TOAST cell of a moved row is still where the row was — in what the
    /// window carried for that key, or in the destination's row there — never
    /// at the new key, which held nothing. Boxed: a re-key is rare, and every
    /// other event of the window then pays a pointer for the field, not an
    /// empty Vec.
    pub moved_from: Option<Box<CKey>>,
}

/// One table's captured window, in WAL order.
#[derive(Debug)]
pub(crate) struct Changes {
    pub events: Vec<Change>,
    /// Row events consumed (for reporting) — matches `Collapsed::events`.
    pub count: u64,
    /// Any event carries an unchanged-TOAST cell, so the apply has to resolve
    /// them before writing. Tracked here so the overwhelmingly common window —
    /// no TOAST at all — pays nothing for the machinery below.
    pub masked: bool,
    /// The table's columns: its keys decide where a re-key moved a row from
    /// and which row a masked cell belongs to, and the sealed window carries
    /// it to the applies.
    layout: Arc<Layout>,
}

impl Changes {
    pub(crate) fn new(layout: Arc<Layout>) -> Self {
        Self { events: Vec::new(), count: 0, masked: false, layout }
    }

    /// The window this accumulated, with its layout.
    pub(crate) fn seal(self, table: &str) -> Result<TableWindow<Changes>> {
        let layout = self.layout.clone();
        TableWindow::seal(table, layout, self)
    }

    /// O(cols) once, and only until the first masked row is seen.
    fn note_mask(&mut self, row: &Tuple) {
        if !self.masked {
            self.masked = (0..row.len()).any(|i| matches!(row.view(i), Cellv::UnchangedToast));
        }
    }

    pub(crate) fn insert(&mut self, row: Tuple) {
        self.count += 1;
        self.note_mask(&row);
        self.events.push(Change { op: ChangeOp::Insert, row: Some(row), moved_from: None });
    }

    /// An update lands as ONE `U` carrying the new image. A PK-changing update
    /// additionally lands a `D` for the old identity FIRST, so a consumer
    /// replaying the log sees the old key die before the new one appears —
    /// the same ordering the collapsed path encodes as delete-then-upsert —
    /// and its `U` names the old key in `moved_from`.
    pub(crate) fn update(&mut self, old: Option<&Tuple>, row: Tuple) {
        self.count += 1;
        self.note_mask(&row);
        let mut moved_from = None;
        let pk_idx = self.layout.key_idx();
        if let Some(old) = old {
            let from = key_of(old, pk_idx);
            if from != key_of(&row, pk_idx) {
                self.events.push(Change { op: ChangeOp::Delete, row: Some(old.clone()), moved_from: None });
                moved_from = Some(Box::new(from));
            }
        }
        self.events.push(Change { op: ChangeOp::Update, row: Some(row), moved_from });
    }

    pub(crate) fn delete(&mut self, old: Tuple) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Delete, row: Some(old), moved_from: None });
    }

    /// Which keys carry a masked cell, and the union of the columns they are
    /// missing — the shopping list for ONE readback per window. A moved row's
    /// cells are read at the key it came from (`moved_from`), and a `D` image
    /// is never rebuilt (see `resolve_masked`), so it asks for nothing.
    pub(crate) fn mask_plan(&self) -> (Vec<CKey>, Vec<usize>) {
        let pk_idx = self.layout.key_idx();
        let mut keys: Vec<CKey> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut cols = std::collections::BTreeSet::new();
        for ev in &self.events {
            if ev.op == ChangeOp::Delete {
                continue;
            }
            let Some(row) = &ev.row else { continue };
            let mut any = false;
            for i in 0..row.len() {
                if matches!(row.view(i), Cellv::UnchangedToast) {
                    cols.insert(i);
                    any = true;
                }
            }
            if any {
                let k = match &ev.moved_from {
                    Some(from) => CKey::clone(from),
                    None => key_of(row, pk_idx),
                };
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
    /// Both are looked up at the key the row WAS at: `moved_from` for the `U`
    /// half of a re-key (the new key held nothing before this event), its own
    /// key otherwise. Three rules keep the carry honest:
    /// - every `I`/`U` writes its WHOLE image into the carry, masked cells as
    ///   resolved, so a later event of a moved row (a masked update at its new
    ///   key, or a second move) finds them where the row now is;
    /// - a `D` image writes nothing: under REPLICA IDENTITY DEFAULT it is the
    ///   key and NULLs, and the `D` half of a re-key would blank the old key's
    ///   cells just before its `U` half reads them;
    /// - a `T` empties the carry, and from then on `base` is not the row's
    ///   past — the destination's pre-window row was truncated away.
    ///
    /// A cell neither source can fill is a row the destination has never seen —
    /// a torn window, not a NULL. It is refused loudly.
    pub(crate) fn resolve_masked(
        &self,
        cols: &[usize],
        base: &HashMap<CKey, Vec<Option<bytes::Bytes>>>,
    ) -> Result<HashMap<usize, Tuple>> {
        let (pk_idx, names) = (self.layout.key_idx(), self.layout.cols());
        let mut carry: HashMap<CKey, HashMap<usize, Option<bytes::Bytes>>> = HashMap::new();
        let mut truncated = false;
        let mut out = HashMap::new();
        for (idx, ev) in self.events.iter().enumerate() {
            let row = match (ev.op, &ev.row) {
                (ChangeOp::Truncate, _) => {
                    carry.clear();
                    truncated = true;
                    continue;
                }
                (ChangeOp::Delete, _) | (_, None) => continue,
                (_, Some(row)) => row,
            };
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
            if masked_any {
                // Where the untouched cells still are: the old key of a move.
                let src = ev.moved_from.as_deref().unwrap_or(&key);
                for i in 0..n {
                    if !matches!(cells[i], Cell::UnchangedToast) {
                        continue;
                    }
                    let from_window = carry.get(src).and_then(|s| s.get(&i)).cloned();
                    let from_dest = || {
                        if truncated {
                            return None;
                        }
                        cols.iter()
                            .position(|&c| c == i)
                            .and_then(|p| base.get(src).and_then(|b| b.get(p).cloned()))
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
            }
            // What the row holds after THIS event — every cell, the ones just
            // resolved included — is what later events of this key inherit.
            let slot = carry.entry(key).or_default();
            for (i, c) in cells.iter().enumerate() {
                let v = match c {
                    Cell::Text(t) => Some(t.clone()),
                    Cell::Null => None,
                    // None is left: every masked cell was resolved above.
                    Cell::UnchangedToast => continue,
                };
                slot.insert(i, v);
            }
            if masked_any {
                out.insert(idx, Tuple::from_cells(&cells));
            }
        }
        Ok(out)
    }

    /// TRUNCATE is captured as its own record, NOT as a wipe: the log keeps
    /// what came before it. A consumer deriving current state treats every
    /// row of that table older than the truncate as gone.
    pub(crate) fn truncate(&mut self) {
        self.count += 1;
        self.events.push(Change { op: ChangeOp::Truncate, row: None, moved_from: None });
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
    /// `(id, v)`, keyed on id.
    fn two() -> Changes {
        Changes::new(Layout::for_test(&["id", "v"], &[], &["id"]))
    }
    /// `(id, title, body)`, keyed on id; `body` is the TOASTed column.
    fn three() -> Changes {
        Changes::new(Layout::for_test(&["id", "title", "body"], &[], &["id"]))
    }

    #[test]
    fn every_operation_is_kept_in_order() {
        let mut c = two();
        c.insert(row(&[t("1"), t("a")]));
        c.update(None, row(&[t("1"), t("a2")]));
        c.update(None, row(&[t("1"), t("a3")]));
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
        let mut c = two();
        c.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("9"), t("a")]));
        let ops: Vec<ChangeOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(ops, vec![ChangeOp::Delete, ChangeOp::Update]);
        // …but it is ONE source event.
        assert_eq!(c.count, 1);
        // The U half names where the row was; the D half is that key itself.
        assert_eq!(c.events[0].moved_from, None);
        assert_eq!(c.events[1].moved_from.as_deref(), Some(&vec![b"1".to_vec()]));
    }

    #[test]
    fn same_key_update_does_not_emit_a_delete() {
        let mut c = two();
        c.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("1"), t("z")]));
        assert_eq!(c.events.len(), 1);
        assert_eq!(c.events[0].op, ChangeOp::Update);
        assert_eq!(c.events[0].moved_from, None);
    }

    // C1 (moved_key_masked_update_resolves_from_old_key) and its helper.
    /// Rows are `id, title, body`, keyed on id, `body` the TOASTed column. The
    /// rebuilt rows' `body` by event index, or the error's text; `base` is the
    /// destination's `body` per key.
    fn bodies(c: &Changes, base: &[(&str, &str)]) -> std::result::Result<Vec<(usize, String)>, String> {
        let (_, cols) = c.mask_plan();
        let base: HashMap<CKey, Vec<Option<bytes::Bytes>>> = base
            .iter()
            .map(|(k, v)| (vec![k.as_bytes().to_vec()], vec![Some(bytes::Bytes::copy_from_slice(v.as_bytes()))]))
            .collect();
        let fixed = c.resolve_masked(&cols, &base).map_err(|e| e.to_string())?;
        let mut out: Vec<(usize, String)> = fixed
            .iter()
            .map(|(i, r)| {
                let body = match r.view(2) {
                    Cellv::Text(b) => String::from_utf8_lossy(b).to_string(),
                    other => format!("{other:?}"),
                };
                (*i, body)
            })
            .collect();
        out.sort();
        Ok(out)
    }

    #[test]
    fn moved_key_masked_update_resolves_from_old_key() {
        // `UPDATE t SET id = 9 WHERE id = 1` with `body` untouched: a D of the
        // old identity, then a U at 9 whose body is masked. Key 9 held nothing
        // before this event; the body is still at key 1.
        let big = || row(&[t("1"), t("a"), t("BIG")]);
        let key_only = |k: &str| row(&[t(k), Cell::Null, Cell::Null]); // REPLICA IDENTITY DEFAULT
        let masked = |k: &str, title: &str| row(&[t(k), t(title), Cell::UnchangedToast]);
        let torn = |r: std::result::Result<Vec<(usize, String)>, String>| {
            assert!(matches!(&r, Err(m) if m.contains("torn")), "want the torn error, got {r:?}")
        };

        // (a) the window carried key 1's body (a full old image, as under
        // REPLICA IDENTITY FULL); (b) only the destination holds it, at key 1.
        let mut a = three();
        a.insert(big());
        a.update(Some(&big()), masked("9", "b"));
        let mut b = three();
        b.update(Some(&key_only("1")), masked("9", "b"));
        assert_eq!(
            [bodies(&a, &[]), bodies(&b, &[("1", "OLD")])],
            [Ok(vec![(2, "BIG".to_string())]), Ok(vec![(1, "OLD".to_string())])]
        );
        // …so the window's one readback asks for key 1, not 9.
        assert_eq!(b.mask_plan(), (vec![vec![b"1".to_vec()]], vec![2]));

        // (c) a value at the NEW key is not this row's past: nothing holds the
        // body where the row was, so the window is torn.
        torn(bodies(&b, &[("9", "NINE")]));

        // (d) the D half's key-only image does not blank the carry its U half
        // reads next.
        let mut d = three();
        d.insert(big());
        d.update(Some(&key_only("1")), masked("9", "b"));
        assert_eq!(bodies(&d, &[]), Ok(vec![(2, "BIG".to_string())]));

        // (e) after an in-window TRUNCATE, neither the destination's row nor
        // what the window carried before it is this row's.
        let mut e = three();
        e.truncate();
        e.update(None, masked("1", "c"));
        torn(bodies(&e, &[("1", "OLD")]));
        let mut e = three();
        e.insert(big());
        e.truncate();
        e.update(None, masked("1", "c"));
        torn(bodies(&e, &[]));

        // (f) a chain 1→9→5, then a masked update at 5: each event finds the
        // body resolved where the row now is, the destination read at 1 only.
        let mut f = three();
        f.update(Some(&key_only("1")), masked("9", "b")); // D 0, U 1
        f.update(Some(&key_only("9")), masked("5", "c")); // D 2, U 3
        f.update(None, masked("5", "d")); // U 4
        assert_eq!(
            bodies(&f, &[("1", "OLD")]),
            Ok(vec![(1, "OLD".to_string()), (3, "OLD".to_string()), (4, "OLD".to_string())])
        );
    }

    #[test]
    fn masked_cells_are_rebuilt_from_the_window_then_the_destination() {
        // id, title, body — body is the TOASTed column.
        let mut c = three();
        c.insert(row(&[t("1"), t("a"), t("BIG")]));               // full image
        c.update(None, row(&[t("1"), t("a2"), Cell::UnchangedToast]));
        c.update(None, row(&[t("2"), t("b"), Cell::UnchangedToast]));
        assert!(c.masked);
        let (keys, cols) = c.mask_plan();
        assert_eq!(cols, vec![2]);
        assert_eq!(keys, vec![vec![b"1".to_vec()], vec![b"2".to_vec()]]);

        // key 2 was never seen in this window, so it comes from the readback.
        let mut base = HashMap::new();
        base.insert(vec![b"2".to_vec()], vec![Some(bytes::Bytes::from_static(b"OLD"))]);
        let fixed = c.resolve_masked(&cols, &base).unwrap();

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
        let mut c = Changes::new(Layout::for_test(&["id", "body"], &[], &["id"]));
        c.update(None, row(&[t("7"), Cell::UnchangedToast]));
        let (_, cols) = c.mask_plan();
        let err = c.resolve_masked(&cols, &HashMap::new()).unwrap_err();
        assert!(format!("{err}").contains("torn"), "{err}");
    }

    #[test]
    fn a_window_with_no_toast_never_sets_the_mask_flag() {
        let mut c = two();
        c.insert(row(&[t("1"), t("a")]));
        c.update(None, row(&[t("1"), Cell::Null]));
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
        let mut c = two();
        c.insert(row(&[t("1"), t("a")]));
        c.truncate();
        c.insert(row(&[t("2"), t("b")]));
        let ops: Vec<ChangeOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(ops, vec![ChangeOp::Insert, ChangeOp::Truncate, ChangeOp::Insert]);
    }
}
