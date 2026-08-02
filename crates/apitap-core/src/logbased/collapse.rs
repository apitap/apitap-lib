//! Per-key collapse of one drained WAL window — ape-dts's RdbMerger shape,
//! adapted for one-shot set-based apply (docs/design/log_based.md).
//!
//! In: the window's row events for ONE table, in WAL order.
//! Out: `deletes` (replica-identity keys whose destination rows must go),
//! `upserts` (final row images, last-write-wins), and `residue` (events that
//! cannot ride the set-based path, in original order — today that is
//! exactly the unchanged-TOAST updates, applied as column-masked UPDATEs).
//!
//! Rules that carry correctness:
//! - update = delete(old key) + upsert(new row): PK-changing updates come
//!   out right with no special case.
//! - insert-then-delete inside the window nets to DELETE (dropping it would
//!   leave a phantom row at the destination).
//! - a key seen as upsert then deleted leaves ONLY the delete; a key seen
//!   as delete then re-inserted keeps BOTH (delete phase runs first).
//! - TRUNCATE flushes everything collected so far for the table and sets
//!   the `truncate` flag — apply order is truncate → deletes → upserts.

use crate::error::{Error, Result};
use crate::wire::pgoutput::Cell;
use std::collections::HashMap;

/// A replica-identity key: the key columns' text values in key-column order.
/// NULLs are illegal in identity keys (Postgres enforces NOT NULL on them).
pub(crate) type Key = Vec<Vec<u8>>;

/// One table's collapsed window.
#[derive(Debug, Default)]
pub(crate) struct Collapsed {
    /// Destination rows to delete (dedup'd). Applied FIRST.
    pub deletes: Vec<Key>,
    /// Final row images to land (one per surviving key). Applied second.
    pub upserts: Vec<Vec<Cell>>,
    /// Ordered tail for keys that touched an unchanged-TOAST update: once a
    /// key needs a masked UPDATE, every later event on that key stays in
    /// this ordered list (sticky per key — the set phases can no longer
    /// order correctly against it). Applied serially, last.
    pub residue: Vec<ResidueOp>,
    /// A TRUNCATE was seen: destination truncates before applying the rest.
    pub truncate: bool,
    /// Row events consumed (for reporting).
    pub events: u64,
}

/// One serially-applied trailing operation (see `Collapsed::residue`).
#[derive(Debug, PartialEq)]
pub(crate) enum ResidueOp {
    /// Column-masked UPDATE: set only the non-`UnchangedToast` columns.
    MaskedUpdate { key: Key, row: Vec<Cell> },
    /// Full upsert of a row whose key previously went masked.
    Upsert { row: Vec<Cell> },
    /// Delete of a key that previously went masked.
    Delete { key: Key },
}

#[derive(Debug)]
enum Slot {
    /// Row pending upsert, at `upsert_seq` insertion order.
    Upsert(usize),
    /// Key pending delete only.
    Delete,
    /// Key lives in the residue tail now — all later events follow it there.
    Residue,
}

pub(crate) struct Collapser {
    /// Indices of the key columns within the row tuple.
    key_idx: Vec<usize>,
    map: HashMap<Key, Slot>,
    upserts: Vec<Option<(Key, Vec<Cell>)>>,
    deletes: Vec<Key>,
    residue: Vec<ResidueOp>,
    truncate: bool,
    events: u64,
}

impl Collapser {
    pub(crate) fn new(key_idx: Vec<usize>) -> Self {
        Self {
            key_idx,
            map: HashMap::new(),
            upserts: Vec::new(),
            deletes: Vec::new(),
            residue: Vec::new(),
            truncate: false,
            events: 0,
        }
    }

    /// Key of a FULL row tuple (new image) — from the key column indices.
    fn key_of_row(&self, row: &[Cell]) -> Result<Key> {
        self.key_idx
            .iter()
            .map(|&i| match row.get(i) {
                Some(Cell::Text(t)) => Ok(t.clone()),
                Some(Cell::Null) | None => Err(Error::Transfer(
                    "log_based: NULL/missing replica-identity key column in row \
                     image — is REPLICA IDENTITY sane on the source table?"
                        .into(),
                )),
                Some(Cell::UnchangedToast) => Err(Error::Transfer(
                    "log_based: replica-identity key column arrived as \
                     unchanged-TOAST — unsupported layout"
                        .into(),
                )),
            })
            .collect()
    }

    /// Key of an OLD image. `K` images carry ONLY key columns as Text, with
    /// non-key columns Null; `O` (FULL) images carry everything — either
    /// way the key columns are at the same indices.
    fn key_of_old(&self, old: &[Cell]) -> Result<Key> {
        self.key_of_row(old)
    }

    pub(crate) fn insert(&mut self, row: Vec<Cell>) -> Result<()> {
        self.events += 1;
        let key = self.key_of_row(&row)?;
        if matches!(self.map.get(&key), Some(Slot::Residue)) {
            self.residue.push(ResidueOp::Upsert { row });
            return Ok(());
        }
        self.put_upsert(key, row);
        Ok(())
    }

    pub(crate) fn update(&mut self, old: Option<&[Cell]>, row: Vec<Cell>) -> Result<()> {
        self.events += 1;
        let new_key = self.key_of_row(&row)?;
        if let Some(old) = old {
            let old_key = self.key_of_old(old)?;
            if old_key != new_key {
                // Identity changed: the old row must die.
                self.put_delete(old_key);
            }
        }
        let sticky = matches!(self.map.get(&new_key), Some(Slot::Residue));
        if sticky || row.iter().any(|c| matches!(c, Cell::UnchangedToast)) {
            // Cannot ride the fat upsert path — either the missing TOAST
            // values would overwrite real data, or an earlier event on this
            // key already lives in the ordered tail. Any pending SET-phase
            // upsert for the key stays where it is (the phases run before
            // the tail, so it lands first — correct order).
            if row.iter().any(|c| matches!(c, Cell::UnchangedToast)) {
                self.residue.push(ResidueOp::MaskedUpdate { key: new_key.clone(), row });
            } else {
                self.residue.push(ResidueOp::Upsert { row });
            }
            self.map.insert(new_key, Slot::Residue);
            return Ok(());
        }
        self.put_upsert(new_key, row);
        Ok(())
    }

    pub(crate) fn delete(&mut self, old: &[Cell]) -> Result<()> {
        self.events += 1;
        let key = self.key_of_old(old)?;
        if matches!(self.map.get(&key), Some(Slot::Residue)) {
            self.residue.push(ResidueOp::Delete { key });
            return Ok(());
        }
        self.put_delete(key);
        Ok(())
    }

    pub(crate) fn truncate(&mut self) {
        self.events += 1;
        // Everything staged so far is moot — the destination table restarts
        // empty at this point in the sequence.
        self.map.clear();
        self.upserts.clear();
        self.deletes.clear();
        self.residue.clear();
        self.truncate = true;
    }

    fn put_upsert(&mut self, key: Key, row: Vec<Cell>) {
        match self.map.get(&key) {
            Some(Slot::Upsert(seq)) => {
                // Last write wins in place.
                self.upserts[*seq] = Some((key, row));
            }
            Some(Slot::Residue) => unreachable!("residue keys handled by callers"),
            Some(Slot::Delete) | None => {
                // (delete then re-insert keeps both: delete phase runs first.)
                let seq = self.upserts.len();
                self.upserts.push(Some((key.clone(), row)));
                self.map.insert(key, Slot::Upsert(seq));
            }
        }
    }

    fn put_delete(&mut self, key: Key) {
        match self.map.get(&key) {
            Some(Slot::Upsert(seq)) => {
                // insert-then-delete nets to delete-only.
                let seq = *seq;
                self.upserts[seq] = None;
                self.deletes.push(key.clone());
                self.map.insert(key, Slot::Delete);
            }
            Some(Slot::Residue) => unreachable!("residue keys handled by callers"),
            Some(Slot::Delete) => {}
            None => {
                self.deletes.push(key.clone());
                self.map.insert(key, Slot::Delete);
            }
        }
    }

    pub(crate) fn finish(self) -> Collapsed {
        Collapsed {
            deletes: self.deletes,
            upserts: self
                .upserts
                .into_iter()
                .flatten()
                .map(|(_, row)| row)
                .collect(),
            residue: self.residue,
            truncate: self.truncate,
            events: self.events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Cell {
        Cell::Text(s.as_bytes().to_vec())
    }
    fn key(parts: &[&str]) -> Key {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn c() -> Collapser {
        Collapser::new(vec![0])
    }

    #[test]
    fn last_write_wins_and_order_survives() {
        let mut cl = c();
        cl.insert(vec![t("1"), t("a")]).unwrap();
        cl.insert(vec![t("2"), t("b")]).unwrap();
        cl.update(None, vec![t("1"), t("a2")]).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.len(), 0);
        assert_eq!(out.upserts, vec![vec![t("1"), t("a2")], vec![t("2"), t("b")]]);
        assert_eq!(out.events, 3);
    }

    #[test]
    fn insert_then_delete_nets_to_delete() {
        let mut cl = c();
        cl.insert(vec![t("1"), t("a")]).unwrap();
        cl.delete(&[t("1"), Cell::Null]).unwrap();
        let out = cl.finish();
        assert_eq!(out.upserts.len(), 0);
        assert_eq!(out.deletes, vec![key(&["1"])]);
    }

    #[test]
    fn delete_then_reinsert_keeps_both_phases() {
        let mut cl = c();
        cl.delete(&[t("1"), Cell::Null]).unwrap();
        cl.insert(vec![t("1"), t("new")]).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes, vec![key(&["1"])]);
        assert_eq!(out.upserts, vec![vec![t("1"), t("new")]]);
    }

    #[test]
    fn pk_change_update_deletes_the_old_identity() {
        let mut cl = c();
        cl.insert(vec![t("1"), t("a")]).unwrap();
        cl.update(Some(&[t("1"), Cell::Null]), vec![t("9"), t("a")]).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes, vec![key(&["1"])]);
        assert_eq!(out.upserts, vec![vec![t("9"), t("a")]]);
    }

    #[test]
    fn unchanged_toast_routes_to_residue_not_upsert() {
        let mut cl = c();
        cl.update(Some(&[t("1"), Cell::Null]), vec![t("1"), Cell::UnchangedToast])
            .unwrap();
        let out = cl.finish();
        assert!(out.upserts.is_empty());
        assert_eq!(out.residue.len(), 1);
        let ResidueOp::MaskedUpdate { key: k, row } = &out.residue[0] else { panic!() };
        assert_eq!(k, &key(&["1"]));
        assert_eq!(row[1], Cell::UnchangedToast);
    }

    #[test]
    fn residue_is_sticky_and_ordered_per_key() {
        let mut cl = c();
        cl.insert(vec![t("1"), t("a")]).unwrap();
        cl.update(Some(&[t("1"), Cell::Null]), vec![t("1"), Cell::UnchangedToast])
            .unwrap();
        // Later full update on the same key must FOLLOW the masked update.
        cl.update(Some(&[t("1"), Cell::Null]), vec![t("1"), t("z")]).unwrap();
        cl.delete(&[t("1"), Cell::Null]).unwrap();
        let out = cl.finish();
        // The original insert stays in the set phase (applies first)…
        assert_eq!(out.upserts, vec![vec![t("1"), t("a")]]);
        // …and the tail replays in order: masked, full upsert, delete.
        assert!(matches!(out.residue[0], ResidueOp::MaskedUpdate { .. }));
        assert!(matches!(out.residue[1], ResidueOp::Upsert { .. }));
        assert!(matches!(out.residue[2], ResidueOp::Delete { .. }));
    }

    #[test]
    fn truncate_wipes_prior_window_and_flags() {
        let mut cl = c();
        cl.insert(vec![t("1"), t("a")]).unwrap();
        cl.delete(&[t("2"), Cell::Null]).unwrap();
        cl.truncate();
        cl.insert(vec![t("3"), t("post")]).unwrap();
        let out = cl.finish();
        assert!(out.truncate);
        assert!(out.deletes.is_empty());
        assert_eq!(out.upserts, vec![vec![t("3"), t("post")]]);
    }

    #[test]
    fn null_key_fails_loudly() {
        let mut cl = c();
        assert!(cl.insert(vec![Cell::Null, t("a")]).is_err());
    }
}
