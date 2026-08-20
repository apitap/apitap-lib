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
use crate::wire::pgoutput::{Cell, Cellv, Tuple};
use std::collections::hash_map::Entry;
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
    pub upserts: Vec<Tuple>,
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
    /// A key-changing UPDATE whose new image is missing its TOASTed cells.
    ///
    /// This cannot be expressed as delete-old + write-new: the new image does
    /// not contain the TOASTed value, and the delete phase runs FIRST, so by
    /// the time anything could read the old row it is gone. The row is MOVED
    /// instead — one `UPDATE ... SET <pk = new>, <changed cols> WHERE <old
    /// key>` carries the untouched value across without anyone having to know
    /// what it is.
    ///
    /// Carrying both keys is what lets every destination do that: the OLTP
    /// appliers address the old key, ClickHouse reads its missing cells back
    /// from the old key before replacing the row, and `resolve.rs` folds the
    /// old key to `Gone` and the new key to the patched image.
    Rekey { old_key: Key, new_key: Key, row: Vec<Cell> },
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
    /// foldhash: the keys are our own PK bytes from a database we connect
    /// to — hashDoS is not in the threat model, and SipHash was 6.5% of the
    /// capped my→ch drain's samples.
    map: HashMap<Key, Slot, foldhash::fast::RandomState>,
    /// Row images by insertion order. The KEY is not stored here — an earlier
    /// version kept `(Key, Vec<Cell>)` and `finish()` threw the key away,
    /// which cost a clone per upsert for nothing.
    upserts: Vec<Option<Tuple>>,
    deletes: Vec<Key>,
    residue: Vec<ResidueOp>,
    truncate: bool,
    events: u64,
}

impl Collapser {
    pub(crate) fn new(key_idx: Vec<usize>) -> Self {
        Self {
            key_idx,
            map: HashMap::default(),
            upserts: Vec::new(),
            deletes: Vec::new(),
            residue: Vec::new(),
            truncate: false,
            events: 0,
        }
    }

    /// Key of a FULL row tuple (new image) — from the key column indices.
    fn key_of_row(&self, row: &Tuple) -> Result<Key> {
        self.key_idx
            .iter()
            .map(|&i| match row.get(i) {
                // Key stays owned (`Vec<Vec<u8>>`) on purpose: a Bytes key
                // would pin the whole frame it was carved from, and a
                // delete-only key under REPLICA IDENTITY FULL would then hold
                // a full old row image alive for the window instead of ~8
                // bytes. Copying the key columns keeps window memory bounded
                // by what the 256 MB tier budgets for.
                Some(Cellv::Text(t)) => Ok(t.to_vec()),
                Some(Cellv::Null) | None => Err(Error::Transfer(
                    "log_based: NULL/missing replica-identity key column in row \
                     image — is REPLICA IDENTITY sane on the source table?"
                        .into(),
                )),
                Some(Cellv::UnchangedToast) => Err(Error::Transfer(
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
    fn key_of_old(&self, old: &Tuple) -> Result<Key> {
        self.key_of_row(old)
    }

    pub(crate) fn insert(&mut self, row: Tuple) -> Result<()> {
        self.events += 1;
        let key = self.key_of_row(&row)?;
        // ONE map operation per event. The old shape did a residue pre-check
        // get, then put_upsert's own get, then an insert — three hash+probe
        // walks of a Vec<Vec<u8>> key for every change.
        match self.map.entry(key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue => self.residue.push(ResidueOp::Upsert { row: row.to_cells() }),
                Slot::Upsert(seq) => {
                    // Last write wins in place.
                    self.upserts[seq] = Some(row);
                }
                Slot::Delete => {
                    // (delete then re-insert keeps both: delete phase first.)
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert(seq));
                }
            },
            Entry::Vacant(e) => {
                let seq = self.upserts.len();
                self.upserts.push(Some(row));
                e.insert(Slot::Upsert(seq));
            }
        }
        Ok(())
    }

    pub(crate) fn update(&mut self, old: Option<&Tuple>, row: Tuple) -> Result<()> {
        self.events += 1;
        let new_key = self.key_of_row(&row)?;
        let toast = row.has_toast();
        if let Some(old) = old {
            let old_key = self.key_of_old(old)?;
            if old_key != new_key {
                if toast {
                    // Identity changed AND the new image is masked. Deleting
                    // the old row and writing the new one loses the TOASTed
                    // value: the write has no value to carry, and the delete
                    // phase has already run by the time anything could read it
                    // back. Before this existed, the pair became
                    // delete(old) + MaskedUpdate(new) — an UPDATE against a key
                    // that had never existed at the destination, which matched
                    // zero rows on Postgres and MySQL and made the row vanish
                    // while the run reported success.
                    //
                    // Move the row instead. Both keys go sticky: the new one
                    // for the usual reason, and the OLD one so that a later
                    // INSERT reusing that key lands in the ordered tail AFTER
                    // this move rather than in the set phase before it — where
                    // this UPDATE would pick it up and move the wrong row.
                    self.residue.push(ResidueOp::Rekey {
                        old_key: old_key.clone(),
                        new_key: new_key.clone(),
                        row: row.to_cells(),
                    });
                    // A set-phase upsert already queued for the old key stays
                    // queued: it lands first, and the move then carries it to
                    // the new key with its real TOAST value. That is the
                    // insert-then-rekey case, and it is correct.
                    self.map.insert(old_key, Slot::Residue);
                    self.map.insert(new_key, Slot::Residue);
                    return Ok(());
                }
                // Identity changed: the old row must die.
                self.put_delete(old_key);
            }
        }
        match self.map.entry(new_key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue => {
                    // Sticky: later events on a residue key stay in the
                    // ordered tail. The key is only cloned on the masked
                    // path, where the op itself must carry it.
                    if toast {
                        let key = e.key().clone();
                        self.residue.push(ResidueOp::MaskedUpdate { key, row: row.to_cells() });
                    } else {
                        self.residue.push(ResidueOp::Upsert { row: row.to_cells() });
                    }
                }
                Slot::Upsert(seq) if !toast => {
                    self.upserts[seq] = Some(row);
                }
                Slot::Delete if !toast => {
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert(seq));
                }
                _ => {
                    // Masked TOAST update on a non-residue key: the missing
                    // values would overwrite real data on the fat path, so the
                    // key goes sticky. A pending SET-phase upsert stays where
                    // it is (phases run before the tail — correct order).
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::MaskedUpdate { key, row: row.to_cells() });
                    e.insert(Slot::Residue);
                }
            },
            Entry::Vacant(e) => {
                if toast {
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::MaskedUpdate { key, row: row.to_cells() });
                    e.insert(Slot::Residue);
                } else {
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert(seq));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn delete(&mut self, old: &Tuple) -> Result<()> {
        self.events += 1;
        let key = self.key_of_old(old)?;
        match self.map.entry(key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue => {
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::Delete { key });
                }
                Slot::Upsert(seq) => {
                    // insert-then-delete nets to delete-only.
                    self.upserts[seq] = None;
                    self.deletes.push(e.key().clone());
                    e.insert(Slot::Delete);
                }
                Slot::Delete => {}
            },
            Entry::Vacant(e) => {
                self.deletes.push(e.key().clone());
                e.insert(Slot::Delete);
            }
        }
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

    /// Old-identity kill on a PK-changing update. Entry-shaped like the rest;
    /// a residue-slotted old key follows the ordered tail.
    fn put_delete(&mut self, key: Key) {
        match self.map.entry(key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue => {
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::Delete { key });
                }
                Slot::Upsert(seq) => {
                    self.upserts[seq] = None;
                    self.deletes.push(e.key().clone());
                    e.insert(Slot::Delete);
                }
                Slot::Delete => {}
            },
            Entry::Vacant(e) => {
                self.deletes.push(e.key().clone());
                e.insert(Slot::Delete);
            }
        }
    }

    pub(crate) fn finish(self) -> Collapsed {
        Collapsed {
            deletes: self.deletes,
            upserts: self.upserts.into_iter().flatten().collect(),
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
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
    fn row(cells: &[Cell]) -> Tuple {
        Tuple::from_cells(cells)
    }
    fn cells_of(ups: &[Tuple]) -> Vec<Vec<Cell>> {
        ups.iter().map(|u| u.to_cells()).collect()
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
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.insert(row(&[t("2"), t("b")])).unwrap();
        cl.update(None, row(&[t("1"), t("a2")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.len(), 0);
        assert_eq!(cells_of(&out.upserts), vec![vec![t("1"), t("a2")], vec![t("2"), t("b")]]);
        assert_eq!(out.events, 3);
    }

    #[test]
    fn insert_then_delete_nets_to_delete() {
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        let out = cl.finish();
        assert_eq!(out.upserts.len(), 0);
        assert_eq!(out.deletes, vec![key(&["1"])]);
    }

    #[test]
    fn delete_then_reinsert_keeps_both_phases() {
        let mut cl = c();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        cl.insert(row(&[t("1"), t("new")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes, vec![key(&["1"])]);
        assert_eq!(cells_of(&out.upserts), vec![vec![t("1"), t("new")]]);
    }

    #[test]
    fn pk_change_update_deletes_the_old_identity() {
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("9"), t("a")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes, vec![key(&["1"])]);
        assert_eq!(cells_of(&out.upserts), vec![vec![t("9"), t("a")]]);
    }

    #[test]
    fn unchanged_toast_routes_to_residue_not_upsert() {
        let mut cl = c();
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("1"), Cell::UnchangedToast]))
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
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("1"), Cell::UnchangedToast]))
            .unwrap();
        // Later full update on the same key must FOLLOW the masked update.
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("1"), t("z")])).unwrap();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        let out = cl.finish();
        // The original insert stays in the set phase (applies first)…
        assert_eq!(cells_of(&out.upserts), vec![vec![t("1"), t("a")]]);
        // …and the tail replays in order: masked, full upsert, delete.
        assert!(matches!(out.residue[0], ResidueOp::MaskedUpdate { .. }));
        assert!(matches!(out.residue[1], ResidueOp::Upsert { .. }));
        assert!(matches!(out.residue[2], ResidueOp::Delete { .. }));
    }

    #[test]
    fn truncate_wipes_prior_window_and_flags() {
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.delete(&row(&[t("2"), Cell::Null])).unwrap();
        cl.truncate();
        cl.insert(row(&[t("3"), t("post")])).unwrap();
        let out = cl.finish();
        assert!(out.truncate);
        assert!(out.deletes.is_empty());
        assert_eq!(cells_of(&out.upserts), vec![vec![t("3"), t("post")]]);
    }

    #[test]
    fn null_key_fails_loudly() {
        let mut cl = c();
        assert!(cl.insert(row(&[Cell::Null, t("a")])).is_err());
    }
}
