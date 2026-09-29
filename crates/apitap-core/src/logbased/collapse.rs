//! Per-key collapse of one drained WAL window — ape-dts's RdbMerger shape,
//! adapted for one-shot set-based apply (docs/design/log_based.md).
//!
//! In: the window's row events for ONE table, in WAL order.
//! Out: `deletes` (replica-identity keys whose destination rows must go, each
//! once), `upserts` (final row images, last-write-wins), and `residue` (events that
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
use crate::logbased::window::{Layout, TableWindow};
use crate::wire::pgoutput::{Cell, Cellv, Tuple};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// A replica-identity key: the key columns' text values in key-column order.
/// NULLs are illegal in identity keys (Postgres enforces NOT NULL on them).
pub(crate) type Key = Vec<Vec<u8>>;

/// The keys whose destination rows must go, each once, in no particular
/// order. A set by construction: `Collapser::finish` reads it off the key map,
/// where a key can only be once. It used to be a Vec pushed at every delete, so
/// `delete 1; insert 1; delete 1` pushed key 1 twice — harmless to the SQL
/// engines, whose delete joins a key table, and fatal to BigQuery, whose MERGE
/// refuses a target row matched by two staging rows.
#[derive(Debug, Default)]
pub(crate) struct DeleteSet(Vec<Key>);

impl DeleteSet {
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Key> {
        self.0.iter()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn from_keys(mut keys: Vec<Key>) -> Self {
        keys.sort();
        keys.dedup();
        Self(keys)
    }

    #[cfg(test)]
    pub(crate) fn sorted(&self) -> Vec<Key> {
        let mut keys = self.0.clone();
        keys.sort();
        keys
    }
}

/// One table's collapsed window.
#[derive(Debug, Default)]
pub(crate) struct Collapsed {
    /// Keys whose destination rows must go, each once. Order unspecified.
    /// Applied FIRST.
    pub deletes: DeleteSet,
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

/// Where a key stands in the window. `del` says the key's PRE-window row is
/// removed in the delete phase: the first delete of the key (or of it as the
/// old identity of a key-changing update) sets it, and nothing but a TRUNCATE,
/// which empties the map, clears it again.
#[derive(Debug, Clone, Copy)]
enum Slot {
    /// Row pending upsert, at `seq` in insertion order.
    Upsert { seq: usize, del: bool },
    /// Key pending delete only.
    Delete,
    /// Key lives in the residue tail now — all later events follow it there.
    Residue { del: bool },
}

impl Slot {
    fn del(&self) -> bool {
        match *self {
            Slot::Upsert { del, .. } | Slot::Residue { del } => del,
            Slot::Delete => true,
        }
    }
}

pub(crate) struct Collapser {
    /// The table's columns; its key positions are what a row is keyed by, and
    /// the sealed window carries it to the applies.
    layout: Arc<Layout>,
    /// foldhash: the keys are our own PK bytes from a database we connect
    /// to — hashDoS is not in the threat model, and SipHash was 6.5% of the
    /// capped my→ch drain's samples.
    map: HashMap<Key, Slot, foldhash::fast::RandomState>,
    /// Row images by insertion order. The KEY is not stored here — an earlier
    /// version kept `(Key, Vec<Cell>)` and `finish()` threw the key away,
    /// which cost a clone per upsert for nothing.
    upserts: Vec<Option<Tuple>>,
    residue: Vec<ResidueOp>,
    truncate: bool,
    events: u64,
}

impl Collapser {
    pub(crate) fn new(layout: Arc<Layout>) -> Self {
        Self {
            layout,
            map: HashMap::default(),
            upserts: Vec::new(),
            residue: Vec::new(),
            truncate: false,
            events: 0,
        }
    }

    /// Key of a FULL row tuple (new image) — from the key column indices.
    fn key_of_row(&self, row: &Tuple) -> Result<Key> {
        self.layout
            .key_idx()
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
                Slot::Residue { .. } => self.residue.push(ResidueOp::Upsert { row: row.to_cells() }),
                Slot::Upsert { seq, .. } => {
                    // Last write wins in place.
                    self.upserts[seq] = Some(row);
                }
                Slot::Delete => {
                    // (delete then re-insert keeps both: delete phase first.)
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert { seq, del: true });
                }
            },
            Entry::Vacant(e) => {
                let seq = self.upserts.len();
                self.upserts.push(Some(row));
                e.insert(Slot::Upsert { seq, del: false });
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
                    //
                    // Each key keeps its pending delete: after `delete 9;
                    // insert 9`, a move onto 9 still has to clear 9's
                    // pre-window row before the tail runs.
                    let d_old = self.map.get(&old_key).is_some_and(Slot::del);
                    let d_new = self.map.get(&new_key).is_some_and(Slot::del);
                    self.map.insert(old_key, Slot::Residue { del: d_old });
                    self.map.insert(new_key, Slot::Residue { del: d_new });
                    return Ok(());
                }
                // Identity changed: the old row must die.
                self.put_delete(old_key);
            }
        }
        match self.map.entry(new_key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue { .. } => {
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
                Slot::Upsert { seq, .. } if !toast => {
                    self.upserts[seq] = Some(row);
                }
                Slot::Delete if !toast => {
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert { seq, del: true });
                }
                slot => {
                    // Masked TOAST update on a non-residue key: the missing
                    // values would overwrite real data on the fat path, so the
                    // key goes sticky. A pending SET-phase upsert stays where
                    // it is (phases run before the tail — correct order), and
                    // so does a pending delete.
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::MaskedUpdate { key, row: row.to_cells() });
                    e.insert(Slot::Residue { del: slot.del() });
                }
            },
            Entry::Vacant(e) => {
                if toast {
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::MaskedUpdate { key, row: row.to_cells() });
                    e.insert(Slot::Residue { del: false });
                } else {
                    let seq = self.upserts.len();
                    self.upserts.push(Some(row));
                    e.insert(Slot::Upsert { seq, del: false });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn delete(&mut self, old: &Tuple) -> Result<()> {
        self.events += 1;
        let key = self.key_of_old(old)?;
        self.put_delete(key);
        Ok(())
    }

    pub(crate) fn truncate(&mut self) {
        self.events += 1;
        // Everything staged so far is moot — the destination table restarts
        // empty at this point in the sequence.
        self.map.clear();
        self.upserts.clear();
        self.residue.clear();
        self.truncate = true;
    }

    /// A delete, and the old-identity kill on a PK-changing update.
    /// Entry-shaped like the rest; a residue-slotted key follows the ordered
    /// tail.
    fn put_delete(&mut self, key: Key) {
        match self.map.entry(key) {
            Entry::Occupied(mut e) => match *e.get() {
                Slot::Residue { .. } => {
                    let key = e.key().clone();
                    self.residue.push(ResidueOp::Delete { key });
                }
                Slot::Upsert { seq, .. } => {
                    // insert-then-delete nets to delete-only.
                    self.upserts[seq] = None;
                    e.insert(Slot::Delete);
                }
                Slot::Delete => {}
            },
            Entry::Vacant(e) => {
                e.insert(Slot::Delete);
            }
        }
    }

    /// The window this collapser accumulated, with the layout it was keyed by.
    pub(crate) fn seal(self, table: &str) -> Result<TableWindow<Collapsed>> {
        let layout = self.layout.clone();
        TableWindow::seal(table, layout, self.finish())
    }

    fn finish(self) -> Collapsed {
        // One pass over the key map at the window's end: no per-event cost,
        // and each deleted key moves out instead of being cloned at its delete.
        let deletes = DeleteSet(self.map.into_iter().filter(|(_, s)| s.del()).map(|(k, _)| k).collect());
        Collapsed {
            deletes,
            upserts: self.upserts.into_iter().flatten().collect(),
            residue: self.residue,
            truncate: self.truncate,
            events: self.events,
        }
    }
}

/// Random windows for the property tests here and in `resolve.rs`: any mix of
/// these events on a handful of keys. Not source-consistent on purpose (an
/// insert may hit a live key): the collapser assumes nothing about its input,
/// so neither do its tests.
#[cfg(test)]
pub(crate) mod gen {
    use super::*;

    /// xorshift64*: deterministic, so a failing sequence reproduces.
    pub(crate) struct Rng(pub(crate) u64);

    impl Rng {
        pub(crate) fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            (self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 33) % n
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub(crate) enum Ev {
        Insert(u8),
        /// A full new image, no old one (REPLICA IDENTITY DEFAULT, key kept).
        Update(u8),
        /// The same with the TOASTed column unchanged.
        Masked(u8),
        Delete(u8),
        /// A key-changing update (`from == to` is an update with an old image).
        Rekey { from: u8, to: u8, toast: bool },
        Truncate,
    }

    pub(crate) fn events(rng: &mut Rng, keys: u64, len: usize) -> Vec<Ev> {
        (0..len)
            .map(|_| {
                let k = rng.below(keys) as u8;
                match rng.below(100) {
                    0..=19 => Ev::Insert(k),
                    20..=34 => Ev::Update(k),
                    35..=54 => Ev::Masked(k),
                    55..=74 => Ev::Delete(k),
                    75..=97 => Ev::Rekey { from: k, to: rng.below(keys) as u8, toast: rng.below(2) == 0 },
                    _ => Ev::Truncate,
                }
            })
            .collect()
    }

    fn text(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }

    /// The `(id, v, big)` table `feed` writes, keyed on `id`.
    pub(crate) fn layout() -> Arc<Layout> {
        Layout::for_test(&["id", "v", "big"], &[], &["id"])
    }

    /// The key column's text for key `k`, as `Key` spells it.
    pub(crate) fn key(k: u8) -> Key {
        vec![k.to_string().into_bytes()]
    }

    /// Feed `evs` to a collapser keyed on column 0 of `(id, v, big)`; `v`
    /// carries the event's ordinal, so every image is distinguishable.
    pub(crate) fn feed(c: &mut Collapser, evs: &[Ev]) {
        for (i, ev) in evs.iter().enumerate() {
            let v = text(&format!("v{i}"));
            let full = |k: u8| Tuple::from_cells(&[text(&k.to_string()), v.clone(), text("big")]);
            let masked = |k: u8| Tuple::from_cells(&[text(&k.to_string()), v.clone(), Cell::UnchangedToast]);
            let old = |k: u8| Tuple::from_cells(&[text(&k.to_string()), Cell::Null, Cell::Null]);
            match *ev {
                Ev::Insert(k) => c.insert(full(k)),
                Ev::Update(k) => c.update(None, full(k)),
                Ev::Masked(k) => c.update(None, masked(k)),
                Ev::Delete(k) => c.delete(&old(k)),
                Ev::Rekey { from, to, toast } => {
                    c.update(Some(&old(from)), if toast { masked(to) } else { full(to) })
                }
                Ev::Truncate => {
                    c.truncate();
                    Ok(())
                }
            }
            .expect("a well-formed event");
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
        Collapser::new(Layout::for_test(&["id", "v"], &[], &["id"]))
    }

    #[test]
    fn last_write_wins_and_order_survives() {
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.insert(row(&[t("2"), t("b")])).unwrap();
        cl.update(None, row(&[t("1"), t("a2")])).unwrap();
        let out = cl.finish();
        assert!(out.deletes.is_empty());
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
        assert_eq!(out.deletes.sorted(), vec![key(&["1"])]);
    }

    #[test]
    fn delete_then_reinsert_keeps_both_phases() {
        let mut cl = c();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        cl.insert(row(&[t("1"), t("new")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.sorted(), vec![key(&["1"])]);
        assert_eq!(cells_of(&out.upserts), vec![vec![t("1"), t("new")]]);
    }

    #[test]
    fn delete_insert_delete_yields_one_delete() {
        // The audit's BigQuery rejection (§3.11): one key deleted twice in a
        // window was pushed twice, and a MERGE refuses two staging rows for
        // one target row.
        let mut cl = c();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.sorted(), vec![key(&["1"])]);
        assert!(out.upserts.is_empty());

        // Two lives of one key in one window: still one delete.
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        cl.insert(row(&[t("1"), t("b")])).unwrap();
        cl.delete(&row(&[t("1"), Cell::Null])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.sorted(), vec![key(&["1"])]);
        assert!(out.upserts.is_empty());

        // A move onto a key the window deleted and re-inserted: both
        // identities go, each once, and the moved row lands.
        let mut cl = c();
        cl.delete(&row(&[t("9"), Cell::Null])).unwrap();
        cl.insert(row(&[t("9"), t("x")])).unwrap();
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("9"), t("y")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.sorted(), vec![key(&["1"]), key(&["9"])]);
        assert_eq!(cells_of(&out.upserts), vec![vec![t("9"), t("y")]]);
    }

    /// 0.56.0's delete list, transcribed for keys alone: a push at every step
    /// INTO `Delete` (from a pending upsert or from nothing), none from the
    /// residue tail, all cleared by a TRUNCATE. Those were the right keys;
    /// only the count was wrong.
    fn pushes_0560(evs: &[gen::Ev]) -> Vec<Key> {
        #[derive(Clone, Copy, PartialEq)]
        enum S {
            Up,
            Del,
            Res,
        }
        fn kill(map: &mut HashMap<Key, S>, pushed: &mut Vec<Key>, k: Key) {
            if matches!(map.get(&k), None | Some(S::Up)) {
                pushed.push(k.clone());
                map.insert(k, S::Del);
            }
        }
        fn land(map: &mut HashMap<Key, S>, k: Key, toast: bool) {
            if toast {
                map.insert(k, S::Res);
            } else if map.get(&k) != Some(&S::Res) {
                map.insert(k, S::Up);
            }
        }
        let mut map = HashMap::new();
        let mut pushed = Vec::new();
        for ev in evs {
            match *ev {
                gen::Ev::Insert(k) | gen::Ev::Update(k) => land(&mut map, gen::key(k), false),
                gen::Ev::Masked(k) => land(&mut map, gen::key(k), true),
                gen::Ev::Delete(k) => kill(&mut map, &mut pushed, gen::key(k)),
                gen::Ev::Rekey { from, to, toast } => {
                    if from != to {
                        if toast {
                            map.insert(gen::key(from), S::Res);
                            map.insert(gen::key(to), S::Res);
                            continue;
                        }
                        kill(&mut map, &mut pushed, gen::key(from));
                    }
                    land(&mut map, gen::key(to), toast);
                }
                gen::Ev::Truncate => {
                    map.clear();
                    pushed.clear();
                }
            }
        }
        pushed
    }

    #[test]
    fn delete_set_is_every_old_push_once() {
        let mut rng = gen::Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..5000 {
            let len = 1 + rng.below(12) as usize;
            let evs = gen::events(&mut rng, 6, len);
            let mut cl = Collapser::new(gen::layout());
            gen::feed(&mut cl, &evs);
            let got = cl.finish().deletes.sorted();
            let mut want = pushes_0560(&evs);
            want.sort();
            want.dedup();
            assert_eq!(got, want, "window {evs:?}");
        }
    }

    #[test]
    fn pk_change_update_deletes_the_old_identity() {
        let mut cl = c();
        cl.insert(row(&[t("1"), t("a")])).unwrap();
        cl.update(Some(&row(&[t("1"), Cell::Null])), row(&[t("9"), t("a")])).unwrap();
        let out = cl.finish();
        assert_eq!(out.deletes.sorted(), vec![key(&["1"])]);
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
