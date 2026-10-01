//! Shared residue resolution for the log_based apply paths.
//!
//! A drained window collapses to `deletes`, `upserts` and an ordered `residue`
//! tail (unchanged-TOAST updates that can't ride the set path). Every
//! destination that applies a window as ONE image per key needs the same fold:
//! replay the residue over the set-phase upserts into a single final state per
//! key, preserving insertion order. Iceberg and BigQuery both consume it; the
//! only difference is what they do with a leftover TOAST hole (iceberg refetches
//! from the source, BigQuery masks the column and lets its MERGE keep the target
//! value — read at the key the row came from, when the window moved it).
//!
//! The fold is also the one place that decides which keys a set-image apply
//! writes and how many times: `Resolved::rows` yields every key the window
//! touched exactly once — its final image, or its delete. Each consumer used to
//! add the delete set to the fold on its own terms, and BigQuery's (skip a
//! delete only for a key that re-landed) staged a key the tail had folded to
//! `Gone` twice: once for the `Gone`, once more for the delete set.

use crate::error::{Error, Result};
use crate::logbased::collapse::{Collapsed, Key, ResidueOp};
use crate::logbased::dest_pg::{quote_ident, quote_table};
use crate::logbased::rowtext::{row_key_refs, row_key_refs_cells, BYTEA_OID};
use crate::logbased::window::TableWindow;
use crate::wire::pgoutput::{Cell, Tuple};
use sqlx::Row as _;
use std::collections::HashMap;

/// One key's final state after replaying the residue tail over the set-phase
/// upserts (insertion order preserved — collapse.rs's map+vec move).
#[derive(Debug)]
enum Fin<'a> {
    /// Complete row image, borrowed from the collapsed window.
    Row(&'a [Cell]),
    /// Complete row image, owned (TOAST holes patched from an in-window base).
    Owned(Vec<Cell>),
    /// Still holds `UnchangedToast` cells — the destination resolves them
    /// (iceberg: refetch; bigquery: per-column mask against the target).
    /// `moved_from` is the key the row lived at before a key-changing update
    /// moved it, the first one of a chain: at the destination, that is where
    /// the untouched cells still are. `None` for a row that never moved.
    Masked { row: Vec<Cell>, moved_from: Option<Key> },
    /// Deleted (or vanished at the source): its image is a delete.
    Gone,
}

/// What a set-image apply writes for one key.
#[derive(Debug)]
pub(crate) enum Image<'a> {
    /// The key's complete final row.
    Row(&'a [Cell]),
    /// The key's final row with `UnchangedToast` holes, and where the row was
    /// before it moved (see `Fin::Masked`).
    Masked {
        row: &'a [Cell],
        // BigQuery's replica MERGE reads a moved row's holes at this key.
        moved_from: Option<&'a Key>,
    },
    /// The key's pre-window row goes, and nothing replaces it.
    Delete,
}

/// A window folded to one entry per key.
#[derive(Debug)]
pub(crate) struct Resolved<'a> {
    /// Every key an upsert or the residue tail touched, in first-touch order.
    finals: Vec<(Key, Fin<'a>)>,
    /// The deleted keys `finals` does not hold: a key there already has its
    /// one entry, a `Gone` one included.
    delete_only: Vec<&'a Key>,
}

impl<'a> Resolved<'a> {
    /// Every key the window touched, EXACTLY once: `finals` in order, then
    /// `delete_only`.
    pub(crate) fn rows(&self) -> impl Iterator<Item = (&Key, Image<'_>)> + '_ {
        let finals = self.finals.iter().map(|(key, fin)| {
            let image = match fin {
                Fin::Row(row) => Image::Row(row),
                Fin::Owned(row) => Image::Row(row),
                Fin::Masked { row, moved_from } => Image::Masked { row, moved_from: moved_from.as_ref() },
                Fin::Gone => Image::Delete,
            };
            (key, image)
        });
        finals.chain(self.delete_only.iter().map(|key| (*key, Image::Delete)))
    }

    /// Whether some key's final image still has a hole AND moved in this
    /// window: its untouched cells are at another key of the destination.
    /// BigQuery pays for the join that reads them there only when this holds.
    pub(crate) fn any_moved_mask(&self) -> bool {
        self.finals
            .iter()
            .any(|(_, fin)| matches!(fin, Fin::Masked { moved_from: Some(_), .. }))
    }

    /// The entries still holding a TOAST hole, for the source refetch to
    /// overwrite in place.
    fn masked_mut(&mut self) -> impl Iterator<Item = (&Key, &mut Fin<'a>)> {
        self.finals
            .iter_mut()
            .filter(|(_, fin)| matches!(fin, Fin::Masked { .. }))
            .map(|(key, fin)| (&*key, fin))
    }
}

/// Fold a collapsed window into one entry per key (see `Resolved`), keyed as
/// the window's layout says.
pub(crate) fn resolve_window<'a>(w: &'a TableWindow<Collapsed>) -> Resolved<'a> {
    let (c, pk_idx) = (w.body(), w.layout().key_idx());
    fn put<'a>(
        order: &mut Vec<(Key, Fin<'a>)>,
        index: &mut HashMap<Key, usize>,
        key: Key,
        fin: Fin<'a>,
    ) {
        match index.get(&key) {
            Some(&i) => order[i].1 = fin,
            None => {
                index.insert(key.clone(), order.len());
                order.push((key, fin));
            }
        }
    }
    /// The image a residue op patches its holes from, and the key the row came
    /// from if that image is itself a moved row's.
    fn base_of<'f>(
        order: &'f [(Key, Fin<'_>)],
        index: &HashMap<Key, usize>,
        key: &Key,
    ) -> (Option<&'f [Cell]>, Option<&'f Key>) {
        match index.get(key).map(|&i| &order[i].1) {
            Some(Fin::Row(b)) => (Some(*b), None),
            Some(Fin::Owned(b)) => (Some(b.as_slice()), None),
            Some(Fin::Masked { row, moved_from }) => (Some(row.as_slice()), moved_from.as_ref()),
            Some(Fin::Gone) | None => (None, None),
        }
    }
    fn patch<'a>(row: &[Cell], base: Option<&[Cell]>, moved_from: Option<Key>) -> Fin<'a> {
        let mut patched = row.to_vec();
        if let Some(b) = base {
            for (cell, bc) in patched.iter_mut().zip(b.iter()) {
                if matches!(cell, Cell::UnchangedToast) {
                    *cell = bc.clone();
                }
            }
        }
        if patched.iter().any(|x| matches!(x, Cell::UnchangedToast)) {
            Fin::Masked { row: patched, moved_from }
        } else {
            Fin::Owned(patched)
        }
    }
    let key_of = |row: &[Cell]| -> Key {
        row_key_refs_cells(row, pk_idx).into_iter().map(<[u8]>::to_vec).collect()
    };
    let key_of_t = |row: &Tuple| -> Key {
        row_key_refs(row, pk_idx).into_iter().map(<[u8]>::to_vec).collect()
    };
    let mut order: Vec<(Key, Fin<'a>)> = Vec::with_capacity(c.upserts.len());
    let mut index: HashMap<Key, usize> = HashMap::with_capacity(c.upserts.len());
    for row in &c.upserts {
        put(&mut order, &mut index, key_of_t(row), Fin::Owned(row.to_cells()));
    }
    for op in &c.residue {
        match op {
            ResidueOp::Upsert { row } => put(&mut order, &mut index, key_of(row), Fin::Row(row)),
            ResidueOp::Delete { key } => put(&mut order, &mut index, key.clone(), Fin::Gone),
            ResidueOp::MaskedUpdate { key, row } => {
                // The row is at its own key, unless the window moved it there:
                // then its holes are still wherever the move started.
                let (base, origin) = base_of(&order, &index, key);
                let fin = patch(row, base, origin.cloned());
                put(&mut order, &mut index, key.clone(), fin);
            }
            ResidueOp::Rekey { old_key, new_key, row } => {
                // The row moves: the old key ends up Gone, the new key takes
                // the image. The TOASTed cells the source did not resend are
                // patched from whatever this window already knows about the
                // OLD key — that is where the row still is. If nothing in the
                // window carries it, the hole survives as Masked and names the
                // key it came from (the first one, for a chain 1→9→5), which is
                // where the destination's copy of the untouched cells still
                // sits: BigQuery's MERGE reads them from the target's row
                // there. Iceberg, which cannot read its own rows back,
                // refetches from the source by the new key instead.
                let (base, origin) = base_of(&order, &index, old_key);
                let from = origin.unwrap_or(old_key).clone();
                let fin = patch(row, base, Some(from));
                put(&mut order, &mut index, old_key.clone(), Fin::Gone);
                put(&mut order, &mut index, new_key.clone(), fin);
            }
        }
    }
    // A TRUNCATE window has nothing old to delete: the destination restarts
    // empty, and what the window deleted after its TRUNCATE never landed.
    let delete_only = if c.truncate {
        Vec::new()
    } else {
        c.deletes.iter().filter(|k| !index.contains_key(*k)).collect()
    };
    Resolved { finals: order, delete_only }
}

/// The source a window was drained from, for the one read a destination may
/// make of it: filling a TOAST hole the WAL did not carry, which Iceberg cannot
/// read back from its own immutable files. Only reads — a destination module
/// holds one without holding anything it could write through (see
/// `lease::tests::no_connection_types_outside_store`).
pub(crate) struct Source<'a>(pub(crate) &'a sqlx::PgPool);

impl Source<'_> {
    /// Fill each remaining TOAST hole from the source's CURRENT row (see module
    /// docs for why the destination can't be read back). A key whose source row
    /// is already gone becomes a delete; its WAL delete arrives in a later
    /// window.
    pub(crate) async fn refetch_masked(
        &self,
        r: &mut Resolved<'_>,
        qualified_src: &str,
        pk: &str,
        wal_cols: &[String],
        oids: &[u32],
    ) -> Result<()> {
        if r.masked_mut().next().is_none() {
            return Ok(());
        }
        let dbg = std::env::var("APITAP_DEBUG").is_ok();
        let sel = wal_cols
            .iter()
            .zip(oids.iter())
            .map(|(c, &oid)| {
                let q = quote_ident(c);
                // bytea's ::text honors bytea_output — force the WAL's \x-hex form.
                if oid == BYTEA_OID {
                    format!("'\\x' || encode({q}, 'hex')")
                } else {
                    format!("{q}::text")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {sel} FROM {} WHERE {}::text = $1",
            quote_table(qualified_src),
            quote_ident(pk)
        );
        // One tx pins the session UTC so timestamptz::text matches the WAL's
        // +00-suffixed rendering (SET LOCAL dies with the tx).
        let mut tx = self.0.begin().await.map_err(db_err)?;
        sqlx::query("SET LOCAL TimeZone = 'UTC'")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        for (key, fin) in r.masked_mut() {
            let Fin::Masked { row: cells, .. } = fin else { continue };
            let ktext = String::from_utf8(key[0].clone())
                .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?;
            let row = sqlx::query(&sql)
                .bind(&ktext)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_err)?;
            match row {
                Some(r) => {
                    for (i, cell) in cells.iter_mut().enumerate() {
                        if matches!(cell, Cell::UnchangedToast) {
                            let v: Option<String> = r.try_get(i).map_err(db_err)?;
                            *cell = match v {
                                None => Cell::Null,
                                Some(s) => Cell::Text(bytes::Bytes::from(s)),
                            };
                        }
                    }
                    *fin = Fin::Owned(std::mem::take(cells));
                }
                None => {
                    if dbg {
                        eprintln!(
                            "[log_based] TOAST refetch: {qualified_src} key '{ktext}' is \
                             gone on the source — dropping the row image"
                        );
                    }
                    *fin = Fin::Gone;
                }
            }
        }
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
}

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based: source refetch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbased::collapse::{gen, Collapser, DeleteSet};
    use crate::logbased::window::Layout;
    use std::collections::HashSet;

    fn t(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
    fn key1(s: &str) -> Key {
        vec![s.as_bytes().to_vec()]
    }

    /// Each row of `r` as `key:what`: `row=<cell 1>`, `masked<-<origin>`,
    /// `masked` (never moved), or `delete`.
    fn shape(r: &Resolved) -> Vec<String> {
        let s = |b: &[u8]| String::from_utf8(b.to_vec()).unwrap();
        r.rows()
            .map(|(k, img)| match img {
                Image::Row(row) => match &row[1] {
                    Cell::Text(b) => format!("{}:row={}", s(&k[0]), s(b)),
                    other => format!("{}:row={other:?}", s(&k[0])),
                },
                Image::Masked { moved_from: Some(m), .. } => format!("{}:masked<-{}", s(&k[0]), s(&m[0])),
                Image::Masked { moved_from: None, .. } => format!("{}:masked", s(&k[0])),
                Image::Delete => format!("{}:delete", s(&k[0])),
            })
            .collect()
    }

    /// Seal `c` as a window of `(id, v)` keyed on id, the shape every
    /// hand-built window here has.
    fn sealed(c: Collapsed) -> TableWindow<Collapsed> {
        TableWindow::seal("t", Layout::for_test(&["id", "v"], &[], &["id"]), c).expect("rows fit")
    }

    fn hole(k: &str) -> Vec<Cell> {
        vec![t(k), Cell::UnchangedToast]
    }

    fn rekey(from: &str, to: &str) -> ResidueOp {
        ResidueOp::Rekey { old_key: key1(from), new_key: key1(to), row: hole(to) }
    }

    #[test]
    fn residue_replay_lands_final_rows_in_order() {
        let c = Collapsed {
            deletes: DeleteSet::from_keys(vec![key1("9")]),
            upserts: vec![
                Tuple::from_cells(&[t("1"), t("a")]),
                Tuple::from_cells(&[t("2"), t("b")]),
            ],
            residue: vec![
                ResidueOp::MaskedUpdate { key: key1("1"), row: hole("1") },
                ResidueOp::Upsert { row: vec![t("3"), t("c")] },
                ResidueOp::Delete { key: key1("2") },
            ],
            truncate: false,
            events: 5,
        };
        // Finals in first-touch order, the masked update patched from its
        // in-window base, then the key the window only deleted.
        assert_eq!(shape(&resolve_window(&sealed(c))), ["1:row=a", "2:delete", "3:row=c", "9:delete"]);
    }

    #[test]
    fn masked_without_base_needs_refetch_and_later_ops_still_win() {
        let c = Collapsed {
            residue: vec![ResidueOp::MaskedUpdate { key: key1("7"), row: hole("7") }],
            ..Default::default()
        };
        assert_eq!(shape(&resolve_window(&sealed(c))), ["7:masked"]);

        let c2 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate { key: key1("7"), row: hole("7") },
                ResidueOp::MaskedUpdate { key: key1("7"), row: hole("7") },
                ResidueOp::Delete { key: key1("7") },
            ],
            ..Default::default()
        };
        assert_eq!(shape(&resolve_window(&sealed(c2))), ["7:delete"]);

        let c3 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate { key: key1("7"), row: hole("7") },
                ResidueOp::Upsert { row: vec![t("7"), t("z")] },
            ],
            ..Default::default()
        };
        assert_eq!(shape(&resolve_window(&sealed(c3))), ["7:row=z"]);
    }

    #[test]
    fn moved_mask_records_origin() {
        // A move with a hole and nothing in the window for the old key: the
        // hole is still at key 1 on the destination.
        let c = Collapsed { residue: vec![rekey("1", "9")], ..Default::default() };
        let w = sealed(c);
        assert_eq!(shape(&resolve_window(&w)), ["1:delete", "9:masked<-1"]);
        assert!(resolve_window(&w).any_moved_mask(), "BigQuery's MERGE must read key 1");

        // A chain 1→9→5 still points at 1: 9 never held the row there.
        let c = Collapsed { residue: vec![rekey("1", "9"), rekey("9", "5")], ..Default::default() };
        assert_eq!(shape(&resolve_window(&sealed(c))), ["1:delete", "9:delete", "5:masked<-1"]);

        // A masked update at a key the row was moved to: still at 1.
        let c = Collapsed {
            residue: vec![rekey("1", "9"), ResidueOp::MaskedUpdate { key: key1("9"), row: hole("9") }],
            ..Default::default()
        };
        assert_eq!(shape(&resolve_window(&sealed(c))), ["1:delete", "9:masked<-1"]);

        // A plain masked update never moved; a masked row moved later came
        // from the key it was masked at.
        let c = Collapsed {
            residue: vec![ResidueOp::MaskedUpdate { key: key1("1"), row: hole("1") }],
            ..Default::default()
        };
        let w = sealed(c);
        assert_eq!(shape(&resolve_window(&w)), ["1:masked"]);
        assert!(!resolve_window(&w).any_moved_mask(), "a hole at its own key needs no join");
        let c = Collapsed {
            residue: vec![ResidueOp::MaskedUpdate { key: key1("1"), row: hole("1") }, rekey("1", "9")],
            ..Default::default()
        };
        assert_eq!(shape(&resolve_window(&sealed(c))), ["1:delete", "9:masked<-1"]);

        // A move whose old key the window already holds whole is no hole at all.
        let c = Collapsed {
            upserts: vec![Tuple::from_cells(&[t("1"), t("full")])],
            residue: vec![rekey("1", "9")],
            ..Default::default()
        };
        let w = sealed(c);
        assert_eq!(shape(&resolve_window(&w)), ["1:delete", "9:row=full"]);
        assert!(!resolve_window(&w).any_moved_mask(), "a move with no hole needs no join");
    }

    #[test]
    fn every_key_once() {
        // B2: 5,000 random windows of inserts, updates, masked updates,
        // deletes, moves and truncates on six keys.
        let mut rng = gen::Rng(0x2545_f491_4f6c_dd1d);
        for _ in 0..5000 {
            let len = 1 + rng.below(12) as usize;
            let evs = gen::events(&mut rng, 6, len);
            let mut cl = Collapser::new(gen::layout());
            gen::feed(&mut cl, &evs);
            let w = cl.seal("t").expect("rows fit");
            let (c, r) = (w.body(), resolve_window(&w));
            let keys: Vec<&Key> = r.rows().map(|(k, _)| k).collect();
            let unique: HashSet<&Key> = keys.iter().copied().collect();
            assert_eq!(keys.len(), unique.len(), "a key twice: {keys:?} from {evs:?}");
            assert!(
                r.delete_only.iter().all(|k| !r.finals.iter().any(|(f, _)| f == *k)),
                "delete_only meets finals, from {evs:?}"
            );
            // Nothing is lost either: every deleted key and every landed key
            // is there, and a TRUNCATE window adds no delete of its own.
            if c.truncate {
                assert!(r.delete_only.is_empty(), "{evs:?}");
            } else {
                assert!(c.deletes.iter().all(|k| unique.contains(k)), "a delete lost, from {evs:?}");
            }
            for row in &c.upserts {
                let k: Key = vec![match &row.to_cells()[0] {
                    Cell::Text(b) => b.to_vec(),
                    other => panic!("key cell {other:?}"),
                }];
                assert!(unique.contains(&k), "an upsert lost, from {evs:?}");
            }
        }
    }
}
