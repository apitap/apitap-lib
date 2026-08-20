//! Shared residue resolution for the log_based apply paths.
//!
//! A drained window collapses to `deletes`, `upserts` and an ordered `residue`
//! tail (unchanged-TOAST updates that can't ride the set path). Every
//! destination that applies a window as ONE image per key needs the same fold:
//! replay the residue over the set-phase upserts into a single final state per
//! key, preserving insertion order. Iceberg and BigQuery both consume it; the
//! only difference is what they do with a leftover TOAST hole (iceberg refetches
//! from the source, BigQuery masks the column and lets its MERGE keep the target
//! value).

use crate::logbased::collapse::{Collapsed, Key, ResidueOp};
use crate::logbased::rowtext::{row_key_refs, row_key_refs_cells};
use crate::wire::pgoutput::{Cell, Tuple};
use std::collections::HashMap;

/// One key's final state after replaying the residue tail over the set-phase
/// upserts (insertion order preserved — collapse.rs's map+vec move).
#[derive(Debug)]
pub(crate) enum Fin<'a> {
    /// Complete row image, borrowed from the collapsed window.
    Row(&'a [Cell]),
    /// Complete row image, owned (TOAST holes patched from an in-window base).
    Owned(Vec<Cell>),
    /// Still holds `UnchangedToast` cells — the destination resolves them
    /// (iceberg: refetch; bigquery: per-column mask against the target).
    Refetch(Vec<Cell>),
    /// Deleted (or vanished at the source): delete-set only.
    Gone,
}

/// Fold a collapsed window into an ordered `(key, final-state)` list.
pub(crate) fn resolve_window<'a>(c: &'a Collapsed, pk_idx: &[usize]) -> Vec<(Key, Fin<'a>)> {
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
                let base: Option<&[Cell]> = index.get(key).and_then(|&i| match &order[i].1 {
                    Fin::Row(b) => Some(*b),
                    Fin::Owned(b) | Fin::Refetch(b) => Some(b.as_slice()),
                    Fin::Gone => None,
                });
                let mut patched = row.clone();
                if let Some(b) = base {
                    for (cell, bc) in patched.iter_mut().zip(b.iter()) {
                        if matches!(cell, Cell::UnchangedToast) {
                            *cell = bc.clone();
                        }
                    }
                }
                let fin = if patched.iter().any(|x| matches!(x, Cell::UnchangedToast)) {
                    Fin::Refetch(patched)
                } else {
                    Fin::Owned(patched)
                };
                put(&mut order, &mut index, key.clone(), fin);
            }
            ResidueOp::Rekey { old_key, new_key, row } => {
                // The row moves: the old key ends up Gone, the new key takes
                // the image. The TOASTed cells the source did not resend are
                // patched from whatever this window already knows about the
                // OLD key — that is where the row still is. If nothing in the
                // window carries it, the hole survives as Refetch and the
                // destination resolves it the way it resolves any other one
                // (iceberg refetches from the source; BigQuery masks the column
                // and lets the MERGE keep the target's value).
                let base: Option<Vec<Cell>> =
                    index.get(old_key).and_then(|&i| match &order[i].1 {
                        Fin::Row(b) => Some(b.to_vec()),
                        Fin::Owned(b) | Fin::Refetch(b) => Some(b.clone()),
                        Fin::Gone => None,
                    });
                let mut patched = row.clone();
                if let Some(b) = base {
                    for (cell, bc) in patched.iter_mut().zip(b.iter()) {
                        if matches!(cell, Cell::UnchangedToast) {
                            *cell = bc.clone();
                        }
                    }
                }
                put(&mut order, &mut index, old_key.clone(), Fin::Gone);
                let fin = if patched.iter().any(|x| matches!(x, Cell::UnchangedToast)) {
                    Fin::Refetch(patched)
                } else {
                    Fin::Owned(patched)
                };
                put(&mut order, &mut index, new_key.clone(), fin);
            }
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
    fn key1(s: &str) -> Key {
        vec![s.as_bytes().to_vec()]
    }

    #[test]
    fn residue_replay_lands_final_rows_in_order() {
        let c = Collapsed {
            deletes: vec![key1("9")],
            upserts: vec![
                Tuple::from_cells(&[t("1"), t("a")]),
                Tuple::from_cells(&[t("2"), t("b")]),
            ],
            residue: vec![
                ResidueOp::MaskedUpdate {
                    key: key1("1"),
                    row: vec![t("1"), Cell::UnchangedToast],
                },
                ResidueOp::Upsert { row: vec![t("3"), t("c")] },
                ResidueOp::Delete { key: key1("2") },
            ],
            truncate: false,
            events: 5,
        };
        let out = resolve_window(&c, &[0]);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].0, key1("1"));
        assert!(matches!(&out[0].1, Fin::Owned(r) if r[1] == t("a")));
        assert!(matches!(out[1].1, Fin::Gone));
        assert_eq!(out[2].0, key1("3"));
        assert!(matches!(&out[2].1, Fin::Row(r) if r[0] == t("3")));
    }

    #[test]
    fn masked_without_base_needs_refetch_and_later_ops_still_win() {
        let c = Collapsed {
            residue: vec![ResidueOp::MaskedUpdate {
                key: key1("7"),
                row: vec![t("7"), Cell::UnchangedToast],
            }],
            ..Default::default()
        };
        let out = resolve_window(&c, &[0]);
        assert!(matches!(&out[0].1, Fin::Refetch(r) if r[1] == Cell::UnchangedToast));

        let c2 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate { key: key1("7"), row: vec![t("7"), Cell::UnchangedToast] },
                ResidueOp::MaskedUpdate { key: key1("7"), row: vec![t("7"), Cell::UnchangedToast] },
                ResidueOp::Delete { key: key1("7") },
            ],
            ..Default::default()
        };
        let out2 = resolve_window(&c2, &[0]);
        assert_eq!(out2.len(), 1);
        assert!(matches!(out2[0].1, Fin::Gone));

        let c3 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate { key: key1("7"), row: vec![t("7"), Cell::UnchangedToast] },
                ResidueOp::Upsert { row: vec![t("7"), t("z")] },
            ],
            ..Default::default()
        };
        assert!(matches!(&resolve_window(&c3, &[0])[0].1, Fin::Row(r) if r[1] == t("z")));
    }
}
