//! A table's window carries its column layout (brief §2.B §3.A).
//!
//! The drains used to hand every destination a loose set of parallel maps —
//! the bodies per table, the column names per table, the type OIDs per table —
//! and each apply looked the layout up for itself, failing when it was not
//! there. A body without its layout was expressible, and a MySQL-source window
//! holding a table's TRUNCATE and no row of it was exactly that: the layout was
//! only ever filled by a rows event, so every apply failed for want of a column
//! list, on every run, and the watermark never moved again (audit §3.4).
//!
//! Now `TableWindow::seal` is the only way to build a table's window and it
//! takes the layout, so every apply reads its columns, OIDs and keys from the
//! window it was handed. The drains resolve a layout before the first op of a
//! table lands in a body, and never let one window span a layout change: the
//! MySQL drain cuts the window at every DDL, the Postgres drain carries a
//! transaction whose Relation changed a table's layout into the next window.

use crate::error::{Error, Result};
use crate::logbased::changelog::{ChangeOp, Changes};
use crate::logbased::collapse::{Collapsed, Collapser, ResidueOp};
use crate::logbased::replay::WindowId;
use crate::wire::mybinlog::TableSchema;
use crate::wire::pgoutput::Relation;
use std::collections::HashMap;
use std::sync::Arc;

/// One table's columns as the source ships its rows: names and type OIDs in
/// row order, and the run's key columns with their positions in that order.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Layout {
    cols: Vec<String>,
    oids: Vec<u32>,
    key_cols: Vec<String>,
    key_idx: Vec<usize>,
}

impl Layout {
    /// A pgoutput Relation. `Ok(None)` is a relation this run does not track.
    ///
    /// The key is found by NAME, from the run's key list (the destination's
    /// plan), never from the Relation's key flags: under REPLICA IDENTITY FULL
    /// the WAL flags every column. The flags still decide one thing: under
    /// DEFAULT or USING INDEX an old image carries only the flagged columns, so
    /// the run's key must be among them.
    pub(crate) fn from_relation(r: &Relation, key_cols: &HashMap<String, Vec<String>>) -> Result<Option<Layout>> {
        let table = format!("{}.{}", r.namespace, r.name);
        let Some(want) = key_cols.get(&table) else {
            return Ok(None);
        };
        if r.replica_identity == b'n' {
            return Err(Error::InvalidInput(format!(
                "log_based: table {table} has REPLICA IDENTITY NOTHING — updates and \
                 deletes carry no key. Run: ALTER TABLE {table} REPLICA IDENTITY \
                 DEFAULT (with a primary key) or FULL"
            )));
        }
        let cols: Vec<String> = r.cols.iter().map(|c| c.name.clone()).collect();
        let key_idx = key_positions(&cols, want, |k| {
            Error::Transfer(format!("log_based: key column '{k}' not in WAL relation for {table}"))
        })?;
        if r.replica_identity != b'f' {
            for &i in &key_idx {
                if !r.cols[i].key {
                    return Err(Error::InvalidInput(format!(
                        "log_based: key column '{}' of {table} is not part of the \
                         source's REPLICA IDENTITY — old images won't carry it. \
                         Use the source PK as the key, or ALTER TABLE {table} \
                         REPLICA IDENTITY FULL",
                        r.cols[i].name
                    )));
                }
            }
        }
        Ok(Some(Layout {
            oids: r.cols.iter().map(|c| c.type_oid).collect(),
            cols,
            key_cols: want.clone(),
            key_idx,
        }))
    }

    /// A MySQL/MariaDB table, from its information_schema columns
    /// (`binlog_row_metadata=MINIMAL` ships no names). Keyed by NAME from the
    /// run's key list — the names every apply's SQL spells — not by the
    /// catalog's PRI flags, which only decided the key when the run resolved
    /// it and may say something else after an ALTER. OIDs are all 0: the value
    /// text carries the type, and the appliers read the destination's DDL.
    pub(crate) fn from_mysql(table: &str, sc: &TableSchema, key_cols: &[String]) -> Result<Layout> {
        let key_idx = key_positions(&sc.names, key_cols, |k| {
            Error::Transfer(format!(
                "log_based: {table}: key column '{k}' is not among the source's columns ({}) — \
                 the table's key changed since this run resolved it. Clear this table's apitap \
                 state on the destination and re-run to bootstrap it from its current schema.",
                sc.names.join(", ")
            ))
        })?;
        Ok(Layout {
            cols: sc.names.clone(),
            oids: vec![0; sc.names.len()],
            key_cols: key_cols.to_vec(),
            key_idx,
        })
    }

    /// Column names in row order.
    pub(crate) fn cols(&self) -> &[String] {
        &self.cols
    }

    /// Type OIDs, parallel to `cols` (0 on the MySQL lane).
    pub(crate) fn oids(&self) -> &[u32] {
        &self.oids
    }

    /// The run's key columns, in key order — the names the applies' SQL uses.
    pub(crate) fn key_cols(&self) -> &[String] {
        &self.key_cols
    }

    /// Where each key column sits in a row, in key order.
    pub(crate) fn key_idx(&self) -> &[usize] {
        &self.key_idx
    }

    /// The key columns' type OIDs, in key order.
    pub(crate) fn key_oids(&self) -> Vec<u32> {
        self.key_idx.iter().map(|&i| self.oids[i]).collect()
    }

    /// A layout for a test: `keys` must be among `cols`; `oids` empty means
    /// all 0.
    #[cfg(test)]
    pub(crate) fn for_test(cols: &[&str], oids: &[u32], keys: &[&str]) -> Arc<Layout> {
        let cols: Vec<String> = cols.iter().map(|c| c.to_string()).collect();
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let key_idx = key_positions(&cols, &keys, |k| Error::Transfer(format!("no key {k}"))).expect("keys in cols");
        let oids = if oids.is_empty() { vec![0; cols.len()] } else { oids.to_vec() };
        Arc::new(Layout { cols, oids, key_cols: keys, key_idx })
    }
}

/// Each key column's position among `cols`, in `keys` order; `missing` names
/// the first one that is not there.
fn key_positions(cols: &[String], keys: &[String], missing: impl Fn(&str) -> Error) -> Result<Vec<usize>> {
    keys.iter()
        .map(|k| cols.iter().position(|c| c == k).ok_or_else(|| missing(k)))
        .collect()
}

/// A body a window can carry: its rows must fit the window's layout.
pub(crate) trait WindowBody {
    /// `Err(len)` names the first row that does not fit `ncols` columns.
    fn rows_fit(&self, ncols: usize) -> std::result::Result<(), usize>;
    /// Source events the body consumed, for reporting.
    fn event_count(&self) -> u64;
}

impl WindowBody for Collapsed {
    /// Every image the apply writes is a whole row: the upserts, and every
    /// residue op that carries one.
    fn rows_fit(&self, ncols: usize) -> std::result::Result<(), usize> {
        for row in &self.upserts {
            if row.len() != ncols {
                return Err(row.len());
            }
        }
        for op in &self.residue {
            let row = match op {
                ResidueOp::MaskedUpdate { row, .. } | ResidueOp::Upsert { row } | ResidueOp::Rekey { row, .. } => row,
                ResidueOp::Delete { .. } => continue,
            };
            if row.len() != ncols {
                return Err(row.len());
            }
        }
        Ok(())
    }

    fn event_count(&self) -> u64 {
        self.events
    }
}

impl WindowBody for Changes {
    /// `I`/`U` records are whole rows. A `D` record is the old image, and a
    /// short one is a real wire shape (a key-only image), so it only may not be
    /// LONGER than the layout.
    fn rows_fit(&self, ncols: usize) -> std::result::Result<(), usize> {
        for ev in &self.events {
            let Some(row) = &ev.row else { continue };
            let fits = match ev.op {
                ChangeOp::Delete => row.len() <= ncols,
                _ => row.len() == ncols,
            };
            if !fits {
                return Err(row.len());
            }
        }
        Ok(())
    }

    fn event_count(&self) -> u64 {
        self.count
    }
}

/// One table's window: its body and the layout its rows are in. Only `seal`
/// builds one.
pub(crate) struct TableWindow<B> {
    layout: Arc<Layout>,
    body: B,
}

impl<B: WindowBody> TableWindow<B> {
    /// The window's rows must all be in `layout`'s shape. One source
    /// transaction that changes a tracked table's columns between two of its
    /// own row changes is the one way they are not; it is refused here, where
    /// it would otherwise render one shape under the other's column names.
    pub(crate) fn seal(table: &str, layout: Arc<Layout>, body: B) -> Result<Self> {
        let n = layout.cols.len();
        if let Err(got) = body.rows_fit(n) {
            return Err(Error::Transfer(format!(
                "log_based: {table}: a row in this window has {got} columns but the window's \
                 layout has {n} — the table's definition changed inside one source transaction. \
                 Clear this table's apitap state on the destination and re-run to bootstrap it \
                 from its current schema."
            )));
        }
        Ok(Self { layout, body })
    }

    pub(crate) fn layout(&self) -> &Layout {
        &self.layout
    }

    pub(crate) fn body(&self) -> &B {
        &self.body
    }
}

/// A drained window's bodies, one per table that had traffic. A run is one
/// lane, so a window is all replica bodies or all changelog bodies.
pub(crate) enum Bodies {
    Replica(HashMap<String, TableWindow<Collapsed>>),
    Changelog(HashMap<String, TableWindow<Changes>>),
}

impl Bodies {
    /// Seal a drain's accumulators, each body with the layout it was built
    /// with. Only the run's own lane's map holds anything.
    pub(crate) fn seal(
        changelog: bool,
        collapsers: HashMap<String, Collapser>,
        changelogs: HashMap<String, Changes>,
    ) -> Result<Bodies> {
        debug_assert!(if changelog { collapsers.is_empty() } else { changelogs.is_empty() });
        Ok(if changelog {
            Bodies::Changelog(
                changelogs.into_iter().map(|(t, c)| c.seal(&t).map(|w| (t, w))).collect::<Result<_>>()?,
            )
        } else {
            Bodies::Replica(
                collapsers.into_iter().map(|(t, c)| c.seal(&t).map(|w| (t, w))).collect::<Result<_>>()?,
            )
        })
    }
}

/// One table's part of a window, in its lane; `None` when the window had no
/// traffic for it (the apply then only moves the watermark).
pub(crate) enum Slice<'a> {
    Replica(Option<&'a TableWindow<Collapsed>>),
    Changelog(Option<&'a TableWindow<Changes>>),
}

/// One drained window, as both drains hand it to the applies.
pub(crate) struct DrainOutcome {
    pub bodies: Bodies,
    pub id: WindowId,
    /// Apply this window and drain again at once: the drain stopped at the
    /// memory budget, or at a layout change, not at the stop-line.
    pub hit_budget: bool,
    /// The stream negotiated `binary 'true'` AND this lane renders RowBinary
    /// bodies: the tuples' cells are send-format bytes, not text.
    pub binary: bool,
}

impl DrainOutcome {
    /// The window of `qualified` (the source's "schema.table" / "db.table").
    pub(crate) fn slice(&self, qualified: &str) -> Slice<'_> {
        match &self.bodies {
            Bodies::Replica(m) => Slice::Replica(m.get(qualified)),
            Bodies::Changelog(m) => Slice::Changelog(m.get(qualified)),
        }
    }

    /// Source events across every body, for the debug lines.
    pub(crate) fn events(&self) -> u64 {
        match &self.bodies {
            Bodies::Replica(m) => m.values().map(|w| w.body.event_count()).sum(),
            Bodies::Changelog(m) => m.values().map(|w| w.body.event_count()).sum(),
        }
    }

    /// Tables with traffic in this window.
    pub(crate) fn tables(&self) -> usize {
        match &self.bodies {
            Bodies::Replica(m) => m.len(),
            Bodies::Changelog(m) => m.len(),
        }
    }

    /// No table had traffic.
    pub(crate) fn is_empty(&self) -> bool {
        self.tables() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pgoutput::{Cell, RelationCol, Tuple};

    fn t(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
    fn row(n: usize) -> Tuple {
        Tuple::from_cells(&(0..n).map(|i| t(&i.to_string())).collect::<Vec<_>>())
    }

    /// A1. A body whose rows are not all the layout's width is refused, naming
    /// the table; a short `D` image is a real wire shape and is not.
    #[test]
    fn seal_rejects_mixed_row_lengths() {
        let l = Layout::for_test(&["id", "v", "w"], &[], &["id"]);
        let refused = |r: Result<()>| match r {
            Err(e) => {
                let m = e.to_string();
                assert!(m.contains("bench.t") && m.contains("has 4 columns") && m.contains("layout has 3"), "{m}");
            }
            Ok(()) => panic!("a 4-column row sealed into a 3-column layout"),
        };

        // Replica: rows of 3 and 4 cells — an upsert, and a residue row.
        let up = Collapsed { upserts: vec![row(3), row(4)], ..Default::default() };
        refused(TableWindow::seal("bench.t", l.clone(), up).map(|_| ()));
        let tail = Collapsed {
            upserts: vec![row(3)],
            residue: vec![ResidueOp::Upsert { row: row(4).to_cells() }],
            ..Default::default()
        };
        refused(TableWindow::seal("bench.t", l.clone(), tail).map(|_| ()));
        let good = Collapsed {
            upserts: vec![row(3)],
            residue: vec![ResidueOp::Delete { key: vec![b"1".to_vec()] }],
            ..Default::default()
        };
        assert!(TableWindow::seal("bench.t", l.clone(), good).is_ok());

        // Changelog: an I of 3 and a U of 4.
        let mut c = Changes::new(l.clone());
        c.insert(row(3));
        c.update(None, row(4));
        refused(TableWindow::seal("bench.t", l.clone(), c).map(|_| ()));
        // A key-only D image is shorter than the layout, and fits; a T has no row.
        let mut c = Changes::new(l.clone());
        c.insert(row(3));
        c.delete(row(1));
        c.truncate();
        let w = c.seal("bench.t").expect("a short D image fits");
        assert_eq!(w.layout(), &*l);
        // …but a D image longer than the layout does not.
        let mut c = Changes::new(l.clone());
        c.delete(row(4));
        refused(c.seal("bench.t").map(|_| ()));
    }

    fn rel(identity: u8, cols: &[(&str, bool)]) -> Relation {
        Relation {
            rel_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: identity,
            cols: cols
                .iter()
                .enumerate()
                .map(|(i, (n, key))| RelationCol { key: *key, name: n.to_string(), type_oid: 20 + i as u32, type_mod: -1 })
                .collect(),
        }
    }

    /// A3. Key positions come from the run's key NAMES, in the run's key order;
    /// a key column the source does not have is an error naming it.
    #[test]
    fn layout_by_name() {
        // MySQL: the catalog's PRI flag sits on `a`, the run's key is (k2, id).
        let sc = TableSchema {
            names: vec!["a".into(), "id".into(), "b".into(), "k2".into()],
            key: vec![true, false, false, false],
            unsigned: vec![false; 4],
            labels: vec![None; 4],
        };
        let keys = vec!["k2".to_string(), "id".to_string()];
        let l = Layout::from_mysql("bench.t", &sc, &keys).expect("layout");
        assert_eq!(l.key_idx(), &[3, 1]);
        assert_eq!(l.key_cols(), keys.as_slice());
        assert_eq!(l.cols(), sc.names.as_slice());
        assert_eq!(l.oids(), &[0, 0, 0, 0]);
        let e = Layout::from_mysql("bench.t", &sc, &["nope".to_string()]).expect_err("a missing key column");
        assert!(e.to_string().contains("'nope'") && e.to_string().contains("bench.t"), "{e}");

        // Postgres: key (id1, id2) sits at 2 and 0, whatever the column order.
        let keys: HashMap<String, Vec<String>> =
            [("public.t".to_string(), vec!["id1".to_string(), "id2".to_string()])].into_iter().collect();
        let r = rel(b'd', &[("id2", true), ("v", false), ("id1", true)]);
        let l = Layout::from_relation(&r, &keys).expect("ok").expect("tracked");
        assert_eq!(l.key_idx(), &[2, 0]);
        assert_eq!(l.key_oids(), vec![22, 20]);
        assert_eq!(l.cols(), &["id2".to_string(), "v".to_string(), "id1".to_string()]);
        // FULL flags every column; the names still decide.
        let r = rel(b'f', &[("id2", true), ("v", true), ("id1", true)]);
        assert_eq!(Layout::from_relation(&r, &keys).unwrap().unwrap().key_idx(), &[2, 0]);
        // A key column the relation lacks names itself.
        let r = rel(b'd', &[("id2", true), ("v", false)]);
        let e = Layout::from_relation(&r, &keys).expect_err("missing id1");
        assert!(e.to_string().contains("'id1'"), "{e}");
        // Under DEFAULT the key must be flagged; NOTHING is refused; an
        // untracked relation has no layout.
        let r = rel(b'd', &[("id2", true), ("v", false), ("id1", false)]);
        assert!(Layout::from_relation(&r, &keys).is_err());
        assert!(Layout::from_relation(&rel(b'n', &[("id1", true), ("id2", true)]), &keys).is_err());
        assert!(Layout::from_relation(&r, &HashMap::new()).unwrap().is_none());
    }
}
