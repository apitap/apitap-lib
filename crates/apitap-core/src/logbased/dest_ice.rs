//! log_based apply into Apache Iceberg — one snapshot per window is the
//! atom: the window's final row images land as a data file, every touched
//! key becomes an equality-delete file in the SAME snapshot (same-snapshot
//! data is exempt from its own deletes by sequence inheritance — the merge
//! path's trick), and the LSN watermark rides that catalog commit as table
//! properties. A crashed window replays whole and converges, exactly like
//! the SQL destinations.
//!
//! Unchanged-TOAST updates can't read the destination back (snapshots are
//! immutable files), so unresolved holes refetch the CURRENT row from the
//! source instead — a possibly-later image, which later windows overwrite
//! again: convergent, not time-travel-exact for that key.

use crate::error::{Error, Result};
use crate::logbased::collapse::{Collapsed, Key, ResidueOp};
use crate::logbased::dest_pg::{quote_ident, quote_table};
use crate::logbased::drain::DrainOutcome;
use crate::logbased::rowtext::{decode_bytea, pk_indices, row_key_refs, strip_utc_offset, BYTEA_OID};
use crate::plan::Delivered;
use crate::sink::iceberg::{cdc_bind, cdc_read_state, cdc_set_watermark, CdcWindow, IcebergConn};
use crate::wire::bqparquet::ParquetEncoder;
use crate::wire::pgcopy as pgc;
use crate::wire::pgoutput::Cell;
use sqlx::{PgPool, Row as _};
use std::collections::{HashMap, HashSet};

pub(crate) struct IceDest {
    conn: IcebergConn,
}

/// `dest_table` may arrive schema-qualified; the iceberg namespace comes from
/// the URL, so only the bare name addresses the table (same trim as the sink).
fn bare(dest_table: &str) -> &str {
    dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t)
}

fn single_pk(pk_cols: &[String]) -> Result<&str> {
    match pk_cols {
        [one] => Ok(one),
        _ => Err(Error::InvalidInput(format!(
            "iceberg log_based needs a single-column primary key — the source \
             PK is ({})",
            pk_cols.join(", ")
        ))),
    }
}

impl IceDest {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        Ok(Self { conn: IcebergConn::parse(url).await? })
    }

    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<u64>> {
        cdc_read_state(&self.conn, bare(dest_table), source_id).await
    }

    /// The bootstrap's replace just created the table (and cleared every
    /// apitap watermark property) — stamp the slot's LSN as state. No
    /// snapshot: the data is already committed.
    pub(crate) async fn bootstrap_finish(
        &self,
        dest_table: &str,
        source_id: &str,
        pk_cols: &[String],
        lsn: u64,
        _rows: u64,
    ) -> Result<()> {
        single_pk(pk_cols)?;
        cdc_set_watermark(&self.conn, bare(dest_table), source_id, lsn).await
    }

    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
        src: &PgPool,
    ) -> Result<u64> {
        let table = bare(dest_table);
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            cdc_set_watermark(&self.conn, table, source_id, outcome.end_lsn).await?;
            return Ok(0);
        };
        let wal_cols = outcome
            .wal_cols
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL column list".into()))?;
        let oids = outcome
            .wal_oids
            .get(qualified_src)
            .ok_or_else(|| Error::Transfer("log_based: missing WAL type list".into()))?;
        let pk = single_pk(pk_cols)?;
        let pk_idx = pk_indices(pk_cols, wal_cols)?;
        let pk_i = pk_idx[0];
        let key_int = match oids[pk_i] {
            20 | 21 | 23 => true,
            25 | 1043 | 2950 => false,
            other => {
                return Err(Error::InvalidInput(format!(
                    "log_based: primary key '{pk}' has type oid {other} — iceberg \
                     equality deletes support integer, text/varchar and uuid keys"
                )))
            }
        };

        let bound = cdc_bind(&self.conn, table, wal_cols, oids).await?;

        // Replay the residue tail over the set-phase upserts into one final
        // per-key state; unresolved TOAST holes go back to the source.
        let mut finals = resolve_window(c, &pk_idx);
        refetch_masked(&mut finals, qualified_src, pk, wal_cols, oids, src).await?;

        // Delete-set: every touched key (deleted or re-landed). A TRUNCATE
        // window starts from an empty manifest list — nothing old to delete.
        let (mut del_ints, mut del_texts) = (Vec::new(), Vec::new());
        if !c.truncate {
            let mut seen: HashSet<&[u8]> =
                HashSet::with_capacity(c.deletes.len() + finals.len());
            for key in c.deletes.iter().chain(finals.iter().map(|(k, _)| k)) {
                let k = key[0].as_slice();
                if !seen.insert(k) {
                    continue;
                }
                if key_int {
                    del_ints.push(parse_int_key(k)?);
                } else {
                    del_texts.push(
                        String::from_utf8(k.to_vec())
                            .map_err(|_| Error::Transfer("log_based: non-UTF8 key value".into()))?,
                    );
                }
            }
        }

        let n_rows = finals.iter().filter(|(_, f)| !matches!(f, Fin::Gone)).count() as u64;
        let data = if n_rows > 0 {
            let mut enc = ParquetEncoder::new_ext(
                wal_cols.clone(),
                bound.delivered.clone(),
                None,
                Some(bound.field_ids.clone()),
                None,
            )?;
            let mut chunk = Vec::with_capacity(256 << 10);
            pgc::header(&mut chunk);
            let mut sent = 0u64;
            for (_, fin) in &finals {
                let row: &[Cell] = match fin {
                    Fin::Row(r) => r,
                    Fin::Owned(r) => r,
                    Fin::Gone => continue,
                    Fin::Refetch(_) => {
                        return Err(Error::Transfer(
                            "log_based: unchanged-TOAST cell survived the refetch — bug".into(),
                        ))
                    }
                };
                pgc::tuple_start(row.len(), &mut chunk);
                for (cell, d) in row.iter().zip(bound.delivered.iter()) {
                    match cell {
                        Cell::Null => pgc::null_field(&mut chunk),
                        Cell::Text(t) => encode_cell(t, d, &mut chunk)?,
                        Cell::UnchangedToast => {
                            return Err(Error::Transfer(
                                "log_based: unchanged-TOAST cell reached the encode \
                                 path — bug"
                                    .into(),
                            ))
                        }
                    }
                }
                if chunk.len() >= (1 << 20) {
                    sent += enc.push(&chunk)?;
                    chunk.clear();
                }
            }
            pgc::trailer(&mut chunk);
            sent += enc.push(&chunk)?;
            enc.finish_file()?;
            if sent != n_rows {
                return Err(Error::Transfer(format!(
                    "log_based: parquet encoder consumed {sent} of {n_rows} rows — bug"
                )));
            }
            let bytes = std::mem::take(&mut *enc.out.0.lock().expect("parquet buf"));
            Some((bytes, n_rows))
        } else {
            None
        };

        if data.is_none() && del_ints.is_empty() && del_texts.is_empty() && !c.truncate {
            // Nothing materialized (aborted transactions only): advance state.
            cdc_set_watermark(&self.conn, table, source_id, outcome.end_lsn).await?;
            return Ok(c.events);
        }
        bound
            .cdc_commit(
                source_id,
                pk_i,
                CdcWindow {
                    data,
                    delete_ints: del_ints,
                    delete_texts: del_texts,
                    truncate: c.truncate,
                    end_lsn: outcome.end_lsn,
                },
            )
            .await?;
        Ok(c.events)
    }
}

// ── residue resolution ──────────────────────────────────────────────────────

/// One key's final state after replaying the residue tail over the set-phase
/// upserts (insertion order preserved — collapse.rs's map+vec move).
#[derive(Debug)]
enum Fin<'a> {
    /// Complete row image, borrowed from the collapsed window.
    Row(&'a [Cell]),
    /// Complete row image, owned (TOAST holes patched).
    Owned(Vec<Cell>),
    /// Still holds UnchangedToast cells — needs the source refetch.
    Refetch(Vec<Cell>),
    /// Deleted (or vanished at the source): delete-set only.
    Gone,
}

fn resolve_window<'a>(c: &'a Collapsed, pk_idx: &[usize]) -> Vec<(Key, Fin<'a>)> {
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
        row_key_refs(row, pk_idx).into_iter().map(<[u8]>::to_vec).collect()
    };
    let mut order: Vec<(Key, Fin<'a>)> = Vec::with_capacity(c.upserts.len());
    let mut index: HashMap<Key, usize> = HashMap::with_capacity(c.upserts.len());
    for row in &c.upserts {
        put(&mut order, &mut index, key_of(row), Fin::Row(row));
    }
    for op in &c.residue {
        match op {
            ResidueOp::Upsert { row } => {
                put(&mut order, &mut index, key_of(row), Fin::Row(row))
            }
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
        }
    }
    order
}

/// Fill each remaining TOAST hole from the source's CURRENT row (see module
/// docs for why the destination can't be read back). A key whose source row
/// is already gone stays delete-set-only; its WAL delete arrives in a later
/// window.
async fn refetch_masked(
    finals: &mut [(Key, Fin<'_>)],
    qualified_src: &str,
    pk: &str,
    wal_cols: &[String],
    oids: &[u32],
    src: &PgPool,
) -> Result<()> {
    if !finals.iter().any(|(_, f)| matches!(f, Fin::Refetch(_))) {
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
    let mut tx = src.begin().await.map_err(db_err)?;
    sqlx::query("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    for (key, fin) in finals.iter_mut() {
        let Fin::Refetch(cells) = fin else { continue };
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
                            Some(s) => Cell::Text(s.into_bytes()),
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

// ── cell encoding ───────────────────────────────────────────────────────────

/// One WAL text cell as a typed PgCopyBinary field — the github_api encoder's
/// vocabulary, driven by the delivered type instead of the extractor.
fn encode_cell(t: &[u8], d: &Delivered, out: &mut Vec<u8>) -> Result<()> {
    match d {
        Delivered::Int { .. } => {
            let v: i64 = text(t)?.parse().map_err(|_| bad_cell(t, "integer"))?;
            pgc::field(&v.to_be_bytes(), out);
        }
        Delivered::Float32 => {
            let v: f32 = text(t)?.parse().map_err(|_| bad_cell(t, "float4"))?;
            pgc::field(&v.to_be_bytes(), out);
        }
        Delivered::Float64 => {
            let v: f64 = text(t)?.parse().map_err(|_| bad_cell(t, "float8"))?;
            pgc::field(&v.to_be_bytes(), out);
        }
        // WAL renders t/f; a source refetch's ::text renders true/false.
        Delivered::Bool => match t {
            b"t" | b"true" => pgc::field(&[1], out),
            b"f" | b"false" => pgc::field(&[0], out),
            _ => return Err(bad_cell(t, "bool")),
        },
        Delivered::Decimal { .. } => pgc::numeric_field_from_str(text(t)?, out)?,
        Delivered::Date => {
            let d = chrono::NaiveDate::parse_from_str(text(t)?, "%Y-%m-%d")
                .map_err(|_| bad_cell(t, "date"))?;
            let unix = d
                .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
                .num_days() as i32;
            pgc::field(&(unix - pgc::PG_EPOCH_DAYS).to_be_bytes(), out);
        }
        Delivered::DateTime { utc } => {
            let s = if *utc { strip_utc_offset(t)? } else { t };
            let dt = chrono::NaiveDateTime::parse_from_str(text(s)?, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|_| bad_cell(t, "timestamp"))?;
            let us = dt.and_utc().timestamp_micros() - pgc::PG_EPOCH_MICROS;
            pgc::field(&us.to_be_bytes(), out);
        }
        Delivered::Uuid => {
            let u = uuid::Uuid::parse_str(text(t)?).map_err(|_| bad_cell(t, "uuid"))?;
            pgc::field(u.as_bytes(), out);
        }
        Delivered::Json => pgc::jsonb_field(t, out),
        Delivered::Text => pgc::field(t, out),
        Delivered::Bytes => pgc::field(&decode_bytea(t)?, out),
    }
    Ok(())
}

fn text(t: &[u8]) -> Result<&str> {
    std::str::from_utf8(t)
        .map_err(|_| Error::Transfer("log_based: non-UTF8 text cell".into()))
}

fn bad_cell(t: &[u8], what: &str) -> Error {
    Error::Transfer(format!(
        "log_based: {what} value '{}' didn't parse for the iceberg lane",
        String::from_utf8_lossy(t)
    ))
}

fn parse_int_key(k: &[u8]) -> Result<i64> {
    std::str::from_utf8(k)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| {
            Error::Transfer(format!(
                "log_based: integer key '{}' didn't parse",
                String::from_utf8_lossy(k)
            ))
        })
}

fn db_err(e: sqlx::Error) -> Error {
    Error::Transfer(format!("log_based: source refetch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn t(s: &str) -> Cell {
        Cell::Text(s.as_bytes().to_vec())
    }
    fn key1(s: &str) -> Key {
        vec![s.as_bytes().to_vec()]
    }

    #[test]
    fn residue_replay_lands_final_rows_in_order() {
        let c = Collapsed {
            deletes: vec![key1("9")],
            upserts: vec![vec![t("1"), t("a")], vec![t("2"), t("b")]],
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
        // Key 1: masked hole patched from the in-window base — complete row.
        assert_eq!(out[0].0, key1("1"));
        assert!(matches!(&out[0].1, Fin::Owned(r) if r[1] == t("a")));
        // Key 2: upserted then residue-deleted — delete-set only.
        assert!(matches!(out[1].1, Fin::Gone));
        // Key 3: late residue upsert appends in order.
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

        // Chained masked updates keep the hole; a delete after them wins.
        let c2 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate {
                    key: key1("7"),
                    row: vec![t("7"), Cell::UnchangedToast],
                },
                ResidueOp::MaskedUpdate {
                    key: key1("7"),
                    row: vec![t("7"), Cell::UnchangedToast],
                },
                ResidueOp::Delete { key: key1("7") },
            ],
            ..Default::default()
        };
        let out2 = resolve_window(&c2, &[0]);
        assert_eq!(out2.len(), 1);
        assert!(matches!(out2[0].1, Fin::Gone));

        // A full upsert after a masked update materializes the row.
        let c3 = Collapsed {
            residue: vec![
                ResidueOp::MaskedUpdate {
                    key: key1("7"),
                    row: vec![t("7"), Cell::UnchangedToast],
                },
                ResidueOp::Upsert { row: vec![t("7"), t("z")] },
            ],
            ..Default::default()
        };
        assert!(matches!(&resolve_window(&c3, &[0])[0].1, Fin::Row(r) if r[1] == t("z")));
    }

    #[test]
    fn wal_cells_roundtrip_through_parquet() {
        let names: Vec<String> =
            ["id", "name", "ok", "ts", "day", "uid", "amt"].map(String::from).to_vec();
        let delivered = vec![
            Delivered::Int { bytes: 8, unsigned: false },
            Delivered::Text,
            Delivered::Bool,
            Delivered::DateTime { utc: true },
            Delivered::Date,
            Delivered::Uuid,
            Delivered::Decimal { p: 18, s: 4 },
        ];
        let mut enc = ParquetEncoder::new_ext(
            names,
            delivered.clone(),
            None,
            Some((1..=7).collect()),
            None,
        )
        .unwrap();
        let mut buf = Vec::new();
        pgc::header(&mut buf);
        let row = [
            t("42"),
            t("héllo"),
            t("t"),
            t("2000-01-01 00:00:01.5+00"),
            t("2000-01-02"),
            t("0f14d0ab-9605-4a62-a9e4-5ed26688389b"),
            t("1234.5678"),
        ];
        pgc::tuple_start(row.len(), &mut buf);
        for (cell, d) in row.iter().zip(delivered.iter()) {
            let Cell::Text(v) = cell else { panic!() };
            encode_cell(v, d, &mut buf).unwrap();
        }
        pgc::trailer(&mut buf);
        assert_eq!(enc.push(&buf).unwrap(), 1);
        enc.finish_file().unwrap();

        let bytes = enc.out.0.lock().unwrap().clone();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let r = reader.get_row_iter(None).unwrap().next().unwrap().unwrap().to_string();
        assert!(r.contains("id: 42"), "{r}");
        assert!(r.contains("héllo"), "{r}");
        assert!(r.contains("ok: true"), "{r}");
        assert!(r.contains("2000-01-01"), "{r}");
        assert!(r.contains("2000-01-02"), "{r}");
        assert!(r.contains("0f14d0ab-9605-4a62-a9e4-5ed26688389b"), "{r}");
        assert!(r.contains("1234.5678"), "{r}");
    }

    #[test]
    fn refetch_dialect_and_garbage_are_handled() {
        // A refetched bool comes back as true/false, the WAL form as t/f.
        let mut out = Vec::new();
        encode_cell(b"true", &Delivered::Bool, &mut out).unwrap();
        encode_cell(b"f", &Delivered::Bool, &mut out).unwrap();
        assert_eq!(out, [&1i32.to_be_bytes()[..], &[1], &1i32.to_be_bytes(), &[0]].concat());
        assert!(encode_cell(b"yes", &Delivered::Bool, &mut Vec::new()).is_err());
        assert!(encode_cell(b"abc", &Delivered::Int { bytes: 8, unsigned: false }, &mut Vec::new())
            .is_err());
        // bytea rides \x-hex from both the WAL and the forced refetch form.
        let mut b = Vec::new();
        encode_cell(b"\\x4869", &Delivered::Bytes, &mut b).unwrap();
        assert_eq!(b, [&2i32.to_be_bytes()[..], b"Hi"].concat());
        assert_eq!(parse_int_key(b"-7").unwrap(), -7);
        assert!(parse_int_key(b"7; DROP").is_err());
    }
}
