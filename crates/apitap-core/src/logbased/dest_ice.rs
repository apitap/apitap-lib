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
//!
//! Every catalog call is in `mod store`. The apply body renders its window
//! and STAGES it in the `IceUnit` it is handed; the unit's close is the one
//! commit — the window with its watermark, or the watermark alone when
//! nothing was staged. Iceberg holds no lease (its claims live in object
//! storage under the table's location, which only the bulk sink resolves), so
//! the store is the one `Unguarded` one: it opens units without a fence.

use crate::error::{Error, Result};
use crate::lease::Watermark;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::resolve::{resolve_window, Image, Source};
use crate::logbased::rowtext::{decode_bytea, pk_indices, strip_utc_offset};
use crate::plan::Delivered;
use crate::sink::iceberg::CdcWindow;
use crate::wire::bqparquet::ParquetEncoder;
use crate::wire::pgcopy as pgc;
use crate::wire::pgoutput::Cell;

pub(crate) use store::{IceStore, IceUnit};

pub(crate) struct IceDest {
    store: IceStore,
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
        Ok(Self { store: IceStore::connect(url).await? })
    }

    /// The store: the one `Unguarded` one — see the module doc.
    pub(crate) fn store(&self) -> &IceStore {
        &self.store
    }

    /// The drain's watermark, through the one verdict both lanes share (the
    /// table properties read as a state row: `sink::iceberg::cdc_read_state`).
    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<crate::naming::CdcWatermark>> {
        crate::naming::cdc_watermark(bare(dest_table), self.store.read_state(dest_table, source_id).await?)
    }

    /// The bootstrap's replace just created the table (and cleared every
    /// apitap watermark property): nothing to add, and no snapshot — the data
    /// is already committed. The unit's close stamps the slot's LSN. Refused
    /// here if the key cannot be an equality delete's.
    pub(crate) fn bootstrap_finish(&self, pk_cols: &[String]) -> Result<()> {
        single_pk(pk_cols).map(|_| ())
    }

    /// Render one collapsed window for one table and stage it in the unit,
    /// whose close commits it WITH its watermark as ONE catalog commit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply(
        &self,
        u: &mut IceUnit<'_>,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
        src: &Source<'_>,
    ) -> Result<(u64, Watermark)> {
        apply_unit(u, dest_table, qualified_src, pk_cols, outcome, source_id, src).await
    }
}

/// Render one collapsed window for one table — final row images as a data
/// file, every touched key as a delete — stage it in the unit, and name the
/// watermark its close commits it with. A window with nothing to write stages
/// nothing, and its close commits the watermark alone.
async fn apply_unit(
    u: &mut IceUnit<'_>,
    dest_table: &str,
    qualified_src: &str,
    pk_cols: &[String],
    outcome: &DrainOutcome,
    source_id: &str,
    src: &Source<'_>,
) -> Result<(u64, Watermark)> {
    let set = |rows: u64| Watermark::Set {
        table: dest_table.to_string(),
        source_id: source_id.to_string(),
        lsn: outcome.end_lsn,
        rows,
    };
    let Some(c) = outcome.tables.get(qualified_src) else {
        // Foreign-table traffic only: nothing for our table, still advance.
        return Ok((0, set(0)));
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

    let bound = u.bind(dest_table, wal_cols, oids).await?;

    // Replay the residue tail over the set-phase upserts into one entry per
    // key; unresolved TOAST holes go back to the source.
    let mut r = resolve_window(c, &pk_idx);
    src.refetch_masked(&mut r, qualified_src, pk, wal_cols, oids).await?;

    // Delete-set: every touched key (deleted or re-landed), each once. A
    // TRUNCATE window starts from an empty manifest list — nothing old to
    // delete.
    let (mut del_ints, mut del_texts) = (Vec::new(), Vec::new());
    if !c.truncate {
        for (key, _) in r.rows() {
            let k = key[0].as_slice();
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

    let n_rows = r.rows().filter(|(_, image)| !matches!(image, Image::Delete)).count() as u64;
    let data = if n_rows > 0 {
        let mut enc = ParquetEncoder::new_ext(
            wal_cols.clone(),
            bound.delivered().to_vec(),
            None,
            Some(bound.field_ids().to_vec()),
            None,
        )?;
        let mut chunk = Vec::with_capacity(256 << 10);
        pgc::header(&mut chunk);
        let mut sent = 0u64;
        for (_, image) in r.rows() {
            let row: &[Cell] = match image {
                Image::Row(row) => row,
                Image::Delete => continue,
                Image::Masked { .. } => {
                    return Err(Error::Transfer(
                        "log_based: unchanged-TOAST cell survived the refetch — bug".into(),
                    ))
                }
            };
            pgc::tuple_start(row.len(), &mut chunk);
            for (cell, d) in row.iter().zip(bound.delivered().iter()) {
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
        // Nothing materialized (aborted transactions only): the close
        // advances the watermark alone.
        return Ok((c.events, set(c.events)));
    }
    u.stage(
        dest_table,
        bound,
        pk_i,
        CdcWindow {
            data,
            delete_ints: del_ints,
            delete_texts: del_texts,
            truncate: c.truncate,
            end_lsn: outcome.end_lsn,
        },
    )?;
    Ok((c.events, set(c.events)))
}

// ── residue resolution ──────────────────────────────────────────────────────
// `resolve_window` + `Image` live in `crate::logbased::resolve` (shared with the
// BigQuery apply path), and so does `Source`, the read-only handle Iceberg
// fills its leftover TOAST holes through.

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

/// Everything that reaches the catalog. See the module doc.
mod store {
    use super::bare;
    use crate::error::{Error, Result};
    use crate::guard::GuardStore;
    use crate::lease::{Fence, LeaseStore, Watermark};
    use crate::plan::Delivered;
    use crate::sink::iceberg::{
        cdc_bind, cdc_clear_watermark, cdc_read_state, cdc_set_watermark, CdcBound, CdcWindow,
        IcebergConn,
    };
    use std::collections::HashMap;

    /// The one `Unguarded` store. Its lease side is bookkeeping with no I/O:
    /// nothing is written, nothing can be claimed, and every key a run opened
    /// stays that run's until it ends.
    pub(crate) struct IceStore {
        conn: IcebergConn,
        /// The keys each run opened. The tenure's keeper asks which of its keys
        /// are still unclaimed and stops the run over any it does not hear back,
        /// so an unguarded store answers with every one — never with nothing.
        opened: std::sync::Mutex<HashMap<String, Vec<String>>>,
    }

    /// A table conformed against a window's column layout: what the apply body
    /// encodes with. Only the unit can commit to it.
    pub(crate) struct Bound(CdcBound);

    impl Bound {
        pub(crate) fn delivered(&self) -> &[Delivered] {
            &self.0.delivered
        }

        pub(crate) fn field_ids(&self) -> &[i32] {
            &self.0.field_ids
        }
    }

    struct Staged {
        table: String,
        bound: CdcBound,
        pk_idx: usize,
        window: CdcWindow,
    }

    /// One unit: at most one rendered window, waiting for its watermark.
    /// Nothing reaches the table until the close.
    pub(crate) struct IceUnit<'a> {
        s: &'a IceStore,
        staged: Option<Staged>,
    }

    impl IceUnit<'_> {
        /// Load the table and conform it to the window's columns (a read).
        pub(crate) async fn bind(&self, dest_table: &str, wal_cols: &[String], oids: &[u32]) -> Result<Bound> {
            cdc_bind(&self.s.conn, bare(dest_table), wal_cols, oids).await.map(Bound)
        }

        /// Hold a rendered window for the close, which commits it and its
        /// watermark as one snapshot.
        pub(crate) fn stage(&mut self, dest_table: &str, b: Bound, pk_idx: usize, window: CdcWindow) -> Result<()> {
            if let Some(s) = &self.staged {
                return Err(Error::Transfer(format!(
                    "internal: {dest_table}: a second window staged in one unit (it holds {})",
                    s.table
                )));
            }
            self.staged = Some(Staged { table: dest_table.to_string(), bound: b.0, pk_idx, window });
            Ok(())
        }
    }

    /// The mark a staged window commits with: the `Set` naming its table. A
    /// `Clear`, or another table's `Set`, never carries it.
    pub(super) fn carrier(staged: &str, marks: &[Watermark]) -> Option<usize> {
        marks.iter().position(|m| matches!(m, Watermark::Set { table, .. } if bare(table) == bare(staged)))
    }

    /// The guard of a destination that has none: announces nothing, sees
    /// nothing, holds no lease. Iceberg's claims live in object storage under
    /// the table's location, which only the bulk sink resolves; an Iceberg CDC
    /// bootstrap rides that sink, so the expensive half is guarded, and the
    /// incremental windows are not (stated in usage.md).
    pub(crate) struct Unguarded;

    #[async_trait::async_trait]
    impl GuardStore for Unguarded {
        fn limit(&self) -> usize {
            crate::naming::ROOMY
        }
        fn dest_label(&self, bare: &str) -> String {
            bare.to_string()
        }
        async fn list(&self, _bare: &str, _kinds: &[crate::naming::Artifact]) -> Result<Vec<crate::guard::Listed>> {
            Ok(Vec::new())
        }
        async fn create_marker(&self, _raw: &str) -> Result<()> {
            Ok(())
        }
        async fn drop_object(&self, _raw: &str) -> Result<()> {
            Ok(())
        }
        async fn lease_get(&self, _key: &str, _token: &str) -> Result<Option<crate::lease::Lease>> {
            Ok(None)
        }
        async fn lease_claim(&self, _key: &str, _token: &str) -> Result<crate::guard::Claim> {
            Ok(crate::guard::Claim::Absent)
        }
        async fn lease_close(&self, _proof: crate::guard::Released) {}
    }

    impl IceStore {
        pub(crate) async fn connect(url: &str) -> Result<Self> {
            Ok(Self { conn: IcebergConn::parse(url).await?, opened: Default::default() })
        }

        pub(crate) async fn read_state(
            &self,
            dest_table: &str,
            source_id: &str,
        ) -> Result<Option<crate::naming::StateRow>> {
            cdc_read_state(&self.conn, bare(dest_table), source_id).await
        }
    }

    impl LeaseStore for IceStore {
        fn lease_key(&self, dest_table: &str) -> String {
            dest_table.to_string()
        }

        async fn lease_open(&self, keys: &[String], token: &str) -> Result<()> {
            self.opened.lock().expect("opened").insert(token.to_string(), keys.to_vec());
            Ok(())
        }

        async fn lease_renew(&self, _keys: &[String], _token: &str) -> Result<u64> {
            Ok(0)
        }

        async fn lease_unclaimed(&self, token: &str) -> Result<Vec<String>> {
            Ok(self.opened.lock().expect("opened").get(token).cloned().unwrap_or_default())
        }

        async fn close_run(&self, token: &str) {
            self.opened.lock().expect("opened").remove(token);
        }
    }

    impl Fence for IceStore {
        type Unit<'a> = IceUnit<'a>;

        fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String) {
            (Box::new(Unguarded), dest_table.to_string())
        }

        /// No I/O and no fence: there is no lease row to hold.
        async fn open_unit<'a>(&'a self, _keys: &[String], _token: &str) -> Result<IceUnit<'a>> {
            Ok(IceUnit { s: self, staged: None })
        }

        /// Each mark in order. The `Set` for the staged table commits the
        /// window WITH its watermark, as one snapshot (the module doc's atom);
        /// any other `Set` stamps a watermark alone, and a `Clear` removes one.
        /// A staged window that no `Set` names is refused before anything is
        /// committed: dropping it would report a window applied that never was.
        async fn close_unit<'a>(&'a self, u: IceUnit<'a>, _token: &str, marks: Vec<Watermark>) -> Result<()> {
            let IceUnit { mut staged, .. } = u;
            let at = match &staged {
                Some(s) => Some(carrier(&s.table, &marks).ok_or_else(|| {
                    Error::Transfer(format!(
                        "internal: {}: a staged window closed without its watermark",
                        s.table
                    ))
                })?),
                None => None,
            };
            for (i, m) in marks.into_iter().enumerate() {
                match m {
                    Watermark::Set { source_id, lsn, .. } if Some(i) == at => {
                        let Staged { bound, pk_idx, window, .. } = staged.take().expect("the carried window");
                        // The watermark is the mark's, like every other
                        // destination's: the body only rendered the window.
                        bound.cdc_commit(&source_id, pk_idx, CdcWindow { end_lsn: lsn, ..window }).await?
                    }
                    Watermark::Set { table, source_id, lsn, .. } => {
                        cdc_set_watermark(&self.conn, bare(&table), &source_id, lsn).await?
                    }
                    Watermark::Clear { table, source_id } => {
                        cdc_clear_watermark(&self.conn, bare(&table), &source_id).await?
                    }
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn t(s: &str) -> Cell {
        Cell::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
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

    /// A staged window commits with the `Set` for ITS table (however the name
    /// was qualified), never with a `Clear` or another table's watermark.
    #[test]
    fn a_staged_window_rides_its_own_set() {
        let set = |t: &str| Watermark::Set { table: t.into(), source_id: "s".into(), lsn: 7, rows: 0 };
        let clear = |t: &str| Watermark::Clear { table: t.into(), source_id: "s".into() };
        assert_eq!(store::carrier("t", &[set("u"), set("t")]), Some(1));
        assert_eq!(store::carrier("ns.t", &[set("t")]), Some(0));
        assert_eq!(store::carrier("t", &[clear("t"), set("u")]), None);
    }

    /// A catalog stand-in that answers every request with `{}` — all
    /// `IcebergConn::parse` asks (its config fetch). Nothing else reaches it:
    /// the store's lease side and an empty unit do no I/O.
    async fn catalog() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = s.read(&mut buf).await;
                    let _ = s
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                              Content-Length: 2\r\nConnection: close\r\n\r\n{}",
                        )
                        .await;
                });
            }
        });
        format!("iceberg://127.0.0.1:{port}/ns?access_key_id=k&secret_access_key=s")
    }

    /// The tenure's keeper stops a run over any key its store does not report
    /// unclaimed. An unguarded store that answered like its other no-ops —
    /// with nothing — would evict every Iceberg drain at its first tick.
    #[test]
    fn an_unguarded_tenure_is_never_evicted() {
        use std::time::Duration;
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let store = std::sync::Arc::new(IceStore::connect(&catalog().await).await.unwrap());
            let tables = ["a".to_string(), "b".to_string()];
            let run = crate::naming::RunId::mint_drain("s");
            let t = crate::lease::Tenure::acquire_every(store, &tables, run, Duration::from_millis(10))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            let h = t.open(&["a"]).await.expect("a keeper tick evicted an unguarded run");
            t.close(h, vec![]).await.unwrap();
            t.release().await;
        });
    }
}
