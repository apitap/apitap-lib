//! log_based apply into ClickHouse.
//!
//! ClickHouse has no multi-statement transactions, so the WINDOW is the
//! atom instead: the apply order (truncate → clear keys → plain insert →
//! residue → state row LAST) makes replaying the same window idempotent —
//! the delete phase clears every key the insert phase lands, so a crash
//! between insert and state write converges on the re-run, exactly like the
//! slot re-drain does. Deletes are lightweight `DELETE FROM` joined against
//! a key table (synchronous on the issuing replica by default).

use crate::error::{Error, Result};
use crate::logbased::collapse::ResidueOp;
use crate::logbased::drain::DrainOutcome;
use crate::logbased::rowtext::{
    ch_key_literal, pk_indices, render_ch_key, render_ch_row, render_ch_value, row_key_refs,
    tsv_unescape,
};
use crate::sink::clickhouse::{ch_ident, ch_str, ChConn};
use crate::wire::pgoutput::Cell;

const STATE_CURSOR: &str = "_lsn";

pub(crate) struct ChDest {
    ch: ChConn,
    /// DDL this connection has already issued. `CREATE TABLE IF NOT EXISTS` is
    /// idempotent but not free: it is a full HTTP round trip against a window
    /// that only has ~7 of them, repeated for every window of every table. The
    /// first window creates; the rest remember. A dropped-out-from-under-us
    /// table would resurface as a loud error on the next statement, which is
    /// the same failure the unconditional CREATE would have hidden.
    ensured: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl ChDest {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            ch: ChConn::parse(url)?,
            ensured: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// True the FIRST time this connection is asked about `key`.
    fn first_time(&self, key: &str) -> bool {
        self.ensured.lock().unwrap().insert(key.to_string())
    }

    /// Default the created table's ORDER BY to the PK so the per-window
    /// key-join delete probes the sorting key instead of scanning.
    pub(crate) fn tweak_bootstrap_opts(
        &self,
        o2: &mut crate::TransferOptions,
        pk_cols: &[String],
    ) {
        if o2.order_by.is_none() {
            o2.order_by = Some(pk_cols.join(", "));
        }
    }

    async fn ensure_state_table(&self) -> Result<()> {
        if !self.first_time("\u{1}state") {
            return Ok(());
        }
        self.ch
            .exec(
                "CREATE TABLE IF NOT EXISTS `_apitap_state` (\
                   dest_table String, source_id String, cursor_col String, \
                   watermark String, mode String, last_rows UInt64, \
                   synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) \
                 ENGINE = ReplacingMergeTree(synced_at) ORDER BY (dest_table, source_id)",
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn read_state(
        &self,
        dest_table: &str,
        source_id: &str,
    ) -> Result<Option<u64>> {
        let sql = format!(
            "SELECT watermark, cursor_col, mode FROM `_apitap_state` FINAL \
             WHERE dest_table = '{}' AND source_id = '{}' \
             FORMAT TabSeparated",
            ch_str(dest_table),
            ch_str(source_id)
        );
        let body = match self.ch.exec(&sql).await {
            Ok(b) => b,
            // No state table at all = fresh destination.
            Err(Error::Transfer(m))
                if m.contains("UNKNOWN_TABLE") || m.contains("doesn't exist") =>
            {
                return Ok(None)
            }
            Err(e) => return Err(e),
        };
        let Some(line) = body.lines().next() else { return Ok(None) };
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() != 3 {
            return Err(Error::Transfer(format!(
                "log_based: malformed state row from ClickHouse: {line:?}"
            )));
        }
        if f[2] != "log_based" || f[1] != STATE_CURSOR {
            return Err(Error::InvalidInput(format!(
                "log_based: state row for this table tracks cursor '{}' in mode \
                 '{}', not an LSN — it was written by another mode. Use a \
                 different dest_table or delete the state row",
                f[1], f[2]
            )));
        }
        f[0].parse::<u64>()
            .map(Some)
            .map_err(|_| Error::Transfer(format!("log_based: bad LSN state '{}'", f[0])))
    }

    pub(crate) async fn write_state(
        &self,
        dest_table: &str,
        source_id: &str,
        lsn: u64,
        rows: u64,
    ) -> Result<()> {
        self.ensure_state_table().await?;
        self.ch
            .exec(&format!(
                "INSERT INTO `_apitap_state` \
                 (dest_table, source_id, cursor_col, watermark, mode, last_rows) \
                 VALUES ('{}', '{}', '{STATE_CURSOR}', '{lsn}', 'log_based', {rows})",
                ch_str(dest_table),
                ch_str(source_id),
            ))
            .await?;
        Ok(())
    }

    /// Apply one collapsed window. State is written LAST — a re-run of the
    /// same window is idempotent (see module docs).
    pub(crate) async fn apply(
        &self,
        dest_table: &str,
        qualified_src: &str,
        pk_cols: &[String],
        outcome: &DrainOutcome,
        source_id: &str,
    ) -> Result<u64> {
        let Some(c) = outcome.tables.get(qualified_src) else {
            // Foreign-table traffic only: nothing for our table, still advance.
            self.write_state(dest_table, source_id, outcome.end_lsn, 0).await?;
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
        let ft = ch_ident(dest_table);
        let pk_idx = pk_indices(pk_cols, wal_cols)?;
        let pk_oids: Vec<u32> = pk_idx.iter().map(|&i| oids[i]).collect();
        let pklist = pk_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");
        let collist = wal_cols.iter().map(|k| ch_ident(k)).collect::<Vec<_>>().join(", ");

        if c.truncate {
            self.ch.exec(&format!("TRUNCATE TABLE {ft}")).await?;
        }

        // Clear the delete-set ∪ every upsert key first, so the insert phase
        // is a plain bulk INSERT (same move as the pg apply).
        if !c.deletes.is_empty() || !c.upserts.is_empty() {
            let del = ch_ident(&format!("{dest_table}__apitap_cdc_del"));
            // The key table is created ONCE and truncated per window. The old
            // DROP → CREATE → … → DROP cycle spent three HTTP round trips of
            // pure ceremony on every window of every table, and at ~20k events
            // per window that ceremony is a real slice of the apply: the whole
            // window is only ~7 round trips. CREATE IF NOT EXISTS + TRUNCATE is
            // two, and TRUNCATE leaves the same empty table the CREATE did, so
            // a replayed window still sees exactly the state it expects.
            if self.first_time(&del) {
                self.ch
                    .exec(&format!(
                        "CREATE TABLE IF NOT EXISTS {del} ENGINE = MergeTree \
                         ORDER BY tuple() AS SELECT {pklist} FROM {ft} WHERE 0"
                    ))
                    .await?;
            }
            self.ch.exec(&format!("TRUNCATE TABLE {del}")).await?;
            let mut buf = Vec::with_capacity(1 << 20);
            for key in &c.deletes {
                let refs: Vec<&[u8]> = key.iter().map(|k| k.as_slice()).collect();
                render_ch_key(&refs, &pk_oids, &mut buf)?;
            }
            for row in &c.upserts {
                render_ch_key(&row_key_refs(row, &pk_idx), &pk_oids, &mut buf)?;
            }
            self.ch
                .insert_stream(
                    &format!("INSERT INTO {del} ({pklist}) FORMAT TabSeparated"),
                    reqwest::Body::from(buf),
                )
                .await?;
            let pred = if pk_cols.len() == 1 {
                format!("{pklist} IN (SELECT {pklist} FROM {del})")
            } else {
                format!("({pklist}) IN (SELECT {pklist} FROM {del})")
            };
            self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
        }

        if !c.upserts.is_empty() {
            let mut buf = Vec::with_capacity(4 << 20);
            for row in &c.upserts {
                render_ch_row(row, oids, &mut buf)?;
            }
            self.ch
                .insert_stream(
                    &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                    reqwest::Body::from(buf),
                )
                .await?;
        }

        // Residue tail: serial, ordered. Masked TOAST updates read the
        // missing columns back from the destination, then delete + reinsert
        // the patched row (ClickHouse has no cheap row UPDATE).
        for op in &c.residue {
            match op {
                ResidueOp::MaskedUpdate { key, row } => {
                    let mut full = row.clone();
                    let missing: Vec<usize> = full
                        .iter()
                        .enumerate()
                        .filter(|(_, cell)| matches!(cell, Cell::UnchangedToast))
                        .map(|(i, _)| i)
                        .collect();
                    let pred = key_pred(pk_cols, key, &pk_oids)?;
                    if !missing.is_empty() {
                        let sel = missing
                            .iter()
                            .map(|&i| ch_ident(&wal_cols[i]))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let body = self
                            .ch
                            .exec(&format!(
                                "SELECT {sel} FROM {ft} WHERE {pred} FORMAT TabSeparated"
                            ))
                            .await?;
                        let Some(line) = body.lines().next() else {
                            return Err(Error::Transfer(
                                "log_based: masked update for a row missing at the \
                                 destination — window replay out of order?"
                                    .into(),
                            ));
                        };
                        let fields: Vec<&str> = line.split('\t').collect();
                        if fields.len() != missing.len() {
                            return Err(Error::Transfer(
                                "log_based: masked-update readback column count \
                                 mismatch"
                                    .into(),
                            ));
                        }
                        for (&i, f) in missing.iter().zip(fields.iter()) {
                            // Readback is already destination-dialect: escape it
                            // straight back out, no OID translation.
                            full[i] = match tsv_unescape(f) {
                                None => Cell::Null,
                                Some(v) => Cell::Text(v),
                            };
                            // Mark: this cell must NOT be re-translated.
                        }
                    }
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                    let mut buf = Vec::new();
                    render_residue_row(&full, oids, &missing, &mut buf)?;
                    self.ch
                        .insert_stream(
                            &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                            reqwest::Body::from(buf),
                        )
                        .await?;
                }
                ResidueOp::Upsert { row } => {
                    let key: Vec<Vec<u8>> = row_key_refs(row, &pk_idx)
                        .into_iter()
                        .map(|k| k.to_vec())
                        .collect();
                    let pred = key_pred(pk_cols, &key, &pk_oids)?;
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                    let mut buf = Vec::new();
                    render_ch_row(row, oids, &mut buf)?;
                    self.ch
                        .insert_stream(
                            &format!("INSERT INTO {ft} ({collist}) FORMAT TabSeparated"),
                            reqwest::Body::from(buf),
                        )
                        .await?;
                }
                ResidueOp::Delete { key } => {
                    let pred = key_pred(pk_cols, key, &pk_oids)?;
                    self.ch.exec(&format!("DELETE FROM {ft} WHERE {pred}")).await?;
                }
            }
        }

        self.write_state(dest_table, source_id, outcome.end_lsn, c.events).await?;
        Ok(c.events)
    }
}

/// `col = lit AND …` for one replica-identity key, typed by OID.
fn key_pred(pk_cols: &[String], key: &[Vec<u8>], pk_oids: &[u32]) -> Result<String> {
    Ok(pk_cols
        .iter()
        .zip(key.iter())
        .zip(pk_oids.iter())
        .map(|((c, v), &oid)| Ok(format!("{} = {}", ch_ident(c), ch_key_literal(v, oid)?)))
        .collect::<Result<Vec<_>>>()?
        .join(" AND "))
}

/// Render a residue row where the cells at `verbatim` indices came back from
/// the destination readback (already destination-dialect — escape only, no
/// OID translation); everything else is WAL text and translates as usual.
fn render_residue_row(
    row: &[Cell],
    oids: &[u32],
    verbatim: &[usize],
    out: &mut Vec<u8>,
) -> Result<()> {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            Cell::Null => out.extend_from_slice(b"\\N"),
            Cell::Text(t) if verbatim.contains(&i) => {
                crate::logbased::rowtext::copy_escape(t, out)
            }
            Cell::Text(t) => render_ch_value(t, oids[i], out)?,
            Cell::UnchangedToast => {
                return Err(Error::Transfer(
                    "log_based: unchanged-TOAST cell survived the readback — bug".into(),
                ))
            }
        }
    }
    out.push(b'\n');
    Ok(())
}
