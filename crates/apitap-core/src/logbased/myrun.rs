//! `mode="log_based"` with a **MySQL** source: bootstrap from a consistent
//! read, then drain the binlog in windows — the same shape the Postgres
//! orchestration has, reusing its collapse, appliers and state contract.
//!
//! Bootstrap ordering (the correctness argument): the binlog coordinate is
//! captured BEFORE the full load, so the replayed window overlaps the
//! snapshot instead of skipping past it. Every apply path is idempotent by
//! primary key (last-write-wins upserts, key-set deletes), so re-applying
//! events the load already contains converges to the same rows — the same
//! at-least-once property the Postgres path relies on after a restart.
//!
//! v1 destinations: postgres, clickhouse, mysql. Iceberg needs a source
//! pool the MySQL path does not carry and is refused loudly.

use crate::error::{Error, Result};
use crate::logbased::drain::DrainOutcome;
use crate::logbased::mysource::{
    self, drain_binlog, fetch_schema, master_position, pack_pos, MySession,
};
use crate::wire::mywire::MyWire;
use crate::{Mode, TransferOptions};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::collections::HashMap;

/// One member of a MySQL CDC group.
pub(crate) struct MyCtx {
    pub table_arg: String,
    /// "db.table" — matches what TABLE_MAP announces.
    pub qualified: String,
    pub dest_table: String,
    pub pk_cols: Vec<String>,
    pub source_id: String,
}

/// A replica id derived from the group identity: stable across runs (so a
/// restart reclaims its own stream) and unlikely to collide with a real
/// replica or another apitap group.
fn replica_id(seed: &str) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for b in seed.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    // Keep it out of the low range operators hand out by convention.
    0x4000_0000 | (h & 0x3FFF_FFFF)
}

/// Resolve db/table + PK for each member off information_schema.
pub(crate) async fn resolve(
    pool: &MySqlPool,
    default_db: &str,
    tables: &[String],
    single: bool,
    opts: &TransferOptions,
) -> Result<Vec<MyCtx>> {
    let mut out = Vec::with_capacity(tables.len());
    for t in tables {
        let (db, bare) = match t.split_once('.') {
            Some((d, b)) => (d.to_string(), b.to_string()),
            None => (default_db.to_string(), t.clone()),
        };
        let qualified = format!("{db}.{bare}");
        let sc = fetch_schema(pool, &db, &bare).await?;
        let pk_cols: Vec<String> = sc
            .names
            .iter()
            .zip(&sc.key)
            .filter(|(_, k)| **k)
            .map(|(n, _)| n.clone())
            .collect();
        if pk_cols.is_empty() {
            return Err(Error::InvalidInput(format!(
                "log_based: {qualified} has no primary key — updates/deletes \
                 need an identity. Add a PK first."
            )));
        }
        let dest_table = if single {
            opts.dest_table.clone().unwrap_or_else(|| bare.clone())
        } else {
            bare.clone()
        };
        out.push(MyCtx {
            table_arg: t.clone(),
            qualified,
            dest_table,
            pk_cols,
            source_id: format!("mysql:{db}.{bare}"),
        });
    }
    Ok(out)
}

/// Open the control pool a MySQL CDC run needs (coordinates, schemas,
/// prechecks) — separate from the terminal binlog connection.
pub(crate) async fn control_pool(src_url: &str) -> Result<MySqlPool> {
    // Same reason as the read path: the binlog connection is the raw plane,
    // and it must not be the last thing to discover the URL asks for a mode
    // it cannot speak.
    crate::wire::mywire::check_ssl_mode(src_url)?;
    MySqlPoolOptions::new()
        .max_connections(2)
        .connect(src_url)
        .await
        .map_err(|e| Error::Transfer(format!("log_based: mysql source connect: {e}")))
}

/// First run: capture the coordinate, then full-load every member.
/// Returns `(watermark, per-table (rows, parallel))` — the caller writes
/// the state rows, so no destination handle needs to cross into here.
pub(crate) async fn bootstrap<F, Fut>(
    pool: &MySqlPool,
    ctxs: &[MyCtx],
    opts: &TransferOptions,
    mut full_load: F,
) -> Result<(u64, Vec<(u64, usize)>)>
where
    F: FnMut(String, TransferOptions) -> Fut,
    Fut: std::future::Future<Output = Result<(u64, usize)>>,
{
    mysource::precheck(pool).await?;
    // BEFORE the load — see the module docs on why overlap beats a gap.
    let (file, pos) = master_position(pool).await?;
    let mark = pack_pos(&file, pos);

    let mut out = Vec::with_capacity(ctxs.len());
    for c in ctxs {
        let mut o2 = opts.clone();
        o2.mode = Mode::Replace;
        o2.dest_table = Some(c.dest_table.clone());
        // `slots` belongs to the CDC group, never to the bulk full-load leg
        // (which rejects it loudly if it leaks through).
        o2.slots = None;
        // The changelog's clauses belong to the rebuild, not to the bulk load's
        // table — see `run::strip_changelog_ddl`.
        if o2.changelog {
            o2.order_by = None;
            o2.partition_by = None;
        }
        let r = full_load(c.table_arg.clone(), o2).await.map_err(|e| {
            Error::Transfer(format!(
                "log_based: bootstrap of {} failed (no state written; re-run \
                 re-bootstraps the group): {e}",
                c.qualified
            ))
        })?;
        out.push(r);
    }
    Ok((mark, out))
}

/// Later runs: drain windows from `start` to the live stop-line, handing
/// each window to `apply_window` (which lands it and returns once every
/// destination committed) before the next window is drained.
pub(crate) async fn drain_windows<F, Fut>(
    src_url: &str,
    pool: &MySqlPool,
    ctxs: &[MyCtx],
    start: u64,
    group_seed: &str,
    max_secs: u64,
    max_buf_bytes: usize,
    changelog: bool,
    mut apply_window: F,
) -> Result<u64>
where
    F: FnMut(DrainOutcome) -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    mysource::precheck(pool).await?;
    let (live_file, live_pos) = master_position(pool).await?;
    let stop_line = pack_pos(&live_file, live_pos);
    // A watermark AHEAD of the server's own position is not "up to date" — it
    // is impossible on a log that only grows. Either the binlog was reset or
    // rebuilt (RESET MASTER, a restored dump, a rebuilt replica), or this URL
    // now points at a DIFFERENT server whose numbering is its own. Both mean
    // the changes between the two positions are unreachable, and both used to
    // exit through the "nothing to do" path below: a silent hole, reported as
    // success with zero changes. Found by a release smoke doing exactly that.
    if start > stop_line {
        let (want_idx, want_pos) = mysource::unpack(start);
        return Err(Error::InvalidInput(format!(
            "log_based: the stored position ({}, {want_pos}) is AHEAD of the \
             server's current one ({live_file}, {live_pos}). A binlog only \
             grows, so this server's log was reset or rebuilt (RESET MASTER, a \
             restored backup, a re-created replica), or this URL now points at \
             a different server. Either way the changes in between are not on \
             this server and cannot be drained — resuming would report success \
             while skipping them. Recovery is a fresh bootstrap: clear this \
             table's apitap state on the destination and re-run.",
            mysource::file_name(&prefix_of(&live_file), want_idx)
        )));
    }
    if start >= stop_line {
        return Ok(start);
    }

    let (idx, pos) = mysource::unpack(start);
    let prefix = live_file
        .rsplit_once('.')
        .map(|(p, _)| p.to_string())
        .unwrap_or_else(|| "binlog".into());
    let start_file = mysource::file_name(&prefix, idx.max(1));
    // Position 4 is the first event after a file's magic header.
    let start_pos = pos.max(4);

    // Resuming into a purged binlog is a HOLE, not a hiccup: check before
    // asking the server, so the user gets the reason and the remedy instead of
    // MySQL's error 1236.
    if start > 0 && !mysource::binlog_file_present(pool, &start_file).await? {
        return Err(Error::InvalidInput(format!(
            "log_based: the binlog this run must resume from ({start_file},              position {start_pos}) is no longer on the server — it was purged              by the server's own retention (binlog_expire_logs_seconds /              expire_logs_days / PURGE BINARY LOGS) while this table was not              being drained. Every change between that position and now is gone              from the log, so resuming would skip them silently. Recovery is a              fresh bootstrap: clear this table's apitap state on the              destination and re-run. To prevent it, keep binlog retention              longer than the longest gap between runs."
        )));
    }

    let mut w = MyWire::connect(src_url).await?;
    w.binlog_dump(replica_id(group_seed), &start_file, start_pos, 5)
        .await?;

    let mut sess = MySession {
        file: start_file,
        tracked: ctxs
            .iter()
            .map(|c| (c.qualified.clone(), c.pk_cols.clone()))
            .collect::<HashMap<_, _>>(),
        ..Default::default()
    };

    let mut watermark = start;
    loop {
        let o = drain_binlog(
            &mut w,
            pool,
            &mut sess,
            watermark,
            stop_line,
            max_secs,
            max_buf_bytes,
            changelog,
        )
        .await?;
        let hit_budget = o.hit_budget;
        let end = o.end_lsn;
        if end == watermark && o.tables.is_empty() && o.changes.is_empty() {
            break;
        }
        watermark = apply_window(o).await?;
        if !hit_budget && watermark >= stop_line {
            break;
        }
        if !hit_budget && end < stop_line {
            // Deadline stop with nothing new to fetch — leave the rest for
            // the next scheduled run rather than spinning.
            break;
        }
    }
    Ok(watermark)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_ids_are_stable_distinct_and_high() {
        let a = replica_id("apitap_g_abc");
        let b = replica_id("apitap_g_abd");
        assert_eq!(a, replica_id("apitap_g_abc"), "stable across runs");
        assert_ne!(a, b, "different groups get different ids");
        for id in [a, b] {
            assert!(id >= 0x4000_0000, "stays out of the hand-assigned range");
            assert_ne!(id, 0, "0 gets disconnected at end-of-log");
        }
    }
}

/// The `binlog`/`mariadb-bin` part of a binlog file name.
fn prefix_of(file: &str) -> String {
    file.rsplit_once('.')
        .map(|(p, _)| p.to_string())
        .unwrap_or_else(|| "binlog".into())
}
