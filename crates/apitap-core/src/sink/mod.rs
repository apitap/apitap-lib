//! The SINK side of a transfer: stage rows through per-worker [`Loader`]
//! streams, then land them atomically (swap / append / merge).
//!
//! Adding a sink = one file here implementing [`Sink`], plus its URL scheme in
//! [`crate::pipeline::dispatch`]. Database-specific SQL vocabulary shared with
//! the same database's source lives in [`crate::dialect`].

use crate::error::{Error, Result};
use md5::Digest;
use crate::plan::{DestState, Lane, TablePlan, WireFormat};
use crate::Mode;
use std::future::Future;

pub(crate) mod bigquery;
pub(crate) mod clickhouse;
pub(crate) mod gcs;
pub(crate) mod iceberg;
pub(crate) mod mysql;
pub(crate) mod s3;
pub(crate) mod postgres;

/// Per-worker stream consumer on the sink side. One loader = one physical ingest
/// stream (one `COPY … FROM STDIN`, one ClickHouse `INSERT` body).
pub(crate) trait Loader: Send + 'static {
    /// Ship one coalesced buffer. The worker owns coalescing (~chunk bytes per send —
    /// tiny sends are syscall/protocol overhead, huge ones just buffer memory).
    ///
    /// Framing contract: for row-oriented formats (RowBinary, TabSeparated) buffers
    /// end on a RECORD boundary — a loader that splits its input into files/batches
    /// (object-store staging, multi-row INSERT) may rely on that. Byte-relay formats
    /// (PgCopyBinary) give NO alignment guarantee; such loaders must treat the stream
    /// as opaque bytes.
    fn send(&mut self, buf: Vec<u8>) -> impl Future<Output = Result<()>> + Send;
    /// Hand back an emptied buffer from an already-shipped send, if this sink has one
    /// ready. Workers use it to recycle chunk buffers instead of allocating fresh —
    /// the fresh 4 MiB chunk Vec was 99.9% of steady-state allocator traffic
    /// (benchmarks/profiling.md). Default: none; sinks that consume the buffer
    /// zero-copy (ClickHouse HTTP body) simply never return one.
    fn reclaim(&mut self) -> Option<Vec<u8>> {
        None
    }
    /// Frame-aware fast lane: this loader can consume a RAW wire window
    /// (CopyData headers embedded) in place. Only the Arrow read loader
    /// says yes; everyone else keeps the copying planes.
    fn framed_capable(&self) -> bool {
        false
    }
    /// Consume one raw window ([`Loader::framed_capable`] loaders only).
    /// Returns bytes consumed and why the scan stopped.
    fn send_framed(
        &mut self,
        _win: &[u8],
    ) -> impl Future<Output = Result<(usize, crate::wire::arrowcol::FramedPush)>> + Send {
        async move {
            Err(Error::Transfer(
                "send_framed on a loader that is not framed_capable".into(),
            ))
        }
    }
    /// Close the stream cleanly. Returns rows ingested if this sink reports them
    /// (Postgres COPY does; ClickHouse counts via [`Sink::rows_staged`] instead).
    fn finish(self) -> impl Future<Output = Result<u64>> + Send;
    /// Source-side failure: make the sink DISCARD the partial stream (a clean close
    /// could commit it), then hand the cause back for propagation.
    fn abort(self, cause: Error) -> impl Future<Output = Error> + Send;
}

/// Wraps any loader to count what passes through it. Every byte of every
/// source→destination pair goes through a `Loader`, which makes this the one
/// place progress can measure the whole matrix without each sink or source
/// having to remember to report. Delegation is total: a lane's fast paths
/// (`reclaim`, `send_framed`) stay exactly as fast, minus one relaxed atomic
/// add per buffer — and nothing at all when progress is off.
pub(crate) struct Counted<L: Loader>(pub(crate) L);

impl<L: Loader> Loader for Counted<L> {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        crate::progress::add_bytes(buf.len() as u64);
        self.0.send(buf).await
    }

    fn reclaim(&mut self) -> Option<Vec<u8>> {
        self.0.reclaim()
    }

    fn framed_capable(&self) -> bool {
        self.0.framed_capable()
    }

    async fn send_framed(
        &mut self,
        win: &[u8],
    ) -> Result<(usize, crate::wire::arrowcol::FramedPush)> {
        let r = self.0.send_framed(win).await;
        if let Ok((consumed, _)) = &r {
            crate::progress::add_bytes(*consumed as u64);
        }
        r
    }

    async fn finish(self) -> Result<u64> {
        self.0.finish().await
    }

    async fn abort(self, cause: Error) -> Error {
        self.0.abort(cause).await
    }
}

pub(crate) trait Sink: Sized + Send + Sync {
    type Loader: Loader;
    /// Ingest formats this sink accepts, best first. Negotiation picks the first one
    /// the source can produce. Non-static so a sink may ORDER lanes per
    /// connection (BigQuery prefers Parquet when CPU is plentiful, CSV when
    /// starved — measured, not guessed).
    fn accepts(&self) -> &[WireFormat];
    /// Can this sink take `format` for THIS plan? Default yes; a sink whose
    /// fast lane can't represent some column (BigQuery's Parquet lane vs
    /// unconstrained NUMERIC, bytea, exotic udts) declines and negotiation
    /// falls through to its next lane instead of hard-failing.
    fn lane_ok(&self, _plan: &TablePlan, _format: WireFormat) -> bool {
        true
    }
    /// Sink-specific plan constraints, applied before lane planning so the DDL and the
    /// encoders agree (e.g. ClickHouse: the ORDER BY column must be non-nullable).
    /// Default: none — a first-cut sink doesn't have to think about this.
    fn adjust_plan(&self, _plan: &mut TablePlan) {}
    /// Create the staging table for this lane. `mode` is the effective mode: replace
    /// honors `durable`; incremental modes always stage UNLOGGED (staging never
    /// becomes the final table). Replace implementations should also capture whatever
    /// the swap would destroy (indexes, constraints, grants) for re-application.
    fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        durable: bool,
        mode: Mode,
    ) -> impl Future<Output = Result<()>> + Send;
    /// One ingest stream into staging (called once per worker).
    fn loader(&self) -> impl Future<Output = Result<Self::Loader>> + Send;
    /// Rows now in staging. `loaded` is the loaders' own count — sinks whose protocol
    /// reports rows return it as-is; others count server-side.
    fn rows_staged(&self, loaded: u64) -> impl Future<Output = Result<u64>> + Send;
    /// Incremental modes only: inspect the destination BEFORE staging.
    ///
    /// The watermark DECISION is shared — fetch your inputs (own state row,
    /// data max, sibling-row presence) however your database wants, then call
    /// [`crate::plan::resolve_watermark`] with your [`crate::plan::WmArbitration`].
    /// Its invariants (fan-in guard, empty-dest, no-state fallback) are the
    /// contract; hand-rolling them is how the MySQL sink drifted once.
    ///
    /// Returns whether
    /// the final table exists and its current `max(cursor)` as text. Implementations
    /// must also (a) verify the destination's columns match the plan (schema drift →
    /// a clear error, never a silent mis-append), (b) reject unsupported modes early
    /// (e.g. merge on ClickHouse), and (c) stash whatever finalize will need (merge
    /// keys). Never called for `Mode::Replace`.
    /// May also CONFORM the plan to the existing destination (e.g. ClickHouse
    /// mirrors the dest's column nullability so staging's structure matches for
    /// ATTACH — a view-sourced plan reports everything nullable, but the dest is
    /// the structural authority once it exists).
    fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> impl Future<Output = Result<DestState>> + Send {
        // Default for replace-only first-cut sinks: incremental modes refuse
        // loudly instead of forcing every new sink to implement state handling
        // before it can ship a full-refresh path.
        let _ = (plan, mode, cursor, source_id);
        async move {
            Err(Error::InvalidInput(
                "append/merge are not supported by this destination yet — use \
                 mode='replace'"
                    .into(),
            ))
        }
    }
    /// Land the staged rows: `Replace` = atomic swap; `Append` = move staged rows into
    /// the existing table; `Merge` = upsert them by primary key. When `rows == 0`,
    /// drop staging and leave the destination untouched (the 0-row guard) in every
    /// mode. `mode` here is the EFFECTIVE mode (a bootstrapped incremental run gets
    /// `Replace`).
    fn finalize(&self, rows: u64, mode: Mode) -> impl Future<Output = Result<()>> + Send;
}

/// `<bare>__apitap_staging`, kept inside a dialect's identifier limit.
///
/// The obvious `format!("{bare}__apitap_staging")` is wrong at the edges, and
/// wrong in a way that is hard to see. Postgres truncates identifiers at 63
/// bytes, so a 63-character destination produced a staging name IDENTICAL to
/// the destination. `prepare`'s unconditional `DROP TABLE IF EXISTS <staging>`
/// then aimed at the destination itself, and every run ended with
///
///     rename staging: relation "txxx…" does not exist
///
/// *after* the data had landed — the finalize transaction rolled back the DROP
/// and left the freshly loaded table sitting under the destination's name. So
/// the run reported failure while having changed the destination table, which
/// is the one thing this library promises never happens.
///
/// Two different tables sharing a long prefix collided the same way: at 48
/// characters or more the suffix starts getting cut, and everything past the
/// limit is simply the same name.
///
/// Truncating the BARE part instead keeps the whole suffix — which is what
/// makes the object recognisable as ours, sweepable, and excluded from table
/// discovery — and a short hash of the FULL original name keeps distinct
/// tables distinct. The hash is md5 because it is already a dependency and
/// this is a naming aid, not a security boundary.
pub(crate) fn staging_ident(bare: &str, limit: usize) -> String {
    apitap_ident(bare, "__apitap_staging", limit)
}

/// The same, for any of apitap's `__apitap_*` sidecar names.
///
/// There is more than one: MySQL's swap parks the outgoing table at
/// `<bare>__apitap_old` between its two RENAMEs. That suffix overflows just as
/// readily, and MySQL does not truncate — it REJECTS the statement — so a
/// 64-character table simply could not be replaced, with the error naming an
/// identifier the user never wrote. Postgres is the more dangerous of the two
/// precisely because it accepts the name and silently shortens it.
pub(crate) fn apitap_ident(bare: &str, suffix: &str, limit: usize) -> String {
    const HASH: usize = 8;
    if bare.len() + suffix.len() <= limit {
        return format!("{bare}{suffix}");
    }
    // `limit` is a byte count and `bare` may be UTF-8, so cut on a char
    // boundary rather than slicing blind.
    let room = limit.saturating_sub(suffix.len() + HASH + 1);
    let mut head = String::with_capacity(room);
    for c in bare.chars() {
        if head.len() + c.len_utf8() > room {
            break;
        }
        head.push(c);
    }
    // Hash the suffix in as well: two different sidecars of the same
    // over-long table are truncated to the same head, and only the suffix
    // would tell them apart otherwise.
    let h = hex::encode(md5::Md5::digest(format!("{bare}{suffix}").as_bytes()));
    format!("{head}_{}{suffix}", &h[..HASH])
}

/// Identifier byte limits, per dialect. Postgres is NAMEDATALEN-1; MySQL
/// allows 64 for a table name.
pub(crate) const PG_IDENT_MAX: usize = 63;
pub(crate) const MY_IDENT_MAX: usize = 64;

#[cfg(test)]
mod staging_ident_tests {
    use super::*;

    #[test]
    fn a_short_name_is_left_alone() {
        assert_eq!(staging_ident("orders", PG_IDENT_MAX), "orders__apitap_staging");
    }

    /// The case that made every run fail while changing the destination: at
    /// exactly 63 characters the suffix used to truncate away completely.
    #[test]
    fn a_max_length_name_never_collapses_onto_itself() {
        let bare = "t".repeat(63);
        let st = staging_ident(&bare, PG_IDENT_MAX);
        assert!(st.len() <= PG_IDENT_MAX, "{} bytes", st.len());
        assert_ne!(st, bare, "staging must not be the destination");
        assert!(st.ends_with("__apitap_staging"), "{st}");
    }

    /// Two tables sharing a long prefix must not share a staging object.
    #[test]
    fn a_shared_prefix_does_not_produce_a_shared_staging_name() {
        let a = format!("{}_alpha", "p".repeat(60));
        let b = format!("{}_beta", "p".repeat(60));
        let (sa, sb) = (staging_ident(&a, PG_IDENT_MAX), staging_ident(&b, PG_IDENT_MAX));
        assert_ne!(sa, sb, "{sa} vs {sb}");
        assert!(sa.len() <= PG_IDENT_MAX && sb.len() <= PG_IDENT_MAX);
    }

    /// The same source name must always produce the same staging name, or the
    /// sweep that removes a crashed run's leftovers can never find them.
    #[test]
    fn the_name_is_deterministic() {
        let bare = "z".repeat(80);
        assert_eq!(staging_ident(&bare, PG_IDENT_MAX), staging_ident(&bare, PG_IDENT_MAX));
    }

    /// A multi-byte name must not be cut through the middle of a character.
    #[test]
    fn a_utf8_name_is_cut_on_a_character_boundary() {
        let bare = "\u{e9}".repeat(60); // 120 bytes
        let st = staging_ident(&bare, PG_IDENT_MAX);
        assert!(st.len() <= PG_IDENT_MAX, "{} bytes", st.len());
        assert!(st.ends_with("__apitap_staging"));
        // The assertion is that this line runs at all: a blind byte slice
        // would have panicked inside staging_ident.
    }
}
