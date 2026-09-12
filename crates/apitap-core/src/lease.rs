//! Liveness for a run's `__apitap_lock`, so a killed drain does not wedge a
//! pipeline for ever.
//!
//! # Why this exists
//!
//! [`crate::naming::classify`] refuses to collect anything on age, and the
//! argument there is right: the timestamp inside a run token records when the
//! RUN started, not when the artifact was created, so `now - token` is an UPPER
//! bound on the artifact's age and can never prove it is stale. Table 40 of a
//! long multi-table load mints its artifact under a token that is already hours
//! old.
//!
//! That argument forbids reaping on the TOKEN. It does not forbid reaping on a
//! fact the dead run itself wrote down while it was alive — which is exactly
//! what `classify`'s own text names as the missing piece: "a liveness signal
//! from the engine itself — an object mtime that advances as the run writes, a
//! catalog lock".
//!
//! A lease is that signal, spelled portably. Every CDC run writes one row per
//! destination table it holds, carrying an `expires_at` STAMPED AND COMPARED BY
//! THE DESTINATION SERVER, and renews it on a timer. A peer may drop a blocking
//! lock if and only if a lease row exists for that exact peer token and the
//! destination itself says it has lapsed. No lease row, no collection — which
//! keeps every artifact that predates this mechanism, every planted one, and
//! every bulk lock exactly as refused as it is today.
//!
//! # Why the clock cannot lie
//!
//! Both the stamp and the comparison happen inside the destination, in one
//! statement, against `now()`/`UTC_TIMESTAMP()`/`now64()`/`CURRENT_TIMESTAMP()`.
//! No client clock enters the decision, so two runs on two machines with two
//! badly-set clocks still agree. A destination clock that steps BACKWARD fails
//! safe — nothing can be collected until it catches up, which is the refusal
//! direction; the refusal prints the remaining life so an absurd figure is
//! visible in the log rather than mute.
//!
//! # Why a lapse is not enough on its own
//!
//! A live run that is merely partitioned from its destination also stops
//! renewing. So on Postgres and MySQL the lease row is ALSO the fence: the
//! drain's apply transaction takes it `FOR UPDATE` as its first statement and
//! renews it in the same transaction, and the collector's claim is an `UPDATE`
//! of that row with `NOWAIT`. The two cannot interleave, so a run whose lease
//! was collected writes nothing afterwards — it fails its own fence and exits.
//! ClickHouse and BigQuery have no such row lock; there the check is
//! check-then-act and a wrongly-evicted drain can land the one window already
//! in flight, which the window machinery already makes idempotent. That
//! weakening is stated in `docs/failure-modes.md` rather than papered over.

use crate::error::Result;

/// The lease table. One per destination, like `_apitap_state` — it is keyed by
/// (destination table, run token), so it never needs shortening.
pub(crate) const LEASE_TABLE: &str = "_apitap_lease";

/// What a peer's lease says about it.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    /// Seconds of life left, by the DESTINATION's clock. Negative = lapsed.
    pub expires_in: i64,
    /// Some other run already claimed this lease.
    pub collected: bool,
}

impl Lease {
    /// May this peer's lock be collected? Lapsed, or already claimed by someone
    /// else — the second disjunct is what makes two simultaneous collectors
    /// agree instead of one of them refusing over a lock that is already gone.
    pub(crate) fn lapsed(&self) -> bool {
        self.expires_in <= 0 || self.collected
    }
}

/// Seconds a lease stays live after its last renewal.
///
/// 300 by default: five wasted runs on a per-minute cron, at most one on a
/// five-minute schedule, and far above any plausible renewal stall. Clamped
/// rather than free so a typo cannot make it either meaningless or eternal, and
/// overridable because the e2e legs must not sleep five minutes per engine and
/// because a per-minute schedule has a real reason to choose 60.
pub(crate) fn ttl_secs() -> u64 {
    std::env::var("APITAP_LEASE_TTL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300)
        .clamp(30, 3600)
}

/// How often the keeper renews. Always a TENTH of the TTL — the RATIO is the
/// thing being chosen, not the absolute.
///
/// Ten missed ticks before a live run looks dead. The keeper is a tokio task
/// and the CDC pipeline runs on a `current_thread` runtime, so it only ticks at
/// await points; ten is the margin for an await-free stretch, and the genuinely
/// await-free stretches (collapsing a window, rendering a body) are bounded by
/// the window byte budget.
pub(crate) fn renew_secs() -> u64 {
    (ttl_secs() / 10).clamp(3, 360)
}

/// Renews a run's leases on a timer, off the window path.
///
/// Off the window path is the whole point: the renewal must not be able to
/// block behind an apply, and an apply must not be able to starve the renewal.
/// The one row the group-wide renewal cannot lock is the row that run's own
/// apply transaction is holding — and that transaction renews it itself, so
/// skipping it is exactly right rather than a compromise.
pub(crate) struct Keeper {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Keeper {
    /// `renew` is called every [`renew_secs`]; it returns how many rows it
    /// actually touched, which the keeper only logs.
    pub(crate) fn spawn<F, Fut>(renew: F) -> Keeper
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<u64>> + Send,
    {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stop.clone();
        let every = std::time::Duration::from_secs(renew_secs());
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if flag.load(std::sync::atomic::Ordering::Relaxed)
                    || crate::shutdown::requested()
                {
                    return;
                }
                // A failed renewal is NOT a reason to kill a drain: a blip on
                // the destination would then end a run that is perfectly
                // healthy, and there are nine more ticks before the lease
                // lapses. Note it and try again.
                if let Err(e) = renew().await {
                    if std::env::var("APITAP_DEBUG").is_ok() {
                        eprintln!("[lease] renewal failed, retrying next tick: {e}");
                    }
                }
            }
        });
        Keeper { stop, handle: Some(handle) }
    }

    /// Stop AND JOIN. Joining matters: on ClickHouse a renewal is an INSERT, so
    /// a tick still in flight when the lease is closed would resurrect the row
    /// and leave a lock nothing can collect.
    pub(crate) async fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.abort();
            let _ = h.await;
        }
    }
}
