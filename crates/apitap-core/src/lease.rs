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
//! # Ownership is a value, not a check
//!
//! **Owning this run's claim means one thing: my row exists and is not
//! collected.** `expires_at` is not part of it — it is the collector's
//! permission to claim, and a renewal's right to keep publishing. So a lease
//! that lapsed with nobody claiming it is still this run's (the next renewal
//! revives it), and a claimed one is gone for good however healthy the run
//! behind it still looks.
//!
//! # Why a lapse is not enough on its own
//!
//! A live run that is merely partitioned from its destination also stops
//! renewing, so each engine has to say the same thing in its own words:
//!
//! - **Postgres, MySQL** — the lease row is ALSO the fence. The apply
//!   transaction takes it `FOR UPDATE` as its first statement and renews it in
//!   the same transaction; a collector's claim is a locked write of that same
//!   row (`NOWAIT` on Postgres, a one-second wait on MySQL). The two cannot
//!   interleave, so a run whose claim was collected writes nothing afterwards:
//!   its next unit fails its own fence and exits.
//! - **ClickHouse** — no row lock to hold, so the predicate travels inside
//!   every statement instead: the row exists, is not collected, and has more
//!   than half a TTL of life left. True at analysis means no claim can be taken
//!   before `expires_at`, so each statement is bounded server-side to that same
//!   half and an evicted drain lands at most the one already executing. The
//!   lease table carries no TTL, and a claim's version outranks every renewal's.
//! - **BigQuery** — no transaction to roll back either, so every apply script's
//!   first statement updates this run's own `_apitap_fence<token>` table, and a
//!   collector marks that table claimed before it proceeds. A script that
//!   overlaps the claim matches no row and rolls back whole.
//! - **Iceberg** — the one `Unguarded` store, by design.
//!
//! What is weaker on each of them, and what stays deferred, is written down in
//! `docs/failure-modes.md` rather than papered over.

use crate::error::{Error, Result};
use crate::guard::{GuardStore, Mine};
use crate::naming::RunId;

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
///
/// It renews until it is stopped, and nothing else stops it. A SIGTERM
/// wind-down used to end it early (`shutdown::requested()`), which let a run
/// that was still landing its last window look dead to a peer; the lease now
/// lives exactly as long as the run that holds it. `Drop` aborts it, so a
/// panic that unwinds past its owner cannot leave it renewing a dead run.
pub(crate) struct Keeper {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Keeper {
    /// The tenure's keeper: renew, then ask the destination which of this
    /// run's rows still exist uncollected. A key missing from that answer was
    /// CLAIMED (or deleted) by someone else — that, and never expiry, is an
    /// eviction. A lapsed lease that nobody has claimed is still this run's:
    /// the next renewal revives it, and every write is fenced on the row
    /// anyway.
    ///
    /// A failed renewal is NOT a reason to stop a drain: a blip on the
    /// destination would then end a run that is perfectly healthy, and there
    /// are nine more ticks before the lease lapses. It is noted and retried.
    fn for_tenure<F: Fence>(
        dest: std::sync::Arc<F>,
        keys: Vec<String>,
        token: String,
        evicted: std::sync::Arc<std::sync::atomic::AtomicBool>,
        every: std::time::Duration,
    ) -> Keeper {
        Self::every(every, move || {
            let (dest, keys, token, evicted) = (dest.clone(), keys.clone(), token.clone(), evicted.clone());
            async move {
                if let Err(e) = dest.lease_renew(&keys, &token).await {
                    if std::env::var("APITAP_DEBUG").is_ok() {
                        eprintln!("[lease] renewal failed, retrying next tick: {e}");
                    }
                }
                if let Ok(still) = dest.lease_unclaimed(&token).await {
                    for k in keys.iter().filter(|k| !still.contains(k)) {
                        if !evicted.swap(true, std::sync::atomic::Ordering::SeqCst) {
                            eprintln!(
                                "apitap: {k}: another run collected this drain's claim; it stops \
                                 before its next write"
                            );
                        }
                    }
                }
            }
        })
    }

    fn every<T, Fut>(every: std::time::Duration, tick: T) -> Keeper
    where
        T: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stop.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                tick().await;
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

impl Drop for Keeper {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Ownership as a value
// ───────────────────────────────────────────────────────────────────────────
//
// Every CDC write goes through a unit a `Tenure` opened (logbased/run.rs), and
// each destination's `mod store` is the only code holding a connection it
// could write through (`no_connection_types_outside_store`).

/// What a script names when its fence found the run's claim gone (BigQuery).
pub(crate) const LOST_MARK: &str = "apitap-lease-lost";

/// The time fence where no row lock exists (ClickHouse statements, BigQuery
/// DDL): an owner may write only while more than half its TTL is left, and a
/// statement is bounded server-side to the same half, so it ends before any
/// peer can claim the lease.
pub(crate) fn owned_margin_secs() -> u64 {
    ttl_secs() / 2
}

/// The refusal a drain gives when its claim is gone. Nothing was written.
pub(crate) fn no_longer_holds(keys: &[String]) -> Error {
    Error::Locked(format!(
        "{}: this drain no longer holds the table — another run collected its claim, so it is \
         not allowed to write. Nothing more was written. Re-run; the other run either finished \
         or will be collected in turn.",
        keys.join(", ")
    ))
}

/// The one test a store applies to its own row before a unit may write.
///
/// Owner = the row EXISTS and is NOT collected. Expiry is not part of it: a
/// lapsed lease only gives a collector permission to CLAIM, and until one does
/// the run still owns the table (the next renewal revives it). A missing row is
/// not an owner — 0.56.0 read "no row" as "nothing to fence against" and wrote,
/// which is exactly what a collector that deleted the row would have wanted to
/// stop.
pub(crate) fn owner_verdict(row: Option<&Lease>, keys: &[String]) -> Result<()> {
    match row {
        Some(l) if !l.collected => Ok(()),
        _ => Err(no_longer_holds(keys)),
    }
}

/// The watermark a unit writes when it closes — the only place one is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Watermark {
    Set { table: String, source_id: String, lsn: u64, rows: u64 },
    Clear { table: String, source_id: String },
}

impl Watermark {
    pub(crate) fn table(&self) -> &str {
        match self {
            Watermark::Set { table, .. } | Watermark::Clear { table, .. } => table,
        }
    }
}

/// The lease rows of one destination: open, renew, and who still holds what.
/// Liveness questions a COLLECTOR asks (get, claim, close) are the guard's
/// (`crate::guard::GuardStore`), so there is one spelling of each.
pub(crate) trait LeaseStore: Send + Sync + 'static {
    fn lease_key(&self, dest_table: &str) -> String;
    /// Open this run's rows (and on BigQuery its per-run fence table).
    fn lease_open(&self, keys: &[String], token: &str)
        -> impl std::future::Future<Output = Result<()>> + Send;
    /// Never blocks; never touches a collected or missing row.
    fn lease_renew(&self, keys: &[String], token: &str)
        -> impl std::future::Future<Output = Result<u64>> + Send;
    /// Keys of `token` whose row EXISTS and is NOT collected. Expiry ignored.
    fn lease_unclaimed(&self, token: &str)
        -> impl std::future::Future<Output = Result<Vec<String>>> + Send;
    /// Drop run-scoped objects not tied to a table (BigQuery's fence). An
    /// error keeps every member's claim, so a collector finishes the job.
    fn close_run(&self, token: &str) -> impl std::future::Future<Output = Result<()>> + Send;
    #[cfg(test)]
    fn note(&self, _event: &str) {}
}

/// A destination that can fence a unit of writes on its own lease row.
pub(crate) trait Fence: LeaseStore {
    type Unit<'a>: Send
    where
        Self: 'a;
    /// The guard adapter for one member, and the bare name it spells it with.
    /// Per member, not per destination: a Postgres group's tables may live in
    /// different schemas, and the guard lists one schema.
    fn guard(&self, dest_table: &str) -> (Box<dyn GuardStore + '_>, String);
    /// BigQuery: every close of one run mutates the same fence table, so the
    /// closes are serialized rather than left to abort each other.
    fn serial_commit(&self) -> bool {
        false
    }
    fn open_unit<'a>(&'a self, keys: &[String], token: &str)
        -> impl std::future::Future<Output = Result<Self::Unit<'a>>> + Send;
    /// Writes every mark (and on BigQuery/Iceberg the data commit) and commits.
    /// `Ok` = the destination accepted the unit as the owner's.
    fn close_unit<'a>(&'a self, u: Self::Unit<'a>, token: &str, marks: Vec<Watermark>)
        -> impl std::future::Future<Output = Result<()>> + Send;
}

/// One open unit of writes. Only `Tenure::open` makes one, and `release`
/// cannot pass it: it carries a read guard `release` waits for.
pub(crate) struct Held<'t, F: Fence + 't> {
    pub(crate) unit: F::Unit<'t>,
    tables: Vec<String>,
    _inflight: tokio::sync::OwnedRwLockReadGuard<()>,
}

/// This run's ownership of a group of destination tables, as a value: every
/// CDC write goes through a unit it opens, and nothing else can open one.
pub(crate) struct Tenure<F: Fence> {
    dest: std::sync::Arc<F>,
    run: RunId,
    announced: std::sync::Mutex<Vec<(String, crate::guard::Announced)>>,
    evicted: std::sync::Arc<std::sync::atomic::AtomicBool>,
    closing: std::sync::atomic::AtomicBool,
    units: std::sync::Arc<tokio::sync::RwLock<()>>,
    commit_gate: tokio::sync::Mutex<()>,
    keeper: std::sync::Mutex<Option<Keeper>>,
}

impl<F: Fence> Tenure<F> {
    /// Lease first, then announce every member, then check every member — the
    /// order that makes a dead run collectable and two live ones unable to
    /// both proceed — then start renewing.
    pub(crate) async fn acquire(dest: std::sync::Arc<F>, tables: &[String], run: RunId) -> Result<Self> {
        Self::acquire_every(dest, tables, run, std::time::Duration::from_secs(renew_secs())).await
    }

    /// `acquire` with the keeper's period given, so a store's tests can watch
    /// its keeper tick in milliseconds.
    pub(crate) async fn acquire_every(
        dest: std::sync::Arc<F>,
        tables: &[String],
        run: RunId,
        every: std::time::Duration,
    ) -> Result<Self> {
        let keys: Vec<String> = tables.iter().map(|t| dest.lease_key(t)).collect();
        dest.lease_open(&keys, run.token()).await?;
        let mut held: Vec<(String, crate::guard::Announced)> = Vec::new();
        // A refusal gives back what it holds, the run's own objects included:
        // a refused BigQuery drain would otherwise leave its fence table.
        for t in tables {
            let (g, bare) = dest.guard(t);
            match crate::guard::announce(&*g, &bare, &run).await {
                Ok(a) => held.push((t.clone(), a)),
                Err(e) => {
                    give_back_run(&*dest, held, run.token()).await;
                    return Err(e);
                }
            }
        }
        for t in tables {
            let (g, bare) = dest.guard(t);
            if let Err(e) = crate::guard::check_peers(&*g, &bare, &run, Mine::Keep).await {
                give_back_run(&*dest, held, run.token()).await;
                return Err(e);
            }
        }
        let evicted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let keeper = Keeper::for_tenure(dest.clone(), keys, run.token().to_string(), evicted.clone(), every);
        Ok(Tenure {
            dest,
            run,
            announced: std::sync::Mutex::new(held),
            evicted,
            closing: std::sync::atomic::AtomicBool::new(false),
            units: std::sync::Arc::new(tokio::sync::RwLock::new(())),
            commit_gate: tokio::sync::Mutex::new(()),
            keeper: std::sync::Mutex::new(Some(keeper)),
        })
    }

    pub(crate) fn run(&self) -> &RunId {
        &self.run
    }

    /// Open a unit over `tables`. Refused, with no I/O, once the run is
    /// winding down or the keeper has seen its claim taken.
    pub(crate) async fn open(&self, tables: &[&str]) -> Result<Held<'_, F>> {
        use std::sync::atomic::Ordering::SeqCst;
        let keys: Vec<String> = tables.iter().map(|t| self.dest.lease_key(t)).collect();
        if self.closing.load(SeqCst) {
            return Err(Error::Locked(format!("{}: the run is winding down", keys.join(", "))));
        }
        if self.evicted.load(SeqCst) {
            return Err(no_longer_holds(&keys));
        }
        let g = self.units.clone().read_owned().await;
        // `release` may have started (and finished) while this waited.
        if self.closing.load(SeqCst) {
            return Err(Error::Locked(format!("{}: the run is winding down", keys.join(", "))));
        }
        let unit = self.dest.open_unit(&keys, self.run.token()).await?;
        Ok(Held { unit, tables: tables.iter().map(|t| t.to_string()).collect(), _inflight: g })
    }

    /// Close a unit: its marks, then the commit — serialized where one run's
    /// closes share a fence object.
    pub(crate) async fn close<'t>(&'t self, h: Held<'t, F>, marks: Vec<Watermark>) -> Result<()> {
        if let Some(m) = marks.iter().find(|m| !h.tables.iter().any(|t| t == m.table())) {
            return Err(Error::Transfer(format!("internal: mark for unheld table {}", m.table())));
        }
        let _g = if self.dest.serial_commit() { Some(self.commit_gate.lock().await) } else { None };
        let Held { unit, _inflight, .. } = h;
        let r = self.dest.close_unit(unit, self.run.token(), marks).await;
        drop(_inflight);
        r
    }

    /// The destination this tenure fences.
    pub(crate) fn dest(&self) -> &F {
        &self.dest
    }

    /// Give the table back: wait for every open unit, stop the keeper, then
    /// per member its scratch, its markers, and its lease only with the proof
    /// that its markers are gone (`give_back`).
    pub(crate) async fn release(&self) {
        self.closing.store(true, std::sync::atomic::Ordering::SeqCst);
        let _all = self.units.write().await;
        let keeper = self.keeper.lock().expect("keeper").take();
        if let Some(mut k) = keeper {
            k.stop().await;
            #[cfg(test)]
            self.dest.note("keeper_stop");
        }
        let held = std::mem::take(&mut *self.announced.lock().expect("announced"));
        give_back_run(&*self.dest, held, self.run.token()).await;
    }

}

impl<F: Fence> Drop for Tenure<F> {
    fn drop(&mut self) {
        if let Some(k) = self.keeper.lock().ok().and_then(|mut k| k.take()) {
            drop(k); // Keeper::drop aborts; there is no await in Drop.
        }
        if let Ok(mut held) = self.announced.lock() {
            for (_, a) in held.drain(..) {
                a.abandon();
            }
        }
    }
}

/// Per member: its run-scoped scratch first, then its markers, then — with
/// the proof that every marker is gone — its lease. `true` when every member
/// was given back.
///
/// Scratch before markers, the order the collector uses (`guard::check_peers`)
/// and for the same reason. The scratch (ClickHouse's key table, BigQuery's
/// staging, a changelog temp) is named by this run's token and is not a
/// marker: once the markers and the lease are gone, no run will ever name the
/// token again, and nothing reaps on age — a failed drop would leak it for
/// ever. So a failed sweep keeps the member's markers AND its lease, loudly:
/// after the TTL the next run collects the member like a dead drain's, and its
/// sweep retries the drop. (The handoff's §0 L5 swept between markers and
/// lease and ignored a failed sweep.)
async fn give_back<F: Fence>(dest: &F, held: Vec<(String, crate::guard::Announced)>, token: &str) -> bool {
    let mut all = true;
    for (t, a) in held {
        let (g, bare) = dest.guard(&t);
        if let Err(e) = g.sweep_run(&bare, token).await {
            eprintln!(
                "apitap: {}: could not drop this run's scratch ({e}); its claim is kept so the \
                 next run collects it after the TTL and retries the drop",
                g.dest_label(&bare)
            );
            a.abandon();
            all = false;
            continue;
        }
        match crate::guard::release(&*g, a).await {
            Ok(proof) => g.lease_close(proof).await,
            Err(a) => {
                a.abandon();
                all = false;
            }
        }
    }
    all
}

/// The run's own objects (BigQuery's fence table) first, then `give_back`.
///
/// First, and not best-effort: like a member's scratch, the fence is named by
/// the token alone, so once the markers are gone nothing would ever look for
/// it again — 0.57.0 deleted it last and ignored a failure, which leaked it
/// for good. A failure here keeps every member's markers and lease, and the
/// collector that takes them after the TTL deletes it (`lease_claim`).
/// Deleting it first is safe: `release` has waited for every unit, and only a
/// unit's close writes through the fence.
async fn give_back_run<F: Fence>(dest: &F, held: Vec<(String, crate::guard::Announced)>, token: &str) {
    if let Err(e) = dest.close_run(token).await {
        eprintln!(
            "apitap: could not drop run {token}'s own objects ({e}); every claim is kept so the \
             next run collects them after the TTL"
        );
        for (_, a) in held {
            a.abandon();
        }
        return;
    }
    give_back(dest, held, token).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type Log = Arc<Mutex<Vec<String>>>;

    /// The guard side of the fake: no peers, and every call logged. The flag
    /// makes `sweep_run` fail.
    struct FakeGuard(Log, bool);

    #[async_trait::async_trait]
    impl GuardStore for FakeGuard {
        fn limit(&self) -> usize {
            crate::naming::ROOMY
        }
        fn dest_label(&self, bare: &str) -> String {
            format!("s.{bare}")
        }
        async fn list(&self, _b: &str, _k: &[crate::naming::Artifact]) -> Result<Vec<crate::guard::Listed>> {
            Ok(Vec::new())
        }
        async fn create_marker(&self, _raw: &str) -> Result<()> {
            Ok(())
        }
        async fn drop_object(&self, _raw: &str) -> Result<()> {
            self.0.lock().unwrap().push("drop_marker".into());
            Ok(())
        }
        async fn lease_get(&self, _k: &str, _t: &str) -> Result<Option<Lease>> {
            Ok(None)
        }
        async fn lease_claim(&self, _k: &str, _t: &str) -> Result<crate::guard::Claim> {
            Ok(crate::guard::Claim::Absent)
        }
        async fn lease_close(&self, _p: crate::guard::Released) {
            self.0.lock().unwrap().push("lease_close".into());
        }
        async fn sweep_run(&self, _b: &str, _t: &str) -> Result<()> {
            self.0.lock().unwrap().push("sweep_run".into());
            if self.1 {
                return Err(Error::Transfer("sweep: injected".into()));
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeFence {
        log: Log,
        keys: Mutex<Vec<String>>,
        collected: Mutex<HashSet<String>>,
        expired: Mutex<HashSet<String>>,
        renews: AtomicUsize,
        opens: AtomicUsize,
        serial: bool,
        in_close: AtomicUsize,
        max_in_close: AtomicUsize,
        close_waits: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        sweep_fails: bool,
        close_run_fails: bool,
    }

    impl FakeFence {
        fn events(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    impl LeaseStore for FakeFence {
        fn lease_key(&self, t: &str) -> String {
            format!("s.{t}")
        }
        async fn lease_open(&self, keys: &[String], _t: &str) -> Result<()> {
            *self.keys.lock().unwrap() = keys.to_vec();
            Ok(())
        }
        async fn lease_renew(&self, _k: &[String], _t: &str) -> Result<u64> {
            self.renews.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }
        async fn lease_unclaimed(&self, _t: &str) -> Result<Vec<String>> {
            let collected = self.collected.lock().unwrap().clone();
            Ok(self.keys.lock().unwrap().iter().filter(|k| !collected.contains(*k)).cloned().collect())
        }
        async fn close_run(&self, _t: &str) -> Result<()> {
            self.log.lock().unwrap().push("close_run".into());
            if self.close_run_fails {
                return Err(Error::Transfer("close_run: injected".into()));
            }
            Ok(())
        }
        fn note(&self, e: &str) {
            self.log.lock().unwrap().push(e.into());
        }
    }

    impl Fence for FakeFence {
        type Unit<'a> = ();
        fn guard(&self, t: &str) -> (Box<dyn GuardStore + '_>, String) {
            (Box::new(FakeGuard(self.log.clone(), self.sweep_fails)), t.to_string())
        }
        fn serial_commit(&self) -> bool {
            self.serial
        }
        async fn open_unit<'a>(&'a self, _k: &[String], _t: &str) -> Result<()> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("open_unit".into());
            Ok(())
        }
        async fn close_unit<'a>(&'a self, _u: (), _t: &str, _m: Vec<Watermark>) -> Result<()> {
            let now = self.in_close.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_close.fetch_max(now, Ordering::SeqCst);
            let wait = self.close_waits.lock().unwrap().take();
            if let Some(rx) = wait {
                let _ = rx.await;
            } else {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            self.in_close.fetch_sub(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("close_unit".into());
            Ok(())
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    const SLOW: Duration = Duration::from_secs(3600);

    async fn tenure(f: Arc<FakeFence>, every: Duration) -> Tenure<FakeFence> {
        Tenure::acquire_every(f, &["a".to_string(), "b".to_string()], RunId::mint_drain("s"), every)
            .await
            .unwrap()
    }

    #[test]
    fn open_refuses_after_eviction() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), SLOW).await;
            t.evicted.store(true, Ordering::SeqCst);
            let e = t.open(&["a"]).await.err().expect("an evicted run opens nothing");
            assert!(matches!(e, Error::Locked(_)), "{e}");
            assert_eq!(f.opens.load(Ordering::SeqCst), 0, "no I/O once evicted");
            t.release().await;
        });
    }

    #[test]
    fn release_waits_for_open_units() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let (tx, rx) = tokio::sync::oneshot::channel();
            *f.close_waits.lock().unwrap() = Some(rx);
            let t = tenure(f.clone(), SLOW).await;
            let holder = async {
                let h = t.open(&["a"]).await.unwrap();
                t.close(h, vec![]).await.unwrap();
            };
            let releaser = async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                t.release().await;
            };
            let opener = async {
                tokio::time::sleep(Duration::from_millis(60)).await;
                let _ = tx.send(());
            };
            tokio::join!(holder, releaser, opener);
            let ev = f.events();
            let at = |e: &str| ev.iter().position(|x| x == e).unwrap_or_else(|| panic!("{e} missing: {ev:?}"));
            assert!(at("open_unit") < at("close_unit"), "{ev:?}");
            assert!(at("close_unit") < at("keeper_stop"), "release passed an open unit: {ev:?}");
            assert!(at("keeper_stop") < at("close_run"), "{ev:?}");
            assert!(at("close_run") < at("sweep_run"), "the run's objects go before any member's: {ev:?}");
            assert!(at("sweep_run") < at("drop_marker"), "scratch goes before the markers: {ev:?}");
            assert!(at("drop_marker") < at("lease_close"), "{ev:?}");
        });
    }

    /// A member whose scratch would not drop keeps its markers and its lease,
    /// so the next run collects it after the TTL and its sweep retries the
    /// drop. Given back anyway, nothing would ever name the token again.
    #[test]
    fn a_failed_sweep_keeps_markers_and_lease() {
        rt().block_on(async {
            let f = Arc::new(FakeFence { sweep_fails: true, ..Default::default() });
            let t = tenure(f.clone(), SLOW).await;
            t.release().await;
            let ev = f.events();
            assert!(ev.iter().any(|e| e == "sweep_run"), "{ev:?}");
            for kept in ["drop_marker", "lease_close"] {
                assert!(!ev.iter().any(|e| e == kept), "{kept} after a failed sweep: {ev:?}");
            }
        });
    }

    /// The run's own objects (BigQuery's fence table) are named by the token
    /// alone: if they would not drop, every member keeps its markers and its
    /// lease, so the collector that takes them deletes the fence. 0.57.0's
    /// first cut dropped them last and ignored the error — after the markers,
    /// when no run would ever name the token again.
    #[test]
    fn a_failed_close_run_keeps_every_claim() {
        rt().block_on(async {
            let f = Arc::new(FakeFence { close_run_fails: true, ..Default::default() });
            let t = tenure(f.clone(), SLOW).await;
            t.release().await;
            let ev = f.events();
            assert!(ev.iter().any(|e| e == "close_run"), "{ev:?}");
            for kept in ["drop_marker", "lease_close"] {
                assert!(!ev.iter().any(|e| e == kept), "{kept} after a failed close_run: {ev:?}");
            }
        });
    }

    /// I1: the only writable handle is a unit. Outside each destination's
    /// `mod store` there is no connection type a write could go through, and
    /// no transaction statement a body could open or end one with.
    #[test]
    fn no_connection_types_outside_store() {
        const FILES: &[(&str, &str)] = &[
            ("dest_pg.rs", include_str!("logbased/dest_pg.rs")),
            ("dest_my.rs", include_str!("logbased/dest_my.rs")),
            ("dest_ch.rs", include_str!("logbased/dest_ch.rs")),
            ("dest_bq.rs", include_str!("logbased/dest_bq.rs")),
            ("dest_ice.rs", include_str!("logbased/dest_ice.rs")),
        ];
        const TYPES: &[&str] = &["PgPool", "MySqlShared", "mysql_async::Pool", "ChConn", "BqConn"];
        const STATEMENTS: &[&str] = &["COMMIT", "START TRANSACTION", "ROLLBACK"];
        for (name, src) in FILES {
            let (code, strings) = outside_store(src).unwrap_or_else(|e| panic!("{name}: {e}"));
            for t in TYPES {
                assert!(!has_word(&code, t), "{name}: {t} outside mod store");
            }
            for st in STATEMENTS {
                // As a STATEMENT: the literal's first word, or the first after a
                // `;`. `ON COMMIT DROP` is a clause of a temp table inside a
                // unit, and prose ("the commit") is not SQL at all.
                for lit in &strings {
                    // Source text, escapes unprocessed: `\n` and a line
                    // continuation are whitespace to SQL.
                    let sql = lit.replace("\\\n", " ").replace("\\n", " ").replace("\\t", " ").replace("\\r", " ");
                    let opens = sql.split(';').any(|stmt| {
                        let t = stmt.trim_start();
                        t.starts_with(st) && !t[st.len()..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
                    });
                    assert!(!opens, "{name}: {st} outside mod store: {lit:?}");
                }
            }
        }
    }

    /// `word` in `hay`, not as part of a longer identifier.
    fn has_word(hay: &str, word: &str) -> bool {
        let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
        hay.match_indices(word).any(|(i, _)| {
            !hay[..i].chars().next_back().is_some_and(ident) && !hay[i + word.len()..].chars().next().is_some_and(ident)
        })
    }

    /// The file with its one `mod store { … }` block — and its `mod tests`,
    /// which spells statements to assert on them — cut out, as (code with
    /// comments and literals blanked, the string literals). Braces are matched
    /// on code only, so a `{` inside a format string or a comment counts for
    /// nothing.
    fn outside_store(src: &str) -> std::result::Result<(String, Vec<String>), String> {
        #[derive(PartialEq)]
        enum K { Code, Str }
        // Lex into (kind, text) runs.
        let b = src.as_bytes();
        let mut runs: Vec<(K, String)> = vec![(K::Code, String::new())];
        let push = |runs: &mut Vec<(K, String)>, k: K, t: &str| match runs.last_mut() {
            Some((lk, lt)) if *lk == k && k == K::Code => lt.push_str(t),
            _ => runs.push((k, t.to_string())),
        };
        let mut i = 0;
        while i < b.len() {
            let rest = &src[i..];
            if rest.starts_with("//") {
                let end = rest.find('\n').map_or(src.len(), |n| i + n);
                push(&mut runs, K::Code, " ");
                i = end;
            } else if rest.starts_with("/*") {
                let (mut depth, mut j) = (0usize, i);
                while j < b.len() {
                    if src[j..].starts_with("/*") { depth += 1; j += 2; }
                    else if src[j..].starts_with("*/") { depth -= 1; j += 2; if depth == 0 { break; } }
                    else { j += 1; }
                }
                push(&mut runs, K::Code, " ");
                i = j;
            } else if rest.starts_with("r\"") || rest.starts_with("r#") || rest.starts_with("br\"") || rest.starts_with("br#") {
                let start = if rest.starts_with('b') { i + 2 } else { i + 1 };
                let hashes = src[start..].bytes().take_while(|&c| c == b'#').count();
                if b.get(start + hashes) != Some(&b'"') {
                    push(&mut runs, K::Code, &src[i..i + 1]);
                    i += 1;
                    continue;
                }
                let close = format!("\"{}", "#".repeat(hashes));
                let body = start + hashes + 1;
                let end = src[body..].find(&close).ok_or("unterminated raw string")? + body;
                push(&mut runs, K::Str, &src[body..end]);
                push(&mut runs, K::Code, " \"\" ");
                i = end + close.len();
            } else if b[i] == b'"' || rest.starts_with("b\"") {
                let mut j = if b[i] == b'b' { i + 2 } else { i + 1 };
                let body = j;
                while j < b.len() && b[j] != b'"' {
                    j += if b[j] == b'\\' { 2 } else { 1 };
                }
                push(&mut runs, K::Str, &src[body..j.min(b.len())]);
                push(&mut runs, K::Code, " \"\" ");
                i = j + 1;
            } else if b[i] == b'\'' {
                // A char literal ('x', '\n', '\''), or a lifetime ('a).
                let lit = if b.get(i + 1) == Some(&b'\\') {
                    // Past the escaped character, so '\'' ends where it should.
                    src[i + 3..].find('\'').map(|n| i + 3 + n + 1)
                } else {
                    let c = src[i + 1..].chars().next().map_or(0, char::len_utf8);
                    (b.get(i + 1 + c) == Some(&b'\'')).then_some(i + 2 + c)
                };
                match lit {
                    Some(end) => { push(&mut runs, K::Code, " ' ' "); i = end; }
                    None => { push(&mut runs, K::Code, "'"); i += 1; }
                }
            } else {
                let c = rest.chars().next().unwrap();
                push(&mut runs, K::Code, &rest[..c.len_utf8()]);
                i += c.len_utf8();
            }
        }
        // Cut `mod store { … }` by matching braces in code only.
        let mut out = String::new();
        let mut strings = Vec::new();
        let (mut cutting, mut depth, mut found) = (false, 0i64, 0);
        for (k, t) in runs {
            if k == K::Str {
                if !cutting { strings.push(t); }
                continue;
            }
            let mut code = t.as_str();
            loop {
                if !cutting {
                    let next = ["mod store {", "mod tests {"]
                        .iter()
                        .filter_map(|m| code.find(m).map(|at| (at, *m)))
                        .min();
                    match next {
                        Some((at, m)) => {
                            out.push_str(&code[..at]);
                            cutting = true;
                            found += usize::from(m == "mod store {");
                            depth = 1;
                            code = &code[at + m.len()..];
                        }
                        _ => { out.push_str(code); break; }
                    }
                } else {
                    let mut end = None;
                    for (n, ch) in code.char_indices() {
                        match ch {
                            '{' => depth += 1,
                            '}' => { depth -= 1; if depth == 0 { end = Some(n + 1); break; } }
                            _ => {}
                        }
                    }
                    match end {
                        Some(e) => { cutting = false; code = &code[e..]; }
                        None => break,
                    }
                }
            }
        }
        if found != 1 || cutting {
            return Err(format!("expected one closed `mod store {{ … }}`, found {found} (open: {cutting})"));
        }
        Ok((out, strings))
    }

    #[test]
    fn keeper_flags_claim_not_expiry() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), Duration::from_millis(10)).await;
            f.collected.lock().unwrap().insert("s.a".into());
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(t.evicted.load(Ordering::SeqCst), "a claimed key is an eviction");
            t.release().await;

            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), Duration::from_millis(10)).await;
            f.expired.lock().unwrap().insert("s.b".into());
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(!t.evicted.load(Ordering::SeqCst), "an expired but unclaimed lease is still ours");
            t.release().await;
        });
    }

    #[test]
    fn keeper_ignores_shutdown_flag() {
        let _serial = crate::shutdown::tests::SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        crate::shutdown::request();
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), Duration::from_millis(10)).await;
            tokio::time::sleep(Duration::from_millis(45)).await;
            let n = f.renews.load(Ordering::SeqCst);
            t.release().await;
            crate::shutdown::clear();
            assert!(n >= 3, "a wind-down keeps the lease alive until release: {n} renewals");
        });
        crate::shutdown::clear();
    }

    #[test]
    fn hold_refuses_missing_collected_lapsed() {
        let keys = vec!["s.a".to_string()];
        assert!(matches!(owner_verdict(None, &keys), Err(Error::Locked(_))), "no row is not an owner");
        let collected = Lease { expires_in: 100, collected: true };
        assert!(matches!(owner_verdict(Some(&collected), &keys), Err(Error::Locked(_))));
        let lapsed = Lease { expires_in: -5, collected: false };
        assert!(owner_verdict(Some(&lapsed), &keys).is_ok(), "lapsed but unclaimed is still ours");
    }

    #[test]
    fn dropping_tenure_aborts_keeper() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), Duration::from_millis(10)).await;
            tokio::time::sleep(Duration::from_millis(35)).await;
            drop(t);
            let n = f.renews.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(f.renews.load(Ordering::SeqCst), n, "a dropped tenure keeps renewing");
        });
    }

    #[test]
    fn open_after_release_is_refused() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), SLOW).await;
            t.release().await;
            assert!(matches!(t.open(&["a"]).await, Err(Error::Locked(_))));
            assert_eq!(f.opens.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn serial_commit_gate() {
        rt().block_on(async {
            let f = Arc::new(FakeFence { serial: true, ..Default::default() });
            let t = tenure(f.clone(), SLOW).await;
            let one = async {
                let h = t.open(&["a"]).await.unwrap();
                t.close(h, vec![]).await.unwrap();
            };
            let two = async {
                let h = t.open(&["b"]).await.unwrap();
                t.close(h, vec![]).await.unwrap();
            };
            tokio::join!(one, two);
            assert_eq!(f.max_in_close.load(Ordering::SeqCst), 1, "two closes overlapped");
            t.release().await;
        });
    }

    #[test]
    fn a_mark_must_name_a_held_table() {
        rt().block_on(async {
            let f = Arc::new(FakeFence::default());
            let t = tenure(f.clone(), SLOW).await;
            let h = t.open(&["a"]).await.unwrap();
            let bad = Watermark::Clear { table: "b".into(), source_id: "x".into() };
            assert!(t.close(h, vec![bad]).await.is_err());
            t.release().await;
        });
    }
}
