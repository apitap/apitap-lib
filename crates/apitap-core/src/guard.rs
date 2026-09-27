//! The one announce / scan / collect loop, for every destination and both lanes.
//!
//! Until 0.57.0 this loop was open-coded eleven times — seven bulk sinks and
//! four CDC destinations — and the copies drifted exactly where it mattered:
//! the BigQuery `_N` worker strip existed in the bulk lane and not in its CDC
//! twin, "mine" was deleted by three sinks and ignored by a fourth, and the
//! per-engine extra kinds (`Old` on MySQL, `New` on ClickHouse) were invisible
//! to the drains. Every decision now lives here, over a [`GuardStore`] adapter
//! that only SPELLS catalog calls: an adapter lists, creates and drops; it never
//! classifies, never decides what blocks, and never decides what is collected.
//! `no_guard_decision_outside` fails the build if a sink or destination module
//! starts deciding again.
//!
//! The protocol, in order:
//!
//! 1. [`announce`] creates this run's markers (`naming::compat::markers`), empty.
//!    A half-announcement is taken back: one marker without the other is worse
//!    than none, because a 0.55.1 reader sees only the staging one.
//! 2. [`check_peers`] lists, classifies (`naming::blockers`) and decides. Any
//!    blocker that cannot be collected refuses BEFORE any lease row is read.
//!    The rest are grouped by token — a dead drain has a lock and a marker, and
//!    one claim covers both — and each token is claimed, its scratch swept, and
//!    only then its markers dropped. A collector never closes the victim's
//!    lease: the row stays `collected`, so a collection that dies half way is
//!    finished by the next run instead of leaving a marker nothing can collect.
//! 3. [`release`] drops this run's markers and returns a [`Released`] proof
//!    only when every one is observed gone. `GuardStore::lease_close` takes that
//!    proof, so a lease closed while its lock still stands — the permanent
//!    wedge — does not type-check.

// Wired into the sinks and the CDC lane one engine at a time (handoff §3 steps
// 7-13); until each adapter lands, only the tests below call this module.
#![cfg_attr(not(test), allow(dead_code))]

use crate::error::{Error, Result};
use crate::naming::{self, Artifact, RunId};

/// One object a listing found, in both spellings. `canonical` is what
/// classification reads; `raw` is what is refused, dropped and printed. They
/// differ only where an engine decorates a name (BigQuery's `_N` workers, the
/// object stores' segments).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Listed {
    pub(crate) canonical: String,
    pub(crate) raw: String,
}

impl Listed {
    /// A name every engine spells one way.
    pub(crate) fn same(name: impl Into<String>) -> Self {
        let n = name.into();
        Listed { canonical: n.clone(), raw: n }
    }
}

/// The outcome of trying to take a lapsed lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Claim {
    /// This run took it; the victim's markers may go.
    Taken,
    /// The row could not be taken — its owner holds it, or renewed in time.
    Refused,
    /// There is no row at all: no record of liveness, so nothing may collect.
    Absent,
}

/// What `check_peers` does with this run's own leftovers from an earlier
/// attempt (exact token only — never a parent's, never a peer's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mine {
    Keep,
    DeleteLeftovers,
}

/// A destination's catalog, spelled. Nothing in an implementation decides.
#[async_trait::async_trait]
pub(crate) trait GuardStore: Send + Sync {
    /// The identifier byte limit: `PG_IDENT_MAX`, `MY_IDENT_MAX` or `ROOMY`.
    fn limit(&self) -> usize;
    /// The lease key AND the name a refusal gives; must equal the CDC lane's
    /// `lease_key`, or a drain and a bulk run read different liveness rows.
    fn dest_label(&self, bare: &str) -> String;
    /// The kinds a scan classifies: GUARDED plus what this engine's `prepare`
    /// also refuses on. The same set in both lanes.
    fn scan_kinds(&self) -> &'static [Artifact] {
        naming::GUARDED
    }
    /// How this engine spells a marker. Iceberg writes its lock under the
    /// staging suffix, which is what its 0.55.x runs scan for.
    fn spelling(&self, a: Artifact) -> Artifact {
        a
    }
    async fn list(&self, bare: &str, kinds: &[Artifact]) -> Result<Vec<Listed>>;
    /// Idempotent CREATE of an empty object.
    async fn create_marker(&self, raw: &str) -> Result<()>;
    /// `Ok` only when the object is observed gone (IF EXISTS / 404 count).
    async fn drop_object(&self, raw: &str) -> Result<()>;
    async fn lease_get(&self, key: &str, token: &str) -> Result<Option<crate::lease::Lease>>;
    async fn lease_claim(&self, key: &str, token: &str) -> Result<Claim>;
    /// Close the OWNER's own lease row. Only a [`Released`] opens this door.
    async fn lease_close(&self, proof: Released);
    /// Drop a collected run's tokenized scratch (the stores fill it in).
    async fn sweep_run(&self, _bare: &str, _token: &str) -> Result<()> {
        Ok(())
    }
}

/// A run's markers on one table, held until [`release`] or [`Announced::abandon`].
#[must_use = "an announcement that is not released wedges the table for the next run"]
#[derive(Debug)]
pub(crate) struct Announced {
    key: String,
    token: String,
    names: Vec<String>,
    done: bool,
}

impl Announced {
    /// The caller acknowledges it could not release, and says what is left.
    pub(crate) fn abandon(mut self) {
        self.done = true;
        eprintln!(
            "apitap: {}: could not drop {} — the next run will name it",
            self.key,
            self.names.join(", ")
        );
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }
}

impl Drop for Announced {
    fn drop(&mut self) {
        // Loud, and never an abort on an unwind: a panic already has a story.
        if !self.done && !std::thread::panicking() {
            eprintln!(
                "apitap: internal: announcement {:?} on {} dropped unreleased",
                self.names, self.key
            );
            debug_assert!(false, "Announced dropped without release/abandon");
        }
    }
}

/// Every marker of one announcement, observed gone. Constructible only here.
#[derive(Debug)]
pub(crate) struct Released {
    pub(crate) key: String,
    pub(crate) token: String,
    _priv: (),
}

/// Create this run's markers on `bare`, empty. Nothing is listed here: the
/// scan that decides must be taken AFTER the announcement it protects.
pub(crate) async fn announce(s: &dyn GuardStore, bare: &str, run: &RunId) -> Result<Announced> {
    let mut names: Vec<String> = Vec::new();
    for &a in naming::compat::markers(run.kind()) {
        let n = naming::artifact_ident_run(bare, s.spelling(a), s.limit(), run);
        if let Err(e) = s.create_marker(&n).await {
            // A half-announcement is worse than none: a 0.55.1 reader would see
            // the marker it scans for and nothing else, or nothing at all.
            for d in &names {
                let _ = s.drop_object(d).await;
            }
            return Err(e);
        }
        names.push(n);
    }
    Ok(Announced { key: s.dest_label(bare), token: run.token().into(), names, done: false })
}

/// Scan `bare`'s neighbours and decide: proceed, collect the provably dead, or
/// refuse. See the module doc for the order and why each step is where it is.
pub(crate) async fn check_peers(s: &dyn GuardStore, bare: &str, run: &RunId, mine: Mine) -> Result<()> {
    let kinds = s.scan_kinds();
    let listed = s.list(bare, kinds).await?;
    let scan = naming::blockers(
        bare,
        s.limit(),
        run,
        kinds,
        listed.iter().map(|l| (l.canonical.as_str(), l.raw.as_str())),
    );
    let dest = s.dest_label(bare);
    let now = naming::now_unix();
    // 1. An uncollectable blocker refuses before ANY lease row is read or
    //    claimed: no dead drain is claimed on the way to a refusal this run
    //    would issue anyway.
    if let Some(b) = scan.blockers.iter().find(|b| naming::collectable(b).is_none()) {
        return Err(naming::blocker_error(&dest, b, now, None));
    }
    // 2. One decision per TOKEN: a dead drain has two markers, one claim.
    let mut by_tok: std::collections::BTreeMap<String, Vec<&naming::Blocker>> = Default::default();
    for b in &scan.blockers {
        let tok = naming::collectable(b).expect("every uncollectable blocker refused above");
        by_tok.entry(tok.to_string()).or_default().push(b);
    }
    for (tok, bs) in by_tok {
        let lease = s.lease_get(&dest, &tok).await?;
        let Some(l) = lease.as_ref().filter(|l| l.lapsed()) else {
            return Err(naming::blocker_error(&dest, bs[0], now, lease.as_ref()));
        };
        match s.lease_claim(&dest, &tok).await? {
            Claim::Taken => {}
            Claim::Refused => return Err(naming::blocker_error(&dest, bs[0], now, Some(l))),
            Claim::Absent => return Err(naming::blocker_error(&dest, bs[0], now, None)),
        }
        // Scratch before markers. If the sweep fails the markers still stand,
        // the lease stays collected (a claim reads `collected` as lapsed), and
        // the next run retries the whole step. The other order would leak the
        // scratch for ever: it is not in GUARDED, and nothing reaps on age.
        s.sweep_run(bare, &tok).await?;
        let mut failed = Vec::new();
        for b in &bs {
            if s.drop_object(b.name()).await.is_err() {
                failed.push(b.name());
            }
        }
        if !failed.is_empty() {
            return Err(Error::Transfer(format!(
                "{dest}: took the lapsed claim of run {tok} but could not drop {}; the claim \
                 stays taken and the next run finishes the collection",
                failed.join(", ")
            )));
        }
        for b in &bs {
            eprintln!(
                "apitap: {dest}: collected {} — the run that wrote it stopped renewing its \
                 claim on this destination's own clock. Resuming.",
                b.name()
            );
        }
        // No lease_close here. The victim's row stays `collected`: "no row"
        // would be ambiguous to every fence that reads it.
    }
    if mine == Mine::DeleteLeftovers {
        for raw in &scan.mine_leftovers {
            s.drop_object(raw).await?;
        }
    }
    Ok(())
}

/// Drop this run's markers. The proof comes back only when every one is gone;
/// otherwise the announcement comes back, for the caller to abandon (loudly).
pub(crate) async fn release(
    s: &dyn GuardStore,
    mut a: Announced,
) -> std::result::Result<Released, Announced> {
    let mut all = true;
    for n in &a.names {
        if s.drop_object(n).await.is_err() {
            all = false;
        }
    }
    if !all {
        return Err(a);
    }
    a.done = true;
    Ok(Released { key: a.key.clone(), token: a.token.clone(), _priv: () })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::Lease;
    use crate::naming::{artifact_ident_run, BulkKind, PG_IDENT_MAX};
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::sync::Mutex;

    /// An in-memory catalog: a name set, a lease map, a call log, and the
    /// failures a test asks for.
    #[derive(Default)]
    struct FakeStore {
        names: Mutex<BTreeSet<String>>,
        leases: Mutex<BTreeMap<String, Lease>>,
        log: Mutex<Vec<String>>,
        fail_create: Mutex<HashSet<String>>,
        fail_drop: Mutex<HashSet<String>>,
        /// canonical -> raw, for a store that decorates names
        decorate: Mutex<BTreeMap<String, String>>,
    }

    impl FakeStore {
        fn with(names: &[&str]) -> Self {
            let s = FakeStore::default();
            s.names.lock().unwrap().extend(names.iter().map(|n| n.to_string()));
            s
        }
        fn lease(&self, tok: &str, expires_in: i64, collected: bool) {
            self.leases.lock().unwrap().insert(tok.into(), Lease { expires_in, collected });
        }
        fn note(&self, what: String) {
            self.log.lock().unwrap().push(what);
        }
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
        fn count(&self, prefix: &str) -> usize {
            self.log().iter().filter(|l| l.starts_with(prefix)).count()
        }
        fn at(&self, prefix: &str) -> Vec<usize> {
            self.log().iter().enumerate().filter(|(_, l)| l.starts_with(prefix)).map(|(i, _)| i).collect()
        }
        fn has(&self, n: &str) -> bool {
            self.names.lock().unwrap().contains(n)
        }
    }

    #[async_trait::async_trait]
    impl GuardStore for FakeStore {
        fn limit(&self) -> usize {
            PG_IDENT_MAX
        }
        fn dest_label(&self, bare: &str) -> String {
            format!("public.{bare}")
        }
        async fn list(&self, _bare: &str, _kinds: &[Artifact]) -> Result<Vec<Listed>> {
            self.note("list".into());
            let dec = self.decorate.lock().unwrap().clone();
            Ok(self
                .names
                .lock()
                .unwrap()
                .iter()
                .map(|n| match dec.iter().find(|(_, raw)| *raw == n) {
                    Some((canonical, raw)) => Listed { canonical: canonical.clone(), raw: raw.clone() },
                    None => Listed::same(n.clone()),
                })
                .collect())
        }
        async fn create_marker(&self, raw: &str) -> Result<()> {
            self.note(format!("create {raw}"));
            if self.fail_create.lock().unwrap().contains(raw) {
                return Err(Error::Transfer(format!("create {raw}: injected")));
            }
            self.names.lock().unwrap().insert(raw.into());
            Ok(())
        }
        async fn drop_object(&self, raw: &str) -> Result<()> {
            self.note(format!("drop {raw}"));
            if self.fail_drop.lock().unwrap().contains(raw) {
                return Err(Error::Transfer(format!("drop {raw}: injected")));
            }
            self.names.lock().unwrap().remove(raw);
            Ok(())
        }
        async fn lease_get(&self, _key: &str, token: &str) -> Result<Option<Lease>> {
            self.note(format!("lease_get {token}"));
            Ok(self.leases.lock().unwrap().get(token).map(|l| Lease { ..*l }))
        }
        async fn lease_claim(&self, _key: &str, token: &str) -> Result<Claim> {
            self.note(format!("lease_claim {token}"));
            let mut ls = self.leases.lock().unwrap();
            Ok(match ls.get_mut(token) {
                None => Claim::Absent,
                Some(l) if l.lapsed() => {
                    l.collected = true;
                    Claim::Taken
                }
                Some(_) => Claim::Refused,
            })
        }
        async fn lease_close(&self, proof: Released) {
            self.note(format!("lease_close {}", proof.token));
            self.leases.lock().unwrap().remove(&proof.token);
        }
        async fn sweep_run(&self, _bare: &str, token: &str) -> Result<()> {
            self.note(format!("sweep_run {token}"));
            Ok(())
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    fn names_of(r: &RunId) -> (String, String) {
        (artifact_ident_run("orders", Artifact::Lock, PG_IDENT_MAX, r),
         artifact_ident_run("orders", Artifact::Staging, PG_IDENT_MAX, r))
    }

    /// G1. The scan that decides is taken after every marker of this run exists.
    #[test]
    fn announce_before_any_list() {
        rt().block_on(async {
            let s = FakeStore::default();
            let run = RunId::mint_drain("postgres://h/db");
            let a = announce(&s, "orders", &run).await.unwrap();
            check_peers(&s, "orders", &run, Mine::Keep).await.unwrap();
            let creates = s.at("create ");
            assert_eq!(creates.len(), 2, "a drain announces a lock and a marker: {:?}", s.log());
            assert!(s.at("list")[0] > *creates.last().unwrap(), "{:?}", s.log());
            release(&s, a).await.unwrap();
        });
    }

    /// G2. When the second marker fails, the first is taken back.
    #[test]
    fn half_announcement_is_taken_back() {
        rt().block_on(async {
            let s = FakeStore::default();
            let run = RunId::mint_drain("postgres://h/db");
            let (lock, marker) = names_of(&run);
            s.fail_create.lock().unwrap().insert(marker.clone());
            assert!(announce(&s, "orders", &run).await.is_err());
            assert!(!s.has(&lock), "the lock alone must not stand: {:?}", s.log());
            assert_eq!(s.count(&format!("drop {lock}")), 1, "{:?}", s.log());
        });
    }

    /// G3. A blocker nothing can collect refuses before any lease row is read.
    #[test]
    fn uncollectable_refuses_before_any_lease_read() {
        rt().block_on(async {
            let dead = RunId::mint_drain("postgres://other/db");
            let bulk = RunId::mint_bulk(BulkKind::Swap, "postgres://third/db");
            let (dead_lock, _) = names_of(&dead);
            let (_, bulk_staging) = names_of(&bulk);
            let s = FakeStore::with(&[&dead_lock, &bulk_staging]);
            s.lease(dead.token(), -60, false);
            let run = RunId::mint_bulk(BulkKind::Swap, "postgres://h/db");
            let e = check_peers(&s, "orders", &run, Mine::DeleteLeftovers).await.unwrap_err();
            assert!(format!("{e}").contains("locked:"), "{e}");
            assert_eq!(s.count("lease_get"), 0, "{:?}", s.log());
            assert_eq!(s.count("lease_claim"), 0, "{:?}", s.log());
            assert!(s.has(&dead_lock), "nothing is collected on the way to a refusal");
        });
    }

    /// G4. A dead drain's lock and marker: one claim, one sweep, two drops, and
    /// the victim's lease is never closed by the collector.
    #[test]
    fn one_claim_per_token() {
        rt().block_on(async {
            let dead = RunId::mint_drain("postgres://other/db");
            let (lock, marker) = names_of(&dead);
            let s = FakeStore::with(&[&lock, &marker]);
            s.lease(dead.token(), -60, false);
            let run = RunId::mint_bulk(BulkKind::Swap, "postgres://h/db");
            check_peers(&s, "orders", &run, Mine::DeleteLeftovers).await.unwrap();
            assert_eq!(s.count("lease_claim"), 1, "{:?}", s.log());
            assert_eq!(s.count("sweep_run"), 1, "{:?}", s.log());
            assert_eq!(s.count("drop "), 2, "{:?}", s.log());
            assert_eq!(s.count("lease_close"), 0, "{:?}", s.log());
            assert!(!s.has(&lock) && !s.has(&marker));
            assert!(s.leases.lock().unwrap()[dead.token()].collected, "the row stays, collected");
        });
    }

    /// G5. A collection that dies half way is finished by the next run — which
    /// only works because the victim's row is still there, `collected`.
    #[test]
    fn partial_drop_keeps_the_claim() {
        rt().block_on(async {
            let dead = RunId::mint_drain("postgres://other/db");
            let (lock, marker) = names_of(&dead);
            let s = FakeStore::with(&[&lock, &marker]);
            s.lease(dead.token(), -60, false);
            s.fail_drop.lock().unwrap().insert(marker.clone());
            let run = RunId::mint_bulk(BulkKind::Swap, "postgres://h/db");
            let e = check_peers(&s, "orders", &run, Mine::DeleteLeftovers).await.unwrap_err();
            assert!(format!("{e}").contains("next run finishes the collection"), "{e}");
            assert!(s.leases.lock().unwrap()[dead.token()].collected);
            assert!(s.has(&marker), "the marker still stands");
            s.fail_drop.lock().unwrap().clear();
            check_peers(&s, "orders", &run, Mine::DeleteLeftovers).await
                .expect("the next run re-claims the collected row and finishes");
            assert!(!s.has(&lock) && !s.has(&marker), "{:?}", s.names.lock().unwrap());
        });
    }

    /// G6. The victim's scratch goes before its markers.
    #[test]
    fn sweep_before_markers() {
        rt().block_on(async {
            let dead = RunId::mint_drain("postgres://other/db");
            let (lock, marker) = names_of(&dead);
            let s = FakeStore::with(&[&lock, &marker]);
            s.lease(dead.token(), -60, false);
            let run = RunId::mint_bulk(BulkKind::Swap, "postgres://h/db");
            check_peers(&s, "orders", &run, Mine::Keep).await.unwrap();
            let (claim, sweep, drops) = (s.at("lease_claim")[0], s.at("sweep_run")[0], s.at("drop "));
            assert!(claim < sweep && sweep < drops[0], "{:?}", s.log());
        });
    }

    /// G7. The proof exists only when every marker is gone.
    #[test]
    fn release_proof_only_when_all_gone() {
        rt().block_on(async {
            let run = RunId::mint_drain("postgres://h/db");
            let (_, marker) = names_of(&run);
            let s = FakeStore::default();
            let a = announce(&s, "orders", &run).await.unwrap();
            s.fail_drop.lock().unwrap().insert(marker.clone());
            let back = release(&s, a).await.expect_err("a marker still stands");
            assert_eq!(back.names().len(), 2);
            s.fail_drop.lock().unwrap().clear();
            let proof = release(&s, back).await.expect("all gone now");
            assert_eq!((proof.key.as_str(), proof.token.as_str()), ("public.orders", run.token()));
            s.lease_close(proof).await;
            assert_eq!(s.count("lease_close"), 1);
        });
    }

    /// I9: the only quiet way to give up an announcement is to say so. A
    /// release that could not drop everything hands the announcement back,
    /// and `abandon` consumes it without tripping the drop-unreleased assert.
    #[test]
    fn an_unreleasable_announcement_is_abandoned_not_forgotten() {
        rt().block_on(async {
            let run = RunId::mint_drain("postgres://h/db");
            let (lock, _) = names_of(&run);
            let s = FakeStore::default();
            let a = announce(&s, "orders", &run).await.unwrap();
            s.fail_drop.lock().unwrap().insert(lock.clone());
            let back = release(&s, a).await.expect_err("the lock would not drop");
            back.abandon();
            assert!(s.has(&lock), "abandoning names what is left; it deletes nothing");
            assert_eq!(s.count("lease_close"), 0, "no proof, so no lease close");
        });
    }

    /// A store that decorates names (BigQuery's `_N` workers): the run's own
    /// leftover is dropped by the RAW name it was listed as.
    #[test]
    fn own_leftovers_are_dropped_by_raw_name() {
        rt().block_on(async {
            let run = RunId::mint_bulk(BulkKind::Swap, "postgres://h/db");
            let (_, staging) = names_of(&run);
            let raw = format!("{staging}_3");
            let s = FakeStore::with(&[&raw]);
            s.decorate.lock().unwrap().insert(staging.clone(), raw.clone());
            check_peers(&s, "orders", &run, Mine::DeleteLeftovers).await.unwrap();
            assert_eq!(s.at(&format!("drop {raw}")).len(), 1, "{:?}", s.log());
            assert!(!s.has(&raw));
        });
    }

    /// G9. Every guard decision lives in this module. The allow-list is the
    /// files that still carry their own loop, and it may only shrink: a listed
    /// file that stops deciding must leave the list in the same commit.
    #[test]
    fn no_guard_decision_outside() {
        const DECISIONS: &[&str] =
            &["naming::classify(", "naming::blockers(", "naming::collectable(", "peer_blocks("];
        // Shrinks by one file per adapter (handoff §3 steps 7-13) and is empty
        // once the CDC lane is on this module.
        const NOT_YET: &[&str] = &[
            "sink/clickhouse.rs",
            "sink/bigquery.rs",
            "sink/s3.rs",
            "sink/gcs.rs",
            "sink/iceberg.rs",
            "logbased/dest_my.rs",
            "logbased/dest_ch.rs",
            "logbased/dest_bq.rs",
        ];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut deciding = Vec::new();
        for dir in ["sink", "logbased"] {
            for e in std::fs::read_dir(src.join(dir)).unwrap() {
                let p = e.unwrap().path();
                if p.extension().is_some_and(|x| x == "rs") {
                    let text = std::fs::read_to_string(&p).unwrap();
                    if DECISIONS.iter().any(|d| text.contains(d)) {
                        deciding.push(format!("{dir}/{}", p.file_name().unwrap().to_string_lossy()));
                    }
                }
            }
        }
        deciding.sort();
        let mut allowed: Vec<String> = NOT_YET.iter().map(|s| s.to_string()).collect();
        allowed.sort();
        assert_eq!(deciding, allowed,
                   "guard decisions outside guard.rs must match the shrinking allow-list exactly");
    }
}
