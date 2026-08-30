//! Every identifier apitap creates next to a user's table, in one place.
//!
//! # Why this module exists
//!
//! Names used to be built where they were needed: `format!("{bare}__apitap_
//! staging")` in the Postgres sink, the same string again in the MySQL sink,
//! `{dest_table}__apitap_cl` in two destinations, and so on. Nine distinct
//! suffixes across the tree. Nothing enumerated them, so nothing could check
//! anything about them as a set — and three separate defects came out of that
//! one gap:
//!
//! * **Truncation.** Postgres cuts identifiers at 63 bytes without complaint,
//!   so a 63-character destination produced a staging name identical to the
//!   destination. `prepare`'s unconditional DROP then aimed at the destination
//!   itself, and the run reported failure *after* replacing it.
//! * **Namespace drift.** `dispatch` reserved exactly two of the nine suffixes,
//!   so a user table named like any of the other seven artifacts would be
//!   silently destroyed by a sibling's run.
//! * **Patch-by-patch discovery.** The truncation was fixed for
//!   `__apitap_staging`, and only then did a test reveal `__apitap_old`
//!   overflowing too. There was no reason to believe there was not a third.
//!
//! The fix is not another careful `format!`. It is making the SET of artifacts
//! a thing the compiler knows about: [`Artifact::ALL`] is the source of truth,
//! every name goes through [`artifact_ident`], and the invariants are tested
//! across the whole enum rather than on the examples someone thought of. A new
//! artifact gets length safety, namespace reservation and discovery-exclusion
//! by existing, not by someone remembering three call sites.

use md5::Digest;

/// An object apitap creates beside a destination table.
///
/// **Adding a variant is the only supported way to add an artifact.** The
/// namespace reservation in `pipeline::dispatch`, the exclusion lists in table
/// discovery, and the length-safety tests all iterate [`Artifact::ALL`], so a
/// new one is covered the moment it is declared — and cannot be half-covered,
/// which is the state the tree was in before this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Artifact {
    /// Rows land here first; the swap renames it onto the destination.
    Staging,
    /// MySQL parks the outgoing table here between its two RENAMEs.
    Old,
    /// ClickHouse's swap target for a replace.
    New,
    /// Changelog scratch table for a window's apply.
    ChangelogTmp,
    /// The delete-marker sidecar the ClickHouse CDC apply keeps.
    CdcDelete,
    /// The view that derives current state from a changelog table.
    Current,
}

impl Artifact {
    /// Every artifact kind. Iterate this; never write a literal list.
    pub(crate) const ALL: &'static [Artifact] = &[
        Artifact::Staging,
        Artifact::Old,
        Artifact::New,
        Artifact::ChangelogTmp,
        Artifact::CdcDelete,
        Artifact::Current,
    ];

    /// May apitap claim this suffix as its own?
    ///
    /// Every suffix but one contains the word `apitap`, which makes it a
    /// namespace: a user table ending in `__apitap_staging` is a collision
    /// worth refusing, and hiding such a name from discovery cannot hide
    /// anybody's real data.
    ///
    /// `__current` is different and must not be treated the same way. It is
    /// not namespaced, `orders__current` is a perfectly ordinary thing for a
    /// person to call a table, and reserving it would refuse to transfer real
    /// data. It also does not need reserving: the object apitap creates there
    /// is a VIEW, and discovery already lists only base tables and
    /// materialized views.
    ///
    /// The distinction is the reason this is a method and not a blanket rule
    /// over `ALL` — a hand-written list would have gotten it wrong in one
    /// direction or the other, and this one is wrong in the direction that
    /// loses data.
    pub(crate) const fn reserved(self) -> bool {
        !matches!(self, Artifact::Current)
    }

    pub(crate) const fn suffix(self) -> &'static str {
        match self {
            Artifact::Staging => "__apitap_staging",
            Artifact::Old => "__apitap_old",
            Artifact::New => "__apitap_new",
            Artifact::ChangelogTmp => "__apitap_cl",
            Artifact::CdcDelete => "__apitap_cdc_del",
            Artifact::Current => "__current",
        }
    }
}

/// Identifier byte limits per dialect. Postgres is `NAMEDATALEN - 1`; MySQL
/// allows 64 bytes for a table name. ClickHouse and BigQuery are far longer
/// than anything reachable here, so they use [`ROOMY`].
pub(crate) const PG_IDENT_MAX: usize = 63;
pub(crate) const MY_IDENT_MAX: usize = 64;
pub(crate) const ROOMY: usize = 1024;

/// Bytes of md5 hex mixed in when a name has to be shortened.
const HASH: usize = 8;

/// `<bare><suffix>`, guaranteed to fit `limit` and to stay distinct.
///
/// When the whole name fits, it is exactly `bare + suffix` — the shape every
/// existing deployment already has on disk, so this is not a migration.
///
/// When it does not fit, the BARE part is what gives way. That direction is
/// deliberate and load-bearing: the suffix is what makes an object recognisable
/// as apitap's, which is what the sweeps, the namespace reservation and the
/// discovery-exclusion all match on. Truncating the suffix instead — which is
/// what the databases do on their own — is precisely how a staging name came to
/// equal the table it was staging.
pub(crate) fn artifact_ident(bare: &str, artifact: Artifact, limit: usize) -> String {
    let suffix = artifact.suffix();
    if bare.len() + suffix.len() <= limit {
        return format!("{bare}{suffix}");
    }
    // `limit` counts bytes and `bare` may be UTF-8, so cut on a char boundary.
    let room = limit.saturating_sub(suffix.len() + HASH + 1);
    let mut head = String::with_capacity(room);
    for c in bare.chars() {
        if head.len() + c.len_utf8() > room {
            break;
        }
        head.push(c);
    }
    // Hash the suffix in too: two artifacts of the same over-long table share a
    // head, and only the suffix would tell them apart otherwise.
    let h = hex::encode(md5::Md5::digest(format!("{bare}{suffix}").as_bytes()));
    format!("{head}_{}{suffix}", &h[..HASH])
}

// ───────────────────────────────────────────────────────────────────────────
// Run identity
// ───────────────────────────────────────────────────────────────────────────

/// Everything about a run that a CONCURRENT run needs to know, packed into
/// something a table name can carry.
///
/// # Why this is in the name rather than beside it
///
/// Two runs of one destination table share a staging object today, and the
/// interleave is destructive: A streams its rows in, B's `prepare` drops that
/// object and creates a fresh one, and A's `finalize` publishes B's empty table
/// over the destination — returning A's row count as a success. A green run and
/// an empty table.
///
/// The obvious fixes both fail on the destinations that need them most.
/// Advisory locks exist on Postgres and MySQL and nowhere else; ClickHouse,
/// BigQuery and the object stores have no lock to take and no transaction that
/// spans the statements a publish needs. A stamp written *beside* the staging
/// object has to be read and checked before publishing, and on those same
/// engines nothing can check-and-publish atomically, so the check is always
/// TOCTOU.
///
/// Putting the identity in the NAME needs neither. A run can only publish an
/// object whose name it minted, and that is enforced by the object system
/// itself — on every engine, with no extra round trip and no atomicity
/// requirement. What the peer listing then buys is not exclusion but a *loud
/// refusal*: seeing another run's live staging is how a `replace` learns it
/// should not proceed, rather than discovering it at the swap.
///
/// # What the token carries, and why each part is there
///
/// `_` + 7 (start) + 1 (mode) + 4 (source) + 3 (nonce) = 16 bytes, fixed width,
/// no inner separator, so it parses positionally from any name.
///
/// * **start**, base36 unix seconds. Postgres stores no table creation time —
///   MySQL has `create_time`, ClickHouse `metadata_modification_time`, BigQuery
///   `creationTime`, object stores their own — so the one place an age is
///   readable on EVERY engine is the name. It is what lets a sweep tell a
///   crashed run's leftovers from a live run's staging without asking the
///   catalog anything.
/// * **mode**, one letter. A `replace` publishes by swapping the whole table,
///   which cannot coexist with anything; an `append` adds rows, which can.
///   Refusing both alike would have shipped a regression dressed as a fix.
/// * **source**, 4 base36 chars of the source identity. This is the part a
///   first draft of the design left out, and it is the difference between a fix
///   and a duplicate-row bug: two `append` runs of the SAME (source, table)
///   pair both read watermark W and land the same delta twice, while two
///   appends from DIFFERENT sources into one table are the fan-in the manual
///   advertises. Only the source tells those apart.
/// * **nonce**, so two runs that start in the same second with the same mode
///   and source still mint different names. Within one process it is a
///   COUNTER, so uniqueness there is guaranteed rather than probable — a
///   collision is the exact bug this whole mechanism exists to prevent, and
///   "unlikely" is not the right guarantee for it. Across processes a
///   per-process base offsets the counter, so two processes collide only if
///   their offset-plus-counter land on the same value in the same second on
///   the same table, which is 1 in 36^4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunId {
    token: String,
}

/// How a run's landing operation behaves towards a concurrent one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LandKind {
    /// Swaps the whole table. Exclusive with everything, including itself.
    Swap,
    /// Adds rows against a watermark. Safe beside a DIFFERENT source; not
    /// beside the same one, which would read the same watermark twice.
    Incremental,
    /// A CDC drain. Exclusive: it owns a replication slot and a watermark.
    Cdc,
}

impl LandKind {
    const fn letter(self) -> char {
        match self {
            LandKind::Swap => 'r',
            LandKind::Incremental => 'a',
            LandKind::Cdc => 'l',
        }
    }

    fn from_letter(c: char) -> Option<Self> {
        match c {
            'r' => Some(LandKind::Swap),
            'a' => Some(LandKind::Incremental),
            'l' => Some(LandKind::Cdc),
            _ => None,
        }
    }
}

/// Total token width, including its leading `_`.
pub(crate) const RUN_TOKEN_LEN: usize = 16;

fn base36(mut n: u64, width: usize) -> String {
    const D: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];
    for i in (0..width).rev() {
        out[i] = D[(n % 36) as usize];
        n /= 36;
    }
    String::from_utf8(out).expect("base36 digits are ascii")
}

impl RunId {
    /// Mint one identity for one `transfer()` call.
    ///
    /// Deliberately NOT a process-wide global: two `apitap.transfer()` calls in
    /// one Python process are two runs and must not share a token. (The
    /// progress counters already have that bug and it is documented; this is
    /// not the place to add a second instance of it.)
    pub(crate) fn mint(kind: LandKind, source_id: &str) -> Self {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let src = u64::from_str_radix(
            &hex::encode(md5::Md5::digest(source_id.as_bytes()))[..8],
            16,
        )
        .unwrap_or(0);
        // A COUNTER, not a hash of the clock. A hash gives collisions at the
        // birthday rate — 500 mints into 36^3 values collide 93% of the time,
        // which a test found immediately — and a collision here means two runs
        // share a staging object, which is the defect this mechanism exists to
        // prevent. Inside one process the counter makes that impossible.
        //
        // The per-process base is what separates processes: without it every
        // process would start at 0 and two of them would mint the same
        // sequence. It is derived once from the clock and the pid, which is
        // enough entropy for a name and needs no RNG dependency.
        static BASE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let base = *BASE.get_or_init(|| {
            let ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64)
                .unwrap_or(0);
            ns.wrapping_mul(0x9E37_79B9).wrapping_add(std::process::id() as u64)
        });
        let nonce = base.wrapping_add(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        RunId {
            token: format!(
                "_{}{}{}{}",
                base36(secs, 7),
                kind.letter(),
                base36(src, 3),
                base36(nonce, 4),
            ),
        }
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

/// What a run can read off a PEER's artifact name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerRun {
    pub(crate) started_unix: u64,
    pub(crate) kind: LandKind,
    /// The peer's source identity, hashed. Comparable, not reversible — which
    /// is all the decision needs, and means a source URL never appears in a
    /// table name.
    pub(crate) source_hash: String,
    /// The whole token, so a run can recognise its OWN artifacts. One `RunId`
    /// is minted per dispatch and shared by every table in a multi-table run,
    /// so "is this mine?" is a question every sink has to be able to ask.
    pub(crate) token: String,
}

/// Read a token out of a name that carries one, or `None` for a name that does
/// not (an artifact from before this existed, or something that merely ends the
/// same way).
pub(crate) fn parse_peer(name: &str, artifact: Artifact) -> Option<PeerRun> {
    let head = name.strip_suffix(artifact.suffix())?;
    if head.len() < RUN_TOKEN_LEN {
        return None;
    }
    let tok = &head[head.len() - RUN_TOKEN_LEN..];
    let b = tok.as_bytes();
    if b[0] != b'_' {
        return None;
    }
    let started = u64::from_str_radix(&tok[1..8], 36).ok()?;
    let kind = LandKind::from_letter(tok.as_bytes()[8] as char)?;
    Some(PeerRun {
        started_unix: started,
        kind,
        // 3 base36 chars = 46,656 buckets. Two different sources sharing a
        // bucket makes a fan-in pair refuse each other — an error in the safe
        // direction (refuse, never corrupt), which is the only direction a
        // hash this short is allowed to be wrong in.
        source_hash: tok[9..12].to_string(),
        token: tok.to_string(),
    })
}

/// May `mine` proceed while `peer` is running?
///
/// The matrix, and the reasoning for each cell:
///
/// * A **swap** cannot coexist with anything, including another swap: it
///   replaces the whole table, so whatever the other run lands is thrown away
///   or throws this one away.
/// * A **CDC drain** cannot coexist with anything either: it owns a
///   replication slot and a watermark, and two drains of one destination fight
///   over both.
/// * Two **incremental** runs are the interesting cell, and the one a first
///   draft got wrong. From DIFFERENT sources they are the fan-in the manual
///   advertises — independent watermarks, per `_apitap_state`'s
///   `(dest_table, source_id)` key — and refusing them would remove a
///   documented capability. From the SAME source they both read watermark W
///   and both land every row past it: duplicate rows, silently, which
///   `usage.md` warns about in prose and nothing enforced.
pub(crate) fn peer_blocks(mine: &PeerRun, peer: &PeerRun) -> bool {
    use LandKind::*;
    match (mine.kind, peer.kind) {
        (Swap, _) | (_, Swap) => true,
        (Cdc, _) | (_, Cdc) => true,
        (Incremental, Incremental) => mine.source_hash == peer.source_hash,
    }
}

/// What a name found next to a destination table means to THIS run.
///
/// # Why this is one function and not one per sink
///
/// The first pass implemented this classification separately in seven sinks.
/// Six of them got it wrong, in six different ways, and the review found every
/// one: a pattern anchored at only one end reaped a *sibling table's* staging;
/// a run's own artifacts read as a foreign peer and refused the run that made
/// them; a horizon check placed before the ownership check deleted a live
/// peer's work. None of those were careless — they are all the same shape of
/// mistake, which is what a rule reimplemented seven times produces.
///
/// So the rule lives here, once, with the ordering that matters written down:
///
/// 1. **Is it even ours?** The token is fixed width and sits between a known
///    head and a known suffix, so the name's LENGTH is exact. Anchoring on
///    only `starts_with`/`ends_with` lets `orders` match
///    `orders_items<token>__apitap_staging` — a different table's workspace,
///    which a reap would then delete.
/// 2. **Is it MINE?** One `RunId` is minted per dispatch and shared by every
///    table in a multi-table run. Without this check a long run can outlive the
///    reap horizon and collect its own sibling's live staging, or read it as a
///    peer and refuse itself.
/// 3. **Is it dead?** Only then does age matter.
/// 4. Otherwise it is a live peer, and [`peer_blocks`] decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Found {
    /// Not this table's artifact at all — a sibling's, or someone else's table.
    /// Leave it alone.
    Foreign,
    /// This run's own. Never reaped, never blocking.
    Mine,
    /// A crashed run's leftover: older than the horizon, or an un-tokenized
    /// name from before this mechanism existed. Safe to collect.
    Dead,
    /// A live run's workspace.
    Live(PeerRun),
}

/// Classify one name found beside `bare`'s destination.
///
/// `now` is passed in rather than read here so a test can place a name at any
/// age without touching the clock.
pub(crate) fn classify(
    name: &str,
    bare: &str,
    artifact: Artifact,
    limit: usize,
    mine: &RunId,
    now: u64,
) -> Found {
    let (head, suffix) = artifact_match(bare, artifact, limit);
    // The un-tokenized name an older apitap would have written. It carries no
    // age, so it can never be aged out — but leaving it forever would break the
    // cleanup these sinks have always promised, and a concurrent old-version
    // run is the very bug being fixed.
    if name == artifact_ident(bare, artifact, limit) {
        return Found::Dead;
    }
    // Exact length is the anchor. Both ends matching is not enough.
    if name.len() != head.len() + RUN_TOKEN_LEN + suffix.len()
        || !name.starts_with(head.as_str())
        || !name.ends_with(suffix)
    {
        return Found::Foreign;
    }
    let Some(peer) = parse_peer(name, artifact) else {
        return Found::Foreign;
    };
    if peer.token == mine.token() {
        return Found::Mine;
    }
    // NOT aged out. This is where an age check used to be, and removing it is
    // the most important line in this module.
    //
    // The token carries the time the RUN started, which is not the age of the
    // object: an artifact is created at or after that moment, so `now - token`
    // is an UPPER bound on its age and can never prove it is old. For the
    // multi-hour loads this engine exists for, the two differ by the whole
    // duration of the load — table N of a long multi-table run mints its
    // artifact under a token that is already hours old, and a concurrent run
    // would then "collect" a workspace that is being written to. On Postgres
    // the victim dies loudly at its next statement; on BigQuery and the object
    // stores the deleted staging is silently re-created and the run reports a
    // full row count over a truncated table — which is the exact defect this
    // whole mechanism exists to remove, reintroduced one horizon later.
    //
    // So: refusing is the safe action and collecting is the dangerous one, and
    // they get different standards of proof. A live-looking artifact is
    // refused; only something PROVABLY dead is removed, and a timestamp that
    // records the wrong event proves nothing. Automatic collection needs a
    // liveness signal from the engine itself — an object mtime that advances
    // as the run writes, a catalog lock — which each engine has and each
    // spells differently; that is deliberate follow-up work rather than
    // something to guess at here.
    //
    // What this costs: a crashed run's artifact is not collected on its own,
    // and later runs of that table refuse until someone removes it. That is a
    // real operational cost, and it is the one the error message names in its
    // last sentence. It is the right trade against silently truncating a live
    // run's data.
    let _ = now;
    Found::Live(peer)
}

/// Seconds since the epoch, for [`classify`].
pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The refusal one run gives when another already holds the table.
///
/// Here rather than in each sink because it was written seven times and drifted
/// immediately: one copy told the operator their leftover would be "reaped
/// automatically after 0s" when `APITAP_STAGING_REAP_SECS=0` means reaping is
/// switched off entirely — advice that would have them waiting for a cleanup
/// that never comes. A message an operator reads at 3am is an interface, and
/// interfaces do not get seven implementations.
///
/// It names four things, because each answers a question the reader will
/// otherwise have to guess at: WHO holds the table, WHAT they are doing, WHY
/// the two cannot share it, and WHAT to do next.
pub(crate) fn locked_error(
    dest: &str,
    artifact_name: &str,
    mine: &PeerRun,
    peer: &PeerRun,
    now: u64,
) -> crate::error::Error {
    let age = now.saturating_sub(peer.started_unix);
    let doing = match peer.kind {
        LandKind::Swap => "replacing the whole table",
        LandKind::Incremental => "appending to it",
        LandKind::Cdc => "draining changes into it",
    };
    let why = match (mine.kind, peer.kind) {
        (LandKind::Swap, _) | (_, LandKind::Swap) =>
            "a replace swaps the whole table, so whichever finishes second \
             throws the other's work away",
        (LandKind::Cdc, _) | (_, LandKind::Cdc) =>
            "a CDC drain owns the watermark and the replication slot, and two \
             of them cannot share either",
        _ =>
            "both read the same watermark and would land the same rows twice",
    };
    // No promise of automatic collection, because there is none: see
    // `classify`. Naming the object is the whole recovery instruction, so it
    // has to be exact.
    let stale = format!(
        "If that run is dead rather than slow, remove {artifact_name} and \
         re-run — nothing collects it on its own, because a timestamp cannot \
         tell a crashed run from a slow one."
    );
    crate::error::Error::Locked(format!(
        "{dest}: another apitap run is already loading this table — it started \
         {age}s ago and is {doing}. They cannot share one destination: {why}. \
         Run them one at a time; a scheduler's own concurrency setting is the \
         usual answer (Airflow max_active_runs=1, a cron flock). {stale}"
    ))
}

/// `<head><token><suffix>`, inside `limit`.
///
/// The token goes BEFORE the suffix so the suffix stays terminal — every sweep,
/// namespace reservation and discovery exclusion in the tree matches on it, and
/// `is_artifact` still works unchanged.
///
/// Note this is NOT a prefix extension of [`artifact_ident`]: the token sits in
/// the middle, so a sweep matches `head` + wildcard + suffix rather than a
/// prefix. [`artifact_match`] returns exactly that pair, and is the only
/// supported way to build the pattern.
pub(crate) fn artifact_ident_run(
    bare: &str,
    artifact: Artifact,
    limit: usize,
    run: &RunId,
) -> String {
    let (head, suffix) = artifact_match(bare, artifact, limit);
    format!("{head}{}{suffix}", run.token())
}

/// The two fixed parts of every name this table+artifact can produce: the head
/// it always starts with, and the suffix it always ends with. A sweep is
/// `LIKE '<head>%<suffix>'`.
pub(crate) fn artifact_match(bare: &str, artifact: Artifact, limit: usize) -> (String, &'static str) {
    let suffix = artifact.suffix();
    // The token is part of the budget, so the head gives way sooner than it
    // does for an un-tokenized name. Everything else is `artifact_ident`'s
    // rule, unchanged — including hashing the suffix in, so two artifacts of
    // one over-long table stay distinct.
    let room = limit.saturating_sub(suffix.len() + RUN_TOKEN_LEN);
    if bare.len() <= room {
        return (bare.to_string(), suffix);
    }
    let keep = room.saturating_sub(HASH + 1);
    let mut head = String::with_capacity(keep);
    for c in bare.chars() {
        if head.len() + c.len_utf8() > keep {
            break;
        }
        head.push(c);
    }
    let h = hex::encode(md5::Md5::digest(format!("{bare}{suffix}").as_bytes()));
    (format!("{head}_{}", &h[..HASH]), suffix)
}

/// Reserved for the per-engine liveness work that will drive automatic
/// collection; not currently a gate on anything.
///
/// It was a gate, briefly, and the review of seven sinks is why it is not: an
/// age taken from the run's START time cannot show that an object is old (see
/// `classify`), and four destinations lose data silently when a live
/// workspace is collected. The knob stays so the name is stable when
/// collection returns with evidence behind it.
#[allow(dead_code)]
pub(crate) fn reap_horizon_secs() -> u64 {
    std::env::var("APITAP_STAGING_REAP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(3600)
}

/// Does this name look like something apitap made?
///
/// Used by table discovery and by the namespace reservation, so both answer the
/// question the same way and neither can fall behind [`Artifact::ALL`].
pub(crate) fn is_artifact(name: &str) -> bool {
    name == STATE_TABLE
        || Artifact::ALL
            .iter()
            .filter(|a| a.reserved())
            .any(|a| name.ends_with(a.suffix()))
}

/// A SQL predicate that hides every apitap artifact from table discovery.
///
/// Generated, not written out. The hand-written versions listed three of the
/// eight artifacts, so a `schema=` transfer happily picked up
/// `orders__apitap_cl`, `orders__apitap_cdc_del`, `orders__apitap_new` and
/// `orders__current` and replicated apitap's own scratch objects as if a user
/// had made them. Deriving the clause from [`Artifact::ALL`] means a new
/// artifact disappears from discovery the moment it is declared.
///
/// `_` is a LIKE wildcard and every suffix is full of them, so each one is
/// escaped — with a backslash for Postgres, and with `|` plus an explicit
/// `ESCAPE` for MySQL, which does not enable backslash escaping by default
/// under `NO_BACKSLASH_ESCAPES`.
pub(crate) fn sql_exclusion(col: &str, dialect: Dialect) -> String {
    let mut out = String::new();
    for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
        let (pat, esc) = match dialect {
            Dialect::Postgres => (a.suffix().replace('_', "\\_"), String::new()),
            Dialect::MySql => (a.suffix().replace('_', "|_"), " ESCAPE '|'".to_string()),
        };
        out.push_str(&format!(" AND {col} NOT LIKE '%{pat}'{esc}"));
    }
    out.push_str(&format!(" AND {col} <> '{STATE_TABLE}'"));
    out
}

/// Which LIKE-escaping convention to generate for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Dialect {
    Postgres,
    MySql,
}

/// The two spellings a Postgres `_apitap_state` key has historically had, and
/// which one is canonical now.
///
/// The bulk lane always keyed state rows by the schema-qualified name
/// (`public.orders`); the CDC lane keyed them by the name it was handed
/// (usually bare `orders`). Same table, same purpose, two vocabularies — and
/// the meeting points were exactly where it mattered: a bulk `replace` deleted
/// state rows under ITS spelling and missed the CDC row, so the next
/// `log_based` run resumed from a watermark that predated the replace; and a
/// mode switch read the other lane's row or missed it depending on which
/// spelling the caller used.
///
/// Nothing is rewritten. Each lane keeps the spelling it has always written —
/// the CDC lane the bare name, the bulk lane the qualified one — and every
/// READ and every DELETE covers both. That closes the defect without touching
/// a single row on anybody's disk.
///
/// Canonicalising instead was the first attempt, and it was wrong for a reason
/// worth keeping written down: these rows are not private. Error messages tell
/// operators to "clear the state row", runbooks and fixtures do it with
/// `DELETE FROM _apitap_state WHERE dest_table = 'orders'`, and moving the key
/// to `public.orders` would have made every one of those silently match
/// nothing. A tidier key is not worth breaking the recovery instructions the
/// software itself prints.
///
/// Returns (bare, qualified) — both spellings a row for this table may wear.
pub(crate) fn pg_state_keys(dest_table: &str) -> (String, String) {
    match dest_table.split_once('.') {
        Some((_, bare)) => (bare.to_string(), dest_table.to_string()),
        None => (dest_table.to_string(), format!("public.{dest_table}")),
    }
}

#[cfg(test)]
mod pg_state_key_tests {
    use super::*;

    /// The bug this exists for: the two lanes must produce the SAME canonical
    /// key for the same table, however the caller spelled it.
    /// However the caller spelled the table, the pair must contain BOTH
    /// spellings a row could wear — that is what lets one lane's delete reach
    /// the other lane's row.
    #[test]
    fn the_pair_covers_both_spellings_either_way_round() {
        assert_eq!(pg_state_keys("orders"), ("orders".into(), "public.orders".into()));
        assert_eq!(pg_state_keys("public.orders"), ("orders".into(), "public.orders".into()));
        assert_eq!(pg_state_keys("sales.orders"), ("orders".into(), "sales.orders".into()));
    }

    /// The bare name comes FIRST, because it is what the CDC lane writes and
    /// what every "clear the state row" instruction names.
    #[test]
    fn the_bare_name_is_the_one_operators_are_told_to_use() {
        assert_eq!(pg_state_keys("public.orders").0, "orders");
    }
}

/// The state table's name is fixed — it is per-destination, not per-table, so
/// it never needs shortening.
pub(crate) const STATE_TABLE: &str = "_apitap_state";

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariants below run over `Artifact::ALL`, not over a handful of
    /// examples. That is the point of the enum: a new artifact is tested by
    /// existing, so the next `__apitap_old` cannot slip through by being one
    /// nobody thought to write a case for.
    const LIMITS: &[usize] = &[PG_IDENT_MAX, MY_IDENT_MAX];

    #[test]
    fn every_artifact_fits_every_limit_at_every_length() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in [1usize, 10, 40, 46, 47, 48, 62, 63, 64, 100, 500] {
                    let bare = "t".repeat(n);
                    let out = artifact_ident(&bare, a, lim);
                    assert!(out.len() <= lim,
                            "{:?} at bare={n} limit={lim}: {} bytes", a, out.len());
                }
            }
        }
    }

    /// The failure that started this module: a name that shortens onto the
    /// table it belongs to. `DROP TABLE IF EXISTS <staging>` then drops the
    /// destination.
    #[test]
    fn an_artifact_is_never_equal_to_the_table_it_belongs_to() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in 1..=(lim + 20) {
                    let bare = "t".repeat(n);
                    assert_ne!(artifact_ident(&bare, a, lim), bare,
                               "{:?} at bare={n} limit={lim}", a);
                }
            }
        }
    }

    /// Two artifacts of the SAME table must never collide, or a swap renames
    /// the wrong object onto the destination.
    #[test]
    fn two_artifacts_of_one_table_never_share_a_name() {
        for &lim in LIMITS {
            for n in [10usize, 50, 63, 64, 200] {
                let bare = "t".repeat(n);
                let mut seen = std::collections::HashSet::new();
                for &a in Artifact::ALL {
                    let name = artifact_ident(&bare, a, lim);
                    assert!(seen.insert(name.clone()),
                            "{:?} collides at bare={n} limit={lim}: {name}", a);
                }
            }
        }
    }

    /// Two DIFFERENT tables must never share an artifact, however long a
    /// prefix they have in common — that was the second half of the same bug.
    #[test]
    fn two_tables_never_share_an_artifact() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                let x = format!("{}alpha", "p".repeat(lim));
                let y = format!("{}beta1", "p".repeat(lim));
                assert_ne!(artifact_ident(&x, a, lim), artifact_ident(&y, a, lim),
                           "{:?} at limit={lim}", a);
            }
        }
    }

    /// The suffix has to survive at the END, because every sweep, reservation
    /// and exclusion in the tree matches on it.
    #[test]
    fn the_suffix_always_survives_and_stays_terminal() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in [1usize, 63, 300] {
                    let out = artifact_ident(&"z".repeat(n), a, lim);
                    assert!(out.ends_with(a.suffix()), "{:?}: {out}", a);
                    // Only the namespaced ones are recognised as apitap's —
                    // `__current` is deliberately not, so that a user table
                    // called `orders__current` is left alone.
                    assert_eq!(is_artifact(&out), a.reserved(),
                               "{:?} recognition disagrees with reserved(): {out}", a);
                }
            }
        }
    }

    /// Deterministic, or a crashed run's leftovers can never be swept.
    #[test]
    fn the_same_inputs_always_give_the_same_name() {
        for &a in Artifact::ALL {
            let bare = "z".repeat(200);
            assert_eq!(artifact_ident(&bare, a, PG_IDENT_MAX),
                       artifact_ident(&bare, a, PG_IDENT_MAX));
        }
    }

    /// A short name keeps exactly the shape deployments already have on disk,
    /// so adopting this module is not a migration.
    #[test]
    fn short_names_are_unchanged_from_the_historical_format() {
        assert_eq!(artifact_ident("orders", Artifact::Staging, PG_IDENT_MAX),
                   "orders__apitap_staging");
        assert_eq!(artifact_ident("orders", Artifact::Old, MY_IDENT_MAX),
                   "orders__apitap_old");
    }

    /// The generated exclusion must name EVERY artifact, in both dialects.
    /// The hand-written clauses it replaces listed three of eight; this test is
    /// the thing that makes that impossible to repeat.
    #[test]
    fn the_sql_exclusion_covers_every_artifact() {
        for d in [Dialect::Postgres, Dialect::MySql] {
            let sql = sql_exclusion("c.relname", d);
            for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
                // The suffix appears with its underscores escaped, so compare
                // on the escaped form rather than the raw one.
                let esc = match d {
                    Dialect::Postgres => a.suffix().replace('_', "\\_"),
                    Dialect::MySql => a.suffix().replace('_', "|_"),
                };
                assert!(sql.contains(&esc), "{:?} missing from {:?}: {sql}", a, d);
            }
            assert!(sql.contains(STATE_TABLE), "state table missing: {sql}");
        }
    }

    /// `__current` must stay OUT of the reserved set: it is not namespaced,
    /// and refusing every `*__current` table would refuse real user data.
    #[test]
    fn the_unnamespaced_suffix_is_not_reserved() {
        assert!(!Artifact::Current.reserved());
        assert!(!is_artifact("orders__current"), "would refuse a real table");
        assert!(is_artifact("orders__apitap_staging"));
        for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
            assert!(a.suffix().contains("apitap"),
                    "{:?} is reserved but not namespaced — reserving it could \
                     hide a user's table", a);
        }
    }

    // ── run identity ──────────────────────────────────────────────────────

    fn peer(kind: LandKind, src: &str) -> PeerRun {
        let id = RunId::mint(kind, src);
        parse_peer(&artifact_ident_run("t", Artifact::Staging, PG_IDENT_MAX, &id),
                   Artifact::Staging)
            .expect("a name this module minted must parse")
    }

    /// Everything a concurrent run needs must survive the round trip through a
    /// table name — that is the whole premise, and if it does not hold the
    /// peer check silently degrades to "no peers found".
    #[test]
    fn a_minted_token_round_trips_through_the_name() {
        for &a in Artifact::ALL {
            for &lim in &[PG_IDENT_MAX, MY_IDENT_MAX, ROOMY] {
                let id = RunId::mint(LandKind::Incremental, "postgres://h/db::orders");
                let name = artifact_ident_run("orders", a, lim, &id);
                assert!(name.len() <= lim, "{:?} at {lim}: {} bytes", a, name.len());
                let p = parse_peer(&name, a).expect("must parse");
                assert_eq!(p.kind, LandKind::Incremental);
                assert!(name.ends_with(a.suffix()), "suffix stays terminal: {name}");
                assert_eq!(is_artifact(&name), a.reserved());
            }
        }
    }

    /// The invariants `artifact_ident` has, the tokenized form must have too —
    /// at every length, or a long table name loses them exactly where the
    /// original bug lived.
    #[test]
    fn a_tokenized_name_keeps_every_naming_invariant() {
        for &a in Artifact::ALL {
            for &lim in &[PG_IDENT_MAX, MY_IDENT_MAX] {
                for n in [1usize, 30, 31, 32, 46, 47, 63, 64, 200] {
                    let bare = "t".repeat(n);
                    let id = RunId::mint(LandKind::Swap, "s");
                    let name = artifact_ident_run(&bare, a, lim, &id);
                    assert!(name.len() <= lim, "{:?} bare={n} lim={lim}: {}", a, name.len());
                    assert_ne!(name, bare, "never equal to its own table");
                    assert!(name.ends_with(a.suffix()));
                    assert!(parse_peer(&name, a).is_some(), "{name}");
                }
            }
        }
    }

    /// Two runs must never mint the same name — that is the entire defence,
    /// so this asserts a guarantee rather than a likelihood. It failed on the
    /// first draft, which hashed the clock into 36^3 values: 500 mints collide
    /// 93% of the time at that width. Inside one process the nonce is now a
    /// counter, so the property is exact.
    #[test]
    fn two_runs_never_mint_the_same_name() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50_000 {
            let id = RunId::mint(LandKind::Swap, "postgres://h/db::orders");
            let name = artifact_ident_run("orders", Artifact::Staging, PG_IDENT_MAX, &id);
            assert!(seen.insert(name.clone()), "collision: {name}");
        }
    }

    /// The sweep pattern has to match what the minter produces. These two are
    /// used in different files and drift apart silently if nothing pins them.
    #[test]
    fn the_match_pattern_brackets_every_name_the_minter_makes() {
        for &a in Artifact::ALL {
            for n in [5usize, 40, 100] {
                let bare = "x".repeat(n);
                let (head, suffix) = artifact_match(&bare, a, PG_IDENT_MAX);
                let id = RunId::mint(LandKind::Cdc, "s");
                let name = artifact_ident_run(&bare, a, PG_IDENT_MAX, &id);
                assert!(name.starts_with(&head), "head {head} vs {name}");
                assert!(name.ends_with(suffix), "suffix {suffix} vs {name}");
                assert_eq!(name.len(), head.len() + RUN_TOKEN_LEN + suffix.len());
            }
        }
    }

    /// A name from before tokens existed must not parse as a peer — otherwise
    /// an old orphan reads as a live run and blocks every future run forever.
    #[test]
    fn an_untokenized_name_is_not_mistaken_for_a_peer() {
        let old = artifact_ident("orders", Artifact::Staging, PG_IDENT_MAX);
        assert_eq!(old, "orders__apitap_staging");
        assert!(parse_peer(&old, Artifact::Staging).is_none());
        // And something that merely ends the same way is not one either.
        assert!(parse_peer("__apitap_staging", Artifact::Staging).is_none());
        assert!(parse_peer("x__apitap_staging", Artifact::Staging).is_none());
    }

    /// The matrix. Fan-in — two DIFFERENT sources appending into one table — is
    /// a capability the manual advertises, and the first draft of this design
    /// would have removed it. Same-source appends are the duplicate-row bug.
    #[test]
    fn the_matrix_refuses_what_collides_and_permits_fan_in() {
        let a1 = peer(LandKind::Incremental, "postgres://h/db::orders");
        let a2 = peer(LandKind::Incremental, "postgres://h/db::orders");
        let b = peer(LandKind::Incremental, "mysql://other/db::orders");
        let r = peer(LandKind::Swap, "postgres://h/db::orders");
        let c = peer(LandKind::Cdc, "postgres://h/db::orders");

        assert!(peer_blocks(&a1, &a2), "same source appending twice = duplicate rows");
        assert!(!peer_blocks(&a1, &b), "fan-in from two sources must stay allowed");
        assert!(!peer_blocks(&b, &a1), "and it is symmetric");

        for other in [&a1, &b, &c] {
            assert!(peer_blocks(&r, other), "a swap coexists with nothing");
            assert!(peer_blocks(other, &r), "and nothing coexists with a swap");
        }
        assert!(peer_blocks(&c, &a1), "a CDC drain owns the watermark");
        assert!(peer_blocks(&c, &c), "including against another drain");
    }

    /// The age gate the reap depends on: a token minted now must read as now.
    #[test]
    fn the_start_time_is_readable_from_the_name() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let p = peer(LandKind::Swap, "s");
        assert!(p.started_unix <= now && now - p.started_unix < 5,
                "started {} vs now {now}", p.started_unix);
    }

    // ── classification ────────────────────────────────────────────────────

    /// The bug the review found in six sinks at once: a pattern anchored at
    /// only one end lets a run reap a DIFFERENT table's workspace.
    #[test]
    fn a_siblings_artifact_is_never_mistaken_for_ours() {
        let run = RunId::mint(LandKind::Swap, "s");
        let now = now_unix();
        for &a in Artifact::ALL {
            // `orders` and `orders_items` share a prefix, which is entirely
            // ordinary, and their artifacts must not see each other.
            let theirs = artifact_ident_run("orders_items", a, PG_IDENT_MAX, &run);
            assert_eq!(classify(&theirs, "orders", a, PG_IDENT_MAX, &run, now),
                       Found::Foreign, "{:?}: {theirs}", a);
            // And the reverse: the longer name must not claim the shorter's.
            let ours = artifact_ident_run("orders", a, PG_IDENT_MAX, &run);
            assert_eq!(classify(&ours, "orders_items", a, PG_IDENT_MAX, &run, now),
                       Found::Foreign, "{:?}: {ours}", a);
        }
    }

    /// One RunId is shared by every table in a multi-table run, so a run must
    /// recognise its own work — otherwise a long run outlives the horizon and
    /// collects its own sibling's LIVE staging, or reads it as a peer and
    /// refuses itself.
    #[test]
    fn a_run_recognises_its_own_artifacts_however_old_they_are() {
        let run = RunId::mint(LandKind::Swap, "s");
        let mine = artifact_ident_run("orders", Artifact::Staging, PG_IDENT_MAX, &run);
        // Far past any horizon: ownership is checked before age, on purpose.
        let much_later = now_unix() + 10_000_000;
        assert_eq!(
            classify(&mine, "orders", Artifact::Staging, PG_IDENT_MAX, &run, much_later),
            Found::Mine);
    }

    /// A foreign artifact is NEVER collected on age, however old its token
    /// looks. The token records when the RUN started, not when the object was
    /// made, so on a long load the two are hours apart — and collecting a live
    /// workspace is silent data loss on BigQuery and the object stores.
    /// Refusing is the safe action; collecting needs evidence this does not
    /// have.
    #[test]
    fn a_foreign_artifact_is_never_collected_on_age_alone() {
        let ours = RunId::mint(LandKind::Swap, "a");
        let theirs = RunId::mint(LandKind::Swap, "b");
        let name = artifact_ident_run("orders", Artifact::Staging, PG_IDENT_MAX, &theirs);
        for at in [now_unix(), now_unix() + 10_000_000] {
            assert!(matches!(
                classify(&name, "orders", Artifact::Staging, PG_IDENT_MAX, &ours, at),
                Found::Live(_)),
                "a peer stays a peer at every age — see classify's comment");
        }
    }

    /// The one name an older apitap wrote carries no age, so it has to be
    /// recognised explicitly or it would sit there forever.
    #[test]
    fn the_pre_token_name_is_collectable() {
        let run = RunId::mint(LandKind::Swap, "s");
        for &a in Artifact::ALL {
            let legacy = artifact_ident("orders", a, PG_IDENT_MAX);
            assert_eq!(classify(&legacy, "orders", a, PG_IDENT_MAX, &run, now_unix()),
                       Found::Dead, "{:?}: {legacy}", a);
        }
    }

    /// Anything else beside the table is none of our business.
    #[test]
    fn an_unrelated_name_is_left_alone() {
        let run = RunId::mint(LandKind::Swap, "s");
        let now = now_unix();
        for name in ["orders", "orders_backup", "orders__apitap_stagingX",
                     "totally_unrelated", "__apitap_staging"] {
            assert_eq!(classify(name, "orders", Artifact::Staging, PG_IDENT_MAX, &run, now),
                       Found::Foreign, "{name}");
        }
    }

    /// The wording bug the review found: with reaping switched off, six sinks
    /// told the operator to wait 0 seconds for a cleanup that never happens.
    #[test]
    fn the_refusal_tells_the_truth_about_collection() {
        let mine = PeerRun { started_unix: 100, kind: LandKind::Swap,
                             source_hash: "aaa".into(), token: "_x".into() };
        let peer = PeerRun { started_unix: 40, kind: LandKind::Incremental,
                             source_hash: "bbb".into(), token: "_y".into() };
        let msg = format!("{}", locked_error("public.orders", "orders_x__apitap_staging",
                                             &mine, &peer, 100));
        assert!(msg.contains("60s ago"), "{msg}");
        assert!(msg.contains("appending to it"), "names what the peer is doing: {msg}");
        assert!(msg.contains("throws the other's work away"), "names why: {msg}");
        assert!(msg.contains("max_active_runs"), "names the remedy: {msg}");
        // The recovery instruction must name the object and must not promise a
        // cleanup that does not exist — an earlier draft told the operator to
        // wait N seconds for one.
        assert!(msg.contains("orders_x__apitap_staging"), "names the object: {msg}");
        assert!(msg.contains("remove"), "{msg}");
        assert!(!msg.contains("automatically"), "promises no cleanup: {msg}");
    }

    /// Multi-byte names must not be cut through a character.
    #[test]
    fn a_utf8_name_is_cut_on_a_character_boundary() {
        for &a in Artifact::ALL {
            let out = artifact_ident(&"é".repeat(100), a, PG_IDENT_MAX);
            assert!(out.len() <= PG_IDENT_MAX);
            assert!(out.ends_with(a.suffix()));
        }
    }
}
