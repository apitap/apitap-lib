//! Window identity and the one replay rule (brief §2.B §3.D).
//!
//! A changelog destination (ClickHouse, BigQuery) appends every event of a
//! window under the window's START (`_apitap_lsn`), numbered by `_apitap_seq`.
//! A window can be applied more than once: its rows land and its watermark
//! does not (a crash between the two; a sibling's failure restarting the group
//! from its minimum). And a replay is not the same window. The stream cannot
//! seek, so a re-drain from the same start may end sooner (a smaller budget)
//! or later, or carry nothing of this table at all. Older releases also wrote
//! at stamps this one reaches: 0.55.x stamped a window with its END, which is
//! the next window's start, and 0.56.0 numbered every window from 0.
//!
//! `replay_plan` is the only place that decides what a window appends at a
//! stamp, under which seq, what it trims first and whether it leaves a marker.
//! The destinations spell its inputs in SQL (`StampFacts`, `Pending`) and
//! execute its steps; nothing else writes at a stamp. Until step 31 wires the
//! ClickHouse apply to it, nothing outside the tests calls it.
#![cfg_attr(not(test), allow(dead_code))]

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Mutex;

/// Where a window was drained from and to. `start` is the watermark the drain
/// began at — the one position a re-drain of the same window reproduces —
/// and `end` is the last complete commit it read, the only valid next
/// watermark. A re-drain recomputes `end`, so only `start` can stamp a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowId {
    start: u64,
    end: u64,
}

impl WindowId {
    pub(crate) fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// The watermark every apply of this window hands its unit's close.
    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    /// The changelog stamp (`_apitap_lsn`). The ClickHouse and BigQuery
    /// changelog applies read it until they take their stamp from
    /// `ReplayPlan::stamp`.
    pub(crate) fn start(&self) -> u64 {
        self.start
    }
}

/// The destination's record of the last append ATTEMPT at a table: the start
/// of the window it was appending, and the seq that attempt numbered from.
/// A marker 0.56.0 wrote carries the start alone (its `events` reads 0): that
/// release numbered every window from 0, so its base is 0 — unless the stamp
/// also holds another version's rows, which nothing can tell apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pending {
    start: u64,
    /// `None` = written by 0.56.0.
    seq_base: Option<u32>,
}

impl Pending {
    pub(crate) fn recorded(start: u64, seq_base: u32) -> Self {
        Self { start, seq_base: Some(seq_base) }
    }

    pub(crate) fn legacy(start: u64) -> Self {
        Self { start, seq_base: None }
    }

    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    /// The base the facts are counted from: a legacy attempt numbered from 0.
    fn query_base(&self) -> u32 {
        self.seq_base.unwrap_or(0)
    }
}

/// How many non-baseline rows a stamp holds, and their highest seq.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Landed {
    pub count: u64,
    pub max_seq: Option<u32>,
}

/// What is already at a stamp: every non-baseline row (`all`), the ones at or
/// above the attempt's base (`from_base`), and how many distinct seqs `all`
/// holds (a legacy attempt beside an older version's rows repeats some).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct StampFacts {
    pub all: Landed,
    pub from_base: Landed,
    pub distinct_seq: u64,
}

/// The marker an attempt writes BEFORE it appends: its start, its base, the
/// end it was drained to and how many events it carries. `events > 0` is what
/// tells it from a 0.56.0 marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MarkerRow {
    pub start: u64,
    pub seq_base: u32,
    pub end: u64,
    pub events: u64,
}

/// One table's changelog apply for one window: trim, mark, append, watermark.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplayPlan {
    stamp: u64,
    seq_base: u32,
    skip: usize,
    events: usize,
    trim_from: Option<u32>,
    mark: bool,
    watermark: u64,
}

impl ReplayPlan {
    /// `_apitap_lsn` of every row this window appends.
    pub(crate) fn stamp(&self) -> u64 {
        self.stamp
    }

    /// What the unit's close writes: the window's end, whatever was skipped.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Delete this stamp's non-baseline rows with `_apitap_seq >= x` first.
    pub(crate) fn trim_from(&self) -> Option<u32> {
        self.trim_from
    }

    /// The table's events (by ordinal) this window appends.
    pub(crate) fn to_append(&self) -> std::ops::Range<usize> {
        self.skip..self.events
    }

    /// `_apitap_seq` of the event at `ordinal`.
    pub(crate) fn seq_of(&self, ordinal: usize) -> u32 {
        debug_assert!(self.to_append().contains(&ordinal), "{ordinal} is not appended by {self:?}");
        // `replay_plan` proved `seq_base + events` fits.
        self.seq_base + ordinal as u32
    }

    /// The marker to write before the append, when there is one.
    pub(crate) fn marker(&self) -> Option<MarkerRow> {
        self.mark.then_some(MarkerRow {
            start: self.stamp,
            seq_base: self.seq_base,
            end: self.watermark,
            events: self.events as u64,
        })
    }
}

/// The one replay rule. `table_events` is THIS table's event count in the
/// window — never the window's total, which a group's other members inflate —
/// and 0 for a member the window does not carry: an absent member still runs
/// the rule, because an earlier attempt at this start may have appended rows
/// for it that the re-drain no longer holds.
///
/// - **R0**, the marker is past this start: the watermark was moved backwards.
///   Nothing here can tell which rows are whose, so refuse.
/// - **R1**, no marker at this start: whatever is at the stamp belongs to
///   another writer (a 0.55.x window stamped with its end, which is this
///   start, or an attempt with no marker). Number above it.
/// - **R2**, the marker names this start: a previous attempt of this window.
///   Make the log at the stamp exactly this window — resume an intact prefix,
///   trim a torn one and append again, trim the tail a shorter replay does not
///   carry (the next window brings it back under its own stamp).
pub(crate) fn replay_plan(
    table: &str,
    p: Option<Pending>,
    f: Option<&StampFacts>,
    id: &WindowId,
    table_events: usize,
) -> Result<ReplayPlan> {
    let n_ev = table_events;
    let overflow = |base: u32| {
        Error::Transfer(format!(
            "log_based: {table}: the window at {s} would number its events past \
             _apitap_seq's range ({base} + {n_ev} > {max})",
            s = id.start,
            max = u32::MAX,
        ))
    };
    let (seq_base, skip, trim_from, mark) = match p {
        Some(p) if p.start > id.start => {
            return Err(Error::Transfer(format!(
                "log_based: {table}: the destination records an append attempt at position {p}, \
                 past this window's start {s} — the watermark was moved backwards. Either put \
                 it back, or delete this table's rows with _apitap_lsn >= {s} AND _apitap_op != \
                 'B' together with its _apitap_cdc_pending rows, then re-run.",
                p = p.start,
                s = id.start,
            )))
        }
        Some(p) if p.start == id.start => {
            let f = f.ok_or_else(|| {
                Error::Transfer(format!(
                    "log_based: {table}: internal: replaying the window at {} needs the facts at its stamp",
                    id.start
                ))
            })?;
            let base = match p.seq_base {
                Some(b) => b,
                // 0.56.0 numbered its attempt from 0, and so did whatever
                // older version shares the stamp: a repeated seq means both
                // are there, and no row says which is which.
                None if f.all.count != f.distinct_seq => {
                    return Err(Error::Transfer(format!(
                        "log_based: {table}: a 0.56.0 run was interrupted while appending the \
                         window at {s}, and rows of an older version share that stamp; apitap \
                         cannot tell them apart. Inspect `SELECT _apitap_at, count(), \
                         min(_apitap_seq), max(_apitap_seq) FROM {table} WHERE _apitap_lsn = {s} \
                         AND _apitap_op != 'B' GROUP BY 1`, delete the newest group, clear this \
                         table's _apitap_cdc_pending rows, and re-run.",
                        s = id.start,
                    )))
                }
                None => 0,
            };
            let n = f.from_base.count;
            // Rows go out in seq order and the engine keeps what it received,
            // so an interrupted attempt normally leaves `base..base+n`. Only
            // count == span proves it.
            let prefix = n == 0
                || u32::try_from(n - 1).ok().and_then(|k| base.checked_add(k)).is_some_and(|last| {
                    f.from_base.max_seq == Some(last)
                });
            if !prefix {
                (base, 0, Some(base), n_ev > 0)
            } else if n <= n_ev as u64 {
                (base, n as usize, None, n < n_ev as u64)
            } else {
                let t = u32::try_from(n_ev).ok().and_then(|k| base.checked_add(k)).ok_or_else(|| overflow(base))?;
                (base, n_ev, Some(t), false)
            }
        }
        _ => {
            let base = match f.and_then(|f| f.all.max_seq) {
                None => 0,
                Some(m) => m.checked_add(1).ok_or_else(|| overflow(m))?,
            };
            (base, 0, None, n_ev > 0)
        }
    };
    u32::try_from(n_ev).ok().and_then(|k| seq_base.checked_add(k)).ok_or_else(|| overflow(seq_base))?;
    Ok(ReplayPlan { stamp: id.start, seq_base, skip, events: n_ev, trim_from, mark, watermark: id.end })
}

fn unreadable(what: &str, v: &str) -> Error {
    Error::Transfer(format!("log_based: the destination answered {v:?} for {what}"))
}

fn count_of(what: &str, v: &str) -> Result<u64> {
    v.trim().parse().map_err(|_| unreadable(what, v))
}

/// A max that an empty set spells `-1`.
fn max_of(what: &str, v: &str) -> Result<Option<u32>> {
    match v.trim() {
        "-1" => Ok(None),
        s => s.parse().map(Some).map_err(|_| unreadable(what, v)),
    }
}

/// The newest marker of a table, from the engines' probe row: the marker
/// count, then the newest marker's start, base and event count. No marker is
/// `None`; an `events` of 0 is a marker 0.56.0 wrote. Anything unreadable is
/// an error, never "no marker": that reading appends a replay again.
pub(crate) fn parse_pending(n: &str, lsn: &str, seq_base: &str, events: &str) -> Result<Option<Pending>> {
    if count_of("the marker count", n)? == 0 {
        return Ok(None);
    }
    let start = count_of("a marker's start", lsn)?;
    let base = u32::try_from(count_of("a marker's seq_base", seq_base)?)
        .map_err(|_| unreadable("a marker's seq_base", seq_base))?;
    Ok(Some(if count_of("a marker's events", events)? == 0 {
        Pending::legacy(start)
    } else {
        Pending::recorded(start, base)
    }))
}

/// A stamp's facts from the engines' row `[n_all, max_all, n_from_base,
/// max_from_base, distinct_seq]`, maxima `-1` when their count is 0.
pub(crate) fn parse_facts(cells: &[&str]) -> Result<StampFacts> {
    let [n_all, mx_all, n_b, mx_b, distinct] = cells else {
        return Err(unreadable("a stamp's facts", &cells.join("\t")));
    };
    let landed = |n: &str, mx: &str| -> Result<Landed> {
        let l = Landed { count: count_of("a stamp's row count", n)?, max_seq: max_of("a stamp's max seq", mx)? };
        // A count with no max, or a max over nothing, is not a row this
        // query can return: the columns are misread.
        if (l.count == 0) != l.max_seq.is_none() {
            return Err(unreadable("a stamp's facts", &cells.join("\t")));
        }
        Ok(l)
    };
    let f = StampFacts {
        all: landed(n_all, mx_all)?,
        from_base: landed(n_b, mx_b)?,
        distinct_seq: count_of("a stamp's distinct seqs", distinct)?,
    };
    if f.from_base.count > f.all.count || f.distinct_seq > f.all.count {
        return Err(unreadable("a stamp's facts", &cells.join("\t")));
    }
    Ok(f)
}

/// The highest non-baseline stamp in a table when the run started, from its
/// row count and max stamp (either `0` or `-1` over an empty table).
pub(crate) fn parse_ceiling(n: &str, max_lsn: &str) -> Result<Option<u64>> {
    if count_of("the log's row count", n)? == 0 {
        return Ok(None);
    }
    count_of("the log's highest stamp", max_lsn).map(Some)
}

/// What one run knows of each changelog table's stamps, so the facts are
/// asked only where rows can be (one per destination, i.e. per run).
///
/// Rows at a stamp exist only where something wrote there. Earlier runs wrote
/// at or below the `ceiling` the first probe saw; this run writes only at its
/// own window starts, which strictly increase; a concurrent writer is shut
/// out by the lease. So after a run's first window the memo answers
/// `Nothing`, and a window costs no query at all.
#[derive(Default)]
pub(crate) struct Memo(Mutex<HashMap<(String, String), Seen>>);

#[derive(Clone, Copy)]
struct Seen {
    pending: Option<Pending>,
    ceiling: Option<u64>,
}

/// What the destination must read before `Memo::plan`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ask {
    /// The newest marker and the ceiling (then `probed`, then `ask` again).
    Probe,
    /// The facts at this window's stamp, counted from `base`.
    Facts { base: u32 },
    Nothing,
}

impl Memo {
    fn key(t: &str, sid: &str) -> (String, String) {
        (t.to_string(), sid.to_string())
    }

    pub(crate) fn ask(&self, t: &str, sid: &str, id: &WindowId) -> Ask {
        let m = self.0.lock().unwrap();
        let Some(s) = m.get(&Self::key(t, sid)) else { return Ask::Probe };
        match (s.pending, s.ceiling) {
            (Some(p), _) if p.start == id.start => Ask::Facts { base: p.query_base() },
            (_, Some(c)) if c >= id.start => Ask::Facts { base: 0 },
            _ => Ask::Nothing,
        }
    }

    /// The probe's answer: the newest marker, and the highest non-baseline
    /// stamp (`None` for a log with no event yet).
    pub(crate) fn probed(&self, t: &str, sid: &str, pending: Option<Pending>, ceiling: Option<u64>) {
        self.0.lock().unwrap().insert(Self::key(t, sid), Seen { pending, ceiling });
    }

    /// `replay_plan` over what this memo holds and the facts it asked for.
    pub(crate) fn plan(
        &self,
        t: &str,
        sid: &str,
        f: Option<&StampFacts>,
        id: &WindowId,
        events: usize,
    ) -> Result<ReplayPlan> {
        let pending = match self.0.lock().unwrap().get(&Self::key(t, sid)) {
            Some(s) => s.pending,
            None => {
                return Err(Error::Transfer(format!("log_based: {t}: internal: a changelog plan before its probe")))
            }
        };
        replay_plan(t, pending, f, id, events)
    }

    /// The unit holding `plan` committed: its marker is now the newest.
    pub(crate) fn committed(&self, t: &str, sid: &str, plan: &ReplayPlan) {
        if !plan.mark {
            return;
        }
        if let Some(s) = self.0.lock().unwrap().get_mut(&Self::key(t, sid)) {
            s.pending = Some(Pending::recorded(plan.stamp, plan.seq_base));
        }
    }

    /// An apply or its close failed: what the destination holds is unknown
    /// again, and the next window probes.
    pub(crate) fn forget(&self, t: &str, sid: &str) {
        self.0.lock().unwrap().remove(&Self::key(t, sid));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The rules under test. Each RED control swaps ONE of these lines for its
    // 0.56.0 transcription in `v0560` and runs the tests that use it.
    use super::replay_plan as plan_rule;
    use super::Memo as MemoRule;
    use super::{parse_facts as facts_rule, parse_pending as pending_rule};

    /// What 0.56.0 did, transcribed from dest_ch.rs at d45f932
    /// (`apply_changelog`, `pending_window`, `appended_prefix`) into this
    /// module's types. Nothing calls it but a RED control.
    #[allow(dead_code)]
    mod v0560 {
        use super::super::*;

        pub(in super::super) fn replay_plan(
            _table: &str,
            p: Option<Pending>,
            f: Option<&StampFacts>,
            id: &WindowId,
            n_ev: usize,
        ) -> Result<ReplayPlan> {
            let only_watermark =
                ReplayPlan { stamp: id.start, seq_base: 0, skip: n_ev, events: n_ev, trim_from: None, mark: false, watermark: id.end };
            // :847-850 — no event for this table: the watermark and nothing else.
            if n_ev == 0 {
                return Ok(only_watermark);
            }
            let mut skip = 0usize;
            // :889 — the stamp was counted only when the marker named this
            // very start; any other marker, or none, was a fresh window.
            if p.map(|p| p.start) == Some(id.start) {
                // :351-382 — `count(), ifNull(max(seq), 0)` over every
                // non-baseline row at the stamp: a prefix from 0, or not.
                let (n, max) = f.map_or((0, 0), |f| (f.all.count, f.all.max_seq.unwrap_or(0)));
                if n > 0 && n == u64::from(max) + 1 {
                    // :906
                    skip = (n as usize).min(n_ev);
                }
                // :907-921 — not a prefix: append the whole window again.
            }
            // :923-927 — everything already there: the watermark only.
            if skip >= n_ev {
                return Ok(only_watermark);
            }
            // :898, :938 — seq is the ordinal from 0, whatever the stamp holds.
            Ok(ReplayPlan { stamp: id.start, seq_base: 0, skip, events: n_ev, trim_from: None, mark: true, watermark: id.end })
        }

        /// 0.56.0 kept nothing between windows: every window read its table's
        /// marker (:889), and counted the stamp only when that marker named
        /// this start. It never looked for another writer's rows.
        #[derive(Default)]
        pub(in super::super) struct Memo(Mutex<HashMap<(String, String), Option<Pending>>>);

        impl Memo {
            pub(in super::super) fn ask(&self, t: &str, sid: &str, id: &WindowId) -> Ask {
                match self.0.lock().unwrap().get(&(t.to_string(), sid.to_string())) {
                    None => Ask::Probe,
                    Some(Some(p)) if p.start == id.start => Ask::Facts { base: 0 },
                    Some(_) => Ask::Nothing,
                }
            }
            pub(in super::super) fn probed(&self, t: &str, sid: &str, p: Option<Pending>, _ceiling: Option<u64>) {
                self.0.lock().unwrap().insert((t.to_string(), sid.to_string()), p);
            }
            /// The marker read is used up: the next window reads it again.
            pub(in super::super) fn plan(
                &self,
                t: &str,
                sid: &str,
                f: Option<&StampFacts>,
                id: &WindowId,
                events: usize,
            ) -> Result<ReplayPlan> {
                let p = self.0.lock().unwrap().remove(&(t.to_string(), sid.to_string())).flatten();
                replay_plan(t, p, f, id, events)
            }
            pub(in super::super) fn committed(&self, _t: &str, _sid: &str, _plan: &ReplayPlan) {}
            pub(in super::super) fn forget(&self, t: &str, sid: &str) {
                self.0.lock().unwrap().remove(&(t.to_string(), sid.to_string()));
            }
        }

        /// :324-337 — `toString(argMax(lsn, at))`, parsed with `.ok()`: an
        /// empty table answers "0", which reads as a marker at 0, and junk
        /// reads as no marker.
        pub(in super::super) fn parse_pending(_n: &str, lsn: &str, _base: &str, _events: &str) -> Result<Option<Pending>> {
            Ok(lsn.trim().parse::<u64>().ok().map(Pending::legacy))
        }

        /// :351-382 — each cell `unwrap_or(0)`: a missing max and junk both
        /// read as 0. It counted every row at the stamp, from 0.
        pub(in super::super) fn parse_facts(cells: &[&str]) -> Result<StampFacts> {
            let num = |i: usize| cells.get(i).and_then(|c| c.trim().parse::<u64>().ok()).unwrap_or(0);
            let all = Landed { count: num(0), max_seq: Some(num(1) as u32) };
            Ok(StampFacts { all, from_base: all, distinct_seq: all.count })
        }
    }

    const S: u64 = 5_000;
    const E: u64 = 9_000;

    fn id() -> WindowId {
        WindowId::new(S, E)
    }

    fn landed(count: u64, max_seq: Option<u32>) -> Landed {
        Landed { count, max_seq }
    }

    /// A stamp holding only an attempt of this window, numbered from `base`.
    fn attempt(base: u32, count: u64, max_seq: Option<u32>) -> StampFacts {
        StampFacts { all: landed(count, max_seq), from_base: landed(count, max_seq), distinct_seq: count }
            .with_base(base)
    }

    impl StampFacts {
        fn with_base(mut self, base: u32) -> Self {
            // Rows below the base are another writer's: `all` counts them too.
            if base > 0 && self.all.count > 0 {
                self.all.count += u64::from(base);
                self.distinct_seq = self.all.count;
            }
            self
        }
    }

    /// Another writer's rows at the stamp, seq 0..count.
    fn foreign(count: u64) -> StampFacts {
        let l = landed(count, count.checked_sub(1).map(|m| m as u32));
        StampFacts { all: l, from_base: l, distinct_seq: count }
    }

    #[test]
    fn shorter_replay_trims_tail() {
        // D1: the attempt landed 100 events; the replay from the same start
        // carries 60. The 40 it does not carry come back in the next window
        // under that window's own stamp, so they must not stay at this one.
        for p in [Pending::recorded(S, 0), Pending::legacy(S)] {
            let plan = plan_rule("t", Some(p), Some(&attempt(0, 100, Some(99))), &id(), 60).unwrap();
            assert_eq!(plan.to_append(), 60..60, "{p:?}");
            assert_eq!(plan.trim_from(), Some(60), "{p:?}: the tail the replay does not carry stayed");
            assert_eq!(plan.marker(), None, "{p:?}");
            assert_eq!((plan.stamp(), plan.watermark()), (S, E), "{p:?}");
        }
        // The same above a base: the trim starts where this window's 60th is.
        let f = attempt(7, 100, Some(106));
        let plan = plan_rule("t", Some(Pending::recorded(S, 7)), Some(&f), &id(), 60).unwrap();
        assert_eq!((plan.to_append(), plan.trim_from()), (60..60, Some(67)));
    }

    #[test]
    fn foreign_rows_continue_seq() {
        // D2: 40 rows another writer left at this stamp (a 0.55.x window
        // stamped with its end), and no marker here — none at all, or an
        // earlier window's. This window numbers above them.
        for p in [None, Some(Pending::recorded(S - 1, 0)), Some(Pending::legacy(S - 1))] {
            let plan = plan_rule("t", p, Some(&foreign(40)), &id(), 5).unwrap();
            assert_eq!(plan.to_append(), 0..5, "{p:?}");
            assert_eq!(plan.seq_of(0), 40, "{p:?}: seq restarted under the rows already at the stamp");
            assert_eq!(plan.seq_of(4), 44, "{p:?}");
            assert_eq!(plan.trim_from(), None, "{p:?}: another writer's rows were trimmed");
            assert_eq!(plan.marker(), Some(MarkerRow { start: S, seq_base: 40, end: E, events: 5 }), "{p:?}");
        }
        // Nothing at the stamp (no facts asked, or none found): from 0.
        for f in [None, Some(foreign(0))] {
            let plan = plan_rule("t", None, f.as_ref(), &id(), 3).unwrap();
            assert_eq!((plan.seq_of(0), plan.to_append()), (0, 0..3));
        }
        // A window with nothing for this table and no attempt here: nothing.
        let plan = plan_rule("t", None, Some(&foreign(40)), &id(), 0).unwrap();
        assert_eq!((plan.to_append(), plan.trim_from(), plan.marker()), (0..0, None, None));
    }

    #[test]
    fn torn_trims_then_reappends() {
        // D3: 50 rows from the base with a hole in them (max 60). No place to
        // resume: trim everything from the base, append the window again. The
        // rows the trim leaves are foreign; the append reuses exactly the
        // attempt's seqs, so no pair repeats.
        let f = StampFacts { all: landed(50, Some(60)), from_base: landed(50, Some(60)), distinct_seq: 50 };
        for p in [Pending::recorded(S, 0), Pending::legacy(S)] {
            let plan = plan_rule("t", Some(p), Some(&f), &id(), 80).unwrap();
            assert_eq!(plan.trim_from(), Some(0), "{p:?}: the torn attempt stays beside its re-append");
            assert_eq!(plan.to_append(), 0..80, "{p:?}");
            assert_eq!(plan.seq_of(0), 0, "{p:?}");
            assert!(plan.marker().is_some(), "{p:?}");
        }
        // Above a base of 5: the foreign 0..4 stay.
        let f = StampFacts { all: landed(55, Some(60)), from_base: landed(50, Some(60)), distinct_seq: 55 };
        let plan = plan_rule("t", Some(Pending::recorded(S, 5)), Some(&f), &id(), 80).unwrap();
        assert_eq!((plan.trim_from(), plan.to_append(), plan.seq_of(0)), (Some(5), 0..80, 5));
        // Torn, and the replay no longer carries the table: trim only.
        let plan = plan_rule("t", Some(Pending::recorded(S, 5)), Some(&f), &id(), 0).unwrap();
        assert_eq!((plan.trim_from(), plan.to_append(), plan.marker()), (Some(5), 0..0, None));
    }

    #[test]
    fn intact_resumes() {
        // D4: 30 of 100 landed as a prefix: resume at the 31st, re-mark.
        let plan = plan_rule("t", Some(Pending::legacy(S)), Some(&attempt(0, 30, Some(29))), &id(), 100).unwrap();
        assert_eq!((plan.to_append(), plan.seq_of(30), plan.trim_from()), (30..100, 30, None));
        assert_eq!(plan.marker(), Some(MarkerRow { start: S, seq_base: 0, end: E, events: 100 }));
        // The same attempt numbered from 7, above 7 foreign rows: the prefix
        // is counted from the base, and 0.56.0 — counting every row at the
        // stamp — resumed at the 38th and lost seven events.
        let f = attempt(7, 30, Some(36));
        assert_eq!(f.all.count, 37);
        let plan = plan_rule("t", Some(Pending::recorded(S, 7)), Some(&f), &id(), 100).unwrap();
        assert_eq!(plan.to_append(), 30..100, "resumed past events that never landed");
        assert_eq!((plan.seq_of(30), plan.trim_from()), (37, None));
        assert_eq!(plan.marker(), Some(MarkerRow { start: S, seq_base: 7, end: E, events: 100 }));
        // Everything landed: nothing to append, nothing to mark.
        let plan = plan_rule("t", Some(Pending::recorded(S, 0)), Some(&attempt(0, 100, Some(99))), &id(), 100).unwrap();
        assert_eq!((plan.to_append(), plan.trim_from(), plan.marker()), (100..100, None, None));
        // Nothing landed yet (the kill came between the marker and the rows).
        let plan = plan_rule("t", Some(Pending::recorded(S, 0)), Some(&attempt(0, 0, None)), &id(), 100).unwrap();
        assert_eq!((plan.to_append(), plan.trim_from(), plan.marker().is_some()), (0..100, None, true));
    }

    #[test]
    fn absent_member_trims_attempt() {
        // D5: an attempt at this start landed 100 rows for the table; the
        // replay carries none of its events (a group member the shorter
        // window does not reach). Every one comes back later, so none stays.
        let plan = plan_rule("t", Some(Pending::recorded(S, 0)), Some(&attempt(0, 100, Some(99))), &id(), 0).unwrap();
        assert_eq!(plan.trim_from(), Some(0), "the absent member kept the attempt's rows");
        assert_eq!((plan.to_append(), plan.marker()), (0..0, None));
        let f = attempt(5, 100, Some(104));
        let plan = plan_rule("t", Some(Pending::recorded(S, 5)), Some(&f), &id(), 0).unwrap();
        assert_eq!((plan.trim_from(), plan.to_append()), (Some(5), 0..0));
        // Absent, and no attempt at this start: the watermark alone.
        let plan = plan_rule("t", Some(Pending::recorded(S - 1, 0)), None, &id(), 0).unwrap();
        assert_eq!((plan.trim_from(), plan.to_append(), plan.marker()), (None, 0..0, None));
    }

    #[test]
    fn table_count_not_window_total() {
        // D6: a group's window of 240 events, 40 of them this table's; an
        // attempt landed 100 of the table's. The rule is given the table's
        // own count, and its plan slices the table's own events.
        let events: Vec<u32> = (0..40).collect();
        let f = attempt(0, 100, Some(99));
        let plan = plan_rule("t", Some(Pending::recorded(S, 0)), Some(&f), &id(), events.len()).unwrap();
        assert_eq!(plan.to_append(), 40..40);
        assert_eq!(plan.trim_from(), Some(40), "the 60 events this window does not carry stayed");
        assert!(events[plan.to_append()].is_empty());
    }

    #[test]
    fn unmatched_is_err() {
        // D7. A marker past this start: the watermark went backwards.
        for f in [None, Some(attempt(0, 3, Some(2)))] {
            let e = plan_rule("orders", Some(Pending::recorded(S + 1, 0)), f.as_ref(), &id(), 4).unwrap_err().to_string();
            assert!(e.contains("moved backwards"), "{e}");
            assert!(e.contains(&format!("{}", S + 1)) && e.contains(&format!("_apitap_lsn >= {S}")), "{e}");
        }
        // A 0.56.0 marker at a stamp where seqs repeat: its attempt and an
        // older version's rows, which nothing tells apart.
        let f = StampFacts { all: landed(5, Some(2)), from_base: landed(5, Some(2)), distinct_seq: 3 };
        let e = plan_rule("orders", Some(Pending::legacy(S)), Some(&f), &id(), 4).unwrap_err().to_string();
        assert!(e.contains("0.56.0 run was interrupted") && e.contains("FROM orders WHERE _apitap_lsn = 5000"), "{e}");
        // …the same stamp with a 0.57.0 marker knows its base: rows below it
        // may repeat (0.55.x under 0.56.0), and they are not its own.
        let f = StampFacts { all: landed(8, Some(4)), from_base: landed(3, Some(4)), distinct_seq: 5 };
        let plan = plan_rule("orders", Some(Pending::recorded(S, 2)), Some(&f), &id(), 4).unwrap();
        assert_eq!((plan.to_append(), plan.seq_of(3)), (3..4, 5));
        // A clean 0.56.0 marker: base 0, as 0.56.0 itself read it.
        let plan = plan_rule("orders", Some(Pending::legacy(S)), Some(&attempt(0, 3, Some(2))), &id(), 4).unwrap();
        assert_eq!((plan.to_append(), plan.seq_of(3)), (3..4, 3));
        // A replay with no facts cannot decide anything.
        assert!(plan_rule("orders", Some(Pending::recorded(S, 0)), None, &id(), 4).is_err());
        // Past u32: seq is a UInt32 / INT64 column read back as u32.
        let e = plan_rule("orders", Some(Pending::recorded(S, u32::MAX - 10)), Some(&attempt(0, 0, None)), &id(), 11)
            .unwrap_err()
            .to_string();
        assert!(e.contains("_apitap_seq's range"), "{e}");
        let f = StampFacts { all: landed(1, Some(u32::MAX - 5)), from_base: landed(1, Some(u32::MAX - 5)), distinct_seq: 1 };
        assert!(plan_rule("orders", None, Some(&f), &id(), 10).is_err());
        let f = StampFacts { all: landed(1, Some(u32::MAX)), from_base: landed(1, Some(u32::MAX)), distinct_seq: 1 };
        assert!(plan_rule("orders", None, Some(&f), &id(), 1).is_err());
        // …and the largest window that still fits.
        assert!(plan_rule("orders", Some(Pending::recorded(S, u32::MAX - 10)), Some(&attempt(0, 0, None)), &id(), 10).is_ok());
    }

    #[test]
    fn memo_asks() {
        // D9.
        let m = MemoRule::default();
        let (s0, s1, s2) = (WindowId::new(S, S + 10), WindowId::new(S + 10, S + 20), WindowId::new(S + 20, S + 30));
        assert_eq!(m.ask("t", "sid", &s0), Ask::Probe, "a table never probed");
        // The run's first probe: an attempt at s0, and the log reaches s0.
        m.probed("t", "sid", Some(Pending::recorded(S, 3)), Some(S));
        assert_eq!(m.ask("t", "sid", &s0), Ask::Facts { base: 3 }, "a replay of the marked window");
        let plan = m.plan("t", "sid", Some(&attempt(3, 2, Some(4))), &s0, 5).unwrap();
        assert_eq!(plan.to_append(), 2..5);
        m.committed("t", "sid", &plan);
        // Steady state: past the ceiling, no marker there — no query at all.
        assert_eq!(m.ask("t", "sid", &s1), Ask::Nothing, "a window past everything the run found was queried");
        let plan = m.plan("t", "sid", None, &s1, 4).unwrap();
        assert_eq!(plan.seq_of(0), 0);
        m.committed("t", "sid", &plan);
        // This run's own marker is what a replay of s1 would count from.
        assert_eq!(m.ask("t", "sid", &s1), Ask::Facts { base: 0 });
        assert_eq!(m.ask("t", "sid", &s2), Ask::Nothing);
        // A failed apply or close: unknown again.
        m.forget("t", "sid");
        assert_eq!(m.ask("t", "sid", &s2), Ask::Probe);
        // Another writer's rows at this start, no marker: count them.
        m.probed("u", "sid", None, Some(S));
        assert_eq!(m.ask("u", "sid", &s0), Ask::Facts { base: 0 }, "an older version's rows at the stamp were not counted");
        assert_eq!(m.ask("u", "sid", &s1), Ask::Nothing);
        // A legacy marker counts from 0; tables and sources are apart.
        m.probed("v", "sid", Some(Pending::legacy(S)), Some(S));
        assert_eq!(m.ask("v", "sid", &s0), Ask::Facts { base: 0 });
        assert_eq!(m.ask("v", "other", &s0), Ask::Probe);
        // A log with no event yet and no marker: nothing to count.
        m.probed("w", "sid", None, None);
        assert_eq!(m.ask("w", "sid", &s0), Ask::Nothing);
    }

    #[test]
    fn parse_rows() {
        // D11. The ClickHouse probe row over an empty marker table, and a
        // BigQuery table with no 'm' row, both read as no marker.
        assert_eq!(pending_rule("0", "0", "0", "0").unwrap(), None, "no marker read as one at position 0");
        // A marker 0.56.0 wrote, read through the ALTERed table: events = 0.
        assert_eq!(pending_rule("1", "812", "0", "0").unwrap(), Some(Pending::legacy(812)));
        let p = pending_rule("3", "812", "7", "100").unwrap();
        assert_eq!(p, Some(Pending::recorded(812, 7)));
        assert_eq!(p.map(|p| p.start()), Some(812));
        for bad in [("1", "x", "0", "4"), ("1", "812", "-3", "4"), ("1", "812", "7", ""), ("n", "812", "7", "4"),
                    ("1", "812", "4294967296", "4")] {
            assert!(pending_rule(bad.0, bad.1, bad.2, bad.3).is_err(), "{bad:?} read as a marker or as none");
        }
        // Facts: -1 is "no row", on both engines.
        assert_eq!(facts_rule(&["0", "-1", "0", "-1", "0"]).unwrap(), StampFacts::default(), "an empty stamp read as seq 0");
        assert_eq!(
            facts_rule(&["5", "9", "3", "9", "5"]).unwrap(),
            StampFacts { all: landed(5, Some(9)), from_base: landed(3, Some(9)), distinct_seq: 5 }
        );
        for bad in [&["5", "nine", "3", "9", "5"][..], &["5", "9", "3", "9"], &["5", "-1", "0", "-1", "5"],
                    &["0", "4", "0", "-1", "0"], &["3", "9", "5", "9", "3"], &[" ", "9", "3", "9", "5"]] {
            assert!(facts_rule(bad).is_err(), "{bad:?} was read as facts");
        }
        // The ceiling: ClickHouse's max over nothing is 0, BigQuery's -1.
        assert_eq!(parse_ceiling("0", "0").unwrap(), None);
        assert_eq!(parse_ceiling("0", "-1").unwrap(), None);
        assert_eq!(parse_ceiling("12", "812").unwrap(), Some(812));
        assert!(parse_ceiling("12", "x").is_err());
    }
}
