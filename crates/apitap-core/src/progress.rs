//! Live progress for a running transfer, emitted by the engine itself.
//!
//! A transfer that moves half a billion rows used to say nothing at all until
//! it finished or died. This module is the fix: a reporter thread that prints
//! what has actually moved, in a shape that suits wherever it is running.
//!
//! **Where the numbers come from.** Bytes are counted at the `Loader` — the one
//! place every byte of every source→destination pair passes through, so no lane
//! can silently escape the count. Rows are counted by the lanes that decode
//! rows anyway (the MySQL wire client, the RowBinary transcoder, the text
//! lanes, the CDC collapse layer). The raw binary COPY relay (pg→pg) does NOT
//! count rows in flight on purpose: not parsing tuples is precisely what makes
//! that lane the fastest one, so it reports bytes while running and the exact
//! row count at the end, where `finish()`/`rows_staged()` report it. The final
//! line always carries the same number `TransferReport.rows` does — progress
//! never invents a second, disagreeing tally.
//!
//! **Where it prints.** stderr, so stdout stays clean for piping. On a terminal
//! it rewrites one line every 2 s. Everywhere else — Airflow, Kubernetes,
//! docker logs, cron — it prints a plain `key=value` line every 30 s with no
//! ANSI and no carriage returns, flushed per line so the orchestrator shows it
//! while the transfer runs instead of at the end.
//!
//! `APITAP_PROGRESS`: unset = the behaviour above, `0`/`off` = silent,
//! `1`/`on` = force the human format, `json` = one JSON object per line for a
//! log collector. `APITAP_PROGRESS_INTERVAL` overrides the cadence in seconds.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Counters. Globals rather than a handle threaded through every source: the
/// increment is a relaxed `fetch_add` on a hot path, and a transfer is one
/// logical operation per process — a `slots=N` CDC run spreading over several
/// threads and runtimes then aggregates into one honest total for free.
static ROWS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static TABLES_DONE: AtomicU64 = AtomicU64::new(0);
static TABLES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// `-2` = nobody has set a denominator yet, `-1` = set but unknown.
static EST_ROWS: AtomicI64 = AtomicI64::new(UNSET_EST);
const UNSET_EST: i64 = -2;
static PIPES: AtomicU64 = AtomicU64::new(0);
static WINDOW: AtomicU64 = AtomicU64::new(0);
/// Set once per run: cheap enough that the counters cost nothing when off.
static ON: AtomicBool = AtomicBool::new(false);
/// True while a reporter thread is alive; cleared to stop it.
static RUNNING: AtomicBool = AtomicBool::new(false);
/// Set by `add_rows` itself: a lane that reports rows is, by definition, a
/// lane that decodes them. Nothing claims exactness on a lane's behalf, so a
/// relay that never counts shows bytes instead of a misleading zero — the
/// first version of this defaulted to `true` and printed `rows=0` beside
/// hundreds of live megabytes on the Postgres lanes.
static ROWS_EXACT: AtomicBool = AtomicBool::new(false);

fn label() -> &'static Mutex<String> {
    static L: OnceLock<Mutex<String>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(String::new()))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    /// One rewritten line, for a human watching a terminal.
    Live,
    /// `ts key=value …`, for orchestrator logs.
    Plain,
    /// One JSON object per line, for a log collector.
    Json,
}

/// What the unit of work is called in the output.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    Rows,
    Changes,
}

impl Unit {
    fn word(self) -> &'static str {
        match self {
            Unit::Rows => "rows",
            Unit::Changes => "changes",
        }
    }
}

fn env_choice() -> Option<Format> {
    match std::env::var("APITAP_PROGRESS") {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "0" | "off" | "false" | "no" => None,
            "json" => Some(Format::Json),
            "" => Some(default_format()),
            _ => Some(if std::io::stderr().is_terminal() {
                Format::Live
            } else {
                Format::Plain
            }),
        },
        Err(_) => Some(default_format()),
    }
}

fn default_format() -> Format {
    if std::io::stderr().is_terminal() {
        Format::Live
    } else {
        Format::Plain
    }
}

fn interval(fmt: Format) -> Duration {
    if let Ok(v) = std::env::var("APITAP_PROGRESS_INTERVAL") {
        if let Ok(secs) = v.trim().parse::<f64>() {
            if secs > 0.05 {
                return Duration::from_secs_f64(secs);
            }
        }
    }
    // A terminal can take a live number; a log file being written every two
    // seconds for a six-hour DAG cannot.
    match fmt {
        Format::Live => Duration::from_secs(2),
        _ => Duration::from_secs(30),
    }
}

/// Count rows that have actually reached the destination stream. Called by the
/// lanes that decode rows; a no-op when progress is off.
#[inline]
pub(crate) fn add_rows(n: u64) {
    if ON.load(Relaxed) {
        ROWS.fetch_add(n, Relaxed);
        ROWS_EXACT.store(true, Relaxed);
    }
}

/// Count bytes handed to a sink loader — every lane passes here.
#[inline]
pub(crate) fn add_bytes(n: u64) {
    if ON.load(Relaxed) {
        BYTES.fetch_add(n, Relaxed);
    }
}

pub(crate) fn set_pipes(n: usize) {
    PIPES.store(n as u64, Relaxed);
}

pub(crate) fn table_done() {
    TABLES_DONE.fetch_add(1, Relaxed);
}

/// A named measurement, in whatever shape the environment reads.
///
/// Notes are prose: fine for a human, useless to an alert. A gauge is the
/// number itself — `retained_bytes=4294967296` — so a log pipeline can graph
/// it or page on it without parsing an English sentence that may be reworded
/// next release.
///
/// It exists because the one operational number apitap had (the WAL a
/// replication slot is holding on the SOURCE, which is the difference between
/// a paused schedule and a full disk) was emitted two ways, both unusable: a
/// prose note under the threshold, and a raw `eprintln!` above it that
/// bypassed this module entirely — so in JSON mode the single most important
/// line was the only one that was not JSON.
///
/// `fields` are pre-rendered `key=value` pairs; numbers stay unquoted in JSON
/// so they arrive as numbers.
pub(crate) fn gauge(event: &str, fields: &[(&str, String)]) {
    let Some(fmt) = env_choice() else { return };
    let mut err = std::io::stderr().lock();
    let _ = match fmt {
        Format::Json => {
            let body: Vec<String> = fields
                .iter()
                .map(|(k, v)| {
                    // A value that parses as a number is written as one; only
                    // the rest are quoted.
                    if v.parse::<f64>().is_ok() {
                        format!("\"{k}\":{v}")
                    } else {
                        format!("\"{k}\":\"{}\"", v.replace('"', "'"))
                    }
                })
                .collect();
            writeln!(
                err,
                "{{\"ts\":\"{}\",\"event\":\"{event}\",{}}}",
                stamp(),
                body.join(",")
            )
        }
        Format::Plain => {
            let body: Vec<String> =
                fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
            writeln!(err, "{} apitap {event} {}", stamp(), body.join(" "))
        }
        Format::Live => {
            let body: Vec<String> =
                fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
            writeln!(err, "\r\x1b[Kapitap ▸ {event} {}", body.join(" "))
        }
    };
    let _ = err.flush();
}

/// A one-off line the engine wants an operator to see, in whatever shape the
/// environment reads: a terminal gets it on its own row above the live line, a
/// captured pipe gets it as a timestamped `note=` record. Silent when progress
/// is off, because a user who asked for silence meant it.
pub(crate) fn note(msg: &str) {
    warn_or_note(msg, false)
}

/// A note the operator must not be able to silence by accident.
///
/// `APITAP_PROGRESS=0` means "stop telling me about throughput". It has been
/// taken to also mean "stop telling me this connection is unencrypted", which
/// is not a trade anyone would make deliberately. Security-relevant notes go
/// to stderr whatever the progress setting says.
pub(crate) fn warn(msg: &str) {
    warn_or_note(msg, true)
}

fn warn_or_note(msg: &str, always: bool) {
    let fmt = match env_choice() {
        Some(f) => f,
        None if always => Format::Plain,
        None => return,
    };
    let mut err = std::io::stderr().lock();
    let _ = match fmt {
        Format::Json => writeln!(
            err,
            "{{\"ts\":\"{}\",\"event\":\"transfer.note\",\"note\":\"{}\"}}",
            stamp(),
            msg.replace('"', "'")
        ),
        Format::Plain => writeln!(err, "{} apitap note={msg}", stamp()),
        // \r first so the note does not land on top of a half-written live line.
        Format::Live => writeln!(err, "\r\x1b[Kapitap ▸ {msg}"),
    };
    let _ = err.flush();
}

/// CDC drains in windows; the window number says where a long catch-up is.
pub(crate) fn next_window() {
    WINDOW.fetch_add(1, Relaxed);
}

/// Name the table the readout is currently about. Deliberately does NOT touch
/// the estimate: in a multi-table run the row counter is CUMULATIVE across
/// tables, so pairing it with one table's estimate produced "≈100%" while the
/// first table was still streaming. The numerator and the denominator have to
/// describe the same thing.
pub(crate) fn set_label(name: &str) {
    if !ON.load(Relaxed) {
        return;
    }
    if let Ok(mut l) = label().lock() {
        name.clone_into(&mut l);
    }
}

/// The denominator for the whole run. FIRST WRITER WINS, deliberately: a
/// multi-table run sets the sum of every table's estimate before any table
/// starts, and the shared single-table path would otherwise overwrite it with
/// one table's estimate — which is the bug that printed "≈100%" while the
/// first of five tables was still streaming. `-1` = known-unknown.
pub(crate) fn set_total_estimate(est_rows: i64) {
    let _ = EST_ROWS.compare_exchange(UNSET_EST, est_rows, Relaxed, Relaxed);
}

pub(crate) fn is_on() -> bool {
    ON.load(Relaxed)
}

/// Stops the reporter and prints the closing line when the transfer returns —
/// including when it returns an error, which is exactly when a user most wants
/// to know how far it got.
pub(crate) struct Reporter {
    fmt: Format,
    unit: Unit,
    started: Instant,
}

impl Reporter {
    /// Begin reporting. `tables` is 0 for a single-table run.
    pub(crate) fn start(table: &str, unit: Unit, est_rows: i64, tables: usize) -> Option<Self> {
        let fmt = env_choice()?;
        // A second transfer in the same process starts from zero.
        ROWS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        TABLES_DONE.store(0, Relaxed);
        TABLES_TOTAL.store(tables as u64, Relaxed);
        WINDOW.store(0, Relaxed);
        PIPES.store(0, Relaxed);
        EST_ROWS.store(if est_rows == -1 { UNSET_EST } else { est_rows }, Relaxed);
        ROWS_EXACT.store(false, Relaxed);
        ON.store(true, Relaxed);
        if let Ok(mut l) = label().lock() {
            table.clone_into(&mut l);
        }
        let started = Instant::now();
        RUNNING.store(true, Relaxed);
        let tick = interval(fmt);
        std::thread::spawn(move || {
            let mut last = (0u64, 0u64, Instant::now());
            // Wake often enough to notice the stop flag promptly, print only
            // on the cadence: a killed pod should not wait 30 s to exit.
            let step = Duration::from_millis(200);
            let mut waited = Duration::ZERO;
            while RUNNING.load(Relaxed) {
                std::thread::sleep(step);
                waited += step;
                if waited < tick {
                    continue;
                }
                waited = Duration::ZERO;
                let now = Instant::now();
                let rows = ROWS.load(Relaxed);
                let bytes = BYTES.load(Relaxed);
                let secs = now.duration_since(last.2).as_secs_f64().max(0.001);
                let line = render(
                    fmt,
                    unit,
                    started.elapsed(),
                    rows,
                    bytes,
                    ((rows - last.0) as f64 / secs) as u64,
                    ((bytes - last.1) as f64 / secs) as u64,
                    false,
                );
                emit(fmt, &line);
                last = (rows, bytes, now);
            }
        });
        Some(Reporter { fmt, unit, started })
    }

    /// The closing line. `rows` is the authoritative count the caller is about
    /// to report, so the summary can never disagree with `TransferReport`.
    pub(crate) fn finish(self, rows: u64) {
        RUNNING.store(false, Relaxed);
        ON.store(false, Relaxed);
        let elapsed = self.started.elapsed();
        let bytes = BYTES.load(Relaxed);
        let secs = elapsed.as_secs_f64().max(0.001);
        if self.fmt == Format::Live {
            // Leave the live line behind rather than overwriting it with the
            // summary halfway through.
            let _ = writeln!(std::io::stderr());
        }
        let line = render(
            self.fmt,
            self.unit,
            elapsed,
            rows,
            bytes,
            (rows as f64 / secs) as u64,
            (bytes as f64 / secs) as u64,
            true,
        );
        // Always a whole line: this is the last thing the engine prints, and
        // whatever the caller prints next must start on its own row.
        let mut err = std::io::stderr().lock();
        let _ = writeln!(err, "{line}");
        let _ = err.flush();
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        // finish() consumes self, so reaching Drop means the transfer unwound
        // early. Stop the thread; the error itself is the caller's to report.
        RUNNING.store(false, Relaxed);
        ON.store(false, Relaxed);
    }
}

fn emit(fmt: Format, line: &str) {
    let mut err = std::io::stderr().lock();
    let _ = match fmt {
        // \r rewrites the line in place; \x1b[K clears whatever the previous,
        // longer line left behind.
        Format::Live => write!(err, "\r\x1b[K{line}"),
        _ => writeln!(err, "{line}"),
    };
    // Flush every line: a container's stderr is a pipe, and a buffered pipe
    // means the operator sees nothing until the process exits.
    let _ = err.flush();
}

#[allow(clippy::too_many_arguments)]
fn render(
    fmt: Format,
    unit: Unit,
    elapsed: Duration,
    rows: u64,
    bytes: u64,
    rows_s: u64,
    bytes_s: u64,
    done: bool,
) -> String {
    let name = label().lock().map(|l| l.clone()).unwrap_or_default();
    let (tdone, ttotal) = (TABLES_DONE.load(Relaxed), TABLES_TOTAL.load(Relaxed));
    let est = EST_ROWS.load(Relaxed);
    let exact = ROWS_EXACT.load(Relaxed);
    let window = WINDOW.load(Relaxed);
    let pipes = PIPES.load(Relaxed);
    match fmt {
        Format::Json => {
            let mut s = format!(
                "{{\"ts\":\"{}\",\"event\":\"{}\",\"table\":\"{}\",\"{}\":{},\
                 \"rows_exact\":{},\"bytes\":{},\"{}_per_s\":{},\"bytes_per_s\":{},\
                 \"elapsed_s\":{:.1}",
                stamp(),
                if done { "transfer.done" } else { "transfer.progress" },
                name.replace('"', "'"),
                unit.word(),
                rows,
                exact || done,
                bytes,
                unit.word(),
                rows_s,
                bytes_s,
                elapsed.as_secs_f64(),
            );
            if ttotal > 0 {
                s.push_str(&format!(",\"tables_done\":{tdone},\"tables_total\":{ttotal}"));
            }
            if window > 0 {
                s.push_str(&format!(",\"window\":{window}"));
            }
            if pipes > 0 {
                s.push_str(&format!(",\"pipes\":{pipes}"));
            }
            if let Some(p) = percent(rows, est, exact) {
                s.push_str(&format!(",\"percent_est\":{p:.1}"));
            }
            s.push('}');
            s
        }
        Format::Plain => {
            let mut s = format!(
                "{} apitap {} table={} {}={} bytes={} {}_per_s={} bytes_per_s={} elapsed_s={:.1}",
                stamp(),
                if done { "done" } else { "progress" },
                name,
                unit.word(),
                rows,
                bytes,
                unit.word(),
                rows_s,
                bytes_s,
                elapsed.as_secs_f64(),
            );
            if !exact && !done {
                s.push_str(" rows_live=no");
            }
            if ttotal > 0 {
                s.push_str(&format!(" tables={tdone}/{ttotal}"));
            }
            if window > 0 {
                s.push_str(&format!(" window={window}"));
            }
            if pipes > 0 {
                s.push_str(&format!(" pipes={pipes}"));
            }
            if let Some(p) = percent(rows, est, exact) {
                s.push_str(&format!(" percent_est={p:.1}"));
            }
            s
        }
        Format::Live => {
            let head = if done {
                format!("apitap ▸ done · {name}")
            } else if ttotal > 0 {
                format!("apitap ▸ {tdone}/{ttotal} tables · {name}")
            } else {
                format!("apitap ▸ {name}")
            };
            let mut s = head;
            if exact || done {
                s.push_str(&format!(" · {} {}", commas(rows), unit.word()));
            }
            s.push_str(&format!(" · {}", human_bytes(bytes)));
            if let Some(p) = percent(rows, est, exact) {
                s.push_str(&format!(" · ≈{p:.0}% (est)"));
            }
            if done {
                s.push_str(&format!(" · {} · avg {}/s", clock(elapsed), short(rows_s)));
            } else {
                if exact {
                    s.push_str(&format!(" · {}/s", short(rows_s)));
                } else {
                    s.push_str(&format!(" · {}/s", human_bytes(bytes_s)));
                }
                s.push_str(&format!(" · {}", clock(elapsed)));
                if window > 0 {
                    s.push_str(&format!(" · window {window}"));
                }
                if pipes > 0 {
                    s.push_str(&format!(" · {pipes} pipes"));
                }
            }
            s
        }
    }
}

/// Percent against the planner's row estimate — never against a number we made
/// up. `None` when the catalog had no estimate or the lane's live count is not
/// exact, because a percentage of a guess is two guesses.
fn percent(rows: u64, est: i64, exact: bool) -> Option<f64> {
    if !exact || est <= 0 {
        return None;
    }
    Some(((rows as f64 / est as f64) * 100.0).min(99.9))
}

fn stamp() -> String {
    // RFC3339 UTC without pulling a date crate into the hot path: seconds
    // since the epoch is unambiguous and every log collector parses it, but
    // humans reading `docker logs` want a real clock, so render both.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Howard Hinnant's days→civil algorithm (public domain), so a timestamp does
/// not cost a dependency.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn short(n: u64) -> String {
    match n {
        0..=9_999 => commas(n),
        10_000..=999_999 => format!("{:.0}K", n as f64 / 1e3),
        1_000_000..=999_999_999 => format!("{:.2}M", n as f64 / 1e6),
        _ => format!("{:.2}B", n as f64 / 1e9),
    }
}

fn human_bytes(n: u64) -> String {
    const K: f64 = 1024.0;
    let f = n as f64;
    if f < K {
        format!("{n} B")
    } else if f < K * K {
        format!("{:.0} KB", f / K)
    } else if f < K * K * K {
        format!("{:.1} MB", f / (K * K))
    } else {
        format!("{:.2} GB", f / (K * K * K))
    }
}

fn clock(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counters are deliberately global (one transfer per process, and a
    /// slots=N run aggregates for free) — which means two tests touching them
    /// in parallel corrupt each other. They found this themselves: the JSON
    /// test passed alone and failed in the suite. Anything reading or writing
    /// the globals takes this lock first.
    fn reset_est(v: i64) {
        EST_ROWS.store(UNSET_EST, Relaxed);
        set_total_estimate(v);
    }

    fn exclusive() -> std::sync::MutexGuard<'static, ()> {
        static L: std::sync::Mutex<()> = std::sync::Mutex::new(());
        L.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn numbers_render_the_way_a_human_reads_them() {
        assert_eq!(commas(0), "0");
        assert_eq!(commas(497_622_597), "497,622,597");
        assert_eq!(short(9_999), "9,999");
        assert_eq!(short(412_000), "412K");
        assert_eq!(short(1_440_000), "1.44M");
        assert_eq!(human_bytes(900), "900 B");
        assert_eq!(human_bytes(3_665_397_862), "3.41 GB");
        assert_eq!(clock(Duration::from_secs(30)), "0:30");
        assert_eq!(clock(Duration::from_secs(401)), "6:41");
        assert_eq!(clock(Duration::from_secs(7_384)), "2:03:04");
    }

    #[test]
    fn percent_only_exists_when_both_halves_are_real() {
        // A catalog estimate and an exact live count: a percentage is fair.
        assert_eq!(percent(50, 100, true), Some(50.0));
        // No estimate (the catalog never analyzed the table).
        assert_eq!(percent(50, -1, true), None);
        assert_eq!(percent(50, 0, true), None);
        // The lane relays bytes without decoding rows: the numerator is not a
        // row count, so a percentage would be fiction.
        assert_eq!(percent(50, 100, false), None);
        // Estimates undershoot; the readout must never claim completion.
        assert_eq!(percent(500, 100, true), Some(99.9));
    }

    #[test]
    fn the_timestamp_is_a_real_utc_date() {
        // 20,682 days after the epoch is 2026-08-17 (20,683 is the 18th —
        // this expectation was off by one until the test said so).
        assert_eq!(civil_from_days(20_682), (2026, 8, 17));
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // A leap day, because that is where date maths goes wrong.
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
    }

    #[test]
    fn off_means_the_counters_cost_nothing() {
        let _g = exclusive();
        ON.store(false, Relaxed);
        ROWS.store(0, Relaxed);
        add_rows(1_000);
        add_bytes(1_000);
        assert_eq!(ROWS.load(Relaxed), 0, "counters must be inert when off");
        ON.store(true, Relaxed);
        add_rows(7);
        assert_eq!(ROWS.load(Relaxed), 7);
        ON.store(false, Relaxed);
    }

    #[test]
    fn a_json_line_is_one_object_and_carries_the_unit() {
        let _g = exclusive();
        ON.store(true, Relaxed);
        set_label("bank_transfer");
        reset_est(500_000_000);
        // A percentage needs a lane that actually counts rows, and exactness is
        // claimed by add_rows — so stand in for one, exactly as the MySQL
        // workers and the pg transcoder do.
        add_rows(0);
        let line = render(
            Format::Json,
            Unit::Rows,
            Duration::from_secs(30),
            12_480_000,
            3_665_397_862,
            412_000,
            122_000_000,
            false,
        );
        ON.store(false, Relaxed);
        assert!(line.starts_with('{') && line.ends_with('}'));
        assert!(!line.contains('\n'), "one object per line");
        assert!(line.contains("\"rows\":12480000"));
        assert!(line.contains("\"rows_per_s\":412000"));
        assert!(line.contains("\"table\":\"bank_transfer\""));
        assert!(line.contains("\"percent_est\":2.5"));
    }

    #[test]
    fn cdc_counts_changes_not_rows() {
        let _g = exclusive();
        ON.store(true, Relaxed);
        set_label("orders");
        reset_est(-1);
        let line = render(
            Format::Plain,
            Unit::Changes,
            Duration::from_secs(11),
            1_240_000,
            0,
            118_000,
            0,
            false,
        );
        ON.store(false, Relaxed);
        assert!(line.contains("changes=1240000"));
        assert!(line.contains("changes_per_s=118000"));
        // No estimate for a change stream — nothing to be a percentage of.
        assert!(!line.contains("percent_est"));
    }
}
