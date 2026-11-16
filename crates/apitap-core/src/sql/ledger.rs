//! The statement's memory by class (W1, `docs/design/2026-11-12-sql-bounded-memory-and-scaling.md`).
//!
//! The debug account: with `APITAP_DEBUG` set, one `[mem]` line per phase
//! gives what each class of structure holds (the engine's own count of its
//! bytes) next to the process's resident bytes and the cgroup's `anon`,
//! `file`, `kernel` and `sock` at the highest the cgroup reached since the
//! line before. The difference between the cgroup's high mark and the
//! classes' sum is what nobody counts: the read pipes' buffers, the kernel's
//! socket buffers, the allocator's slack. The line and its sampler thread
//! (started by the first line) only run with `APITAP_DEBUG`; without it a
//! mark costs its arguments, counted at a phase boundary.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

/// The cgroup's highest `memory.current` since the last [`mark`], and its
/// `memory.stat` split there (anon, file, kernel, sock).
static HIGH: AtomicU64 = AtomicU64::new(0);
static AT_HIGH: Mutex<[u64; 4]> = Mutex::new([0; 4]);
/// glibc's split (in use, free, mmapped) taken with the high mark.
static AT_HIGH_MALLOC: Mutex<(u64, u64, u64)> = Mutex::new((0, 0, 0));
/// The most glibc held free at any high mark this process (the report's
/// line: glibc free is bounded by the cage, not reserved).
static FREE_MAX: AtomicU64 = AtomicU64::new(0);
static SAMPLER: OnceLock<()> = OnceLock::new();

fn cgroup_now() -> Option<(u64, [u64; 4])> {
    let cur: u64 = std::fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?.trim().parse().ok()?;
    let st = std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let mut v = [0u64; 4];
    for l in st.lines() {
        let mut it = l.split_whitespace();
        let (Some(k), Some(n)) = (it.next(), it.next()) else { continue };
        let i = match k {
            "anon" => 0,
            "file" => 1,
            "kernel" => 2,
            "sock" => 3,
            _ => continue,
        };
        v[i] = n.parse().unwrap_or(0);
    }
    Some((cur, v))
}

fn sample() {
    if let Some((cur, v)) = cgroup_now() {
        if cur > HIGH.load(Relaxed) {
            HIGH.store(cur, Relaxed);
            *AT_HIGH.lock().unwrap_or_else(|e| e.into_inner()) = v;
            *AT_HIGH_MALLOC.lock().unwrap_or_else(|e| e.into_inner()) = malloc_split().unwrap_or_default();
        }
    }
}

/// The sampler: every 20 ms, the cgroup's current bytes (debug runs only).
fn start() {
    SAMPLER.get_or_init(|| {
        if cgroup_now().is_some() {
            let _ = std::thread::Builder::new().name("apitap-mem".into()).spawn(|| loop {
                sample();
                std::thread::sleep(std::time::Duration::from_millis(20));
            });
        }
    });
}

/// Seconds since this process started (`/proc/self/stat` starttime against
/// `/proc/uptime`): the `[mem]` lines' clock, so a phase's wall and CPU
/// (and the wall it spent with the CPU idle) read off two lines.
fn since_start() -> f64 {
    let up: f64 = std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(0.0);
    let start = std::fs::read_to_string("/proc/self/stat").ok().and_then(|s| {
        let rest = &s[s.rfind(')')? + 2..];
        rest.split_whitespace().nth(19)?.parse::<f64>().ok()
    });
    // SAFETY: sysconf reads a constant.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    match start {
        Some(t) if hz > 0.0 => up - t / hz,
        _ => 0.0,
    }
}

/// Socket memory not in a read socket's receive buffer: the send buffers
/// and the few control connections (catalog probes, snapshot holders).
const SOCK_SLACK: u64 = 4 << 20;

/// The process's own anonymous memory when the first statement began (the
/// interpreter, the runtime, the connections pinned): the floor every
/// reservation sits on, measured.
static FLOOR: AtomicU64 = AtomicU64::new(0);

/// A statement begins: the sampler starts (debug runs) and the floor is
/// taken once per process.
pub(super) fn begin() {
    if !super::dbg() {
        return;
    }
    start();
    // The statement before ran on past its last mark (the answer built, its
    // structures dropped): that tail is held to its last reservation here,
    // and the first interval of this statement starts now.
    if PREV_RESERVED.load(Relaxed) > 0 {
        mark("tail of the statement before", &[]);
    }
    if FLOOR.load(Relaxed) == 0 {
        if let Some((_, v)) = cgroup_now() {
            FLOOR.store(v[0].max(1), Relaxed);
        }
    }
    sample();
}

/// glibc's own account (`mallinfo2`): bytes in use, free inside its arenas
/// (held, not returned), and mmapped chunks; `None` off glibc, and on a
/// glibc older than 2.33, which has no `mallinfo2`. The symbol is looked up
/// once at run time, never linked: the wheels are manylinux2014 (glibc
/// 2.17) and load with `-z now`, so a direct call failed `import apitap` on
/// every older glibc.
pub(super) fn malloc_split() -> Option<(u64, u64, u64)> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let f = mallinfo2_fn()?;
        // SAFETY: `f` is glibc's own `mallinfo2` (resolved by name below),
        // which takes nothing and only reads the allocator's statistics.
        let mi = unsafe { f() };
        Some((mi.uordblks as u64, mi.fordblks as u64, mi.hblkhd as u64))
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    None
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
type Mallinfo2 = unsafe extern "C" fn() -> libc::mallinfo2;

/// `mallinfo2` from the running glibc, or `None` (older glibc), resolved once.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn mallinfo2_fn() -> Option<Mallinfo2> {
    #[cfg(test)]
    if MALLINFO2_ABSENT.load(Relaxed) {
        return None;
    }
    static F: OnceLock<Option<Mallinfo2>> = OnceLock::new();
    *F.get_or_init(|| {
        // SAFETY: a NUL-terminated name looked up in the global scope; a
        // null result is handled, a non-null one is glibc's `mallinfo2`,
        // whose signature `Mallinfo2` is.
        let p = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"mallinfo2".as_ptr()) };
        (!p.is_null()).then(|| unsafe { std::mem::transmute::<*mut libc::c_void, Mallinfo2>(p) })
    })
}

/// Tests: behave as on a glibc without `mallinfo2`.
#[cfg(test)]
pub(super) static MALLINFO2_ABSENT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Slack on reserved against actual: the allocator's and the kernel's own
/// rounding, a twentieth of the actual and 8 MiB, never past 16 MiB.
fn slack(actual: u64) -> u64 {
    ((8 << 20) + actual / 20).min(16 << 20)
}

/// What was reserved at the mark before: the actual at a mark is the high
/// mark of the interval since that one, so the interval is reserved at the
/// larger of its two ends (a structure dropped at the mark was held through
/// the interval it closes).
static PREV_RESERVED: AtomicU64 = AtomicU64::new(0);

/// The bytes every stage table and writer holds in memory now.
pub(super) fn stage_bytes() -> u64 {
    super::mem::stage_held()
}

/// One `[mem]` line for `phase`: the classes' bytes as the engine counts
/// them, the process's resident bytes, and the cgroup's high mark since the
/// last line with its split. Debug runs only.
pub(super) fn mark(phase: &str, classes: &[(&str, u64)]) {
    if !super::dbg() {
        return;
    }
    start();
    sample();
    let high = HIGH.swap(0, Relaxed);
    let at = std::mem::take(&mut *AT_HIGH.lock().unwrap_or_else(|e| e.into_inner()));
    let at_m = std::mem::take(&mut *AT_HIGH_MALLOC.lock().unwrap_or_else(|e| e.into_inner()));
    let mib = |b: u64| b as f64 / (1u64 << 20) as f64;
    let mut sum = 0u64;
    let mut s = String::new();
    for (k, b) in classes.iter().chain(std::iter::once(&("stage", stage_bytes()))) {
        sum += b;
        s.push_str(&format!(" {k}={:.1}", mib(*b)));
    }
    // The read sockets' receive buffers reserved (the most since the last
    // line) against the cgroup's socket memory at its high mark.
    let rcv = crate::rcvbuf::take_high();
    let sock_over = at[3] > rcv + SOCK_SLACK;
    // Reserved: the floor, the engine's structures, the read pipes' user
    // price, their receive buffers, the spill writers' buffers; against the
    // cgroup's non-file memory at the high mark (anon + kernel + sock).
    let user = crate::rcvbuf::take_user_high();
    let reserved = FLOOR.load(Relaxed) + sum + user + rcv + super::mem::spill_reserved();
    // What the engine holds (B8 decision, architect 2026-11-14): the cgroup's
    // anon at the high mark less what glibc kept on its free lists there
    // ([`retained`], the rule `free_pipes` prices by), with kernel and sock.
    // glibc's free is not reserved; the cage bounds it, and the peak
    // condition guards it. `APITAP_LEDGER_RAW=1` checks raw anon instead
    // (the leg's control).
    let free_held = retained(at[0], at_m);
    FREE_MAX.fetch_max(at_m.1, Relaxed);
    let raw = std::env::var("APITAP_LEDGER_RAW").is_ok_and(|v| v == "1");
    let actual = if raw { at[0] } else { at[0] - free_held } + at[2] + at[3];
    let interval = reserved.max(PREV_RESERVED.swap(reserved, Relaxed));
    let over = actual > interval + slack(actual);
    let malloc = if malloc_split().is_none() {
        " | malloc split unknown (no mallinfo2): held = anon".to_string()
    } else {
        format!(
            " | malloc at high in-use={:.1} free={:.1} mmap={:.1} held anon={:.1} | glibc free max={:.1}",
            mib(at_m.0),
            mib(at_m.1),
            mib(at_m.2),
            mib(at[0] - free_held),
            mib(FREE_MAX.load(Relaxed))
        )
    };
    eprintln!(
        "[mem] t={:.1}s cpu={:.2}s {phase}:{s} pipes={:.1} | counted={:.1} rss={:.1} | cgroup high={:.1} anon={:.1} file={:.1} kernel={:.1} sock={:.1} (MiB) | reserved={:.1} actual={:.1}{} | rcvbuf reserved={:.1}{}{malloc}",
        since_start(),
        super::cpu_s(),
        mib(user),
        mib(sum),
        mib(super::semi::rss_bytes().unwrap_or(0)),
        mib(high),
        mib(at[0]),
        mib(at[1]),
        mib(at[2]),
        mib(at[3]),
        mib(reserved),
        mib(actual),
        if over { " OVER_RESERVED" } else { "" },
        mib(rcv),
        if sock_over { " SOCK_OVER" } else { "" }
    );
    // The next phase's high mark starts from where the cgroup is now.
    if let Some((cur, v)) = cgroup_now() {
        HIGH.store(cur, Relaxed);
        *AT_HIGH.lock().unwrap_or_else(|e| e.into_inner()) = v;
        *AT_HIGH_MALLOC.lock().unwrap_or_else(|e| e.into_inner()) = malloc_split().unwrap_or_default();
    }
}

/// Of `anon`, what glibc keeps on its free lists: its free bytes, at most
/// the anonymous bytes past its live ones (in use and mmapped), so pages a
/// trim already gave back are not taken off twice.
pub(super) fn retained(anon: u64, (used, free, mmap): (u64, u64, u64)) -> u64 {
    free.min(anon.saturating_sub(used + mmap))
}

// ---------------------------------------------------------------------------
// Sizes as functions of the memory limit (W1 B4). Each was an absolute
// constant measured in the 256 MiB cage; at 256 MiB each function gives that
// constant exactly (pinned by the unit test below), so every plan of that
// cage is the plan it was; at other limits the size follows the limit, with
// a floor where a small cage must not shrink it. Without a known limit the
// 256 MiB values hold, as the constants did.

/// The cage every absolute size was measured in.
pub(super) const REF_LIMIT: u64 = 256 << 20;

fn lim(limit: Option<u64>) -> u64 {
    limit.unwrap_or(REF_LIMIT)
}

/// `limit × num / den`, at least `floor`.
fn share_at(limit: Option<u64>, num: u64, den: u64, floor: u64) -> u64 {
    (lim(limit) / den * num).max(floor)
}

fn limit_now() -> Option<u64> {
    crate::pipeline::mem_limit_bytes()
}

/// The largest direct-address build array (was `DIRECT_MAX`, 64 MiB): a quarter.
pub(super) fn direct_max_at(limit: Option<u64>) -> u64 {
    share_at(limit, 1, 4, 16 << 20)
}
pub(super) fn direct_max() -> u64 {
    direct_max_at(limit_now())
}

/// What a probe holds past its pipes and the build (was `PROBE_HEADROOM`,
/// 64 MiB): a quarter, never under 64 MiB (the host process alone is ~8 MiB
/// of anonymous memory plus its batches; a smaller cage keeps today's room).
pub(super) fn probe_headroom_at(limit: Option<u64>) -> u64 {
    share_at(limit, 1, 4, 64 << 20)
}
pub(super) fn probe_headroom() -> u64 {
    probe_headroom_at(limit_now())
}

/// The most a key gate may take (was `GATE_MAX_BYTES`, 16 MiB): a sixteenth.
pub(super) fn gate_max_bytes_at(limit: Option<u64>) -> u64 {
    share_at(limit, 1, 16, 4 << 20)
}
pub(super) fn gate_max_bytes() -> u64 {
    gate_max_bytes_at(limit_now())
}

/// A dimension's key range held as a direct array (was `DIM_DIRECT_MAX`,
/// 64 MiB of keys): a quarter.
pub(super) fn dim_direct_max_at(limit: Option<u64>) -> u64 {
    share_at(limit, 1, 4, 16 << 20)
}
pub(super) fn dim_direct_max() -> u64 {
    dim_direct_max_at(limit_now())
}

/// The room a HAVING subquery's range read keeps beside its arrays (was
/// 32 MiB): an eighth.
pub(super) fn having_room_at(limit: Option<u64>) -> u64 {
    share_at(limit, 1, 8, 16 << 20)
}
pub(super) fn having_room() -> u64 {
    having_room_at(limit_now())
}

/// `probe_pipe_cap`'s "held" thresholds (were 32 / 40 / 48 / 96 / 112 MiB):
/// an eighth, 5/32, 3/16, 3/8 and 7/16 of the limit, no floor (a smaller
/// cage gives fewer pipes beside the same map).
pub(super) fn held_thresholds_at(limit: Option<u64>) -> [u64; 5] {
    let l = lim(limit);
    [l / 8, l / 32 * 5, l / 16 * 3, l / 8 * 3, l / 16 * 7]
}
pub(super) fn held_thresholds() -> [u64; 5] {
    held_thresholds_at(limit_now())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// At 256 MiB every size is the constant it replaced: the 256 MiB plans
    /// are the plans they were.
    /// On a glibc without `mallinfo2` (older than 2.33, as manylinux2014
    /// allows) the split is unknown and nothing is taken off anon; the
    /// symbol is never linked (W1 review R1).
    #[test]
    fn without_mallinfo2_the_split_is_unknown_and_held_is_anon() {
        MALLINFO2_ABSENT.store(true, Relaxed);
        assert!(malloc_split().is_none());
        assert_eq!(retained(100 << 20, malloc_split().unwrap_or_default()), 0);
        MALLINFO2_ABSENT.store(false, Relaxed);
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        assert!(malloc_split().is_some(), "the build host's glibc has mallinfo2");
    }

    #[test]
    fn sizes_at_256_mib_are_the_old_constants() {
        let l = Some(256u64 << 20);
        assert_eq!(direct_max_at(l), 64 << 20);
        assert_eq!(probe_headroom_at(l), 64 << 20);
        assert_eq!(gate_max_bytes_at(l), 16 << 20);
        assert_eq!(dim_direct_max_at(l), 64 << 20);
        assert_eq!(having_room_at(l), 32 << 20);
        assert_eq!(held_thresholds_at(l), [32 << 20, 40 << 20, 48 << 20, 96 << 20, 112 << 20]);
        // No known limit: the same values.
        assert_eq!(direct_max_at(None), 64 << 20);
        assert_eq!(held_thresholds_at(None), held_thresholds_at(l));
    }

    /// They follow the limit both ways, the floors holding small cages.
    #[test]
    fn sizes_follow_the_limit_with_floors() {
        let g = Some(8u64 << 30);
        assert_eq!(direct_max_at(g), 2 << 30);
        assert_eq!(gate_max_bytes_at(g), 512 << 20);
        let s = Some(128u64 << 20);
        assert_eq!(direct_max_at(s), 32 << 20);
        assert_eq!(probe_headroom_at(s), 64 << 20, "a small cage keeps today's probe room");
        assert_eq!(gate_max_bytes_at(Some(32 << 20)), 4 << 20);
        assert_eq!(held_thresholds_at(s), [16 << 20, 20 << 20, 24 << 20, 48 << 20, 56 << 20]);
    }
}
