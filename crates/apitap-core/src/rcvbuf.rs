//! Receive buffers by law (W1 B1, `docs/design/2026-11-12-sql-bounded-memory-and-scaling.md` §6.2).
//!
//! A read socket's receive buffer is kernel memory the cgroup charges
//! (`memory.stat` `sock`). The raw planes asked for 4 MiB on every socket
//! (the kernel doubles it): eight `sql()` pipes held 63 MiB of a 256 MiB cage
//! while the 0.5-core reader was behind, priced nowhere. A buffer cut to a
//! flat 256 KiB took TPC-H Q9 over a 67 ms path from 124 s to 354 s at the
//! same CPU. So the size follows what one round trip must carry:
//!
//! ```text
//! need     = cores × RATE × rtt            bytes the cage consumes in one round trip
//! per_pipe = clamp(2 × SKEW × need / pipes, FLOOR, CEIL)
//! ```
//!
//! `rtt` is the connection's own (`TCP_INFO`, after the startup exchange);
//! the effective size is read back and reserved ([`reserved`]) for the
//! process's memory account, and [`pipes_within`] answers a reservation past
//! its share with fewer pipes, never a window under the law.
//! `APITAP_RECV_BUF_BYTES` overrides (`0`/`off`: the kernel's autotuning).
//! The read lanes (`read()`, `sql()`) use it; the transfer planes keep 4 MiB.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Bytes received per CPU-second of the receive-heaviest TPC-H query in the
/// phase A profile: Q5 on the 10g seed (31% of its CPU in the kernel's
/// receive path), measured 2026-11-12 on the docker bridge at 0.5 core /
/// 256 MiB, 0.67.2 HEAD: 3935 MiB on the container's eth0 for 10.8 CPU-s
/// (Q2 435, Q8 359, Q11 317 MiB per CPU-s on the same run).
pub(crate) const RATE: f64 = 364.0 * 1048576.0;

/// Pipes are not equally loaded: a pipe may carry this many times its share.
pub(crate) const SKEW: f64 = 2.0;

/// The smallest buffer asked for (requested bytes; the kernel doubles it):
/// the smallest whose wall and CPU stayed inside 0.67.2's run-to-run range
/// on the 10g queries that read through eight Postgres pipes (W1 B1 sweep,
/// 0.5 core / 256 MiB, n = 3 interleaved; a0 Q1 79.0 s / 39.4 s CPU, Q11
/// 55.5 / 23.7, Q17 30.4 / 11.7): at 1 MiB Q1 75.1 / 37.5, Q11 55.9 / 24.0,
/// Q17 30.6 / 11.8; at 256 KiB Q11 took 58.7 / 24.9 (more, smaller reads),
/// at 512 KiB 56.9 / 24.7. Eight pipes then hold 16 MiB, not 64.
pub(crate) const FLOOR: usize = 1 << 20;

/// The largest: the value every raw-plane socket asked for before the law.
pub(crate) const CEIL: usize = 4 << 20;

/// The receive buffer to ask for on one of `pipes` read sockets with
/// round-trip `rtt` in a cage of `cores`.
pub(crate) fn law(pipes: usize, rtt: Duration, cores: f64) -> usize {
    let need = cores.max(0.01) * RATE * rtt.as_secs_f64();
    let per = 2.0 * SKEW * need / pipes.max(1) as f64;
    if !per.is_finite() || per >= CEIL as f64 {
        return CEIL;
    }
    (per as usize).clamp(FLOOR, CEIL)
}

/// What one socket asks for: the law is the least; with `room` in the
/// receive buffers' share each socket gets up to it (at most [`CEIL`]), so a
/// cage with room keeps today's size and only a tight one goes down toward
/// the law. No limit known (`None`): [`CEIL`]. (W1 B5: at the law's floor
/// the short 10g queries paid 2-10% more CPU in receive wakeups, TPC-H Q19
/// 3.8 -> 4.2 s.)
pub(crate) fn size(pipes: usize, rtt: Duration, cores: f64, room: Option<u64>) -> usize {
    let law = law(pipes, rtt, cores);
    let room = room.map_or(CEIL, |r| usize::try_from(r).unwrap_or(CEIL).min(CEIL));
    law.max(room)
}

/// One socket's room when a read of `pipes` opens beside sockets already
/// holding `taken` bytes: what they leave of the share of `limit`, over the
/// read's sockets (the kernel doubles what is asked). Each read sized as if
/// alone held eight one-pipe parts to 64 MiB of a 32 MiB share (W1 review R8).
pub(crate) fn room_at(pipes: usize, limit: u64, taken: u64) -> u64 {
    share(limit).saturating_sub(taken) / (2 * pipes.max(1) as u64)
}

/// The receive buffers of one read's sockets, planned once before any of
/// them opens: their count and each one's room ([`room_at`], from what the
/// process's open read sockets hold then).
///
/// The plan holds its sockets' room in the account until they open (each
/// socket's real reservation takes its share back), so reads planned back to
/// back each see the ones before: with nothing held, eight one-pipe parts
/// started together all saw an empty account (W1 review 2, R8).
#[derive(Clone, Debug)]
pub(crate) struct Sockets {
    pub(crate) pipes: usize,
    room: Option<u64>,
    held: Option<std::sync::Arc<Provisional>>,
}

/// A read's provisional hold on the receive buffers' account: `per` for each
/// socket not opened yet; what is left goes back when the read's plan drops.
#[derive(Debug)]
struct Provisional {
    left: AtomicU64,
    per: u64,
}

impl Provisional {
    /// One socket opened: its share of the hold back to the account.
    fn settle(&self) {
        if let Ok(prev) = self.left.fetch_update(Relaxed, Relaxed, |l| Some(l.saturating_sub(self.per))) {
            let n = prev.min(self.per);
            if n > 0 {
                let _ = RESERVED.fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(n)));
            }
        }
    }
}

impl Drop for Provisional {
    fn drop(&mut self) {
        let n = self.left.swap(0, Relaxed);
        if n > 0 {
            let _ = RESERVED.fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(n)));
        }
    }
}

impl Sockets {
    pub(crate) fn plan(pipes: usize) -> Sockets {
        Sockets::plan_in(pipes, crate::pipeline::mem_limit_bytes())
    }

    /// Sockets this side does not size (a transfer plane's): no room, no hold.
    pub(crate) fn unplanned(pipes: usize) -> Sockets {
        Sockets { pipes, room: None, held: None }
    }

    /// [`plan`](Self::plan) under `limit`: the room read and this read's hold
    /// added in one step.
    pub(crate) fn plan_in(pipes: usize, limit: Option<u64>) -> Sockets {
        let Some(l) = limit else { return Sockets { pipes, room: None, held: None } };
        let (mut room, mut per) = (0, 0);
        let pipes1 = pipes.max(1) as u64;
        let _ = RESERVED.fetch_update(Relaxed, Relaxed, |taken| {
            room = room_at(pipes, l, taken);
            per = 2 * room.clamp(FLOOR as u64, CEIL as u64);
            Some(taken + per * pipes1)
        });
        HIGH.fetch_max(RESERVED.load(Relaxed), Relaxed);
        Sockets { pipes, room: Some(room), held: Some(std::sync::Arc::new(Provisional { left: AtomicU64::new(per * pipes1), per })) }
    }

    /// A socket of the plan opened (its real reservation follows).
    fn settle(&self) {
        if let Some(h) = &self.held {
            h.settle();
        }
    }
}

fn cores() -> f64 {
    crate::pipeline::cpu_limit_cores().unwrap_or_else(|| std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64))
}

/// `APITAP_RECV_BUF_BYTES`, one parser for every plane: unset or not a
/// size → [`EnvBuf::Unset`] (the plane's own default: the law on the read
/// lanes, 4 MiB on the transfer planes); `0`/`off` → the kernel's
/// autotuning; a number → that size. (A typo was autotuning on the read
/// lanes and 4 MiB on the transfer planes: W1 review R10.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnvBuf {
    Unset,
    Autotune,
    Bytes(usize),
}

pub(crate) fn env_buf() -> EnvBuf {
    parse_env_buf(std::env::var("APITAP_RECV_BUF_BYTES").ok().as_deref())
}

/// `APITAP_RECV_BUF_BYTES` set to something that is neither a byte count
/// nor `0`/`off` is refused before a run starts (it used to fall back to the
/// default without a word: QA p13).
pub(crate) fn check_env() -> crate::Result<()> {
    match std::env::var("APITAP_RECV_BUF_BYTES") {
        Err(std::env::VarError::NotPresent) => Ok(()),
        Err(std::env::VarError::NotUnicode(v)) => Err(bad_env(&v.to_string_lossy())),
        Ok(v) if parse_env_buf(Some(&v)) == EnvBuf::Unset => Err(bad_env(&v)),
        Ok(_) => Ok(()),
    }
}

fn bad_env(v: &str) -> crate::Error {
    crate::Error::InvalidInput(format!(
        "APITAP_RECV_BUF_BYTES='{v}' is not a byte count: set a positive number of bytes (e.g. 4194304), \
         0 or off for the kernel's autotuning, or unset it for the default"
    ))
}

fn parse_env_buf(v: Option<&str>) -> EnvBuf {
    // Untrimmed, as the transfer planes parsed it in 0.67.2.
    match v {
        None => EnvBuf::Unset,
        Some(v) if v == "0" || v.eq_ignore_ascii_case("off") => EnvBuf::Autotune,
        Some(v) => v.parse::<usize>().ok().filter(|n| *n > 0).map_or(EnvBuf::Unset, EnvBuf::Bytes),
    }
}

/// The effective receive buffer of every open read socket (bytes the
/// kernel may hold for them), and the highest it reached since
/// [`take_high`].
static RESERVED: AtomicU64 = AtomicU64::new(0);
static HIGH: AtomicU64 = AtomicU64::new(0);
/// Per endpoint (`host:port`), the latest round trip a read socket to it
/// measured (µs): what [`pipes_within`] prices the next read there at. (A
/// process-wide maximum let one far server shrink every later read: W1
/// review R7.)
static RTTS: std::sync::Mutex<Vec<(String, u64)>> = std::sync::Mutex::new(Vec::new());

fn rtt_note(endpoint: &str, r: Duration) {
    let mut v = RTTS.lock().unwrap_or_else(|e| e.into_inner());
    let us = r.as_micros() as u64;
    match v.iter_mut().find(|(e, _)| e == endpoint) {
        Some(x) => x.1 = us,
        None => v.push((endpoint.to_string(), us)),
    }
}

fn rtt_at(endpoint: &str) -> Duration {
    let v = RTTS.lock().unwrap_or_else(|e| e.into_inner());
    Duration::from_micros(v.iter().find(|(e, _)| e == endpoint).map_or(0, |x| x.1))
}

/// A source URL's `host:port`, the key round trips are kept by.
pub(crate) fn endpoint_of(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |x| x.1);
    let hostport = rest.split(['/', '?']).next().unwrap_or(rest);
    hostport.rsplit_once('@').map_or(hostport, |x| x.1).to_ascii_lowercase()
}

/// `net.core.rmem_max`: the most `SO_RCVBUF` may ask (Linux), read once.
fn rmem_max() -> u64 {
    static M: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *M.get_or_init(|| {
        std::fs::read_to_string("/proc/sys/net/core/rmem_max").ok().and_then(|s| s.trim().parse().ok()).unwrap_or(u64::MAX)
    })
}

/// The read lane's planes: their pipes' user-space price differs.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Plane {
    Pg,
    My,
}

/// User-space bytes one read pipe holds at worst (its batches in hand and
/// queued, its window), measured in W1 phase A at 0.5 core / 256 MiB on the
/// 10g seed, anonymous memory past the statement's floor and the engine's
/// counted structures, per pipe: Postgres raw plane TPC-H Q1 (8 ungated
/// pipes) 16.2 MiB; MySQL raw plane Q12 (4 pipes) 14.1 MiB, Q22 (4) 15.6
/// MiB. A gated read holds less (Q19, 7 pipes at density 0.06: ~2.6 MiB).
pub(crate) const PG_PIPE_USER: u64 = 16 << 20;
pub(crate) const MY_PIPE_USER: u64 = 16 << 20;

/// Of a pipe's user-space price, what the read's batches may hold for it
/// (sealed, queued, in the consumer's hand: [`crate::read::BatchTx`]). The
/// rest is its window (1 MiB socket read buffer, 1 MiB scan window), the
/// filling builder's growth past its seal size, and the allocator's slack.
pub(crate) const PIPE_BATCHES: u64 = 6 << 20;

static USER: AtomicU64 = AtomicU64::new(0);
static USER_HIGH: AtomicU64 = AtomicU64::new(0);

/// The read pipes' user-space price reserved now, and the most since the
/// last call.
pub(crate) fn take_user_high() -> u64 {
    USER_HIGH.swap(USER.load(Relaxed), Relaxed).max(USER.load(Relaxed))
}

/// Bytes reserved now for open read sockets' receive buffers (the tests'
/// view: [`Sockets::plan_in`] reads and adds to the account in one step).
#[cfg(test)]
pub(crate) fn reserved() -> u64 {
    RESERVED.load(Relaxed)
}

/// The most reserved since the last call (then reset to what is reserved now).
pub(crate) fn take_high() -> u64 {
    HIGH.swap(RESERVED.load(Relaxed), Relaxed).max(RESERVED.load(Relaxed))
}

/// A read pipe in the account: its socket's receive buffer (kernel) and
/// its user-space price; released when dropped.
#[derive(Debug, Default)]
pub(crate) struct Reservation(u64, u64);

impl Drop for Reservation {
    fn drop(&mut self) {
        let (k, u) = (std::mem::take(&mut self.0), std::mem::take(&mut self.1));
        if k > 0 {
            let _ = RESERVED.fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(k)));
        }
        if u > 0 {
            let _ = USER.fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(u)));
        }
    }
}

fn reserve(n: u64) -> Reservation {
    let now = RESERVED.fetch_add(n, Relaxed) + n;
    HIGH.fetch_max(now, Relaxed);
    Reservation(n, 0)
}

impl Reservation {
    /// The pipe's user-space price on top.
    fn with_user(mut self, u: u64) -> Reservation {
        let now = USER.fetch_add(u, Relaxed) + u;
        USER_HIGH.fetch_max(now, Relaxed);
        self.1 = u;
        self
    }
}

#[cfg(unix)]
type Fd = std::os::fd::RawFd;
#[cfg(not(unix))]
type Fd = i32;

/// The connection's smoothed round trip (`TCP_INFO`), where the kernel has one.
#[cfg(target_os = "linux")]
pub(crate) fn rtt_of(fd: Fd) -> Option<Duration> {
    let mut ti: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
    // SAFETY: a valid socket descriptor; the kernel writes at most `len` bytes into `ti`.
    let r = unsafe { libc::getsockopt(fd, libc::IPPROTO_TCP, libc::TCP_INFO, (&mut ti as *mut libc::tcp_info).cast(), &mut len) };
    (r == 0 && ti.tcpi_rtt > 0).then(|| Duration::from_micros(u64::from(ti.tcpi_rtt)))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn rtt_of(_fd: Fd) -> Option<Duration> {
    None
}

#[cfg(unix)]
fn set_and_read(fd: Fd, want: usize) -> Option<u64> {
    let v = libc::c_int::try_from(want).unwrap_or(libc::c_int::MAX);
    // SAFETY: a valid socket descriptor; an int option in, an int option out.
    unsafe {
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, (&v as *const libc::c_int).cast(), std::mem::size_of::<libc::c_int>() as libc::socklen_t);
    }
    effective(fd)
}

#[cfg(not(unix))]
fn set_and_read(_fd: Fd, _want: usize) -> Option<u64> {
    None
}

/// The receive buffer the kernel holds the socket to (`SO_RCVBUF` read back:
/// on Linux twice what was asked).
#[cfg(unix)]
pub(crate) fn effective(fd: Fd) -> Option<u64> {
    let mut v: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: as above.
    let r = unsafe { libc::getsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, (&mut v as *mut libc::c_int).cast(), &mut len) };
    (r == 0 && v > 0).then_some(v as u64)
}

#[cfg(not(unix))]
pub(crate) fn effective(_fd: Fd) -> Option<u64> {
    None
}

/// The tcp_rmem maximum autotuning may grow a socket to (Linux), else 6 MiB.
fn autotune_max() -> u64 {
    std::fs::read_to_string("/proc/sys/net/ipv4/tcp_rmem")
        .ok()
        .and_then(|s| s.split_whitespace().nth(2)?.parse().ok())
        .unwrap_or(6 << 20)
}

/// Size one of `pipes` read sockets by the law (its round trip measured now)
/// and reserve what the kernel granted, with the pipe's user-space price.
/// Call after the startup exchange.
pub(crate) fn apply(fd: Fd, sockets: Sockets, plane: Plane, endpoint: &str) -> Reservation {
    let user = match plane {
        Plane::Pg => PG_PIPE_USER,
        Plane::My => MY_PIPE_USER,
    };
    apply_kernel(fd, sockets, plane, endpoint).with_user(user)
}

/// A read pipe whose socket this side cannot size (a sqlx connection, a TLS
/// stream): its user-space price, and the most autotuning may grow its
/// receive buffer to.
pub(crate) fn unsized_pipe(plane: Plane) -> Reservation {
    let user = match plane {
        Plane::Pg => PG_PIPE_USER,
        Plane::My => MY_PIPE_USER,
    };
    reserve(autotune_max()).with_user(user)
}

fn apply_kernel(fd: Fd, sockets: Sockets, plane: Plane, endpoint: &str) -> Reservation {
    sockets.settle();
    let rtt = rtt_of(fd);
    if let Some(r) = rtt {
        rtt_note(endpoint, r);
    }
    let want = match env_buf() {
        // Autotuning: the kernel may grow it to tcp_rmem's maximum.
        EnvBuf::Autotune => return reserve(autotune_max()),
        EnvBuf::Bytes(n) => n,
        // No round trip known: today's size (never a window under the law).
        EnvBuf::Unset => rtt.map_or(CEIL, |r| size(sockets.pipes, r, cores(), sockets.room)),
    };
    // MySQL's raw lane autotuned in 0.67.2. `SO_RCVBUF` past
    // `net.core.rmem_max` is cut to it and stops the autotuning: on a host
    // left at the default (208 KiB) the socket held 416 KiB for good. Past
    // it, the option is not set; the reservation is the autotuning's most
    // (W1 review R5). The Postgres lane asked 4 MiB in 0.67.2 and asks no
    // more now, so it keeps setting it.
    if !sets_option(plane, want, rmem_max()) {
        // A size the user asked for is said, not silently dropped.
        if matches!(env_buf(), EnvBuf::Bytes(_)) {
            static SAID: std::sync::Once = std::sync::Once::new();
            SAID.call_once(|| {
                crate::progress::note(&format!(
                    "APITAP_RECV_BUF_BYTES={want} is past net.core.rmem_max ({}); the MySQL read sockets are left to the kernel's autotuning",
                    rmem_max()
                ))
            });
        }
        if std::env::var_os("APITAP_DEBUG").is_some() {
            eprintln!(
                "[rcvbuf] {} pipe(s): {} KiB past net.core.rmem_max ({} KiB), left to the kernel's autotuning",
                sockets.pipes,
                want >> 10,
                rmem_max() >> 10
            );
        }
        return reserve(autotune_max());
    }
    let eff = set_and_read(fd, want).unwrap_or((want as u64).saturating_mul(2));
    if std::env::var_os("APITAP_DEBUG").is_some() {
        eprintln!(
            "[rcvbuf] {} pipe(s), rtt {}, asked {} KiB, kernel holds {} KiB",
            sockets.pipes,
            rtt.map_or("unknown".into(), |r| format!("{} us", r.as_micros())),
            want >> 10,
            eff >> 10
        );
    }
    reserve(eff)
}

/// Whether a socket of `plane` asks `want` by `SO_RCVBUF` (else it is left to
/// the kernel's autotuning): not on MySQL's raw lane past `rmem_max` (W1
/// review R5).
fn sets_option(plane: Plane, want: usize, rmem_max: u64) -> bool {
    !(matches!(plane, Plane::My) && want as u64 > rmem_max)
}

/// The share of the memory limit receive buffers may take (an eighth).
pub(crate) fn share(limit: u64) -> u64 {
    limit / 8
}

/// `pipes` read sockets to `endpoint`, fewer while their buffers (by the
/// law, at the latest round trip measured to that endpoint) are past their
/// share of the limit. The window each keeps is the law's; only the count
/// gives.
pub(crate) fn pipes_within(pipes: usize, limit: Option<u64>, endpoint: &str) -> usize {
    let Some(limit) = limit else { return pipes };
    if env_buf() != EnvBuf::Unset {
        return pipes;
    }
    pipes_at(pipes, limit, rtt_at(endpoint), cores())
}

/// [`pipes_within`] at a given round trip and core count.
fn pipes_at(pipes: usize, limit: u64, rtt: Duration, cores: f64) -> usize {
    let mut n = pipes.max(1);
    while n > 1 && n as u64 * 2 * size(n, rtt, cores, Some(room_at(n, limit, 0))) as u64 > share(limit) {
        n -= 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn law_is_the_floor_on_loopback_and_grows_with_rtt_and_cores() {
        let us = |n| Duration::from_micros(n);
        // Loopback (~50 µs) at 8 pipes: the floor, at half a core or sixteen.
        assert_eq!(law(8, us(50), 0.5), FLOOR);
        assert_eq!(law(8, us(50), 16.0), FLOOR);
        // 20 ms at half a core, 8 pipes: 2 × 2 × 0.5 × RATE × 0.02 / 8.
        let w = law(8, us(20_000), 0.5);
        assert!(w > FLOOR && w < CEIL, "{w}");
        assert_eq!(w, (2.0 * SKEW * 0.5 * RATE * 0.02 / 8.0) as usize);
        // 67 ms (the netem leg): today's 4 MiB.
        assert_eq!(law(8, us(67_000), 0.5), CEIL);
        // More cores or a longer path never ask for less; the cap holds.
        assert!(law(8, us(20_000), 2.0) >= w);
        assert_eq!(law(8, us(500_000), 8.0), CEIL);
    }

    /// The law is a lower bound: a cage with room gives each socket its
    /// share (8 pipes at 256 MiB: 32 MiB / 16 = 2 MiB asked, 4 MiB held), a
    /// tight one the law, a long path the law above the room.
    #[test]
    fn size_is_the_room_bounded_below_by_the_law() {
        let us = |n| Duration::from_micros(n);
        let room = |p, l| Some(room_at(p, l, 0));
        assert_eq!(size(8, us(50), 0.5, room(8, 256 << 20)), 2 << 20);
        assert_eq!(size(2, us(50), 0.5, room(2, 256 << 20)), CEIL);
        assert_eq!(size(8, us(50), 0.5, room(8, 64 << 20)), FLOOR, "8 MiB of share over 16: the floor");
        assert_eq!(size(8, us(67_000), 0.5, room(8, 64 << 20)), CEIL, "a long path keeps the law's 4 MiB");
        assert_eq!(size(8, us(50), 0.5, None), CEIL);
    }

    #[test]
    fn pipes_shrink_only_past_the_share_and_never_below_one() {
        let us = |n| Duration::from_micros(n);
        // Loopback at 256 MiB: 8 pipes × 2 × 1 MiB = 16 MiB, under its 32 MiB share.
        assert_eq!(pipes_at(8, 256 << 20, us(50), 0.5), 8);
        // A long path in a tiny cage: fewer pipes, at least one.
        let n = pipes_at(8, 16 << 20, us(500_000), 0.5);
        assert!((1..8).contains(&n), "{n}");
        assert_eq!(pipes_at(8, 1 << 20, us(500_000), 0.5), 1);
        assert_eq!(pipes_within(8, None, "h:1"), 8);
    }

    /// Eight one-pipe reads opening together (a star plan's `Early` parts):
    /// each sized from what the sockets already open leave of the share, not
    /// as if alone, so together they hold the share and the law's floor at
    /// most, not 8 × 8 MiB (W1 review R8).
    #[test]
    fn concurrent_reads_share_one_receive_buffer_share() {
        let limit = 256u64 << 20;
        let mut taken = 0u64;
        for _ in 0..8 {
            let ask = size(1, Duration::from_micros(50), 0.5, Some(room_at(1, limit, taken)));
            taken += 2 * ask as u64;
        }
        assert!(taken <= share(limit) + 8 * 2 * FLOOR as u64, "{} MiB held against a {} MiB share", taken >> 20, share(limit) >> 20);
        // One read alone keeps its size (8 pipes at 256 MiB: 2 MiB each).
        assert_eq!(size(8, Duration::from_micros(50), 0.5, Some(room_at(8, limit, 0))), 2 << 20);
    }

    /// MySQL's raw lane autotuned in 0.67.2: past `net.core.rmem_max` (208
    /// KiB on a default host) asking would cut it to 416 KiB for good, so
    /// it is not asked; under it, and on the Postgres lane (which asked 4
    /// MiB in 0.67.2 too), it is (W1 review R5).
    #[test]
    fn mysql_sockets_past_rmem_max_stay_on_autotuning() {
        assert!(!sets_option(Plane::My, 2 << 20, 212_992));
        assert!(sets_option(Plane::My, 2 << 20, 16 << 20));
        assert!(sets_option(Plane::Pg, 2 << 20, 212_992));
    }

    /// One parser for `APITAP_RECV_BUF_BYTES`: a typo is the plane's own
    /// default on every plane, never autotuning (W1 review R10).
    #[test]
    fn one_parser_one_meaning_for_the_receive_buffer_knob() {
        assert_eq!(parse_env_buf(None), EnvBuf::Unset);
        assert_eq!(parse_env_buf(Some("not-a-number")), EnvBuf::Unset);
        assert_eq!(parse_env_buf(Some("0")), EnvBuf::Autotune);
        assert_eq!(parse_env_buf(Some("OFF")), EnvBuf::Autotune);
        assert_eq!(parse_env_buf(Some(" off ")), EnvBuf::Unset, "untrimmed, as 0.67.2 parsed it");
        assert_eq!(parse_env_buf(Some("1048576")), EnvBuf::Bytes(1 << 20));
    }

    /// A value the knob does not take is refused by name before a run
    /// (QA p13), and every value it takes passes.
    #[test]
    fn a_receive_buffer_typo_is_refused_by_name() {
        let _g = crate::dialect::postgres::env_guard();
        for (v, ok) in [("junk", false), ("", false), (" off ", false), ("-1", false), ("0", true), ("off", true), ("4194304", true)] {
            std::env::set_var("APITAP_RECV_BUF_BYTES", v);
            let r = check_env();
            assert_eq!(r.is_ok(), ok, "{v:?}");
            if let Err(e) = r {
                assert!(e.to_string().contains(&format!("APITAP_RECV_BUF_BYTES='{v}' is not a byte count")), "{e}");
            }
        }
        std::env::remove_var("APITAP_RECV_BUF_BYTES");
        assert!(check_env().is_ok());
    }

    /// Round trips are kept per endpoint, the latest one: a far server does
    /// not shrink reads from a near one, and a path that got shorter counts
    /// (W1 review R7).
    #[test]
    fn round_trips_are_kept_per_endpoint_latest_first() {
        rtt_note("far.test:5432", Duration::from_millis(80));
        rtt_note("near.test:5432", Duration::from_micros(60));
        assert_eq!(rtt_at("near.test:5432"), Duration::from_micros(60));
        rtt_note("far.test:5432", Duration::from_millis(5));
        assert_eq!(rtt_at("far.test:5432"), Duration::from_millis(5));
        assert_eq!(rtt_at("other.test:1"), Duration::ZERO);
        assert_eq!(endpoint_of("postgres://u:p@Db.Host:5544/x?sslmode=disable"), "db.host:5544");
        assert_eq!(endpoint_of("mysql://root@127.0.0.1:3307/bench"), "127.0.0.1:3307");
    }

    /// Tests that move the process-wide account run one at a time.
    static ACCOUNT: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Eight one-pipe reads planned at once (a star plan's `Early` parts):
    /// each plan holds its room until its socket opens, so together they ask
    /// the share and the floor at most (W1 review 2: plans that held nothing
    /// all saw an empty account and asked 8 MiB each).
    #[test]
    fn reads_planned_at_once_see_each_other() {
        let _g = ACCOUNT.lock().unwrap_or_else(|e| e.into_inner());
        let limit = 256u64 << 20;
        let plans: Vec<Sockets> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..8).map(|_| sc.spawn(|| Sockets::plan_in(1, Some(limit)))).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let asked: u64 = plans.iter().map(|p| 2 * p.room.unwrap_or(0).clamp(FLOOR as u64, CEIL as u64)).sum();
        assert!(asked <= share(limit) + 8 * 2 * FLOOR as u64, "{} MiB asked against a {} MiB share", asked >> 20, share(limit) >> 20);
        let held = reserved();
        drop(plans);
        assert!(reserved() < held, "the holds go back with the plans");
    }

    #[test]
    fn reservations_are_released_on_drop() {
        let _g = ACCOUNT.lock().unwrap_or_else(|e| e.into_inner());
        let before = reserved();
        let r = reserve(1 << 20).with_user(16 << 20);
        assert_eq!(reserved(), before + (1 << 20));
        assert!(take_high() >= before + (1 << 20));
        assert!(take_user_high() >= 16 << 20);
        drop(r);
        assert_eq!(reserved(), before);
    }
}
