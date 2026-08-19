//! One flag, set by SIGTERM, read where stopping is safe.
//!
//! A scheduler that stops a task sends SIGTERM and follows it with SIGKILL a
//! few seconds later — Kubernetes on eviction, Airflow on a cleared run,
//! systemd on `stop`. Without a handler the first signal ends the process
//! immediately. That is *safe*: a CDC watermark is written after the rows it
//! covers, and a replay is idempotent, so nothing is lost or duplicated. But it
//! is wasteful — everything the in-flight window has drained is thrown away and
//! re-read on the next run, and on a busy table that is minutes per redeploy.
//!
//! So the signal sets a flag rather than ending anything. Both drains (the
//! Postgres walsender loop and the MySQL binlog loop) read it in the same place
//! they read their wall-clock deadline: between events, never inside a
//! half-decoded frame. The drain stops with its end position at the last
//! COMPLETE commit, the window applies, the watermark advances — exactly the
//! path a budget-limited window already takes every day.
//!
//! # Where this is armed, and where it deliberately is not
//!
//! Only around the incremental drain. A CDC table's FIRST run is a full
//! bootstrap load, and there SIGTERM stays fatal on purpose: a bulk load
//! publishes at the swap, so a bootstrap stopped halfway has nothing worth
//! landing, and swallowing the signal would leave the process ignoring its
//! scheduler for as long as the load takes — trading a clean stop for a
//! guaranteed SIGKILL later. Bulk `replace`/`append`/`merge` transfers arm
//! nothing for the same reason.
//!
//! # What it does to a handler that is already there
//!
//! It keeps it. Whatever SIGTERM pointed at before the run is called from
//! inside ours, right after the flag is set, so a host's own handler still sees
//! every signal it would have seen. This matters more than it looks: CPython's
//! `signal.signal(SIGTERM, ...)` installs a C trampoline that only sets a flag
//! and writes to the wakeup pipe — the Python function itself runs later, when
//! the interpreter next reaches the eval loop. During a transfer the main
//! thread is inside Rust, so that moment does not arrive until the run is over.
//! Chaining is what makes both true at once: the drain stops at its next safe
//! point, and the caller's Python handler still runs when control comes back.
//!
//! Two dispositions are left completely alone:
//!
//! * `SIG_IGN` — the process has deliberately said it does not want SIGTERM.
//! * a handler installed with `SA_SIGINFO`, which takes three arguments and may
//!   dereference a `siginfo_t` we have no safe way to forge. Calling it through
//!   the one-argument prototype would be a crash waiting for the worst possible
//!   moment, so we install nothing and leave it in charge. Such a host can opt
//!   in explicitly by calling [`request`].
//!
//! SIGINT is untouched. Taking ^C away from the interpreter is how a library
//! ends up swallowing a user's interrupt — so ^C during a transfer behaves
//! exactly as it did before this module existed, which is to say it waits for
//! the eval loop.
//!
//! # What it does not promise
//!
//! * SIGKILL is not catchable and is not handled. If the window's apply runs
//!   past the scheduler's grace period, SIGKILL still arrives — and that is
//!   still safe, for the reason it always was.
//! * A *second* SIGTERM is not absorbed. The handler restores the default
//!   disposition and re-raises, so an operator who wants the process gone now
//!   gets it gone now — including a process whose own handler would have
//!   survived the signal. Once to ask, twice to insist.
//! * Nothing is interrupted mid-read. If the source has gone quiet and the
//!   drain is blocked waiting on a socket, the flag is not seen until the next
//!   event or keepalive: seconds on a live connection, the second SIGTERM on a
//!   wedged one.
//! * A stop is a property of the PROCESS, not of one call. It is cleared when a
//!   run starts and no other run is in flight; a run that begins while another
//!   is still winding down inherits the stop, because a SIGTERM asked for the
//!   process to stop and starting more work would be answering a question
//!   nobody asked.
//!
//! `APITAP_GRACEFUL_STOP=0` skips installing the handler, so SIGTERM goes back
//! to ending the process. It does not disable [`request`], which is an explicit
//! call and not something a scheduler can do to you by surprise.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Has a stop been asked for — by a signal, or through [`request`]?
static REQUESTED: AtomicBool = AtomicBool::new(false);
/// How many SIGTERMs this armed period has taken.
///
/// Deliberately NOT the same thing as `REQUESTED`. An earlier version decided
/// "this is the second signal, insist" from `REQUESTED.swap(true)` returning
/// true — but `REQUESTED` is also set by [`request`], so a host that called
/// `apitap.request_stop()` had its next SIGTERM, the process's FIRST, treated
/// as the insistent second one and was killed mid-apply. The two questions are
/// different and now have different answers.
static SIGNALS: AtomicUsize = AtomicUsize::new(0);
/// The previous SIGTERM handler, if we chained one. Read from inside a signal
/// handler, so it lives in an atomic and never behind a lock.
static PREV: AtomicUsize = AtomicUsize::new(0);
/// Everything install/drop need to agree on, under one lock so that no thread
/// can ever observe "no run is armed" while our handler is still the live
/// disposition. Without the lock, a guard dropping and a guard installing could
/// interleave so that the installing one read OUR handler out of the live
/// disposition and recorded it as the host's — losing the host's handler for
/// good and pointing `PREV` at `on_sigterm` itself.
static STATE: Mutex<State> = Mutex::new(State {
    depth: 0,
    saved: None,
});

struct State {
    depth: usize,
    saved: Option<Saved>,
}

struct Saved(libc::sigaction);
// SAFETY: `sigaction` is plain C data — an integer, a bitmask and a couple of
// pointers to code. Moving it between threads copies bytes and nothing else.
unsafe impl Send for Saved {}

/// Has a stop been asked for?
pub(crate) fn requested() -> bool {
    REQUESTED.load(Ordering::Relaxed)
}

/// Ask the current CDC drain to stop at its next safe point.
///
/// Public so a host whose signal handling apitap will not touch — `SIG_IGN`, or
/// an `SA_SIGINFO` handler — can hand the request through itself, and so a
/// transfer running on another thread can be wound down from the main one.
/// Safe to call from anywhere, including a signal handler: it is one relaxed
/// atomic store.
///
/// It has no effect on a bulk transfer or on a CDC table's first (bootstrap)
/// run — see the module docs for why neither of those arms anything.
pub fn request() {
    REQUESTED.store(true, Ordering::Relaxed);
}

/// Clear the flag.
///
/// Called when a run starts and no other run is in flight. A run that begins
/// while another is still going does NOT clear it: a SIGTERM asked for the
/// process to stop, and a second thread starting work must not un-stop the
/// thread that already took the signal.
pub fn clear() {
    REQUESTED.store(false, Ordering::Relaxed);
}

extern "C" fn on_sigterm(sig: libc::c_int) {
    // Second signal of this armed period: give the operator what they asked
    // for. `signal` and `raise` are both on the async-signal-safe list; nothing
    // here allocates, locks, or touches the runtime.
    if SIGNALS.fetch_add(1, Ordering::Relaxed) >= 1 {
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
        return;
    }
    REQUESTED.store(true, Ordering::Relaxed);
    // Hand the signal on to whoever had it before us. For CPython that is the
    // trampoline behind `signal.signal`, which sets a flag and writes one byte
    // to the wakeup pipe — async-signal-safe, and the reason a caller's Python
    // handler still runs once the transfer returns.
    let prev = PREV.load(Ordering::Relaxed);
    if prev != 0 {
        // SAFETY: `prev` is non-zero only when `install` read a one-argument
        // handler out of the live disposition that was neither SIG_DFL, SIG_IGN,
        // SA_SIGINFO, nor this function itself; and it is cleared only after
        // that handler is back in place.
        let f: extern "C" fn(libc::c_int) = unsafe { std::mem::transmute(prev) };
        f(sig);
    }
}

/// Arms the handler for as long as it is alive, then puts back exactly what was
/// there before. Install it around the INCREMENTAL drain only — see the module
/// docs on where this deliberately is not armed.
pub(crate) struct Guard;

impl Guard {
    pub(crate) fn install() -> Self {
        let mut st = STATE.lock().unwrap_or_else(|e| e.into_inner());
        st.depth += 1;
        if st.depth > 1 {
            // A run was already going. Its stop request, if it has one, stands.
            return Guard;
        }
        // First run in flight: start from clean. This happens even when the
        // handler itself is not installed below, because `request()` works
        // regardless and a flag nothing ever clears would poison every later
        // run in the process.
        clear();
        SIGNALS.store(0, Ordering::Relaxed);
        if std::env::var("APITAP_GRACEFUL_STOP").as_deref() == Ok("0") {
            return Guard;
        }
        // SAFETY: reading the current disposition with a null `act` is the
        // documented no-change query; the struct is plain C data.
        let cur = unsafe {
            let mut cur: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGTERM, std::ptr::null(), &mut cur) != 0 {
                return Guard;
            }
            cur
        };
        if cur.sa_sigaction == libc::SIG_IGN || (cur.sa_flags & libc::SA_SIGINFO) != 0 {
            // Deliberately ignored, or a three-argument handler we cannot call
            // safely. Not ours to touch — see the module docs.
            return Guard;
        }
        let ours = on_sigterm as *const () as usize;
        let prev = if cur.sa_sigaction == libc::SIG_DFL || cur.sa_sigaction == ours {
            // `ours` should be unreachable now that install and drop share a
            // lock, but chaining a handler to itself is an unbounded recursion
            // inside a signal handler, and the check costs one comparison.
            0
        } else {
            cur.sa_sigaction
        };
        PREV.store(prev, Ordering::Relaxed);
        // SAFETY: the handler performs relaxed atomic loads/stores, an optional
        // call to the previous one-argument handler, and (on a second signal)
        // `signal`/`raise` — all async-signal-safe. No SA_SIGINFO, so the
        // one-argument prototype is the right one.
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = ours;
            libc::sigemptyset(&mut act.sa_mask);
            // Carry the host's restart behaviour rather than imposing ours:
            // SA_RESTART belongs to the disposition, not to the thread, so
            // forcing it on would silently turn every EINTR in the process into
            // a restarted syscall for as long as the transfer runs. With no
            // host handler, SA_RESTART is the kinder default — a library should
            // not hand a caller a spurious EINTR they never had to handle.
            act.sa_flags = if prev != 0 {
                cur.sa_flags & libc::SA_RESTART
            } else {
                libc::SA_RESTART
            };
            if libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut()) != 0 {
                PREV.store(0, Ordering::Relaxed);
                return Guard;
            }
        }
        st.saved = Some(Saved(cur));
        Guard
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut st = STATE.lock().unwrap_or_else(|e| e.into_inner());
        st.depth -= 1;
        if st.depth > 0 {
            return;
        }
        if let Some(Saved(old)) = st.saved.take() {
            // Restore FIRST, clear the chain pointer second. A signal landing
            // between the two finds the host's own handler already back in
            // charge, which is the outcome we want; doing it the other way
            // round leaves a window where our handler is still armed with
            // nothing to chain to, and the host's handler never runs at all.
            //
            // SAFETY: `old` is the `sigaction` struct the kernel filled in for
            // us at install time; putting it back is the documented restore.
            unsafe {
                libc::sigaction(libc::SIGTERM, &old, std::ptr::null_mut());
            }
        }
        PREV.store(0, Ordering::Relaxed);
        SIGNALS.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every test here mutates PROCESS-wide state — the SIGTERM disposition and
    /// an env var. Cargo runs tests on parallel threads, so without this they
    /// would read each other's handlers and pass or fail by timing.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn disposition() -> usize {
        unsafe {
            let mut cur: libc::sigaction = std::mem::zeroed();
            assert_eq!(libc::sigaction(libc::SIGTERM, std::ptr::null(), &mut cur), 0);
            cur.sa_sigaction
        }
    }

    /// Raise SIGTERM only once something is demonstrably armed to catch it.
    ///
    /// Without the precondition, a silently-failed install turns every test
    /// below into a signal-15 death of the whole `cargo test` process — no
    /// assertion message, no failing test name, just a harness that vanished.
    fn raise_sigterm_safely() {
        assert_ne!(
            disposition(),
            libc::SIG_DFL,
            "refusing to raise SIGTERM with the default disposition armed — \
             that would kill the test binary instead of failing a test"
        );
        unsafe { libc::raise(libc::SIGTERM) };
    }

    /// The whole point: a SIGTERM during a run is absorbed into a flag the
    /// drain can see, and the disposition is handed back on the way out.
    #[test]
    fn sigterm_becomes_a_flag_and_the_handler_is_returned() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(disposition(), libc::SIG_DFL, "test rig starts clean");
        {
            let _g = Guard::install();
            assert_ne!(disposition(), libc::SIG_DFL, "handler is in place");
            assert!(!requested(), "install clears any stale request");
            raise_sigterm_safely();
            assert!(requested(), "the signal turned into a request to stop");
        }
        assert_eq!(disposition(), libc::SIG_DFL, "disposition handed back");
        clear();
    }

    /// A run that begins with no other run in flight must not inherit a stale
    /// stop from a previous one.
    #[test]
    fn a_fresh_run_does_not_inherit_the_previous_stop() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        request();
        assert!(requested());
        let g = Guard::install();
        assert!(!requested());
        drop(g);
        clear();
    }

    /// …but a run starting while another is live must not un-stop it.
    #[test]
    fn a_second_run_starting_does_not_wipe_a_stop_meant_for_the_first() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let outer = Guard::install();
        raise_sigterm_safely();
        assert!(requested(), "run 1 has been asked to stop");
        let inner = Guard::install();
        assert!(requested(), "run 2 starting must not un-stop run 1");
        drop(inner);
        assert!(requested(), "nor may run 2 finishing un-stop it");
        drop(outer);
        clear();
    }

    /// The bug this file exists to never have again: `request_stop()` sets the
    /// same flag a signal sets, so an earlier version read the operator's FIRST
    /// SIGTERM as the insistent second one and killed the process mid-apply.
    #[test]
    fn request_stop_does_not_make_the_first_sigterm_lethal() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let _g = Guard::install();
        request();
        assert!(requested());
        // Still alive after this is the assertion. On the old code the handler
        // saw REQUESTED already true, restored SIG_DFL and re-raised — and the
        // test binary died here with no failure reported.
        raise_sigterm_safely();
        assert_eq!(SIGNALS.load(Ordering::Relaxed), 1, "that was signal one");
        assert!(requested());
        drop(_g);
        clear();
    }

    /// Twice to insist — proven in a child process, because proving it in this
    /// one would kill the test harness.
    ///
    /// The parent arms the guard and only THEN forks, so the child inherits an
    /// armed disposition and a zeroed signal count and does nothing but raise,
    /// twice, and `_exit`. That keeps the child to `raise`, two relaxed atomics
    /// and `_exit` — all async-signal-safe, none of them touching the malloc
    /// lock or the mutex, which is what makes forking a threaded test binary
    /// safe here and would not be if the child ran `Guard::install()` itself.
    ///
    /// The e2e legs cannot prove this branch: a second SIGTERM that arrives
    /// after the guard has been restored also ends the process with SIGTERM,
    /// and from outside the two are the same death. Here they are not — if the
    /// second signal were absorbed the child would reach `_exit(3)` and the
    /// parent would see an exit status instead of a signal.
    #[test]
    fn the_second_signal_is_not_absorbed() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let _g = Guard::install();
        assert_ne!(disposition(), libc::SIG_DFL, "armed before the fork");
        // SAFETY: the child touches nothing but `raise`, two relaxed atomic
        // operations inside the inherited handler, and `_exit`.
        unsafe {
            let pid = libc::fork();
            assert!(pid >= 0, "fork failed");
            if pid == 0 {
                libc::raise(libc::SIGTERM); // absorbed: a request to stop
                libc::raise(libc::SIGTERM); // must not be absorbed
                libc::_exit(3); // reached only if the contract is broken
            }
            let mut status: libc::c_int = 0;
            assert_eq!(libc::waitpid(pid, &mut status, 0), pid, "waitpid");
            assert!(
                libc::WIFSIGNALED(status),
                "the child survived its second SIGTERM and exited {} instead",
                libc::WEXITSTATUS(status)
            );
            assert_eq!(libc::WTERMSIG(status), libc::SIGTERM, "died of the right signal");
        }
        // The parent took no signal of its own.
        assert_eq!(SIGNALS.load(Ordering::Relaxed), 0);
        assert!(!requested());
        drop(_g);
        clear();
    }

    /// Nesting must not hand the disposition back while an outer run is still
    /// using it — the inner guard is a no-op on the way out.
    #[test]
    fn nested_guards_keep_the_handler_until_the_outer_one_drops() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let outer = Guard::install();
        let inner = Guard::install();
        assert_ne!(disposition(), libc::SIG_DFL);
        drop(inner);
        assert_ne!(
            disposition(),
            libc::SIG_DFL,
            "inner drop must not disarm the outer run"
        );
        drop(outer);
        assert_eq!(disposition(), libc::SIG_DFL);
        clear();
    }

    /// A handler that was already there must still see the signal — that is
    /// what keeps a caller's `signal.signal(SIGTERM, ...)` working while a
    /// transfer holds the main thread inside Rust.
    #[test]
    fn an_existing_handler_is_chained_not_replaced() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        static THEIRS_RAN: AtomicBool = AtomicBool::new(false);
        extern "C" fn theirs(_: libc::c_int) {
            THEIRS_RAN.store(true, Ordering::Relaxed);
        }
        let theirs_ptr = theirs as *const () as usize;
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = theirs_ptr;
            libc::sigemptyset(&mut act.sa_mask);
            act.sa_flags = libc::SA_RESTART;
            assert_eq!(libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut()), 0);
        }
        {
            let _g = Guard::install();
            assert_ne!(disposition(), theirs_ptr, "ours is armed on top");
            raise_sigterm_safely();
            assert!(requested(), "we took the stop request");
            assert!(THEIRS_RAN.load(Ordering::Relaxed), "and they still ran");
        }
        assert_eq!(disposition(), theirs_ptr, "their handler is back in place");
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_DFL) };
        clear();
    }

    /// Chaining must never point at us: a handler that calls itself is an
    /// unbounded recursion inside a signal handler.
    #[test]
    fn we_never_chain_to_our_own_handler() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = on_sigterm as *const () as usize;
            libc::sigemptyset(&mut act.sa_mask);
            act.sa_flags = libc::SA_RESTART;
            assert_eq!(libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut()), 0);
        }
        {
            let _g = Guard::install();
            assert_eq!(PREV.load(Ordering::Relaxed), 0, "no self-chain recorded");
            raise_sigterm_safely();
            assert!(requested());
        }
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_DFL) };
        clear();
    }

    /// SIG_IGN is a decision, not an accident. Leave it — but still manage the
    /// flag, or a `request_stop()` under such a host would never be cleared.
    #[test]
    fn a_deliberately_ignored_sigterm_is_left_ignored() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_IGN) };
        request();
        {
            let _g = Guard::install();
            assert_eq!(disposition(), libc::SIG_IGN, "we did not arm over SIG_IGN");
            assert!(!requested(), "but the run still started from clean");
            unsafe { libc::raise(libc::SIGTERM) };
            assert!(!requested(), "and the signal stayed ignored");
        }
        assert_eq!(disposition(), libc::SIG_IGN);
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_DFL) };
        clear();
    }

    /// A three-argument handler cannot be called through the one-argument
    /// prototype, so we install nothing rather than risk a crash inside a
    /// signal handler.
    #[test]
    fn a_sa_siginfo_handler_is_left_in_charge() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        extern "C" fn theirs(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {}
        let theirs_ptr = theirs as *const () as usize;
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = theirs_ptr;
            libc::sigemptyset(&mut act.sa_mask);
            act.sa_flags = libc::SA_SIGINFO;
            assert_eq!(libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut()), 0);
        }
        {
            let _g = Guard::install();
            assert_eq!(disposition(), theirs_ptr, "we stayed out of the way");
        }
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_DFL) };
        clear();
    }

    /// `APITAP_GRACEFUL_STOP=0` skips the handler — but must NOT skip the flag
    /// bookkeeping, or a `request_stop()` would latch for the process's life
    /// while the drains go on reading it.
    #[test]
    fn the_opt_out_skips_the_handler_but_still_clears_the_flag() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("APITAP_GRACEFUL_STOP", "0");
        request();
        {
            let _g = Guard::install();
            assert_eq!(disposition(), libc::SIG_DFL, "no handler armed");
            assert!(!requested(), "and the stale request was still cleared");
        }
        std::env::remove_var("APITAP_GRACEFUL_STOP");
        clear();
    }

    /// A guard that bails out (SIG_IGN, SA_SIGINFO, the opt-out) still holds a
    /// depth slot, so a concurrent run cannot re-enter the clearing path and
    /// wipe a pending stop.
    #[test]
    fn a_guard_that_installs_nothing_still_holds_the_depth() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("APITAP_GRACEFUL_STOP", "0");
        let outer = Guard::install();
        request();
        let inner = Guard::install();
        assert!(requested(), "the second run must not clear the first's stop");
        drop(inner);
        drop(outer);
        std::env::remove_var("APITAP_GRACEFUL_STOP");
        clear();
    }
}
