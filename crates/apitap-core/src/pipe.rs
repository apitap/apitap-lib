//! Owned pipes and the one crew join loop: the worker lifecycle every source
//! lane is rewired onto. A source receives a [`Pipes`] and can only `send`
//! through a [`Pipe`]; the engine `drive`s each pipe exactly once, and a
//! [`Spans`] queue refuses the next statement as soon as any sibling failed.
//!
//! Why: 0.56.0 spawned one task per loader and then ran `for t in tasks {
//! t.await?? }` — the first `Err` returned and dropped the remaining
//! `JoinHandle`s, which on tokio detaches them. Siblings kept pulling from a
//! queue that had no notion of cancellation and kept writing, ending in
//! `loader.finish()`, while `pipeline::run`'s error arm already ran
//! `sink.discard()`. The owner that outlives an error is this module: every
//! loader is finished or aborted before the join loop returns, and after a
//! failure no worker starts another commit.

use std::{
    collections::VecDeque,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use futures::FutureExt as _;

use crate::{
    error::{Error, Result},
    sink::Loader,
};

/// One set's failure latch. `claim` is its ONLY writer.
pub(crate) struct Latch {
    failed: AtomicBool,
}

impl Latch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            failed: AtomicBool::new(false),
        })
    }

    fn is_set(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// true for exactly ONE caller per set: the task whose error is THE error.
    fn claim(&self) -> bool {
        self.failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

pub(crate) fn cancelled() -> Error {
    Error::Transfer("cancelled: a sibling worker failed first — its error is the one reported".into())
}

/// The statement queue, owned by the set's latch. Replaces
/// `WorkQueue`/`work_queue`/`pop` (source/mod.rs): every pop is gated, so a
/// latched set drains nothing further.
#[derive(Clone)]
pub(crate) struct Spans {
    q: Arc<Mutex<VecDeque<String>>>,
    latch: Arc<Latch>,
}

impl Spans {
    fn new(stmts: Vec<String>, latch: Arc<Latch>) -> Self {
        Self {
            q: Arc::new(Mutex::new(VecDeque::from(stmts))),
            latch,
        }
    }

    /// `while let Some(sql) = spans.next()? {..}` is the only loop shape; it
    /// stops by construction: a latched set refuses the next statement.
    pub(crate) fn next(&self) -> Result<Option<String>> {
        if self.latch.is_set() {
            return Err(cancelled());
        }
        Ok(self.q.lock().unwrap().pop_front())
    }

    #[cfg(test)]
    pub(crate) fn remaining(&self) -> usize {
        self.q.lock().unwrap().len()
    }
}

/// How a worker's row count is read. Encodes TODAY's per-source rule exactly
/// (I10): the value handed to `sink.rows_staged` must not change.
pub(crate) enum Rows {
    /// pg (`source/postgres.rs` finish), mysql (both transfer workers): the
    /// loader's `finish()` value.
    FromLoader,
    /// clickhouse: the worker's own tally, `finish()` ignored.
    Own(u64),
    /// csv, gsheets, github+api: the loader's value when non-zero, the own
    /// tally otherwise.
    LoaderElseOwn(u64),
}

impl Rows {
    fn resolve(self, rep: u64) -> u64 {
        match self {
            Rows::FromLoader => rep,
            Rows::Own(n) => n,
            Rows::LoaderElseOwn(n) => {
                if rep > 0 {
                    rep
                } else {
                    n
                }
            }
        }
    }
}

/// One worker's handle on its loader. A source can `send` but cannot `finish`
/// or `abort`; only [`Pipe::drive`] consumes the loader.
pub(crate) struct Pipe<L: Loader> {
    inner: Option<L>,
    latch: Arc<Latch>,
    counted: bool,
}

impl<L: Loader> Pipe<L> {
    pub(crate) async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        if self.latch.is_set() {
            return Err(cancelled());
        }
        if self.counted {
            crate::progress::add_bytes(buf.len() as u64);
        }
        self.inner.as_mut().expect("pipe consumed").send(buf).await
    }

    pub(crate) async fn send_framed(
        &mut self,
        win: &[u8],
    ) -> Result<(usize, crate::wire::arrowcol::FramedPush)> {
        if self.latch.is_set() {
            return Err(cancelled());
        }
        let r = self.inner.as_mut().expect("pipe consumed").send_framed(win).await;
        if let (true, Ok((n, _))) = (self.counted, &r) {
            crate::progress::add_bytes(*n as u64);
        }
        r
    }

    pub(crate) fn reclaim(&mut self) -> Option<Vec<u8>> {
        self.inner.as_mut()?.reclaim()
    }

    pub(crate) fn framed_capable(&self) -> bool {
        self.inner.as_ref().is_some_and(|l| l.framed_capable())
    }

    // NO finish, NO abort, NO into_inner. `drive` is MODULE-PRIVATE.

    /// Consume this pipe exactly once: run the body, then finish or abort the
    /// loader. The latch is claimed BEFORE abort (D1) so siblings stop while a
    /// slow abort (BigQuery, S3) is still in flight.
    async fn drive<B: PipeBody<L>>(mut self, body: B, spans: Spans) -> Outcome {
        let r = AssertUnwindSafe(body.run(&mut self, spans)).catch_unwind().await;
        let inner = self.inner.take().expect("a pipe is consumed exactly once");
        match r {
            Ok(Ok(rows)) if !self.latch.is_set() => match inner.finish().await {
                Ok(rep) => Outcome::Done(rows.resolve(rep)),
                Err(e) => {
                    if self.latch.claim() {
                        Outcome::Cause(e)
                    } else {
                        Outcome::Cancelled
                    }
                }
            },
            Ok(Ok(_)) => {
                let _ = inner.abort(cancelled()).await;
                Outcome::Cancelled
            }
            Ok(Err(e)) => {
                let first = self.latch.claim();          // BEFORE abort: siblings stop during it (D1)
                let e = inner.abort(e).await;
                if first {
                    Outcome::Cause(e)
                } else {
                    debug_note(&e);
                    Outcome::Cancelled
                }
            }
            Err(p) => {
                let first = self.latch.claim();
                let e = inner
                    .abort(Error::Transfer(format!("worker panicked: {}", panic_text(&*p))))
                    .await;
                if first {
                    Outcome::Cause(e)
                } else {
                    Outcome::Cancelled
                }
            }
        }
    }
}

impl<L: Loader> Drop for Pipe<L> {
    // Tripwire only: reachable solely via JoinSet drop on the read path
    // (ReadHandle::drop), where the loader is an ArrowLoader that owes nothing.
    fn drop(&mut self) {
        if self.inner.is_some() && std::env::var_os("APITAP_DEBUG").is_some() {
            eprintln!("[pipe] dropped undriven — cancelled from above");
        }
    }
}

/// A loader's background task, joinable AT MOST ONCE.
///
/// Tokio answers a second poll of a finished handle with a PANIC ("JoinHandle
/// polled after completion"), and a loader's handle is reached from two
/// directions BY DESIGN: `send`/`finish` join it to learn the REAL failure — a
/// 413 from a body-limiting proxy, a COPY the server aborted — and the engine
/// then calls `abort` on that same loader, which must still WAIT for the task
/// before `discard` runs (I1, I5). Dropping the handle instead would detach
/// the task, which is the 0.56.0 defect all over again.
///
/// 0.57.0 held the bare handle, so the abort that follows a failed `send`
/// polled a completed one: the panic escaped `drive` (whose `catch_unwind`
/// covers `body.run` only), the join loop saw a `JoinError`, and the whole set
/// answered `worker panicked: JoinHandle polled after completion` INSTEAD of
/// the sink's own error. `join` TAKES the handle, so the second caller gets
/// `None` and the task is joined exactly once — and taking before the await
/// keeps the invariant true even when the joiner itself is cancelled.
pub(crate) struct JoinOnce<T> {
    h: Option<tokio::task::JoinHandle<T>>,
}

impl<T> JoinOnce<T> {
    pub(crate) fn new(h: tokio::task::JoinHandle<T>) -> Self {
        Self { h: Some(h) }
    }

    /// The task has ended, or there is none left to wait for. A loader about to
    /// stop something SERVER-side asks this first — the handle is usually gone
    /// by then, because `send` joined it to report the failure. Never a poll.
    pub(crate) fn is_finished(&self) -> bool {
        self.h.as_ref().map_or(true, |h| h.is_finished())
    }

    /// Join the task once. `None` = a previous caller already joined it: there
    /// is nothing left to wait for, and NOT "it succeeded".
    pub(crate) async fn join(
        &mut self,
    ) -> Option<std::result::Result<T, tokio::task::JoinError>> {
        match self.h.take() {
            Some(h) => Some(h.await),
            None => None,
        }
    }
}

/// `PendingJob` derives `Debug` and holds one of these, so the handle stays
/// unprinted: its `T` need not be `Debug`.
impl<T> std::fmt::Debug for JoinOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinOnce").finish_non_exhaustive()
    }
}

enum Outcome {
    Done(u64),
    Cause(Error),
    Cancelled,
}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string payload".into()
    }
}

fn debug_note(e: &Error) {
    if std::env::var_os("APITAP_DEBUG").is_some() {
        eprintln!("[pipe] sibling error after the set failed: {e}");
    }
}

/// What one task of a [`Pipes`] crew does between the queue and its loader.
pub(crate) trait PipeBody<L: Loader>: Send {
    fn run(self, pipe: &mut Pipe<L>, spans: Spans) -> impl Future<Output = Result<Rows>> + Send;
}

/// Loader-less body (the MySQL direct-Arrow read lane). Same latch, same queue.
pub(crate) trait SpanBody: Send {
    fn run(self, spans: Spans) -> impl Future<Output = Result<u64>> + Send;
}

async fn drive_span<T: SpanBody>(body: T, spans: Spans, latch: Arc<Latch>) -> Outcome {
    match AssertUnwindSafe(body.run(spans)).catch_unwind().await {
        Ok(Ok(n)) => Outcome::Done(n),
        Ok(Err(e)) => {
            if latch.claim() {
                Outcome::Cause(e)
            } else {
                Outcome::Cancelled
            }
        }
        Err(p) => {
            if latch.claim() {
                Outcome::Cause(Error::Transfer(format!("worker panicked: {}", panic_text(&*p))))
            } else {
                Outcome::Cancelled
            }
        }
    }
}

/// A task that ended in a `JoinError` is a pipe the crew did not consume:
/// `catch_unwind` covers `body.run` only, so a panic in `finish`/`abort`
/// escapes `drive`, and a runtime shutdown cancels the task. It is an ERROR,
/// and it names itself.
fn join_failure(je: tokio::task::JoinError) -> Error {
    match je.try_into_panic() {
        Ok(p) => Error::Transfer(format!("worker panicked: {}", panic_text(&*p))),
        // Not a panic: the task was cancelled from outside, so this pipe was
        // dropped mid-flight. Still an error — a crew that lost a pipe cannot
        // report that everything it sent landed.
        Err(je) => Error::Transfer(format!("worker ended without finishing: {je}")),
    }
}

/// THE join loop. Never returns early: the JoinSet is empty at return, so
/// `discard` runs only after every loader has finished or aborted (I1, I5).
async fn join_all(mut set: tokio::task::JoinSet<Outcome>, latch: &Latch) -> Result<u64> {
    // `lost` holds a JoinError whose task had ALREADY claimed the latch: the
    // 0.57.0 arm tried to claim and dropped the error when it lost the CAS, so
    // a body that failed and then panicked in its own `abort` left the set with
    // no cause and no cancel — and the loop answered `Ok(rows)` over a partial
    // commit. The latch cannot carry the error (the claiming task died with it),
    // so the join loop has to.
    let (mut cause, mut saw_cancel, mut rows, mut lost) = (None, false, 0u64, None);
    while let Some(j) = set.join_next().await {
        match j {
            Ok(Outcome::Done(n)) => rows += n,
            Ok(Outcome::Cause(e)) => cause = Some(e), // exactly one, by `claim`
            Ok(Outcome::Cancelled) => saw_cancel = true,
            Err(je) => {
                let e = join_failure(je);
                if latch.claim() {
                    cause = Some(e);
                } else if lost.is_none() {
                    lost = Some(e);
                } else {
                    debug_note(&e);
                }
            }
        }
    }
    // A claimed cause is still THE error (D1: the claim decides, not the
    // completion order); a pipe that ended in a JoinError comes next; a
    // cancelled sibling last. `Ok` requires EVERY task to have reported.
    match (cause, lost, saw_cancel) {
        (Some(e), _, _) => Err(e),
        (None, Some(e), _) => Err(e),
        (None, None, true) => Err(cancelled()),
        (None, None, false) => Ok(rows),
    }
}

pub(crate) async fn run_tasks<T: SpanBody + 'static>(
    stmts: Vec<String>,
    bodies: Vec<T>,
) -> Result<u64> {
    let latch = Latch::new();
    let spans = Spans::new(stmts, latch.clone());
    let mut set = tokio::task::JoinSet::new();
    for b in bodies {
        set.spawn(drive_span(b, spans.clone(), latch.clone()));
    }
    join_all(set, &latch).await
}

/// The set of loaders one `run_workers` call may drive. Fields are private to
/// this module: the only ways to consume a set are `run`, `run_inline`.
pub(crate) struct Pipes<L: Loader> {
    pipes: Vec<Pipe<L>>,
    latch: Arc<Latch>,
}

impl<L: Loader> Pipes<L> {
    /// Bulk lane: open `n` loaders, counted. If the k-th open fails, abort the
    /// k-1 already open (D4) — dropping them would detach what they started.
    pub(crate) async fn open<F, Fut>(n: usize, mut open: F) -> Result<Self>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<L>>,
    {
        let latch = Latch::new();
        let mut pipes = Vec::with_capacity(n);
        for _ in 0..n {
            match open().await {
                Ok(l) => pipes.push(Pipe {
                    inner: Some(l),
                    latch: latch.clone(),
                    counted: true,
                }),
                Err(e) => {
                    for mut p in pipes {
                        if let Some(l) = p.inner.take() {
                            let _ = l
                                .abort(Error::Transfer("a sibling pipe failed to open".into()))
                                .await;
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(Self { pipes, latch })
    }

    /// Read lane: ArrowLoaders, NOT counted (0.56.0's `Counted` wrapper existed
    /// only on the bulk lane — I10: progress bytes stay identical).
    pub(crate) fn for_read(loaders: Vec<L>) -> Self {
        let latch = Latch::new();
        Self {
            pipes: loaders
                .into_iter()
                .map(|l| Pipe {
                    inner: Some(l),
                    latch: latch.clone(),
                    counted: false,
                })
                .collect(),
            latch,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.pipes.len()
    }

    pub(crate) async fn run<B: PipeBody<L> + 'static>(
        self,
        stmts: Vec<String>,
        mut make: impl FnMut(usize) -> B,
    ) -> Result<u64> {
        let Pipes { pipes, latch } = self;
        let spans = Spans::new(stmts, latch.clone());
        let mut set = tokio::task::JoinSet::new();
        for (i, p) in pipes.into_iter().enumerate() {
            set.spawn(p.drive(make(i), spans.clone()));
        }
        join_all(set, &latch).await
    }

    /// Single-stream sources: no spawn, SAME drive.
    pub(crate) async fn run_inline<B: PipeBody<L>>(
        self,
        stmts: Vec<String>,
        body: B,
    ) -> Result<u64> {
        assert_eq!(self.pipes.len(), 1, "one span always yields one loader");
        let Pipes { pipes, latch } = self;
        let spans = Spans::new(stmts, latch.clone());
        match pipes.into_iter().next().unwrap().drive(body, spans).await {
            Outcome::Done(n) => Ok(n),
            Outcome::Cause(e) => Err(e),
            Outcome::Cancelled => Err(cancelled()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ev {
        Send(usize),
        Finish(usize),
        Abort(usize, String),
        /// The abort reached the point where it panics (logged so a test can
        /// prove the panic, not some earlier exit, is what it is looking at).
        Panic(usize, String),
    }

    /// One fake loader per pipe. `fail_send_at = Some(k)` makes the k-th send
    /// carry the error under test; `abort_delay` holds an abort open long
    /// enough to order it against sibling cancellation; `abort_panic` makes the
    /// abort itself panic, which is the one way a pipe can end in a JoinError.
    struct FakeLoader {
        id: usize,
        log: Arc<Mutex<Vec<Ev>>>,
        fail_send_at: Option<usize>,
        abort_delay: Duration,
        abort_panic: Option<&'static str>,
        finish_rows: u64,
        sends: usize,
    }

    impl FakeLoader {
        fn new(id: usize, log: Arc<Mutex<Vec<Ev>>>) -> Self {
            Self {
                id,
                log,
                fail_send_at: None,
                abort_delay: Duration::ZERO,
                abort_panic: None,
                finish_rows: 0,
                sends: 0,
            }
        }

        fn fail_send_at(mut self, k: usize) -> Self {
            self.fail_send_at = Some(k);
            self
        }

        fn abort_delay(mut self, d: Duration) -> Self {
            self.abort_delay = d;
            self
        }

        fn panics_in_abort(mut self, msg: &'static str) -> Self {
            self.abort_panic = Some(msg);
            self
        }

        fn finish_rows(mut self, n: u64) -> Self {
            self.finish_rows = n;
            self
        }

        fn log(&self) -> std::sync::MutexGuard<'_, Vec<Ev>> {
            self.log.lock().unwrap()
        }
    }

    impl Loader for FakeLoader {
        async fn send(&mut self, _buf: Vec<u8>) -> Result<()> {
            self.sends += 1;
            self.log().push(Ev::Send(self.id));
            if self.fail_send_at == Some(self.sends) {
                return Err(Error::Transfer(format!("send failed at {}", self.sends)));
            }
            Ok(())
        }

        async fn finish(self) -> Result<u64> {
            self.log().push(Ev::Finish(self.id));
            Ok(self.finish_rows)
        }

        async fn abort(self, cause: Error) -> Error {
            // Log at COMPLETION, so log order is completion order: an abort
            // that finished before a slow one appears before it.
            tokio::time::sleep(self.abort_delay).await;
            if let Some(msg) = self.abort_panic {
                self.log().push(Ev::Panic(self.id, msg.into()));
                panic!("{msg}");
            }
            self.log().push(Ev::Abort(self.id, cause.to_string()));
            cause
        }
    }

    /// Counts live bodies; drops on every exit, including unwinding.
    struct Alive(Arc<AtomicUsize>);

    impl Alive {
        fn new(counter: &Arc<AtomicUsize>) -> Self {
            counter.fetch_add(1, SeqCst);
            Self(counter.clone())
        }
    }

    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.fetch_sub(1, SeqCst);
        }
    }

    fn abort_events(log: &Mutex<Vec<Ev>>) -> Vec<(usize, String)> {
        log.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Ev::Abort(id, cause) => Some((*id, cause.clone())),
                _ => None,
            })
            .collect()
    }

    fn finish_count(log: &Mutex<Vec<Ev>>) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Ev::Finish(_)))
            .count()
    }

    fn panic_count(log: &Mutex<Vec<Ev>>) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Ev::Panic(_, _)))
            .count()
    }

    fn drain(log: &Mutex<Vec<Ev>>) -> Vec<Ev> {
        log.lock().unwrap().clone()
    }

    /// 1. The 0.56.0 loop returned at the first `Err` and left the siblings
    /// running; the crew instead stops them and consumes every pipe.
    enum CrewBody {
        Boom { alive: Arc<AtomicUsize> },
        Spin { alive: Arc<AtomicUsize>, seen: Arc<Mutex<Option<Spans>>> },
    }

    impl PipeBody<FakeLoader> for CrewBody {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, spans: Spans) -> Result<Rows> {
            match self {
                CrewBody::Boom { alive } => {
                    let _alive = Alive::new(&alive);
                    pipe.send(vec![0; 8]).await?;
                    Err(Error::Transfer("boom".into()))
                }
                CrewBody::Spin { alive, seen } => {
                    let _alive = Alive::new(&alive);
                    *seen.lock().unwrap() = Some(spans.clone());
                    while let Some(_sql) = spans.next()? {
                        pipe.send(vec![0; 8]).await?;
                        tokio::task::yield_now().await;
                    }
                    Ok(Rows::FromLoader)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn first_error_cancels_siblings_and_consumes_every_pipe() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..4).map(|i| FakeLoader::new(i, log.clone())).collect::<Vec<_>>();
        let alive = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(None));
        let stmts: Vec<String> = (0..400).map(|i| format!("span {i}")).collect();
        let pipes = Pipes::for_read(loaders);
        assert_eq!(pipes.len(), 4);

        let res = pipes
            .run(stmts, |i| {
                if i == 0 {
                    CrewBody::Boom { alive: alive.clone() }
                } else {
                    CrewBody::Spin { alive: alive.clone(), seen: seen.clone() }
                }
            })
            .await;

        let err = res.expect_err("the failing worker's error must be returned");
        assert!(err.to_string().contains("boom"), "got: {err}");
        let events = drain(&log);
        let aborts = abort_events(&log);
        assert_eq!(aborts.len(), 4, "every pipe is consumed: {events:?}");
        assert_eq!(finish_count(&log), 0, "a failed set never finishes a loader: {events:?}");
        assert!(aborts.iter().any(|(id, cause)| *id == 0 && cause.contains("boom")));
        for (id, cause) in &aborts {
            if *id != 0 {
                assert!(cause.contains("cancelled:"), "sibling {id}: {cause}");
            }
        }
        assert!(
            seen.lock().unwrap().as_ref().unwrap().remaining() > 0,
            "siblings must stop before the queue drains"
        );
        assert_eq!(alive.load(SeqCst), 0, "run returns only after every task ended");
    }

    /// 2. The cause is preserved even when its abort is the slowest thing in
    /// the set (I4), and siblings stop while it runs (I6).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_cause_wins_even_when_its_abort_is_slowest() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..4)
            .map(|i| {
                let l = FakeLoader::new(i, log.clone());
                if i == 0 {
                    l.abort_delay(Duration::from_millis(300))
                } else {
                    l
                }
            })
            .collect::<Vec<_>>();
        let alive = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(None));
        let stmts: Vec<String> = (0..16).map(|i| format!("span {i}")).collect();

        let res = Pipes::for_read(loaders)
            .run(stmts, |i| {
                if i == 0 {
                    CrewBody::Boom { alive: alive.clone() }
                } else {
                    CrewBody::Spin { alive: alive.clone(), seen: seen.clone() }
                }
            })
            .await;

        let err = res.expect_err("a slow abort must not swallow the cause");
        assert!(err.to_string().contains("boom"), "got: {err}");
        let events = drain(&log);
        let pos = |id: usize| {
            events
                .iter()
                .position(|e| matches!(e, Ev::Abort(i, _) if *i == id))
                .unwrap_or_else(|| panic!("pipe {id} was never aborted: {events:?}"))
        };
        for id in 1..4 {
            assert!(
                pos(id) < pos(0),
                "sibling {id} aborted after the slow abort completed: {events:?}"
            );
        }
        assert!(
            seen.lock().unwrap().as_ref().unwrap().remaining() > 0,
            "siblings stop while the slow abort runs: {events:?}"
        );
    }

    /// 3. A `?` on `send` leaves the pipe to the engine, which aborts it with
    /// the send's own error (the csvfile shape, on an owned pipe).
    struct SendBody;

    impl PipeBody<FakeLoader> for SendBody {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, spans: Spans) -> Result<Rows> {
            while let Some(_sql) = spans.next()? {
                pipe.send(vec![0; 8]).await?;
            }
            Ok(Rows::FromLoader)
        }
    }

    #[tokio::test]
    async fn a_failing_send_is_still_aborted() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loader = FakeLoader::new(0, log.clone()).fail_send_at(2);
        let stmts = vec!["s1".to_string(), "s2".to_string(), "s3".to_string()];

        let res = Pipes::for_read(vec![loader]).run_inline(stmts, SendBody).await;

        let err = res.expect_err("the send error must propagate");
        assert_eq!(err.to_string(), "transfer: send failed at 2");
        let events = drain(&log);
        assert_eq!(
            abort_events(&log),
            vec![(0, "transfer: send failed at 2".to_string())],
            "the failing pipe is aborted with its own error: {events:?}"
        );
        assert_eq!(finish_count(&log), 0, "no clean end after a failed send: {events:?}");
    }

    /// 4. A panicking body is caught, aborts its own pipe, and still counts
    /// toward the crew's exit.
    enum PanicCrew {
        Panic { alive: Arc<AtomicUsize> },
        Spin { alive: Arc<AtomicUsize> },
    }

    impl PipeBody<FakeLoader> for PanicCrew {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, spans: Spans) -> Result<Rows> {
            match self {
                PanicCrew::Panic { alive } => {
                    let _alive = Alive::new(&alive);
                    pipe.send(vec![0; 8]).await?;
                    panic!("kaboom");
                }
                PanicCrew::Spin { alive } => {
                    let _alive = Alive::new(&alive);
                    while let Some(_sql) = spans.next()? {
                        pipe.send(vec![0; 8]).await?;
                        tokio::task::yield_now().await;
                    }
                    Ok(Rows::FromLoader)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panicking_body_still_aborts_its_pipe() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..3).map(|i| FakeLoader::new(i, log.clone())).collect::<Vec<_>>();
        let alive = Arc::new(AtomicUsize::new(0));
        let stmts: Vec<String> = (0..64).map(|i| format!("span {i}")).collect();

        let res = Pipes::for_read(loaders)
            .run(stmts, |i| {
                if i == 2 {
                    PanicCrew::Panic { alive: alive.clone() }
                } else {
                    PanicCrew::Spin { alive: alive.clone() }
                }
            })
            .await;

        let err = res.expect_err("a panicking worker is an error");
        assert!(err.to_string().contains("worker panicked"), "got: {err}");
        let events = drain(&log);
        let aborts = abort_events(&log);
        assert_eq!(aborts.len(), 3, "every pipe is consumed: {events:?}");
        assert_eq!(finish_count(&log), 0, "a panicked body never finishes: {events:?}");
        assert!(aborts.iter().any(|(id, cause)| *id == 2 && cause.contains("worker panicked")));
        assert_eq!(alive.load(SeqCst), 0, "run returns only after every task ended");
    }

    /// 5. Completion order is not ownership: a body that returns `Ok` after
    /// the set failed is aborted, not finished.
    enum LateCrew {
        Fail { ready: Arc<AtomicBool> },
        Late { ready: Arc<AtomicBool>, seen: Arc<Mutex<Option<Spans>>> },
    }

    impl PipeBody<FakeLoader> for LateCrew {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, spans: Spans) -> Result<Rows> {
            match self {
                LateCrew::Fail { ready } => {
                    while !ready.load(SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Err(Error::Transfer("boom".into()))
                }
                LateCrew::Late { ready, seen } => {
                    let _ = spans.next()?.expect("one span");
                    *seen.lock().unwrap() = Some(spans.clone());
                    // The failure can only be ordered if the span is taken
                    // first; then this body deliberately returns Ok AFTER it.
                    ready.store(true, SeqCst);
                    for _ in 0..1_000_000 {
                        if pipe.latch.is_set() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(Rows::FromLoader)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_body_that_drains_after_the_failure_does_not_finish() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..2).map(|i| FakeLoader::new(i, log.clone())).collect::<Vec<_>>();
        let ready = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(None));

        let res = Pipes::for_read(loaders)
            .run(vec!["only span".to_string()], |i| {
                if i == 0 {
                    LateCrew::Fail { ready: ready.clone() }
                } else {
                    LateCrew::Late { ready: ready.clone(), seen: seen.clone() }
                }
            })
            .await;

        let err = res.expect_err("the failing pipe's error is returned");
        assert!(err.to_string().contains("boom"), "got: {err}");
        let events = drain(&log);
        assert_eq!(finish_count(&log), 0, "a body that returned Ok after the failure is aborted: {events:?}");
        let aborts = abort_events(&log);
        assert_eq!(aborts.len(), 2, "{events:?}");
        assert!(
            aborts.iter().any(|(id, cause)| *id == 1 && cause.contains("cancelled:")),
            "pipe 1 must be cancelled, not finished: {events:?}"
        );
    }

    /// 6. A partially opened crew aborts what it already opened (D4).
    #[tokio::test]
    async fn partial_open_aborts_the_open_pipes() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut calls = 0usize;

        let res = Pipes::<FakeLoader>::open(4, || {
            let id = calls;
            calls += 1;
            let log = log.clone();
            async move {
                if id == 2 {
                    Err(Error::Transfer("the third open failed".into()))
                } else {
                    Ok(FakeLoader::new(id, log))
                }
            }
        })
        .await;

        let err = match res {
            Ok(_) => panic!("the failing open must be returned"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("the third open failed"), "got: {err}");
        let events = drain(&log);
        assert_eq!(
            abort_events(&log),
            vec![
                (0, "transfer: a sibling pipe failed to open".to_string()),
                (1, "transfer: a sibling pipe failed to open".to_string()),
            ],
            "the already-open pipes are aborted: {events:?}"
        );
    }

    /// 7. `Rows::resolve` reproduces each source's current return expression
    /// exactly (I10).
    struct RowsBody(Rows);

    impl PipeBody<FakeLoader> for RowsBody {
        async fn run(self, _pipe: &mut Pipe<FakeLoader>, _spans: Spans) -> Result<Rows> {
            Ok(self.0)
        }
    }

    async fn resolved(rows: Rows, rep: u64) -> u64 {
        let log = Arc::new(Mutex::new(Vec::new()));
        let pipes = Pipes::for_read(vec![FakeLoader::new(0, log).finish_rows(rep)]);
        pipes
            .run_inline(Vec::new(), RowsBody(rows))
            .await
            .expect("finish succeeds")
    }

    #[tokio::test]
    async fn rows_rule_matches_each_source() {
        for rep in [0u64, 7] {
            assert_eq!(
                resolved(Rows::FromLoader, rep).await,
                rep,
                "pg/mysql take the finish() value (rep={rep})"
            );
            assert_eq!(
                resolved(Rows::Own(5), rep).await,
                5,
                "clickhouse ignores finish() (rep={rep})"
            );
            let own = 11;
            let want = if rep > 0 { rep } else { own };
            assert_eq!(
                resolved(Rows::LoaderElseOwn(own), rep).await,
                want,
                "csv/gsheets/github take the loader value when non-zero (rep={rep})"
            );
        }
    }

    /// 8. The loader-less lane stops siblings at their next span, exactly as
    /// the loader-bound crew does.
    enum SpanCrew {
        Boom { alive: Arc<AtomicUsize> },
        Spin { alive: Arc<AtomicUsize>, seen: Arc<Mutex<Option<Spans>>> },
    }

    impl SpanBody for SpanCrew {
        async fn run(self, spans: Spans) -> Result<u64> {
            match self {
                SpanCrew::Boom { alive } => {
                    let _alive = Alive::new(&alive);
                    Err(Error::Transfer("span boom".into()))
                }
                SpanCrew::Spin { alive, seen } => {
                    let _alive = Alive::new(&alive);
                    *seen.lock().unwrap() = Some(spans.clone());
                    let mut n = 0u64;
                    while let Some(_sql) = spans.next()? {
                        n += 1;
                        tokio::task::yield_now().await;
                    }
                    Ok(n)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn run_tasks_loaderless_stops_siblings() {
        let alive = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(None));
        let stmts: Vec<String> = (0..400).map(|i| format!("span {i}")).collect();
        let bodies = (0..4)
            .map(|i| {
                if i == 0 {
                    SpanCrew::Boom { alive: alive.clone() }
                } else {
                    SpanCrew::Spin { alive: alive.clone(), seen: seen.clone() }
                }
            })
            .collect();

        let res = crate::pipe::run_tasks(stmts, bodies).await;

        let err = res.expect_err("the first error is returned");
        assert!(err.to_string().contains("span boom"), "got: {err}");
        assert!(
            seen.lock().unwrap().as_ref().unwrap().remaining() > 0,
            "siblings stop at their next span"
        );
        assert_eq!(alive.load(SeqCst), 0, "run_tasks returns only after every task ended");
    }

    /// 9. The crew is the only spawn: a source that pushed a loader into its
    /// own task or queue would outlive the join loop, escape the latch, and
    /// keep writing while `discard` runs. read_impl.rs (the read supervisor)
    /// is deliberately not listed — its `JoinSet` drop is the one legal abort
    /// from above, and `ArrowLoader::abort` owes nothing.
    ///
    /// RED (mutation: add `tokio::spawn` or a `WorkQueue` pop to any listed
    /// file): the matching assert names that file; on the pre-rewire tree the
    /// production text of every database source contains one.
    const WATCHED: [(&str, &str); 11] = [
        ("source/mod.rs", include_str!("source/mod.rs")),
        ("source/postgres.rs", include_str!("source/postgres.rs")),
        ("source/mysql.rs", include_str!("source/mysql.rs")),
        ("source/clickhouse.rs", include_str!("source/clickhouse.rs")),
        ("source/csvfile.rs", include_str!("source/csvfile.rs")),
        ("source/github.rs", include_str!("source/github.rs")),
        ("source/gsheets.rs", include_str!("source/gsheets.rs")),
        ("source/github_api.rs", include_str!("source/github_api.rs")),
        ("pipeline/mod.rs", include_str!("pipeline/mod.rs")),
        ("pipeline/dispatch.rs", include_str!("pipeline/dispatch.rs")),
        ("sink/mod.rs", include_str!("sink/mod.rs")),
    ];

    const CFG_TEST: &str = "#[cfg(test)]";

    /// The production text of a watched file: the whole file minus every
    /// `#[cfg(test)]` item, each skipped by brace match.
    ///
    /// Why not "cut at the first `#[cfg(test)]`" (0.57.0): that is only correct
    /// when a file's test module comes last. `pipeline/dispatch.rs` carries an
    /// inline `#[cfg(test)] const ROUTES` INSIDE `macro_rules! routes`, ahead of
    /// the `route_single`/`route_multi`/`unsupported` arms the macro expands to
    /// and every helper below it — so that whole tail of production text was
    /// never scanned, and a worker pushed into `one`/`many` would have passed.
    fn production_text(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let (mut at, mut last) = (0usize, 0usize);
        while let Some(rel) = text[at..].find(CFG_TEST) {
            let i = at + rel;
            out.push_str(&text[last..i]);
            let end = item_end(text, i + CFG_TEST.len());
            at = end;
            last = end;
        }
        out.push_str(&text[last..]);
        out
    }

    /// Where the item behind an attribute ends: past the `}` that closes its
    /// block, or past the `;` of a `const`/`use`. A `(`/`[` nests (a `const`
    /// initializer is a bracket soup before its `;`), strings and comments are
    /// stepped over, and an unrecognisable tail stops at the attribute — an
    /// over-skip would hide production text, which is the defect being fixed.
    fn item_end(text: &str, from: usize) -> usize {
        let b = text.as_bytes();
        let (mut i, mut nest) = (from, 0i32);
        while i < b.len() {
            match b[i] {
                b'/' if b.get(i + 1) == Some(&b'/') => {
                    i += 2;
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b'"' | b'\'' if nest == 0 => {
                    i = literal_end(text, i, b[i]);
                    continue;
                }
                b'(' | b'[' => nest += 1,
                b')' | b']' => nest -= 1,
                b'{' | b';' if nest == 0 => {
                    return if b[i] == b'{' { block_end(text, i) } else { i + 1 };
                }
                _ => {}
            }
            i += 1;
        }
        from
    }

    /// Past a `"`/`'` string or `'a` lifetime, starting at the quote. A quote
    /// followed by an identifier char is a lifetime (nothing to skip); otherwise
    /// the literal ends at the next unescaped quote of the same kind.
    fn literal_end(text: &str, at: usize, q: u8) -> usize {
        let b = text.as_bytes();
        let nxt = b.get(at + 1).copied().unwrap_or(0);
        if q == b'\'' && (nxt.is_ascii_alphanumeric() || nxt == b'_') {
            return at + 1;
        }
        let mut i = at + 1;
        while i < b.len() {
            if b[i] == b'\\' {
                i += 2;
                continue;
            }
            if b[i] == q {
                return i + 1;
            }
            if b[i] == b'\n' && q == b'\'' {
                return i; // not a char literal after all
            }
            i += 1;
        }
        b.len()
    }

    /// Past the `}` matching the `{` at `at`. Braces inside strings, raws and
    /// comments do not count: a test's `"{name}: {bad:?}"` is the reason a raw
    /// count would mis-pair and swallow the production text after it.
    fn block_end(text: &str, at: usize) -> usize {
        let b = text.as_bytes();
        let (mut i, mut depth) = (at, 0i32);
        while i < b.len() {
            match b[i] {
                b'/' if b.get(i + 1) == Some(&b'/') => {
                    i += 2;
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b'/' if b.get(i + 1) == Some(&b'*') => {
                    i += 2;
                    while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                        i += 1;
                    }
                    i += 2;
                    continue;
                }
                b'r' if matches!(b.get(i + 1), Some(b'"') | Some(b'#'))
                    && !b.get(i.wrapping_sub(1)).is_some_and(|c| c.is_ascii_alphanumeric()) =>
                {
                    let mut hashes = 0;
                    while b.get(i + 1 + hashes) == Some(&b'#') {
                        hashes += 1;
                    }
                    if b.get(i + 1 + hashes) == Some(&b'"') {
                        let close = format!("\"{}", "#".repeat(hashes));
                        i += match text[i + 1..].find(&close) {
                            Some(j) => j + close.len() + 1,
                            None => b.len(),
                        };
                        continue;
                    }
                }
                b'"' | b'\'' => {
                    i = literal_end(text, i, b[i]);
                    continue;
                }
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return i + 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        b.len()
    }

    #[test]
    fn no_spawn_outside_the_crew() {
        // The token set lint, not a parser: these five spellings are the ways
        // 0.56.0 detached a worker, and a source cannot be written into the
        // crew without deleting every one of them.
        const FORBIDDEN: [&str; 6] =
            ["tokio::spawn", "task::spawn", "JoinSet", "JoinHandle", "WorkQueue", "fn pop("];
        for (name, text) in WATCHED {
            // The test modules legitimately spawn (their 0.56.0-control bodies
            // live there); only production text is the source's surface, and
            // `production_text` keeps ALL of it that is not under `#[cfg(test)]`.
            let prod = production_text(text);
            assert!(
                prod.len() > 0,
                "{name}: the scanner stripped the whole file — it would pass on anything"
            );
            for bad in FORBIDDEN {
                assert!(
                    !prod.contains(bad),
                    "{name}: {bad:?} outside the crew — a worker could outlive the join loop"
                );
            }
        }
        // A new source must be listed above, or it never enters this lint.
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/source");
        let mut listed: Vec<String> = WATCHED
            .iter()
            .filter_map(|(n, _)| n.strip_prefix("source/").map(str::to_string))
            .collect();
        listed.sort();
        let mut actual: Vec<String> = std::fs::read_dir(dir)
            .expect("src/source")
            .map(|e| e.expect("dir entry").file_name().to_string_lossy().into_owned())
            .collect();
        actual.sort();
        assert_eq!(listed, actual, "a source file is missing from WATCHED: add it (it must not spawn)");
    }

    /// 10. The surface sources see is pinned: no `into_inner`, no `finish`,
    /// no `abort`, no public field. compile_fail doctests would compile
    /// against the PUBLIC API and pass vacuously (D6), so this lint pins it.
    const SRC: &str = include_str!("pipe.rs");

    fn fn_names_in(src: &str, header: &str) -> Vec<String> {
        let start = src.find(header).unwrap_or_else(|| panic!("impl not found: {header}"));
        let mut names = Vec::new();
        for line in src[start..].lines().skip(1) {
            if line == "}" {
                break;
            }
            let t = line.trim();
            let Some(t) = t.strip_prefix("pub(crate) ") else {
                continue;
            };
            let t = t.strip_prefix("async ").unwrap_or(t);
            if let Some(rest) = t.strip_prefix("fn ") {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                names.push(name);
            }
        }
        names
    }

    #[test]
    fn pipe_surface_is_pinned() {
        // The test module itself contains impls; sources only ever see the
        // production text, so cut at the test module. `rfind`, not `find`: the
        // `#[cfg(test)]` on `Spans::remaining` sits in the production text.
        let src = &SRC[..SRC.rfind("#[cfg(test)]").unwrap()];
        assert_eq!(
            fn_names_in(src, "impl<L: Loader> Pipe<L> {"),
            ["send", "send_framed", "reclaim", "framed_capable"],
            "Pipe's pub(crate) surface changed: a source could finish or abort a loader"
        );
        assert_eq!(
            fn_names_in(src, "impl<L: Loader> Pipes<L> {"),
            ["open", "for_read", "len", "run", "run_inline"],
            "Pipes' pub(crate) surface changed"
        );
        for line in src.lines() {
            let t = line.trim();
            if t.starts_with("pub ") || t.starts_with("pub(") {
                assert!(
                    !t.contains(':')
                        || t.contains("fn ")
                        || t.contains("struct ")
                        || t.contains("enum ")
                        || t.contains("trait ")
                        || t.contains("const ")
                        || t.contains("type ")
                        || t.contains("mod ")
                        || t.contains("use "),
                    "a public struct field would let a source take the loader out from under the engine: {line}"
                );
            }
        }
    }

    /// 12. (0.57.0 fix) A pipe whose body fails and whose loader PANICS in
    /// `abort`: the latch was already claimed by that same task, so the cause
    /// died with it and the old `Err(je)` arm could not claim either — the set
    /// answered `Ok(rows)` over a commit nothing verified. A JoinError is
    /// always an error, and it names itself.
    struct FailBody;

    impl PipeBody<FakeLoader> for FailBody {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, _spans: Spans) -> Result<Rows> {
            pipe.send(vec![0; 8]).await?;
            Err(Error::Transfer("boom".into()))
        }
    }

    #[tokio::test]
    async fn a_panicking_abort_is_never_swallowed() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loader = FakeLoader::new(0, log.clone()).panics_in_abort("abort exploded");
        // `run`, not `run_inline`: the JoinError only exists because the drive
        // runs in its own task.
        let res = Pipes::for_read(vec![loader])
            .run(vec!["one span".to_string()], |_| FailBody)
            .await;

        let err = res.expect_err("a panicking abort is an error, never a committed Ok");
        let msg = err.to_string();
        assert!(msg.contains("worker panicked"), "the panic must be named: {msg}");
        assert!(msg.contains("abort exploded"), "its payload must be named: {msg}");
        let events = drain(&log);
        assert_eq!(panic_count(&log), 1, "the abort really panicked: {events:?}");
        assert_eq!(finish_count(&log), 0, "a panicking abort never finishes: {events:?}");
    }

    /// 13. (0.57.0 fix) The mirror of 12, so the fix cannot become "any panic
    /// wins": the pipe that claimed the latch keeps the error even when a
    /// sibling's abort panics afterwards (D1 — the claim decides).
    enum MixedCrew {
        First,
        Second { log: Arc<Mutex<Vec<Ev>>> },
    }

    impl PipeBody<FakeLoader> for MixedCrew {
        async fn run(self, pipe: &mut Pipe<FakeLoader>, _spans: Spans) -> Result<Rows> {
            match self {
                MixedCrew::First => {
                    pipe.send(vec![0; 8]).await?;
                    Err(Error::Transfer("boom".into()))
                }
                MixedCrew::Second { log } => {
                    // Ordered on pipe 0's completed abort, so the claim is
                    // deterministic: pipe 1 can only fail after pipe 0 claimed.
                    while !abort_events(&log).iter().any(|(id, _)| *id == 0) {
                        tokio::task::yield_now().await;
                    }
                    Err(Error::Transfer("late".into()))
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_siblings_panicking_abort_does_not_steal_the_first_cause() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..2)
            .map(|i| {
                let l = FakeLoader::new(i, log.clone());
                if i == 1 {
                    l.panics_in_abort("sibling abort exploded")
                } else {
                    l
                }
            })
            .collect::<Vec<_>>();

        let res = Pipes::for_read(loaders)
            .run(vec!["one span".to_string()], |i| {
                if i == 0 {
                    MixedCrew::First
                } else {
                    MixedCrew::Second { log: log.clone() }
                }
            })
            .await;

        let err = res.expect_err("the first cause is still an error");
        assert!(
            err.to_string().contains("boom"),
            "the pipe that claimed keeps the error: {err}"
        );
        assert_eq!(panic_count(&log), 1, "the sibling really panicked: {:?}", drain(&log));
    }

    /// 14. The scanner itself, so lint 9's fix cannot rot back: an inline
    /// `#[cfg(test)]` item — `pipeline/dispatch.rs` has one inside
    /// `macro_rules!` — must hide ONLY its own lines, a test module's
    /// braces-inside-strings must not mis-pair and swallow what follows it, and
    /// production text on both sides must survive.
    #[test]
    fn the_production_scanner_skips_only_cfg_test_items() {
        let text = "\
macro_rules! m {
    #[cfg(test)]
    const ROUTES: &[(&str, &str)] = &[ (\"a\", \"b\") ];
    fn one() { let _ = 1; }
}
#[cfg(test)]
mod tests {
    // braces in a comment: {name} and {0} must not pair up
    const S: &str = \"{not code}\";
    #[test]
    fn t() { assert_eq!(S, \"{not code}\"); }
}
fn production() { tokio::spawn(async {}); }
";
        let prod = production_text(text);
        assert!(!prod.contains("ROUTES"), "the inline const is test text: {prod}");
        assert!(!prod.contains("not code"), "the test module is test text: {prod}");
        assert!(!prod.contains("braces in a comment"), "and its comment: {prod}");
        assert!(prod.contains("fn one()"), "the macro arm is production text: {prod}");
        assert!(
            prod.contains("tokio::spawn"),
            "production text after the test module must survive — lint 9 missed it entirely in 0.57.0: {prod}"
        );
    }

    /// The 413 a body-limiting proxy answers with, and the three things the
    /// operator has to read to fix it — the message `sink::clickhouse` builds
    /// and `benchmarks/e2e_ch_body_cap.py` asserts on the real transfer.
    const BODY_CAP_413: &str = "clickhouse 413 Payload Too Large: a proxy in front of \
         ClickHouse refused the request body. apitap streams each worker's data \
         as ONE chunked request, so a body limit (nginx client_max_body_size, \
         ingress/ALB caps) rejects it regardless of chunk size. Either raise that \
         limit, or set APITAP_CH_MAX_BODY=<size below the limit, e.g. 512K> plus a \
         matching chunk_bytes= to split the load across requests.";

    /// 15. (0.57.0 fix) The panic the CHECKPOINT 5 gate caught, in its own
    /// type: a finished task handle polled a second time. `join` TAKES, so the
    /// second caller has nothing to poll — it cannot panic, and it is told
    /// there was nothing left (`None`) rather than being handed a fake success.
    #[tokio::test]
    async fn a_loader_task_handle_is_joined_at_most_once() {
        let mut j = JoinOnce::new(tokio::spawn(async { 7u64 }));

        assert!(
            matches!(j.join().await, Some(Ok(7))),
            "the first join carries the task's value"
        );
        assert!(j.join().await.is_none(), "the second join has nothing to poll");
        assert!(j.is_finished(), "no task left is finished");
        assert!(j.join().await.is_none(), "and joining again stays None");
    }

    /// 16. (0.57.0 fix) The gate's whole case, at the crew's own level: pg →
    /// ClickHouse through a 1 MB-body proxy with `parallel=32`. Every worker's
    /// `send` fails; the first one joins its own request task to learn the REAL
    /// error, and `Pipe::drive` then calls `abort` on that same loader — which
    /// must still consume it and wait for its task. With the bare handle it
    /// waited for a task that had already ended, the panic escaped `drive`, the
    /// join loop saw a `JoinError`, and the set answered `worker panicked:
    /// JoinHandle polled after completion`, naming a tokio internal instead of
    /// the 413 that names the fix.
    struct SelfJoiningLoader {
        id: usize,
        log: Arc<Mutex<Vec<Ev>>>,
        join: JoinOnce<Result<u64>>,
    }

    impl SelfJoiningLoader {
        /// Every worker's request is refused by the proxy, so every task ends
        /// in the same 413.
        fn refused(id: usize, log: Arc<Mutex<Vec<Ev>>>) -> Self {
            let task = tokio::spawn(async { Err(Error::Transfer(BODY_CAP_413.to_string())) });
            Self { id, log, join: JoinOnce::new(task) }
        }
    }

    impl Loader for SelfJoiningLoader {
        async fn send(&mut self, _buf: Vec<u8>) -> Result<()> {
            self.log.lock().unwrap().push(Ev::Send(self.id));
            match self.join.join().await {
                Some(Ok(Ok(_))) => Ok(()),
                // The real failure, which the channel close alone never carried.
                Some(Ok(Err(e))) => Err(e),
                Some(Err(e)) => Err(Error::Transfer(format!("join: {e}"))),
                None => Err(Error::Transfer("the insert task was already joined".into())),
            }
        }

        async fn finish(self) -> Result<u64> {
            Err(Error::Transfer("a refused insert never finishes".into()))
        }

        async fn abort(mut self, cause: Error) -> Error {
            // The engine MUST consume the loader after a failed send, and this
            // is the wait that panicked: `send` had already joined the task.
            let joined = self.join.join().await;
            self.log.lock().unwrap().push(Ev::Abort(
                self.id,
                format!("{cause} [task joined: {}]", joined.is_some()),
            ));
            cause
        }
    }

    enum CappedCrew {
        /// The worker whose `send` is refused by the proxy, and which claims
        /// the set with the 413.
        First,
        /// A sibling: it keeps working until the claim, then returns `Ok` —
        /// which `drive` turns into an abort, so it finishes beside the failed
        /// worker instead of racing it for the first cause.
        Second,
    }

    impl PipeBody<SelfJoiningLoader> for CappedCrew {
        async fn run(self, pipe: &mut Pipe<SelfJoiningLoader>, _spans: Spans) -> Result<Rows> {
            match self {
                CappedCrew::First => {
                    pipe.send(vec![0; 8]).await?;
                    Ok(Rows::Own(1))
                }
                CappedCrew::Second => {
                    while !pipe.latch.is_set() {
                        tokio::task::yield_now().await;
                    }
                    Ok(Rows::Own(2))
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_sends_own_error_survives_the_abort_that_follows_it() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let loaders = (0..4)
            .map(|i| SelfJoiningLoader::refused(i, log.clone()))
            .collect::<Vec<_>>();

        let res = Pipes::for_read(loaders)
            .run(vec!["one span".to_string()], |i| {
                if i == 0 {
                    CappedCrew::First
                } else {
                    CappedCrew::Second
                }
            })
            .await;

        let err = res.expect_err("the 413 is the run's error");
        let msg = err.to_string();
        for want in ["413", "APITAP_CH_MAX_BODY", "client_max_body_size"] {
            assert!(msg.contains(want), "the 413 must name {want}: {msg}");
        }
        assert!(
            !msg.contains("worker panicked") && !msg.contains("polled after completion"),
            "a tokio internal must never stand in for the sink's error: {msg}"
        );
        let events = drain(&log);
        let aborts = abort_events(&log);
        assert_eq!(aborts.len(), 4, "every pipe is consumed: {events:?}");
        assert!(
            aborts.iter().any(|(id, cause)| *id == 0 && cause.contains("413")),
            "and the abort that panicked is the one carrying it: {events:?}"
        );
        assert!(
            aborts.iter().any(|(id, cause)| *id == 0 && cause.contains("task joined: false")),
            "the abort waits for a task its send already joined, and does not \
             poll it again: {events:?}"
        );
        assert_eq!(finish_count(&log), 0, "a refused insert never finishes: {events:?}");
    }

    /// 17. The rule the two tests above rest on, as a lint: every task a loader
    /// owns is a [`JoinOnce`]. A loader whose `send` joins its task to report
    /// the real failure and whose `abort` must wait for it again cannot hold a
    /// bare handle — that is the exact shape that panicked, and it is invisible
    /// in review because both awaits look locally correct.
    const SINKS_WITH_TASKS: [(&str, &str); 4] = [
        ("sink/clickhouse.rs", include_str!("sink/clickhouse.rs")),
        ("sink/postgres.rs", include_str!("sink/postgres.rs")),
        ("sink/mysql.rs", include_str!("sink/mysql.rs")),
        ("sink/bigquery.rs", include_str!("sink/bigquery.rs")),
    ];

    #[test]
    fn every_loader_task_handle_is_a_join_once() {
        for (name, text) in SINKS_WITH_TASKS {
            let prod = production_text(text);
            assert!(prod.len() > 0, "{name}: the scanner stripped the whole file — it would pass on anything");
            // A FIELD declaration is the one spelling that means "this loader
            // holds a handle it will await": a spawn site's return type has no
            // colon in front of it.
            assert!(
                !prod.contains(": tokio::task::JoinHandle"),
                "{name}: a loader holds a raw task handle. `send` and `abort` \
                 both reach it, and awaiting a finished one panics — hold it in \
                 a `pipe::JoinOnce` instead."
            );
        }
    }
}
