//! One HTTP client builder for every service apitap talks to.
//!
//! A `reqwest::Client` with no timeouts will wait forever — not "a long time":
//! a peer that accepts the connection and then stops reading leaves the
//! request parked with no deadline of its own, and in a scheduled pipeline
//! that is worse than an error, because the DAG task never fails and never
//! finishes.
//!
//! The obvious fix is the wrong one, and it was shipped once before this note
//! existed. `reqwest::ClientBuilder::read_timeout` sounds like a gap-between-
//! bytes deadline; it is not. It bounds the whole span from sending the
//! request to receiving the response HEADERS, and only resets per frame once
//! the body starts. apitap's ClickHouse loader holds ONE request open for a
//! whole worker's share of a table and asks for `wait_end_of_query=1`, so the
//! server sends nothing at all until the INSERT is done. Measured: with a
//! 5-second setting, a transfer that takes 40 seconds died at 5.3 seconds
//! while streaming perfectly. At the 120-second default that was every load
//! over two minutes.
//!
//! So the deadlines here are the two that can tell "dead" from "slow":
//!
//! * `connect_timeout` bounds getting a socket, which should take a moment on
//!   any network worth transferring over, and cannot be confused with a long
//!   transfer because no transfer has started.
//! * TCP keepalive is what actually detects a peer that vanished mid-stream.
//!   The kernel probes an idle connection and errors the socket when nobody
//!   answers — which is the real question ("is anyone there?"), not the one a
//!   wall-clock deadline asks ("is this taking long?").
//!
//! `APITAP_HTTP_READ_TIMEOUT` still exists for deployments that genuinely want
//! a total request deadline, and is OFF by default because it cannot tell a
//! slow load from a dead peer.

use std::time::Duration;

/// Seconds allowed to establish a connection.
const CONNECT_SECS: u64 = 15;

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(key)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default),
    )
}

/// The client every HTTP-speaking source and sink should use.
///
/// Falls back to a default client if the builder somehow fails, so a timeout
/// setting can never be the reason a transfer refuses to start.
pub(crate) fn client() -> reqwest::Client {
    // A pooled connection an idle scheduler kept for an hour is usually dead
    // on the other side; dropping it early turns a mysterious reset
    // mid-request into a clean reconnect.
    client_idle(Duration::from_secs(90))
}

/// `client()` for a server whose own keep-alive is SHORTER than 90 s, with
/// `idle` below it. ClickHouse closes an idle connection after 10 s
/// (`keep_alive_timeout`); a pooled connection older than that is closed on
/// the far side, and reusing it fails the statement with "error sending
/// request". A running process usually notices the FIN first — a process
/// that was stopped (a frozen VM, a SIGSTOP, a starved cgroup) does not, and
/// its first statement after the pause was the one that failed.
/// Idle-pool lifetime for pooled HTTP connections (`APITAP_HTTP_POOL_IDLE_MS`):
/// unset = 5000 ms (the historical default), `0` disables connection reuse
/// entirely. The park-cost pass (2026-10-12, §14.33) found the drain's tokio
/// driver spinning ~10k epoll_wait/s on an always-ready pooled ClickHouse
/// socket between requests (fdinfo: EPOLLIN|EPOLLOUT|ET fired forever); a
/// fresh loopback connection costs almost nothing, a remote TLS one costs a
/// handshake — hence the knob, not a hardcoded choice.
pub(crate) fn pool_idle_ms() -> u64 {
    std::env::var("APITAP_HTTP_POOL_IDLE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(POOL_IDLE_MS)
}

const POOL_IDLE_MS: u64 = 5000;

pub(crate) fn client_idle(idle: Duration) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .connect_timeout(env_secs("APITAP_HTTP_CONNECT_TIMEOUT", CONNECT_SECS))
        .pool_idle_timeout(idle)
        // The one deadline that asks the right question. Probes start after a
        // minute of silence, and the socket errors when nobody answers them —
        // a transfer that is merely slow keeps sending and is never probed.
        .tcp_keepalive(Duration::from_secs(60));
    // Opt-in only, and it is a TOTAL request deadline despite its name in
    // reqwest — see the module note. A deployment that sets it is choosing to
    // fail long loads, which is sometimes what you want and never a default.
    if let Some(secs) = std::env::var("APITAP_HTTP_READ_TIMEOUT")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
    {
        b = b.read_timeout(Duration::from_secs(secs));
    }
    b.build().unwrap_or_default()
}

/// A reqwest error that says WHY. reqwest's Display stops at "error sending
/// request for url (...)": the cause (timed out, connection refused, reset by
/// peer, a certificate refused) lives in the source chain, which nothing
/// printed, so a 5-second deadline and a dead server read the same. The URL
/// is left out: every caller already names the endpoint, the ClickHouse one
/// is 600 characters of settings, and some URLs carry a credential (a
/// resumable session URI's upload_id).
pub(crate) fn why(e: reqwest::Error) -> String {
    why_in(e, std::env::var("APITAP_HTTP_READ_TIMEOUT").ok())
}

/// `why` with the read-timeout setting passed in, so a test never races
/// another one over the process environment.
fn why_in(e: reqwest::Error, read_timeout: Option<String>) -> String {
    let (timeout, connect) = (e.is_timeout(), e.is_connect());
    let e = e.without_url();
    let mut out = e.to_string();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        let m = s.to_string();
        if !out.contains(&m) {
            out.push_str(": ");
            out.push_str(&m);
        }
        src = s.source();
    }
    if timeout {
        if !out.contains("timed out") {
            out.push_str(": timed out");
        }
        out.push_str(&if connect {
            format!(
                " (no connection within APITAP_HTTP_CONNECT_TIMEOUT={}s)",
                env_secs("APITAP_HTTP_CONNECT_TIMEOUT", CONNECT_SECS).as_secs()
            )
        } else {
            match read_timeout {
                Some(v) if v.trim().parse::<u64>().is_ok_and(|v| v > 0) => format!(
                    " (APITAP_HTTP_READ_TIMEOUT={}s is a total deadline per request — \
                     unset it to let a long load finish)",
                    v.trim()
                ),
                _ => format!(
                    " (a control request gets APITAP_HTTP_REQUEST_TIMEOUT={}s)",
                    control_timeout().as_secs()
                ),
            }
        });
    }
    out
}

#[cfg(test)]
mod why_tests {
    /// A peer that accepts and never answers, behind a deadline: the error
    /// has to say it timed out and which knob set the deadline. reqwest's own
    /// Display said "error sending request for url (...)" and nothing else.
    #[tokio::test]
    async fn a_timeout_says_it_timed_out_and_names_the_knob() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let hold = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            drop(s);
        });
        // The deadline client_idle builds from APITAP_HTTP_READ_TIMEOUT=1.
        let c = reqwest::Client::builder()
            .read_timeout(std::time::Duration::from_secs(1))
            .build()
            .unwrap();
        let e = c
            .post(format!("http://{addr}/?password=SECRET"))
            .body("x")
            .send()
            .await
            .unwrap_err();
        let m = super::why_in(e, Some("1".into()));
        hold.abort();
        assert!(m.contains("timed out"), "{m}");
        assert!(m.contains("APITAP_HTTP_READ_TIMEOUT=1s"), "{m}");
        assert!(!m.contains("SECRET"), "the URL leaked: {m}");
    }

    /// A refused connection names the refusal, not just "error sending".
    #[tokio::test]
    async fn a_refused_connection_says_refused() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let e = super::client()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap_err();
        let m = super::why(e);
        assert!(m.to_lowercase().contains("refused"), "{m}");
    }
}

#[cfg(test)]
mod tests {
    /// The pool-idle knob: unset = 5000 ms, 0 disables reuse, junk falls
    /// back (a typo must not silently change connection semantics).
    #[test]
    fn pool_idle_ms_defaults_disables_and_rejects_junk() {
        // Literal, not the symbol: a drift of the default must be a test
        // failure, not something the assertion follows silently.
        std::env::remove_var("APITAP_HTTP_POOL_IDLE_MS");
        assert_eq!(super::pool_idle_ms(), 5000);
        std::env::set_var("APITAP_HTTP_POOL_IDLE_MS", "0");
        assert_eq!(super::pool_idle_ms(), 0);
        std::env::set_var("APITAP_HTTP_POOL_IDLE_MS", "junk");
        assert_eq!(super::pool_idle_ms(), 5000);
        std::env::set_var("APITAP_HTTP_POOL_IDLE_MS", "250");
        assert_eq!(super::pool_idle_ms(), 250);
        std::env::remove_var("APITAP_HTTP_POOL_IDLE_MS");
    }
    use super::*;

    #[test]
    fn the_read_deadline_is_off_unless_asked_for() {
        // The regression this module exists to not repeat: a default deadline
        // here kills a healthy long upload, because the server sends nothing
        // until it is finished.
        std::env::remove_var("APITAP_HTTP_READ_TIMEOUT");
        let _ = client();
        assert!(std::env::var("APITAP_HTTP_READ_TIMEOUT").is_err());
    }

    #[test]
    fn env_overrides_are_read_and_junk_is_ignored() {
        // A deployment that needs a wider gap can say so.
        std::env::set_var("APITAP_TEST_SECS", "5");
        assert_eq!(env_secs("APITAP_TEST_SECS", 99), Duration::from_secs(5));
        // Zero would mean "no deadline", which is the bug this module exists
        // to prevent, so it falls back rather than disabling the timeout.
        std::env::set_var("APITAP_TEST_SECS", "0");
        assert_eq!(env_secs("APITAP_TEST_SECS", 99), Duration::from_secs(99));
        std::env::set_var("APITAP_TEST_SECS", "soon");
        assert_eq!(env_secs("APITAP_TEST_SECS", 99), Duration::from_secs(99));
        std::env::remove_var("APITAP_TEST_SECS");
        assert_eq!(env_secs("APITAP_TEST_SECS", 99), Duration::from_secs(99));
    }

    #[test]
    fn the_shared_client_builds() {
        let _ = client();
    }
}

/// The deadline for one small control-plane request (a metadata or listing
/// page, a token exchange): `APITAP_HTTP_REQUEST_TIMEOUT` seconds, default
/// 120. The client has no read timeout by default (long uploads need that), so
/// a live peer that accepted the request and never answered hung the run
/// forever; keepalive only finds dead ones (P2). Never on a body that streams.
pub(crate) fn control_timeout() -> Duration {
    env_secs("APITAP_HTTP_REQUEST_TIMEOUT", 120)
}

/// Attempts an object-store call gets (S3, GCS).
pub(crate) const STORE_ATTEMPTS: u32 = 6;

/// Whether an object-store answer is worth another attempt: a 5xx, a 408,
/// or a 429. 429 was not retried at all (GCS rateLimitExceeded, S3-compatible
/// stores that throttle), and the 1.5 s total backoff was far below what a
/// throttle asks for, so a brief one failed a whole load at finalize (P2).
pub(crate) fn store_retryable(s: reqwest::StatusCode) -> bool {
    s.is_server_error() || s.as_u16() == 408 || s.as_u16() == 429
}

/// The wait before attempt `attempt + 1` (1-based): 0.5, 1, 2, 4, 8 s plus up
/// to a quarter of jitter, or the server's `Retry-After` seconds (capped at
/// 60 s) when it sent one.
pub(crate) fn store_backoff(
    attempt: u32,
    retry_after: Option<&reqwest::header::HeaderValue>,
) -> Duration {
    if let Some(secs) = retry_after
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        return Duration::from_secs(secs.min(60));
    }
    let base = 500u64 << attempt.saturating_sub(1).min(4);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(base + nanos % (base / 4 + 1))
}

#[cfg(test)]
mod store_retry_tests {
    use super::*;

    /// P2: 429 and 408 retry like a 5xx; the backoff grows, and a
    /// Retry-After wins.
    #[test]
    fn throttles_are_retried_with_growing_backoff() {
        use reqwest::StatusCode as S;
        assert!(store_retryable(S::TOO_MANY_REQUESTS) && store_retryable(S::REQUEST_TIMEOUT));
        assert!(store_retryable(S::SERVICE_UNAVAILABLE) && !store_retryable(S::FORBIDDEN));
        let d1 = store_backoff(1, None);
        let d5 = store_backoff(5, None);
        assert!(
            d1 >= Duration::from_millis(500) && d1 < Duration::from_millis(700),
            "{d1:?}"
        );
        assert!(d5 >= Duration::from_secs(8), "{d5:?}");
        let ra = reqwest::header::HeaderValue::from_static("3");
        assert_eq!(store_backoff(1, Some(&ra)), Duration::from_secs(3));
    }
}
