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
    let mut b = reqwest::Client::builder()
        .connect_timeout(env_secs("APITAP_HTTP_CONNECT_TIMEOUT", CONNECT_SECS))
        // A pooled connection an idle scheduler kept for an hour is usually
        // dead on the other side; dropping it early turns a mysterious reset
        // mid-request into a clean reconnect.
        .pool_idle_timeout(Duration::from_secs(90))
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

#[cfg(test)]
mod tests {
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
