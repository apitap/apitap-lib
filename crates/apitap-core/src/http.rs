//! One HTTP client builder for every service apitap talks to.
//!
//! A `reqwest::Client` with no timeouts will wait forever. Not "a long time" —
//! forever: a peer that accepts the connection and then stops reading, a load
//! balancer that drops the flow without an RST, a proxy that half-closes, all
//! leave the request parked with no deadline of its own. In a scheduled
//! pipeline that is worse than an error, because the DAG task never fails and
//! never finishes; it just occupies its slot until something outside kills it.
//!
//! The deadlines here are deliberately of two kinds:
//!
//! * `connect_timeout` bounds getting a socket, which should take a moment on
//!   any network worth transferring over.
//! * `read_timeout` bounds the gap BETWEEN bytes, not the transfer. A 40 GB
//!   load job that keeps streaming is healthy no matter how long it runs; a
//!   connection that has sent nothing for minutes is not. A total `timeout()`
//!   cannot tell those apart, which is why it is not used here.
//!
//! Both are overridable per deployment — a WAN link to another continent, or
//! a warehouse that legitimately pauses mid-response under load, may need a
//! wider gap than the default.

use std::time::Duration;

/// Seconds allowed to establish a connection.
const CONNECT_SECS: u64 = 15;
/// Seconds allowed between two bytes of a response before the peer is
/// treated as gone. Long enough for a warehouse to think, short enough that
/// a hung socket does not outlive the schedule that started it.
const READ_SECS: u64 = 120;

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
    reqwest::Client::builder()
        .connect_timeout(env_secs("APITAP_HTTP_CONNECT_TIMEOUT", CONNECT_SECS))
        .read_timeout(env_secs("APITAP_HTTP_READ_TIMEOUT", READ_SECS))
        // A pooled connection an idle scheduler kept for an hour is usually
        // dead on the other side; dropping it early turns a mysterious reset
        // mid-request into a clean reconnect.
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

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
