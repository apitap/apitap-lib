//! MySQL dialect vocabulary shared by [`crate::source::mysql`] and
//! [`crate::sink::mysql`]: identifier quoting and the binary-type set that must
//! agree column-for-column between the source SELECT (HEX) and the sink
//! LOAD DATA (UNHEX).

/// MySQL udts whose bytes aren't safe as connection-charset text — the TSV lane
/// ships them HEX-encoded and the sink UNHEXes them, so binary round-trips exactly.
pub(crate) fn is_binary_udt(udt: &str) -> bool {
    matches!(
        udt,
        "blob"
            | "tinyblob"
            | "mediumblob"
            | "longblob"
            | "binary"
            | "varbinary"
            | "bit"
            | "geometry"
            // Postgres vocabulary — a postgres → mysql plan flows through the
            // same LOAD DATA column list and arrives HEX-encoded too.
            | "bytea"
    )
}

/// `` ` ``-quote a MySQL identifier / dotted path.
pub(crate) fn my_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}
pub(crate) fn my_ident_path(path: &str) -> String {
    path.split('.').map(my_ident).collect::<Vec<_>>().join(".")
}

pub(crate) const DEFAULT_PORT: u16 = 3306;

/// Is this MySQL DATA_TYPE usable as an incremental cursor, and does its SQL
/// literal need quoting? (Same contract as the Postgres twin.)
pub(crate) fn cursor_quoted(udt: &str) -> crate::error::Result<bool> {
    match udt {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => Ok(false),
        "date" | "timestamp" | "datetime" => Ok(true),
        other => Err(crate::error::Error::InvalidInput(format!(
            "cursor type '{other}' is not usable for append/merge — use an integer or \
             timestamp column"
        ))),
    }
}

/// Whether a MySQL URL points at this machine — the one case where sending
/// credentials in clear is a deliberate, local choice rather than an accident.
///
/// This is the switch behind apitap's TLS default, and it is deliberately
/// narrow: a literal loopback address or `localhost`, nothing else. A private
/// RFC1918 address is NOT loopback — "it's only the internal network" is how
/// credentials end up on a switch span port — and a hostname that happens to
/// resolve to 127.0.0.1 is not either, because this must be decidable from the
/// URL alone, before any DNS.
pub(crate) fn host_is_loopback(url: &str) -> bool {
    match reqwest::Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_string)) {
        Some(h) => {
            let h = h.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
            h == "localhost"
                || h == "::1"
                || h.parse::<std::net::Ipv4Addr>().map(|a| a.is_loopback()).unwrap_or(false)
                || h.parse::<std::net::Ipv6Addr>().map(|a| a.is_loopback()).unwrap_or(false)
        }
        None => false, // unparseable: assume the risky case, require TLS
    }
}

/// The TLS default for a MySQL URL (0.55.1 for the bulk lanes, extended to
/// the CDC lanes by the 2026-10-14 review, P0-4): a URL that says nothing
/// about TLS and does not point at loopback gets `ssl-mode=VERIFY_IDENTITY`.
/// Explicit modes of any value are honoured, including `disabled`.
pub(crate) fn secure_url(url: &str) -> String {
    if has_explicit_ssl_mode(url) || host_is_loopback(url) {
        return url.to_string();
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}ssl-mode=VERIFY_IDENTITY")
}

/// Does this URL already say what it wants from TLS?
pub(crate) fn has_explicit_ssl_mode(url: &str) -> bool {
    reqwest::Url::parse(url)
        .map(|u| u.query_pairs().any(|(k, _)| k == "ssl-mode" || k == "sslmode"))
        .unwrap_or(false)
}

/// The error a plaintext-only server gets once TLS is the default.
///
/// It names the opt-out verbatim, because an operator who genuinely has an
/// internal server without certificates needs a ten-second fix, not a research
/// project — and because a vague TLS error is how people end up disabling
/// security wholesale instead of narrowly.
pub(crate) fn tls_required_hint(url: &str) -> String {
    let host = reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "the server".into());
    format!(
        "apitap requires TLS for MySQL connections to non-loopback hosts since \
         0.55.1, and {host} is not loopback. If this server genuinely has no \
         TLS, add ?ssl-mode=disabled to the URL to send credentials and data in \
         clear on purpose; ?ssl-mode=required encrypts without verifying the \
         certificate, and verify_identity checks it (verify_ca is refused: use \
         verify_identity or required)."
    )
}

#[cfg(test)]
mod tls_default_tests {
    use super::*;

    #[test]
    fn loopback_is_narrow_and_explicit_modes_are_respected() {
        for u in [
            "mysql://u:p@127.0.0.1:3307/db",
            "mysql://u:p@localhost/db",
            "mysql://u:p@[::1]:3306/db",
            "mysql://u:p@127.5.5.5/db",
        ] {
            assert!(host_is_loopback(u), "{u} is loopback");
        }
        for u in [
            "mysql://u:p@10.0.0.9/db",       // private, but off this machine
            "mysql://u:p@192.168.1.20/db",
            "mysql://u:p@db.internal/db",
            "mysql://u:p@prod-host:3306/db",
        ] {
            assert!(!host_is_loopback(u), "{u} is NOT loopback");
        }
        assert!(has_explicit_ssl_mode("mysql://h/db?ssl-mode=disabled"));
        assert!(has_explicit_ssl_mode("mysql://h/db?sslmode=required"));
        assert!(!has_explicit_ssl_mode("mysql://h/db?application_name=x"));
        // The message has to carry the opt-out verbatim or it is not actionable.
        let hint = tls_required_hint("mysql://u:p@prod-host/db");
        assert!(hint.contains("?ssl-mode=disabled"), "{hint}");
        assert!(hint.contains("prod-host"), "{hint}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P2/R3-16: the hint never offers a mode every entry point refuses.
    #[test]
    fn the_tls_hint_does_not_offer_verify_ca() {
        let h = super::tls_required_hint("mysql://u@db.example/x");
        assert!(h.contains("verify_ca is refused") && !h.contains("verify_ca /"), "{h}");
    }

    /// P0-4: the CDC lanes now share this mapping with the bulk lanes —
    /// remote + no mode gets verified TLS, explicit modes and loopback are
    /// untouched.
    #[test]
    fn the_tls_default_appends_verify_identity_off_loopback() {
        assert_eq!(
            secure_url("mysql://u:p@db.example.com:3306/app"),
            "mysql://u:p@db.example.com:3306/app?ssl-mode=VERIFY_IDENTITY"
        );
        assert_eq!(
            secure_url("mysql://u:p@db.example.com/app?charset=utf8mb4"),
            "mysql://u:p@db.example.com/app?charset=utf8mb4&ssl-mode=VERIFY_IDENTITY"
        );
        assert_eq!(
            secure_url("mysql://u:p@127.0.0.1:3306/app"),
            "mysql://u:p@127.0.0.1:3306/app"
        );
        assert_eq!(
            secure_url("mysql://u:p@localhost/app"),
            "mysql://u:p@localhost/app"
        );
        assert_eq!(
            secure_url("mysql://u:p@db.example.com/app?ssl-mode=DISABLED"),
            "mysql://u:p@db.example.com/app?ssl-mode=DISABLED"
        );
    }
}
