//! Postgres dialect vocabulary shared by [`crate::source::postgres`] and
//! [`crate::sink::postgres`]: identifier quoting and the UTC-pinned pool.

/// `foo` → `"foo"`, `fo"o` → `"fo""o"` — safe Postgres identifier quoting.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!(r#""{}""#, ident.replace('"', r#""""#))
}

/// The host sqlx will actually connect to: its `hostaddr`/`host` query
/// parameters override the URL authority (`sqlx-postgres/src/options/parse.rs`),
/// so the TLS decision must follow them, not `Url::host_str` alone
/// (review round 2 §1.1).
fn effective_host(url: &reqwest::Url) -> Option<String> {
    let q = |k: &str| {
        url.query_pairs()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.into_owned())
    };
    q("hostaddr").or_else(|| q("host")).or_else(|| url.host_str().map(str::to_string))
}

/// A host that never crosses a switch: a Unix socket path (a query `host`
/// value starting with `/`; `Url` percent-decodes it), `localhost`, or a
/// literal loopback address. Deliberately narrow — a private RFC1918 address
/// is NOT local, and a hostname that happens to resolve to 127.0.0.1 is not
/// either, because this must be decidable from the URL alone.
pub(crate) fn host_is_local(h: &str) -> bool {
    let h = h.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
    // `%2F…` is the same socket path, percent-encoded in the authority —
    // without this it read as remote and got verify-full (MASTER M-50).
    h.starts_with('/') || h.starts_with("%2f")
        || h == "localhost"
        || h == "::1"
        || h.parse::<std::net::Ipv4Addr>().map(|a| a.is_loopback()).unwrap_or(false)
        || h.parse::<std::net::Ipv6Addr>().map(|a| a.is_loopback()).unwrap_or(false)
}

/// Is the EFFECTIVE host of this URL local? Unparseable counts as NOT local,
/// the risky side, like the MySQL helper.
pub(crate) fn url_is_local(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| effective_host(&u))
        .map(|h| host_is_local(&h))
        .unwrap_or(false)
}

/// The Postgres TLS default (review 2026-10-14 P0-3; round 2 §1.1 hardened
/// it): a URL that says nothing about TLS and whose EFFECTIVE host is not
/// local gets `sslmode=verify-full`. sqlx's own default is `prefer` — it
/// tries TLS and falls back to plaintext, then answers a cleartext password
/// request over that fallback, which a man-in-the-middle arranges by
/// answering SSLRequest `N` on the server's behalf.
///
/// Round-2 corrections: a Unix socket (`?host=/var/run/postgresql`) is local
/// and MUST NOT get TLS — it broke every socket deployment at 0.62; the
/// `host`/`hostaddr` overrides count as the host; and a set `PGSSLMODE`
/// (sqlx reads it; the URL used to silently override it) is treated as the
/// operator's explicit decision. Explicit URL modes of any value, including
/// `disable`, always win.
pub(crate) fn secure_url(url: &str) -> String {
    if crate::dialect::mysql::has_explicit_ssl_mode(url)
        || std::env::var_os("PGSSLMODE").is_some()
    {
        return url.to_string();
    }
    let Ok(u) = reqwest::Url::parse(url) else {
        // Unparseable: sqlx will say so; never guess a mode onto it.
        return url.to_string();
    };
    match effective_host(&u) {
        Some(h) if host_is_local(&h) => url.to_string(),
        Some(_) => {
            let sep = if url.contains('?') { '&' } else { '?' };
            format!("{url}{sep}sslmode=verify-full")
        }
        // No host anywhere (`postgres:///db`): sqlx defaults to a socket.
        None => url.to_string(),
    }
}

/// A Postgres connect failure, diagnosed against the URL the USER wrote.
///
/// Both pools dial `secure_url(url)`, which appends `sslmode=verify-full` to
/// the raw string. Diagnosing that string instead of the user's lost the
/// reserved-character hint for a password holding `?` (the appended
/// parameter lands inside the "password"), and the TLS hint wrapped an
/// already-prefixed error a second time ("connect: connect: …").
pub(crate) fn connect_error(what: &str, user_url: &str, e: impl std::fmt::Display) -> crate::error::Error {
    let base = crate::urlerr::connect_err(what, user_url, e);
    if url_is_local(user_url) {
        return base;
    }
    match base {
        crate::error::Error::Connect(m) => {
            crate::error::Error::Connect(format!("{m} — {}", tls_required_hint(user_url)))
        }
        other => other,
    }
}

/// The error a plaintext-only (or private-CA) server gets once TLS is the
/// default — names the opt-outs verbatim, same reasoning as
/// `dialect::mysql::tls_required_hint` (review round 2 §1.1.4).
pub(crate) fn tls_required_hint(url: &str) -> String {
    let host = reqwest::Url::parse(url)
        .ok()
        .and_then(|u| effective_host(&u))
        .unwrap_or_else(|| "the server".into());
    format!(
        "apitap requires TLS for Postgres connections to non-loopback hosts by          default, and {host} is not local. A server without TLS needs          sslmode=require (or sslmode=disable) in the URL; a private CA needs          sslrootcert=/path/to/ca.crt."
    )
}

/// `public.events` → `"public"."events"` (each path segment quoted).
pub(crate) fn quote_ident_path(path: &str) -> String {
    path.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}


/// The PG*-reading tests (here and in `wire::walsender`) run in parallel
/// with the ones that set those variables (cargo's default); without one
/// crate-wide lock the setter's window leaks into the readers and the suite
/// goes flaky.
#[cfg(test)]
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::env_guard;

    /// P0-3: remote + no sslmode gets verified TLS; explicit modes and
    /// loopback hosts are untouched (the false-positive and the
    /// false-negative both matter — one breaks plaintext dev servers, the
    /// other reintroduces the downgrade).
    #[test]
    fn the_tls_default_appends_verify_full_off_loopback() {
        let _env = env_guard();
        std::env::remove_var("PGSSLMODE");
        assert_eq!(
            secure_url("postgres://u:p@db.example.com/app"),
            "postgres://u:p@db.example.com/app?sslmode=verify-full"
        );
        assert_eq!(
            secure_url("postgres://u:p@db.example.com/app?application_name=x"),
            "postgres://u:p@db.example.com/app?application_name=x&sslmode=verify-full"
        );
        assert_eq!(
            secure_url("postgres://u:p@127.0.0.1:5432/app"),
            "postgres://u:p@127.0.0.1:5432/app"
        );
        assert_eq!(
            secure_url("postgres://u:p@localhost/app"),
            "postgres://u:p@localhost/app"
        );
        assert_eq!(
            secure_url("postgres://u:p@db.example.com/app?sslmode=disable"),
            "postgres://u:p@db.example.com/app?sslmode=disable"
        );
        assert_eq!(
            secure_url("postgres://u:p@db.example.com/app?sslmode=require"),
            "postgres://u:p@db.example.com/app?sslmode=require"
        );
    }

    /// Round-2 §1.1: the shapes the first cut broke. A Unix socket — plain or
    /// percent-encoded — is local and must stay untouched (TLS over a socket
    /// is a hard failure); the `host`/`hostaddr` overrides decide, not the
    /// authority (`localhost?host=prod` is REMOTE); a set `PGSSLMODE` is
    /// explicit.
    #[test]
    fn sockets_overrides_and_pgsslmode_are_respected() {
        let _env = env_guard();
        std::env::remove_var("PGSSLMODE");
        assert_eq!(
            secure_url("postgres:///app?host=/var/run/postgresql"),
            "postgres:///app?host=/var/run/postgresql"
        );
        assert_eq!(
            secure_url("postgres:///app?host=%2Fvar%2Frun%2Fpostgresql"),
            "postgres:///app?host=%2Fvar%2Frun%2Fpostgresql"
        );
        // M-50: the socket path percent-encoded in the AUTHORITY.
        assert_eq!(
            secure_url("postgres://%2Fvar%2Flib%2Fpostgresql/db"),
            "postgres://%2Fvar%2Flib%2Fpostgresql/db"
        );
        assert_eq!(
            secure_url("postgres://u:p@localhost/app?host=prod.example.com"),
            "postgres://u:p@localhost/app?host=prod.example.com&sslmode=verify-full"
        );
        assert_eq!(
            secure_url("postgres://u:p@127.0.0.1/app?hostaddr=10.0.0.5"),
            "postgres://u:p@127.0.0.1/app?hostaddr=10.0.0.5&sslmode=verify-full"
        );
        std::env::set_var("PGSSLMODE", "disable");
        assert_eq!(
            secure_url("postgres://u:p@db.example.com/app"),
            "postgres://u:p@db.example.com/app"
        );
        std::env::remove_var("PGSSLMODE");
    }

    #[test]
    fn quoting_escapes_embedded_quotes_and_paths() {
        assert_eq!(quote_ident("events"), r#""events""#);
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(quote_ident_path("public.events"), r#""public"."events""#);
        assert_eq!(quote_ident_path("bare"), r#""bare""#);
    }

}

pub(crate) const DEFAULT_PORT: u16 = 5432;

/// Unqualified table names live in `public` under the default search_path —
/// `events` and `public.events` are ONE relation, and every identity/collision
/// check must agree on that (this is the single copy of the rule).
pub(crate) fn canonical_table(t: &str) -> String {
    if t.contains('.') {
        t.to_string()
    } else {
        format!("public.{t}")
    }
}

/// Is this Postgres udt usable as an incremental cursor, and does its SQL
/// literal need quoting? Integers embed raw; date/time embed as quoted text
/// (`::text` round-trips them losslessly at microsecond precision).
pub(crate) fn cursor_quoted(udt: &str) -> crate::error::Result<bool> {
    match udt {
        "int2" | "int4" | "int8" => Ok(false),
        "date" | "timestamp" | "timestamptz" => Ok(true),
        other => Err(crate::error::Error::InvalidInput(format!(
            "cursor type '{other}' is not usable for append/merge — use an integer or \
             timestamp column"
        ))),
    }
}
