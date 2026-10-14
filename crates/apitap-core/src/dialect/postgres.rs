//! Postgres dialect vocabulary shared by [`crate::source::postgres`] and
//! [`crate::sink::postgres`]: identifier quoting and the UTC-pinned pool.

/// `foo` → `"foo"`, `fo"o` → `"fo""o"` — safe Postgres identifier quoting.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!(r#""{}""#, ident.replace('"', r#""""#))
}

/// The Postgres TLS default (review 2026-10-14 P0-3), mirroring the MySQL
/// side's rule from 0.55.1: a URL that says nothing about TLS and does not
/// point at loopback gets `sslmode=verify-full`. sqlx's own default is
/// `prefer` — it tries TLS and falls back to plaintext, then answers a
/// cleartext password request over that fallback, which a man-in-the-middle
/// arranges by answering SSLRequest `N` on the server's behalf. An explicit
/// `sslmode` of any value is honoured, including `disable`.
pub(crate) fn secure_url(url: &str) -> String {
    if crate::dialect::mysql::has_explicit_ssl_mode(url)
        || crate::dialect::mysql::host_is_loopback(url)
    {
        return url.to_string();
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}sslmode=verify-full")
}

/// `public.events` → `"public"."events"` (each path segment quoted).
pub(crate) fn quote_ident_path(path: &str) -> String {
    path.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}


#[cfg(test)]
mod tests {
    use super::*;

    /// P0-3: remote + no sslmode gets verified TLS; explicit modes and
    /// loopback hosts are untouched (the false-positive and the
    /// false-negative both matter — one breaks plaintext dev servers, the
    /// other reintroduces the downgrade).
    #[test]
    fn the_tls_default_appends_verify_full_off_loopback() {
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
