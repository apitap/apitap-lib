//! Postgres dialect vocabulary shared by [`crate::source::postgres`] and
//! [`crate::sink::postgres`]: identifier quoting and the UTC-pinned pool.

/// `foo` → `"foo"`, `fo"o` → `"fo""o"` — safe Postgres identifier quoting.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!(r#""{}""#, ident.replace('"', r#""""#))
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
