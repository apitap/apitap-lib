//! Postgres dialect vocabulary shared by [`crate::source::postgres`] and
//! [`crate::sink::postgres`]: identifier quoting and the UTC-pinned pool.

use crate::error::{Error, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

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

pub(crate) async fn connect_pool(url: &str, max: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // timestamptz then serializes with a "+00" offset in text mode, which
                // ClickHouse's best_effort parser reads — saves an AT TIME ZONE cast on
                // every row. Binary streams are timezone-independent, so this is safe
                // for every lane.
                sqlx::Executor::execute(conn, "SET timezone = 'UTC'").await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .map_err(|e| Error::Connect(e.to_string()))
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
