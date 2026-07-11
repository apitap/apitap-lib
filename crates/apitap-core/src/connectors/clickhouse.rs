//! ClickHouse connector: [`ChSink`] — streaming HTTP `INSERT … FORMAT RowBinary`
//! (or TabSeparated for the text fallback lane) into a staging MergeTree, swapped in
//! atomically with `EXCHANGE TABLES`.

use crate::driver::Loader;
use crate::error::{Error, Result};
use crate::plan::{Delivered, Lane, TablePlan, WireFormat};

/// A ClickHouse HTTP endpoint parsed from a `clickhouse://user:pass@host:port/db` URL
/// (`clickhouse+https://` or port 8443 → TLS; port defaults to 8123).
#[derive(Clone)]
pub(crate) struct ChConn {
    base: String,
    user: String,
    password: String,
    database: String,
    client: reqwest::Client,
}

impl ChConn {
    pub(crate) fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("clickhouse url: {e}")))?;
        let https = matches!(u.scheme(), "clickhouse+https" | "https") || u.port() == Some(8443);
        let host = u
            .host_str()
            .ok_or_else(|| Error::InvalidInput("clickhouse url: missing host".into()))?;
        let port = u.port().unwrap_or(if https { 8443 } else { 8123 });
        let database = u.path().trim_start_matches('/').to_string();
        Ok(Self {
            base: format!("{}://{host}:{port}/", if https { "https" } else { "http" }),
            user: if u.username().is_empty() {
                "default".into()
            } else {
                u.username().into()
            },
            password: u.password().unwrap_or("").to_string(),
            database: if database.is_empty() {
                "default".into()
            } else {
                database
            },
            client: reqwest::Client::new(),
        })
    }

    /// Common query params. `wait_end_of_query=1` buffers the response until the
    /// statement fully completed, so the HTTP status is trustworthy (otherwise an
    /// insert can fail after a 200 was already sent).
    fn params<'a>(&'a self, extra: &'a str) -> [(&'a str, &'a str); 4] {
        [
            ("database", self.database.as_str()),
            ("wait_end_of_query", "1"),
            ("date_time_input_format", "best_effort"),
            ("query", extra),
        ]
    }

    /// Run a statement with no input data (DDL, small SELECTs); returns the body.
    /// The SQL travels as the POST body — a body-less POST has no Content-Length and
    /// ClickHouse rejects it with 411.
    pub(crate) async fn exec(&self, query: &str) -> Result<String> {
        let resp = self
            .client
            .post(&self.base)
            .basic_auth(&self.user, Some(&self.password))
            .query(&self.params("")[..3])
            .body(query.to_string())
            .send()
            .await
            .map_err(|e| Error::Connect(format!("clickhouse: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "clickhouse {status}: {}",
                body.trim()
            )));
        }
        Ok(body)
    }

    /// Stream `body` into `query` (an `INSERT … FORMAT …`): query in the URL, data as
    /// a chunked body. Squash settings force ~1M-row blocks so each worker writes a few
    /// big MergeTree parts instead of hundreds of small ones (the probe showed
    /// OpenFileForWrite climbing into the hundreds — parts churn).
    pub(crate) async fn insert_stream(&self, query: &str, body: reqwest::Body) -> Result<()> {
        let resp = self
            .client
            .post(&self.base)
            .basic_auth(&self.user, Some(&self.password))
            .query(&self.params(query))
            .query(&[
                ("min_insert_block_size_rows", "1048576"),
                ("min_insert_block_size_bytes", "536870912"),
            ])
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("clickhouse insert: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!(
                "clickhouse {status}: {}",
                body.trim()
            )));
        }
        Ok(())
    }
}

/// `` ` ``-quote a ClickHouse identifier.
pub(crate) fn ch_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))
}

/// ClickHouse column type for a delivered value.
fn ch_type_of(d: &Delivered) -> String {
    match d {
        Delivered::Int { bytes, unsigned } => {
            let width = match bytes {
                1 => "8",
                2 => "16",
                4 => "32",
                _ => "64",
            };
            format!("{}Int{width}", if *unsigned { "U" } else { "" })
        }
        Delivered::Float32 => "Float32".into(),
        Delivered::Float64 => "Float64".into(),
        Delivered::Decimal { p: 0, .. } => "Float64".into(), // defensive; planners avoid it
        Delivered::Decimal { p, s } => format!("Decimal({p}, {s})"),
        Delivered::Bool => "UInt8".into(),
        Delivered::Date => "Date32".into(),
        Delivered::DateTime { utc: false } => "DateTime64(6)".into(),
        Delivered::DateTime { utc: true } => "DateTime64(6, 'UTC')".into(),
        Delivered::Uuid => "UUID".into(),
        Delivered::Json | Delivered::Text => "String".into(),
        Delivered::Bytes => "String".into(),
    }
}

pub(crate) struct ChSink {
    ch: ChConn,
    final_t: String,
    staging_t: String,
    /// `INSERT INTO staging FORMAT …`, fixed at prepare time.
    insert_sql: String,
}

impl ChSink {
    pub(crate) fn connect(url: &str, dest_table: &str) -> Result<Self> {
        // ClickHouse table names aren't schema-qualified the Postgres way — take the
        // bare name (the URL's /database picks the namespace).
        let dest_bare = dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t);
        Ok(Self {
            ch: ChConn::parse(url)?,
            final_t: ch_ident(dest_bare),
            staging_t: ch_ident(&format!("{dest_bare}__apitap_staging")),
            insert_sql: String::new(),
        })
    }
}

impl crate::driver::Sink for ChSink {
    type Loader = ChLoader;

    fn accepts(&self) -> &'static [WireFormat] {
        // Best first: binary when the source can transcode every column, else text.
        &[WireFormat::RowBinary, WireFormat::TabSeparated]
    }

    fn adjust_plan(&self, plan: &mut TablePlan) {
        // The ORDER BY column must be non-nullable in ClickHouse; the encoders read
        // the same flag, so DDL and wire stay in agreement.
        if let Some(cursor) = plan.cursor.clone() {
            for c in &mut plan.cols {
                if c.name == cursor {
                    c.nullable = false;
                }
            }
        }
    }

    async fn prepare(&mut self, plan: &TablePlan, lane: &Lane, _durable: bool) -> Result<()> {
        let ddl_list = plan
            .cols
            .iter()
            .zip(lane.cols.iter())
            .map(|(c, lc)| {
                let ty = ch_type_of(&lc.delivered);
                let ty = if c.nullable {
                    format!("Nullable({ty})")
                } else {
                    ty
                };
                format!("{} {}", ch_ident(&c.name), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let order_by = plan
            .cursor
            .as_deref()
            .map_or("tuple()".to_string(), ch_ident);
        self.ch
            .exec(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
            .await?;
        self.ch
            .exec(&format!(
                "CREATE TABLE {} ({ddl_list}) ENGINE = MergeTree ORDER BY {order_by}",
                self.staging_t
            ))
            .await?;
        let fmt = match lane.format {
            WireFormat::TabSeparated => "TabSeparated",
            WireFormat::RowBinary => "RowBinary",
            // accepts() never offers it — negotiation can't get here.
            WireFormat::PgCopyBinary => unreachable!("guarded by accepts()"),
        };
        self.insert_sql = format!("INSERT INTO {} FORMAT {fmt}", self.staging_t);
        Ok(())
    }

    async fn loader(&self) -> Result<ChLoader> {
        Ok(ChLoader::open(self.ch.clone(), self.insert_sql.clone()))
    }

    async fn rows_staged(&self, _loaded: u64) -> Result<u64> {
        Ok(self
            .ch
            .exec(&format!("SELECT count() FROM {}", self.staging_t))
            .await?
            .trim()
            .parse()
            .unwrap_or(0))
    }

    async fn finalize(&self, rows: u64) -> Result<()> {
        // Atomic swap-in; a 0-row source leaves any existing destination untouched.
        if rows == 0 {
            let _ = self
                .ch
                .exec(&format!("DROP TABLE IF EXISTS {}", self.staging_t))
                .await;
            return Ok(());
        }
        self.ch
            .exec(&format!(
                "CREATE TABLE IF NOT EXISTS {} AS {}",
                self.final_t, self.staging_t
            ))
            .await?;
        self.ch
            .exec(&format!(
                "EXCHANGE TABLES {} AND {}",
                self.staging_t, self.final_t
            ))
            .await?;
        self.ch
            .exec(&format!("DROP TABLE {}", self.staging_t))
            .await?;
        Ok(())
    }
}

/// One streaming HTTP insert. The worker's buffers go through a 2-slot channel into
/// the request body, so encoding overlaps the HTTP flush; the request itself runs in a
/// spawned task whose result carries the REAL failure (reqwest reduces a mid-body
/// error to an opaque "error sending request" on the body side).
pub(crate) struct ChLoader {
    tx: futures::channel::mpsc::Sender<std::io::Result<bytes::Bytes>>,
    join: tokio::task::JoinHandle<Result<()>>,
}

impl ChLoader {
    fn open(ch: ChConn, insert_sql: String) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel::<std::io::Result<bytes::Bytes>>(2);
        let join = tokio::spawn(async move {
            let body = reqwest::Body::wrap_stream(rx);
            ch.insert_stream(&insert_sql, body).await
        });
        Self { tx, join }
    }

    async fn real_error(join: &mut tokio::task::JoinHandle<Result<()>>) -> Error {
        match join.await {
            Ok(Ok(())) => Error::Transfer("clickhouse insert closed early".into()),
            Ok(Err(e)) => e,
            Err(e) => Error::Transfer(format!("join: {e}")),
        }
    }
}

impl Loader for ChLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        use futures::SinkExt;
        if self.tx.send(Ok(bytes::Bytes::from(buf))).await.is_err() {
            // The insert died — its task holds the real error.
            return Err(Self::real_error(&mut self.join).await);
        }
        Ok(())
    }

    async fn finish(self) -> Result<u64> {
        let Self { tx, mut join } = self;
        drop(tx); // clean end-of-body: ClickHouse commits the insert
        match (&mut join).await {
            Ok(r) => r.map(|_| 0), // rows counted server-side by the sink
            Err(e) => Err(Error::Transfer(format!("join: {e}"))),
        }
    }

    async fn abort(self, cause: Error) -> Error {
        use futures::SinkExt;
        let Self { mut tx, join } = self;
        // Erroring the body aborts the HTTP request, so ClickHouse DISCARDS the
        // partial stream instead of committing it.
        let _ = tx
            .send(Err(std::io::Error::other("apitap: source failed")))
            .await;
        drop(tx);
        let _ = join.await;
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ch_url_parses_scheme_port_and_database() {
        let c = ChConn::parse("clickhouse://alice:secret@ch.example:8123/bench").unwrap();
        assert_eq!(c.base, "http://ch.example:8123/");
        assert_eq!(c.user, "alice");
        assert_eq!(c.database, "bench");
        // Defaults: user `default`, db `default`, port 8123 http.
        let c = ChConn::parse("clickhouse://ch.example").unwrap();
        assert_eq!(c.base, "http://ch.example:8123/");
        assert_eq!(c.user, "default");
        assert_eq!(c.database, "default");
        // TLS via scheme or port.
        assert!(ChConn::parse("clickhouse+https://ch.example/d")
            .unwrap()
            .base
            .starts_with("https://ch.example:8443"));
        assert!(ChConn::parse("clickhouse://ch.example:8443/d")
            .unwrap()
            .base
            .starts_with("https://"));
    }

    #[test]
    fn ch_types_for_deliveries_match_the_old_maps() {
        assert_eq!(
            ch_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: true
            }),
            "UInt64"
        );
        assert_eq!(ch_type_of(&Delivered::Bool), "UInt8");
        assert_eq!(
            ch_type_of(&Delivered::Decimal { p: 18, s: 4 }),
            "Decimal(18, 4)"
        );
        assert_eq!(
            ch_type_of(&Delivered::DateTime { utc: true }),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(
            ch_type_of(&Delivered::DateTime { utc: false }),
            "DateTime64(6)"
        );
        assert_eq!(ch_type_of(&Delivered::Date), "Date32");
        assert_eq!(ch_type_of(&Delivered::Uuid), "UUID");
        assert_eq!(ch_type_of(&Delivered::Json), "String");
    }
}
